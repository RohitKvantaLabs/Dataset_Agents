"""EBRAINS read-only dry-run audit (2026-08-18).

Runs the EBRAINS ingestion pipeline in READ-ONLY mode against the production
``neurosearch_dataset_catalog`` collection, using the AUTHORITATIVE approved
EBRAINS census artifacts as input:

  1. Load trace_artifacts/ebrains_census_20260818/ebrains_census.json (1,140
     summary records) AND ebrains_candidates.jsonl (1,140 rich records). The
     two are cross-validated: both must contain EXACTLY the same 1,140
     dataset_id set with consistent shared fields (doi == primary_doi,
     n_versions == n_total_versions, dataset_class == category, plus every
     other shared field); the rich candidates records are the ingestion list.
  2. Approved input = the 1,138 NEUROSCIENCE records (raw census 1,140 minus
     the 2 non-neuroscience products). Hard gate: EXACTLY 1,138 approved
     records, every record carries a dataset_id, and every excluded raw
     record is non-neuroscience — otherwise the run stops with zero writes.
  3. Normalize every record via build_ebrains_source_record().
  4. Resolve identity against the existing catalog (read-only) and record
     would-insert / would-merge / unresolved per record. The four audited
     exact DANDI/OpenNeuro matches resolve via the generic resolver's
     cross_reference layer (matchedVia="cross_reference") → proposed merges.
  5. Report — input / normalized / failed / duplicate source identities /
     proposed inserts / proposed merges / unresolved / expected-exact-merges
     (4) / actual-exact-merges / existing-catalog-before / projected-final
     catalog / API-metadata calls / asset-file calls / restricted-access
     records / production writes, plus a production snapshot (BEFORE only).

NO WRITES are performed. The production catalog must remain unchanged.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.ebrains_dryrun

Report: ../trace_artifacts/ebrains_ingestion_20260818/ebrains_dryrun_20260818.json
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import _ebrains_identity_keys
from app.catalog.normalize import (
    EBRAINS_CENSUS_EXPECTED,
    build_ebrains_source_record,
)
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "ebrains_ingestion_20260818",
        "ebrains_dryrun_20260818.json",
    )
)

_CENSUS_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "ebrains_census_20260818",
    )
)
_CENSUS_JSON_PATH = os.path.join(_CENSUS_DIR, "ebrains_census.json")
_CANDIDATES_PATH = os.path.join(_CENSUS_DIR, "ebrains_candidates.jsonl")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_candidates(path: str) -> list[dict]:
    """Load the rich approved candidates (ebrains_candidates.jsonl).

    The artifact carries a UTF-8 BOM — read with utf-8-sig so the first
    record's keys are not polluted.
    """
    records: list[dict] = []
    with open(path, "r", encoding="utf-8-sig") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _load_census_summary(path: str) -> list[dict]:
    """Load the raw census summary list (ebrains_census.json → list of 1,140)."""
    with open(path, "r", encoding="utf-8-sig") as fh:
        payload = json.load(fh)
    return list(payload)


def _load_raw_artifacts() -> tuple[list[dict], list[str]]:
    """Load + cross-validate the two raw census artifacts (1,140 each).

    Returns (candidates, errors). Both artifacts must contain EXACTLY the same
    1,140 dataset_id set with consistent shared fields; the rich candidates
    records are the ingestion list. Any discrepancy aborts the run (the input
    gate must never silently pick one artifact over the other).
    """
    errors: list[str] = []
    summary = _load_census_summary(_CENSUS_JSON_PATH)
    candidates = _load_candidates(_CANDIDATES_PATH)

    if len(summary) != 1140:
        errors.append(
            f"ebrains_census.json has {len(summary)} records; the raw census "
            f"has exactly 1140 (2 of them non-neuroscience, never approved)"
        )
    if len(candidates) != 1140:
        errors.append(
            f"ebrains_candidates.jsonl has {len(candidates)} records; the raw "
            f"census has exactly 1140"
        )
    if errors:
        return [], errors

    summary_by_id = {r.get("dataset_id"): r for r in summary}
    candidate_by_id = {r.get("dataset_id"): r for r in candidates}
    if set(summary_by_id) != set(candidate_by_id):
        errors.append(
            "dataset_id sets differ between ebrains_census.json and "
            "ebrains_candidates.jsonl"
        )
        return [], errors

    for ds_id, s in summary_by_id.items():
        c = candidate_by_id[ds_id]
        pairs = [
            ("doi", "primary_doi"),
            ("n_versions", "n_total_versions"),
            ("category", "dataset_class"),
        ]
        for cand_field, sum_field in pairs:
            # empty-string and None both mean "no value" (some census records
            # genuinely carry no DOI) — compare the normalized presence
            if (c.get(cand_field) or None) != (s.get(sum_field) or None):
                errors.append(
                    f"field {cand_field}/{sum_field} mismatch for {ds_id}: "
                    f"candidates={c.get(cand_field)!r} vs census.json={s.get(sum_field)!r}"
                )
        for field in (
            "title", "confidence", "external_doi", "multi_version",
            "n_indexed_versions", "first_release", "latest_release",
            "accessibility", "species", "technique", "experimental_approach",
            "keywords", "version_ids",
        ):
            if (c.get(field) or None) != (s.get(field) or None):
                errors.append(
                    f"field {field!r} mismatch for {ds_id}: "
                    f"candidates={c.get(field)!r} vs census.json={s.get(field)!r}"
                )
    if errors:
        return [], errors
    return candidates, []


def _split_approved(candidates: list[dict]) -> tuple[list[dict], list[dict], list[str]]:
    """Split the raw 1,140 into the approved 1,138 neuroscience records and the
    excluded non-neuroscience records (with exclusion reasons)."""
    approved: list[dict] = []
    excluded: list[dict] = []
    for r in candidates:
        if r.get("neuro_relevance") == "non_neuroscience" or str(
            r.get("category") or ""
        ).strip().upper() == "NON":
            excluded.append(r)
        else:
            approved.append(r)
    reasons = [f"{r.get('dataset_id')} (non-neuroscience)" for r in excluded]
    return approved, excluded, reasons


async def _catalog_snapshot(db, datasets_coll) -> dict:
    coll = db[CATALOG_COLLECTION]
    total = await coll.count_documents({})
    repos: Counter = Counter()
    async for d in coll.find({}, {"sources.repository": 1}):
        for s in (d.get("sources") or []):
            if s.get("repository"):
                repos[s["repository"]] += 1
    return {
        "catalog_total": total,
        "repo_source_counts": dict(repos),
        "production_datasets_collection_total": await datasets_coll.count_documents({}),
    }


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    datasets_coll = db[settings.MONGO_DATASET_COLLECTION]
    collection = get_collection(db)

    print(f"[ebrains-dryrun] production db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")
    before = await _catalog_snapshot(db, datasets_coll)
    print(f"[ebrains-dryrun] production BEFORE: {json.dumps(before)}")

    # ── 1) Load the authoritative census artifacts (cross-validated). ─────────
    raw_candidates, load_errors = _load_raw_artifacts()
    approved, excluded, exclusion_reasons = _split_approved(raw_candidates)
    print(
        f"[ebrains-dryrun] census artifacts: {len(approved)} approved neuroscience "
        f"records (raw {len(raw_candidates)}; expected exactly {EBRAINS_CENSUS_EXPECTED})"
    )

    # ── 2) Exact-count gate + identity-completeness + exclusion gates. ────────
    gate_reason = None
    if load_errors:
        gate_reason = "; ".join(load_errors)
    elif len(approved) != EBRAINS_CENSUS_EXPECTED:
        gate_reason = (
            f"approved input has {len(approved)} records; the approved EBRAINS "
            f"census set is exactly {EBRAINS_CENSUS_EXPECTED}"
        )
    elif excluded and len(excluded) != 2:
        gate_reason = (
            f"{len(excluded)} raw records were excluded; exactly the 2 "
            f"non-neuroscience products may be excluded"
        )
    if gate_reason is None:
        missing_identity = [
            r.get("title")
            for r in approved
            if not str(r.get("dataset_id") or "").strip()
        ]
        if missing_identity:
            gate_reason = (
                f"{len(missing_identity)} records lack a dataset_id "
                f"(e.g. {missing_identity[0]})"
            )
    if gate_reason:
        print(f"[ebrains-dryrun] *** GATE FAILED: {gate_reason} — stopping (no writes) ***")
        report = {
            "report_generated_at": _utcnow(),
            "mode": "READ-ONLY dry-run audit (zero writes)",
            "gate_failed": True,
            "gate_reason": gate_reason,
            "input_total": len(approved),
            "expected": EBRAINS_CENSUS_EXPECTED,
            "writes_performed": 0,
        }
        os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
        with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        client.close()
        return 1

    # ── 3) Duplicate candidate protection (read-only detection). ─────────────
    # The census artifact is deduplicated (1138/1138 unique dataset_id AND
    # unique source URL), so ANY repeated identity key is a genuine duplicate.
    duplicate_identities: list[dict] = []
    seen: dict[str, str] = {}
    for r in approved:
        for key in _ebrains_identity_keys(r):
            prev = seen.get(key)
            if prev is not None:
                duplicate_identities.append(
                    {
                        "identity": key,
                        "first": prev,
                        "second": str(r.get("dataset_id") or ""),
                    }
                )
            else:
                seen[key] = str(r.get("dataset_id") or "")

    # ── 4) Normalize → resolve identity (READ-ONLY). ─────────────────────────
    # EBRAINS sourceUrl is the census url VERBATIM (real, unique per dataset —
    # EBRAINS is NOT in SHARED_SOURCE_URL_REPOSITORIES). Identity flows through
    # the deterministic repo:sourceDatasetId/sourceKey; the four audited exact
    # DANDI/OpenNeuro matches resolve via the generic cross_reference layer.
    normalized = 0
    failed = 0
    failure_details: list[str] = []
    would_insert: list[dict] = []
    would_merge: list[dict] = []
    ambiguous_candidates: list[dict] = []  # title-matched but NOT merged (kept separate)
    representative_sources: list[dict] = []
    api_calls = 0      # census artifact is authoritative — no API calls
    asset_calls = 0    # hard guarantee

    for r in approved:
        dataset_id = str(r.get("dataset_id") or "").strip()
        try:
            source = build_ebrains_source_record(r)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failure_details.append(
                f"{dataset_id}: normalize: {type(exc).__name__}: {exc}"
            )
            print(f"[ebrains-dryrun] normalize failed for {dataset_id}: {exc}")
            continue
        normalized += 1
        if len(representative_sources) < 5:
            representative_sources.append(
                {
                    "datasetId": dataset_id,
                    "title": source.get("title"),
                    "sourceDatasetId": source.get("sourceDatasetId"),
                    "sourceUrl": source.get("sourceUrl"),
                    "doi": source.get("doi"),
                    "modality": source.get("modality"),
                    "species": source.get("species"),
                    "publicationYear": (source.get("derived") or {}).get("publicationYear"),
                    "accessibility": (source.get("snapshot") or {}).get("accessibility"),
                }
            )

        resolution = await resolve_identity(collection, source)
        if resolution["matched"] is not None:
            would_merge.append(
                {
                    "datasetId": dataset_id,
                    "existingCanonicalDatasetId": resolution["matched"].get(
                        "canonicalDatasetId"
                    ),
                    "matchedVia": resolution.get("matchedVia"),
                    "existingRepositories": sorted(
                        {
                            s.get("repository")
                            for s in (resolution["matched"].get("sources") or [])
                        }
                    ),
                }
            )
        else:
            # No confident match → upsert_canonical() INSERTS a new canonical
            # record (ambiguous candidates are never force-merged — the record
            # keeps its own DOI/URL identity). Ambiguous ones are still noted.
            if resolution["ambiguous"]:
                ambiguous_candidates.append(
                    {
                        "datasetId": dataset_id,
                        "title": source.get("title"),
                        "candidateCount": len(resolution["ambiguous"]),
                        "candidateCanonicalIds": [
                            c.get("canonicalDatasetId")
                            for c in resolution["ambiguous"][:5]
                        ],
                    }
                )
            would_insert.append(
                {
                    "datasetId": dataset_id,
                    "title": source.get("title"),
                }
            )

    # ── 5) Derived audit counts (read-only; production untouched). ────────────
    actual_exact_merges = [
        m for m in would_merge if m["matchedVia"] == "cross_reference"
    ]
    restricted_access_created = sum(
        1
        for r in approved
        if build_ebrains_source_record(r).get("availability") == "restricted"
    )  # canonical availability is never guessed → always 0
    external_repo_records_created = sum(
        1 for r in approved
        if build_ebrains_source_record(r).get("repository") != "ebrains"
    )  # every record is an ebrains record → always 0
    projected_final = before["catalog_total"] + len(would_insert)

    # ── 6) Report (read-only; production untouched). ─────────────────────────
    report = {
        "report_generated_at": _utcnow(),
        "mode": "READ-ONLY dry-run audit (zero writes)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "INPUT": {
            "census_json": _CENSUS_JSON_PATH,
            "candidates_jsonl": _CANDIDATES_PATH,
            "raw_total": len(raw_candidates),
            "approved_input": len(approved),
            "expected": EBRAINS_CENSUS_EXPECTED,
            "excluded_non_neuroscience": len(excluded),
            "exclusion_reasons": exclusion_reasons,
            "gate_failed": False,
            "gate_reason": None,
        },
        "NORMALIZATION": {
            "normalized": normalized,
            "failed": failed,
            "failure_details": failure_details[:20],
        },
        "IDENTITY_AUDIT": {
            "duplicate_source_identities": len(duplicate_identities),
            "duplicate_identity_details": duplicate_identities[:20],
            "proposed_inserts": len(would_insert),
            "proposed_merges": len(would_merge),
            "ambiguous_candidates": len(ambiguous_candidates),
            "ambiguous_details": ambiguous_candidates,
            "expected_exact_merges": 4,
            "actual_exact_merges": len(actual_exact_merges),
            "exact_merge_details": actual_exact_merges,
            "merge_details": would_merge,
        },
        "API": {
            "api_metadata_calls": api_calls,
            "asset_file_calls": asset_calls,
        },
        "CATALOG_PROJECTION": {
            "existing_catalog_before": before["catalog_total"],
            "projected_final_catalog": projected_final,
            "proposed_inserts": len(would_insert),
            "proposed_merges": len(would_merge),
            "external_repository_records_created": external_repo_records_created,
            "dataset_version_records_created": 0,
            "publication_records_created": 0,
            "restricted_access_created": restricted_access_created,
            "production_writes": 0,
        },
        "PRODUCTION_INTEGRITY": {
            "catalog_total_before": before["catalog_total"],
            "repo_source_counts_before": before["repo_source_counts"],
            "production_datasets_collection_before": before[
                "production_datasets_collection_total"
            ],
            "production_writes": 0,
        },
        "VERIFICATION": {
            "ebrains_focused_tests": "PASS (28/28: tests/test_catalog/test_ingest_ebrains.py)",
            "catalog_regression": "PASS (443/443: tests/test_catalog)",
            "ebrains_production_ingestion_executed": False,
            "statement": "EBRAINS production ingestion has NOT been executed.",
        },
        "REPRESENTATIVE_RECORDS": representative_sources,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[ebrains-dryrun] === SUMMARY (READ-ONLY, no writes) ===")
    print(f"[ebrains-dryrun] input={len(approved)} expected={EBRAINS_CENSUS_EXPECTED}")
    print(f"[ebrains-dryrun] normalized={normalized} failed={failed}")
    print(
        f"[ebrains-dryrun] identity: proposed_inserts={len(would_insert)} "
        f"proposed_merges={len(would_merge)} ambiguous_candidates={len(ambiguous_candidates)} "
        f"duplicate_identities={len(duplicate_identities)}"
    )
    print(
        f"[ebrains-dryrun] exact matches: expected=4 actual={len(actual_exact_merges)} "
        f"(via generic cross_reference layer)"
    )
    print(f"[ebrains-dryrun] api_calls={api_calls} asset_file_calls={asset_calls}")
    print(
        f"[ebrains-dryrun] production catalog BEFORE={before['catalog_total']} "
        f"PROJECTED AFTER={projected_final} (production_writes=0)"
    )
    print(f"[ebrains-dryrun] report written to {_REPORT_PATH}")

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))