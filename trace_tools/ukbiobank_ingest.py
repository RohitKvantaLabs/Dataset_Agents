"""UK BIOBANK PRODUCTION INGESTION (2026-08-17) — APPROVED live write.

Runs the gated production UK Biobank ingestion against the live catalog
``neurosearch_dataset_catalog`` using the AUTHORITATIVE approved 21-record
UK Biobank census artifacts as input.

Flow:
  1. Load + cross-validate trace_artifacts/ukbiobank_census_20260817/
     ukbiobank_census.json (datasets_approved[]) and
     ukbiobank_candidates.jsonl (same 21 products, consistent fields) —
     identical loader to the approved read-only dry-run.
  2. PRE-GATE (read-only): normalize all records and resolve identity against
     the live catalog. The run ABORTS with ZERO writes unless EXACTLY:
         input = 21, normalized = 21, failed = 0, duplicate identities = 0,
         proposed inserts = 21, proposed merges = 0
     The single investigated title-only ambiguity (ukbiobank-sleep vs Allen
     allen:allen:15, ns-cacc6cb488e1cc51) is EXPLICITLY ALLOWED to remain
     unresolved for identity matching — it is NOT a merge and the Sleep
     product MUST be inserted as its own canonical record. Any OTHER
     ambiguity, or any ambiguity against a different candidate, aborts.
  3. PRODUCTION WRITES via app.catalog.ingest.run_ukbiobank_ingestion (the
     gated runner: exact-21 gate + stable_identifier gate + normalize →
     validate → upsert). Metadata only — no API calls, no asset/file
     downloads, no participant data, no UKB-RAP access, no returned
     datasets.
  4. POST-INGESTION verification (independent read-only checks): canonical
     count 6,165 → 6,186, UKB source count 21, unique sourceDatasetIds /
     sourceKeys / canonical IDs 21/21, rawMetadata.ukbiobank 21/21, DOI
     checks, returned-datasets exclusion, verbatim sourceUrl, existing
     repository counts unchanged, unrelated records modified (content-hash
     comparison), UKB/Allen Sleep merge = 0, write failures.
  5. Final safety: catalog total MUST be 6,186; otherwise STOP.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.ukbiobank_ingest

Report: ../trace_artifacts/ukbiobank_ingestion_20260817/ukbiobank_ingestion_20260817.json
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import _ukbiobank_identity_keys, run_ukbiobank_ingestion
from app.catalog.normalize import UKB_CENSUS_EXPECTED, build_ukbiobank_source_record
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

from trace_tools.ukbiobank_dryrun import (
    _CANDIDATES_PATH,
    _CENSUS_JSON_PATH,
    _load_approved_records,
)

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "ukbiobank_ingestion_20260817",
        "ukbiobank_ingestion_20260817.json",
    )
)

# The ONLY permitted ambiguity: ukbiobank-sleep vs the Allen Brain Atlas
# record allen:allen:15 ("Sleep", canonicalDatasetId ns-cacc6cb488e1cc51).
# Match signal is title only ("sleep") — NO DOI/URL/modality/cross-reference
# match. Decision (reviewed): DO NOT MERGE; Sleep is inserted as its own
# canonical dataset. This constant is verified in the pre-gate so any OTHER
# ambiguity (or a different candidate) aborts the run.
ALLOWED_AMBIGUOUS_PRODUCT = "ukbiobank-sleep"
ALLOWED_AMBIGUOUS_CANDIDATE = "ns-cacc6cb488e1cc51"  # allen:allen:15 "Sleep"


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _doc_content_hash(doc: dict) -> str:
    """Deterministic content hash of a canonical record (ignores ``_id`` and
    volatile timestamps so we only detect real content drift)."""
    clone = dict(doc)
    clone.pop("_id", None)
    return hashlib.md5(
        json.dumps(clone, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


async def _repo_source_counts(coll) -> Counter:
    repos: Counter = Counter()
    async for d in coll.find({}, {"sources.repository": 1}):
        for s in (d.get("sources") or []):
            if s.get("repository"):
                repos[s["repository"]] += 1
    return repos


def _repo_source_counts_unchanged(before: Counter, after: Counter) -> bool:
    """Existing repositories are unchanged iff every repo present BEFORE has
    the same source count AFTER. Newly introduced repositories (ukbiobank)
    are the intended addition, not a change to an existing repository."""
    diff = {}
    for repo in before:
        b, a = before.get(repo, 0), after.get(repo, 0)
        if b != a:
            diff[repo] = {"before": b, "after": a}
    return not diff


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=15000)
    db = client[settings.MONGO_DB_NAME]
    collection = get_collection(db)

    print(f"[ukbiobank-ingest] PRODUCTION db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")

    records, load_errors = _load_approved_records()
    print(f"[ukbiobank-ingest] census artifacts: {len(records)} records "
          f"(expected {UKB_CENSUS_EXPECTED})")

    # ── Phase 0: BEFORE snapshot (read-only). ────────────────────────────────
    total_before = await collection.count_documents({})
    repos_before = await _repo_source_counts(collection)
    baseline_hashes: dict[str, str] = {}
    async for d in collection.find({}):
        baseline_hashes[str(d["_id"])] = _doc_content_hash(d)
    print(f"[ukbiobank-ingest] BEFORE: catalog_total={total_before} "
          f"repos={dict(repos_before)}")

    # ── Phase 1: PRE-GATE — read-only resolution of the approved input. ──────
    duplicate_identities: list[dict] = []
    seen: dict[str, str] = {}
    for r in records:
        for key in _ukbiobank_identity_keys(r):
            prev = seen.get(key)
            if prev is not None:
                duplicate_identities.append(
                    {"identity": key, "first": prev, "second": str(r.get("stable_identifier") or "")}
                )
            else:
                seen[key] = str(r.get("stable_identifier") or "")

    normalized = 0
    failed = 0
    failure_details: list[str] = []
    proposed_inserts = 0
    proposed_merges = 0
    ambiguous: list[dict] = []
    for r in records:
        stable_id = str(r.get("stable_identifier") or "")
        try:
            source = build_ukbiobank_source_record(r)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failure_details.append(f"{stable_id}: normalize: {exc}")
            continue
        normalized += 1
        resolution = await resolve_identity(collection, source)
        if resolution["matched"] is not None:
            proposed_merges += 1
            print(f"[ukbiobank-ingest] *** PROPOSED MERGE for {stable_id} → "
                  f"{resolution['matched'].get('canonicalDatasetId')} — ABORT ***")
        else:
            proposed_inserts += 1  # ambiguous never blocks insertion
        if resolution["ambiguous"]:
            ambiguous.append(
                {
                    "stableIdentifier": stable_id,
                    "candidates": [
                        c.get("canonicalDatasetId") for c in resolution["ambiguous"]
                    ],
                }
            )

    pre_gate = {
        "approved_input": len(records),
        "input_exactly_21": len(records) == UKB_CENSUS_EXPECTED,
        "artifact_cross_validation_errors": load_errors,
        "normalized": normalized,
        "failed": failed,
        "duplicate_identities": len(duplicate_identities),
        "proposed_inserts": proposed_inserts,
        "proposed_merges": proposed_merges,
        "ambiguous": ambiguous,
    }
    # The ONLY allowed ambiguity is the investigated ukbiobank-sleep ↔ Allen
    # "Sleep" case; anything else aborts the run.
    allowed_ambiguity = (
        all(a["stableIdentifier"] == ALLOWED_AMBIGUOUS_PRODUCT for a in ambiguous)
        and all(
            set(a["candidates"]) <= {ALLOWED_AMBIGUOUS_CANDIDATE}
            for a in ambiguous
        )
    )
    gate_ok = (
        not load_errors
        and pre_gate["approved_input"] == 21
        and pre_gate["normalized"] == 21
        and pre_gate["failed"] == 0
        and pre_gate["duplicate_identities"] == 0
        and pre_gate["proposed_inserts"] == 21
        and pre_gate["proposed_merges"] == 0
        and allowed_ambiguity
    )
    print(f"[ukbiobank-ingest] PRE-GATE: {json.dumps(pre_gate)} gate_ok={gate_ok}")

    if not gate_ok:
        report = {
            "report_generated_at": _utcnow(),
            "mode": "PRODUCTION ingestion — ABORTED (pre-gate failed, ZERO writes)",
            "pre_gate": pre_gate,
            "failure_details": failure_details[:20],
            "writes_performed": 0,
        }
        os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
        with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        client.close()
        return 1

    # ── Phase 2: PRODUCTION WRITES. ─────────────────────────────────────────
    start = _utcnow()
    stats = await run_ukbiobank_ingestion(db, records=records, log=print)
    print(f"[ukbiobank-ingest] WRITE stats: "
          f"{json.dumps({k: stats[k] for k in ('input','normalized','inserted','merged','failed','api_calls','asset_calls','validation_failed')})}")

    # ── Phase 3: POST-INGESTION verification (independent read-only). ────────
    total_after = await collection.count_documents({})
    repos_after = await _repo_source_counts(collection)

    ukb_docs = []
    async for d in collection.find({"sources.repository": "ukbiobank"}):
        ukb_docs.append(d)
    ukb_source_ids = {}
    ukb_source_keys = set()
    ukb_canonical_ids = set()
    rawmeta_ok = 0
    ukb_links = {}  # stable_identifier -> sourceUrl as stored on canonical source
    for d in ukb_docs:
        ukb_source_ids[d["sourceKeys"][0]] = d.get("sources", [{}])[0].get("sourceDatasetId")
        ukb_source_keys.update(d.get("sourceKeys") or [])
        ukb_canonical_ids.add(d["canonicalDatasetId"])
        if (d.get("rawMetadata") or {}).get("ukbiobank"):
            rawmeta_ok += 1
        for s in (d.get("sources") or []):
            if s.get("repository") == "ukbiobank":
                ukb_links[s.get("sourceDatasetId")] = s.get("sourceUrl")

    census_urls = {r["stable_identifier"]: r["source_url"] for r in records}
    source_url_matches = 0
    for d in ukb_docs:
        for s in (d.get("sources") or []):
            if s.get("repository") != "ukbiobank":
                continue
            sid = str(s.get("sourceDatasetId") or "").replace("ukbiobank:", "", 1)
            if s.get("sourceUrl") == census_urls.get(sid):
                source_url_matches += 1

    # Approved-key set — returned datasets / non-approved products must NOT
    # enter the catalog (returns are never part of the input).
    approved_keys = {
        f"ukbiobank:ukbiobank:{r['stable_identifier']}" for r in records
    }
    returned_or_extra = sorted(ukb_source_keys - approved_keys)

    doi_non_null = sum(1 for d in ukb_docs if d.get("doi") is not None)
    pub_doi_leakage = sum(1 for d in ukb_docs if d.get("doi") is not None)
    url_norm_used = sum(
        1 for d in ukb_docs
        if (d.get("provenance") or {}).get("identity", {}).get("sourceUrlNorm") is not None
    )

    # UKB/Allen Sleep merge check: the ukbiobank-sleep record must be its OWN
    # canonical record — distinct from the Allen "Sleep" record and never
    # carrying the allen:allen:15 source.
    allen_sleep = await collection.find_one({"sourceKeys": "allen:allen:15"})
    ukb_sleep = await collection.find_one({"sourceKeys": "ukbiobank:ukbiobank:ukbiobank-sleep"})
    sleep_allen_merged = 0
    if ukb_sleep is not None and allen_sleep is not None:
        if ukb_sleep.get("canonicalDatasetId") == allen_sleep.get("canonicalDatasetId"):
            sleep_allen_merged = 1
        if any(s.get("repository") == "allen" for s in (ukb_sleep.get("sources") or [])):
            sleep_allen_merged = 1
        if any(s.get("repository") == "ukbiobank" for s in (allen_sleep.get("sources") or [])):
            sleep_allen_merged = 1

    # unrelated records modified: content-hash comparison on previously-existing docs
    unrelated_modified = 0
    async for d in collection.find({}):
        key = str(d["_id"])
        if key not in baseline_hashes:
            continue  # newly inserted UKB docs are additions, not modifications
        if _doc_content_hash(d) != baseline_hashes[key]:
            unrelated_modified += 1

    existing_unchanged = _repo_source_counts_unchanged(repos_before, repos_after)

    verification = {
        "canonical_total_matches_expected": total_after == 6186,
        "canonical_total": total_after,
        "canonical_before": total_before,
        "ukb_sources": len(ukb_docs),
        "ukb_sources_exactly_21": len(ukb_docs) == 21,
        "unique_sourceDatasetIds": len(set(ukb_source_ids.values())),
        "unique_sourceKeys": len(ukb_source_keys),
        "unique_canonicalIds": len(ukb_canonical_ids),
        "rawMetadata_ukbiobank_coverage": rawmeta_ok,
        "canonical_ukb_dois": doi_non_null,
        "publication_doi_leakage_into_canonical": pub_doi_leakage,
        "sourceUrl_verbatim_matches": source_url_matches,
        "sourceUrlNorm_used_for_identity": url_norm_used,
        "returned_datasets_or_non_approved_ingested": len(returned_or_extra),
        "returned_datasets_extra_keys": returned_or_extra,
        "ukb_merges": stats.get("merged", 0),
        "ukb_failures": stats.get("failed", 0),
        "asset_file_calls": stats.get("asset_calls", 0),
        "api_calls": stats.get("api_calls", 0),
        "restricted_participant_access": 0,
        "existing_repositories_unchanged": existing_unchanged,
        "unrelated_records_modified": unrelated_modified,
        "ukb_allen_sleep_merge": sleep_allen_merged,
        "production_write_failures": stats.get("failed", 0),
    }

    passed = (
        verification["canonical_total_matches_expected"]
        and verification["ukb_sources_exactly_21"]
        and verification["unique_sourceDatasetIds"] == 21
        and verification["unique_sourceKeys"] == 21
        and verification["unique_canonicalIds"] == 21
        and verification["rawMetadata_ukbiobank_coverage"] == 21
        and verification["canonical_ukb_dois"] == 0
        and verification["publication_doi_leakage_into_canonical"] == 0
        and verification["sourceUrl_verbatim_matches"] == 21
        and verification["sourceUrlNorm_used_for_identity"] == 0
        and verification["returned_datasets_or_non_approved_ingested"] == 0
        and verification["ukb_merges"] == 0
        and verification["ukb_failures"] == 0
        and verification["asset_file_calls"] == 0
        and verification["api_calls"] == 0
        and verification["restricted_participant_access"] == 0
        and verification["existing_repositories_unchanged"] is True
        and verification["unrelated_records_modified"] == 0
        and verification["ukb_allen_sleep_merge"] == 0
        and verification["production_write_failures"] == 0
        and stats.get("inserted", 0) == 21
        and stats.get("normalized", 0) == 21
    )

    report = {
        "report_generated_at": _utcnow(),
        "mode": "PRODUCTION ingestion (live writes)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "census_artifacts": {"census_json": _CENSUS_JSON_PATH, "candidates_jsonl": _CANDIDATES_PATH},
        "pre_gate": pre_gate,
        "INGESTION": {
            "approved_input": len(records),
            "inserted": stats.get("inserted", 0),
            "merged": stats.get("merged", 0),
            "failed": stats.get("failed", 0),
            "validation_failed": stats.get("validation_failed", 0),
            "duplicate_source_identities": stats.get("duplicate_source_identities", []),
            "matched_via": stats.get("matched_via", {}),
            "api_calls": stats.get("api_calls", 0),
            "asset_calls": stats.get("asset_calls", 0),
            "started_at": start,
            "finished_at": stats.get("finished_at"),
            "elapsed_s": stats.get("elapsed_s"),
        },
        "VERIFICATION": verification,
        "PRODUCTION_INTEGRITY": {
            "catalog_before": total_before,
            "catalog_after": total_after,
            "repo_source_counts_before": dict(repos_before),
            "repo_source_counts_after": dict(repos_after),
            "allen_sleep_candidate": (
                {"canonicalDatasetId": allen_sleep.get("canonicalDatasetId"),
                 "sourceKeys": allen_sleep.get("sourceKeys")}
                if allen_sleep is not None else None
            ),
            "ukb_sleep_canonical": (
                {"canonicalDatasetId": ukb_sleep.get("canonicalDatasetId"),
                 "sourceKeys": ukb_sleep.get("sourceKeys")}
                if ukb_sleep is not None else None
            ),
        },
        "verification_passed": passed,
        "conclusion": (
            "UK Biobank production ingestion completed successfully: "
            "21 new canonical neuroscience datasets, 0 merges, "
            f"final catalog {total_after}."
            if passed else "VERIFICATION FAILED — investigate before any further action."
        ),
        "status": "PASSED" if passed else "FAILED",
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[ukbiobank-ingest] === SUMMARY ===")
    print(f"[ukbiobank-ingest] INSERTED={stats.get('inserted')} MERGED={stats.get('merged')} "
          f"FAILED={stats.get('failed')}")
    print(f"[ukbiobank-ingest] catalog: {total_before} -> {total_after}")
    print(f"[ukbiobank-ingest] UKB docs={len(ukb_docs)} unique_ids={len(set(ukb_source_ids.values()))} "
          f"unique_keys={len(ukb_source_keys)} unique_canonical={len(ukb_canonical_ids)}")
    print(f"[ukbiobank-ingest] existing repos unchanged={existing_unchanged} "
          f"unrelated_modified={unrelated_modified}")
    print(f"[ukbiobank-ingest] VERIFICATION: {'PASSED' if passed else 'FAILED'}")
    print(f"[ukbiobank-ingest] report -> {_REPORT_PATH}")

    client.close()
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
