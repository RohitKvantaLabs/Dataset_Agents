"""ADNI read-only dry-run audit (2026-08-17).

Runs the ADNI ingestion pipeline in READ-ONLY mode against the production
``neurosearch_dataset_catalog`` collection, using the AUTHORITATIVE approved
ADNI census artifact as input:

  1. Load trace_artifacts/adni_census_20260817/adni_census.json → ``datasets[]``.
  2. Hard gate: the ingestion input must be EXACTLY 122 records and every
     record must carry a stable_identifier; otherwise the run stops with zero
     writes.
  3. Normalize every record via build_adni_source_record().
  4. Resolve identity against the existing catalog (read-only) and record
     would-insert / would-merge / unresolved per record.
  5. Report — input / normalized / failed / duplicate source identities /
     proposed inserts / proposed merges / unresolved / API/metadata calls /
     asset-file calls / production writes, plus a production snapshot
     (BEFORE only — zero writes).

NO WRITES are performed. The production catalog must remain unchanged.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.adni_dryrun

Report: ../trace_artifacts/adni_ingestion_20260817/adni_dryrun_20260817.json
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import _adni_identity_keys
from app.catalog.normalize import (
    ADNI_CENSUS_EXPECTED,
    build_adni_source_record,
)
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "adni_ingestion_20260817",
        "adni_dryrun_20260817.json",
    )
)

_CENSUS_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "adni_census_20260817",
        "adni_census.json",
    )
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_census(path: str) -> list[dict]:
    """Load the approved ADNI census artifact (``datasets[]``)."""
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return list(payload.get("datasets") or [])


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

    print(f"[adni-dryrun] production db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")
    before = await _catalog_snapshot(db, datasets_coll)
    print(f"[adni-dryrun] production BEFORE: {json.dumps(before)}")

    # ── 1) Load the authoritative approved ADNI census artifact. ─────────────
    records = _load_census(_CENSUS_PATH)
    print(
        f"[adni-dryrun] census artifact: {len(records)} approved records "
        f"(expected exactly {ADNI_CENSUS_EXPECTED})"
    )

    # ── 2) Exact-count gate + identity-completeness gate. ────────────────────
    gate_reason = None
    if len(records) != ADNI_CENSUS_EXPECTED:
        gate_reason = (
            f"input has {len(records)} records; the approved ADNI census set "
            f"is exactly {ADNI_CENSUS_EXPECTED}"
        )
    missing_identity = [
        r.get("dataset_name")
        for r in records
        if not str(r.get("stable_identifier") or "").strip()
    ]
    if missing_identity:
        gate_reason = (
            f"{len(missing_identity)} records lack a stable_identifier "
            f"(e.g. {missing_identity[0]})"
        )
    if gate_reason:
        print(f"[adni-dryrun] *** GATE FAILED: {gate_reason} — stopping (no writes) ***")
        report = {
            "report_generated_at": _utcnow(),
            "mode": "READ-ONLY dry-run audit (zero writes)",
            "gate_failed": True,
            "gate_reason": gate_reason,
            "input_total": len(records),
            "expected": ADNI_CENSUS_EXPECTED,
            "writes_performed": 0,
        }
        os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
        with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        client.close()
        return 1

    # ── 3) Duplicate candidate protection (read-only detection). ─────────────
    # The census artifact is deduplicated, so ANY repeated identity key is a
    # genuine duplicate.
    duplicate_identities: list[dict] = []
    seen: dict[str, str] = {}
    for r in records:
        for key in _adni_identity_keys(r):
            prev = seen.get(key)
            if prev is not None:
                duplicate_identities.append(
                    {
                        "identity": key,
                        "first": prev,
                        "second": str(r.get("stable_identifier") or ""),
                    }
                )
            else:
                seen[key] = str(r.get("stable_identifier") or "")

    # ── 4) Normalize → resolve identity (READ-ONLY). ─────────────────────────
    # ADNI sourceUrl is the census URL verbatim; identity flows through the
    # deterministic repo:sourceDatasetId/sourceKey (URL layer skipped for
    # ADNI — see SHARED_SOURCE_URL_REPOSITORIES in normalize.py).
    normalized = 0
    failed = 0
    failure_details: list[str] = []
    would_insert: list[dict] = []
    would_merge: list[dict] = []
    unresolved: list[dict] = []
    representative_sources: list[dict] = []
    api_calls = 0      # census artifact is authoritative — no API calls
    asset_calls = 0    # hard guarantee

    for r in records:
        stable_id = str(r.get("stable_identifier") or "").strip()
        try:
            source = build_adni_source_record(r)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failure_details.append(
                f"{stable_id}: normalize: {type(exc).__name__}: {exc}"
            )
            print(f"[adni-dryrun] normalize failed for {stable_id}: {exc}")
            continue
        normalized += 1
        if len(representative_sources) < 5:
            representative_sources.append(
                {
                    "stableIdentifier": stable_id,
                    "title": source.get("title"),
                    "sourceDatasetId": source.get("sourceDatasetId"),
                    "sourceUrl": source.get("sourceUrl"),
                    "doi": source.get("doi"),
                    "modality": source.get("modality"),
                    "documentationUrl": source.get("documentationUrl"),
                    "articleDoi": (source.get("publication") or {}).get("articleDoi"),
                }
            )

        resolution = await resolve_identity(collection, source)
        if resolution["matched"] is not None:
            would_merge.append(
                {
                    "stableIdentifier": stable_id,
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
        elif resolution["ambiguous"]:
            unresolved.append(
                {
                    "stableIdentifier": stable_id,
                    "title": source.get("title"),
                    "candidateCount": len(resolution["ambiguous"]),
                }
            )
        else:
            would_insert.append(
                {
                    "stableIdentifier": stable_id,
                    "title": source.get("title"),
                }
            )

    # ── 5) Report (read-only; production untouched). ─────────────────────────
    report = {
        "report_generated_at": _utcnow(),
        "mode": "READ-ONLY dry-run audit (zero writes)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "INPUT": {
            "census_artifact": _CENSUS_PATH,
            "input_total": len(records),
            "expected": ADNI_CENSUS_EXPECTED,
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
            "unresolved": len(unresolved),
            "merge_details": would_merge,
            "unresolved_details": unresolved[:20],
        },
        "API": {
            "api_metadata_calls": api_calls,
            "asset_file_calls": asset_calls,
        },
        "PRODUCTION_INTEGRITY": {
            "catalog_total_before": before["catalog_total"],
            "repo_source_counts_before": before["repo_source_counts"],
            "production_datasets_collection_before": before[
                "production_datasets_collection_total"
            ],
            "production_writes": 0,
        },
        "REPRESENTATIVE_RECORDS": representative_sources,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[adni-dryrun] === SUMMARY (READ-ONLY, no writes) ===")
    print(f"[adni-dryrun] input={len(records)} expected={ADNI_CENSUS_EXPECTED}")
    print(f"[adni-dryrun] normalized={normalized} failed={failed}")
    print(
        f"[adni-dryrun] identity: proposed_inserts={len(would_insert)} "
        f"proposed_merges={len(would_merge)} unresolved={len(unresolved)} "
        f"duplicate_identities={len(duplicate_identities)}"
    )
    print(f"[adni-dryrun] api_calls={api_calls} asset_file_calls={asset_calls}")
    print(f"[adni-dryrun] production catalog BEFORE: {before['catalog_total']} "
          f"(production_writes=0)")
    print(f"[adni-dryrun] report written to {_REPORT_PATH}")

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
