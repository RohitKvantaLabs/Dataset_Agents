"""Dryad read-only dry-run audit (2026-08-17).

Runs the Dryad ingestion pipeline in READ-ONLY mode against the production
``neurosearch_dataset_catalog`` collection, using the AUTHORITATIVE census
artifact as input:

  1. Load trace_artifacts/dryad_census_20260817/dryad_candidates.jsonl.
  2. Hard gate: ONLY HIGH-confidence (classification == "H") records may be
     processed; MEDIUM / LOW / FALSE-POSITIVE records are refused.
  3. Normalize every HIGH-confidence record via build_dryad_source_record().
  4. Resolve identity against the existing catalog (read-only) and record
     would-insert / would-merge / unresolved per record.
  5. Report — input / normalized / failures / duplicate source identities /
     proposed inserts / proposed merges / unresolved / API calls / asset
     calls, plus a production snapshot (BEFORE only — zero writes).

NO WRITES are performed. The production catalog must remain unchanged.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.dryad_dryrun

Report: ../trace_artifacts/dryad_ingestion_20260817/dryad_dryrun_20260817.json
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import _dryad_identity_keys
from app.catalog.normalize import (
    DRYAD_HIGH_CONFIDENCE,
    build_dryad_source_record,
)
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "dryad_ingestion_20260817",
        "dryad_dryrun_20260817.json",
    )
)

_CANDIDATES_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "dryad_census_20260817",
        "dryad_candidates.jsonl",
    )
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_candidates(path: str) -> list[dict]:
    """Load the census artifact (one JSON object per line)."""
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


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

    print(f"[dryad-dryrun] production db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")
    before = await _catalog_snapshot(db, datasets_coll)
    print(f"[dryad-dryrun] production BEFORE: {json.dumps(before)}")

    # ── 1) Load the authoritative census artifact and select the validated
    #    HIGH-confidence set. The artifact legitimately contains M/L/F records
    #    (they were classified but excluded from ingestion); the INGESTION
    #    INPUT is exactly the HIGH-confidence subset — nothing else may be
    #    processed.
    candidates = _load_candidates(_CANDIDATES_PATH)
    high = [
        c for c in candidates
        if str(c.get("classification") or "").strip().upper() == DRYAD_HIGH_CONFIDENCE
    ]
    excluded = [
        c for c in candidates
        if str(c.get("classification") or "").strip().upper() != DRYAD_HIGH_CONFIDENCE
    ]
    print(
        f"[dryad-dryrun] census artifact: {len(candidates)} total, "
        f"{len(high)} HIGH-confidence, {len(excluded)} excluded (M/L/F)"
    )

    # ── 2) HIGH-confidence gate — the ingestion input must be exactly the
    #    validated set; a missing/empty HIGH set stops before anything runs.
    if not high:
        print(
            f"[dryad-dryrun] *** GATE FAILED: zero HIGH-confidence records — "
            f"stopping (no writes) ***"
        )
        report = {
            "report_generated_at": _utcnow(),
            "mode": "READ-ONLY dry-run audit (zero writes)",
            "gate_failed": True,
            "gate_reason": "zero HIGH-confidence records in the census artifact",
            "input_total": len(candidates),
            "high_confidence": 0,
            "excluded": len(excluded),
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
    for c in high:
        for key in _dryad_identity_keys(c):
            prev = seen.get(key)
            if prev is not None:
                duplicate_identities.append(
                    {"identity": key, "first": prev, "second": c.get("identifier")}
                )
            else:
                seen[key] = c.get("identifier")

    # ── 4) Normalize → resolve identity (READ-ONLY). ─────────────────────────
    normalized = 0
    failed = 0
    failure_details: list[str] = []
    would_insert: list[dict] = []
    would_merge: list[dict] = []
    unresolved: list[dict] = []
    representative_sources: list[dict] = []
    api_calls = 0      # census artifact is authoritative — no API calls
    asset_calls = 0    # hard guarantee

    for c in high:
        try:
            source = build_dryad_source_record(c)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failure_details.append(
                f"{c.get('identifier')}: normalize: {type(exc).__name__}: {exc}"
            )
            print(f"[dryad-dryrun] normalize failed for {c.get('identifier')}: {exc}")
            continue
        normalized += 1
        if len(representative_sources) < 5:
            representative_sources.append(
                {
                    "identifier": c.get("identifier"),
                    "title": source.get("title"),
                    "sourceDatasetId": source.get("sourceDatasetId"),
                    "sourceUrl": source.get("sourceUrl"),
                    "doi": source.get("doi"),
                    "versionNumber": (source.get("snapshot") or {}).get("versionNumber"),
                    "articleDoi": (source.get("publication") or {}).get("articleDoi"),
                    "rawMetadataDryadKeys": sorted(source.get("rawMetadata") or {}),
                }
            )

        resolution = await resolve_identity(collection, source)
        if resolution["matched"] is not None:
            would_merge.append(
                {
                    "identifier": c.get("identifier"),
                    "existingCanonicalDatasetId": resolution["matched"].get(
                        "canonicalDatasetId"
                    ),
                    "matchedVia": resolution.get("matchedVia"),
                    "existingRepositories": sorted(
                        {s.get("repository")
                         for s in (resolution["matched"].get("sources") or [])}
                    ),
                }
            )
        elif resolution["ambiguous"]:
            unresolved.append(
                {"identifier": c.get("identifier"), "title": source.get("title")}
            )
        else:
            would_insert.append(
                {"identifier": c.get("identifier"), "title": source.get("title")}
            )

    # ── 5) Report (read-only; production untouched). ─────────────────────────
    report = {
        "report_generated_at": _utcnow(),
        "mode": "READ-ONLY dry-run audit (zero writes)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "INPUT": {
            "census_artifact": _CANDIDATES_PATH,
            "input_total": len(candidates),
            "high_confidence": len(high),
            "excluded_non_high": len(excluded),
            "excluded_classifications": sorted(
                {c.get("classification") for c in excluded}
            ),
            "gate_failed": bool(excluded),
        },
        "NORMALIZATION": {
            "normalized": normalized,
            "failed": failed,
            "failure_details": failure_details[:20],
        },
        "IDENTITY_AUDIT": {
            "duplicate_source_identities": len(duplicate_identities),
            "duplicate_identity_details": duplicate_identities[:20],
            "would_insert": len(would_insert),
            "would_merge": len(would_merge),
            "unresolved": len(unresolved),
            "merge_details": would_merge,
            "unresolved_details": unresolved[:20],
        },
        "API": {
            "api_calls": api_calls,
            "asset_file_calls": asset_calls,
        },
        "PRODUCTION_INTEGRITY": {
            "catalog_total_before": before["catalog_total"],
            "repo_source_counts_before": before["repo_source_counts"],
            "production_datasets_collection_before": before[
                "production_datasets_collection_total"
            ],
            "writes_performed": 0,
        },
        "REPRESENTATIVE_RECORDS": representative_sources,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[dryad-dryrun] === SUMMARY (READ-ONLY, no writes) ===")
    print(f"[dryad-dryrun] input={len(candidates)} high={len(high)} excluded={len(excluded)}")
    print(f"[dryad-dryrun] normalized={normalized} failed={failed}")
    print(
        f"[dryad-dryrun] identity: would_insert={len(would_insert)} "
        f"would_merge={len(would_merge)} unresolved={len(unresolved)} "
        f"duplicate_identities={len(duplicate_identities)}"
    )
    print(f"[dryad-dryrun] api_calls={api_calls} asset_file_calls={asset_calls}")
    print(f"[dryad-dryrun] production catalog BEFORE: {before['catalog_total']} "
          f"(writes_performed=0)")
    print(f"[dryad-dryrun] report written to {_REPORT_PATH}")

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
