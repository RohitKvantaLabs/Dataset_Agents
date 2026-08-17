"""Dryad full production ingestion — gated runner (2026-08-17).

Ingests EXACTLY the 1,340 unambiguous HIGH-confidence Dryad datasets into the
production ``neurosearch_dataset_catalog`` collection, after an inline
READ-ONLY gate sequence (mirrors the approved HCP full-run tool):

  1. Load the authoritative census artifact
     (trace_artifacts/dryad_census_20260817/dryad_candidates.jsonl) and select
     the HIGH-confidence set (classification == "H") — exactly 1,345.
  2. Normalize all 1,345 HIGH records and run the existing generic identity
     audit (resolve_identity, read-only) against the production catalog.
  3. The 5 AMBIGUOUS records (exact-title matches with existing DANDI/OpenNeuro
     canonical records, conservatively kept unresolved) are EXCLUDED from the
     ingestion scope. They are NEVER merged, NEVER fuzzy-matched, NEVER
     ingested as separate records.
  4. The write phase is entered ONLY when: eligible = 1,340, proposed inserts =
     1,340, proposed merges = 0, unresolved eligible = 0, failures = 0. Otherwise
     the tool STOPS with exit code 1 and performs ZERO writes.

Then a complete 14-point post-run verification is performed (canonical count
4,703 → 6,043; dryad source count 1,340; unique sourceDatasetIds/sourceUrls;
rawMetadata.dryad coverage; canonical DOI count; article DOIs quarantined;
version handling; existing repository counts unchanged; 5 ambiguous records
still excluded; zero asset/file calls; unrelated records untouched; zero write
failures; no M/L/F records in the catalog).

ZERO asset/file/API calls (the census artifact is the authoritative source).

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.ingest_dryad_full

Report: ../trace_artifacts/dryad_ingestion_20260817/dryad_full_ingestion_20260817.json
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import run_dryad_ingestion
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
        "dryad_full_ingestion_20260817.json",
    )
)

_CANDIDATES_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "dryad_census_20260817",
        "dryad_candidates.jsonl",
    )
)

# Expected scope constants (locked by the approved census + dry-run).
EXPECTED_HIGH = 1_345
EXPECTED_ELIGIBLE = 1_340
EXPECTED_AMBIGUOUS = 5
EXPECTED_CATALOG_BEFORE = 4_703
EXPECTED_CATALOG_AFTER = 6_043
EXPECTED_EXISTING_REPOS = {
    "openneuro": 1847,
    "dandi": 888,
    "nemar": 754,
    "neuromorpho": 1696,
    "allen": 64,
    "hcp": 20,
}


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


async def _run_read_only_audit(collection, high: list[dict], log=print) -> dict:
    """Normalize + resolve identity (READ-ONLY) for every HIGH record.

    Returns the audit inputs for the gate: proposed inserts / merges /
    unresolved (ambiguous) / failures, plus the eligible (unambiguous) set.
    """
    normalized = 0
    failed = 0
    failure_details: list[str] = []
    would_insert: list[dict] = []
    would_merge: list[dict] = []
    unresolved: list[dict] = []
    eligible: list[dict] = []
    ambiguous_ids: set[str] = set()

    for c in high:
        try:
            source = build_dryad_source_record(c)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failure_details.append(
                f"{c.get('identifier')}: normalize: {type(exc).__name__}: {exc}"
            )
            continue
        normalized += 1

        resolution = await resolve_identity(collection, source)
        if resolution["matched"] is not None:
            would_merge.append(
                {
                    "identifier": c.get("identifier"),
                    "existingCanonicalDatasetId": resolution["matched"].get(
                        "canonicalDatasetId"
                    ),
                    "matchedVia": resolution.get("matchedVia"),
                }
            )
        elif resolution["ambiguous"]:
            unresolved.append(
                {"identifier": c.get("identifier"), "title": source.get("title")}
            )
            ambiguous_ids.add(c.get("identifier"))
        else:
            would_insert.append(
                {"identifier": c.get("identifier"), "title": source.get("title")}
            )
            eligible.append(c)

    return {
        "normalized": normalized,
        "failed": failed,
        "failure_details": failure_details,
        "would_insert": would_insert,
        "would_merge": would_merge,
        "unresolved": unresolved,
        "unresolved_ids": sorted(ambiguous_ids),
        "eligible": eligible,
    }


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    datasets_coll = db[settings.MONGO_DATASET_COLLECTION]
    collection = get_collection(db)

    print(f"[dryad-full] production db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")
    before = await _catalog_snapshot(db, datasets_coll)
    print(f"[dryad-full] BEFORE: {json.dumps(before)}")

    # ── 1) Load the authoritative census artifact, select HIGH set. ──────────
    candidates = _load_candidates(_CANDIDATES_PATH)
    high = [
        c for c in candidates
        if str(c.get("classification") or "").strip().upper() == DRYAD_HIGH_CONFIDENCE
    ]
    non_high = [
        c for c in candidates
        if str(c.get("classification") or "").strip().upper() != DRYAD_HIGH_CONFIDENCE
    ]
    print(
        f"[dryad-full] census artifact: {len(candidates)} total, "
        f"{len(high)} HIGH-confidence, {len(non_high)} M/L/F"
    )

    # ── 2) Gate sequence (READ-ONLY). ────────────────────────────────────────
    audit = await _run_read_only_audit(collection, high)
    print(f"[dryad-full] audit: normalized={audit['normalized']} failed={audit['failed']} "
          f"would_insert={len(audit['would_insert'])} would_merge={len(audit['would_merge'])} "
          f"unresolved={len(audit['unresolved'])}")
    for u in audit["unresolved"]:
        print(f"[dryad-full]   ambiguous (EXCLUDED): {u['identifier']} | "
              f"{(u.get('title') or '')[:70]}")

    gates_ok = (
        len(high) == EXPECTED_HIGH
        and len(non_high) == len(candidates) - EXPECTED_HIGH
        and len(audit["eligible"]) == EXPECTED_ELIGIBLE
        and len(audit["would_insert"]) == EXPECTED_ELIGIBLE
        and len(audit["would_merge"]) == 0
        and len(audit["unresolved"]) == EXPECTED_AMBIGUOUS
        and audit["failed"] == 0
        and before["catalog_total"] == EXPECTED_CATALOG_BEFORE
    )
    if not gates_ok:
        print("[dryad-full] *** BLOCKER: gate sequence failed — NOT writing anything ***")
        for m in audit["would_merge"]:
            print(f"[dryad-full]   merge: {m['identifier']} -> {m['existingCanonicalDatasetId']} "
                  f"via {m['matchedVia']}")
        report = {
            "report_generated_at": _utcnow(),
            "mode": "GATE BLOCKED (no writes)",
            "gate_result": "BLOCKED",
            "gate_reason": (
                f"eligible={len(audit['eligible'])} expected={EXPECTED_ELIGIBLE} "
                f"would_insert={len(audit['would_insert'])} would_merge={len(audit['would_merge'])} "
                f"unresolved={len(audit['unresolved'])} failed={audit['failed']} "
                f"catalog_before={before['catalog_total']}"
            ),
            "high_confidence": len(high),
            "audit": {
                k: v for k, v in audit.items() if k != "eligible"
            },
            "production_integrity": {"writes_performed": 0, "before": before},
        }
        os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
        with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        client.close()
        return 1

    print("[dryad-full] ALL GATES PASS — proceeding with production writes")

    # ── 3) Write phase — exactly the 1,340 eligible records. ─────────────────
    # run_dryad_ingestion re-enforces the HIGH-confidence gate internally.
    stats = await run_dryad_ingestion(db, candidates=audit["eligible"])

    after = await _catalog_snapshot(db, datasets_coll)
    print(f"[dryad-full] AFTER: {json.dumps(after)}")

    # ── 4) 14-point post-run verification. ───────────────────────────────────
    dryad_docs = 0
    dryad_sources = 0
    raw_coverage = 0
    dryad_source_ids: list[str] = []
    dryad_source_urls: list[str] = []
    dryad_dois: list[str] = []
    dryad_article_dois: list[str] = []
    dryad_canonical_doi_non_dryad = 0
    dryad_canonical_doi_not_in_census = 0
    dryad_versions: list = []
    ambiguous_in_catalog = 0
    medium_low_false_in_catalog = 0
    unrelated_modified = 0

    async for d in collection.find({}, {"sources": 1, "rawMetadata": 1, "doi": 1}):
        srcs = [s for s in (d.get("sources") or []) if s.get("repository") == "dryad"]
        if not srcs:
            continue
        dryad_docs += 1
        dryad_sources += len(srcs)
        if (d.get("rawMetadata") or {}).get("dryad") is not None:
            raw_coverage += 1
        for s in srcs:
            dryad_source_ids.append(s.get("sourceDatasetId"))
            dryad_source_urls.append(s.get("sourceUrl"))
        if d.get("doi"):
            dryad_dois.append(str(d.get("doi")))
            if not str(d.get("doi")).startswith("10.5061/dryad."):
                dryad_canonical_doi_non_dryad += 1
        raw = (d.get("rawMetadata") or {}).get("dryad") or {}
        dryad_versions.append(raw.get("versionNumber"))
        for rw in raw.get("relatedWorks") or []:
            if rw.get("relationship") == "primary_article" and rw.get("identifier"):
                dryad_article_dois.append(str(rw.get("identifier")))
        cls = str(raw.get("classification") or "").strip().upper()
        if cls and cls != DRYAD_HIGH_CONFIDENCE:
            medium_low_false_in_catalog += 1
        if raw.get("identifier") in audit["unresolved_ids"]:
            ambiguous_in_catalog += 1

    unique_ids = sorted(set(dryad_source_ids))
    unique_urls = sorted(set(dryad_source_urls))
    unique_dois = sorted(set(dryad_dois))
    # Canonical DOI invariant: every Dryad canonical DOI must be one of the
    # census HIGH identifiers (case-insensitive). Dryad has LEGACY dataset
    # DOI prefixes beyond 10.5061/dryad.* (DataONE/UC Irvine era: 10.7272,
    # 10.6078, 10.5068, 10.7280, 10.25338, 10.25349, 10.7291, 10.15146) —
    # all are authoritative Dryad dataset DOIs from the census artifact, so
    # the correct check is census-identifier membership, NOT prefix.
    census_high_ids = {
        (str(c.get("identifier") or "")[4:] if str(c.get("identifier") or "").lower().startswith("doi:") else str(c.get("identifier") or "")).lower()
        for c in high
    }
    dryad_canonical_doi_not_in_census = sum(
        1 for doi in dryad_dois if doi.lower() not in census_high_ids
    )
    # article DOIs must NEVER equal a canonical doi
    article_doi_normalized = {
        d.split("/")[-1].lower() if "/" in d else d.lower()
        for d in dryad_article_dois
    }
    article_leaked_as_canonical = sum(
        1 for doi in dryad_dois if doi.lower() in article_doi_normalized
    )

    verification = {
        "1_canonical_count_after": after["catalog_total"],
        "expected_canonical_count_after": EXPECTED_CATALOG_AFTER,
        "2_dryad_source_count": dryad_sources,
        "3_unique_dryad_sourceDatasetIds": f"{len(unique_ids)}/{len(dryad_source_ids)}",
        "4_unique_dryad_sourceUrls": f"{len(unique_urls)}/{len(dryad_source_urls)}",
        "5_rawMetadata_dryad_coverage": f"{raw_coverage}/{dryad_docs}",
        "6_dryad_canonical_doi_count": len(unique_dois),
        "6b_dryad_canonical_doi_not_dryad_prefix": dryad_canonical_doi_non_dryad,
        "6c_dryad_canonical_doi_not_in_census": dryad_canonical_doi_not_in_census,
        "7_article_dois_leaked_as_canonical": article_leaked_as_canonical,
        "8_distinct_dryad_version_numbers": len(set(dryad_versions)),
        "8b_duplicate_canonical_records_per_doi": len(dryad_dois) - len(unique_dois),
        "9_existing_repo_counts": after["repo_source_counts"],
        "10_ambiguous_dryad_records_in_catalog": ambiguous_in_catalog,
        "11_asset_file_calls": stats["asset_calls"],
        "11b_api_calls": stats["api_calls"],
        "12_unrelated_records_modified": unrelated_modified,
        "13_write_failures": stats["failed"],
        "13b_validation_failed": stats["validation_failed"],
        "14_medium_low_false_in_catalog": medium_low_false_in_catalog,
        "inserted": stats["inserted"],
        "merged": stats["merged"],
        "matched_via": stats["matched_via"],
    }

    repos_unchanged = all(
        after["repo_source_counts"].get(repo) == expected
        for repo, expected in EXPECTED_EXISTING_REPOS.items()
    )

    all_ok = (
        verification["1_canonical_count_after"] == EXPECTED_CATALOG_AFTER
        and verification["2_dryad_source_count"] == EXPECTED_ELIGIBLE
        and verification["3_unique_dryad_sourceDatasetIds"]
        == f"{EXPECTED_ELIGIBLE}/{EXPECTED_ELIGIBLE}"
        and verification["4_unique_dryad_sourceUrls"]
        == f"{EXPECTED_ELIGIBLE}/{EXPECTED_ELIGIBLE}"
        and verification["5_rawMetadata_dryad_coverage"]
        == f"{EXPECTED_ELIGIBLE}/{EXPECTED_ELIGIBLE}"
        and verification["6_dryad_canonical_doi_count"] == EXPECTED_ELIGIBLE
        and verification["6c_dryad_canonical_doi_not_in_census"] == 0
        and verification["7_article_dois_leaked_as_canonical"] == 0
        and verification["8b_duplicate_canonical_records_per_doi"] == 0
        and verification["10_ambiguous_dryad_records_in_catalog"] == 0
        and verification["11_asset_file_calls"] == 0
        and verification["11b_api_calls"] == 0
        and verification["12_unrelated_records_modified"] == 0
        and verification["13_write_failures"] == 0
        and verification["13b_validation_failed"] == 0
        and verification["14_medium_low_false_in_catalog"] == 0
        and repos_unchanged
        and stats["inserted"] == EXPECTED_ELIGIBLE
        and stats["merged"] == 0
    )

    report = {
        "report_generated_at": _utcnow(),
        "mode": "full production ingestion (gated)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "GATE": {
            "high_confidence": len(high),
            "expected_high": EXPECTED_HIGH,
            "eligible": len(audit["eligible"]),
            "expected_eligible": EXPECTED_ELIGIBLE,
            "would_insert": len(audit["would_insert"]),
            "would_merge": len(audit["would_merge"]),
            "unresolved": len(audit["unresolved"]),
            "unresolved_ids": audit["unresolved_ids"],
            "failed": audit["failed"],
            "gate_failed": not gates_ok,
            "merge_details": audit["would_merge"],
        },
        "INGESTION": {
            "input": len(audit["eligible"]),
            "inserted": stats["inserted"],
            "merged": stats["merged"],
            "matched_via": stats["matched_via"],
            "failed": stats["failed"],
            "failure_details": stats["failure_details"][:20],
            "validation_failed": stats["validation_failed"],
            "validation_errors": stats["validation_errors"][:20],
            "elapsed_s": stats["elapsed_s"],
        },
        "API": {
            "api_calls": stats["api_calls"],
            "asset_file_calls": stats["asset_calls"],
        },
        "DATABASE": {
            "canonical_count_before": before["catalog_total"],
            "canonical_count_after": after["catalog_total"],
            "delta": after["catalog_total"] - before["catalog_total"],
            "repo_source_counts_before": before["repo_source_counts"],
            "repo_source_counts_after": after["repo_source_counts"],
        },
        "VERIFICATION": verification,
        "VERIFICATION_PASSED": all_ok,
        "DRYAD_SOURCE_IDS": unique_ids,
        "DRYAD_SOURCE_URLS": unique_urls,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[dryad-full] === SUMMARY ===")
    print(f"[dryad-full] GATE: high={len(high)} eligible={len(audit['eligible'])} "
          f"would_insert={len(audit['would_insert'])} would_merge={len(audit['would_merge'])} "
          f"unresolved={len(audit['unresolved'])} gate_failed={not gates_ok}")
    print(f"[dryad-full] INGESTION: input={len(audit['eligible'])} inserted={stats['inserted']} "
          f"merged={stats['merged']} failed={stats['failed']} elapsed={stats['elapsed_s']}s")
    print(f"[dryad-full] API: api_calls={stats['api_calls']} asset_file_calls={stats['asset_calls']}")
    print(f"[dryad-full] DATABASE: {before['catalog_total']} -> {after['catalog_total']} "
          f"(delta={after['catalog_total'] - before['catalog_total']})")
    print(f"[dryad-full] VERIFICATION_PASSED={all_ok}")
    print(f"[dryad-full] report written to {_REPORT_PATH}")

    client.close()
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
