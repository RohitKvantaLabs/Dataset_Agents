"""Isolated Allen Phase-1 smoke test (2026-08-16).

Runs a SMALL live Allen ingestion (5 representative Products across
categories/species) against a DEDICATED isolated test database
(``<MONGO_DB_NAME>_allen_smoke``). The production ``neurosearch_dataset_catalog``
collection is snapshotted before/after and MUST remain unchanged (4619).

Verifies: product enumeration, metadata retrieval, normalization, validation,
persistence, identity resolution, rawMetadata.allen, child dataset statistics,
API request accounting, retries, rate-limit responses, elapsed time, and ZERO
image/file calls.

Smoke set (representative products, varied species + categories):
  62  Mouse  — Allen Brain Observatory (multi-plane optical physiology)
  26  Human  — Allen Human Brain Atlas ISH Autism Study
  28  NHP    — NIH Blueprint NHP Macrodissection Microarray
  33  Mouse  — Mouse Cell Types (large specimen/donor child sets)
  34  Human  — Aging, Dementia and TBI non-ISH data

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.ingest_allen_smoke

Report: ../trace_artifacts/allen_investigation_20260816/allen_smoke_20260816.json
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import run_allen_ingestion
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "allen_investigation_20260816",
        "allen_smoke_20260816.json",
    )
)

# Representative products across species/categories (verified live shapes).
SMOKE_PRODUCT_IDS = [62, 26, 28, 33, 34]

# Repo-global child totals verified live from the RMA model counts
# (criteria=model::SectionDataSet / MicroarrayDataSet / AtlasDataSet, num_rows=1).
VERIFIED_REPO_GLOBAL_CHILD_TOTALS = {
    "SectionDataSet": 170_036,
    "MicroarrayDataSet": 6_703,
    "AtlasDataSet": 22,
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _catalog_snapshot(db, datasets_coll) -> dict:
    """Read-only snapshot of the catalog + production datasets collection."""
    coll = db[CATALOG_COLLECTION]
    total = await coll.count_documents({})
    from collections import Counter
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


async def _representative_records(db, count: int = 5) -> list[dict]:
    coll = db[CATALOG_COLLECTION]
    reps: list[dict] = []
    async for d in coll.find(
        {"sources.repository": "allen"},
        {
            "canonicalDatasetId": 1, "title": 1, "doi": 1, "license": 1,
            "species": 1, "modality": 1, "ageGroup": 1, "sources": 1,
            "sourceKeys": 1, "rawMetadata": 1, "provenance": 1,
        },
    ).sort("canonicalDatasetId", 1):
        allen_srcs = [s for s in (d.get("sources") or []) if s.get("repository") == "allen"]
        if not allen_srcs:
            continue
        s = allen_srcs[0]
        reps.append(
            {
                "canonicalDatasetId": d.get("canonicalDatasetId"),
                "title": d.get("title"),
                "doi": d.get("doi"),
                "license": d.get("license"),
                "species": d.get("species"),
                "modality": d.get("modality"),
                "ageGroup": d.get("ageGroup"),
                "sourceDatasetId": s.get("sourceDatasetId"),
                "sourceUrl": s.get("sourceUrl"),
                "availability": s.get("availability"),
                "snapshot": s.get("snapshot"),
                "rawMetadataAllenKeys": sorted(
                    (d.get("rawMetadata") or {}).get("allen") or {}
                ),
                "rawMetadataAllenChildStats": (
                    (d.get("rawMetadata") or {}).get("allen") or {}
                ).get("childStats"),
                "rawMetadataAllenProduct": (
                    (d.get("rawMetadata") or {}).get("allen") or {}
                ).get("product"),
                "identity": (d.get("provenance") or {}).get("identity"),
            }
        )
        if len(reps) >= count:
            break
    return reps


async def main() -> int:
    settings = get_settings()
    prod_db_name = settings.MONGO_DB_NAME
    smoke_db_name = f"{prod_db_name}_allen_smoke"
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)

    # Production catalog is READ-ONLY in this run.
    prod_db = client[prod_db_name]
    datasets_coll = prod_db[settings.MONGO_DATASET_COLLECTION]
    before = await _catalog_snapshot(prod_db, datasets_coll)
    print(f"[allen-smoke] production BEFORE: {json.dumps(before)}")

    # Isolated test DB — drop for a clean run.
    smoke_db = client[smoke_db_name]
    await smoke_db.drop_collection(CATALOG_COLLECTION)
    print(f"[allen-smoke] isolated db={smoke_db_name} collection={CATALOG_COLLECTION} (dropped)")

    stats = await run_allen_ingestion(
        smoke_db,
        ids=SMOKE_PRODUCT_IDS,
        page_delay=0.15,
    )

    after = await _catalog_snapshot(prod_db, datasets_coll)
    smoke_total = await smoke_db[CATALOG_COLLECTION].count_documents({})
    print(f"[allen-smoke] production AFTER: {json.dumps(after)}")

    reps = await _representative_records(smoke_db)

    # rawMetadata.allen coverage on every smoke record.
    raw_coverage = 0
    smoke_records: list[dict] = []
    async for d in smoke_db[CATALOG_COLLECTION].find({}):
        raw = (d.get("rawMetadata") or {}).get("allen")
        if raw:
            raw_coverage += 1
        smoke_records.append(
            {
                "canonicalDatasetId": d.get("canonicalDatasetId"),
                "sourceKeys": d.get("sourceKeys"),
                "title": d.get("title"),
            }
        )

    production_untouched = (
        before["catalog_total"] == after["catalog_total"]
        and before["repo_source_counts"] == after["repo_source_counts"]
        and before["production_datasets_collection_total"]
        == after["production_datasets_collection_total"]
    )

    report = {
        "report_generated_at": _utcnow(),
        "mode": "isolated 5-product live smoke test (test DB only)",
        "smoke_db": smoke_db_name,
        "production_db": prod_db_name,
        "collection": CATALOG_COLLECTION,
        "SMOKE_SET": SMOKE_PRODUCT_IDS,
        "INGESTION": {
            "products_enumerated": stats["discovered"],
            "api_total_rows": stats["enumerated_total"],
            "expected_products": stats["expected_products"],
            "products_retrieved": stats["retrieved"],
            "normalized": stats["normalized"],
            "validated_ok": stats["validated_ok"],
            "inserted": stats["inserted"],
            "merged": stats["merged"],
            "matched_via": stats["matched_via"],
            "failures": stats["failed"],
            "api_failures": stats["api_failures"],
            "failure_details": stats["failure_details"][:20],
            "validation_failed": stats["validation_failed"],
            "retries": stats["retries"],
            "rate_limit_responses": stats["rate_limit_responses"],
            "ambiguous_candidates": stats["ambiguous_candidates"],
        },
        "API_REQUESTS": {
            "product_enumeration_requests": stats["enumeration_requests"],
            "age_model_requests": stats["age_requests"],
            "product_metadata_requests": stats["product_requests"],
            "extra_api_requests": stats["age_requests"],
            "total_api_requests": stats["total_api_calls"],
            "image_file_calls": stats["asset_calls"],
            "elapsed_s": stats["elapsed_s"],
        },
        "CHILD_STATS_TOTAL": stats["child_stats_total"],
        "VERIFIED_REPO_GLOBAL_CHILD_TOTALS": VERIFIED_REPO_GLOBAL_CHILD_TOTALS,
        "SMOKE_DATABASE": {
            "canonical_total": smoke_total,
            "records": smoke_records,
            "rawMetadata_allen_coverage": f"{raw_coverage}/{smoke_total}",
        },
        "PRODUCTION_INTEGRITY": {
            "catalog_before": before["catalog_total"],
            "catalog_after": after["catalog_total"],
            "repo_source_counts_before": before["repo_source_counts"],
            "repo_source_counts_after": after["repo_source_counts"],
            "production_datasets_collection_before": before["production_datasets_collection_total"],
            "production_datasets_collection_after": after["production_datasets_collection_total"],
            "untouched": production_untouched,
        },
        "REPRESENTATIVE_RECORDS": reps,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[allen-smoke] === SUMMARY ===")
    print(f"[allen-smoke] enumerated={stats['discovered']} (api total={stats['enumerated_total']}) "
          f"retrieved={stats['retrieved']} normalized={stats['normalized']}")
    print(f"[allen-smoke] inserted={stats['inserted']} merged={stats['merged']} "
          f"failed={stats['failed']} api_failures={stats['api_failures']}")
    print(f"[allen-smoke] requests: enumeration={stats['enumeration_requests']} "
          f"ages={stats['age_requests']} product={stats['product_requests']} "
          f"total={stats['total_api_calls']} image_file_calls={stats['asset_calls']} "
          f"retries={stats['retries']} rate_limit={stats['rate_limit_responses']} "
          f"elapsed={stats['elapsed_s']}s")
    print(f"[allen-smoke] smoke db canonical total: {smoke_total} "
          f"(rawMetadata.allen coverage {raw_coverage}/{smoke_total})")
    print(f"[allen-smoke] PRODUCTION: {before['catalog_total']} -> {after['catalog_total']} "
          f"untouched={production_untouched}")
    print(f"[allen-smoke] report written to {_REPORT_PATH}")

    client.close()
    return 0 if (production_untouched and stats["asset_calls"] == 0) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
