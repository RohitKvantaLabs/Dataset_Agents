"""Standalone NEMAR Phase-1 smoke-test runner (investigation 2026-08-10).

Runs a SMALL real NEMAR ingestion (10 datasets) against an ISOLATED database
so the production ``neurosearch_dataset_catalog`` is never written.

- Enumeration + details come from the LIVE api.nemar.org (read-only GETs).
- The isolated DB is seeded with the 4 existing OpenNeuro canonical records
  (copied read-only from the production catalog) so the mirror-MERGE path is
  exercised exactly as it will be in production.
- The production catalog is only ever COUNTED (before/after) to prove it was
  left untouched.

Usage (from Neuro-Agents/)::

    NEMAR_MONGO_DB=neurosearch_catalog_nemar_smoke_20260810 \
        .venv/Scripts/python.exe -m trace_tools.ingest_nemar_catalog

The runner refuses to run against the production MONGO_DB_NAME.
Report: ../trace_artifacts/nemar_investigation_20260810/nemar_smoke_20260810.json
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import run_nemar_ingestion
from app.catalog.persistence import ensure_indexes
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "nemar_investigation_20260810", "nemar_smoke_20260810.json",
    )
)

# Representative 10-dataset sample (verified live in the investigation):
#   4 mirrors whose OpenNeuro sources exist in the catalog (MERGE)
#   1 mirror whose OpenNeuro source (ds007221) is missing (NEW candidate)
#   5 NEMAR-native datasets (NEW canonical)
SAMPLE_IDS = [
    "on004504", "on004584", "on005274", "on007763",   # existing mirrors → MERGE
    "on007221",                                        # missing mirror → NEW
    "nm000103", "nm000114", "nm000108", "nm000276", "nm000232",  # natives → NEW
]
SEED_OPENNEURO = {"ds004504", "ds004584", "ds005274", "ds007763"}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def main() -> int:
    settings = get_settings()
    prod_db_name = settings.MONGO_DB_NAME
    db_name = os.environ.get("NEMAR_MONGO_DB", "nemar_smoke_20260810")

    if db_name == prod_db_name:
        print(f"REFUSING: NEMAR_MONGO_DB={db_name!r} is the production database. "
              "Set an isolated test database name.")
        return 1
    if "smoke" not in db_name.lower() and "test" not in db_name.lower():
        print(f"REFUSING: NEMAR_MONGO_DB={db_name!r} does not look like an isolated "
              "test/smoke database. Refusing to write anywhere ambiguous.")
        return 1

    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    prod_coll = client[prod_db_name][CATALOG_COLLECTION]
    test_db = client[db_name]
    test_coll = test_db[CATALOG_COLLECTION]

    print(f"[nemar-smoke] production db={prod_db_name} isolated db={db_name}")

    prod_before = await prod_coll.count_documents({})
    print(f"[nemar-smoke] production catalog before: {prod_before}")

    await ensure_indexes(test_db)

    # Clean slate for the isolated run, then seed the existing OpenNeuro
    # canonical records (copied READ-ONLY from production).
    await test_coll.delete_many({})
    seeded = []
    missing = []
    for ds_id in SEED_OPENNEURO:
        doc = await prod_coll.find_one({"sourceKeys": f"openneuro:{ds_id}"})
        if doc is None:
            missing.append(ds_id)
            print(f"[nemar-smoke] WARNING: openneuro {ds_id} not in production catalog")
            continue
        doc.pop("_id", None)
        await test_coll.insert_one(doc)
        seeded.append(ds_id)
        print(f"[nemar-smoke] seeded openneuro canonical {ds_id} -> {doc.get('canonicalDatasetId')}")

    # ── Live NEMAR ingestion (read-only GETs, isolated DB writes) ──────────
    stats = await run_nemar_ingestion(
        test_db,
        target=len(SAMPLE_IDS),
        ids=SAMPLE_IDS,
        page_delay=0.15,
    )

    # ── Verification ────────────────────────────────────────────────────────
    prod_after = await prod_coll.count_documents({})
    docs = [d async for d in test_coll.find({})]

    sample_docs = []
    for d in sorted(docs, key=lambda x: x.get("canonicalDatasetId", "")):
        sample_docs.append(
            {
                "canonicalDatasetId": d.get("canonicalDatasetId"),
                "title": (d.get("title") or "")[:90],
                "doi": d.get("doi"),
                "sourceKeys": d.get("sourceKeys"),
                "sources": [
                    {"repository": s.get("repository"), "sourceDatasetId": s.get("sourceDatasetId")}
                    for s in (d.get("sources") or [])
                ],
                "rawMetadataRepos": list((d.get("rawMetadata") or {}).keys()),
            }
        )

    report = {
        "report_generated_at": _utcnow(),
        "mode": "isolated smoke test",
        "isolated_db": db_name,
        "production_db": prod_db_name,
        "sample_ids": SAMPLE_IDS,
        "seeded_openneuro": seeded,
        "seed_missing_from_production": missing,
        "production_catalog_untouched": prod_before == prod_after,
        "production_catalog_count_before": prod_before,
        "production_catalog_count_after": prod_after,
        "ingestion_stats": {k: stats[k] for k in (
            "discovered", "retrieved", "normalized", "validated_ok", "validation_failed",
            "inserted", "merged", "matched_via", "failed", "api_failures", "retries",
            "pages", "list_requests", "detail_requests", "asset_calls", "total_api_calls",
            "ambiguous_candidates", "elapsed_s",
        )},
        "failure_details": stats.get("failure_details", [])[:20],
        "isolated_docs": len(docs),
        "sample_canonical_documents": sample_docs,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))
    print(f"[nemar-smoke] report written to {_REPORT_PATH}")

    await client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
