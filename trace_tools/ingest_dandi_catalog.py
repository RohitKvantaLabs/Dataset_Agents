"""Standalone DANDI catalog ingestion runner (Phase 1).

Mirrors the OpenNeuro catalog runner: connects to MongoDB Atlas, ingests
public DANDI dandisets into the dedicated ``neurosearch_dataset_catalog``
collection, then writes the standard five-section validation report to JSON.

The production ``datasets`` collection is never written; it is only
counted so the report can prove it was left untouched.

Usage::

    $env:CATALOG_TARGET = "10"     # optional --limit equivalent (smoke test)
    python -m trace_tools.ingest_dandi_catalog

Report is written to ``catalog_dandi_report.json`` next to the runner.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

from app.catalog.ingest import run_dandi_ingestion
from app.catalog.persistence import ensure_indexes, get_collection
from app.catalog.report import build_report
from app.config import get_settings
from app.db.mongo import close_client, get_db

_REPORT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "catalog_dandi_report.json")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def main() -> None:
    settings = get_settings()
    db = get_db()
    prod_collection = db[settings.MONGO_DATASET_COLLECTION]

    target_raw = os.environ.get("CATALOG_TARGET", "").strip()
    target = int(target_raw) if target_raw else None

    print(f"[dandi-runner] connecting to {settings.MONGO_DB_NAME} "
          f"(catalog={settings.MONGO_DB_NAME}.neurosearch_dataset_catalog, "
          f"production={settings.MONGO_DATASET_COLLECTION})")

    await ensure_indexes(db)

    prod_before = await prod_collection.count_documents({})
    prod_openneuro_before = await prod_collection.count_documents({"source": "openneuro"})
    print(f"[dandi-runner] production before: count={prod_before} openneuro={prod_openneuro_before}")

    stats = await run_dandi_ingestion(db, target=target)
    stats["production_before"] = {
        "count": prod_before,
        "openneuro_count": prod_openneuro_before,
    }

    coll = get_collection(db)
    docs = [doc async for doc in coll.find({})]
    print(f"[dandi-runner] reading {len(docs)} docs from catalog for report")

    report = await build_report(db, stats, docs, prod_collection)
    report["report_generated_at"] = _utcnow()

    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)
    print(f"[dandi-runner] report written to {_REPORT_PATH}")

    await close_client()


if __name__ == "__main__":
    asyncio.run(main())
