"""Full Allen Phase-1 production ingestion — gated runner (2026-08-16).

Runs ``run_allen_ingestion()`` with NO limit against the production
``neurosearch_dataset_catalog`` collection and writes a complete before/after
report (INGESTION / API / IDENTITY / DATABASE / INTEGRITY sections plus
representative persisted records).

SAFETY GATES (this tool refuses to write when they fail):
  1. The read-only identity audit must report 64 products checked, 0 merges,
     0 unresolved (production ``trace_artifacts/allen_investigation_20260816/
     allen_identity_audit_20260816.json`` is re-checked, not trusted blindly —
     the audit is re-run inline in read-only mode before any write).
  2. The live Product enumeration count must equal 64 (the locked decision).

DO NOT run this in the Phase-1 step — the approved order is:
implementation → tests → read-only identity audit → isolated 5-Product smoke
test → review → ONLY THEN this full run.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.ingest_allen_full

Report: ../trace_artifacts/allen_investigation_20260816/allen_full_ingestion_20260816.json
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import httpx
from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import ALLEN_API_BASE, ALLEN_EXPECTED_PRODUCTS, run_allen_ingestion
from app.catalog.normalize import build_allen_source_record
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "allen_investigation_20260816",
        "allen_full_ingestion_20260816.json",
    )
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _audit_products(collection, products: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Read-only identity audit over the given products (inline safety gate)."""
    inserts: list[dict] = []
    merges: list[dict] = []
    unresolved: list[dict] = []
    for p in products:
        source = build_allen_source_record(p)
        resolution = await resolve_identity(collection, source)
        if resolution["matched"] is not None:
            merges.append(
                {
                    "productId": p.get("id"),
                    "existingCanonicalDatasetId": resolution["matched"].get("canonicalDatasetId"),
                    "matchedVia": resolution.get("matchedVia"),
                    "existingRepositories": sorted(
                        {s.get("repository") for s in (resolution["matched"].get("sources") or [])}
                    ),
                }
            )
        elif resolution["ambiguous"]:
            unresolved.append({"productId": p.get("id"), "title": source.get("title")})
        else:
            inserts.append({"productId": p.get("id"), "title": source.get("title")})
    return inserts, merges, unresolved


async def _snapshot(coll, datasets_coll) -> dict:
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


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    coll = db[CATALOG_COLLECTION]
    datasets_coll = db[settings.MONGO_DATASET_COLLECTION]

    print(f"[allen-full] production db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")

    # ── Gate 1: live enumeration must equal 64. ─────────────────────────────
    async with httpx.AsyncClient(timeout=120) as http:
        resp = await http.get(
            f"{ALLEN_API_BASE}/query.json",
            params={"criteria": "model::Product", "num_rows": "all"},
        )
        resp.raise_for_status()
        payload = resp.json()
    products = payload.get("msg") or []
    api_total = payload.get("total_rows")
    print(f"[allen-full] enumerated {len(products)} products (api total_rows={api_total})")
    if api_total != ALLEN_EXPECTED_PRODUCTS or len(products) != ALLEN_EXPECTED_PRODUCTS:
        print(f"[allen-full] *** BLOCKER: live count {api_total} != expected "
              f"{ALLEN_EXPECTED_PRODUCTS} — NOT running production ingestion ***")
        client.close()
        return 1

    # ── Gate 2: read-only identity audit (zero writes). ─────────────────────
    collection = get_collection(db)
    inserts, merges, unresolved = await _audit_products(collection, products)
    print(f"[allen-full] audit: would_insert={len(inserts)} would_merge={len(merges)} "
          f"unresolved={len(unresolved)}")
    if merges or unresolved:
        print("[allen-full] *** BLOCKER: unexpected identity matches/unresolved — "
              "NOT running production ingestion (review before writing) ***")
        for m in merges:
            print(f"[allen-full]   merge: product {m['productId']} -> "
                  f"{m['existingCanonicalDatasetId']} via {m['matchedVia']}")
        client.close()
        return 1

    before = await _snapshot(coll, datasets_coll)
    print(f"[allen-full] BEFORE: {json.dumps(before)}")

    argv = [a for a in sys.argv[1:] if a.strip()]
    analyze_only = "--analyze-only" in argv
    if analyze_only:
        stats = {
            "discovered": 0, "retrieved": 0, "normalized": 0, "inserted": 0,
            "merged": 0, "matched_via": {}, "failed": 0, "api_failures": 0,
            "retries": 0, "rate_limit_responses": 0, "pages": 0,
            "enumeration_requests": 0, "age_requests": 0, "product_requests": 0,
            "asset_calls": 0, "total_api_calls": 0, "elapsed_s": 0,
            "enumerated_total": api_total, "ambiguous_candidates": 0,
            "failure_details": [], "child_stats_total": {},
            "validation_failed": 0,
        }
    else:
        stats = await run_allen_ingestion(db, page_delay=0.15)

    after = await _snapshot(coll, datasets_coll)
    print(f"[allen-full] finished ingestion at {_utcnow()}")

    reps: list[dict] = []
    async for d in coll.find(
        {"sources.repository": "allen"},
        {
            "canonicalDatasetId": 1, "title": 1, "species": 1, "doi": 1,
            "license": 1, "sources": 1, "sourceKeys": 1,
        },
    ).sort("canonicalDatasetId", 1):
        srcs = [s for s in (d.get("sources") or []) if s.get("repository") == "allen"]
        if not srcs:
            continue
        s = srcs[0]
        reps.append(
            {
                "canonicalDatasetId": d.get("canonicalDatasetId"),
                "title": d.get("title"),
                "species": d.get("species"),
                "doi": d.get("doi"),
                "license": d.get("license"),
                "sourceDatasetId": s.get("sourceDatasetId"),
                "sourceUrl": s.get("sourceUrl"),
                "dataSetCount": (s.get("snapshot") or {}).get("dataSetCount"),
                "specimenCount": (s.get("snapshot") or {}).get("specimenCount"),
                "donorCount": (s.get("snapshot") or {}).get("donorCount"),
            }
        )
        if len(reps) >= 5:
            break

    allen_sources_total = 0
    allen_docs = 0
    raw_coverage = 0
    async for d in coll.find({}, {"sources": 1, "rawMetadata": 1}):
        srcs = [s for s in (d.get("sources") or []) if s.get("repository") == "allen"]
        if not srcs:
            continue
        allen_docs += 1
        allen_sources_total += len(srcs)
        if (d.get("rawMetadata") or {}).get("allen") is not None:
            raw_coverage += 1

    report = {
        "report_generated_at": _utcnow(),
        "mode": "full production ingestion (no limit)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "INGESTION": {
            "products_enumerated": stats["discovered"],
            "api_total_rows": stats.get("enumerated_total", api_total),
            "expected_products": ALLEN_EXPECTED_PRODUCTS,
            "products_retrieved": stats["retrieved"],
            "products_normalized": stats["normalized"],
            "inserted": stats["inserted"],
            "merged": stats["merged"],
            "matched_via": stats["matched_via"],
            "failures": stats["failed"],
            "api_failures": stats["api_failures"],
            "validation_failed": stats.get("validation_failed", 0),
            "failure_details": stats["failure_details"][:20],
            "elapsed_s": stats["elapsed_s"],
        },
        "API": {
            "product_enumeration_requests": stats["enumeration_requests"],
            "age_model_requests": stats["age_requests"],
            "product_metadata_requests": stats["product_requests"],
            "total_api_requests": stats["total_api_calls"],
            "image_file_calls": stats["asset_calls"],
            "retries": stats["retries"],
            "rate_limit_responses": stats["rate_limit_responses"],
        },
        "IDENTITY_AUDIT": {
            "products_checked": len(products),
            "would_insert": len(inserts),
            "would_merge": len(merges),
            "unresolved": len(unresolved),
        },
        "DATABASE": {
            "canonical_count_before": before["catalog_total"],
            "canonical_count_after": after["catalog_total"],
            "delta": after["catalog_total"] - before["catalog_total"],
            "repo_source_counts_before": before["repo_source_counts"],
            "repo_source_counts_after": after["repo_source_counts"],
        },
        "INTEGRITY": {
            "image_file_calls": stats["asset_calls"],
            "production_datasets_collection_before": before["production_datasets_collection_total"],
            "production_datasets_collection_after": after["production_datasets_collection_total"],
            "rawMetadata_allen_coverage": f"{raw_coverage}/{allen_docs}",
            "allen_docs_total": allen_docs,
            "allen_sources_total": allen_sources_total,
        },
        "REPRESENTATIVE_RECORDS": reps,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[allen-full] === SUMMARY ===")
    print(f"[allen-full] INGESTION: enumerated={stats['discovered']} retrieved={stats['retrieved']} "
          f"inserted={stats['inserted']} merged={stats['merged']} failed={stats['failed']} "
          f"elapsed={stats['elapsed_s']}s")
    print(f"[allen-full] API: total_requests={stats['total_api_calls']} "
          f"image_file_calls={stats['asset_calls']} retries={stats['retries']} "
          f"rate_limit={stats['rate_limit_responses']}")
    print(f"[allen-full] DATABASE: {before['catalog_total']} -> {after['catalog_total']} "
          f"(delta={after['catalog_total'] - before['catalog_total']})")
    print(f"[allen-full] INTEGRITY: raw_allen_coverage={raw_coverage}/{allen_docs} "
          f"production_datasets={after['production_datasets_collection_total']}")
    print(f"[allen-full] report written to {_REPORT_PATH}")

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
