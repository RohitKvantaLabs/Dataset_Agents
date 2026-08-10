"""Full NEMAR Phase-1 ingestion — production catalog (2026-08-10).

Runs ``run_nemar_ingestion()`` with NO limit against the production
``neurosearch_dataset_catalog`` collection and writes a complete
before/after report (INGESTION / NEMAR / IDENTITY / DATABASE / INTEGRITY
sections plus the full cross-repository merge and new-canonical lists).

This is the real production run (the smoke runner stays isolated-DB-only).
No adapter code is modified — the existing list pagination, detail endpoint,
normalization, validation, identity resolution and persistence are reused
verbatim.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.ingest_nemar_full

Report: ../trace_artifacts/nemar_investigation_20260810/nemar_full_ingestion_20260810.json
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import run_nemar_ingestion
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "nemar_investigation_20260810",
        "nemar_full_ingestion_20260810.json",
    )
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _snapshot(coll, datasets_coll) -> dict:
    """Read-only before/after snapshot of the catalog collection."""
    total = await coll.count_documents({})
    repo_docs: dict[str, int] = {}
    canonical_ids: set[str] = set()
    source_keys_by_doc: dict[str, list[str]] = {}
    async for d in coll.find({}, {"canonicalDatasetId": 1, "sources": 1, "sourceKeys": 1}):
        cid = d.get("canonicalDatasetId")
        if cid:
            canonical_ids.add(str(cid))
        repos = {s.get("repository") for s in (d.get("sources") or []) if s.get("repository")}
        for r in repos:
            repo_docs[r] = repo_docs.get(r, 0) + 1
        if cid:
            source_keys_by_doc[str(cid)] = list(d.get("sourceKeys") or [])
    datasets_total = await datasets_coll.count_documents({})
    return {
        "catalog_total": total,
        "distinct_canonical_dataset_ids": len(canonical_ids),
        "repos": repo_docs,
        "canonical_ids": sorted(canonical_ids),
        "source_keys_by_doc": source_keys_by_doc,
        "production_datasets_collection_total": datasets_total,
    }


async def main() -> int:
    settings = get_settings()
    db_name = settings.MONGO_DB_NAME
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[db_name]
    coll = db[CATALOG_COLLECTION]
    datasets_coll = db[settings.MONGO_DATASET_COLLECTION]

    print(f"[nemar-full] production db={db_name} collection={CATALOG_COLLECTION}")
    print(f"[nemar-full] started at {_utcnow()}")

    before = await _snapshot(coll, datasets_coll)
    print(
        f"[nemar-full] BEFORE: catalog_total={before['catalog_total']} "
        f"repos={json.dumps(before['repos'])} "
        f"production_datasets_collection={before['production_datasets_collection_total']}"
    )

    # ── Ingestion — NO limit by default (all live NEMAR datasets). ──────────
    #    Optional argv: dataset IDs to retry (e.g. a rate-limited subset);
    #    when given, only those IDs are fetched (enumeration skipped).
    #    ``--analyze-only`` skips ingestion entirely (report from DB state).
    argv = [a for a in sys.argv[1:] if a.strip()]
    analyze_only = "--analyze-only" in argv
    retry_ids = [i for i in argv if i.strip() and i != "--analyze-only"] or None
    if analyze_only:
        stats = {
            "discovered": 0, "retrieved": 0, "normalized": 0, "inserted": 0,
            "merged": 0, "matched_via": {}, "failed": 0, "api_failures": 0,
            "retries": 0, "pages": 0, "list_requests": 0, "detail_requests": 0,
            "asset_calls": 0, "ambiguous_candidates": 0, "ambiguous_sample": [],
            "failure_details": [], "elapsed_s": 0,
        }
    else:
        stats = await run_nemar_ingestion(
            db,
            page_delay=1.0 if retry_ids else 0.15,
            ids=retry_ids,
        )

    after = await _snapshot(coll, datasets_coll)
    print(f"[nemar-full] finished ingestion at {_utcnow()}")

    # ── Post-run analysis ───────────────────────────────────────────────────
    before_repos = before["repos"]
    after_repos = after["repos"]
    before_ids = set(before["canonical_ids"])
    after_ids = set(after["canonical_ids"])
    before_keys_by_doc = before["source_keys_by_doc"]
    after_keys_by_doc = after["source_keys_by_doc"]

    new_canonical_ids = sorted(after_ids - before_ids)

    # NEMAR source-level details (mirrors vs natives, per-doc).
    nemar_sources_total = 0
    nemar_source_ids: list[str] = []
    mirrors: list[dict] = []          # merged mirror rows
    natives: list[dict] = []          # new-candidate rows
    nemar_docs: list[dict] = []       # every doc carrying a nemar source
    openneuro_changed: list[str] = []
    dandi_changed: list[str] = []
    raw_nemar_coverage = 0
    async for d in coll.find({}, {
        "canonicalDatasetId": 1, "sources": 1, "sourceKeys": 1, "rawMetadata": 1,
    }):
        sources = d.get("sources") or []
        nemar_srcs = [s for s in sources if s.get("repository") == "nemar"]
        openneuro_srcs = [s for s in sources if s.get("repository") == "openneuro"]
        dandi_srcs = [s for s in sources if s.get("repository") == "dandi"]
        cid = str(d.get("canonicalDatasetId"))
        keys = set(d.get("sourceKeys") or [])

        if not nemar_srcs:
            continue
        nemar_docs.append(cid)
        nemar_sources_total += len(nemar_srcs)
        if (d.get("rawMetadata") or {}).get("nemar") is not None:
            raw_nemar_coverage += 1
        for s in nemar_srcs:
            sid = s.get("sourceDatasetId")
            nemar_source_ids.append(sid)
            snapshot = s.get("snapshot") or {}
            on_id = snapshot.get("openNeuroSourceId")
            raw = (d.get("rawMetadata") or {}).get("nemar") or {}
            is_mirror = bool(on_id or raw.get("source") == "openneuro")
            if is_mirror:
                mirrors.append({
                    "nemarSourceDatasetId": sid,
                    "openNeuroSourceDatasetId": on_id or raw.get("source_id"),
                    "canonicalDatasetId": cid,
                })
            else:
                natives.append({
                    "nemarSourceDatasetId": sid,
                    "canonicalDatasetId": cid,
                    "reason": "NEMAR-native dataset (no cross-repository source)",
                })

    # OpenNeuro / DANDI records changed = docs whose sourceKeys changed vs before.
    for cid, before_keys in before_keys_by_doc.items():
        after_keys = set(after_keys_by_doc.get(cid, []))
        if set(before_keys) != after_keys:
            if any(k.startswith("openneuro:") for k in before_keys):
                openneuro_changed.append(cid)
            if any(k.startswith("dandi:") for k in before_keys):
                dandi_changed.append(cid)

    # Mirrors that merged into an EXISTING canonical record (their
    # canonicalDatasetId was present before the run).
    mirrors_merged_into_existing = [
        m for m in mirrors if m["canonicalDatasetId"] in before_ids
    ]

    # New canonical records created from NEMAR — reason per doc.
    new_nemar_docs: list[dict] = []
    before_on_keys = {
        k for ks in before_keys_by_doc.values() for k in ks if k.startswith("openneuro:")
    }
    for cid in new_canonical_ids:
        if cid not in nemar_docs:
            continue
        for m in mirrors:
            if m["canonicalDatasetId"] == cid:
                new_nemar_docs.append({
                    "canonicalDatasetId": cid,
                    "nemarSourceDatasetId": m["nemarSourceDatasetId"],
                    "reason": (
                        "OpenNeuro mirror whose OpenNeuro source "
                        f"({m['openNeuroSourceDatasetId']}) is not present in the "
                        "catalog snapshot — new canonical candidate"
                    ),
                })
                break
        else:
            for n in natives:
                if n["canonicalDatasetId"] == cid:
                    new_nemar_docs.append({
                        "canonicalDatasetId": cid,
                        "nemarSourceDatasetId": n["nemarSourceDatasetId"],
                        "reason": n["reason"],
                    })
                    break

    ds007221 = next((m for m in mirrors if m["openNeuroSourceDatasetId"] == "ds007221"), None)

    report = {
        "report_generated_at": _utcnow(),
        "mode": "full production ingestion (no limit)",
        "production_db": db_name,
        "collection": CATALOG_COLLECTION,
        "INGESTION": {
            "datasets_discovered": stats["discovered"],
            "datasets_retrieved": stats["retrieved"],
            "datasets_normalized": stats["normalized"],
            "datasets_persisted": stats["inserted"] + stats["merged"],
            "inserted": stats["inserted"],
            "merged": stats["merged"],
            "failures": stats["failed"],
            "api_failures": stats["api_failures"],
            "retries": stats["retries"],
            "pages": stats["pages"],
            "list_requests": stats["list_requests"],
            "detail_requests": stats["detail_requests"],
            "matched_via": stats["matched_via"],
            "failure_details": stats["failure_details"][:20],
            "elapsed_s": stats["elapsed_s"],
        },
        "NEMAR": {
            "total_nemar_sources": nemar_sources_total,
            "distinct_sourceDatasetIds": len(set(nemar_source_ids)),
            "duplicate_source_ids": len(nemar_source_ids) - len(set(nemar_source_ids)),
        },
        "IDENTITY": {
            "mirrors_detected": len(mirrors),
            "mirrors_merged_into_existing": len(mirrors_merged_into_existing),
            "nemar_native_datasets": len(natives),
            "new_canonical_datasets": len(new_canonical_ids),
            "unresolved_ambiguous_candidates": stats["ambiguous_candidates"],
            "ambiguous_sample": stats["ambiguous_sample"],
            "ds007221": ds007221,
        },
        "DATABASE": {
            "canonical_count_before": before["catalog_total"],
            "canonical_count_after": after["catalog_total"],
            "distinct_canonicalDatasetId_before": before["distinct_canonical_dataset_ids"],
            "distinct_canonicalDatasetId_after": after["distinct_canonical_dataset_ids"],
            "openneuro_docs_before": before_repos.get("openneuro", 0),
            "openneuro_docs_after": after_repos.get("openneuro", 0),
            "dandi_docs_before": before_repos.get("dandi", 0),
            "dandi_docs_after": after_repos.get("dandi", 0),
            "nemar_docs_before": before_repos.get("nemar", 0),
            "nemar_docs_after": after_repos.get("nemar", 0),
            "repo_docs_after": after_repos,
        },
        "INTEGRITY": {
            "asset_file_calls": stats["asset_calls"],
            "production_datasets_collection_before": before["production_datasets_collection_total"],
            "production_datasets_collection_after": after["production_datasets_collection_total"],
            "openneuro_records_changed": openneuro_changed,
            "openneuro_records_changed_count": len(openneuro_changed),
            "dandi_records_changed": dandi_changed,
            "dandi_records_changed_count": len(dandi_changed),
            "rawMetadata_nemar_coverage": raw_nemar_coverage,
            "nemar_docs_total": len(nemar_docs),
        },
        "CROSS_REPOSITORY_MERGES": sorted(
            mirrors, key=lambda m: m["nemarSourceDatasetId"]
        ),
        "NEW_NEMAR_CANONICAL_DATASETS": sorted(
            new_nemar_docs, key=lambda m: m["nemarSourceDatasetId"]
        ),
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[nemar-full] === SUMMARY ===")
    print(f"[nemar-full] INGESTION: discovered={stats['discovered']} retrieved={stats['retrieved']} "
          f"normalized={stats['normalized']} persisted={stats['inserted'] + stats['merged']} "
          f"(inserted={stats['inserted']} merged={stats['merged']}) failures={stats['failed']} "
          f"retries={stats['retries']} asset_calls={stats['asset_calls']} elapsed={stats['elapsed_s']}s")
    print(f"[nemar-full] NEMAR: sources={nemar_sources_total} distinct_ids={len(set(nemar_source_ids))} "
          f"duplicates={len(nemar_source_ids) - len(set(nemar_source_ids))}")
    print(f"[nemar-full] IDENTITY: mirrors={len(mirrors)} natives={len(natives)} "
          f"new_canonical={len(new_canonical_ids)} ambiguous={stats['ambiguous_candidates']}")
    print(f"[nemar-full] DATABASE: {before['catalog_total']} -> {after['catalog_total']} "
          f"(openneuro {before_repos.get('openneuro', 0)} -> {after_repos.get('openneuro', 0)}, "
          f"dandi {before_repos.get('dandi', 0)} -> {after_repos.get('dandi', 0)}, "
          f"nemar {before_repos.get('nemar', 0)} -> {after_repos.get('nemar', 0)})")
    print(f"[nemar-full] INTEGRITY: openneuro_changed={len(openneuro_changed)} "
          f"dandi_changed={len(dandi_changed)} raw_nemar_coverage={raw_nemar_coverage}/{len(nemar_docs)}")
    print(f"[nemar-full] ds007221: {ds007221}")
    print(f"[nemar-full] report written to {_REPORT_PATH}")

    await client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
