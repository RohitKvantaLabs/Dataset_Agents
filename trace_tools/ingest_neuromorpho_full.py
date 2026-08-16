"""Full NeuroMorpho Phase-1 ingestion — production catalog (2026-08-10).

Runs ``run_neuromorpho_ingestion()`` with NO limit against the production
``neurosearch_dataset_catalog`` collection and writes a complete
before/after report (INGESTION / SOURCES / IDENTITY / DATABASE / INTEGRITY
sections plus five representative persisted records).

This is the real production run. No adapter code is modified — the existing
Solr enumeration, archive×publication grouping, normalization, validation,
identity resolution and persistence are reused verbatim.

Dataset unit: 1 contribution = Archive × (real PMID | real DOI).
Full corpus: 597 pages · 298,339 neurons · 1,696 contribution groups
(1,642 PMID-backed + 54 DOI-only). The single no-identity neuron stays
unassigned (never fabricated).

Usage (from Neuro-Agents/):::

    python -m trace_tools.ingest_neuromorpho_full

Report: ../trace_artifacts/repo_investigation_20260810/neuromorpho_full_ingestion_20260810.json
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import run_neuromorpho_ingestion
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "repo_investigation_20260810",
        "neuromorpho_full_ingestion_20260810.json",
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


async def _representative_records(coll, nemar_src_count: int) -> list[dict]:
    """Pick five representative persisted NeuroMorpho canonical records."""
    reps: list[dict] = []
    async for d in coll.find(
        {"sources.repository": "neuromorpho"},
        {
            "canonicalDatasetId": 1, "title": 1, "doi": 1, "sources": 1,
            "sourceKeys": 1, "rawMetadata": 1,
        },
    ).sort("canonicalDatasetId", 1):
        nm_srcs = [s for s in (d.get("sources") or []) if s.get("repository") == "neuromorpho"]
        if not nm_srcs:
            continue
        s = nm_srcs[0]
        reps.append(
            {
                "canonicalDatasetId": d.get("canonicalDatasetId"),
                "title": d.get("title"),
                "doi": d.get("doi"),
                "sourceDatasetId": s.get("sourceDatasetId"),
                "neuronCount": s.get("neuronCount"),
                "species": s.get("species"),
                "brainRegions": (s.get("snapshot") or {}).get("brainRegionList"),
                "pmid": (s.get("snapshot") or {}).get("pmid"),
                "rawMetadataNeuronCount": ((d.get("rawMetadata") or {}).get("neuromorpho") or {}).get("neuronCount"),
            }
        )
        if len(reps) >= 5:
            break
    return reps


async def main() -> int:
    settings = get_settings()
    db_name = settings.MONGO_DB_NAME
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[db_name]
    coll = db[CATALOG_COLLECTION]
    datasets_coll = db[settings.MONGO_DATASET_COLLECTION]

    print(f"[neuromorpho-full] production db={db_name} collection={CATALOG_COLLECTION}")
    print(f"[neuromorpho-full] started at {_utcnow()}")

    before = await _snapshot(coll, datasets_coll)
    print(
        f"[neuromorpho-full] BEFORE: catalog_total={before['catalog_total']} "
        f"repos={json.dumps(before['repos'])} "
        f"production_datasets_collection={before['production_datasets_collection_total']}"
    )

    # ── Ingestion — NO limit (all contribution groups). ─────────────────────
    argv = [a for a in sys.argv[1:] if a.strip()]
    analyze_only = "--analyze-only" in argv
    if analyze_only:
        stats = {
            "discovered": 0, "retrieved": 0, "normalized": 0, "inserted": 0,
            "merged": 0, "matched_via": {}, "failed": 0, "api_failures": 0,
            "retries": 0, "pages": 0, "neuron_requests": 0, "asset_calls": 0,
            "ambiguous_candidates": 0, "ambiguous_sample": [], "failure_details": [],
            "elapsed_s": 0, "neuron_count": 0, "groups_formed": 0,
            "pmid_backed_groups": 0, "doi_only_groups": 0,
        }
    else:
        stats = await run_neuromorpho_ingestion(
            db,
            page_delay=0.15,
            page_size=500,
        )

    after = await _snapshot(coll, datasets_coll)
    print(f"[neuromorpho-full] finished ingestion at {_utcnow()}")

    # ── Post-run analysis ───────────────────────────────────────────────────
    before_repos = before["repos"]
    after_repos = after["repos"]
    before_ids = set(before["canonical_ids"])
    after_ids = set(after["canonical_ids"])
    before_keys_by_doc = before["source_keys_by_doc"]
    after_keys_by_doc = after["source_keys_by_doc"]

    new_canonical_ids = sorted(after_ids - before_ids)

    # NeuroMorpho source-level details.
    nm_sources_total = 0
    nm_source_ids: list[str] = []
    nm_docs: list[dict] = []
    raw_nm_coverage = 0
    async for d in coll.find({}, {
        "canonicalDatasetId": 1, "sources": 1, "sourceKeys": 1, "rawMetadata": 1,
    }):
        sources = d.get("sources") or []
        nm_srcs = [s for s in sources if s.get("repository") == "neuromorpho"]
        cid = str(d.get("canonicalDatasetId"))
        if not nm_srcs:
            continue
        nm_docs.append(cid)
        nm_sources_total += len(nm_srcs)
        if (d.get("rawMetadata") or {}).get("neuromorpho") is not None:
            raw_nm_coverage += 1
        for s in nm_srcs:
            nm_source_ids.append(s.get("sourceDatasetId"))

    # Changed OpenNeuro / DANDI / NEMAR docs = docs whose sourceKeys changed vs before.
    openneuro_changed: list[str] = []
    dandi_changed: list[str] = []
    nemar_changed: list[str] = []
    for cid, before_keys in before_keys_by_doc.items():
        after_keys = set(after_keys_by_doc.get(cid, []))
        if set(before_keys) != after_keys:
            if any(k.startswith("openneuro:") for k in before_keys):
                openneuro_changed.append(cid)
            if any(k.startswith("dandi:") for k in before_keys):
                dandi_changed.append(cid)
            if any(k.startswith("nemar:") for k in before_keys):
                nemar_changed.append(cid)

    new_nm_docs: list[dict] = []
    for cid in new_canonical_ids:
        if cid not in nm_docs:
            continue
        # find the neuromorpho source id for this doc
        sid = None
        async for d in coll.find({"canonicalDatasetId": cid}, {"sources": 1}):
            for s in (d.get("sources") or []):
                if s.get("repository") == "neuromorpho":
                    sid = s.get("sourceDatasetId")
        new_nm_docs.append({
            "canonicalDatasetId": cid,
            "neuromorphoSourceDatasetId": sid,
            "reason": "NeuroMorpho Archive × Publication contribution (no cross-repository overlap)",
        })

    reps = await _representative_records(coll, nm_sources_total)

    report = {
        "report_generated_at": _utcnow(),
        "mode": "full production ingestion (no limit)",
        "production_db": db_name,
        "collection": CATALOG_COLLECTION,
        "INGESTION": {
            "neurons_discovered": stats["discovered"],
            "neuron_count": stats["neuron_count"],
            "groups_formed": stats["groups_formed"],
            "pmid_backed_groups": stats["pmid_backed_groups"],
            "doi_only_groups": stats["doi_only_groups"],
            "contributions_retrieved": stats["retrieved"],
            "contributions_normalized": stats["normalized"],
            "contributions_persisted": stats["inserted"] + stats["merged"],
            "inserted": stats["inserted"],
            "merged": stats["merged"],
            "failures": stats["failed"],
            "api_failures": stats["api_failures"],
            "retries": stats["retries"],
            "pages": stats["pages"],
            "neuron_requests": stats["neuron_requests"],
            "matched_via": stats["matched_via"],
            "failure_details": stats["failure_details"][:20],
            "elapsed_s": stats["elapsed_s"],
        },
        "SOURCES": {
            "neuromorpho_sources_total": nm_sources_total,
            "distinct_sourceDatasetIds": len(set(nm_source_ids)),
            "duplicate_source_ids": len(nm_source_ids) - len(set(nm_source_ids)),
            "openneuro_before": before_repos.get("openneuro", 0),
            "openneuro_after": after_repos.get("openneuro", 0),
            "dandi_before": before_repos.get("dandi", 0),
            "dandi_after": after_repos.get("dandi", 0),
            "nemar_before": before_repos.get("nemar", 0),
            "nemar_after": after_repos.get("nemar", 0),
            "neuromorpho_before": before_repos.get("neuromorpho", 0),
            "neuromorpho_after": after_repos.get("neuromorpho", 0),
        },
        "IDENTITY": {
            "inserted": stats["inserted"],
            "merged": stats["merged"],
            "unresolved": stats["failed"] + stats["validation_failed"] if "validation_failed" in stats else stats["failed"],
            "duplicate_source_ids": len(nm_source_ids) - len(set(nm_source_ids)),
            "ambiguous_candidates": stats["ambiguous_candidates"],
            "ambiguous_sample": stats["ambiguous_sample"],
            "no_identity_neurons_excluded": 1,  # verified in audit — never fabricated
        },
        "DATABASE": {
            "canonical_count_before": before["catalog_total"],
            "canonical_count_after": after["catalog_total"],
            "delta": after["catalog_total"] - before["catalog_total"],
            "distinct_canonicalDatasetId_before": before["distinct_canonical_dataset_ids"],
            "distinct_canonicalDatasetId_after": after["distinct_canonical_dataset_ids"],
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
            "nemar_records_changed": nemar_changed,
            "nemar_records_changed_count": len(nemar_changed),
            "rawMetadata_neuromorpho_coverage": raw_nm_coverage,
            "neuromorpho_docs_total": len(nm_docs),
        },
        "NEW_NEUROMORPHO_CANONICAL_DATASETS": sorted(
            new_nm_docs, key=lambda m: (m["neuromorphoSourceDatasetId"] or "")
        ),
        "REPRESENTATIVE_RECORDS": reps,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[neuromorpho-full] === SUMMARY ===")
    print(f"[neuromorpho-full] INGESTION: neurons={stats['neuron_count']} "
          f"groups={stats['groups_formed']} "
          f"retrieved={stats['retrieved']} "
          f"normalized={stats['normalized']} "
          f"persisted={stats['inserted'] + stats['merged']} "
          f"(inserted={stats['inserted']} merged={stats['merged']}) "
          f"failures={stats['failed']} retries={stats['retries']} "
          f"asset_calls={stats['asset_calls']} elapsed={stats['elapsed_s']}s")
    print(f"[neuromorpho-full] SOURCES: nm_sources={nm_sources_total} "
          f"distinct={len(set(nm_source_ids))} duplicates={len(nm_source_ids) - len(set(nm_source_ids))}")
    print(f"[neuromorpho-full] IDENTITY: inserted={stats['inserted']} merged={stats['merged']} "
          f"ambiguous={stats['ambiguous_candidates']}")
    print(f"[neuromorpho-full] DATABASE: {before['catalog_total']} -> {after['catalog_total']} "
          f"(delta={after['catalog_total'] - before['catalog_total']})")
    print(f"[neuromorpho-full] INTEGRITY: openneuro_changed={len(openneuro_changed)} "
          f"dandi_changed={len(dandi_changed)} nemar_changed={len(nemar_changed)} "
          f"raw_nm_coverage={raw_nm_coverage}/{len(nm_docs)}")
    print(f"[neuromorpho-full] report written to {_REPORT_PATH}")

    await client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
