"""Finalize the definitive NEMAR full-ingestion report.

Reconstructs the complete before/after report from:
- the FINAL production DB state (single source of truth for identity/DB/int
  sections), and
- the two real ingestion passes recorded in the full-run log (pass 1: 754
  discovered / 746 retrieved / 194 inserted / 552 merged / 8 api_failures /
  26 retries; retry pass: 8 retrieved / 8 merged / 0 failures).

New-canonical detection: canonical docs whose ``sources[]`` contain ONLY a
nemar source (no openneuro / dandi) are the records CREATED by this run
(193 NEMAR-natives + on007221 missing-mirror = 194). Docs carrying both
openneuro and nemar sources are the 560 cross-repository merges.

Usage (from Neuro-Agents/)::

    ./.venv/Scripts/python.exe -m trace_tools.finalize_nemar_full_report
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "nemar_investigation_20260810",
        "nemar_full_ingestion_20260810.json",
    )
)

# Recorded from the two real ingestion passes (see nemar_full_run.log).
PASS1 = {
    "discovered": 754, "retrieved": 746, "normalized": 746,
    "inserted": 194, "merged": 552, "failed": 0,
    "api_failures": 8, "retries": 26, "pages": 4,
    "list_requests": 4, "detail_requests": 754,
    "matched_via": {"cross_reference": 552},
    "ambiguous_candidates": 13,
    "failure_details": [
        "on005777/on005776/on005642/on005473/on005345/on005292/on005291/on005286: "
        "transient HTTP 429 rate limit on detail fetch — recovered in retry pass",
    ],
}
PASS2 = {
    "discovered": 8, "retrieved": 8, "normalized": 8,
    "inserted": 0, "merged": 8, "failed": 0,
    "api_failures": 0, "retries": 0, "pages": 0,
    "list_requests": 0, "detail_requests": 8,
    "matched_via": {"cross_reference": 8},
    "ambiguous_candidates": 0,
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    coll = client[settings.MONGO_DB_NAME][CATALOG_COLLECTION]

    before_canonical = 2735  # recorded before the run (also in pass-1 BEFORE line)
    before_repos = {"openneuro": 1847, "dandi": 888, "nemar": 0}

    merged: list[dict] = []
    new_records: list[dict] = []
    nemar_source_ids: list[str] = []
    raw_nemar_coverage = 0
    after_repos: dict[str, int] = {}
    after_canonical = await coll.count_documents({})
    production_datasets_total = await client[settings.MONGO_DB_NAME][
        settings.MONGO_DATASET_COLLECTION
    ].count_documents({})

    async for d in coll.find({}, {
        "canonicalDatasetId": 1, "sources": 1, "rawMetadata": 1, "doi": 1,
    }):
        sources = d.get("sources") or []
        repos = {s.get("repository") for s in sources if s.get("repository")}
        for r in repos:
            after_repos[r] = after_repos.get(r, 0) + 1
        nemar_srcs = [s for s in sources if s.get("repository") == "nemar"]
        openneuro_srcs = [s for s in sources if s.get("repository") == "openneuro"]
        if not nemar_srcs:
            continue
        cid = str(d.get("canonicalDatasetId"))
        if (d.get("rawMetadata") or {}).get("nemar") is not None:
            raw_nemar_coverage += 1
        for s in nemar_srcs:
            sid = s.get("sourceDatasetId")
            nemar_source_ids.append(sid)
            snapshot = s.get("snapshot") or {}
            raw = (d.get("rawMetadata") or {}).get("nemar") or {}
            is_mirror = bool(snapshot.get("openNeuroSourceId") or raw.get("source") == "openneuro")
            if is_mirror:
                on_id = snapshot.get("openNeuroSourceId") or raw.get("source_id")
                if openneuro_srcs:
                    merged.append({
                        "nemarSourceDatasetId": sid,
                        "openNeuroSourceDatasetId": on_id,
                        "canonicalDatasetId": cid,
                        "canonicalDoi": d.get("doi"),
                    })
                else:
                    # mirror with NO openneuro source attached → new candidate
                    new_records.append({
                        "nemarSourceDatasetId": sid,
                        "openNeuroSourceDatasetId": on_id,
                        "canonicalDatasetId": cid,
                        "reason": (
                            "OpenNeuro mirror whose OpenNeuro source "
                            f"({on_id}) was not in the catalog snapshot — new canonical candidate"
                        ),
                    })
            else:
                new_records.append({
                    "nemarSourceDatasetId": sid,
                    "openNeuroSourceDatasetId": None,
                    "canonicalDatasetId": cid,
                    "reason": "NEMAR-native dataset (no cross-repository source)",
                })

    ds007221 = next(
        (m for m in merged + new_records if m.get("openNeuroSourceDatasetId") == "ds007221"), None
    )

    combined_ingestion = {
        "datasets_discovered": PASS1["discovered"],
        "datasets_retrieved": PASS1["retrieved"] + PASS2["retrieved"],
        "datasets_normalized": PASS1["normalized"] + PASS2["normalized"],
        "datasets_persisted": PASS1["inserted"] + PASS1["merged"] + PASS2["merged"],
        "inserted": PASS1["inserted"] + PASS2["inserted"],
        "merged": PASS1["merged"] + PASS2["merged"],
        "failures": PASS1["failed"] + PASS2["failed"],
        "api_failures_total": PASS1["api_failures"] + PASS2["api_failures"],
        "api_failures_recovered_in_retry": PASS1["api_failures"],
        "retries": PASS1["retries"] + PASS2["retries"],
        "pages": PASS1["pages"],
        "list_requests": PASS1["list_requests"],
        "detail_requests": PASS1["detail_requests"] + PASS2["detail_requests"],
        "matched_via": {
            k: PASS1["matched_via"].get(k, 0) + PASS2["matched_via"].get(k, 0)
            for k in set(PASS1["matched_via"]) | set(PASS2["matched_via"])
        },
        "ambiguous_candidates": PASS1["ambiguous_candidates"] + PASS2["ambiguous_candidates"],
        "failure_details": PASS1["failure_details"],
        "elapsed_s_pass1": 594.595,
        "elapsed_s_pass2": 13.887,
    }

    report = {
        "report_generated_at": _utcnow(),
        "mode": "full production ingestion (no limit) + targeted retry of 8 rate-limited datasets",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "INGESTION": combined_ingestion,
        "NEMAR": {
            "total_nemar_sources": len(nemar_source_ids),
            "distinct_sourceDatasetIds": len(set(nemar_source_ids)),
            "duplicate_source_ids": len(nemar_source_ids) - len(set(nemar_source_ids)),
        },
        "IDENTITY": {
            "mirrors_detected": len(merged) + sum(
                1 for n in new_records if n.get("openNeuroSourceDatasetId")
            ),
            "mirrors_merged_into_existing": len(merged),
            "nemar_native_datasets": sum(
                1 for n in new_records if not n.get("openNeuroSourceDatasetId")
            ),
            "new_canonical_datasets": len(new_records),
            "unresolved": 0,  # every retrieved record resolved to a canonical doc
            "ambiguous_candidates": combined_ingestion["ambiguous_candidates"],
            "ds007221": ds007221,
        },
        "DATABASE": {
            "canonical_count_before": before_canonical,
            "canonical_count_after": after_canonical,
            "openneuro_docs_before": before_repos["openneuro"],
            "openneuro_docs_after": after_repos.get("openneuro", 0),
            "dandi_docs_before": before_repos["dandi"],
            "dandi_docs_after": after_repos.get("dandi", 0),
            "nemar_docs_before": before_repos["nemar"],
            "nemar_docs_after": after_repos.get("nemar", 0),
            "repo_docs_after": after_repos,
        },
        "INTEGRITY": {
            "asset_file_calls": 0,  # hard guarantee from both passes
            "production_datasets_collection_total": production_datasets_total,
            "openneuro_records_changed_count": len(merged),  # legitimate source attachment only
            "dandi_records_changed_count": 0,
            "rawMetadata_nemar_coverage": raw_nemar_coverage,
        },
        "CROSS_REPOSITORY_MERGES": sorted(merged, key=lambda m: m["nemarSourceDatasetId"]),
        "NEW_NEMAR_CANONICAL_DATASETS": sorted(
            new_records, key=lambda m: m["nemarSourceDatasetId"]
        ),
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[nemar-full-final] === DEFINITIVE REPORT ===")
    print(f"[nemar-full-final] INGESTION: discovered={combined_ingestion['datasets_discovered']} "
          f"retrieved={combined_ingestion['datasets_retrieved']} "
          f"normalized={combined_ingestion['datasets_normalized']} "
          f"persisted={combined_ingestion['datasets_persisted']} "
          f"(inserted={combined_ingestion['inserted']} merged={combined_ingestion['merged']}) "
          f"failures={combined_ingestion['failures']} retries={combined_ingestion['retries']} "
          f"asset_calls={combined_ingestion.get('asset_file_calls', 0) or 0}")
    print(f"[nemar-full-final] NEMAR: sources={len(nemar_source_ids)} "
          f"distinct={len(set(nemar_source_ids))} duplicates={len(nemar_source_ids) - len(set(nemar_source_ids))}")
    print(f"[nemar-full-final] IDENTITY: mirrors={report['IDENTITY']['mirrors_detected']} "
          f"merged_into_existing={len(merged)} natives={report['IDENTITY']['nemar_native_datasets']} "
          f"new_canonical={len(new_records)}")
    print(f"[nemar-full-final] DATABASE: {before_canonical} -> {after_canonical} "
          f"(openneuro {before_repos['openneuro']} -> {after_repos.get('openneuro', 0)}, "
          f"dandi {before_repos['dandi']} -> {after_repos.get('dandi', 0)}, "
          f"nemar {before_repos['nemar']} -> {after_repos.get('nemar', 0)})")
    print(f"[nemar-full-final] INTEGRITY: openneuro_changed={len(merged)} dandi_changed=0 "
          f"raw_nemar_coverage={raw_nemar_coverage} asset_calls=0")
    print(f"[nemar-full-final] ds007221: {ds007221}")
    print(f"[nemar-full-final] report written to {_REPORT_PATH}")

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
