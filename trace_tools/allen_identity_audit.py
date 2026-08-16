"""Read-only Allen identity audit (Phase 1 — 2026-08-16).

For EVERY Allen Product:
  1. generate the normalized source record (build_allen_source_record)
  2. run the EXISTING generic identity resolver (resolve_identity) against the
     PRODUCTION ``neurosearch_dataset_catalog`` collection
  3. record whether it would: insert / merge / remain unresolved
  4. explain every merge candidate

The audit performs ZERO writes. Identity signals (doi=None, sourceUrl, sourceKey,
title, authors=[], modality=[], publicationYear=None) do NOT depend on child
statistics, so the base enumeration payload is sufficient and faithful — the
audit makes ONE live API request (product enumeration) and 64 resolver lookups.

Per the locked decision: if ANY Product resolves to an existing canonical
dataset, the audit reports it (product id, existing canonicalDatasetId, identity
signal, existing repository, confidence) — production ingestion must NOT
proceed until the unexpected match is reviewed.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.allen_identity_audit

Report: ../trace_artifacts/allen_investigation_20260816/allen_identity_audit_20260816.json
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone

import httpx
from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import ALLEN_API_BASE, ALLEN_EXPECTED_PRODUCTS
from app.catalog.normalize import build_allen_source_record
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "allen_investigation_20260816",
        "allen_identity_audit_20260816.json",
    )
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    collection = get_collection(db)

    production_total = await collection.count_documents({})

    # ── 1) Enumerate ALL Products (one live request). ───────────────────────
    async with httpx.AsyncClient(timeout=120) as http:
        resp = await http.get(
            f"{ALLEN_API_BASE}/query.json",
            params={"criteria": "model::Product", "num_rows": "all"},
        )
        resp.raise_for_status()
        payload = resp.json()
    products = payload.get("msg") or []
    api_total = payload.get("total_rows")

    product_ids = [p.get("id") for p in products if isinstance(p, dict)]
    unique_ids = set(product_ids)

    # ── 2) Per-product identity resolution (read-only). ─────────────────────
    inserts: list[dict] = []
    merges: list[dict] = []
    unresolved: list[dict] = []
    by_signal: dict[str, int] = {}

    for p in products:
        pid = p.get("id")
        source = build_allen_source_record(p)
        resolution = await resolve_identity(collection, source)
        entry = {
            "productId": pid,
            "title": source.get("title"),
            "species": source.get("species"),
            "sourceDatasetId": source.get("sourceDatasetId"),
            "sourceKey": f"allen:{source['sourceDatasetId']}",
            "sourceUrl": source.get("sourceUrl"),
            "doi": source.get("doi"),
        }
        if resolution["matched"] is not None:
            matched = resolution["matched"]
            existing_repos = {
                s.get("repository") for s in (matched.get("sources") or [])
            }
            via = resolution.get("matchedVia") or "unknown"
            by_signal[via] = by_signal.get(via, 0) + 1
            entry.update(
                {
                    "decision": "merge",
                    "existingCanonicalDatasetId": matched.get("canonicalDatasetId"),
                    "existingTitle": matched.get("title"),
                    "matchedVia": via,
                    "existingRepositories": sorted(existing_repos),
                }
            )
            merges.append(entry)
        elif resolution["ambiguous"]:
            entry.update(
                {
                    "decision": "unresolved",
                    "ambiguousCandidates": [
                        {
                            "canonicalDatasetId": c.get("canonicalDatasetId"),
                            "title": c.get("title"),
                            "repositories": sorted(
                                {s.get("repository") for s in (c.get("sources") or [])}
                            ),
                        }
                        for c in resolution["ambiguous"][:5]
                    ],
                }
            )
            unresolved.append(entry)
        else:
            entry["decision"] = "insert"
            inserts.append(entry)

    report = {
        "report_generated_at": _utcnow(),
        "mode": "READ-ONLY identity audit (zero writes)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "PRODUCT_ENUMERATION": {
            "api_total_rows": api_total,
            "expected_products": ALLEN_EXPECTED_PRODUCTS,
            "count_matches_expected": api_total == ALLEN_EXPECTED_PRODUCTS,
            "rows_returned": len(products),
            "unique_product_ids": len(unique_ids),
            "duplicate_ids_in_response": len(product_ids) - len(unique_ids),
            "product_ids": sorted(int(i) for i in unique_ids if i is not None),
        },
        "IDENTITY": {
            "products_checked": len(products),
            "would_insert": len(inserts),
            "would_merge": len(merges),
            "unresolved_ambiguous": len(unresolved),
            "matched_via": by_signal,
        },
        "DATABASE_SNAPSHOT": {
            "production_catalog_total_before": production_total,
            "read_only": True,
        },
        "MERGE_CANDIDATES": merges,
        "UNRESOLVED": unresolved,
        "INSERT_LIST": inserts,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print(f"[allen-audit] === IDENTITY AUDIT (read-only) ===")
    print(f"[allen-audit] products checked: {len(products)} "
          f"(api total_rows={api_total}, expected={ALLEN_EXPECTED_PRODUCTS}, "
          f"unique ids={len(unique_ids)})")
    print(f"[allen-audit] would_insert={len(inserts)} would_merge={len(merges)} "
          f"unresolved={len(unresolved)} matched_via={by_signal}")
    if merges:
        print("[allen-audit] *** UNEXPECTED MATCHES — STOP before production ingestion ***")
        for m in merges:
            print(f"[allen-audit]   product {m['productId']} -> {m['existingCanonicalDatasetId']} "
                  f"via {m['matchedVia']} (repos: {m['existingRepositories']})")
    else:
        print("[allen-audit] no unexpected cross-repository matches — 64 inserts expected")
    print(f"[allen-audit] report written to {_REPORT_PATH}")

    client.close()
    return 1 if merges else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
