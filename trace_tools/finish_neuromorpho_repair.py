"""
Finish the NeuroMorpho repair (2026-08-10).

The main repair (delete 21 wrongly-merged docs + re-persist 1,696
contributions) succeeded: canonical 4,619, nm docs 1,690, 0 failures.

One cleanup bug remained: the provenance.sourceUrlNorm normalization used
``replace_one`` with a PROJECTED document (only _id/sources/provenance), so
the first doc was overwritten without canonicalDatasetId/sourceKeys/
rawMetadata. This script:

1. Restores that one doc (``6a79ae9e47ab124c3bab78de`` /
   ``neuromorpho:Wearne_Hof:pmid:12204204``) from the verified corpus using
   the exact production pipeline (group -> build_neuromorpho_source_record
   -> canonical_record_from_source -> replace preserving the _id).
2. Completes the provenance.sourceUrlNorm normalization on every other nm
   doc using ``$set`` (never replace_one on a projected doc).
3. Verifies the full integrity contract.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import bson
from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.normalize import (
    build_neuromorpho_source_record,
    canonical_record_from_source,
    group_neuromorpho_neurons,
)
from app.catalog.persistence import get_collection
from app.catalog.schema import CATALOG_COLLECTION, normalize_url_key
from app.config import get_settings

CORPUS = Path(__file__).resolve().parents[2] / "trace_artifacts/repo_investigation_20260810/nm_neurons.jsonl"
CORRUPTED_ID = "6a79ae9e47ab124c3bab78de"
CORRUPTED_SOURCE = "neuromorpho:Wearne_Hof:pmid:12204204"


def _load_corpus() -> list[dict]:
    with open(CORPUS, encoding="utf-8") as f:
        return [json.loads(line) for line in f.read().strip().split("\n")]


async def main() -> None:
    settings = get_settings()
    print(f"DB: {settings.MONGO_DB_NAME}  coll: {CATALOG_COLLECTION}")
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    coll = get_collection(db)

    # ── 1) Rebuild the corrupted doc from the corpus ────────────────────────
    neurons = _load_corpus()
    groups = group_neuromorpho_neurons(neurons)
    del neurons
    group = next(
        (g for g in groups if build_neuromorpho_source_record(g)["sourceDatasetId"] == CORRUPTED_SOURCE),
        None,
    )
    assert group is not None, f"group for {CORRUPTED_SOURCE} not found"

    source = build_neuromorpho_source_record(group)
    record = canonical_record_from_source(source)
    record["_id"] = bson.ObjectId(CORRUPTED_ID)  # preserve the original _id
    print(f"restoring {CORRUPTED_SOURCE} -> {record['canonicalDatasetId']} "
          f"(keys={record['sourceKeys']}, rawMetadata={list(record['rawMetadata'].keys())})")

    existing = await coll.find_one({"_id": bson.ObjectId(CORRUPTED_ID)})
    assert existing is not None, "corrupted doc not found"
    # The corrupted doc was ALSO merged with the refreshed source during the
    # re-persist (matchedVia=doi), so its sources[0] is already the fixed
    # record — but canonicalDatasetId/sourceKeys/rawMetadata are gone.
    await coll.replace_one({"_id": bson.ObjectId(CORRUPTED_ID)}, record)
    print("restored")

    # ── 2) Safe provenance.sourceUrlNorm normalization ($set) ───────────────
    updated = 0
    async for d in coll.find({"sources.repository": "neuromorpho"}, {"sources": 1, "provenance": 1}):
        nm_src = next((s for s in d.get("sources") or [] if s.get("repository") == "neuromorpho"), None)
        if not nm_src or not nm_src.get("sourceUrl"):
            continue
        new_norm = normalize_url_key(nm_src["sourceUrl"]) or ""
        ident = ((d.get("provenance") or {}).get("identity") or {})
        if ident.get("sourceUrlNorm") != new_norm:
            await coll.update_one(
                {"_id": d["_id"]},
                {"$set": {"provenance.identity.sourceUrlNorm": new_norm}},
            )
            updated += 1
    print(f"provenance.sourceUrlNorm normalized on {updated} docs")

    # ── 3) Verification ─────────────────────────────────────────────────────
    total = await coll.count_documents({})
    nm_docs = await coll.count_documents({"sources.repository": "neuromorpho"})
    null_cid = await coll.count_documents({"canonicalDatasetId": None})
    print(f"canonical total: {total}  nm docs: {nm_docs}  null canonicalDatasetId: {null_cid}")

    nm_sources = 0
    keys: set[str] = set()
    raw_ok = 0
    stale_prov = 0
    async for d in coll.find({"sources.repository": "neuromorpho"}, {"sourceKeys": 1, "sources": 1, "rawMetadata": 1, "provenance": 1}):
        nm = [x for x in d.get("sources") or [] if x.get("repository") == "neuromorpho"]
        nm_sources += len(nm)
        keys.update(d.get("sourceKeys") or [])
        rm = d.get("rawMetadata") or {}
        if isinstance(rm, dict) and rm.get("neuromorpho"):
            raw_ok += 1
        ident = ((d.get("provenance") or {}).get("identity") or {})
        first_nm = next((x for x in nm if x.get("sourceUrl")), None)
        if first_nm and normalize_url_key(first_nm["sourceUrl"]) != ident.get("sourceUrlNorm"):
            stale_prov += 1
    print(f"nm source records: {nm_sources}  distinct sourceKeys: {len(keys)}")
    print(f"rawMetadata.neuromorpho: {raw_ok}/{nm_docs}")
    print(f"stale provenance.sourceUrlNorm remaining: {stale_prov}")
    for repo in ("openneuro", "dandi", "nemar"):
        print(f"  {repo} docs: {await coll.count_documents({'sources.repository': repo})}")
    print(f"production datasets collection: {await db[settings.MONGO_DATASET_COLLECTION].count_documents({})}")

    # Integrity assertions
    assert total == 4619, f"expected 4619 canonical, got {total}"
    assert null_cid == 0, "null canonicalDatasetId still present"
    assert nm_sources == 1696, f"expected 1696 nm source records, got {nm_sources}"
    assert len(keys) == 1696, f"expected 1696 unique sourceKeys, got {len(keys)}"
    assert raw_ok == nm_docs, "rawMetadata.neuromorpho coverage < 100%"
    assert stale_prov == 0, f"{stale_prov} docs with stale sourceUrlNorm"
    # no wrongly-merged docs remain
    wrong = 0
    async for d in coll.find({"sources.repository": "neuromorpho"}, {"sources": 1}):
        nm = [x for x in d.get("sources") or [] if x.get("repository") == "neuromorpho"]
        if len(nm) <= 1:
            continue
        arch = {x["sourceDatasetId"].split(":")[1] for x in nm}
        pmids = {(x.get("snapshot") or {}).get("pmid") for x in nm}
        if len(arch) == 1 and len(pmids) > 1:
            wrong += 1
    print(f"wrongly-merged docs remaining: {wrong}")
    assert wrong == 0

    print("\nREPAIR FINALIZED: 4,619 canonical | 1,696 sources | 1,696 unique keys | 100% rawMetadata")
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
