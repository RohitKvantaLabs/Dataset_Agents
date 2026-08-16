"""
Repair pass for the NeuroMorpho full ingestion (2026-08-10).

BLOCKER discovered after the full run: 27 distinct Archive × Publication
contributions were wrongly collapsed into 21 canonical datasets because the
generic identity resolver matched on ``sourceUrl`` — and every contribution
in an archive shared ``https://neuromorpho.org/archive/{archive}``. A further
3 within-archive DOI collisions (Ascoli) merged via the DOI signal.

Fix (already applied to app/catalog/normalize.py):
- ``sourceUrl`` is now contribution-UNIQUE (``.../archive/{archive}/pmid/{pmid}``
  or ``.../archive/{archive}/doi/{doi}``) so the source_url signal can no
  longer collide distinct publications within one archive.
- ``_clear_within_archive_doi_artifacts()`` clears DOIs shared by 2+ distinct
  PMID groups inside ONE archive (archive-level artifacts), while DOI-only
  groups and cross-archive same-publication DOIs are preserved.

This script:
1. Loads the verified full corpus (nm_neurons.jsonl — 298,339 neurons).
2. Groups with the FIXED code → 1,696 contributions.
3. Deletes the 21 wrongly-merged canonical docs (same archive, multiple
   distinct PMIDs in one canonical doc).
4. Re-persists all 1,696 contributions through the existing
   resolve_identity → upsert_canonical pipeline (idempotent: correct docs
   are refreshed via source_key; the 27 absorbed sources become their own
   canonical datasets; the 6 defensible cross-archive merges re-merge via
   DOI).

Target state: canonical = 2,929 + 1,696 - 6 = 4,619.
ZERO asset/file crawling; ZERO writes outside neurosearch_dataset_catalog.
"""
from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.normalize import (
    build_neuromorpho_source_record,
    group_neuromorpho_neurons,
)
from app.catalog.persistence import get_collection, upsert_canonical
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

CORPUS = Path(__file__).resolve().parents[2] / "trace_artifacts/repo_investigation_20260810/nm_neurons.jsonl"


def _load_corpus() -> list[dict]:
    with open(CORPUS, encoding="utf-8") as f:
        return [json.loads(line) for line in f.read().strip().split("\n")]


async def _wrong_merged_docs(coll):
    """Canonical docs carrying 2+ neuromorpho sources where the sources are
    in the SAME archive but have DISTINCT PMIDs — the wrongly-collapsed set."""
    wrong = []
    async for d in coll.find({"sources.repository": "neuromorpho"}, {"canonicalDatasetId": 1, "sources": 1}):
        nm = [x for x in d.get("sources") or [] if x.get("repository") == "neuromorpho"]
        if len(nm) <= 1:
            continue
        archives = {x["sourceDatasetId"].split(":")[1] for x in nm}
        pmids = {(x.get("snapshot") or {}).get("pmid") for x in nm}
        if len(archives) == 1 and len(pmids) > 1:
            wrong.append(d["canonicalDatasetId"])
    return wrong


async def main() -> None:
    settings = get_settings()
    print(f"DB: {settings.MONGO_DB_NAME}  coll: {CATALOG_COLLECTION}")
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    coll = get_collection(db)

    # ── 0) Pre-state ────────────────────────────────────────────────────────
    before_total = await coll.count_documents({})
    before_nm_docs = await coll.count_documents({"sources.repository": "neuromorpho"})
    print(f"before: canonical={before_total} nm_docs={before_nm_docs}")

    # ── 1) Group corpus with FIXED code ─────────────────────────────────────
    t0 = time.monotonic()
    neurons = _load_corpus()
    print(f"corpus neurons: {len(neurons)}")
    groups = group_neuromorpho_neurons(neurons)
    del neurons
    pmid_groups = sum(1 for g in groups if g.get("pmid"))
    doi_groups = sum(1 for g in groups if not g.get("pmid") and g.get("doi"))
    print(f"groups: {len(groups)} (PMID={pmid_groups} DOI={doi_groups})")
    assert len(groups) == 1696 and pmid_groups == 1642 and doi_groups == 54, "corpus grouping invariant broken"

    # ── 2) Identify + delete the wrongly-merged docs ────────────────────────
    wrong = await _wrong_merged_docs(coll)
    print(f"wrongly-merged docs to delete: {len(wrong)}")
    for cid in wrong:
        print(f"  delete {cid}")
    if wrong:
        await coll.delete_many({"canonicalDatasetId": {"$in": wrong}})
    print(f"deleted {len(wrong)} wrongly-merged docs")

    # ── 3) Re-persist all contributions (idempotent upsert) ────────────────
    inserted = 0
    merged = 0
    failed = 0
    matched_via: dict[str, int] = {}
    failure_details: list[str] = []
    for i, g in enumerate(groups, 1):
        try:
            source = build_neuromorpho_source_record(g)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failure_details.append(f"{g.get('archive')}:{g.get('pmid') or g.get('doi')}: normalize: {exc}")
            continue
        try:
            result = await upsert_canonical(coll, source)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failure_details.append(f"{source.get('sourceDatasetId')}: upsert: {exc}")
            continue
        if result.get("action") == "inserted":
            inserted += 1
        elif result.get("action") == "merged":
            merged += 1
            via = result.get("matchedVia") or "unknown"
            matched_via[via] = matched_via.get(via, 0) + 1
        else:  # invalid
            failed += 1
            failure_details.append(f"{source.get('sourceDatasetId')}: invalid: {result.get('errors')}")
        if i % 400 == 0:
            print(f"  progress {i}/{len(groups)} inserted={inserted} merged={merged} failed={failed}")

    elapsed = round(time.monotonic() - t0, 1)
    print(f"persist done: inserted={inserted} merged={merged} failed={failed} matched_via={matched_via} elapsed={elapsed}s")
    if failure_details:
        print("FAILURES:")
        for f in failure_details[:20]:
            print("  ", f)

    # ── 3.5) Normalize provenance identity on every nm doc ─────────────────
    # merge_source_into_canonical refreshes sources[] but leaves the OLD
    # bare-archive provenance.identity.sourceUrlNorm on docs created before
    # the fix. Recompute it from the first neuromorpho source's (now
    # contribution-unique) sourceUrl so doc-level identity is consistent.
    prov_updated = 0
    async for d in coll.find({"sources.repository": "neuromorpho"}, {"sources": 1, "provenance": 1}):
        nm_src = next((s for s in d.get("sources") or [] if s.get("repository") == "neuromorpho"), None)
        if not nm_src or not nm_src.get("sourceUrl"):
            continue
        from app.catalog.schema import normalize_url_key

        new_norm = normalize_url_key(nm_src["sourceUrl"]) or ""
        prov = d.get("provenance") or {}
        ident = prov.get("identity") or {}
        if ident.get("sourceUrlNorm") != new_norm:
            ident["sourceUrlNorm"] = new_norm
            prov["identity"] = ident
            d["provenance"] = prov
            await coll.replace_one({"_id": d["_id"]}, d)
            prov_updated += 1
    print(f"provenance.sourceUrlNorm normalized on {prov_updated} docs")

    # ── 4) Post-state ───────────────────────────────────────────────────────
    after_total = await coll.count_documents({})
    after_nm_docs = await coll.count_documents({"sources.repository": "neuromorpho"})
    nm_sources = 0
    keys: set[str] = set()
    raw_ok = 0
    async for d in coll.find({"sources.repository": "neuromorpho"}, {"sourceKeys": 1, "sources": 1, "rawMetadata": 1}):
        nm = [x for x in d.get("sources") or [] if x.get("repository") == "neuromorpho"]
        nm_sources += len(nm)
        keys.update(d.get("sourceKeys") or [])
        rm = d.get("rawMetadata") or {}
        if isinstance(rm, dict) and rm.get("neuromorpho"):
            raw_ok += 1
    print(f"after: canonical={after_total} nm_docs={after_nm_docs}")
    print(f"nm source records: {nm_sources}  distinct doc-level sourceKeys: {len(keys)}")
    print(f"rawMetadata.neuromorpho present: {raw_ok}/{after_nm_docs}")
    for repo in ("openneuro", "dandi", "nemar"):
        print(f"  {repo} docs: {await coll.count_documents({'sources.repository': repo})}")
    print(f"production datasets collection: {await db[settings.MONGO_DATASET_COLLECTION].count_documents({})}")

    # ── 5) Verify no wrongly-merged docs remain ─────────────────────────────
    wrong_after = await _wrong_merged_docs(coll)
    print(f"wrongly-merged docs remaining: {len(wrong_after)}")
    assert wrong_after == [], f"BLOCKER: {len(wrong_after)} wrong merges remain: {wrong_after[:5]}"
    assert nm_sources == 1696, f"expected 1696 nm source records, got {nm_sources}"
    assert len(keys) == 1696, f"expected 1696 unique sourceKeys, got {len(keys)}"
    assert raw_ok == after_nm_docs, "rawMetadata.neuromorpho coverage < 100%"
    assert after_total == 4619, f"expected canonical 4619, got {after_total}"

    print("\nREPAIR COMPLETE: 4,619 canonical (2,929 + 1,696 - 6 legit merges)")
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
