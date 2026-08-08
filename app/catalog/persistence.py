"""
Catalog persistence — the dedicated ``neurosearch_dataset_catalog``
collection. This collection is SEPARATE from the production ``datasets``
collection and is never read by the production search pipeline.

Indexes (idempotent):
- unique ``canonicalDatasetId``
- unique ``sourceKeys`` (array of ``repo:sourceDatasetId``) — hard backstop
  against duplicate canonical records for the same source
- ``doi``                (dedup lookup)
- ``provenance.identity.sourceUrlNorm``  (dedup lookup)
- ``provenance.identity.titleNorm``      (multi-field candidate lookup)
"""

from __future__ import annotations

import logging

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo.errors import DuplicateKeyError

from app.catalog.dedup import evaluate_identity, merge_source_into_canonical
from app.catalog.normalize import canonical_record_from_source
from app.catalog.schema import CATALOG_COLLECTION, validate_canonical_record

logger = logging.getLogger("neuro_platform.catalog.persistence")


async def ensure_indexes(db) -> None:
    """Idempotent index creation on the dedicated catalog collection."""
    coll = db[CATALOG_COLLECTION]
    await coll.create_index([("canonicalDatasetId", 1)], unique=True, name="uniq_canonical_id")
    await coll.create_index([("sourceKeys", 1)], unique=True, name="uniq_source_keys")
    await coll.create_index([("doi", 1)], name="idx_doi")
    await coll.create_index(
        [("provenance.identity.sourceUrlNorm", 1)],
        name="idx_source_url_norm",
    )
    await coll.create_index(
        [("provenance.identity.titleNorm", 1)],
        name="idx_title_norm",
    )


def get_collection(db) -> AsyncIOMotorCollection:
    return db[CATALOG_COLLECTION]


async def resolve_identity(collection, source: dict) -> dict:
    """Resolve the incoming source against existing canonical records using
    the identity priority (DOI → cross-ref → URL → source key → title
    candidates). Matching is decided by the pure ``evaluate_identity``.

    Returns::

        {
          "matched": dict | None,   # strongest confident match
          "matchedVia": str | None,
          "ambiguous": list[dict],  # candidates that matched title but NOT
                                     # the strong threshold (never merged)
        }
    """
    from app.catalog.dedup import extract_cross_references, source_identity

    ident = source_identity(source)
    candidates: list[dict] = []
    seen_ids: set[str] = set()

    async def _consider(doc: dict | None) -> None:
        if not doc:
            return
        doc_id = str(doc.get("_id"))
        if doc_id in seen_ids:
            return
        seen_ids.add(doc_id)
        candidates.append(doc)

    # 1) DOI
    if ident["doi"]:
        await _consider(await collection.find_one({"doi": ident["doi"]}))

    # 2) Cross-references to other repositories
    for ref in extract_cross_references(source):
        key = f"{ref['repository']}:{ref['sourceDatasetId']}"
        await _consider(await collection.find_one({"sourceKeys": key}))

    # 3) Canonical source URL
    if ident["sourceUrlNorm"]:
        await _consider(
            await collection.find_one(
                {"provenance.identity.sourceUrlNorm": ident["sourceUrlNorm"]}
            )
        )

    # 4) Same repository source key
    await _consider(await collection.find_one({"sourceKeys": ident["sourceKey"]}))

    # 5) Multi-field candidates — exact normalized-title lookup, then the
    #    conservative strong-match check runs in evaluate_identity.
    if ident["titleNorm"]:
        cursor = collection.find({"provenance.identity.titleNorm": ident["titleNorm"]}).limit(20)
        async for doc in cursor:
            await _consider(doc)

    # Score candidates by match strength (pure rule); collect ambiguous ones.
    best: dict | None = None
    best_strength: int | None = None
    best_via: str | None = None
    ambiguous: list[dict] = []
    order = {"doi": 0, "cross_reference": 1, "source_url": 2, "source_key": 3, "multi_field": 4}
    for candidate in candidates:
        eval_result = evaluate_identity(candidate, source)
        if eval_result["match"]:
            via = eval_result.get("matchedVia") or "source_key"
            strength = order.get(via, 99)
            if best is None or strength < best_strength:
                best = candidate
                best_strength = strength
                best_via = via
        elif eval_result["ambiguous"]:
            ambiguous.append(candidate)
    return {
        "matched": best,
        "matchedVia": best_via,
        "ambiguous": ambiguous,
    }


async def upsert_canonical(collection, source: dict) -> dict:
    """Insert a new canonical record OR merge the source into the existing
    canonical record (dedup before insert — spec §7 step 5).

    Returns::

        {
          "action": "inserted" | "merged",
          "canonicalDatasetId": str,
          "matchedVia": str,
          "ambiguous": int,          # candidates intentionally NOT merged
          "ambiguousCandidates": list[dict],
        }
    """
    resolution = await resolve_identity(collection, source)
    existing = resolution["matched"]
    if existing is not None:
        matched_via = resolution["matchedVia"] or "source_key"
        updated = merge_source_into_canonical(existing, source, matched_via)
        await collection.replace_one(
            {"_id": existing["_id"]},
            updated,
        )
        return {
            "action": "merged",
            "canonicalDatasetId": updated.get("canonicalDatasetId"),
            "matchedVia": matched_via,
            "ambiguous": len(resolution["ambiguous"]),
            "ambiguousCandidates": resolution["ambiguous"],
        }

    record = canonical_record_from_source(source)
    # Validate the normalized canonical record BEFORE insert (spec §7 step 4).
    validation_errors = validate_canonical_record(record)
    if validation_errors:
        return {
            "action": "invalid",
            "canonicalDatasetId": record.get("canonicalDatasetId"),
            "matchedVia": None,
            "ambiguous": 0,
            "ambiguousCandidates": [],
            "errors": validation_errors,
        }
    try:
        await collection.insert_one(record)
    except DuplicateKeyError:
        # Race or backstop collision → re-resolve and merge.
        resolution = await resolve_identity(collection, source)
        existing = resolution["matched"]
        if existing is not None:
            matched_via = resolution["matchedVia"] or "source_key"
            updated = merge_source_into_canonical(existing, source, matched_via)
            await collection.replace_one({"_id": existing["_id"]}, updated)
            return {
                "action": "merged",
                "canonicalDatasetId": updated.get("canonicalDatasetId"),
                "matchedVia": matched_via,
                "ambiguous": len(resolution["ambiguous"]),
                "ambiguousCandidates": resolution["ambiguous"],
            }
        raise
    return {
        "action": "inserted",
        "canonicalDatasetId": record.get("canonicalDatasetId"),
        "matchedVia": "new",
        "ambiguous": len(resolution["ambiguous"]),
        "ambiguousCandidates": resolution["ambiguous"],
    }
