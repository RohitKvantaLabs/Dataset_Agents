"""
app/db/indexes.py — MongoDB index creation helpers.

Called automatically on application startup via main.py lifespan.
Uses create_index which is idempotent — safe to call on every boot.
"""
import logging

from app.db.mongo import get_db
from app.db.repositories.dataset_repository import COLLECTION_NAME

logger = logging.getLogger("neuro_platform.db.indexes")


async def ensure_indexes() -> None:
    """Create all required MongoDB indexes if they don't already exist."""
    db = get_db()
    collection = db[COLLECTION_NAME]

    # Primary dedup key — also the upsert key for atomic concurrent writes.
    await collection.create_index(
        [("source", 1), ("source_id", 1)],
        unique=True,
        name="uniq_source_source_id",
    )
    await collection.create_index([("url", 1)], name="url_lookup")
    await collection.create_index(
        [("trust_tier", 1), ("last_verified_at", 1)],
        name="verification_cadence",
    )

    # ponytail: indexes for structured filter queries in searchDatasets().
    await collection.create_index([("modality", 1)], name="modality_filter")
    await collection.create_index([("species", 1)], name="species_filter")
    await collection.create_index([("keywords", 1)], name="keywords_filter")

    # ponytail: text index — replaces the unanchored regex full-scan on raw_query fallback.
    await collection.create_index(
        [("title", "text"), ("description", "text"), ("keywords", "text")],
        weights={"title": 10, "keywords": 5, "description": 1},
        name="dataset_text_search",
    )

    # Phase 2 (§2.14) — repository retrieval publication indexes (additive).
    await collection.create_index([("doi", 1)], name="doi_lookup", sparse=True)
    await collection.create_index([("provenance.harvested_at", -1)], name="provenance_harvested")
    await collection.create_index([("access_tier", 1)], name="access_tier_filter")

    logger.info("MongoDB indexes verified/created on collection=%s", COLLECTION_NAME)
