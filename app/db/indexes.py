"""
app/db/indexes.py — MongoDB index creation, called from main.py lifespan.

The actual index definitions live here so they can be called on startup
(ensuring a warm Vercel instance always has indexes) and also run
standalone via scripts/ensure_indexes.py against Atlas before go-live.
"""
import logging

from app.db.mongo import get_db
from app.db.repositories.dataset_repository import COLLECTION_NAME

logger = logging.getLogger("neuro_platform.db.indexes")


async def ensure_indexes() -> None:
    """Create all required MongoDB indexes if they don't already exist.

    Uses ``create_index`` which is idempotent — safe to call on every
    startup. The unique index on (source, source_id) is the hard backstop
    for concurrent fallback writes (the application-level upsert is the
    first line of defence; this is belt-and-suspenders).
    """
    db = get_db()
    collection = db[COLLECTION_NAME]

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
    logger.info("MongoDB indexes verified/created on collection=%s", COLLECTION_NAME)
