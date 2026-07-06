import logging
from datetime import datetime, timezone

from pymongo import ReturnDocument

from app.db.mongo import get_db
from app.models.dataset import Dataset, TrustTier

logger = logging.getLogger("neuro_platform.db.dataset_repository")

COLLECTION_NAME = "datasets"


async def upsert_dataset(dataset: Dataset) -> None:
    """
    Single atomic operation - update_one(upsert=True) keyed on
    (source, source_id). This is deliberately NOT a read-then-write:
    two fallback workers finding the same dataset at the same time will
    both hit this, and Mongo resolves it atomically instead of racing.
    Pair this with a unique index on {source, source_id} (see indexes.py)
    as a hard backstop.
    """
    db = get_db()
    now = datetime.now(timezone.utc)

    payload = dataset.model_dump(exclude={"id"}, exclude_none=False, mode="json")
    payload["updated_at"] = now.isoformat()

    await db[COLLECTION_NAME].find_one_and_update(
        {"source": dataset.source, "source_id": dataset.source_id},
        {
            "$set": payload,
            "$setOnInsert": {"ingested_at": now.isoformat()},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    logger.info("Upserted dataset source=%s source_id=%s", dataset.source, dataset.source_id)


async def upsert_many(datasets: list[Dataset]) -> int:
    count = 0
    for ds in datasets:
        await upsert_dataset(ds)
        count += 1
    return count


async def find_datasets_for_reverification(stale_before: datetime, limit: int) -> list[Dataset]:
    """
    Return verified/stale datasets whose links are due for scheduled re-check.
    Unverified fallback candidates are intentionally excluded.
    """
    db = get_db()
    cursor = (
        db[COLLECTION_NAME]
        .find(
            {
                "trust_tier": {"$in": [TrustTier.VERIFIED.value, TrustTier.STALE.value]},
                "$or": [
                    {"last_verified_at": None},
                    {"last_verified_at": {"$lt": stale_before.isoformat()}},
                    {"last_verified_at": {"$exists": False}},
                ],
            }
        )
        .limit(limit)
    )
    docs = await cursor.to_list(length=limit)
    return [Dataset.model_validate(doc) for doc in docs]


async def update_verification_status(dataset: Dataset, trust_tier: TrustTier, verified_at: datetime) -> None:
    """
    Update the existing document after cron revalidation. This is deliberately
    not an upsert; the cron job only processes documents already in MongoDB.
    """
    db = get_db()
    await db[COLLECTION_NAME].update_one(
        {"source": dataset.source, "source_id": dataset.source_id},
        {
            "$set": {
                "trust_tier": trust_tier.value,
                "last_verified_at": verified_at.isoformat(),
                "updated_at": verified_at.isoformat(),
            }
        },
    )
