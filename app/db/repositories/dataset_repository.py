import logging
from datetime import datetime, timezone

from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import BulkWriteError

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

    payload = dataset.model_dump(exclude={"id", "ingested_at"}, exclude_none=False, mode="json")
    payload["updated_at"] = now.isoformat()

    doc = await db[COLLECTION_NAME].find_one_and_update(
        {"source": dataset.source, "source_id": dataset.source_id},
        {
            "$set": payload,
            "$setOnInsert": {"ingested_at": now.isoformat()},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    if doc and "_id" in doc:
        dataset.id = str(doc["_id"])
    logger.info("Upserted dataset source=%s source_id=%s id=%s", dataset.source, dataset.source_id, dataset.id)


async def bulk_upsert(datasets: list[Dataset]) -> int:
    """
    Batched upsert via a single MongoDB bulkWrite command.

    All upserts are sent in ONE round-trip instead of N individual
    operations (parallel or sequential).  Uses `updateOne` with
    ``upsert=True`` keyed on (source, source_id) — same atomic
    semantics as `upsert_dataset` but without the per-document
    response overhead.

    Chunks at 100 documents to avoid oversized write commands on
    large ingestion runs.
    """
    db = get_db()
    now = datetime.now(timezone.utc)

    chunk_size = 100
    total = 0

    for i in range(0, len(datasets), chunk_size):
        chunk = datasets[i : i + chunk_size]
        operations = []

        for ds in chunk:
            payload = ds.model_dump(
                exclude={"id", "ingested_at"}, exclude_none=False, mode="json"
            )
            payload["updated_at"] = now.isoformat()

            operations.append(
                UpdateOne(
                    filter={"source": ds.source, "source_id": ds.source_id},
                    update={
                        "$set": payload,
                        "$setOnInsert": {"ingested_at": now.isoformat()},
                    },
                    upsert=True,
                )
            )

        if operations:
            try:
                await db[COLLECTION_NAME].bulk_write(operations, ordered=False)
                total += len(chunk)
            except BulkWriteError as exc:
                failed = len(exc.details.get("writeErrors", []))
                total += len(chunk) - failed
                logger.error("bulk_upsert: %d writes failed in a chunk: %s", failed, exc.details)

    logger.info("bulk_upsert: %d datasets in %d chunks", len(datasets), (len(datasets) + chunk_size - 1) // chunk_size)
    return total


async def upsert_many(datasets: list[Dataset]) -> int:
    # ponytail: parallel upserts — N separate network roundtrips fire at once.
    import asyncio
    await asyncio.gather(*[upsert_dataset(ds) for ds in datasets])
    return len(datasets)


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


async def bulk_update_verification_status(
    records: list[tuple[Dataset, TrustTier, datetime]],
) -> None:
    """
    Batch-update verification statuses for multiple datasets in a single
    MongoDB bulkWrite command.

    Each element of *records* is a tuple of
    (dataset, trust_tier, verified_at).  The bulk write replaces what
    would otherwise be N individual ``update_one`` calls.

    Chunks at 100 documents to avoid oversized write commands.
    """
    db = get_db()
    chunk_size = 100

    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        operations = []

        for dataset, trust_tier, verified_at in chunk:
            iso = verified_at.isoformat()
            operations.append(
                UpdateOne(
                    filter={
                        "source": dataset.source,
                        "source_id": dataset.source_id,
                    },
                    update={
                        "$set": {
                            "trust_tier": trust_tier.value,
                            "last_verified_at": iso,
                            "updated_at": iso,
                        }
                    },
                )
            )

        if operations:
            await db[COLLECTION_NAME].bulk_write(operations, ordered=False)
