import asyncio
import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends

from app.agents.verification_agent import VerificationAgent
from app.config import get_settings
from app.core.security import require_cron_secret
from app.db.repositories.dataset_repository import (
    bulk_update_verification_status,
    find_datasets_for_reverification,
)
from app.models.dataset import TrustTier

logger = logging.getLogger("neuro_platform.api.cron")

router = APIRouter(prefix="/cron", dependencies=[Depends(require_cron_secret)])
MAX_CONCURRENT_REVALIDATIONS = 10


@router.get("/reverify-links")
async def reverify_links() -> dict[str, int]:
    """
    Re-check stale verified links for Vercel Cron.
    """
    settings = get_settings()
    now = datetime.now(timezone.utc)
    stale_before = now - timedelta(days=settings.CRON_STALE_THRESHOLD_DAYS)

    datasets = await find_datasets_for_reverification(
        stale_before=stale_before,
        limit=settings.CRON_BATCH_SIZE,
    )

    # Parallel revalidation — all HTTP checks fire simultaneously.
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REVALIDATIONS)

    async def revalidate_one(dataset):
        async with semaphore:
            return await VerificationAgent().revalidate(dataset)

    results = await asyncio.gather(
        *(revalidate_one(dataset) for dataset in datasets),
        return_exceptions=True,
    )

    records: list[tuple[Dataset, TrustTier, datetime]] = []
    verified = 0
    stale = 0
    errors = 0

    for dataset, trust_tier_or_err in zip(datasets, results, strict=False):
        if isinstance(trust_tier_or_err, Exception):
            errors += 1
            logger.warning(
                "Cron revalidation failed for source=%s source_id=%s: %s",
                dataset.source,
                dataset.source_id,
                trust_tier_or_err,
            )
            continue
        if trust_tier_or_err == TrustTier.VERIFIED:
            verified += 1
        else:
            stale += 1
        records.append((dataset, trust_tier_or_err, now))

    # Batch update — single bulkWrite round-trip.
    if records:
        await bulk_update_verification_status(records)

    logger.info(
        "Cron reverified %d dataset links: verified=%d stale=%d errors=%d",
        len(datasets),
        verified,
        stale,
        errors,
    )
    return {"checked": len(datasets), "verified": verified, "stale": stale, "errors": errors}
