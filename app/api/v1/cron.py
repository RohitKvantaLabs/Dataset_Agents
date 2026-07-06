import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends

from app.agents.verification_agent import VerificationAgent
from app.config import get_settings
from app.core.security import require_cron_secret
from app.db.repositories.dataset_repository import (
    find_datasets_for_reverification,
    update_verification_status,
)
from app.models.dataset import TrustTier

logger = logging.getLogger("neuro_platform.api.cron")

router = APIRouter(prefix="/cron", dependencies=[Depends(require_cron_secret)])


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

    verified = 0
    stale = 0
    for dataset in datasets:
        trust_tier = await VerificationAgent().revalidate(dataset)
        await update_verification_status(dataset, trust_tier, now)
        if trust_tier == TrustTier.VERIFIED:
            verified += 1
        else:
            stale += 1

    logger.info(
        "Cron reverified %d dataset links: verified=%d stale=%d",
        len(datasets),
        verified,
        stale,
    )
    return {"checked": len(datasets), "verified": verified, "stale": stale}
