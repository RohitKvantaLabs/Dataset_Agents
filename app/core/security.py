import hmac

from fastapi import Header, HTTPException, status

from app.config import get_settings


async def require_internal_secret(x_internal_secret: str = Header(...)) -> None:
    """
    Every endpoint Node calls must be protected by this. Not JWT - Node
    owns end-user auth entirely. This just stops the endpoint being
    triggerable by anyone who finds the URL (each call costs an LLM call
    and/or web search, so this is a cost control as much as a security one).

    If INTERNAL_API_SECRET is not set in the environment, every call is
    rejected — this is the safe failure mode (deny-by-default).
    """
    settings = get_settings()
    configured_secret = settings.INTERNAL_API_SECRET
    if not configured_secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="INTERNAL_API_SECRET not configured on this instance.",
        )
    if not hmac.compare_digest(x_internal_secret, configured_secret):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal secret.",
        )


async def require_cron_secret(authorization: str = Header(...)) -> None:
    """
    Vercel Cron calls scheduled endpoints with Authorization: Bearer <CRON_SECRET>.
    This is intentionally separate from X-Internal-Secret, which is only for Node.
    """
    settings = get_settings()
    configured_secret = settings.CRON_SECRET
    if not configured_secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="CRON_SECRET not configured on this instance.",
        )

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid cron authorization.",
        )

    if not hmac.compare_digest(token, configured_secret):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid cron authorization.",
        )
