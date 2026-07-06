"""
Publishes finished fallback-search results to Redis. Node is expected to
subscribe to `{REDIS_RESULT_CHANNEL_PREFIX}{query_id}` and forward the
message to the right frontend connection (websocket/SSE) - correlating
by whatever query_id Node originally sent us.
"""
import json
import logging

import redis.asyncio as redis

from app.config import get_settings

logger = logging.getLogger("neuro_platform.redis")

_redis_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)
    return _redis_client


async def publish_fallback_result(query_id: str, payload: dict) -> None:
    settings = get_settings()
    channel = f"{settings.REDIS_RESULT_CHANNEL_PREFIX}{query_id}"
    message = json.dumps(payload, default=str)
    await get_redis().publish(channel, message)
    logger.info("Published fallback result to channel=%s (%d bytes)", channel, len(message))
