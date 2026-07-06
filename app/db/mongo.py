"""
MongoDB Atlas connection management.

Serverless note: Vercel may spin up a fresh function instance per request
(or reuse a warm one). We keep a module-level client so a warm instance
reuses its connection pool instead of opening a new one every call.
"""
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import get_settings

_client: AsyncIOMotorClient | None = None


def get_client() -> AsyncIOMotorClient:
    global _client
    if _client is None:
        settings = get_settings()
        _client = AsyncIOMotorClient(
            settings.MONGO_URI,
            serverSelectionTimeoutMS=8000,  # fail fast rather than hang a serverless request
        )
    return _client


def get_db() -> AsyncIOMotorDatabase:
    settings = get_settings()
    return get_client()[settings.MONGO_DB_NAME]


async def ping() -> bool:
    """Used by /health to confirm Atlas is actually reachable, not just configured."""
    try:
        await get_client().admin.command("ping")
        return True
    except Exception:
        return False


async def close_client() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
