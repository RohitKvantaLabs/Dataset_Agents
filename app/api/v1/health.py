from fastapi import APIRouter

from app.db.mongo import ping

router = APIRouter()


@router.get("/health")
async def health():
    db_ok = await ping()
    return {
        "status": "ok" if db_ok else "degraded",
        "mongo_connected": db_ok,
    }
