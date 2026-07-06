from fastapi import APIRouter

from app.api.v1 import agents, cron, health

router = APIRouter()
router.include_router(health.router, tags=["health"])
router.include_router(agents.router, tags=["agents"])
router.include_router(cron.router, tags=["cron"])
