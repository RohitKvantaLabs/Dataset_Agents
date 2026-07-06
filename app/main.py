"""
FastAPI application factory.

Lifecycle
---------
startup  → ensure MongoDB indexes exist
shutdown → close the Motor client connection pool

CORS is configured to allow the MERN frontend origin (adjust
ALLOWED_ORIGINS in config if needed for production).
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.v1.router import router as api_v1_router
from app.config import get_settings
from app.db.indexes import ensure_indexes
from app.db.mongo import get_client

logger = logging.getLogger("neuro_platform.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Run startup tasks before yielding, then cleanup on shutdown.
    """
    logger.info("Starting up — ensuring MongoDB indexes...")
    try:
        await ensure_indexes()
        logger.info("MongoDB indexes verified.")
    except Exception as exc:  # noqa: BLE001
        # Non-fatal on startup: the app can still serve requests; index
        # creation will be retried on next restart.
        logger.error("Index creation failed at startup: %s", exc)

    yield  # <-- application runs here

    logger.info("Shutting down — closing MongoDB connection pool...")
    try:
        get_client().close()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error closing Motor client: %s", exc)


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.APP_NAME,
        description=(
            "Aggregated search and discovery engine for global "
            "neuroscience and neuroimaging datasets."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS — allow the MERN/Next.js frontend and Vercel preview URLs.
    # Tighten allow_origins in production to your specific domain(s).
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],   # tighten in prod
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_v1_router, prefix=settings.API_V1_PREFIX)

    return app


app = create_app()
