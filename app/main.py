"""
FastAPI application factory.

Lifecycle
---------
startup  → ensure MongoDB indexes (idempotent)
shutdown → close the Motor client connection pool

CORS is configured to allow the MERN frontend origin (adjust
ALLOWED_ORIGINS in config if needed for production).
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from app.api.v1.router import router as api_v1_router
from app.config import get_settings
from app.core.exceptions import register_exception_handlers
from app.db.indexes import ensure_indexes
from app.db.mongo import get_client

logger = logging.getLogger("neuro_platform.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ponytail: ensure indexes on startup — idempotent, fast after first run.
    try:
        await ensure_indexes()
    except Exception as exc:
        logger.error("Index creation failed — app will run without indexes: %s", exc)
    yield
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

    # CORS — read allow_origins from ALLOWED_ORIGINS config.
    # allow_origins and allow_credentials=True are compatible only when
    # origins are explicitly listed (browsers reject ["*"] with credentials).
    allowed = list(settings.ALLOWED_ORIGINS)
    if not allowed:
        allowed = ["https://neuro-frontend-two.vercel.app"]

    # Guardrail: reject any wildcard origin before middleware init.
    # A wildcard combined with allow_credentials=True is silently rejected
    # by browsers but can still create a false sense of security; we fail
    # hard at startup instead of relying on browser-side enforcement.
    if "*" in allowed:
        raise RuntimeError(
            "CORS allow_origins must not contain '*'. "
            "Explicit origins are required when allow_credentials=True. "
            f"Got: {allowed}"
        )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Response compression — gzip for JSON and text responses >= 1 KB.
    # minimum_size is the response-body byte threshold below which compression
    # is skipped (tiny responses like health checks don't benefit).
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    # Register custom exception handlers so UpstreamServiceError surfaces
    # as 502 Bad Gateway instead of a generic 500.
    register_exception_handlers(app)

    app.include_router(api_v1_router, prefix=settings.API_V1_PREFIX)

    return app


app = create_app()
