"""
Repository Retrieval & Sync endpoints (§4.6).

    POST /agents/repository-search   require_internal_secret
    POST /agents/repository-sync     require_internal_secret
    GET  /agents/repository-health   require_internal_secret
    GET  /cron/ingest-repositories   require_cron_secret

All handlers complete synchronously inside the request (per the Python
CLAUDE.md serverless constraint — no BackgroundTasks for anything that
must happen).
"""
import logging
import time
import uuid
from dataclasses import asdict

from fastapi import APIRouter, Depends

from app.config import get_settings
from app.core.security import require_cron_secret, require_internal_secret
from app.connectors.registry import get_circuit_state, get_enabled_sources
from app.db.mongo import get_db
from app.db.repositories.dataset_repository import COLLECTION_NAME
from app.ingestion.quality_pipeline import run_quality_pipeline
from app.ingestion.repository_sync import run_repository_sync
from app.models.dataset import Dataset
from app.models.query_filters import QueryFilters
from app.services.repository_retrieval import aggregate_repository_search
from pydantic import BaseModel

logger = logging.getLogger("neuro_platform.api.repositories")

router = APIRouter()

# In-memory ops tracking for /repository-health (per-serverless-instance,
# best-effort: resets on cold start).
_last_search_at: dict[str, float] = {}
_last_sync_at: dict[str, float] = {}


# ---------------------------------------------------------------------------
# POST /agents/repository-search
# ---------------------------------------------------------------------------
class RepositorySearchRequest(BaseModel):
    query: str
    filters: dict          # QueryFilters JSON (Node already has it from parse-query)
    sources: list[str] | None = None
    limit_per_source: int | None = None


class RepositorySearchResponse(BaseModel):
    query_id: str
    sources_queried: list[str]
    total_found: int
    elapsed_ms: int
    datasets: list[Dataset]


@router.post(
    "/agents/repository-search",
    response_model=RepositorySearchResponse,
    dependencies=[Depends(require_internal_secret)],
)
async def repository_search(payload: RepositorySearchRequest) -> RepositorySearchResponse:
    """
    Run enabled connectors in parallel → aggregate → quality pipeline
    stages 1–6 (no publish) → return scored/deduped Dataset[].

    Synchronous and bounded (limit_per_source, REPO_MAX_PAGES).
    """
    settings = get_settings()
    filters = QueryFilters.model_validate({**payload.filters, "raw_query": payload.query})

    aggregate = await aggregate_repository_search(
        query=payload.query,
        filters=filters,
        sources=payload.sources,
        limit_per_source=payload.limit_per_source,
    )

    for source in aggregate.sources_queried:
        _last_search_at[source] = time.time()

    pipeline = await run_quality_pipeline(
        aggregate.records, filters, publish=False, discovery_method="repository_search"
    )

    return RepositorySearchResponse(
        query_id=uuid.uuid4().hex,
        sources_queried=aggregate.sources_queried,
        total_found=len(pipeline.datasets),
        elapsed_ms=pipeline.elapsed_ms,
        datasets=pipeline.datasets,
    )


# ---------------------------------------------------------------------------
# POST /agents/repository-sync  +  GET /cron/ingest-repositories
# ---------------------------------------------------------------------------
class RepositorySyncRequest(BaseModel):
    source: str | None = None          # None → all enabled sources
    limit_per_source: int | None = None
    embed: bool = True


class RepositorySyncResponse(BaseModel):
    results: dict[str, dict]           # source → PipelineResult (asdict)


@router.post(
    "/agents/repository-sync",
    response_model=RepositorySyncResponse,
    dependencies=[Depends(require_internal_secret)],
)
async def repository_sync(payload: RepositorySyncRequest) -> RepositorySyncResponse:
    """Batch fetch + quality pipeline publish for one/all enabled sources."""
    results = await run_repository_sync(
        source=payload.source,
        limit_per_source=payload.limit_per_source,
        embed=payload.embed,
    )
    for source in results:
        _last_sync_at[source] = time.time()
    return RepositorySyncResponse(results={k: asdict(v) for k, v in results.items()})


@router.get(
    "/cron/ingest-repositories",
    dependencies=[Depends(require_cron_secret)],
)
async def ingest_repositories() -> dict:
    """Scheduled full sync wrapper over repository-sync (Vercel cron)."""
    results = await run_repository_sync()
    for source in results:
        _last_sync_at[source] = time.time()
    return {"results": {k: asdict(v) for k, v in results.items()}}


# ---------------------------------------------------------------------------
# GET /agents/repository-health
# ---------------------------------------------------------------------------
class RepositoryHealthItem(BaseModel):
    source: str
    circuit_state: str
    last_search_at: float | None = None
    last_sync_at: float | None = None
    record_count: int


class RepositoryHealthResponse(BaseModel):
    sources: list[RepositoryHealthItem]


@router.get(
    "/agents/repository-health",
    response_model=RepositoryHealthResponse,
    dependencies=[Depends(require_internal_secret)],
)
async def repository_health() -> RepositoryHealthResponse:
    """Per-source ops snapshot: circuit state, last activity, record count."""
    db = get_db()
    items: list[RepositoryHealthItem] = []
    for source in get_enabled_sources():
        record_count = 0
        try:
            record_count = await db[COLLECTION_NAME].count_documents({"source": source})
        except Exception as exc:  # noqa: BLE001 — health must never raise
            logger.warning("repository-health count failed for %r: %s", source, exc)
        items.append(
            RepositoryHealthItem(
                source=source,
                circuit_state=get_circuit_state(source),
                last_search_at=_last_search_at.get(source),
                last_sync_at=_last_sync_at.get(source),
                record_count=record_count,
            )
        )
    return RepositoryHealthResponse(sources=items)
