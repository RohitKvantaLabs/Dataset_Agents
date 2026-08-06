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
from app.db.repositories.dataset_repository import COLLECTION_NAME, find_datasets_by_identity
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
    stages 1–7 (publish) → re-query Mongo → return canonical documents.

    Canonical persistence contract (§4.6): every repository dataset shown to
    the user must first be persisted into MongoDB (single source of truth).
    Stage 7 bulk-upserts the surviving records idempotently — duplicate
    handling unchanged ((source, source_id) key + DOI/URL canonical merge +
    provenance history). After publication completes, Mongo is re-queried by
    the discovered identities and the response carries the persisted
    canonical documents (``_id``, provenance, quality score) — never
    transient in-memory previews.

    Synchronous and bounded (limit_per_source, REPO_MAX_PAGES). Everything
    is awaited in-request (Vercel-serverless-safe): `bulk_upsert` finishes
    before the response is returned, so any immediate re-query (here or on
    the Node side) observes the writes.
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
        aggregate.records, filters, publish=True, discovery_method="repository_search"
    )

    # Canonical persistence: re-query Mongo by the identities Stage 7 just
    # persisted. bulk_upsert mutates each dataset's (source, source_id) to its
    # canonical identity when a URL/DOI merge re-targets it, so the post-run
    # identities are the authoritative lookup keys.
    seen: set[tuple[str, str]] = set()
    identities: list[tuple[str, str]] = []
    for d in pipeline.datasets:
        key = (d.source, d.source_id)
        if d.source and d.source_id and key not in seen:
            seen.add(key)
            identities.append(key)
    datasets = await find_datasets_by_identity(identities) if identities else []
    # Intentional graceful degradation: on a partial/zero re-query (Stage 7
    # write failure or invisibility), the response is 200 with the subset of
    # canonical docs that WERE persisted — never transient unpersisted
    # previews. Node treats an empty repo pool as "repo tier found nothing"
    # and degrades to the web tier, so no unpersisted dataset is surfaced.
    if len(datasets) != len(identities):
        logger.warning(
            "repository_search: re-query returned %d/%d published identities",
            len(datasets),
            len(identities),
        )

    return RepositorySearchResponse(
        query_id=uuid.uuid4().hex,
        sources_queried=aggregate.sources_queried,
        total_found=len(datasets),
        elapsed_ms=pipeline.elapsed_ms,
        datasets=datasets,
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
