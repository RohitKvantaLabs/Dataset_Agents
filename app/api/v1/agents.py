import hashlib
import logging
import time

from fastapi import APIRouter, Depends, Header

from app.agents.fallback_agent import FallbackAgent, FallbackCandidate
from app.agents.query_understanding_agent import QueryUnderstandingAgent
from app.agents.search_provider import TavilySearchProvider
from app.config import get_settings
from app.core.security import require_internal_secret
from app.ingestion.quality_pipeline import WEB_DISCOVERY_SOURCE, run_quality_pipeline
from app.models.query_filters import ParseQueryRequest, ParseQueryResponse, QueryFilters
from app.models.dataset import Dataset
from app.models.repository_dataset import RepositoryDataset
from app.services.redis_publisher import publish_fallback_result
from pydantic import BaseModel

logger = logging.getLogger("neuro_platform.api.agents")

router = APIRouter(dependencies=[Depends(require_internal_secret)])

# ponytail: module-level parse cache — same query within 5 min returns cached filters.
_parse_cache: dict[str, tuple[QueryFilters, float]] = {}
_PARSE_CACHE_TTL = 300  # seconds


# ---------------------------------------------------------------------
# 1. Query Understanding — BLOCKING. Node waits on this response before
#    it queries Mongo itself.
# ---------------------------------------------------------------------
@router.post("/agents/parse-query", response_model=ParseQueryResponse)
async def parse_query(
    payload: ParseQueryRequest,
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
):
    now = time.time()
    cached = _parse_cache.get(payload.query)
    if cached and now - cached[1] < _PARSE_CACHE_TTL:
        logger.info("parse_query cache hit for query=%r request_id=%s", payload.query, x_request_id)
        return ParseQueryResponse(filters=cached[0])

    agent = QueryUnderstandingAgent()
    filters = agent.parse(payload.query)
    usage = getattr(agent, "last_usage", None)
    # Heuristic fallback produces no usage
    usage_info = None
    model_used = getattr(agent, "last_model_used", None)
    provider_used = getattr(agent, "provider", None)
    if usage and usage.get("prompt_tokens") is not None:
        usage_info = {
            "prompt_tokens": usage.get("prompt_tokens"),
            "completion_tokens": usage.get("completion_tokens"),
            "total_tokens": usage.get("total_tokens"),
            "model": usage.get("model") or model_used,
            "provider": provider_used,
        }
    if x_request_id:
        logger.info("parse_query request_id=%s query=%r usage=%s model=%s", x_request_id, payload.query, usage_info, model_used)
    _parse_cache[payload.query] = (filters, now)
    return ParseQueryResponse(filters=filters, usage=usage_info, model=model_used, provider=provider_used)


# ---------------------------------------------------------------------
# 2. Fallback search — Node fires this and doesn't wait on the response
#    body. IMPORTANT: this handler runs to full completion (search ->
#    verify -> upsert -> publish) before returning, on purpose. Vercel
#    freezes the function once a response is sent, so anything queued to
#    run "after" the response (e.g. FastAPI BackgroundTasks) is NOT safe
#    here. Node not waiting on the body is fine; Python not finishing
#    the work server-side is not.
# ---------------------------------------------------------------------
class FallbackSearchRequest(BaseModel):
    query_id: str          # correlates back to the user's session/socket on Node's side
    query: str
    filters: dict           # the QueryFilters JSON Node already has from step 1


class FallbackSearchResponse(BaseModel):
    query_id: str
    datasets_found: int
    published: bool
    datasets: list[Dataset]
    usage: dict | None = None
    model: str | None = None
    provider: str | None = None
    external_calls: list[dict] | None = None


def _web_candidate_to_repository_dataset(
    candidate: FallbackCandidate, filters: QueryFilters
) -> RepositoryDataset | None:
    """
    Bridge a web-discovery candidate into the quality pipeline's input schema.

    Web-origin candidates always carry ``source=web_search`` (the canonical
    web-discovery source label) — never ``source_guess``. The guess is preserved
    in ``raw`` for provenance/debug only; trust is derived in Stage 4 from the
    verified destination URL + validated metadata.

    ``filters`` (the parsed user query) is deliberately NOT used to populate any
    structured metadata: copying modality/species/condition/task/region from the
    query onto a discovered candidate would fabricate values the page may never
    contain. Structured fields stay empty and are filled later only from
    repository metadata, verified enrichment, ontology normalization, or the
    candidate's own content (Stage 3).
    """
    url = (candidate.url or "").strip()
    if not url:
        return None
    title = (candidate.title or "Untitled dataset").strip() or "Untitled dataset"
    source_id = hashlib.sha1(url.encode()).hexdigest()[:16]

    # Integrity rule: structured metadata (modality/species/condition/task/
    # region/format/keywords) is NEVER copied from the user's query onto a
    # discovered candidate. That would fabricate metadata the page may never
    # contain (incorrect badges / ranking / persistence / provenance).
    # Structured fields stay empty here; Stage 3 enrichment fills them ONLY
    # from exact vocabulary matches in the candidate's own title/description/
    # keywords, and Stage 4 derives trust from the verified destination URL.
    return RepositoryDataset(
        source=WEB_DISCOVERY_SOURCE,
        source_id=source_id,
        url=url,
        title=title,
        description=(candidate.reasoning or "Fallback candidate pending review"),
        modality=[],
        species=[],
        keywords=[],
        raw={"source_guess": candidate.source_guess, "discovery": "web"},
    )


@router.post("/agents/fallback-search", response_model=FallbackSearchResponse)
async def fallback_search(
    payload: FallbackSearchRequest,
    x_request_id: str | None = Header(default=None, alias="X-Request-Id"),
):
    if x_request_id:
        logger.info("fallback_search request_id=%s query_id=%s query=%r", x_request_id, payload.query_id, payload.query)
    settings = get_settings()
    filters = QueryFilters.model_validate({**payload.filters, "raw_query": payload.query})

    fallback_agent = FallbackAgent(search_provider=TavilySearchProvider())
    candidates = await fallback_agent.discover(filters, max_candidates=settings.MAX_FALLBACK_CANDIDATES)
    fb_usage = getattr(fallback_agent, "last_usage", None)
    fb_usage_info = None
    fb_model = getattr(fallback_agent, "last_model_used", None)
    if fb_usage and fb_usage.get("prompt_tokens") is not None:
        fb_usage_info = {
            "prompt_tokens": fb_usage.get("prompt_tokens"),
            "completion_tokens": fb_usage.get("completion_tokens"),
            "total_tokens": fb_usage.get("total_tokens"),
            "model": fb_usage.get("model") or fb_model,
            "provider": "groq",
        }

    # One quality path (§3.0): web candidates run the SAME 7-stage quality
    # pipeline as repository candidates. Stage 1 allows web-origin records
    # (blocklist + required fields; no repository allowlist); Stage 4 derives
    # trust from the verified destination URL + validated metadata and promotes
    # candidates that resolve to a supported repository domain into repository
    # datasets. Stage 7 persists only validated records, idempotently, into the
    # canonical record (URL/DOI merge). This handler still completes everything
    # synchronously before returning (Vercel serverless constraint).
    repo_candidates = [
        ds
        for ds in (_web_candidate_to_repository_dataset(c, filters) for c in candidates)
        if ds is not None
    ]
    pipeline = await run_quality_pipeline(
        repo_candidates, filters, publish=True, discovery_method="web_search"
    )
    datasets = pipeline.datasets

    await publish_fallback_result(
        payload.query_id,
        {
            "query_id": payload.query_id,
            "query": payload.query,
            "datasets": [d.model_dump(mode="json") for d in datasets],
        },
    )

    external_calls = getattr(fallback_agent, "last_external_calls", []) or []
    return FallbackSearchResponse(
        query_id=payload.query_id,
        datasets_found=len(datasets),
        published=True,
        datasets=datasets,
        usage=fb_usage_info,
        model=fb_model,
        provider="groq" if fb_usage_info else None,
        external_calls=external_calls if external_calls else None,
    )
