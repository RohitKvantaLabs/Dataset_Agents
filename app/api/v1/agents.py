import logging

from fastapi import APIRouter, Depends

from app.agents.fallback_agent import FallbackAgent
from app.agents.query_understanding_agent import QueryUnderstandingAgent
from app.agents.search_provider import TavilySearchProvider
from app.agents.verification_agent import VerificationAgent
from app.config import get_settings
from app.core.security import require_internal_secret
from app.db.repositories.dataset_repository import upsert_many
from app.models.query_filters import ParseQueryRequest, ParseQueryResponse, QueryFilters
from app.models.dataset import Dataset
from app.services.redis_publisher import publish_fallback_result
from pydantic import BaseModel

logger = logging.getLogger("neuro_platform.api.agents")

router = APIRouter(dependencies=[Depends(require_internal_secret)])


# ---------------------------------------------------------------------
# 1. Query Understanding — BLOCKING. Node waits on this response before
#    it queries Mongo itself.
# ---------------------------------------------------------------------
@router.post("/agents/parse-query", response_model=ParseQueryResponse)
async def parse_query(payload: ParseQueryRequest):
    agent = QueryUnderstandingAgent()
    filters = agent.parse(payload.query)
    return ParseQueryResponse(filters=filters)


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


@router.post("/agents/fallback-search", response_model=FallbackSearchResponse)
async def fallback_search(payload: FallbackSearchRequest):
    settings = get_settings()
    filters = QueryFilters.model_validate({**payload.filters, "raw_query": payload.query})

    fallback_agent = FallbackAgent(search_provider=TavilySearchProvider())
    candidates = await fallback_agent.discover(filters, max_candidates=settings.MAX_FALLBACK_CANDIDATES)

    verification_agent = VerificationAgent()
    verified_datasets: list[Dataset] = await verification_agent.verify(candidates, filters=filters)

    if verified_datasets:
        await upsert_many(verified_datasets)

    await publish_fallback_result(
        payload.query_id,
        {
            "query_id": payload.query_id,
            "query": payload.query,
            "datasets": [d.model_dump(mode="json") for d in verified_datasets],
        },
    )

    return FallbackSearchResponse(
        query_id=payload.query_id,
        datasets_found=len(verified_datasets),
        published=True,
        datasets=verified_datasets,
    )
