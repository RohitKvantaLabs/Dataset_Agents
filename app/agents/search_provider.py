"""
Abstraction over "search the web." The Fallback Agent should never call a
specific provider's SDK directly - that keeps the provider swappable and
makes the agent testable without hitting the network.
"""
import logging
from abc import ABC, abstractmethod

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import get_settings

logger = logging.getLogger("neuro_platform.agents.search_provider")

TAVILY_SEARCH_URL = "https://api.tavily.com/search"


class SearchResult(dict):
    """{'title': str, 'url': str, 'snippet': str}"""


class SearchProvider(ABC):
    @abstractmethod
    async def search(self, query: str, max_results: int = 10) -> list[SearchResult]:
        ...


class NullSearchProvider(SearchProvider):
    """Safe default: returns no results rather than pretending to search.
    Useful in tests / local dev without a Tavily key."""

    async def search(self, query: str, max_results: int = 10) -> list[SearchResult]:
        return []


class TavilySearchProvider(SearchProvider):
    """
    Real web search via Tavily (https://tavily.com). Grounds the Fallback
    Agent's candidates in actual search results instead of pure LLM guesses -
    the Verification Agent still independently checks every URL afterwards,
    this just improves the odds a candidate is real in the first place.
    """

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._api_key = settings.TAVILY_API_KEY
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(timeout=settings.REQUEST_TIMEOUT_SECONDS)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6), reraise=True)
    async def _call_tavily(self, query: str, max_results: int) -> dict:
        response = await self._client.post(
            TAVILY_SEARCH_URL,
            json={
                "api_key": self._api_key,
                "query": query,
                "search_depth": "advanced",
                "max_results": max_results,
                "include_answer": False,
            },
        )
        response.raise_for_status()
        return response.json()

    async def search(self, query: str, max_results: int = 10) -> list[SearchResult]:
        try:
            data = await self._call_tavily(query, max_results)
        except httpx.HTTPError as exc:
            # Search failing should degrade the Fallback Agent to LLM-only
            # candidates, not crash the whole fallback-search request.
            logger.warning("Tavily search failed for %r: %s", query, exc)
            return []
        finally:
            if self._owns_client:
                await self._client.aclose()

        if not isinstance(data, dict):
            logger.warning("Tavily returned a malformed response for %r", query)
            return []

        results = data.get("results", [])
        if not isinstance(results, list):
            logger.warning("Tavily returned malformed results for %r", query)
            return []

        return [
            SearchResult(
                title=r.get("title", "Untitled"),
                url=r.get("url", ""),
                snippet=r.get("content", ""),
            )
            for r in results
            if isinstance(r, dict) and r.get("url")
        ]
