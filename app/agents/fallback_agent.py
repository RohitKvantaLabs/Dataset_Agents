import json
import logging

from app.agents.search_provider import NullSearchProvider, SearchProvider
from app.config import get_settings
from app.llm.client import LLMClient, LLMJSONParseError
from app.llm.prompts import FALLBACK_DISCOVERY_SYSTEM_PROMPT, fallback_discovery_user_prompt
from app.models.query_filters import QueryFilters

logger = logging.getLogger("neuro_platform.agents.fallback")


class FallbackCandidate:
    def __init__(self, title: str, url: str, source_guess: str, reasoning: str):
        self.title = title
        self.url = url
        self.source_guess = source_guess
        self.reasoning = reasoning


class FallbackAgent:
    """
    Only called by Node when Mongo has no strong match. Proposes candidate
    URLs - it never returns a "final answer". Everything it produces MUST
    pass through the Verification Agent before being stored or shown.
    """

    def __init__(self, llm_client: LLMClient | None = None, search_provider: SearchProvider | None = None) -> None:
        if llm_client is not None:
            self._llm: LLMClient = llm_client
        else:
            settings = get_settings()
            self._llm = LLMClient(model=settings.GROQ_FALLBACK_MODEL)
        self._search = search_provider or NullSearchProvider()

    async def discover(self, filters: QueryFilters, max_candidates: int = 10) -> list[FallbackCandidate]:
        # ponytail: append filetype hint for Tavily; raw_query stays clean for the LLM prompt.
        _SEARCH_HINT = "filetype:nii OR filetype:edf OR BIDS dataset download"
        search_query = f"{filters.raw_query} {_SEARCH_HINT}"
        web_hits = await self._search.search(search_query, max_results=max_candidates)

        try:
            llm_candidates = self._llm.generate_json(
                system_prompt=FALLBACK_DISCOVERY_SYSTEM_PROMPT.format(max_candidates=max_candidates),
                user_prompt=fallback_discovery_user_prompt(
                    filters.raw_query, json.dumps(filters.model_dump())
                ),
                max_tokens=800,
            )
        except Exception as exc:
            logger.warning("Fallback LLM discovery failed for %r: %s", filters.raw_query, exc)
            llm_candidates = []

        if not isinstance(llm_candidates, list):
            llm_candidates = []

        candidates = [
            FallbackCandidate(
                title=c.get("title", "Untitled dataset"),
                url=c.get("url", ""),
                source_guess=c.get("source_guess", "unknown"),
                reasoning=c.get("reasoning", ""),
            )
            for c in llm_candidates
            if c.get("url")
        ]

        # Web-search hits are grounded in a real result, so treat them as
        # higher-confidence candidates than pure LLM guesses.
        for hit in web_hits:
            candidates.append(
                FallbackCandidate(
                    title=hit.get("title", "Untitled dataset"),
                    url=hit.get("url", ""),
                    source_guess="web_search",
                    reasoning=hit.get("snippet", ""),
                )
            )

        return candidates[:max_candidates]