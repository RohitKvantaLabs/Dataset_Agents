"""
Figshare Connector (§2.9).

REST v2 API: https://api.figshare.com/v2/articles/
Search: GET /articles/?search_for=<query_terms>&item_type=dataset
        &page=<n>&page_size=<100>&order=published_date&order_direction=desc
"""
import logging
import time
from typing import Any
from urllib.parse import quote

import httpx

from app.config import get_settings
from app.connectors.base import (
    BaseConnector,
    SearchRequest,
    SearchResult,
    TokenBucket,
    connector_retry,
    get_max_pages,
    get_rate_limit,
)
from app.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from app.ingestion.normalizer import normalize_repository

_figshare_circuit_breaker = CircuitBreaker(
    name="figshare-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.figshare")

FIGSHARE_API_BASE = "https://api.figshare.com/v2/articles"
PAGE_SIZE = 100


class FigshareConnector(BaseConnector):
    """Search-capable connector for Figshare dataset articles."""

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"Accept": "application/json"},
        )
        self._limiter = TokenBucket(rate=get_rate_limit("figshare"), burst=5)

    @property
    def source_name(self) -> str:
        return "figshare"

    async def fetch(self, limit: int = 200) -> list[dict[str, Any]]:
        # §2.9 marks figshare as S (search-only) — batch sync not required.
        raise NotImplementedError("figshare is a search-only connector (S) in v0.2")

    @connector_retry
    async def _get_json(self, url: str) -> list | dict:
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    async def search(self, req: SearchRequest) -> SearchResult:
        start = time.monotonic()
        records = []
        total_available = 0
        truncated = False
        error: str | None = None

        try:
            async with _figshare_circuit_breaker:
                query_terms = self.build_query_terms(req)
                page = 1
                max_pages = get_max_pages()
                page_size = min(PAGE_SIZE, max(req.limit, 1))

                while len(records) < req.limit and page <= max_pages:
                    await self._limiter.acquire()

                    url = (
                        f"{FIGSHARE_API_BASE}/?search_for={quote(query_terms)}"
                        f"&item_type=dataset&page={page}&page_size={page_size}"
                        f"&order=published_date&order_direction=desc"
                    )
                    data = await self._get_json(url)

                    results = data if isinstance(data, list) else data.get("data", [])
                    total_available = len(results)  # list response carries no total

                    for item in results:
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None:
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    if len(results) < page_size:
                        break
                    page += 1

                truncated = len(records) >= req.limit

        except httpx.HTTPStatusError as exc:
            logger.error("Figshare search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("Figshare search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("Figshare search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("Figshare search failed (unexpected): %s", exc)
            error = str(exc)
        finally:
            if self._owns_client:
                await self._client.aclose()

        return SearchResult(
            source=self.source_name,
            query=req.query,
            total_available=total_available,
            records=records[: req.limit],
            elapsed_ms=int((time.monotonic() - start) * 1000),
            truncated=truncated,
            status="offline" if error else "ok",
            error=error,
        )
