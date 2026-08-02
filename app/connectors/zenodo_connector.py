"""
Zenodo Connector (§2.8).

REST API: https://zenodo.org/api/records/
Search: GET /records/?q=<lucene-escaped query_terms>&type=dataset&size=<max 100>&page=<n>

Optional token for higher rate limits; public read works without auth.
"""
import logging
import re
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

_zenodo_circuit_breaker = CircuitBreaker(
    name="zenodo-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.zenodo")

ZENODO_API_BASE = "https://zenodo.org/api/records"
PAGE_SIZE = 100  # Zenodo max page size

# Characters with special meaning in Zenodo's Lucene query syntax.
_LUCENE_SPECIAL = re.compile(r'([+\-&|!(){}[\]^"~*?:\\/])')


def _lucene_escape(terms: str) -> str:
    """Escape Lucene special characters for the ``q`` parameter (§2.8)."""
    return _LUCENE_SPECIAL.sub(r"\\\1", terms)


class ZenodoConnector(BaseConnector):
    """Search-capable connector for Zenodo dataset records."""

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"Accept": "application/json"},
        )
        self._limiter = TokenBucket(rate=get_rate_limit("zenodo"), burst=5)

    @property
    def source_name(self) -> str:
        return "zenodo"

    async def fetch(self, limit: int = 200) -> list[dict[str, Any]]:
        # §2.8 marks zenodo as S (search-only) — batch sync not required.
        raise NotImplementedError("zenodo is a search-only connector (S) in v0.2")

    @connector_retry
    async def _get_json(self, url: str) -> dict:
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
            async with _zenodo_circuit_breaker:
                query_terms = _lucene_escape(self.build_query_terms(req))
                page = 1
                max_pages = get_max_pages()
                size = min(PAGE_SIZE, max(req.limit, 1))

                while len(records) < req.limit and page <= max_pages:
                    await self._limiter.acquire()

                    url = (
                        f"{ZENODO_API_BASE}/?q={quote(query_terms)}"
                        f"&type=dataset&size={size}&page={page}"
                    )
                    data = await self._get_json(url)

                    hits = data.get("hits", {})
                    results = hits.get("hits", [])
                    total = hits.get("total")
                    total_available = total if isinstance(total, int) else (total.get("value") if isinstance(total, dict) else len(results))

                    for item in results:
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None:
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    if page * size >= total_available or not results:
                        break
                    page += 1

                truncated = total_available > len(records)

        except httpx.HTTPStatusError as exc:
            logger.error("Zenodo search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("Zenodo search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("Zenodo search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("Zenodo search failed (unexpected): %s", exc)
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
