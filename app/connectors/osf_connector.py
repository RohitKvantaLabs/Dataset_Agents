"""
OSF Connector (§2.11).

REST v2 API: https://api.osf.io/v2/
Search: primary GET /search/?q=<query_terms>&filter[category]=data
Fallback: GET /registrations/?filter[category]=data paginated (25/page).

Filtered to ``category=data``; tags → keywords; DOIs via ``identifiers``
when present.
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

_osf_circuit_breaker = CircuitBreaker(
    name="osf-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.osf")

OSF_API_BASE = "https://api.osf.io/v2"
PAGE_SIZE = 25


class OSFConnector(BaseConnector):
    """Search-capable connector for OSF data registrations/nodes."""

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"Accept": "application/json"},
        )
        self._limiter = TokenBucket(rate=get_rate_limit("osf"), burst=5)

    @property
    def source_name(self) -> str:
        return "osf"

    async def fetch(self, limit: int = 200) -> list[dict[str, Any]]:
        # §2.11 marks osf as S (search-only) — batch sync not required.
        raise NotImplementedError("osf is a search-only connector (S) in v0.2")

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
            async with _osf_circuit_breaker:
                query_terms = self.build_query_terms(req)
                page = 1
                max_pages = get_max_pages()
                used_search_endpoint = False

                while len(records) < req.limit and page <= max_pages:
                    await self._limiter.acquire()

                    # Primary: /search/?q=... (aggregated search). Fallback:
                    # /registrations/?filter[category]=data paginated.
                    if page == 1 and not used_search_endpoint:
                        url = f"{OSF_API_BASE}/search/?q={quote(query_terms)}&filter[category]=data"
                        try:
                            data = await self._get_json(url)
                        except httpx.HTTPStatusError as exc:
                            if exc.response.status_code >= 400:
                                logger.warning(
                                    "OSF /search endpoint failed (%s) — falling back to /registrations/",
                                    exc.response.status_code,
                                )
                                used_search_endpoint = True
                                url = f"{OSF_API_BASE}/registrations/?filter[category]=data&page={page}&page_size={PAGE_SIZE}"
                                data = await self._get_json(url)
                            else:
                                raise
                    else:
                        url = f"{OSF_API_BASE}/registrations/?filter[category]=data&page={page}&page_size={PAGE_SIZE}"
                        data = await self._get_json(url)

                    results = data.get("data", [])
                    meta = data.get("meta", {})
                    total_available = meta.get("total") or len(results)

                    for item in results:
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None:
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    links = data.get("links", {})
                    if not links.get("next") or not results:
                        break
                    page += 1

                truncated = total_available > len(records)

        except httpx.HTTPStatusError as exc:
            logger.error("OSF search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("OSF search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("OSF search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("OSF search failed (unexpected): %s", exc)
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
