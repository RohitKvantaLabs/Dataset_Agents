"""
DANDI Archive REST Connector.

Two capabilities:
- ``fetch()`` — batch sync of dandiset metadata (existing path, §2.4/B).
- ``search()`` — online structured retrieval via DANDI REST v2
  ``?search=`` query param (§2.5/S), normalized to RepositoryDataset.

DANDI API docs: https://api.dandiarchive.org/swagger/
"""
import logging
import time
from typing import Any

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
    post_filter,
)
from app.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from app.ingestion.normalizer import normalize_repository

# Module-level circuit breaker for DANDI API calls.
_dandi_circuit_breaker = CircuitBreaker(
    name="dandi-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.dandi")

DANDI_API_BASE = "https://api.dandiarchive.org/api"
PAGE_SIZE = 100  # DANDI default max page size


class DandiConnector(BaseConnector):
    """
    Pulls dandiset metadata from the DANDI public REST API.

    Pagination is handled automatically up to *limit* total records.
    No authentication required for public dandisets.
    """

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"Accept": "application/json"},
        )
        self._limiter = TokenBucket(rate=get_rate_limit("dandi"), burst=5)

    @property
    def source_name(self) -> str:
        return "dandi"

    @property
    def searchable(self) -> bool:
        return True

    async def fetch(self, limit: int = 200) -> list[dict[str, Any]]:
        """
        Retrieve up to *limit* dandisets from the DANDI REST API.

        Iterates through paginated /api/dandisets/ responses until
        *limit* records are gathered or no more pages exist.
        """
        records: list[dict] = []
        url: str | None = f"{DANDI_API_BASE}/dandisets/?page_size={min(PAGE_SIZE, limit)}"

        try:
            async with _dandi_circuit_breaker:
                while url and len(records) < limit:
                    remaining = limit - len(records)
                    paged_url = self._inject_page_size(url, min(PAGE_SIZE, remaining))

                    logger.debug("DANDI fetch: GET %s", paged_url)
                    resp = await self._client.get(paged_url)
                    resp.raise_for_status()
                    data = resp.json()

                    results = data.get("results", [])
                    records.extend(results)
                    url = data.get("next")  # None when exhausted

        except httpx.HTTPStatusError as exc:
            logger.error("DANDI API HTTP error: %s %s", exc.response.status_code, exc.request.url)
        except httpx.RequestError as exc:
            logger.error("DANDI API request failed: %s", exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("DANDI API circuit is open: %s", exc)
        finally:
            if self._owns_client:
                await self._client.aclose()

        logger.info("DandiConnector fetched %d records", len(records))
        return records[:limit]

    @connector_retry
    async def _get_json(self, url: str, params: dict | None = None) -> dict:
        """Single page fetch with the §2.3.1 retry policy (2 retries, 0.5→2 s)."""
        resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()

    async def search(self, req: SearchRequest) -> SearchResult:
        """
        DANDI online search (§2.5): GET /api/dandisets/?search=<terms>.

        DANDI supports ``?search=`` natively; modality/species are
        post-filtered client-side from ``metadata``. Never raises — a
        failure yields an empty SearchResult with status="offline".
        """
        start = time.monotonic()
        records = []
        total_available = 0
        truncated = False
        error: str | None = None

        try:
            async with _dandi_circuit_breaker:
                query_terms = self.build_query_terms(req)
                params: dict = {"page_size": min(PAGE_SIZE, req.limit)}
                if query_terms:
                    params["search"] = query_terms

                url = f"{DANDI_API_BASE}/dandisets/"
                pages = 0
                max_pages = get_max_pages()
                first_page = True

                while url and len(records) < req.limit and pages < max_pages:
                    pages += 1
                    await self._limiter.acquire()

                    # First page carries the search/page_size params; the API's
                    # `next` URL is self-sufficient afterwards.
                    if first_page:
                        data = await self._get_json(url, params=params)
                        first_page = False
                    else:
                        data = await self._get_json(url)

                    results = data.get("results", [])
                    total_available = data.get("count") or len(results)

                    for item in results:
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None and post_filter(ds, req.filters):
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    url = data.get("next")

                truncated = total_available > len(records)

        except httpx.HTTPStatusError as exc:
            logger.error("DANDI search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("DANDI search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("DANDI search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("DANDI search failed (unexpected): %s", exc)
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

    @staticmethod
    def _inject_page_size(url: str, page_size: int) -> str:
        """Ensure the page_size query param reflects the requested chunk size."""
        if "page_size=" in url:
            return url
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}page_size={page_size}"
