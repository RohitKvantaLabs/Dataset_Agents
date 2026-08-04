"""
NeuroVault Connector (§2.6).

DRF REST API: https://neurovault.org/api/
Search: GET /api/collections/?search=<query_terms>&page=<n>
Fallback: paginate GET /api/collections/ and filter client-side when
``?search=`` is unsupported (some deployments ignore it).

Focus is collections (statistical maps) = datasets; images are sub-items.
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
    post_filter,
)
from app.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError
from app.ingestion.normalizer import normalize_repository

_neurovault_circuit_breaker = CircuitBreaker(
    name="neurovault-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.neurovault")

NEUROVAULT_API_BASE = "https://neurovault.org/api"
PAGE_SIZE = 50


class NeuroVaultConnector(BaseConnector):
    """Search-capable connector for NeuroVault statistical-map collections."""

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"Accept": "application/json"},
        )
        self._limiter = TokenBucket(rate=get_rate_limit("neurovault"), burst=5)

    @property
    def source_name(self) -> str:
        return "neurovault"

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
            async with _neurovault_circuit_breaker:
                query_terms = self.build_query_terms(req)
                page = 1
                max_pages = get_max_pages()
                used_search_param = False

                while len(records) < req.limit and page <= max_pages:
                    await self._limiter.acquire()

                    params: dict = {"page": page}
                    if query_terms and not used_search_param:
                        params["search"] = query_terms

                    url = f"{NEUROVAULT_API_BASE}/collections/"
                    try:
                        data = await self._get_json(f"{url}?{_urlencode(params)}")
                    except httpx.HTTPStatusError as exc:
                        # Fallback (§2.6): if ?search= is unsupported (400/500),
                        # drop it and paginate + filter client-side.
                        if query_terms and not used_search_param and exc.response.status_code >= 400:
                            logger.warning(
                                "NeuroVault ?search= rejected (%s) — falling back to client-side filter",
                                exc.response.status_code,
                            )
                            used_search_param = True
                            params.pop("search", None)
                            data = await self._get_json(f"{url}?{_urlencode(params)}")
                        else:
                            raise

                    results = data.get("results", [])
                    total_available = data.get("count") or len(results)

                    for item in results:
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None and post_filter(ds, req.filters):
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    if not data.get("next") or not results:
                        break
                    page += 1

                truncated = total_available > len(records)

        except httpx.HTTPStatusError as exc:
            logger.error("NeuroVault search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("NeuroVault search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("NeuroVault search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("NeuroVault search failed (unexpected): %s", exc)
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


def _urlencode(params: dict) -> str:
    """Encode query params, keeping list values as repeated keys."""
    parts = []
    for key, value in params.items():
        if isinstance(value, (list, tuple)):
            for v in value:
                parts.append(f"{quote(key)}={quote(str(v))}")
        else:
            parts.append(f"{quote(key)}={quote(str(value))}")
    return "&".join(parts)
