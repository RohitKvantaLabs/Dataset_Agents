"""
Dryad Connector (§2.10).

REST v2 API: https://datadryad.org/api/v2/
Search: GET /search?q=<query_terms>&per_page=<100>&page=<n>

Datasets are DOI-centric → ``doi`` always present (strong dedup signal);
the package ``identifier`` is the repository-native id.
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

_dryad_circuit_breaker = CircuitBreaker(
    name="dryad-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.dryad")

DRYAD_API_BASE = "https://datadryad.org/api/v2"
PAGE_SIZE = 100


class DryadConnector(BaseConnector):
    """Search-capable connector for Dryad DOI-centric datasets."""

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"Accept": "application/json"},
        )
        self._limiter = TokenBucket(rate=get_rate_limit("dryad"), burst=5)

    @property
    def source_name(self) -> str:
        return "dryad"

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
            async with _dryad_circuit_breaker:
                query_terms = self.build_query_terms(req)
                page = 1
                max_pages = get_max_pages()
                per_page = min(PAGE_SIZE, max(req.limit, 1))

                while len(records) < req.limit and page <= max_pages:
                    await self._limiter.acquire()

                    url = (
                        f"{DRYAD_API_BASE}/search?q={quote(query_terms)}"
                        f"&per_page={per_page}&page={page}"
                    )
                    data = await self._get_json(url)

                    embedded = data.get("_embedded", {})
                    results = embedded.get("stash:datasets", []) or data.get("results", [])
                    total_available = data.get("count") or len(results)

                    for item in results:
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None:
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    if not data.get("_links", {}).get("next") or not results:
                        break
                    page += 1

                truncated = total_available > len(records)

        except httpx.HTTPStatusError as exc:
            logger.error("Dryad search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("Dryad search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("Dryad search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("Dryad search failed (unexpected): %s", exc)
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
