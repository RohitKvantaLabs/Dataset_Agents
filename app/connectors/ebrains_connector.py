"""
EBRAINS Knowledge Graph Connector (§2.7).

Knowledge Graph API: https://kg.ebrains.eu/api/instances/query
Search: GET /instances/query?query=<query_terms>&page=<0-based>&size=<25>
over dataset-type instances (filter ``type`` = dataset).

Auth: ``Authorization: Bearer <EBRAINS_API_KEY>`` (required).
When the key is missing the connector does NOT skip silently — it returns
an ``offline`` SearchResult with a reason (§2.7).
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

_ebrains_circuit_breaker = CircuitBreaker(
    name="ebrains-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.ebrains")

EBRAINS_QUERY_URL = "https://kg.ebrains.eu/api/instances/query"
PAGE_SIZE = 25


class EBRAINSConnector(BaseConnector):
    """Search-capable connector for EBRAINS Knowledge Graph datasets."""

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._api_key: str | None = settings.EBRAINS_API_KEY
        self._owns_client = http_client is None
        headers = {"Accept": "application/json"}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers=headers,
        )
        self._limiter = TokenBucket(rate=get_rate_limit("ebrains"), burst=5)

    @property
    def source_name(self) -> str:
        return "ebrains"

    @connector_retry
    async def _get_json(self, url: str) -> dict:
        resp = await self._client.get(url)
        resp.raise_for_status()
        return resp.json()

    async def search(self, req: SearchRequest) -> SearchResult:
        start = time.monotonic()
        error: str | None = None

        # §2.7: key missing → offline status with reason (do not skip silently).
        if not self._api_key:
            try:
                return SearchResult(
                    source=self.source_name,
                    query=req.query,
                    elapsed_ms=int((time.monotonic() - start) * 1000),
                    status="offline",
                    error="EBRAINS_API_KEY not configured",
                )
            finally:
                if self._owns_client:
                    await self._client.aclose()

        records = []
        total_available = 0
        truncated = False

        try:
            async with _ebrains_circuit_breaker:
                query_terms = self.build_query_terms(req)
                page = 0  # 0-based per §2.7
                max_pages = get_max_pages()

                while len(records) < req.limit and page < max_pages:
                    await self._limiter.acquire()

                    data = await self._get_json(
                        f"{EBRAINS_QUERY_URL}?query={quote(query_terms)}&page={page}&size={PAGE_SIZE}"
                    )

                    # KG returns instances under various keys; be defensive.
                    instances = data.get("data") or data.get("results") or data.get("instances") or []
                    total_available = data.get("total") or data.get("totalElements") or len(instances)

                    for item in instances:
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None:
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    if len(instances) < PAGE_SIZE:
                        break
                    page += 1

                truncated = total_available > len(records)

        except httpx.HTTPStatusError as exc:
            logger.error("EBRAINS search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("EBRAINS search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("EBRAINS search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("EBRAINS search failed (unexpected): %s", exc)
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
