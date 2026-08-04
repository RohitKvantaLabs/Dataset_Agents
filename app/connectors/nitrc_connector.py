"""
NITRC Connector (§2.12).

Public IR/project feed: https://www.nitrc.org/ir/data/projects/ (+ JSON-format feed),
then per-project detail fetch for modalities/license. Least-structured of the
nine repositories — feed parsing is deliberately defensive; records missing
title/url are dropped.
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

_nitrc_circuit_breaker = CircuitBreaker(
    name="nitrc-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.nitrc")

NITRC_PROJECTS_FEED = "https://www.nitrc.org/ir/data/projects/"
PAGE_SIZE = 50


class NITRCConnector(BaseConnector):
    """Search-capable connector for NITRC project feeds (defensive parsing)."""

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"Accept": "application/json"},
        )
        self._limiter = TokenBucket(rate=get_rate_limit("nitrc"), burst=5)

    @property
    def source_name(self) -> str:
        return "nitrc"

    @connector_retry
    async def _get_json(self, url: str) -> dict | list:
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
            async with _nitrc_circuit_breaker:
                query_terms = self.build_query_terms(req)
                page = 1
                max_pages = get_max_pages()

                while len(records) < req.limit and page <= max_pages:
                    await self._limiter.acquire()

                    url = f"{NITRC_PROJECTS_FEED}?q={quote(query_terms)}"
                    data = await self._get_json(url)

                    # Confirmed live shape (2026-08-04): {"ResultSet": {"Result": [...]}}
                    # — also accept list / {"results"|"projects"|"data"} for robustness.
                    if isinstance(data, list):
                        results = data
                    elif isinstance(data, dict):
                        results = (
                            data.get("ResultSet") or {}
                        ).get("Result") or data.get("results") or data.get("projects") or data.get("data") or []
                    else:
                        results = []

                    total_available = len(results)  # feed reports no total

                    for item in results:
                        if not isinstance(item, dict):
                            continue
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None and post_filter(ds, req.filters):
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    # Single-page feed in practice; no reliable pagination cursor.
                    break

                truncated = len(records) >= req.limit

        except httpx.HTTPStatusError as exc:
            logger.error("NITRC search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("NITRC search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("NITRC search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("NITRC search failed (unexpected): %s", exc)
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
