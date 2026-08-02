"""
OpenNeuro GraphQL Connector.

Two capabilities:
- ``fetch()`` — batch sync of dataset metadata (existing path, §2.4/B).
- ``search()`` — online structured retrieval via the public GraphQL API
  with a keyword clause on dataset name/description (§2.4/S), then
  client-side post-filtering of modality/species.

OpenNeuro API playground: https://openneuro.org/crn/graphql (POST)
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

# Module-level circuit breaker for OpenNeuro API calls.
_openneuro_circuit_breaker = CircuitBreaker(
    name="openneuro-api",
    failure_threshold=3,
    recovery_timeout=30.0,
)

logger = logging.getLogger("neuro_platform.connectors.openneuro")

OPENNEURO_GRAPHQL_URL = "https://openneuro.org/crn/graphql"

# GraphQL query — fetches datasets with pagination (cursor-based).
# We request only the fields needed for the Common Schema to keep
# payloads small on the free public endpoint.
DATASETS_QUERY = """
query FetchDatasets($first: Int!, $after: String) {
  datasets(first: $first, after: $after) {
    edges {
      node {
        id
        name: name
        created
        description {
          Name
          Authors
          License
          DatasetDOI
        }
        metadata {
          modalities
          subjectCount
          species
          trialCount
          dataProcessed
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

# Search variant — same shape plus an optional `query` clause applied
# server-side against dataset name/description (§2.4 search strategy).
SEARCH_DATASETS_QUERY = """
query SearchDatasets($first: Int!, $after: String, $query: String) {
  datasets(first: $first, after: $after, query: $query) {
    edges {
      node {
        id
        name: name
        created
        description {
          Name
          Authors
          License
          DatasetDOI
        }
        metadata {
          modalities
          subjectCount
          species
          trialCount
          dataProcessed
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

PAGE_SIZE = 25  # keep requests small against the public endpoint


class OpenNeuroConnector(BaseConnector):
    """
    Fetches OpenNeuro dataset metadata via the public GraphQL API.

    Cursor-based pagination is handled automatically up to *limit* total
    records. No authentication is required for public datasets.
    """

    def __init__(self, http_client: httpx.AsyncClient | None = None):
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.REQUEST_TIMEOUT_SECONDS,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        self._limiter = TokenBucket(rate=get_rate_limit("openneuro"), burst=5)

    @property
    def source_name(self) -> str:
        return "openneuro"

    @property
    def searchable(self) -> bool:
        return True

    async def fetch(self, limit: int = 200) -> list[dict[str, Any]]:
        """
        Pull up to *limit* datasets from OpenNeuro via GraphQL.

        Each returned dict is the raw ``node`` object from the GraphQL
        edges array, with ``source_name`` injected for the normalizer.
        """
        records: list[dict] = []
        cursor: str | None = None
        page_size = min(PAGE_SIZE, limit)

        try:
            async with _openneuro_circuit_breaker:
                while len(records) < limit:
                    variables: dict = {"first": page_size}
                    if cursor:
                        variables["after"] = cursor

                    logger.debug(
                        "OpenNeuro fetch: cursor=%s page_size=%d", cursor, page_size
                    )

                    resp = await self._client.post(
                        OPENNEURO_GRAPHQL_URL,
                        json={"query": DATASETS_QUERY, "variables": variables},
                    )
                    resp.raise_for_status()
                    body = resp.json()

                    if "errors" in body:
                        logger.error("OpenNeuro GraphQL errors: %s", body["errors"])
                        break

                    connection = body.get("data", {}).get("datasets", {})
                    edges = connection.get("edges", [])
                    page_info = connection.get("pageInfo", {})

                    for edge in edges:
                        node = edge.get("node", {})
                        records.append(node)
                        if len(records) >= limit:
                            break

                    if not page_info.get("hasNextPage") or not edges:
                        break
                    cursor = page_info.get("endCursor")

        except httpx.HTTPStatusError as exc:
            logger.error(
                "OpenNeuro API HTTP error: %s %s",
                exc.response.status_code,
                exc.request.url,
            )
        except httpx.RequestError as exc:
            logger.error("OpenNeuro API request failed: %s", exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("OpenNeuro API circuit is open: %s", exc)
        finally:
            if self._owns_client:
                await self._client.aclose()

        logger.info("OpenNeuroConnector fetched %d records", len(records))
        return records[:limit]

    @connector_retry
    async def _post_graphql(self, query: str, variables: dict) -> dict:
        """Single GraphQL POST with the §2.3.1 retry policy (2 retries, 0.5→2 s)."""
        resp = await self._client.post(
            OPENNEURO_GRAPHQL_URL,
            json={"query": query, "variables": variables},
        )
        resp.raise_for_status()
        return resp.json()

    async def search(self, req: SearchRequest) -> SearchResult:
        """
        OpenNeuro online search (§2.4).

        Uses the search query (server-side keyword clause on name/description)
        when query terms exist, otherwise falls back to plain pagination.
        Modality/species are post-filtered client-side. Never raises — a
        failure yields an empty SearchResult with status="offline".
        """
        start = time.monotonic()
        records = []
        total_available = 0
        truncated = False
        error: str | None = None

        try:
            async with _openneuro_circuit_breaker:
                query_terms = self.build_query_terms(req)
                cursor: str | None = None
                page_size = min(PAGE_SIZE, req.limit)
                pages = 0
                max_pages = get_max_pages()
                page_info = {}  # set inside the loop; default keeps `truncated` safe on early break

                while len(records) < req.limit and pages < max_pages:
                    pages += 1
                    await self._limiter.acquire()

                    variables: dict = {"first": page_size}
                    if cursor:
                        variables["after"] = cursor

                    if query_terms:
                        body = await self._post_graphql(SEARCH_DATASETS_QUERY, {**variables, "query": query_terms})
                        if "errors" in body:
                            # Schema drift: API rejected the `query` arg → fall
                            # back to unfiltered pagination + client-side filter
                            # (§2.4), keeping the connector functional.
                            logger.warning(
                                "OpenNeuro rejected `query` arg — falling back to unfiltered pagination: %s",
                                body["errors"],
                            )
                            body = await self._post_graphql(DATASETS_QUERY, variables)
                    else:
                        body = await self._post_graphql(DATASETS_QUERY, variables)

                    if "errors" in body:
                        logger.error("OpenNeuro search GraphQL errors: %s", body["errors"])
                        error = "graphql_errors"
                        break

                    connection = body.get("data", {}).get("datasets", {})
                    edges = connection.get("edges", [])
                    page_info = connection.get("pageInfo", {})

                    for edge in edges:
                        node = edge.get("node", {})
                        ds = normalize_repository(node, self.source_name)
                        if ds is not None and post_filter(ds, req.filters):
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    if not page_info.get("hasNextPage") or not edges:
                        break
                    cursor = page_info.get("endCursor")

                truncated = bool(page_info and page_info.get("hasNextPage")) if len(records) else False

        except httpx.HTTPStatusError as exc:
            logger.error("OpenNeuro search HTTP error: %s %s", exc.response.status_code, exc.request.url)
            error = f"http_{exc.response.status_code}"
        except httpx.RequestError as exc:
            logger.error("OpenNeuro search request failed: %s", exc)
            error = str(exc)
        except CircuitBreakerOpenError as exc:
            logger.warning("OpenNeuro search circuit is open: %s", exc)
            error = "circuit_open"
        except Exception as exc:  # noqa: BLE001 — malformed payload/JSON → isolate, never raise
            logger.error("OpenNeuro search failed (unexpected): %s", exc)
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
