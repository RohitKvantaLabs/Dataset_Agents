"""
OpenNeuro GraphQL Connector.

Fetches dataset metadata from OpenNeuro's public GraphQL API:
  https://openneuro.org/crn/graphql

The query pulls a page of datasets with their metadata objects.
Returned dicts use OpenNeuro's native field names; the normalizer
handles the mapping to Common Schema.

OpenNeuro API playground: https://openneuro.org/crn/graphql (POST)
"""
import logging
from typing import Any

import httpx

from app.config import get_settings
from app.connectors.base import BaseConnector
from app.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError

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

    @property
    def source_name(self) -> str:
        return "openneuro"

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
