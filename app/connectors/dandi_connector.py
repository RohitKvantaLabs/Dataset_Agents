"""
DANDI Archive REST Connector.

Fetches dandiset (dataset) metadata from the DANDI REST API v2:
  https://api.dandiarchive.org/api/dandisets/

Returned dicts use DANDI's native field names; the normalizer maps them
to the Common Schema.

DANDI API docs: https://api.dandiarchive.org/swagger/
"""
import logging
from typing import Any

import httpx

from app.config import get_settings
from app.connectors.base import BaseConnector
from app.core.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError

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

    @property
    def source_name(self) -> str:
        return "dandi"

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

    @staticmethod
    def _inject_page_size(url: str, page_size: int) -> str:
        """Ensure the page_size query param reflects the requested chunk size."""
        if "page_size=" in url:
            return url
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}page_size={page_size}"
