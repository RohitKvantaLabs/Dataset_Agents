"""
EBRAINS Knowledge Graph Connector (§2.7) — current KG Core Query API.

Endpoint: ``POST https://core.kg.ebrains.eu/v3/queries`` (KG Core, production).
The retired ``https://kg.ebrains.eu/api/instances/query`` endpoint is NO LONGER
VALID — the current EBRAINS platform exposes the KG through KG Core at
``core.kg.ebrains.eu`` with the current Query API / Instances API, while the
public search UI is backed by the KG Search service at ``search.kg.ebrains.eu``
(https://docs.kg.ebrains.eu — "OpenAPI specifications / Production").

The Query API executes a JSON-LD query payload (``meta.type`` + ``structure``
+ optional property ``filters``) and returns a PaginatedStreamResultJsonLdDoc:

    {"data": [<instance JSON-LD docs>], "total": N, "size": S, "from": F, ...}

Auth: ``Authorization: Bearer <EBRAINS_API_KEY>`` — an EBRAINS IAM access
token (scopes ``openid email group profile roles team``). When the key is
missing the connector does NOT skip silently — it returns an ``offline``
SearchResult with a reason (§2.7).

Pagination: ``from`` (0-based offset) and ``size`` query parameters; the
response reports ``total`` for the truncated flag.
"""
import copy
import logging
import re
import time

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

# Current production KG Core Query API (docs.kg.ebrains.eu — "The Query API").
EBRAINS_QUERY_URL = "https://core.kg.ebrains.eu/v3/queries"
PAGE_SIZE = 25

# Query only RELEASED instances — this is the content the public KG Search UI
# surfaces (drafts are IN_PROGRESS and not publicly listed).
EBRAINS_QUERY_STAGE = "RELEASED"

# Query payload executed against the Query API. ``meta.type`` restricts the
# traversal to openMINDS DatasetVersion instances (the documented example
# type); ``structure`` selects the JSON-LD properties we normalize.
# ``fullName``/``custodian`` are the exact vocab IRIs used in the official
# docs examples; ``description``/``firstReleasedAt``/``license`` are openMINDS
# DatasetVersion properties.
EBRAINS_QUERY_TEMPLATE: dict = {
    "@context": {
        "@vocab": "https://core.kg.ebrains.eu/vocab/query/",
        "path": {"@id": "path", "@type": "@id"},
    },
    "meta": {
        "type": "https://openminds.ebrains.eu/core/DatasetVersion",
    },
    "structure": [
        {"path": "@id"},
        {"path": "https://openminds.ebrains.eu/vocab/fullName"},
        {"path": "https://openminds.ebrains.eu/vocab/description"},
        {"path": "https://openminds.ebrains.eu/vocab/firstReleasedAt"},
        {
            "path": "https://openminds.ebrains.eu/vocab/custodian",
            "structure": {"path": "https://openminds.ebrains.eu/vocab/fullName"},
        },
        {
            "path": "https://openminds.ebrains.eu/vocab/license",
            "structure": {"path": "@id"},
        },
    ],
}


def _regex_filter_terms(terms: str) -> str | None:
    """Build a documented REGEX property-filter value from the query terms.

    The Query API has no free-text search parameter — keyword search is done
    with per-property filters (docs: "Filters" → REGEX). We emit a
    case-insensitive alternation over the individual terms so a title/abstract
    containing ANY term matches (coarse server-side filtering; the retrieval
    layer's post-filter + Stage-3 enrichment handle precision).
    """
    tokens = [t for t in re.split(r"\s+", terms.strip().lower()) if re.search(r"[a-z0-9]", t)]
    if not tokens:
        return None
    escaped = "|".join(re.escape(t) for t in tokens)
    return f"(?i).*(?:{escaped}).*"


def _build_query_payload(terms: str) -> dict:
    """Deep-copy the template and attach REGEX filters when terms exist."""
    payload = copy.deepcopy(EBRAINS_QUERY_TEMPLATE)
    pattern = _regex_filter_terms(terms)
    if not pattern:
        return payload
    # Filter the title and the abstract/description (documented REGEX op).
    for entry in payload["structure"]:
        path = entry.get("path", "")
        if path in ("https://openminds.ebrains.eu/vocab/fullName", "https://openminds.ebrains.eu/vocab/description"):
            entry["filter"] = {"op": "REGEX", "value": pattern}
    return payload


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
    async def _post_json(self, url: str, body: dict, params: dict) -> dict:
        resp = await self._client.post(url, json=body, params=params)
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
                terms = self.build_query_terms(req)
                page = 0
                max_pages = get_max_pages()
                size = min(PAGE_SIZE, max(req.limit, 1))
                offset = 0
                payload = _build_query_payload(terms)
                payload_ok = True  # degrades to an unfiltered query on rejection

                while len(records) < req.limit and page < max_pages:
                    await self._limiter.acquire()

                    params = {
                        "from": offset,
                        "size": size,
                        "stage": EBRAINS_QUERY_STAGE,
                        "returnTotalResults": "true",
                    }
                    try:
                        data = await self._post_json(EBRAINS_QUERY_URL, payload, params)
                    except httpx.HTTPStatusError as exc:
                        # The REGEX filter is best-effort: if the server rejects
                        # the filtered payload (4xx), retry once without filters
                        # so dataset retrieval still works.
                        if payload_ok and payload != EBRAINS_QUERY_TEMPLATE and 400 <= exc.response.status_code < 500:
                            logger.warning(
                                "EBRAINS rejected filtered query (%s) — retrying unfiltered",
                                exc.response.status_code,
                            )
                            payload_ok = False
                            payload = copy.deepcopy(EBRAINS_QUERY_TEMPLATE)
                            continue
                        raise

                    raw_data = data.get("data")
                    instances = raw_data if isinstance(raw_data, list) else []
                    total = data.get("total")
                    if isinstance(total, int):
                        total_available = total
                    elif not total_available:
                        total_available = len(instances)

                    for item in instances:
                        ds = normalize_repository(item, self.source_name)
                        if ds is not None:
                            records.append(ds)
                            if len(records) >= req.limit:
                                break

                    if not instances:
                        break
                    offset += len(instances)
                    # Stop when a short page is also the last page (total known),
                    # or when the server caps page size below our request and
                    # more results remain (offset still below total).
                    if len(instances) < size and (not total_available or offset >= total_available):
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
