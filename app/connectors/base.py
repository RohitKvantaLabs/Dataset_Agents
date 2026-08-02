"""
Connector base interface.

All external data-source connectors (OpenNeuro, DANDI, etc.) must
implement this abstract class. Two capabilities:

- ``fetch(limit)`` — batch sync (existing path, ingestion pipeline).
- ``search(req)`` — online structured retrieval (§2.3, Phase 2). Returns
  a ``SearchResult`` whose ``records`` are already normalized to the
  ``RepositoryDataset`` schema via ``normalize_repository()``.

Per-connector execution contract (§2.3.1):
- HTTP client: ``httpx.AsyncClient``, timeout = ``REQUEST_TIMEOUT_SECONDS``.
- Retry: tenacity, 2 retries, exponential backoff 0.5 → 2 s, ``reraise=True``.
- Circuit breaker: one module-level breaker per connector (3 failures, 30 s).
- Rate limiting: async token bucket per connector (default 2 req/s, burst 5;
  override via ``REPO_RATE_LIMIT_<SOURCE>`` env var).
- Pagination: follow repo cursor/page until *limit* reached or exhausted;
  cap pages at ``REPO_MAX_PAGES``.
- Partial failure: a connector error returns an empty ``SearchResult`` with
  ``status="offline"`` + ``error`` set and is logged; it never aborts the
  other connectors (per-connector isolation).
"""
import asyncio
import logging
import os
import time
from abc import ABC, abstractmethod

import httpx
from pydantic import BaseModel, Field
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from app.config import get_settings
from app.models.query_filters import QueryFilters
from app.models.repository_dataset import RepositoryDataset

logger = logging.getLogger("neuro_platform.connectors.base")


class SearchRequest(BaseModel):
    query: str                        # raw user query text
    filters: QueryFilters             # parsed structured intent
    limit: int = 10                   # max records to return


class SearchResult(BaseModel):
    source: str
    query: str
    total_available: int = 0          # repo-reported total (if any)
    records: list[RepositoryDataset] = Field(default_factory=list)
    elapsed_ms: int = 0
    truncated: bool = False           # True if repo has more than we fetched
    # Additive status fields — carrier for §2.7's "offline status with
    # reason" requirement (e.g. EBRAINS key missing, connector failure).
    status: str = "ok"                # "ok" | "offline"
    error: str | None = None          # reason when status == "offline"


def _is_transient(exc: BaseException) -> bool:
    """
    Retry only genuinely transient failures: network errors and 429/5xx.
    Deterministic 4xx (400/403/404) fail immediately so connector-level
    fallbacks (e.g. NeuroVault's ?search= rejection) kick in without
    burning the retry budget.
    """
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status == 429 or status >= 500
    return False


def connector_retry(fn):
    """
    §2.3.1 retry policy: 2 retries, exponential backoff 0.5 → 2 s, reraise.

    Retries only transient failures (network errors, 429, 5xx) — deterministic
    4xx errors propagate immediately. Applied to the raw HTTP calls *inside*
    the circuit-breaker context so a known-down API is fast-failed by the
    breaker instead of retry-looped.
    """
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, exp_base=2, min=0.5, max=2.0),
        reraise=True,
        retry=retry_if_exception(_is_transient),
    )(fn)


def get_rate_limit(source: str) -> float:
    """
    Per-source rate limit in req/s (§2.3.1).

    ``REPO_RATE_LIMIT_<SOURCE>`` env var overrides the global
    ``REPO_RATE_LIMIT_PER_SOURCE`` default (2 req/s).
    """
    settings = get_settings()
    default = float(getattr(settings, "REPO_RATE_LIMIT_PER_SOURCE", 2) or 2)
    raw = os.environ.get(f"REPO_RATE_LIMIT_{source.upper()}")
    if raw:
        try:
            return max(float(raw), 0.01)
        except ValueError:
            logger.warning("Invalid REPO_RATE_LIMIT_%s=%r — using default", source.upper(), raw)
    return default


def get_max_pages() -> int:
    """Pagination cap per connector (§2.3.1)."""
    settings = get_settings()
    return max(int(getattr(settings, "REPO_MAX_PAGES", 5) or 5), 1)


class TokenBucket:
    """
    Async token-bucket rate limiter (per-connector, §2.3.1).

    Refills at *rate* tokens/sec up to *burst*. ``acquire()`` blocks until
    a token is available.
    """

    def __init__(self, rate: float, burst: int = 5):
        self._rate = max(rate, 0.001)
        self._burst = max(burst, 1)
        self._tokens = float(self._burst)
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()  # not loop-bound until first use (py3.10+)

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(self._burst, self._tokens + (now - self._updated) * self._rate)
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
                self._tokens = 0.0
            await asyncio.sleep(wait)


def post_filter(dataset: RepositoryDataset, filters: QueryFilters) -> bool:
    """
    Client-side post-filter for connectors whose server search is coarse
    (§2.4 OpenNeuro / §2.5 DANDI / §2.12 NITRC — modality & species).

    A candidate is kept when:
    - it has no declared value for a requested dimension (Stage 3 fills it), or
    - at least one declared value overlaps the requested values.
    """
    if filters.modality and dataset.modality:
        requested = {m.lower() for m in filters.modality}
        declared = {m.lower() for m in dataset.modality}
        if not (requested & declared):
            return False
    if filters.species and dataset.species:
        requested = {s.lower() for s in filters.species}
        declared = {s.lower() for s in dataset.species}
        if not (requested & declared):
            return False
    return True


class BaseConnector(ABC):
    """Structural interface every source connector must satisfy."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """
        Human-readable repository name stored in Dataset.source.
        Example: "openneuro" | "dandi"
        """
        ...

    @property
    def searchable(self) -> bool:
        """Whether this connector supports online structured search (§2.3)."""
        return True

    @abstractmethod
    async def fetch(self, limit: int = 200) -> list[dict]:
        """
        Fetch raw metadata records from the upstream repository.

        Parameters
        ----------
        limit:
            Maximum number of records to pull in one call.

        Returns
        -------
        list[dict]
            Raw upstream records; field names vary by source. The
            normalizer handles the mapping to Common Schema.
        """
        ...

    @abstractmethod
    async def search(self, req: SearchRequest) -> SearchResult:
        """
        Repository-native structured search → normalized RepositoryDataset list (§2.3).

        Never raises: a failure (HTTP/timeout/circuit-open) returns an empty
        ``SearchResult`` with ``status="offline"`` + ``error`` so the caller
        (aggregation) can isolate this source without aborting the others.
        """
        ...

    def build_query_terms(self, req: SearchRequest) -> str:
        """
        Derive the free-text query string a repo's search endpoint accepts.

        Order: keywords → condition → task → modality → region → species,
        deduplicated, truncated to 500 chars; falls back to the raw query.
        """
        terms: list[str] = [t for t in req.filters.keywords if t]
        if req.filters.condition:
            terms.extend(c for c in req.filters.condition if c)
        if req.filters.task:
            terms.append(req.filters.task)
        if req.filters.modality:
            terms.extend(m for m in req.filters.modality if m)
        if req.filters.region:
            terms.append(req.filters.region)
        if req.filters.species:
            terms.extend(s for s in req.filters.species if s)
        return " ".join(dict.fromkeys(terms))[:500] or req.query
