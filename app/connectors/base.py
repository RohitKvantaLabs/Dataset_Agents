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
import re
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


# Modality synonym families — repository-native values are coarse (e.g.
# OpenNeuro stores ``mri`` for all MRI scans; DANDI ``Electrophysiology``),
# while the Query Parser emits precise terms (``fMRI``, ``sMRI``). A requested
# modality matches when it shares a synonym family with a declared value.
# Stabilization (Phase 2) — prevents coarse declared values from silently
# dropping every record for a specific request.
MODALITY_SYNONYMS: dict[str, frozenset[str]] = {
    "fmri": frozenset({"fmri", "mri", "functional mri", "functional magnetic resonance imaging"}),
    "smri": frozenset({"smri", "mri", "structural mri", "structural magnetic resonance imaging"}),
    "mri": frozenset({"mri", "fmri", "smri", "functional mri", "structural mri", "functional nuclear magnetic resonance", "functional nuclear magnetic resonance imaging"}),
    "eeg": frozenset({"eeg", "electroencephalography"}),
    "meg": frozenset({"meg", "magnetoencephalography"}),
    "ieeg": frozenset({"ieeg", "intracranial eeg", "ecog"}),
    "ecog": frozenset({"ecog", "ieeg", "intracranial eeg"}),
    "pet": frozenset({"pet", "positron emission tomography"}),
    "dti": frozenset({"dti", "diffusion mri", "diffusion tensor imaging", "mri"}),
    "nirs": frozenset({"nirs", "fnirs", "functional near-infrared spectroscopy"}),
    "fnirs": frozenset({"fnirs", "nirs"}),
}


def _modality_overlap(requested: str, declared: str) -> bool:
    """True when two modality strings share a synonym family or match directly."""
    r, d = requested.lower().strip(), declared.lower().strip()
    if not r or not d:
        return False
    if r == d:
        return True
    fam_r = MODALITY_SYNONYMS.get(r)
    fam_d = MODALITY_SYNONYMS.get(d)
    if fam_r and fam_d:
        return bool(fam_r & fam_d)
    if fam_r:
        return d in fam_r
    if fam_d:
        return r in fam_d
    # Fuzzy fallback: one is a prefix of the other (e.g. "mri" vs "functional mri")
    return r in d or d in r


# Neuroscience-evidence tokens used by the Issue-4 evidence gate. When a
# specific modality is explicitly requested and a candidate declares NO
# modality, the candidate is kept only if it carries some neuroscience
# evidence in its title/description/keywords (Stage 3 can still enrich it).
# Records with neither modality nor neuroscience evidence are unrelated to
# the request and filtered. Substring matching is deliberately generous
# (false positives only ever KEEP a record — never drop one), so repository
# coverage is not reduced for anything with a neuroscience signal.
NEUROSCIENCE_EVIDENCE_TERMS: frozenset[str] = frozenset(
    {
        "meg", "magnetoencephalography", "magnetoencephalogram",
        "eeg", "electroencephalography", "electroencephalogram",
        "ieeg", "ecog", "electrocorticography", "electrophysiology",
        "electrophysiological", "spikes", "spiking", "spike",
        "mri", "fmri", "smri", "magnetic resonance", "neuroimaging",
        "pet", "positron emission tomography", "dti", "nirs", "fnirs",
        "diffusion tensor", "connectome", "functional connectivity",
        "brain", "cerebral", "cerebellar", "cerebellum", "cortex",
        "cortical", "hippocampus", "hippocampal", "amygdala", "thalamus",
        "striatum", "neural", "neuronal", "neuron", "neurons",
        "neuroscience", "neurological", "neurodegenerative",
        "neuropsychiatric", "neurodevelopmental", "neuromodulation",
        "parkinson", "alzheimer", "dementia", "epilepsy", "seizure",
        "schizophrenia", "stroke", "resting-state", "task-fmri",
    }
)


def _has_modality_or_neuroscience_evidence(dataset: RepositoryDataset) -> bool:
    """True when a candidate declares any modality OR carries neuroscience-
    evidence terms in its title/description/keywords (Issue 4)."""
    if dataset.modality:
        return True
    text = " ".join(
        [dataset.title or "", dataset.description or "", " ".join(dataset.keywords or [])]
    ).lower()
    return any(tok in text for tok in NEUROSCIENCE_EVIDENCE_TERMS)


def post_filter(dataset: RepositoryDataset, filters: QueryFilters) -> bool:
    """
    Client-side post-filter for connectors whose server search is coarse
    (§2.4 OpenNeuro / §2.5 DANDI / §2.12 NITRC — modality & species).

    A candidate is kept when:
    - it has no declared value for a requested dimension AND no contrary
      evidence, or
    - at least one declared value overlaps the requested values (modality
      synonym families included — see MODALITY_SYNONYMS).

    Issue 4 (query-first stabilization): when a specific modality is
    explicitly requested and the candidate declares NO modality at all, the
    candidate is kept only if it carries neuroscience evidence (Stage 3
    enrichment can still fill the modality). Candidates with neither modality
    nor neuroscience evidence are unrelated to the request and are filtered —
    this removes loosely-related generic-repository records without reducing
    coverage for anything with a neuroscience signal.
    """
    if filters.modality:
        requested = {m.lower() for m in filters.modality}
        if dataset.modality:
            declared = {m.lower() for m in dataset.modality}
            if not any(_modality_overlap(r, d) for r in requested for d in declared):
                return False
        elif not _has_modality_or_neuroscience_evidence(dataset):
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

    async def fetch(self, limit: int = 200) -> list[dict]:
        """
        Fetch raw metadata records from the upstream repository.

        Stabilization (Phase 4): the default implementation routes batch sync
        through the connector's own ``search()`` with a broad, filter-free
        query and returns the raw upstream payloads (preserved in
        ``RepositoryDataset.raw``) so the batch normalizer maps them exactly
        like online retrieval. Connectors with a native batch endpoint (e.g.
        DANDI, OpenNeuro) override this for a purpose-built fetch.

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
        result = await self.search(
            SearchRequest(
                query="",
                filters=QueryFilters(raw_query=""),
                limit=limit,
            )
        )
        return [r.raw for r in result.records if r.raw]

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

        Stabilization (Phase 2): repository APIs receive NORMALIZED semantic
        terms from the Query Parser, never raw misspelled keywords. Structured
        fields (modality → task → condition → region → species → age_range)
        are prioritized over free-text keywords; typo-preserved keywords
        (3+ consecutive identical letters, e.g. ``fMRRri`` → ``rrr``) and
        keywords that duplicate a structured value are dropped so they cannot
        dominate repository search requests. Falls back to the raw query only
        when nothing structured remains.
        """
        filters = req.filters

        # Structured fields in priority order (§ plan: modality, task, disease,
        # brain region, species, age group, keywords).
        structured: list[str] = []
        if filters.modality:
            structured.extend(m for m in filters.modality if m)
        if filters.task:
            structured.append(filters.task)
        if filters.condition:
            structured.extend(c for c in filters.condition if c)
        if filters.region:
            structured.append(filters.region)
        if filters.species:
            structured.extend(s for s in filters.species if s)
        if filters.age_range:
            structured.append(filters.age_range)

        normalized_lower = {str(t).strip().lower() for t in structured if str(t).strip()}

        # Keywords only as a last-resort filler — and only when they are not
        # typo fragments (3+ identical consecutive letters) or duplicates of an
        # already-included structured value.
        for kw in filters.keywords:
            k = str(kw).strip()
            if not k:
                continue
            kl = k.lower()
            if kl in normalized_lower:
                continue
            if re.search(r"(.)\1{2,}", kl):  # typo signal: 3+ same letters in a row
                continue
            structured.append(k)

        return " ".join(dict.fromkeys(structured))[:500] or req.query
