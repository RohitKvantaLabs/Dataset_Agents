"""
Repository Retrieval Service (§2.16).

Aggregates the enabled repository connectors into a single candidate
pool *before* quality scoring. All candidates from all enabled sources
are pooled, then passed (by the caller — the quality pipeline in
Phase 3) to the 7-stage quality pipeline for dedup + scoring as ONE set.

Per-connector isolation: a connector error (HTTP/timeout/circuit-open)
returns an empty SearchResult; it never aborts the other connectors.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field

from app.config import get_settings
from app.connectors.base import (
    SearchRequest,
    SearchResult,
    _has_modality_or_neuroscience_evidence,
)
from app.connectors.registry import get_connector, get_enabled_sources
from app.models.query_filters import QueryFilters
from app.models.repository_dataset import RepositoryDataset

logger = logging.getLogger("neuro_platform.services.repository_retrieval")


@dataclass
class AggregateResult:
    """Aggregated candidate pool returned by aggregate_repository_search."""

    query: str
    records: list[RepositoryDataset] = field(default_factory=list)
    per_source: dict[str, SearchResult] = field(default_factory=dict)
    sources_queried: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    elapsed_ms: int = 0
    total_available: int = 0


async def aggregate_repository_search(
    query: str,
    filters: QueryFilters,
    sources: list[str] | None = None,
    limit_per_source: int | None = None,
) -> AggregateResult:
    """
    Run enabled connectors in parallel and aggregate their candidates.

    Parameters
    ----------
    query:
        Raw user query text.
    filters:
        Parsed structured intent (QueryFilters).
    sources:
        Optional explicit source list; defaults to ``get_enabled_sources()``.
    limit_per_source:
        Max records to request per source (defaults to
        ``REPO_SEARCH_LIMIT_PER_SOURCE`` config).
    """
    settings = get_settings()
    start = time.monotonic()
    enabled = sources or get_enabled_sources()
    limit = limit_per_source or settings.REPO_SEARCH_LIMIT_PER_SOURCE

    req = SearchRequest(query=query, filters=filters, limit=limit)

    async def run_one(source: str) -> tuple[str, SearchResult]:
        try:
            connector = get_connector(source)
            result = await connector.search(req)
            return source, result
        except Exception as exc:  # noqa: BLE001 — per-connector isolation
            logger.error("Repository source %r raised during search: %s", source, exc)
            return source, SearchResult(
                source=source,
                query=query,
                status="offline",
                error=str(exc),
            )

    results = await asyncio.gather(*[run_one(s) for s in enabled])

    aggregate = AggregateResult(query=query, sources_queried=enabled)
    for source, result in results:
        aggregate.per_source[source] = result
        aggregate.records.extend(result.records)
        aggregate.total_available += result.total_available
        if result.error:
            aggregate.errors.append(f"{source}: {result.error}")

    # Issue 4 (query-first stabilization) — uniform evidence gate. Only the
    # connectors with coarse server search call ``post_filter`` client-side
    # (openneuro/dandi/neurovault/nitrc); generic repositories
    # (zenodo/figshare/dryad/osf/ebrains) do not. When a specific modality is
    # explicitly requested, a candidate declaring no modality AND carrying no
    # neuroscience evidence is unrelated to the request — filter it here so the
    # gate applies uniformly. Idempotent for sources that already filtered.
    # No other post-filter semantics are applied (modality/species overlap
    # stays connector-level, unchanged).
    if filters.modality:
        aggregate.records = [
            r for r in aggregate.records
            if r.modality or _has_modality_or_neuroscience_evidence(r)
        ]

    aggregate.elapsed_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "repository_search aggregate: sources=%d records=%d errors=%d elapsed_ms=%d",
        len(enabled),
        len(aggregate.records),
        len(aggregate.errors),
        aggregate.elapsed_ms,
    )
    return aggregate
