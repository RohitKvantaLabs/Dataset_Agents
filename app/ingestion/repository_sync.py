"""
Repository Sync Service (§4.8).

Batch-fetch wrapper used by ``POST /agents/repository-sync`` and
``GET /cron/ingest-repositories``: for one or all enabled sources, run
``run_pipeline`` (fetch → normalize_repository → quality pipeline →
publish) and return per-source ``PipelineResult``.

Sources run sequentially — bounded and Vercel-serverless-safe (each
connector has its own rate limiter, so sequential avoids bursting 9
repositories at once).
"""
import logging

from app.connectors.registry import get_connector, get_enabled_sources
from app.ingestion.pipeline import PipelineResult, run_pipeline

logger = logging.getLogger("neuro_platform.ingestion.repository_sync")

# Batch default when the caller omits limit_per_source (matches the legacy
# run_pipeline default of 200 records per source).
DEFAULT_SYNC_LIMIT = 200


async def run_repository_sync(
    source: str | None = None,
    limit_per_source: int | None = None,
    embed: bool = True,
) -> dict[str, PipelineResult]:
    """
    Run the batch pipeline for one or all enabled sources.

    Parameters
    ----------
    source:
        Optional single source label. If None, all enabled sources are synced.
    limit_per_source:
        Max records to fetch per source (default 200).
    embed:
        Best-effort embedding toggle (default True).

    Returns
    -------
    dict[str, PipelineResult]
        ``{source: PipelineResult}`` for every source attempted.
    """
    sources = [source] if source else get_enabled_sources()
    limit = limit_per_source or DEFAULT_SYNC_LIMIT
    results: dict[str, PipelineResult] = {}

    for src in sources:
        try:
            connector = get_connector(src)
            results[src] = await run_pipeline(connector, limit=limit, embed=embed)
        except Exception as exc:  # noqa: BLE001 — one bad source never aborts the sync
            logger.error("repository_sync: source %r failed: %s", src, exc)
            results[src] = PipelineResult(
                source=src,
                errors=1,
                error_samples=[f"pipeline: {exc}"],
            )

    logger.info(
        "repository_sync done: sources=%d synced=%s",
        len(sources),
        ",".join(results),
    )
    return results
