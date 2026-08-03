"""
Ingestion Pipeline — batch sync path (§4.8/§4.9).

This module is the batch-sync controller invoked by the repository-sync
endpoint, the ingest-repositories cron, and the admin resync trigger. It
orchestrates, per source:
  1. Fetch raw records from a connector (`fetch(limit)`).
  2. Normalize each record into the intermediate RepositoryDataset schema
     via the rewritten normalizer (`normalize_repository` — all 9 sources).
  3. Run the 7-stage quality pipeline (Filter → Classify → Enrich → Verify
     → Score → Dedup → Publish) so batch sync goes through the SAME quality
     path as online retrieval (§3.0: "one quality path").
  4. Optional best-effort embedding (parity with the pre-existing pipeline).

Design notes
------------
- Errors in individual records are swallowed and logged so a bad record
  never aborts the entire batch.
- The embedder is optional; if HF_TOKEN is absent, ``embedding`` is skipped.
- §4.9: uses the rewritten normalizer and accepts RepositoryDataset-shaped
  raw records (previously the batch `normalize()` only covered dandi/openneuro).
"""
import logging
from dataclasses import dataclass, field
from typing import Optional

from app.connectors.base import BaseConnector
from app.ingestion.embedder import Embedder
from app.ingestion.normalizer import normalize_repository
from app.ingestion.quality_pipeline import StageStats, run_quality_pipeline

logger = logging.getLogger("neuro_platform.ingestion.pipeline")


@dataclass
class PipelineResult:
    """Summary returned by run_pipeline for observability / cron responses."""

    source: str
    fetched: int = 0
    normalized: int = 0
    upserted: int = 0
    errors: int = 0
    skipped: int = 0
    embedding_enabled: bool = False
    error_samples: list[str] = field(default_factory=list)


async def run_pipeline(
    connector: BaseConnector,
    limit: int = 200,
    embed: bool = True,
    embedder: Optional[Embedder] = None,
) -> PipelineResult:
    """
    Execute the batch-sync pipeline for a single source connector.

    Parameters
    ----------
    connector:
        An initialised BaseConnector subclass (DANDI, OpenNeuro, …).
    limit:
        Maximum records to fetch from the upstream source.
    embed:
        If True (and HF_TOKEN is set), compute vector embeddings
        (best-effort; parity with the pre-existing pipeline).
    embedder:
        Optional pre-built Embedder; created internally if not supplied.

    Returns
    -------
    PipelineResult
        Summary of the run for logging / HTTP response.
    """
    result = PipelineResult(source=connector.source_name)

    # --- Step 0: Prepare embedder ---
    if embed and embedder is None:
        embedder = Embedder()
    result.embedding_enabled = embedder is not None and embedder._client is not None  # noqa: SLF001

    # --- Step 1: Fetch ---
    logger.info("Pipeline[%s]: fetching up to %d records", connector.source_name, limit)
    try:
        raw_records = await connector.fetch(limit=limit)
    except Exception as exc:  # noqa: BLE001
        logger.error("Pipeline[%s]: fetch failed: %s", connector.source_name, exc)
        result.errors += 1
        result.error_samples.append(f"fetch: {exc}")
        return result

    result.fetched = len(raw_records)
    logger.info("Pipeline[%s]: fetched %d records", connector.source_name, result.fetched)

    # --- Step 2: Normalize into RepositoryDataset (rewritten normalizer, §2.2) ---
    candidates = []
    for raw in raw_records:
        try:
            ds = normalize_repository(raw, connector.source_name)
        except Exception as exc:  # noqa: BLE001
            result.errors += 1
            logger.warning(
                "Pipeline[%s]: normalize raised for record: %s",
                connector.source_name,
                exc,
            )
            if len(result.error_samples) < 5:
                result.error_samples.append(f"normalize: {exc}")
            continue
        if ds is None:
            result.skipped += 1
            continue
        candidates.append(ds)
    result.normalized = len(candidates)

    # --- Step 3: Quality pipeline (stages 1–7, publish=True) ---
    # Same quality path as online retrieval: filter, classify, enrich, verify,
    # score, dedup, and publish (atomic bulk upsert + provenance).
    if candidates:
        pipeline_result = await run_quality_pipeline(
            candidates, publish=True, discovery_method="batch_sync"
        )
        result.upserted = pipeline_result.stages.get("publish", StageStats()).accepted
        result.errors += len(pipeline_result.errors)
        result.error_samples.extend(pipeline_result.errors[: max(0, 5 - len(result.error_samples))])

        # --- Step 4: Embed (best-effort, parity with previous pipeline) ---
        # Note: Dataset.embedding is `exclude=True` and the model does not allow
        # extra fields, so persistence is subject to the pre-existing mechanism.
        if result.embedding_enabled:
            for dataset in pipeline_result.datasets:
                vector = embedder.embed_dataset_text(dataset.title, dataset.description)  # noqa: SLF001
                if vector:
                    dataset.__pydantic_extra__ = dataset.__pydantic_extra__ or {}
                    dataset.__pydantic_extra__["embedding"] = vector

    logger.info(
        "Pipeline[%s]: done | fetched=%d normalized=%d upserted=%d errors=%d skipped=%d",
        connector.source_name,
        result.fetched,
        result.normalized,
        result.upserted,
        result.errors,
        result.skipped,
    )
    return result
