"""
Ingestion Pipeline — Sync → Normalize → Score → Embed → Upsert.

This module is the top-level controller invoked by the cron endpoints
(and optionally by CLI scripts). It orchestrates:
  1. Fetch raw records from a connector.
  2. Normalize each record into the Common Schema (Dataset).
  3. Score the dataset using the ranking engine.
  4. Optionally generate a vector embedding.
  5. Upsert into MongoDB (idempotent via original_url key).

Design notes
------------
- The pipeline processes one source at a time but can be called in
  parallel for multiple sources by the cron handler.
- Errors in individual records are swallowed and logged so a bad record
  never aborts the entire batch.
- The embedder is optional; if HF_TOKEN is absent, the ``embedding``
  field is simply omitted from the upsert payload.
"""
import logging
from dataclasses import dataclass, field
from typing import Optional

from app.connectors.base import BaseConnector
from app.db.repositories.dataset_repository import bulk_upsert
from app.ingestion.embedder import Embedder
from app.ingestion.normalizer import normalize
from app.models.dataset import Dataset
from app.ingestion.scorer import score_dataset

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
    Execute the full ingestion pipeline for a single source connector.

    Parameters
    ----------
    connector:
        An initialised BaseConnector subclass (DANDI, OpenNeuro, …).
    limit:
        Maximum records to fetch from the upstream source.
    embed:
        If True (and HF_TOKEN is set), compute vector embeddings.
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

    # --- Steps 2–4: Normalize → Score → Embed (collect in list) ---
    batch: list[Dataset] = []
    for raw in raw_records:
        dataset: Optional[Dataset] = None
        try:
            # 2. Normalize
            dataset = normalize(raw, source_name=connector.source_name)
            if dataset is None:
                result.skipped += 1
                continue
            result.normalized += 1

            # 3. Score
            dataset.quality_score = score_dataset(dataset)

            # 4. Embed (best-effort)
            if embedder and result.embedding_enabled:
                vector = embedder.embed_dataset_text(dataset.title, dataset.description)
                if vector:
                    dataset.__pydantic_extra__ = dataset.__pydantic_extra__ or {}
                    dataset.__pydantic_extra__["embedding"] = vector

            batch.append(dataset)

        except Exception as exc:  # noqa: BLE001
            result.errors += 1
            url = str(getattr(dataset, "original_url", "unknown"))
            sample = f"url={url} err={exc}"
            logger.warning("Pipeline[%s]: error processing record: %s", connector.source_name, sample)
            if len(result.error_samples) < 5:
                result.error_samples.append(sample)

    # --- Step 5: Batch upsert (single bulkWrite round-trip per chunk) ---
    if batch:
        result.upserted = await bulk_upsert(batch)

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
