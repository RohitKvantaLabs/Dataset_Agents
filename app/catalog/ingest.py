"""
Full OpenNeuro metadata catalog ingestion (spec §7).

Reads ALL currently public OpenNeuro datasets through the official GraphQL
API using cursor pagination. Metadata only — no dataset files are ever
downloaded. Every record flows through:

    fetch node → normalize → validate → dedup → insert/merge canonical

The pipeline writes ONLY to the dedicated ``neurosearch_dataset_catalog``
collection. It never touches the production ``datasets`` collection, the
search pipeline, ranking, or any other subsystem.

Rate-limit handling (validated in Phase 2):
- HTTP failures are retried with exponential backoff (tenacity).
- Per-node GraphQL errors (some catalog datasets return partial nodes) are
  tolerated: the broken node is skipped and counted, the page survives.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import time
from datetime import datetime, timezone

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from app.catalog.normalize import build_source_record
from app.catalog.persistence import get_collection, upsert_canonical
from app.catalog.schema import CATALOG_COLLECTION

logger = logging.getLogger("neuro_platform.catalog.ingest")

GRAPHQL_URL = "https://openneuro.org/crn/graphql"

# Rich metadata query validated against the live 5.4.0 schema in the
# Phase 1/2 experiments (probe_openneuro_schema*.py + live runs).
DATASETS_QUERY = """\
query FetchDatasets($first: Int!, $after: String) {
  datasets(first: $first, after: $after) {
    edges {
      node {
        id
        name
        created
        publishDate
        public
        uploader { id name }
        metadata {
          datasetId
          datasetName
          datasetUrl
          modalities
          species
          ages
          studyDesign
          studyDomain
          studyLongitudinal
          tasksCompleted
          trialCount
          associatedPaperDOI
          grantIdentifier
          grantFunderName
          dataProcessed
          dxStatus
          firstSnapshotCreatedAt
          latestSnapshotCreatedAt
        }
        latestSnapshot {
          id
          tag
          created
          hexsha
          size
          readme
          description {
            Name
            Authors
            BIDSVersion
            DatasetDOI
            DatasetType
            EthicsApprovals
            Funding
            HowToAcknowledge
            License
            ReferencesAndLinks
            SeniorAuthor
          }
          contributors {
            name
            givenName
            familyName
            contributorType
            orcid
          }
          summary {
            dataProcessed
            modalities
            primaryModality
            secondaryModalities
            sessions
            size
            subjects
            tasks
            totalFiles
          }
          related {
            description
            kind
            relation
          }
        }
        snapshots {
          id
          tag
          created
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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def offset_cursor(offset: int) -> str:
    """OpenNeuro cursor = base64 JSON ``{"offset": N}`` (verified in Phase 1)."""
    return base64.b64encode(json.dumps({"offset": offset}).encode()).decode()


def make_fetch_page(retry_counter: list[int]):
    """Build the page fetcher with tenacity retry logging.

    ``retry_counter`` is appended to on every actual retry attempt so the
    report can distinguish real retries from per-node GraphQL errors.
    """

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=lambda retry_state: retry_counter.append(1),
    )
    async def fetch_page(client: httpx.AsyncClient, first: int, after: str | None) -> tuple[dict, list]:
        """Single page of datasets. Returns (connection, errors).

        A page may contain per-node GraphQL errors for broken catalog datasets
        — those are returned as ``errors`` and skipped by the caller; the page
        is never discarded wholesale.
        """
        variables: dict = {"first": first}
        if after:
            variables["after"] = after
        resp = await client.post(GRAPHQL_URL, json={"query": DATASETS_QUERY, "variables": variables})
        resp.raise_for_status()
        body = resp.json()
        errors = body.get("errors") or []
        connection = (body.get("data") or {}).get("datasets", {})
        return connection, errors

    return fetch_page


async def run_openneuro_ingestion(
    db,
    *,
    target: int | None = None,
    page_size: int = 25,
    page_delay: float = 0.4,
    start_offset: int = 0,
    log=print,
) -> dict:
    """Ingest all public OpenNeuro datasets into the canonical catalog.

    Returns a full stats dict (used by the report module).
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "openneuro",
        "discovered": 0,
        "retrieved": 0,
        "normalized": 0,
        "validated_ok": 0,
        "validation_failed": 0,
        "validation_errors": [],
        "inserted": 0,
        "merged": 0,
        "matched_via": {},
        "skipped_broken_nodes": 0,
        "per_node_graphql_errors": 0,
        "failed": 0,
        "failure_details": [],
        "api_failures": 0,
        "api_error_details": [],
        "retries": 0,
        "pages": 0,
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "start_offset": start_offset,
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
        "target": target,
    }

    t0 = time.monotonic()
    cursor: str | None = offset_cursor(start_offset) if start_offset else None
    consecutive_failures = 0
    retry_counter: list[int] = []
    fetch_page = make_fetch_page(retry_counter)
    async with httpx.AsyncClient(timeout=90) as client:
        while True:
            if target is not None and stats["retrieved"] >= target:
                break
            stats["pages"] += 1
            try:
                connection, errors = await fetch_page(client, page_size, cursor)
                consecutive_failures = 0
            except Exception as exc:  # noqa: BLE001 — page-level failure
                stats["api_failures"] += 1
                stats["api_error_details"].append(
                    f"page {stats['pages']}: {type(exc).__name__}: {exc}"
                )
                log(f"[ingest] page {stats['pages']} failed: {type(exc).__name__}: {exc}")
                # The cursors are offset-based, so a failed page must NOT abort
                # the whole run — skip ahead to the next offset and continue.
                # A consecutive-failure cap prevents an infinite loop.
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    log("[ingest] too many consecutive page failures — aborting")
                    break
                next_offset = start_offset + stats["discovered"]
                cursor = offset_cursor(next_offset)
                await asyncio.sleep(page_delay * 2)
                continue

            stats["retries"] = len(retry_counter)
            if errors:
                # Some nodes in the catalog return per-node GraphQL errors;
                # the broken nodes are skipped, the page survives.
                stats["per_node_graphql_errors"] += len(errors)

            edges = connection.get("edges", [])
            page_info = connection.get("pageInfo", {}) or {}
            stats["discovered"] += len(edges)
            log(
                f"[ingest] page {stats['pages']}: edges={len(edges)} "
                f"hasNext={page_info.get('hasNextPage')} errors={len(errors)}"
            )

            for edge in edges:
                node = (edge or {}).get("node") if isinstance(edge, dict) else None
                if not node or not node.get("id"):
                    stats["skipped_broken_nodes"] += 1
                    continue
                stats["retrieved"] += 1

                # 1) Normalize into the per-source canonical record.
                try:
                    source = build_source_record(node)
                except Exception as exc:  # noqa: BLE001
                    stats["failed"] += 1
                    stats["failure_details"].append(f"{node.get('id')}: normalize: {exc}")
                    continue
                stats["normalized"] += 1

                # 2) Validate the normalized record (invariants).
                #    We validate the source-level derived fields by
                #    constructing a standalone check: ageGroup/ages, DOI,
                #    license are validated at canonical level after merge —
                #    here we validate the per-source invariants.
                errs = _validate_source(source)
                if errs:
                    stats["validation_failed"] += 1
                    stats["validation_errors"].append(
                        f"{node.get('id')}: {errs[0]}"
                    )
                    log(f"[ingest] validation failed for {node.get('id')}: {errs}")
                    continue
                stats["validated_ok"] += 1

                # 3) Dedup BEFORE inserting a new canonical record.
                try:
                    result = await upsert_canonical(collection, source)
                except Exception as exc:  # noqa: BLE001
                    stats["failed"] += 1
                    stats["failure_details"].append(f"{node.get('id')}: upsert: {exc}")
                    log(f"[ingest] upsert failed for {node.get('id')}: {exc}")
                    continue

                if result.get("action") == "invalid":
                    stats["validation_failed"] += 1
                    stats["validation_errors"].append(
                        f"{node.get('id')}: {result.get('errors', [])[:2]}"
                    )
                    log(f"[ingest] canonical validation failed for {node.get('id')}")
                    continue

                if result["action"] == "inserted":
                    stats["inserted"] += 1
                else:
                    stats["merged"] += 1
                    via = result.get("matchedVia") or "unknown"
                    stats["matched_via"][via] = stats["matched_via"].get(via, 0) + 1

                # Ambiguity accounting (identities intentionally NOT merged).
                ambiguous = result.get("ambiguousCandidates") or []
                if ambiguous:
                    stats["ambiguous_candidates"] += len(ambiguous)
                    if len(stats["ambiguous_sample"]) < 10:
                        stats["ambiguous_sample"].append(
                            {
                                "source": f"{source['repository']}:{source['sourceDatasetId']}",
                                "candidates": [
                                    c.get("canonicalDatasetId") for c in ambiguous[:3]
                                ],
                            }
                        )

                if target is not None and stats["retrieved"] >= target:
                    break

            if not page_info.get("hasNextPage") or not edges:
                break
            cursor = page_info.get("endCursor")
            await asyncio.sleep(page_delay)

    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[ingest] done: discovered={stats['discovered']} retrieved={stats['retrieved']} "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"pages={stats['pages']} elapsed={stats['elapsed_s']}s"
    )
    return stats


def _validate_source(source: dict) -> list[str]:
    """Per-source validation: required identity fields + derived consistency.

    Full canonical-record validation happens after merge; this guard catches
    malformed source records before any DB write.
    """
    errs: list[str] = []
    if not source.get("sourceDatasetId"):
        errs.append("missing sourceDatasetId")
    if not source.get("sourceUrl"):
        errs.append("missing sourceUrl")
    pc = source.get("participantCount")
    if pc is not None and (not isinstance(pc, int) or pc < 0):
        errs.append(f"invalid participantCount {pc}")
    ages = source.get("ages")
    age_group = source.get("ageGroup")
    if age_group and not ages:
        errs.append("ageGroup present without ages (fabrication guard)")
    return errs
