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

from app.catalog.normalize import (
    build_dandi_source_record,
    build_nemar_source_record,
    build_neuromorpho_source_record,
    build_source_record,
    group_neuromorpho_neurons,
)
from app.catalog.persistence import get_collection, upsert_canonical
from app.catalog.schema import CATALOG_COLLECTION

logger = logging.getLogger("neuro_platform.catalog.ingest")

# DANDI Archive public REST API.
DANDI_API_BASE = "https://api.dandiarchive.org/api"

# NEMAR backend API (investigation 2026-08-10 — live v0.9.7).
NEMAR_API_BASE = "https://api.nemar.org"

# NeuroMorpho.Org public Solr API (investigation 2026-08-10).
NEUROMORPHO_API_BASE = "https://neuromorpho.org/api/neuron"

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


# ─────────────────────────────────────────────────────────────────────────────
# DANDI Phase-1 ingestion
#
# Flow (spec — DANDI scope):
#     list → Dandiset IDs → draft version endpoint → full Dandiset metadata
#     → normalization → canonical identity resolution → persistence
#
# Rich scientific metadata is read ONLY from the draft VERSION endpoint — never
# from the list endpoint. Phase 1 deliberately performs ZERO asset-level calls.
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_dandi_list_id(identifier) -> str | None:
    """Normalize a list-endpoint dandiset identifier ('DANDI:000003'/'000003') → '000003'."""
    if not identifier:
        return None
    return str(identifier).strip().replace("DANDI:", "").strip() or None


def make_dandi_fetchers(retry_counter: list[int]):
    """Build retry-wrapped DANDI fetchers.

    ``retry_counter`` is appended to on every actual retry attempt so the
    report can distinguish real retries from hard failures.
    """

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=lambda retry_state: retry_counter.append(1),
    )
    async def fetch_list_page(client: httpx.AsyncClient, url: str) -> dict:
        """One /api/dandisets/ page (list-only — no scientific metadata)."""
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.json()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=lambda retry_state: retry_counter.append(1),
    )
    async def fetch_version(client: httpx.AsyncClient, dandiset_id: str) -> dict:
        """GET /api/dandisets/{id}/versions/draft/ — the Phase-1 metadata source."""
        resp = await client.get(f"{DANDI_API_BASE}/dandisets/{dandiset_id}/versions/draft/")
        resp.raise_for_status()
        return resp.json()

    return fetch_list_page, fetch_version


async def run_dandi_ingestion(
    db,
    *,
    target: int | None = None,
    page_size: int = 100,
    page_delay: float = 0.6,
    list_page_delay: float = 0.3,
    client: httpx.AsyncClient | None = None,
    log=print,
) -> dict:
    """Ingest all public DANDI dandisets into the canonical catalog (Phase 1).

    Enumerates all Dandiset IDs, fetches the draft VERSION metadata for each,
    normalizes into the canonical per-source record, validates, then defers to
    the existing dedup → insert/merge persistence. ``target`` caps the number
    of version records processed (e.g. ``--limit 10`` smoke test).

    NEVER calls any asset-level endpoint (``asset_calls`` stays 0).
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "dandi",
        "discovered": 0,
        "retrieved": 0,
        "normalized": 0,
        "validated_ok": 0,
        "validation_failed": 0,
        "validation_errors": [],
        "inserted": 0,
        "merged": 0,
        "matched_via": {},
        "failed": 0,
        "failure_details": [],
        "api_failures": 0,
        "api_error_details": [],
        "retries": 0,
        "pages": 0,
        "list_requests": 0,
        "version_requests": 0,
        "asset_calls": 0,          # Phase 1 never touches asset endpoints
        "total_api_calls": 0,
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
        "target": target,
    }

    t0 = time.monotonic()
    retry_counter: list[int] = []
    fetch_list_page, fetch_version = make_dandi_fetchers(retry_counter)
    owns_client = client is None
    async with (client or httpx.AsyncClient(timeout=90)) as c:
        # ── 1) Enumerate all Dandiset IDs (list endpoint only). ─────────────
        dandisets: list[tuple[str, dict]] = []  # (dandiset_id, list_record)
        url: str | None = f"{DANDI_API_BASE}/dandisets/?page_size={page_size}"
        consecutive_failures = 0
        while url:
            if target is not None and len(dandisets) >= target:
                break
            stats["pages"] += 1
            stats["list_requests"] = stats["pages"]
            try:
                data = await fetch_list_page(c, url)
                consecutive_failures = 0
            except Exception as exc:  # noqa: BLE001 — page-level failure
                stats["api_failures"] += 1
                stats["api_error_details"].append(
                    f"list page {stats['pages']}: {type(exc).__name__}: {exc}"
                )
                log(f"[dandi] list page {stats['pages']} failed: {type(exc).__name__}: {exc}")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    log("[dandi] too many consecutive list failures — aborting enumeration")
                    break
                await asyncio.sleep(list_page_delay * 2)
                continue
            stats["retries"] = len(retry_counter)
            for rec in data.get("results") or []:
                ds_id = _normalize_dandi_list_id(rec.get("identifier"))
                if not ds_id:
                    continue
                dandisets.append((ds_id, rec))
                if target is not None and len(dandisets) >= target:
                    break
            stats["discovered"] = len(dandisets)
            url = data.get("next")
            if url:
                await asyncio.sleep(list_page_delay)

        # ── 2) Fetch + normalize + validate + dedup + persist each version. ─
        for ds_id, list_rec in dandisets:
            if target is not None and stats["retrieved"] >= target:
                break
            stats["version_requests"] += 1
            try:
                version = await fetch_version(c, ds_id)
            except Exception as exc:  # noqa: BLE001
                stats["api_failures"] += 1
                stats["failure_details"].append(f"{ds_id}: fetch_version: {type(exc).__name__}: {exc}")
                log(f"[dandi] version fetch failed for {ds_id}: {type(exc).__name__}: {exc}")
                continue
            stats["retrieved"] += 1
            stats["retries"] = len(retry_counter)

            published = list_rec.get("most_recent_published_version") or {}
            try:
                source = build_dandi_source_record(version, published)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"{ds_id}: normalize: {exc}")
                continue
            stats["normalized"] += 1

            errs = _validate_source(source)
            if errs:
                stats["validation_failed"] += 1
                stats["validation_errors"].append(f"{ds_id}: {errs[0]}")
                log(f"[dandi] validation failed for {ds_id}: {errs}")
                continue
            stats["validated_ok"] += 1

            try:
                result = await upsert_canonical(collection, source)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"{ds_id}: upsert: {exc}")
                log(f"[dandi] upsert failed for {ds_id}: {exc}")
                continue

            if result.get("action") == "invalid":
                stats["validation_failed"] += 1
                stats["validation_errors"].append(
                    f"{ds_id}: {result.get('errors', [])[:2]}"
                )
                log(f"[dandi] canonical validation failed for {ds_id}")
                continue

            if result["action"] == "inserted":
                stats["inserted"] += 1
            else:
                stats["merged"] += 1
                via = result.get("matchedVia") or "unknown"
                stats["matched_via"][via] = stats["matched_via"].get(via, 0) + 1

            ambiguous = result.get("ambiguousCandidates") or []
            if ambiguous:
                stats["ambiguous_candidates"] += len(ambiguous)
                if len(stats["ambiguous_sample"]) < 10:
                    stats["ambiguous_sample"].append(
                        {
                            "source": f"dandi:{source['sourceDatasetId']}",
                            "candidates": [
                                cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                            ],
                        }
                    )

            await asyncio.sleep(page_delay)  # rate limit — do not hammer DANDI

    stats["asset_calls"] = 0  # hard guarantee: no asset-level crawling
    stats["total_api_calls"] = stats["list_requests"] + stats["version_requests"]
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[dandi] done: discovered={stats['discovered']} retrieved={stats['retrieved']} "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"api_calls={stats['total_api_calls']} (asset_calls={stats['asset_calls']}) "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# NEMAR Phase-1 ingestion
#
# Flow (investigation + compatibility verdict, 2026-08-10):
#     enumerate api.nemar.org/datasets?limit=200&offset=<n>  (offset/limit ONLY;
#     the ``page`` param is a no-op on this API; limit is capped at 200)
#     → per-dataset detail api.nemar.org/datasets/{id}
#     → normalization → identity resolution → insert/merge
#
# Identity: OpenNeuro mirrors (source='openneuro', source_id='ds…') resolve to
# the EXISTING OpenNeuro canonical record via the generic cross-reference
# mechanism (synthesized openneuro.org/datasets/<id> URL → extract_cross_
# references → sourceKeys match). NEMAR-native (nm…, no source_id) become new
# canonical candidates keyed by their concept DOI.
#
# Phase 1 performs ZERO asset-level calls (asset_calls stays 0): the detail
# endpoint is sufficient for all canonical fields.
# ─────────────────────────────────────────────────────────────────────────────

NEMAR_LIST_PAGE_SIZE = 200  # server caps limit at 200 (verified live)


def make_nemar_fetchers(retry_counter: list[int]):
    """Build retry-wrapped NEMAR fetchers.

    ``retry_counter`` is appended to on every actual retry attempt so the
    report can distinguish real retries from hard failures.
    """

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=lambda retry_state: retry_counter.append(1),
    )
    async def fetch_list_page(client: httpx.AsyncClient, offset: int, page_size: int) -> dict:
        """One ``/datasets?limit=&offset=`` page (rich list records)."""
        resp = await client.get(
            f"{NEMAR_API_BASE}/datasets?limit={page_size}&offset={offset}"
        )
        resp.raise_for_status()
        return resp.json()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=lambda retry_state: retry_counter.append(1),
    )
    async def fetch_detail(client: httpx.AsyncClient, ds_id: str) -> dict:
        """GET /datasets/{id} — the single Phase-1 metadata source."""
        resp = await client.get(f"{NEMAR_API_BASE}/datasets/{ds_id}")
        resp.raise_for_status()
        return resp.json()

    return fetch_list_page, fetch_detail


async def run_nemar_ingestion(
    db,
    *,
    target: int | None = None,
    page_size: int = NEMAR_LIST_PAGE_SIZE,
    page_delay: float = 0.15,
    ids: list[str] | None = None,
    client: httpx.AsyncClient | None = None,
    log=print,
) -> dict:
    """Ingest public NEMAR datasets into the canonical catalog (Phase 1).

    - Enumerates via ``/datasets?limit=<page_size>&offset=<n>`` (offset/limit;
      never ``page``). Stops when ``total_count`` is reached or a page returns
      no records.
    - ``ids`` (optional) restricts processing to the given dataset IDs — used
      by the smoke-test runner to force a representative sample; when None all
      discovered datasets are processed.
    - ``target`` caps the number of records processed (retrieved).
    - NEVER calls version/summary/manifest/records/file endpoints; ``asset_calls``
      stays 0.
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "nemar",
        "discovered": 0,
        "retrieved": 0,
        "normalized": 0,
        "validated_ok": 0,
        "validation_failed": 0,
        "validation_errors": [],
        "inserted": 0,
        "merged": 0,
        "matched_via": {},
        "failed": 0,
        "failure_details": [],
        "api_failures": 0,
        "api_error_details": [],
        "retries": 0,
        "pages": 0,
        "list_requests": 0,
        "detail_requests": 0,
        "asset_calls": 0,          # Phase 1 never touches data-plane files
        "total_api_calls": 0,
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
        "target": target,
    }

    t0 = time.monotonic()
    retry_counter: list[int] = []
    fetch_list_page, fetch_detail = make_nemar_fetchers(retry_counter)
    owns_client = client is None
    async with (client or httpx.AsyncClient(timeout=60)) as c:
        # ── 1) Enumerate dataset IDs (offset/limit). ────────────────────────
        list_records: dict[str, dict] = {}
        discovered_ids: list[str] = []
        if ids:
            discovered_ids = list(ids)
        else:
            offset = 0
            consecutive_failures = 0
            while True:
                if target is not None and len(discovered_ids) >= target:
                    break
                stats["pages"] += 1
                stats["list_requests"] = stats["pages"]
                try:
                    data = await fetch_list_page(c, offset, page_size)
                    consecutive_failures = 0
                except Exception as exc:  # noqa: BLE001 — page-level failure
                    stats["api_failures"] += 1
                    stats["api_error_details"].append(
                        f"list page {stats['pages']} (offset {offset}): {type(exc).__name__}: {exc}"
                    )
                    log(f"[nemar] list page {stats['pages']} failed: {type(exc).__name__}: {exc}")
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        log("[nemar] too many consecutive list failures — aborting enumeration")
                        break
                    await asyncio.sleep(page_delay * 2)
                    continue
                stats["retries"] = len(retry_counter)
                recs = data.get("datasets") or []
                # Defensive: only stop early when total_count is actually
                # present; a missing count must never truncate enumeration.
                total_count = data.get("total_count")
                for rec in recs:
                    ds_id = str(rec.get("dataset_id") or rec.get("id") or "").strip()
                    if not ds_id:
                        continue
                    list_records[ds_id] = rec
                    if ds_id not in discovered_ids:
                        discovered_ids.append(ds_id)
                    if target is not None and len(discovered_ids) >= target:
                        break
                stats["discovered"] = len(discovered_ids)
                log(
                    f"[nemar] page {stats['pages']}: offset={offset} records={len(recs)} "
                    f"total={total_count} discovered={len(discovered_ids)}"
                )
                if not recs:
                    break
                if total_count is not None and offset + len(recs) >= total_count:
                    break
                offset += len(recs)
                await asyncio.sleep(page_delay)

        # ``ids``-driven runs never enumerated, so discovered reflects the
        # explicit id set (keeps smoke-test reports truthful).
        stats["discovered"] = len(discovered_ids)

        # ── 2) Fetch + normalize + validate + dedup + persist. ──────────────
        for ds_id in discovered_ids:
            if target is not None and stats["retrieved"] >= target:
                break
            stats["detail_requests"] += 1
            try:
                payload = await fetch_detail(c, ds_id)
            except Exception as exc:  # noqa: BLE001
                stats["api_failures"] += 1
                stats["failure_details"].append(
                    f"{ds_id}: fetch_detail: {type(exc).__name__}: {exc}"
                )
                log(f"[nemar] detail fetch failed for {ds_id}: {type(exc).__name__}: {exc}")
                continue
            stats["retrieved"] += 1
            stats["retries"] = len(retry_counter)

            detail = payload.get("dataset") or {}
            try:
                source = build_nemar_source_record(detail, list_records.get(ds_id))
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"{ds_id}: normalize: {exc}")
                continue
            stats["normalized"] += 1

            errs = _validate_source(source)
            if errs:
                stats["validation_failed"] += 1
                stats["validation_errors"].append(f"{ds_id}: {errs[0]}")
                log(f"[nemar] validation failed for {ds_id}: {errs}")
                continue
            stats["validated_ok"] += 1

            try:
                result = await upsert_canonical(collection, source)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"{ds_id}: upsert: {exc}")
                log(f"[nemar] upsert failed for {ds_id}: {exc}")
                continue

            if result.get("action") == "invalid":
                stats["validation_failed"] += 1
                stats["validation_errors"].append(
                    f"{ds_id}: {result.get('errors', [])[:2]}"
                )
                log(f"[nemar] canonical validation failed for {ds_id}")
                continue

            if result["action"] == "inserted":
                stats["inserted"] += 1
            else:
                stats["merged"] += 1
                via = result.get("matchedVia") or "unknown"
                stats["matched_via"][via] = stats["matched_via"].get(via, 0) + 1

            ambiguous = result.get("ambiguousCandidates") or []
            if ambiguous:
                stats["ambiguous_candidates"] += len(ambiguous)
                if len(stats["ambiguous_sample"]) < 10:
                    stats["ambiguous_sample"].append(
                        {
                            "source": f"nemar:{ds_id}",
                            "candidates": [
                                cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                            ],
                        }
                    )

            await asyncio.sleep(page_delay)  # rate limit — do not hammer NEMAR

    stats["asset_calls"] = 0  # hard guarantee: no asset/file crawling
    stats["total_api_calls"] = stats["list_requests"] + stats["detail_requests"]
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[nemar] done: discovered={stats['discovered']} retrieved={stats['retrieved']} "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"api_calls={stats['total_api_calls']} (asset_calls={stats['asset_calls']}) "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# NeuroMorpho.Org Phase-1 ingestion
#
# Flow (investigation + exact-count report, 2026-08-10):
#    1. Enumerate ALL neurons via Solr /select pagination
#       (q=*:*&size=500&page=N; max page size = 500)
#    2. Group in memory using group_neuromorpho_neurons() into
#       Archive × Publication contribution groups
#    3. For each group → build_neuromorpho_source_record() → validate →
#       upsert_canonical()
#
# Dataset unit = 1 contribution group, NOT 1 neuron, NOT 1 archive.
# Full corpus: 597 pages · 298,339 neurons · 1,696 contribution groups
# (1,642 PMID-backed + 54 DOI-only).
#
# ZERO asset/file crawling (never calls SWC/ASC endpoints).
# ─────────────────────────────────────────────────────────────────────────────

NEUROMORPHO_PAGE_SIZE = 500  # Solr server caps size at 500 (1000 rejected)


def make_neuromorpho_fetchers(retry_counter: list[int]):
    """Build retry-wrapped NeuroMorpho Solr fetchers."""

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=lambda retry_state: retry_counter.append(1),
    )
    async def fetch_neuron_page(client: httpx.AsyncClient, page: int, page_size: int) -> dict:
        """One ``/select?q=*:*&size=500&page=N`` page of neuron metadata."""
        resp = await client.get(
            f"{NEUROMORPHO_API_BASE}/select?q=*:*&size={page_size}&page={page}"
        )
        resp.raise_for_status()
        return resp.json()

    return fetch_neuron_page


async def run_neuromorpho_ingestion(
    db,
    *,
    target: int | None = None,
    group_limit: int | None = None,
    page_size: int = NEUROMORPHO_PAGE_SIZE,
    page_delay: float = 0.15,
    ids: list[str] | None = None,
    neurons: list[dict] | None = None,
    client: httpx.AsyncClient | None = None,
    log=print,
) -> dict:
    """Ingest NeuroMorpho.Org contributions into the canonical catalog
    (Phase 1).

    Flow:
    1. Enumerate all neuron metadata records (Solr pagination) OR accept
       a pre-fetched list (``neurons`` param for test client).
    2. Group by (archive × real PMID)/(archive × DOI) — see
       ``group_neuromorpho_neurons()``.
    3. For each contribution group, normalize → validate → upsert.

    Args:
        ``target``: cap on neuron pages processed (NOT groups).
        ``group_limit``: cap on contribution groups processed.
        ``ids``: restrict to specific sourceDatasetIds (for smoke tests).
        ``neurons``: pre-fetched neuron records (for mocked tests).
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "neuromorpho",
        "discovered": 0,                # neurons enumerated
        "retrieved": 0,                 # contribution groups processed
        "normalized": 0,
        "validated_ok": 0,
        "validation_failed": 0,
        "validation_errors": [],
        "inserted": 0,
        "merged": 0,
        "matched_via": {},
        "failed": 0,
        "failure_details": [],
        "api_failures": 0,
        "api_error_details": [],
        "retries": 0,
        "pages": 0,
        "neuron_requests": 0,
        "asset_calls": 0,               # Phase 1 never touches files
        "total_api_calls": 0,
        "neuron_count": 0,               # total neuron records processed
        "groups_formed": 0,              # groups from grouping
        "pmid_backed_groups": 0,
        "doi_only_groups": 0,
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
        "target": target,
        "group_limit": group_limit,
    }

    t0 = time.monotonic()
    retry_counter: list[int] = []
    fetch_neuron_page = make_neuromorpho_fetchers(retry_counter)
    owns_client = client is None
    async with (client or httpx.AsyncClient(timeout=90)) as c:
        # ── 1) Enumerate all neuron records (Solr pagination). ──────────────
        all_neurons: list[dict] = []
        if neurons is not None:
            all_neurons = list(neurons)
            stats["discovered"] = len(all_neurons)
        else:
            page = 0
            consecutive_failures = 0
            while True:
                if target is not None and stats["discovered"] >= target * page_size:
                    break
                stats["pages"] += 1
                stats["neuron_requests"] = stats["pages"]
                try:
                    data = await fetch_neuron_page(c, page, page_size)
                    consecutive_failures = 0
                except Exception as exc:  # noqa: BLE001
                    stats["api_failures"] += 1
                    stats["api_error_details"].append(
                        f"neuron page {stats['pages']} (page={page}): {type(exc).__name__}: {exc}"
                    )
                    log(f"[neuromorpho] page {stats['pages']} failed: {type(exc).__name__}: {exc}")
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        log("[neuromorpho] too many consecutive failures — aborting")
                        break
                    await asyncio.sleep(page_delay * 2)
                    continue
                stats["retries"] = len(retry_counter)
                recs = (data.get("_embedded") or {}).get("neuronResources") or []
                total_elements = (data.get("page") or {}).get("totalElements") or 0
                all_neurons.extend(recs)
                stats["discovered"] = len(all_neurons)
                log(
                    f"[neuromorpho] page {stats['pages']}: page={page} records={len(recs)} "
                    f"totalElements={total_elements} discovered={len(all_neurons)}"
                )
                if not recs:
                    break
                if total_elements and len(all_neurons) >= total_elements:
                    break
                page += 1
                await asyncio.sleep(page_delay)

        stats["neuron_count"] = len(all_neurons)
        log(f"[neuromorpho] enumerated {len(all_neurons)} neurons across {stats['pages']} pages")

        # ── 2) Group into contribution groups. ─────────────────────────────
        groups = group_neuromorpho_neurons(all_neurons)
        # Free neuron memory before processing groups.
        del all_neurons
        stats["groups_formed"] = len(groups)
        stats["pmid_backed_groups"] = sum(1 for g in groups if g.get("pmid"))
        stats["doi_only_groups"] = sum(1 for g in groups if not g.get("pmid") and g.get("doi"))
        log(f"[neuromorpho] grouped into {len(groups)} contributions "
            f"({stats['pmid_backed_groups']} PMID + {stats['doi_only_groups']} DOI-only)")

        # Filter by ids if specified (for smoke-test targeting).
        if ids:
            groups = [
                g for g in groups
                if _needed_group(g, ids)
            ]
            stats["groups_formed"] = len(groups)
            log(f"[neuromorpho] filtered to {len(groups)} groups by ids")

        # Cap by group_limit if set.
        if group_limit is not None and len(groups) > group_limit:
            groups = groups[:group_limit]
            stats["groups_formed"] = len(groups)

        # ── 3) Normalize + validate + dedup + persist each group. ─────────
        for g in groups:
            if target is not None and stats["retrieved"] >= target:
                break

            try:
                source = build_neuromorpho_source_record(g)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(
                    f"group {g.get('archive')}:{g.get('pmid') or g.get('doi')}: normalize: {exc}"
                )
                continue
            stats["retrieved"] += 1
            stats["normalized"] += 1

            errs = _validate_source(source)
            if errs:
                stats["validation_failed"] += 1
                stats["validation_errors"].append(
                    f"{source.get('sourceDatasetId')}: {errs[0]}"
                )
                log(f"[neuromorpho] validation failed for {source.get('sourceDatasetId')}: {errs}")
                continue
            stats["validated_ok"] += 1

            try:
                result = await upsert_canonical(collection, source)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(
                    f"{source.get('sourceDatasetId')}: upsert: {exc}"
                )
                log(f"[neuromorpho] upsert failed for {source.get('sourceDatasetId')}: {exc}")
                continue

            if result.get("action") == "invalid":
                stats["validation_failed"] += 1
                stats["validation_errors"].append(
                    f"{source.get('sourceDatasetId')}: {result.get('errors', [])[:2]}"
                )
                log(f"[neuromorpho] canonical validation failed for {source.get('sourceDatasetId')}")
                continue

            if result["action"] == "inserted":
                stats["inserted"] += 1
            else:
                stats["merged"] += 1
                via = result.get("matchedVia") or "unknown"
                stats["matched_via"][via] = stats["matched_via"].get(via, 0) + 1

            ambiguous = result.get("ambiguousCandidates") or []
            if ambiguous:
                stats["ambiguous_candidates"] += len(ambiguous)
                if len(stats["ambiguous_sample"]) < 10:
                    stats["ambiguous_sample"].append(
                        {
                            "source": source["sourceDatasetId"],
                            "candidates": [
                                cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                            ],
                        }
                    )

            await asyncio.sleep(page_delay)

    stats["asset_calls"] = 0  # hard guarantee: no asset/file crawling
    stats["total_api_calls"] = stats["neuron_requests"]
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[neuromorpho] done: neurons={stats['neuron_count']} "
        f"groups={stats['groups_formed']} "
        f"retrieved={stats['retrieved']} "
        f"normalized={stats['normalized']} "
        f"inserted={stats['inserted']} "
        f"merged={stats['merged']} "
        f"failed={stats['failed']} "
        f"api_calls={stats['total_api_calls']} (asset_calls={stats['asset_calls']}) "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats


def _needed_group(group: dict, ids: list[str]) -> bool:
    """Check if a group matches any of the given sourceDatasetId prefixes."""
    archive = group.get("archive", "")
    pmid = group.get("pmid", "")
    doi = group.get("doi", "")
    for candidate_id in ids:
        if archive in candidate_id:
            return True
        if pmid and pmid in candidate_id:
            return True
        if doi and doi.replace("/", "__") in candidate_id:
            return True
    return False
