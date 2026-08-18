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
    HCP_INDEX_PAGES,
    HCP_PUBLICATIONS_URL,
    HCP_DATA_RELEASES_URL,
    ADNI_CENSUS_EXPECTED,
    UKB_CENSUS_EXPECTED,
    EBRAINS_CENSUS_EXPECTED,
    DRYAD_HIGH_CONFIDENCE,
    build_adni_source_record,
    build_allen_source_record,
    build_dandi_source_record,
    build_dryad_source_record,
    build_ebrains_source_record,
    build_hcp_source_record,
    build_nemar_source_record,
    build_neuromorpho_source_record,
    build_source_record,
    build_ukbiobank_source_record,
    group_neuromorpho_neurons,
    hcp_index_card_slugs,
    hcp_index_slugs,
    hcp_index_slugs_raw,
    hcp_parse_publications_page,
    hcp_parse_releases_page,
    hcp_parse_study_page,
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


# ─────────────────────────────────────────────────────────────────────────────
# Allen Brain Atlas Phase-1 ingestion
#
# Flow (investigation + implementation 2026-08-16):
#    1. Enumerate ALL Products via the RMA query API
#       (criteria=model::Product&num_rows=all → 64 rows, verified live)
#    2. Fetch the Age model once (899 rows, small) for donor-age summaries
#    3. Per product: criteria=model::Product[id$eqN]&include=data_sets,
#       specimens,donors&num_rows=1 (bulk included relationships — the child
#       arrays are aggregated into statistics and DISCARDED, never stored)
#    4. build_allen_source_record() → validate → upsert_canonical()
#
# Dataset unit = ONE Allen Product. 1 Product = 1 NeuroSearch dataset.
# ZERO asset/file crawling (SectionImages/images/raw data are never requested).
# ─────────────────────────────────────────────────────────────────────────────

ALLEN_API_BASE = "https://api.brain-map.org/api/v2/data"
ALLEN_EXPECTED_PRODUCTS = 64

# The include payload for a product carries ALL its child DataSets / Specimens
# / Donors (up to ~17 MB for the Mouse Brain atlas). We aggregate and discard.
ALLEN_PRODUCT_INCLUDE = "data_sets,specimens,donors"


def make_allen_fetchers(retry_counter: list[int], rate_limit_counter: list[int] | None = None):
    """Build retry-wrapped Allen RMA fetchers.

    ``retry_counter`` is appended to on every actual retry attempt so the
    report can distinguish real retries from hard failures.
    ``rate_limit_counter`` (optional) is appended to on every 429/503
    (rate-limit) response observed, so the report records rate-limit
    responses explicitly.
    """

    def _before_sleep(retry_state):
        retry_counter.append(1)
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (429, 503):
            if rate_limit_counter is not None:
                rate_limit_counter.append(1)

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=_before_sleep,
    )
    async def fetch_enumeration(client: httpx.AsyncClient) -> dict:
        """All Products (base fields only)."""
        resp = await client.get(
            f"{ALLEN_API_BASE}/query.json",
            params={"criteria": "model::Product", "num_rows": "all"},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=_before_sleep,
    )
    async def fetch_ages(client: httpx.AsyncClient) -> dict:
        """All Age model rows (id → name/days/age_group_id/embryonic/organism)."""
        resp = await client.get(
            f"{ALLEN_API_BASE}/query.json",
            params={"criteria": "model::Age", "num_rows": "all"},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
        before_sleep=_before_sleep,
    )
    async def fetch_product(client: httpx.AsyncClient, product_id: int) -> dict:
        """One Product with its child relationships included (bulk)."""
        resp = await client.get(
            f"{ALLEN_API_BASE}/query.json",
            params={
                "criteria": f"model::Product[id$eq{product_id}]",
                "include": ALLEN_PRODUCT_INCLUDE,
                "num_rows": 1,
            },
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json()

    return fetch_enumeration, fetch_ages, fetch_product


async def run_allen_ingestion(
    db,
    *,
    target: int | None = None,
    ids: list[int] | None = None,
    page_delay: float = 0.15,
    client: httpx.AsyncClient | None = None,
    log=print,
) -> dict:
    """Ingest Allen Brain Atlas Products into the canonical catalog (Phase 1).

    - Enumerates ALL Products via the RMA query API and verifies the live
      count against the expected 64 (recorded in ``stats``; the run tool
      enforces the stop policy).
    - ``ids`` (optional) restricts processing to the given product ids — used
      by the smoke-test runner for a representative sample.
    - ``target`` caps the number of products processed (retrieved).
    - Child DataSets/Specimens/Donors are fetched via the bulk include and
      aggregated into statistics; the child rows are never persisted.
    - NEVER requests SectionImages / image files / raw data: ``asset_calls``
      stays 0.
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "allen",
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
        "rate_limit_responses": 0,
        "pages": 0,
        "enumeration_requests": 0,
        "age_requests": 0,
        "product_requests": 0,
        "asset_calls": 0,          # hard guarantee: no image/file crawling
        "total_api_calls": 0,
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "expected_products": ALLEN_EXPECTED_PRODUCTS,
        "enumerated_total": None,
        "product_ids": [],
        "child_stats_total": {
            "dataSetCount": 0,
            "specimenCount": 0,
            "donorCount": 0,
        },
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
        "target": target,
    }

    t0 = time.monotonic()
    retry_counter: list[int] = []
    rate_limit_counter: list[int] = []
    fetch_enumeration, fetch_ages, fetch_product = make_allen_fetchers(
        retry_counter, rate_limit_counter
    )
    owns_client = client is None
    async with (client or httpx.AsyncClient(timeout=300)) as c:
        # ── 1) Enumerate all Products. ─────────────────────────────────────
        stats["enumeration_requests"] = 1
        try:
            enum = await fetch_enumeration(c)
        except Exception as exc:  # noqa: BLE001 — page-level failure
            stats["api_failures"] += 1
            stats["api_error_details"].append(f"enumeration: {type(exc).__name__}: {exc}")
            log(f"[allen] enumeration failed: {type(exc).__name__}: {exc}")
            enum = None
        if enum is None or not enum.get("success"):
            log("[allen] enumeration failed — aborting")
            stats["enumerated_total"] = 0
            stats["elapsed_s"] = round(time.monotonic() - t0, 3)
            stats["finished_at"] = _utcnow()
            return stats
        stats["retries"] = len(retry_counter)
        products = enum.get("msg") or []
        stats["enumerated_total"] = enum.get("total_rows")
        stats["discovered"] = len(products)
        stats["product_ids"] = [
            int(p["id"]) for p in products if isinstance(p, dict) and p.get("id") is not None
        ]
        log(
            f"[allen] enumerated {len(products)} products "
            f"(api total_rows={stats['enumerated_total']} expected={ALLEN_EXPECTED_PRODUCTS})"
        )

        # ── 2) Age model (one small bulk request) for donor-age summaries. ──
        stats["age_requests"] = 1
        age_map: dict[int, dict] = {}
        try:
            ages_payload = await fetch_ages(c)
            for row in (ages_payload.get("msg") or []):
                if isinstance(row, dict) and row.get("id") is not None:
                    age_map[int(row["id"])] = row
        except Exception as exc:  # noqa: BLE001 — ages are enrichment only
            stats["api_failures"] += 1
            stats["api_error_details"].append(f"ages: {type(exc).__name__}: {exc}")
            log(f"[allen] ages fetch failed (non-fatal): {type(exc).__name__}: {exc}")

        # ── 3) Per product: include payload → aggregate → normalize → persist.
        pending = list(ids) if ids else list(stats["product_ids"])
        for product_id in pending:
            if target is not None and stats["retrieved"] >= target:
                break
            stats["product_requests"] += 1
            try:
                payload = await fetch_product(c, product_id)
            except Exception as exc:  # noqa: BLE001
                stats["api_failures"] += 1
                stats["failure_details"].append(
                    f"product {product_id}: fetch: {type(exc).__name__}: {exc}"
                )
                log(f"[allen] product {product_id} fetch failed: {type(exc).__name__}: {exc}")
                continue
            stats["retrieved"] += 1
            stats["retries"] = len(retry_counter)

            msg = payload.get("msg") or []
            product = msg[0] if msg and isinstance(msg[0], dict) else None
            if product is None:
                stats["failed"] += 1
                stats["failure_details"].append(f"product {product_id}: empty payload")
                continue

            try:
                source = build_allen_source_record(product, age_map)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"product {product_id}: normalize: {exc}")
                continue
            stats["normalized"] += 1

            child = (source.get("snapshot") or {})
            for k in ("dataSetCount", "specimenCount", "donorCount"):
                v = child.get(k) or 0
                stats["child_stats_total"][k] = stats["child_stats_total"].get(k, 0) + v

            errs = _validate_source(source)
            if errs:
                stats["validation_failed"] += 1
                stats["validation_errors"].append(f"product {product_id}: {errs[0]}")
                log(f"[allen] validation failed for product {product_id}: {errs}")
                continue
            stats["validated_ok"] += 1

            try:
                result = await upsert_canonical(collection, source)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"product {product_id}: upsert: {exc}")
                log(f"[allen] upsert failed for product {product_id}: {exc}")
                continue

            if result.get("action") == "invalid":
                stats["validation_failed"] += 1
                stats["validation_errors"].append(
                    f"product {product_id}: {result.get('errors', [])[:2]}"
                )
                log(f"[allen] canonical validation failed for product {product_id}")
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
                            "source": f"allen:{product_id}",
                            "candidates": [
                                cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                            ],
                        }
                    )

            await asyncio.sleep(page_delay)  # rate limit — do not hammer Allen

    stats["asset_calls"] = 0  # hard guarantee: no asset/image crawling
    stats["rate_limit_responses"] = len(rate_limit_counter)
    stats["total_api_calls"] = (
        stats["enumeration_requests"] + stats["age_requests"] + stats["product_requests"]
    )
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[allen] done: enumerated={stats['discovered']} retrieved={stats['retrieved']} "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"api_calls={stats['total_api_calls']} (asset_calls={stats['asset_calls']}) "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# HCP / Connectome Coordination Facility ingestion
#
# Flow (investigation + implementation 2026-08-16):
#    1. Discovery — fetch the two authoritative index pages
#       (/lifespan-studies and /disease-studies) and extract the study slugs.
#       The discovery gate requires EXACTLY 20 unique slugs with no
#       duplicates; otherwise the runner stops BEFORE any write (partial
#       catalogs are never silently ingested; the sitemap is NOT used — it
#       was observed to omit two Studies).
#    2. Per study — fetch the Study landing page (name/description/PIs),
#       the data-releases page (aggregated release metadata) and the
#       publications page (publication DOIs, quarantined from identity).
#    3. build_hcp_source_record() → validate → upsert_canonical()
#
# Dataset unit = ONE HCP Study. Releases are versions, NEVER separate
# datasets. ZERO asset/file crawling (ConnectomeDB/BALSA/file URLs are never
# requested — only the three metadata HTML pages per study).
# ─────────────────────────────────────────────────────────────────────────────

HCP_EXPECTED_STUDIES = 20

# The CCF Drupal site throttles rapid sequential requests — default delay is
# deliberately polite (verified during investigation: sequential bursts time
# out).
HCP_DEFAULT_PAGE_DELAY = 1.0

# Metadata HTML pages only. ConnectomeDB/BALSA/file endpoints are NEVER used.
HCP_USER_AGENT = (
    "Mozilla/5.0 (NeuroSearch catalog ingestion; metadata-only; contact: "
    "neurosearch@example.org)"
)


def make_hcp_fetchers(retry_counter: list[int], rate_limit_counter: list[int] | None = None):
    """Build retry-wrapped HCP Drupal page fetchers (metadata HTML only).

    ``retry_counter`` is appended to on every actual retry attempt;
    ``rate_limit_counter`` (optional) on every 429/503 rate-limit response.
    Returns a single ``fetch_page(client, url) -> html`` fetcher — the HCP
    site has no JSON API, so the pages themselves are the metadata source.
    """

    def _before_sleep(retry_state):
        retry_counter.append(1)
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (429, 503):
            if rate_limit_counter is not None:
                rate_limit_counter.append(1)

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=20),
        reraise=True,
        before_sleep=_before_sleep,
    )
    async def fetch_page(client: httpx.AsyncClient, url: str) -> str:
        """One metadata HTML page (never an asset/file endpoint)."""
        resp = await client.get(url, headers={"User-Agent": HCP_USER_AGENT}, timeout=60)
        resp.raise_for_status()
        return resp.text

    return fetch_page


async def run_hcp_ingestion(
    db,
    *,
    ids: list[str] | None = None,
    page_delay: float = HCP_DEFAULT_PAGE_DELAY,
    client: httpx.AsyncClient | None = None,
    log=print,
) -> dict:
    """Ingest HCP/CCF Studies into the canonical catalog.

    Discovery gate (hard, before ANY write):
      - both index pages must be fetched;
      - the union of study slugs must contain EXACTLY ``HCP_EXPECTED_STUDIES``
        (20) unique slugs with NO duplicates across the two pages;
      - otherwise the runner records ``gate_failed`` and returns WITHOUT
        writing anything (a partial or duplicated HCP catalog is never
        silently ingested).

    ``ids`` (optional) restricts processing to the given slugs (used by the
    smoke-test runner). ``page_delay`` sets the polite inter-request delay
    (the CCF site throttles rapid sequential requests).

    Never requests assets/files/images: only the study page, the
    data-releases page and the publications page are fetched per study
    (``asset_calls`` stays 0).
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "hcp",
        "discovered": 0,
        "discovered_slugs": [],
        "duplicate_slugs": [],
        "gate_failed": False,
        "gate_reason": None,
        "expected_studies": HCP_EXPECTED_STUDIES,
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
        "rate_limit_responses": 0,
        "pages": 0,
        "index_requests": 0,
        "study_requests": 0,
        "release_requests": 0,
        "publication_requests": 0,
        "asset_calls": 0,          # hard guarantee: no image/file crawling
        "total_api_calls": 0,
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "release_metadata_total": 0,
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
    }

    t0 = time.monotonic()
    retry_counter: list[int] = []
    rate_limit_counter: list[int] = []
    fetch_page = make_hcp_fetchers(retry_counter, rate_limit_counter)
    owns_client = client is None
    async with (client or httpx.AsyncClient(timeout=60, follow_redirects=True)) as c:
        # ── 1) Discovery — both authoritative index pages. ─────────────────
        #    Each page lists the full 20 via the "Studies" nav dropdown plus a
        #    subset of study CARDS. Nav/sidebar links are chrome and repeat
        #    slugs legitimately; the discovery union therefore deduplicates
        #    ALL links. A corrupted catalog is detected by a slug appearing in
        #    MORE than one study CARD on a single page (duplicate listing).
        unique_slugs: set[str] = set()
        per_page_duplicates: list[str] = []
        page_card_counts: list[int] = []
        page_link_counts: list[int] = []
        for index_url in HCP_INDEX_PAGES:
            stats["index_requests"] += 1
            try:
                html = await fetch_page(c, index_url)
            except Exception as exc:  # noqa: BLE001
                stats["api_failures"] += 1
                stats["api_error_details"].append(f"index {index_url}: {type(exc).__name__}: {exc}")
                log(f"[hcp] index fetch failed: {index_url}: {type(exc).__name__}: {exc}")
                continue
            page_link_counts.append(len(hcp_index_slugs_raw(html)))
            card_slugs = hcp_index_card_slugs(html)
            page_card_counts.append(len(card_slugs))
            seen_in_card: set[str] = set()
            for s in card_slugs:
                if s in seen_in_card and s not in per_page_duplicates:
                    per_page_duplicates.append(s)
                seen_in_card.add(s)
            unique_slugs.update(hcp_index_slugs(html))
        stats["retries"] = len(retry_counter)

        unique_slugs = sorted(unique_slugs)
        stats["discovered"] = len(unique_slugs)
        stats["discovered_slugs"] = unique_slugs
        stats["duplicate_slugs"] = per_page_duplicates
        log(
            f"[hcp] discovered {len(unique_slugs)} unique studies from "
            f"{len(HCP_INDEX_PAGES)} index pages "
            f"(cards_per_page={page_card_counts} links_per_page={page_link_counts} "
            f"expected={HCP_EXPECTED_STUDIES} "
            f"duplicate_cards={len(per_page_duplicates)})"
        )

        # ── Discovery gate: exactly 20 unique slugs, no within-page dupes. ──
        if stats["discovered"] != HCP_EXPECTED_STUDIES or per_page_duplicates:
            stats["gate_failed"] = True
            stats["gate_reason"] = (
                f"discovered={stats['discovered']} expected={HCP_EXPECTED_STUDIES} "
                f"duplicates={len(per_page_duplicates)}"
            )
            log(f"[hcp] *** GATE FAILED: {stats['gate_reason']} — NOT writing anything ***")
            stats["asset_calls"] = 0
            stats["rate_limit_responses"] = len(rate_limit_counter)
            stats["total_api_calls"] = stats["index_requests"]
            stats["elapsed_s"] = round(time.monotonic() - t0, 3)
            stats["finished_at"] = _utcnow()
            return stats

        # ── 2) Per study: landing + data-releases + publications pages. ────
        pending = list(ids) if ids else unique_slugs
        for slug in pending:
            if slug not in unique_slugs:
                stats["failed"] += 1
                stats["failure_details"].append(f"{slug}: not in discovered set")
                continue

            # Study landing page (name/description/PIs).
            stats["study_requests"] += 1
            try:
                study_html = await fetch_page(
                    c, f"https://www.humanconnectome.org/study/{slug}"
                )
            except Exception as exc:  # noqa: BLE001
                stats["api_failures"] += 1
                stats["failure_details"].append(
                    f"{slug}: study page: {type(exc).__name__}: {exc}"
                )
                log(f"[hcp] study page fetch failed for {slug}: {type(exc).__name__}: {exc}")
                continue
            try:
                study = hcp_parse_study_page(study_html, slug)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"{slug}: parse study: {exc}")
                log(f"[hcp] study parse failed for {slug}: {exc}")
                continue
            stats["retrieved"] += 1
            stats["retries"] = len(retry_counter)

            # Data releases page (aggregated metadata — never datasets).
            stats["release_requests"] += 1
            try:
                releases_html = await fetch_page(c, HCP_DATA_RELEASES_URL.format(slug=slug))
                study["dataReleases"] = hcp_parse_releases_page(releases_html)
            except Exception as exc:  # noqa: BLE001 — enrichment only, non-fatal
                stats["api_failures"] += 1
                stats["api_error_details"].append(f"{slug} releases: {type(exc).__name__}: {exc}")
                log(f"[hcp] releases fetch failed for {slug} (non-fatal): {type(exc).__name__}: {exc}")
                study["dataReleases"] = []
            stats["release_metadata_total"] += len(study.get("dataReleases") or [])

            # Publications page (publication DOIs — quarantined from identity).
            stats["publication_requests"] += 1
            try:
                pubs_html = await fetch_page(c, HCP_PUBLICATIONS_URL.format(slug=slug))
                study["publications"] = hcp_parse_publications_page(pubs_html)
            except Exception as exc:  # noqa: BLE001 — enrichment only, non-fatal
                stats["api_failures"] += 1
                stats["api_error_details"].append(f"{slug} pubs: {type(exc).__name__}: {exc}")
                log(f"[hcp] publications fetch failed for {slug} (non-fatal): {type(exc).__name__}: {exc}")
                study["publications"] = []

            try:
                source = build_hcp_source_record(study)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"{slug}: normalize: {exc}")
                continue
            stats["normalized"] += 1

            errs = _validate_source(source)
            if errs:
                stats["validation_failed"] += 1
                stats["validation_errors"].append(f"{slug}: {errs[0]}")
                log(f"[hcp] validation failed for {slug}: {errs}")
                continue
            stats["validated_ok"] += 1

            try:
                result = await upsert_canonical(collection, source)
            except Exception as exc:  # noqa: BLE001
                stats["failed"] += 1
                stats["failure_details"].append(f"{slug}: upsert: {exc}")
                log(f"[hcp] upsert failed for {slug}: {exc}")
                continue

            if result.get("action") == "invalid":
                stats["validation_failed"] += 1
                stats["validation_errors"].append(
                    f"{slug}: {result.get('errors', [])[:2]}"
                )
                log(f"[hcp] canonical validation failed for {slug}")
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
                            "source": f"hcp:{slug}",
                            "candidates": [
                                cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                            ],
                        }
                    )

            await asyncio.sleep(page_delay)  # polite — the CCF site throttles

    stats["asset_calls"] = 0  # hard guarantee: no asset/image crawling
    stats["rate_limit_responses"] = len(rate_limit_counter)
    stats["total_api_calls"] = (
        stats["index_requests"]
        + stats["study_requests"]
        + stats["release_requests"]
        + stats["publication_requests"]
    )
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[hcp] done: discovered={stats['discovered']} retrieved={stats['retrieved']} "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"api_calls={stats['total_api_calls']} (asset_calls={stats['asset_calls']}) "
        f"releases_aggregated={stats['release_metadata_total']} "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# Dryad ingestion (implementation + dry-run 2026-08-17)
#
# Flow:
#    1. Input — the AUTHORITATIVE census artifact
#       (trace_artifacts/dryad_census_20260817/dryad_candidates.jsonl).
#       Every record must carry the HIGH-confidence classification ("H");
#       MEDIUM / LOW / FALSE-POSITIVE records are REFUSED by a hard gate —
#       the adapter never silently expands the candidate set.
#    2. Normalize each HIGH-confidence record via build_dryad_source_record().
#    3. The existing generic pipeline: validate → resolve_identity →
#       upsert_canonical() (insert or merge into the existing canonical
#       record). No new identity/dedup/persistence logic is introduced.
#
# Metadata-only: the census artifact already contains the full Dryad record
# (DOI, title, abstract, authors, keywords, fieldOfScience, relatedWorks,
# license, file METADATA, versions) — ZERO Dryad API calls and ZERO
# asset/file downloads are needed. ``api_calls`` and ``asset_calls`` stay 0.
# ─────────────────────────────────────────────────────────────────────────────


def _dryad_identity_keys(record: dict) -> list[str]:
    """Deterministic candidate-identity keys for duplicate protection.

    Mirrors what the generic resolver will use: the canonical DOI and the
    source key (repository:sourceDatasetId). Returns [] when a record has no
    usable identity (caught as a normalization failure downstream).
    """
    keys: list[str] = []
    identifier = record.get("identifier") or ""
    if identifier:
        # census identifiers carry the doi: prefix ("doi:10.5061/dryad.gc72v")
        raw = str(identifier).strip()
        if raw.lower().startswith("doi:"):
            raw = raw[4:].strip()
        keys.append(f"doi:{raw}")
    ds_id = record.get("id")
    if ds_id is not None and str(ds_id).strip():
        keys.append(f"dryad:dryad:{str(ds_id).strip()}")
    return keys


async def run_dryad_ingestion(
    db,
    *,
    candidates: list[dict],
    log=print,
) -> dict:
    """Ingest the validated Dryad HIGH-confidence dataset set (metadata only).

    ``candidates`` is the authoritative ingestion list — each dict is one
    census-artifact Dryad dataset (dryad_candidates.jsonl line). A hard gate
    refuses any record whose ``classification`` is not the validated
    HIGH-confidence label, so MEDIUM / LOW / FALSE-POSITIVE records can never
    enter the catalog through this adapter.

    Never requests Dryad API endpoints or dataset files: the census artifact
    already carries all metadata needed for canonical insertion.
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "dryad",
        "input": len(candidates),
        "high_confidence": 0,
        "excluded_non_high": 0,
        "gate_failed": False,
        "gate_reason": None,
        "normalized": 0,
        "validated_ok": 0,
        "validation_failed": 0,
        "validation_errors": [],
        "duplicate_source_identities": [],
        "inserted": 0,
        "merged": 0,
        "matched_via": {},
        "failed": 0,
        "failure_details": [],
        "api_calls": 0,       # census artifact is authoritative — no API calls
        "asset_calls": 0,     # hard guarantee: no file/asset downloads
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
    }

    t0 = time.monotonic()

    # ── 1) HIGH-confidence gate — refuse anything that isn't "H". ───────────
    high = [
        c for c in candidates
        if str(c.get("classification") or "").strip().upper() == DRYAD_HIGH_CONFIDENCE
    ]
    excluded = [
        c for c in candidates
        if str(c.get("classification") or "").strip().upper() != DRYAD_HIGH_CONFIDENCE
    ]
    stats["high_confidence"] = len(high)
    stats["excluded_non_high"] = len(excluded)
    if excluded:
        stats["gate_failed"] = True
        stats["gate_reason"] = (
            f"{len(excluded)} non-HIGH-confidence records in the ingestion input "
            f"(classifications={sorted({c.get('classification') for c in excluded})})"
        )
        log(f"[dryad] *** GATE FAILED: {stats['gate_reason']} — NOT writing anything ***")
        stats["finished_at"] = _utcnow()
        stats["elapsed_s"] = round(time.monotonic() - t0, 3)
        return stats

    # ── 2) Duplicate candidate protection (same DOI / source key twice). ────
    # The census artifact is deduplicated, so ANY repeated identity key in the
    # input is a genuine duplicate — the generic resolver would merge them, but
    # the adapter detects and reports it up front.
    seen: dict[str, str] = {}
    for c in high:
        for key in _dryad_identity_keys(c):
            prev = seen.get(key)
            if prev is not None:
                stats["duplicate_source_identities"].append(
                    {"identity": key, "first": prev, "second": c.get("identifier")}
                )
            else:
                seen[key] = c.get("identifier")

    # ── 3) Normalize → validate → generic upsert (insert or merge). ─────────
    for c in high:
        try:
            source = build_dryad_source_record(c)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            stats["failure_details"].append(
                f"{c.get('identifier')}: normalize: {type(exc).__name__}: {exc}"
            )
            log(f"[dryad] normalize failed for {c.get('identifier')}: {exc}")
            continue
        stats["normalized"] += 1

        errs = _validate_source(source)
        if errs:
            stats["validation_failed"] += 1
            stats["validation_errors"].append(f"{c.get('identifier')}: {errs[0]}")
            log(f"[dryad] validation failed for {c.get('identifier')}: {errs}")
            continue
        stats["validated_ok"] += 1

        try:
            result = await upsert_canonical(collection, source)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            stats["failure_details"].append(
                f"{c.get('identifier')}: upsert: {type(exc).__name__}: {exc}"
            )
            log(f"[dryad] upsert failed for {c.get('identifier')}: {exc}")
            continue

        if result.get("action") == "invalid":
            stats["validation_failed"] += 1
            stats["validation_errors"].append(
                f"{c.get('identifier')}: {result.get('errors', [])[:2]}"
            )
            log(f"[dryad] canonical validation failed for {c.get('identifier')}")
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
                        "source": c.get("identifier"),
                        "candidates": [
                            cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                        ],
                    }
                )

    stats["asset_calls"] = 0  # hard guarantee: no file/asset downloads
    stats["api_calls"] = 0    # census artifact is authoritative — no API calls
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[dryad] done: input={stats['input']} high={stats['high_confidence']} "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"api_calls={stats['api_calls']} (asset_calls={stats['asset_calls']}) "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# ADNI ingestion (implementation + dry-run 2026-08-17)
#
# Flow:
#    1. Input — the AUTHORITATIVE ADNI census artifact
#       (trace_artifacts/adni_census_20260817/adni_census.json → datasets[]).
#       A hard gate REFUSES the entire run unless the input is EXACTLY
#       122 records — the adapter never silently expands or trims the
#       approved set. Records must also carry a stable_identifier.
#    2. Normalize each record via build_adni_source_record().
#    3. The existing generic pipeline: validate → resolve_identity →
#       upsert_canonical() (insert or merge into the existing canonical
#       record). No new identity/dedup/persistence logic is introduced.
#
# Metadata-only: the census artifact already contains every field needed
# (stable identifier, name, description, category, modality, phase, source
# URL, access level, dates, version info, publication DOI, classification).
# ZERO ADNI API calls and ZERO asset/file downloads — ``api_calls`` and
# ``asset_calls`` stay 0. Running this function against a LIVE collection is
# the only write path; a read-only dry-run resolution is provided separately
# (trace_tools/adni_dryrun.py) and must be executed before any live run.
# ─────────────────────────────────────────────────────────────────────────────


def _adni_identity_keys(record: dict) -> list[str]:
    """Deterministic candidate-identity keys for duplicate protection.

    Mirrors what the generic resolver will use: the ADNI source key
    (repository:sourceDatasetId = "adni:adni:<stable_identifier>"). Returns
    [] when a record has no usable stable identifier (caught as a
    normalization failure downstream).
    """
    stable_id = str(record.get("stable_identifier") or "").strip()
    if not stable_id:
        return []
    return [f"adni:adni:{stable_id}"]


async def run_adni_ingestion(
    db,
    *,
    records: list[dict],
    log=print,
) -> dict:
    """Ingest the validated ADNI census set (metadata only).

    ``records`` is the authoritative ingestion list — each dict is one
    approved ADNI census artifact record (adni_census.json ``datasets[]``).
    A hard gate refuses the run unless there are EXACTLY ``ADNI_CENSUS_EXPECTED``
    records (122) and every record carries a stable_identifier, so no run can
    silently process a partial or bloated input.

    Never requests ADNI API endpoints or dataset files: the census artifact
    already carries all metadata needed for canonical insertion.
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "adni",
        "input": len(records),
        "expected": ADNI_CENSUS_EXPECTED,
        "gate_failed": False,
        "gate_reason": None,
        "normalized": 0,
        "validated_ok": 0,
        "validation_failed": 0,
        "validation_errors": [],
        "duplicate_source_identities": [],
        "inserted": 0,
        "merged": 0,
        "matched_via": {},
        "failed": 0,
        "failure_details": [],
        "api_calls": 0,       # census artifact is authoritative — no API calls
        "asset_calls": 0,     # hard guarantee: no file/asset downloads
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
    }

    t0 = time.monotonic()

    # ── 1) Exact-count gate — EXACTLY ADNI_CENSUS_EXPECTED records. ──────────
    if len(records) != ADNI_CENSUS_EXPECTED:
        stats["gate_failed"] = True
        stats["gate_reason"] = (
            f"input has {len(records)} records; the approved ADNI census set "
            f"is exactly {ADNI_CENSUS_EXPECTED} — NOT writing anything"
        )
        log(f"[adni] *** GATE FAILED: {stats['gate_reason']} ***")
        stats["finished_at"] = _utcnow()
        stats["elapsed_s"] = round(time.monotonic() - t0, 3)
        return stats

    # 2) Identity-completeness gate — every record needs a stable_identifier.
    #    Malformed entries (non-dict) are treated as missing identity so a
    #    corrupted input refuses the ENTIRE run, never a partial write.
    missing_identity: list[dict] = []
    for record in records:
        stable = (
            str(record.get("stable_identifier") or "").strip()
            if isinstance(record, dict)
            else ""
        )
        if not stable:
            missing_identity.append(
                {
                    "dataset_name": (
                        record.get("dataset_name") if isinstance(record, dict) else None
                    ),
                    "reason": "missing stable_identifier",
                }
            )
    if missing_identity:
        stats["gate_failed"] = True
        stats["gate_reason"] = (
            f"{len(missing_identity)} records lack a stable_identifier "
            f"(e.g. {missing_identity[0].get('dataset_name')}) — NOT writing anything"
        )
        log(f"[adni] *** GATE FAILED: {stats['gate_reason']} ***")
        stats["finished_at"] = _utcnow()
        stats["elapsed_s"] = round(time.monotonic() - t0, 3)
        return stats

    # ── 3) Duplicate candidate protection (same stable identity twice). ──────
    # The census artifact is deduplicated, so ANY repeated identity key in the
    # input is a genuine duplicate — the generic resolver would merge them, but
    # the adapter detects and reports it up front.
    seen: dict[str, str] = {}
    for record in records:
        for key in _adni_identity_keys(record):
            prev = seen.get(key)
            if prev is not None:
                stats["duplicate_source_identities"].append(
                    {
                        "identity": key,
                        "first": prev,
                        "second": str(record.get("stable_identifier") or ""),
                    }
                )
            else:
                seen[key] = str(record.get("stable_identifier") or "")

    # ── 4) Normalize → validate → generic upsert (insert or merge). ──────────
    # ADNI sourceUrl is the census URL verbatim; product identity flows through
    # the deterministic repo:sourceDatasetId/sourceKey (the sourceUrl layer is
    # skipped for ADNI — see SHARED_SOURCE_URL_REPOSITORIES in normalize.py).
    for record in records:
        stable_id = str(record.get("stable_identifier") or "").strip()
        try:
            source = build_adni_source_record(record)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            stats["failure_details"].append(
                f"{stable_id}: normalize: {type(exc).__name__}: {exc}"
            )
            log(f"[adni] normalize failed for {stable_id}: {exc}")
            continue
        stats["normalized"] += 1

        errs = _validate_source(source)
        if errs:
            stats["validation_failed"] += 1
            stats["validation_errors"].append(f"{stable_id}: {errs[0]}")
            log(f"[adni] validation failed for {stable_id}: {errs}")
            continue
        stats["validated_ok"] += 1

        try:
            result = await upsert_canonical(collection, source)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            stats["failure_details"].append(
                f"{stable_id}: upsert: {type(exc).__name__}: {exc}"
            )
            log(f"[adni] upsert failed for {stable_id}: {exc}")
            continue

        if result.get("action") == "invalid":
            stats["validation_failed"] += 1
            stats["validation_errors"].append(
                f"{stable_id}: {result.get('errors', [])[:2]}"
            )
            log(f"[adni] canonical validation failed for {stable_id}")
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
                        "source": stable_id,
                        "candidates": [
                            cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                        ],
                    }
                )

    stats["asset_calls"] = 0  # hard guarantee: no file/asset downloads
    stats["api_calls"] = 0    # census artifact is authoritative — no API calls
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[adni] done: input={stats['input']} (expected={stats['expected']}) "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"api_calls={stats['api_calls']} (asset_calls={stats['asset_calls']}) "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats


# UK Biobank ingestion (implementation + dry-run 2026-08-17)
#
# Flow:
#    1. Input — the AUTHORITATIVE UK Biobank census artifacts
#       (trace_artifacts/ukbiobank_census_20260817/ukbiobank_census.json →
#       datasets_approved[] AND ukbiobank_candidates.jsonl). The hard gate
#       REFUSES the entire run unless the input is EXACTLY 21 records — the
#       adapter never silently expands or trims the approved set. Records
#       must also carry a stable_identifier.
#    2. Normalize each record via build_ukbiobank_source_record().
#    3. The existing generic pipeline: validate → resolve_identity →
#       upsert_canonical() (insert or merge into the existing canonical
#       record). No new identity/dedup/persistence logic is introduced.
#
# Metadata-only: the census artifact already contains every field needed
# (stable identifier, name, description, category + category IDs, modality,
# child Data-Field IDs and counts, participant count, access level, version/
# release information, dataset-unit rationale, classification). ZERO UK
# Biobank API calls and ZERO asset/file downloads — ``api_calls`` and
# ``asset_calls`` stay 0. No participant data, no bulk/image files, no
# UKB-RAP access. The Returns catalogue (158 neuroscience-related returned
# datasets) is deliberately NEVER ingested — returned datasets stay excluded.
# Running this function against a LIVE collection is the only write path; a
# read-only dry-run resolution is provided separately
# (trace_tools/ukbiobank_dryrun.py) and must be executed before any live run.
# ─────────────────────────────────────────────────────────────────────────────


def _ukbiobank_identity_keys(record: dict) -> list[str]:
    """Deterministic candidate-identity keys for duplicate protection.

    Mirrors what the generic resolver will use: the UK Biobank source key
    (repository:sourceDatasetId = "ukbiobank:ukbiobank:<stable_identifier>").
    Returns [] when a record has no usable stable identifier (caught as a
    normalization failure downstream).
    """
    stable_id = str(record.get("stable_identifier") or "").strip()
    if not stable_id:
        return []
    return [f"ukbiobank:ukbiobank:{stable_id}"]


async def run_ukbiobank_ingestion(
    db,
    *,
    records: list[dict],
    log=print,
) -> dict:
    """Ingest the validated UK Biobank census set (metadata only).

    ``records`` is the authoritative ingestion list — each dict is one
    approved UK Biobank census candidate (ukbiobank_candidates.jsonl). A hard
    gate refuses the run unless there are EXACTLY ``UKB_CENSUS_EXPECTED``
    records (21) and every record carries a stable_identifier, so no run can
    silently process a partial or bloated input.

    Never requests UK Biobank API endpoints, participant data or dataset
    files: the census artifact already carries all metadata needed for
    canonical insertion.
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "ukbiobank",
        "input": len(records),
        "expected": UKB_CENSUS_EXPECTED,
        "gate_failed": False,
        "gate_reason": None,
        "normalized": 0,
        "validated_ok": 0,
        "validation_failed": 0,
        "validation_errors": [],
        "duplicate_source_identities": [],
        "inserted": 0,
        "merged": 0,
        "matched_via": {},
        "failed": 0,
        "failure_details": [],
        "api_calls": 0,       # census artifact is authoritative — no API calls
        "asset_calls": 0,     # hard guarantee: no file/asset downloads
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
    }

    t0 = time.monotonic()

    # ── 1) Exact-count gate — EXACTLY UKB_CENSUS_EXPECTED records. ───────────
    if len(records) != UKB_CENSUS_EXPECTED:
        stats["gate_failed"] = True
        stats["gate_reason"] = (
            f"input has {len(records)} records; the approved UK Biobank census "
            f"set is exactly {UKB_CENSUS_EXPECTED} — NOT writing anything"
        )
        log(f"[ukbiobank] *** GATE FAILED: {stats['gate_reason']} ***")
        stats["finished_at"] = _utcnow()
        stats["elapsed_s"] = round(time.monotonic() - t0, 3)
        return stats

    # 2) Identity-completeness gate — every record needs a stable_identifier.
    #    Malformed entries (non-dict) are treated as missing identity so a
    #    corrupted input refuses the ENTIRE run, never a partial write.
    missing_identity: list[dict] = []
    for record in records:
        stable = (
            str(record.get("stable_identifier") or "").strip()
            if isinstance(record, dict)
            else ""
        )
        if not stable:
            missing_identity.append(
                {
                    "dataset_name": (
                        record.get("name") if isinstance(record, dict) else None
                    ),
                    "reason": "missing stable_identifier",
                }
            )
    if missing_identity:
        stats["gate_failed"] = True
        stats["gate_reason"] = (
            f"{len(missing_identity)} records lack a stable_identifier "
            f"(e.g. {missing_identity[0].get('dataset_name')}) — NOT writing anything"
        )
        log(f"[ukbiobank] *** GATE FAILED: {stats['gate_reason']} ***")
        stats["finished_at"] = _utcnow()
        stats["elapsed_s"] = round(time.monotonic() - t0, 3)
        return stats

    # ── 3) Duplicate candidate protection (same stable identity twice). ──────
    # The census artifact is deduplicated, so ANY repeated identity key in the
    # input is a genuine duplicate — the generic resolver would merge them, but
    # the adapter detects and reports it up front.
    seen: dict[str, str] = {}
    for record in records:
        for key in _ukbiobank_identity_keys(record):
            prev = seen.get(key)
            if prev is not None:
                stats["duplicate_source_identities"].append(
                    {
                        "identity": key,
                        "first": prev,
                        "second": str(record.get("stable_identifier") or ""),
                    }
                )
            else:
                seen[key] = str(record.get("stable_identifier") or "")

    # ── 4) Normalize → validate → generic upsert (insert or merge). ──────────
    # UK Biobank sourceUrl is the census Showcase URL verbatim; product
    # identity flows through the deterministic repo:sourceDatasetId/sourceKey
    # (the sourceUrl layer is skipped for UK Biobank — see
    # SHARED_SOURCE_URL_REPOSITORIES in normalize.py).
    for record in records:
        stable_id = str(record.get("stable_identifier") or "").strip()
        try:
            source = build_ukbiobank_source_record(record)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            stats["failure_details"].append(
                f"{stable_id}: normalize: {type(exc).__name__}: {exc}"
            )
            log(f"[ukbiobank] normalize failed for {stable_id}: {exc}")
            continue
        stats["normalized"] += 1

        errs = _validate_source(source)
        if errs:
            stats["validation_failed"] += 1
            stats["validation_errors"].append(f"{stable_id}: {errs[0]}")
            log(f"[ukbiobank] validation failed for {stable_id}: {errs}")
            continue
        stats["validated_ok"] += 1

        try:
            result = await upsert_canonical(collection, source)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            stats["failure_details"].append(
                f"{stable_id}: upsert: {type(exc).__name__}: {exc}"
            )
            log(f"[ukbiobank] upsert failed for {stable_id}: {exc}")
            continue

        if result.get("action") == "invalid":
            stats["validation_failed"] += 1
            stats["validation_errors"].append(
                f"{stable_id}: {result.get('errors', [])[:2]}"
            )
            log(f"[ukbiobank] canonical validation failed for {stable_id}")
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
                        "source": stable_id,
                        "candidates": [
                            cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                        ],
                    }
                )

    stats["asset_calls"] = 0  # hard guarantee: no file/asset downloads
    stats["api_calls"] = 0    # census artifact is authoritative — no API calls
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[ukbiobank] done: input={stats['input']} (expected={stats['expected']}) "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"api_calls={stats['api_calls']} (asset_calls={stats['asset_calls']}) "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats


# EBRAINS ingestion (implementation + dry-run 2026-08-18)
#
# Flow:
#    1. Input — the AUTHORITATIVE, validated EBRAINS census artifact
#       (trace_artifacts/ebrains_census_20260818/ebrains_candidates.jsonl,
#       cross-checked against ebrains_census.json). The hard gate REFUSES the
#       entire run unless the input is EXACTLY 1,138 approved neuroscience
#       records (the raw census holds 1,140 — the two non-neuroscience
#       products are NEVER part of the approved set) and every record carries
#       a dataset_id. The adapter never silently expands or trims the set.
#    2. Normalize each record via build_ebrains_source_record().
#    3. The existing generic pipeline: validate → resolve_identity →
#       upsert_canonical() (insert or merge into the existing canonical
#       record). No new identity/dedup/persistence logic is introduced.
#
# Dataset unit = ONE EBRAINS Dataset product (see normalize.py). The four
# audited EXACT DANDI/OpenNeuro matches resolve through the generic
# cross_reference layer of the resolver (real mirror-page references in
# publication.referencesAndLinks) — they merge into the existing canonical
# record; every other approved record inserts as a new EBRAINS canonical
# record. External-repository DOIs (Zenodo, G-Node, OSF, NITRC, Mendeley,
# figshare, Radboud, publication DOIs) are preserved as relationship/
# provenance metadata ONLY — no records are ever created for the external
# repositories themselves.
#
# Metadata-only: the census artifact already contains every field needed
# (dataset id, title, category, confidence, neuro relevance, DOI, version
# information, release dates, accessibility, species, technique, experimental
# approach, keywords, version ids, URL). ZERO EBRAINS API calls and ZERO
# asset/file downloads — ``api_calls`` and ``asset_calls`` stay 0. No
# participant data, no files, no version-level products. Running this
# function against a LIVE collection is the only write path; a read-only
# dry-run resolution is provided separately (trace_tools/ebrains_dryrun.py)
# and must be executed before any live run.
# ─────────────────────────────────────────────────────────────────────────────


def _ebrains_identity_keys(record: dict) -> list[str]:
    """Deterministic candidate-identity keys for duplicate protection.

    Mirrors what the generic resolver will use: the EBRAINS source key
    (repository:sourceDatasetId = "ebrains:ebrains:<dataset_id>"). Returns []
    when a record has no usable dataset_id (caught as a normalization failure
    downstream).
    """
    dataset_id = str(record.get("dataset_id") or "").strip()
    if not dataset_id:
        return []
    return [f"ebrains:ebrains:{dataset_id}"]


async def run_ebrains_ingestion(
    db,
    *,
    records: list[dict],
    log=print,
) -> dict:
    """Ingest the validated EBRAINS census set (metadata only).

    ``records`` is the authoritative ingestion list — each dict is one
    approved EBRAINS census candidate (ebrains_candidates.jsonl). A hard gate
    refuses the run unless there are EXACTLY ``EBRAINS_CENSUS_EXPECTED``
    records (1,138), every record carries a dataset_id, and NO record is
    classified non-neuroscience — so no run can silently process a partial,
    bloated, or non-neuro input.

    Never requests EBRAINS API endpoints or dataset files: the census
    artifact already carries all metadata needed for canonical insertion.
    """
    collection = get_collection(db)
    stats: dict = {
        "collection": CATALOG_COLLECTION,
        "repository": "ebrains",
        "input": len(records),
        "expected": EBRAINS_CENSUS_EXPECTED,
        "gate_failed": False,
        "gate_reason": None,
        "normalized": 0,
        "validated_ok": 0,
        "validation_failed": 0,
        "validation_errors": [],
        "duplicate_source_identities": [],
        "inserted": 0,
        "merged": 0,
        "matched_via": {},
        "failed": 0,
        "failure_details": [],
        "api_calls": 0,       # census artifact is authoritative — no API calls
        "asset_calls": 0,     # hard guarantee: no file/asset downloads
        "ambiguous_candidates": 0,
        "ambiguous_sample": [],
        "started_at": _utcnow(),
        "finished_at": None,
        "elapsed_s": None,
    }

    t0 = time.monotonic()

    # ── 1) Exact-count gate — EXACTLY EBRAINS_CENSUS_EXPECTED records. ────────
    if len(records) != EBRAINS_CENSUS_EXPECTED:
        stats["gate_failed"] = True
        stats["gate_reason"] = (
            f"input has {len(records)} records; the approved EBRAINS census "
            f"set is exactly {EBRAINS_CENSUS_EXPECTED} — NOT writing anything"
        )
        log(f"[ebrains] *** GATE FAILED: {stats['gate_reason']} ***")
        stats["finished_at"] = _utcnow()
        stats["elapsed_s"] = round(time.monotonic() - t0, 3)
        return stats

    # 2) Completeness gates — every record needs a dataset_id, and no record
    #    may be a non-neuroscience product (the raw census's two NON records
    #    are deliberately never part of the approved set). Malformed entries
    #    (non-dict) are treated as missing identity so a corrupted input
    #    refuses the ENTIRE run, never a partial write.
    missing_identity: list[dict] = []
    for record in records:
        dataset_id = (
            str(record.get("dataset_id") or "").strip()
            if isinstance(record, dict)
            else ""
        )
        if not dataset_id:
            missing_identity.append(
                {
                    "dataset_name": (
                        record.get("title") if isinstance(record, dict) else None
                    ),
                    "reason": "missing dataset_id",
                }
            )
    if missing_identity:
        stats["gate_failed"] = True
        stats["gate_reason"] = (
            f"{len(missing_identity)} records lack a dataset_id "
            f"(e.g. {missing_identity[0].get('dataset_name')}) — NOT writing anything"
        )
        log(f"[ebrains] *** GATE FAILED: {stats['gate_reason']} ***")
        stats["finished_at"] = _utcnow()
        stats["elapsed_s"] = round(time.monotonic() - t0, 3)
        return stats

    non_neuro = [
        str(record.get("title") or "")
        for record in records
        if (record.get("neuro_relevance") == "non_neuroscience")
        or (str(record.get("category") or record.get("dataset_class")).strip().upper() == "NON")
    ]
    if non_neuro:
        stats["gate_failed"] = True
        stats["gate_reason"] = (
            f"{len(non_neuro)} non-neuroscience record(s) present (e.g. "
            f"{non_neuro[0]}) — the approved EBRAINS census set is "
            f"neuroscience-only, NOT writing anything"
        )
        log(f"[ebrains] *** GATE FAILED: {stats['gate_reason']} ***")
        stats["finished_at"] = _utcnow()
        stats["elapsed_s"] = round(time.monotonic() - t0, 3)
        return stats

    # ── 3) Duplicate candidate protection (same dataset identity twice). ──────
    # The census artifact is deduplicated (1138/1138 unique dataset_id AND
    # unique source URL), so ANY repeated identity key in the input is a
    # genuine duplicate — the generic resolver would merge them, but the
    # adapter detects and reports it up front.
    seen: dict[str, str] = {}
    for record in records:
        for key in _ebrains_identity_keys(record):
            prev = seen.get(key)
            if prev is not None:
                stats["duplicate_source_identities"].append(
                    {
                        "identity": key,
                        "first": prev,
                        "second": str(record.get("dataset_id") or ""),
                    }
                )
            else:
                seen[key] = str(record.get("dataset_id") or "")

    # ── 4) Normalize → validate → generic upsert (insert or merge). ──────────
    # EBRAINS sourceUrl is the census url VERBATIM (the real EBRAINS KG
    # instance page, unique per dataset — so EBRAINS is NOT in
    # SHARED_SOURCE_URL_REPOSITORIES). Product identity flows through the
    # deterministic repo:sourceDatasetId/sourceKey; the four audited exact
    # DANDI/OpenNeuro matches are found by the generic resolver's
    # cross_reference layer and merge into the existing canonical record.
    for record in records:
        dataset_id = str(record.get("dataset_id") or "").strip()
        try:
            source = build_ebrains_source_record(record)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            stats["failure_details"].append(
                f"{dataset_id}: normalize: {type(exc).__name__}: {exc}"
            )
            log(f"[ebrains] normalize failed for {dataset_id}: {exc}")
            continue
        stats["normalized"] += 1

        errs = _validate_source(source)
        if errs:
            stats["validation_failed"] += 1
            stats["validation_errors"].append(f"{dataset_id}: {errs[0]}")
            log(f"[ebrains] validation failed for {dataset_id}: {errs}")
            continue
        stats["validated_ok"] += 1

        try:
            result = await upsert_canonical(collection, source)
        except Exception as exc:  # noqa: BLE001
            stats["failed"] += 1
            stats["failure_details"].append(
                f"{dataset_id}: upsert: {type(exc).__name__}: {exc}"
            )
            log(f"[ebrains] upsert failed for {dataset_id}: {exc}")
            continue

        if result.get("action") == "invalid":
            stats["validation_failed"] += 1
            stats["validation_errors"].append(
                f"{dataset_id}: {result.get('errors', [])[:2]}"
            )
            log(f"[ebrains] canonical validation failed for {dataset_id}")
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
                        "source": dataset_id,
                        "candidates": [
                            cand.get("canonicalDatasetId") for cand in ambiguous[:3]
                        ],
                    }
                )

    stats["asset_calls"] = 0  # hard guarantee: no file/asset downloads
    stats["api_calls"] = 0    # census artifact is authoritative — no API calls
    stats["elapsed_s"] = round(time.monotonic() - t0, 3)
    stats["finished_at"] = _utcnow()
    log(
        f"[ebrains] done: input={stats['input']} (expected={stats['expected']}) "
        f"normalized={stats['normalized']} inserted={stats['inserted']} "
        f"merged={stats['merged']} failed={stats['failed']} "
        f"api_calls={stats['api_calls']} (asset_calls={stats['asset_calls']}) "
        f"elapsed={stats['elapsed_s']}s"
    )
    return stats
