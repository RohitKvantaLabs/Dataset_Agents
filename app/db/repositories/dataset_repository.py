import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

from bson import ObjectId
from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import BulkWriteError

from app.db.mongo import get_db
from app.models.dataset import Dataset, TrustTier

logger = logging.getLogger("neuro_platform.db.dataset_repository")

COLLECTION_NAME = "datasets"


# ─────────────────────────────────────────────────────────────────────────────
# Canonical identity keys (§3.6, shared with quality_pipeline Stage 6)
#
# The (source, source_id) upsert key alone cannot guarantee a single canonical
# record: the same dataset can be discovered as a repository record
# (e.g. ``dandi:DANDI:000003``) and again as a web-discovery record
# (``web_search:sha1(url)``), producing two documents.  To make persistence
# idempotent across discovery sources, every write first resolves the
# dataset's normalized URL / DOI against existing documents and re-targets the
# write to the existing canonical record (refresh, never duplicate).
#
# Provenance discovery history (§3.7): the ``provenance`` document keeps the
# latest-snapshot fields (source_repository, harvested_at, …) for backward
# compatibility AND an append-only ``discovery_history`` array that records
# every discovery event across syncs / searches / re-discoveries, trimmed to
# the most recent PROVENANCE_HISTORY_CAP events (FIFO). ``first_seen_at``,
# ``last_seen_at`` and ``discovery_count`` are maintained at the top level of
# ``provenance`` for cheap, index-free querying.
# ─────────────────────────────────────────────────────────────────────────────

# Most recent discovery events retained in provenance.discovery_history (FIFO
# trim — oldest dropped first). Prevents unbounded document growth across
# repeated synchronizations while preserving a complete recent history.
PROVENANCE_HISTORY_CAP: int = 20


def _normalize_doc(doc: Any) -> Any:
    """
    Repository-layer boundary translation: convert every ``bson.ObjectId``
    value in a raw Mongo document to ``str`` before domain-model validation.

    Stored documents carry ``_id`` as a ``bson.ObjectId``, but the pure
    domain ``Dataset`` model declares string identifiers
    (``id: str | None = Field(default=None, alias="_id")``). Pydantic v2
    refuses to coerce ``ObjectId`` → ``str``, so without this normalization
    every persisted document fails validation and is silently skipped by the
    canonical re-query (``find_datasets_by_identity``) — collapsing the
    repository search result to zero datasets.

    The conversion is recursive so ObjectIds nested inside sub-documents or
    arrays (e.g. provenance structures) are normalized too, and ``bson``
    types never leak into business logic — this module is the only place
    that touches them.
    """
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, dict):
        return {k: _normalize_doc(v) for k, v in doc.items()}
    if isinstance(doc, (list, tuple)):
        return [_normalize_doc(v) for v in doc]
    return doc


def _derive_discovery_event(prov: dict, now: datetime) -> dict | None:
    """Synthesize a discovery event from a provenance snapshot that predates
    ``discovery_history`` (legacy documents / non-pipeline callers). Returns
    None when there is nothing meaningful to record."""
    if not isinstance(prov, dict) or not prov.get("source_repository"):
        return None
    return {
        "source": prov.get("source_repository"),
        "discovery_method": prov.get("discovery_method"),
        "source_api": prov.get("source_api"),
        "harvest_query": prov.get("harvest_query"),
        "harvested_at": prov.get("harvested_at") or now.isoformat(),
        "pipeline_version": prov.get("pipeline_version"),
    }


def merge_provenance(
    existing: dict | None,
    incoming: dict | None,
    now: datetime | None = None,
) -> dict:
    """
    Merge provenance across a canonical write: refresh, never lose history.

    Returns a single provenance dict with:
    - Latest-snapshot fields: incoming wins where present, else existing
      (backward compatible — legacy fields such as ``dedup_key`` survive).
    - ``discovery_history``: existing events + the incoming event(s), FIFO-
      trimmed to the most recent PROVENANCE_HISTORY_CAP.
    - Top-level ``first_seen_at`` / ``last_seen_at`` / ``discovery_count``
      maintained for efficient querying.

    When ``incoming`` carries no ``discovery_history`` (legacy caller), a
    single event is derived from its snapshot fields so every write is still
    recorded.
    """
    now = now or datetime.now(timezone.utc)
    existing = existing if isinstance(existing, dict) else {}
    incoming = incoming if isinstance(incoming, dict) else {}

    existing_events = [e for e in existing.get("discovery_history") or [] if isinstance(e, dict)]
    incoming_events = [e for e in incoming.get("discovery_history") or [] if isinstance(e, dict)]
    # Legacy documents (or callers that predate discovery_history) still record
    # their original discovery: derive an event from the snapshot so the
    # upgrade path keeps first-seen fidelity.
    if not existing_events:
        derived = _derive_discovery_event(existing, now)
        if derived is not None:
            existing_events = [derived]
    if not incoming_events:
        derived = _derive_discovery_event(incoming, now)
        if derived is not None:
            incoming_events = [derived]

    merged = dict(existing)          # base: existing snapshot survives
    for k, v in incoming.items():    # incoming snapshot fields win where present
        merged[k] = v

    merged["discovery_history"] = (existing_events + incoming_events)[-PROVENANCE_HISTORY_CAP:]
    # discovery_count = total events ever seen (NOT capped). Prefer the stored
    # counter; when absent (legacy doc), fall back to its event count so a
    # derived legacy event is still counted.
    existing_count = existing.get("discovery_count")
    if existing_count is None:
        existing_count = len(existing_events)
    merged["discovery_count"] = int(existing_count) + len(incoming_events)

    first = existing.get("first_seen_at")
    if first is None and existing_events:
        first = existing_events[0].get("harvested_at")
    if first is None and incoming_events:
        first = incoming_events[0].get("harvested_at")
    merged["first_seen_at"] = first or merged.get("first_seen_at")

    last = incoming_events[-1].get("harvested_at") if incoming_events else None
    merged["last_seen_at"] = (
        last
        or incoming.get("last_seen_at")
        or merged.get("last_seen_at")
        or merged.get("harvested_at")
    )
    return merged


async def _find_canonical_matches(
    datasets: list[Dataset],
) -> tuple[dict[str, dict], dict[str, dict], dict[tuple[str, str], dict]]:
    """
    Resolve existing canonical documents for a batch of datasets.

    Returns (url_by_key, doi_by_key, id_by_key) — maps from normalized
    URL key / normalized DOI / ``(source, source_id)`` identity → the existing
    document (projected with source, source_id, url, doi, provenance).  A
    candidate whose normalized URL, DOI, or identity collides with an existing
    document is a mirror, a re-discovery, or a cross-source duplicate — it must
    refresh that record (including merging discovery history), never be
    inserted as a second document.

    Uses the existing ``url`` / ``doi`` (sparse) indexes and the unique
    ``(source, source_id)`` index; normalization happens in Python so stored
    vs. incoming URL formats need not be byte-identical.
    """
    db = get_db()
    url_by_key: dict[str, dict] = {}
    doi_by_key: dict[str, dict] = {}
    id_by_key: dict[tuple[str, str], dict] = {}

    raw_urls = {str(ds.url) for ds in datasets}
    url_query = raw_urls | {normalize_url_key(u) for u in raw_urls if u}
    raw_dois = {d for d in (ds.doi for ds in datasets) if d}
    doi_query = raw_dois | {d for d in (normalize_doi(x) for x in raw_dois) if d}
    id_pairs = {(ds.source, ds.source_id) for ds in datasets if ds.source and ds.source_id}

    clauses = []
    if url_query:
        clauses.append({"url": {"$in": list(url_query)}})
    if doi_query:
        clauses.append({"doi": {"$in": list(doi_query)}})
    if id_pairs:
        clauses.append({"$or": [{"source": s, "source_id": i} for s, i in id_pairs]})
    if not clauses:
        return url_by_key, doi_by_key, id_by_key

    projection = {"source": 1, "source_id": 1, "url": 1, "doi": 1, "provenance": 1}
    cursor = db[COLLECTION_NAME].find({"$or": clauses}, projection)
    async for doc in cursor:
        if not doc.get("source") or not doc.get("source_id"):
            continue
        key = normalize_url_key(doc.get("url") or "")
        if key:
            url_by_key.setdefault(key, doc)
        d = normalize_doi(doc.get("doi"))
        if d:
            doi_by_key.setdefault(d, doc)
        id_by_key.setdefault((doc["source"], doc["source_id"]), doc)
    return url_by_key, doi_by_key, id_by_key


def _canonical_target(
    dataset: Dataset,
    url_by_key: dict[str, dict],
    doi_by_key: dict[str, dict],
) -> dict | None:
    """Return the existing canonical document for a dataset, if any (URL/DOI)."""
    target = url_by_key.get(normalize_url_key(str(dataset.url)))
    if target is None:
        target = doi_by_key.get(normalize_doi(dataset.doi))
    return target


def _rewrite_provenance_source(prov: dict, source: str, source_id: str) -> None:
    """Point the provenance snapshot at the canonical record after a merge."""
    if isinstance(prov, dict):
        prov["dedup_key"] = f"{source}:{source_id}"
        prov["source_repository"] = source


def normalize_url_key(url: str) -> str:
    """§3.6 key 2 — lowercase host, http/https merged, strip www., trailing /, query, fragment."""
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "").rstrip("/")
        return f"http://{host}{path}"
    except Exception:  # noqa: BLE001
        return (url or "").lower().strip().rstrip("/")


def normalize_doi(doi: str | None) -> str | None:
    """§3.6 key 3 — case-insensitive, strip common DOI prefixes."""
    if not doi:
        return None
    d = doi.strip().lower()
    for prefix in (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "doi:",
    ):
        if d.startswith(prefix):
            d = d[len(prefix):]
            break
    return d or None


async def upsert_dataset(dataset: Dataset) -> None:
    """
    Single atomic operation - update_one(upsert=True) keyed on
    (source, source_id). This is deliberately NOT a read-then-write:
    two fallback workers finding the same dataset at the same time will
    both hit this, and Mongo resolves it atomically instead of racing.
    Pair this with a unique index on {source, source_id} (see indexes.py)
    as a hard backstop.
    """
    db = get_db()
    now = datetime.now(timezone.utc)

    # Canonical merge: if the dataset's URL/DOI (or its own identity) already
    # exists in Mongo, refresh that canonical record instead of inserting a
    # duplicate (idempotent across discovery sources and sync runs), and merge
    # provenance discovery history so repeated discoveries accumulate events
    # instead of overwriting them.
    url_by_key, doi_by_key, id_by_key = await _find_canonical_matches([dataset])
    target_doc = _canonical_target(dataset, url_by_key, doi_by_key)
    if target_doc is None:
        target_doc = id_by_key.get((dataset.source, dataset.source_id))
    if target_doc is not None:
        target_key = (target_doc.get("source"), target_doc.get("source_id"))
        if dataset.provenance is not None or target_doc.get("provenance"):
            dataset.provenance = merge_provenance(
                target_doc.get("provenance"), dataset.provenance, now
            )
        if target_key != (dataset.source, dataset.source_id):
            logger.info(
                "Canonical merge: %s:%s refreshed into existing %s:%s",
                dataset.source, dataset.source_id, target_key[0], target_key[1],
            )
            dataset.source, dataset.source_id = target_key[0], target_key[1]
            _rewrite_provenance_source(dataset.provenance, dataset.source, dataset.source_id)

    payload = dataset.model_dump(exclude={"id", "ingested_at"}, exclude_none=False, mode="json")
    payload["updated_at"] = now.isoformat()

    doc = await db[COLLECTION_NAME].find_one_and_update(
        {"source": dataset.source, "source_id": dataset.source_id},
        {
            "$set": payload,
            "$setOnInsert": {"ingested_at": now.isoformat()},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    if doc and "_id" in doc:
        dataset.id = str(doc["_id"])
    logger.info("Upserted dataset source=%s source_id=%s id=%s", dataset.source, dataset.source_id, dataset.id)


async def bulk_upsert(datasets: list[Dataset]) -> int:
    """
    Batched upsert via a single MongoDB bulkWrite command.

    All upserts are sent in ONE round-trip instead of N individual
    operations (parallel or sequential).  Uses `updateOne` with
    ``upsert=True`` keyed on (source, source_id) — same atomic
    semantics as `upsert_dataset` but without the per-document
    response overhead.

    Chunks at 100 documents to avoid oversized write commands on
    large ingestion runs.
    """
    db = get_db()
    now = datetime.now(timezone.utc)

    if not datasets:
        return 0

    # Canonical merge across the whole batch before building the write ops:
    # any dataset whose normalized URL/DOI/identity matches an existing document
    # is re-targeted to that record (refresh, never duplicate) AND its
    # provenance discovery history is merged so repeated discoveries accumulate
    # events (FIFO-capped) instead of overwriting them.
    url_by_key, doi_by_key, id_by_key = await _find_canonical_matches(datasets)
    merged_prov_by_key: dict[tuple[str, str], dict] = {}
    for ds in datasets:
        original_key = (ds.source, ds.source_id)
        target_doc = _canonical_target(ds, url_by_key, doi_by_key)
        if target_doc is None:
            target_doc = id_by_key.get(original_key)
        if target_doc is not None:
            target_key = (target_doc.get("source"), target_doc.get("source_id"))
            existing_prov = merged_prov_by_key.get(target_key) or target_doc.get("provenance")
            if ds.provenance is not None or existing_prov:
                ds.provenance = merge_provenance(existing_prov, ds.provenance, now)
            merged_prov_by_key[target_key] = ds.provenance or {}
            if target_key != original_key:
                logger.info(
                    "Canonical merge: %s:%s refreshed into existing %s:%s",
                    ds.source, ds.source_id, target_key[0], target_key[1],
                )
                ds.source, ds.source_id = target_key[0], target_key[1]
                _rewrite_provenance_source(ds.provenance, ds.source, ds.source_id)

    chunk_size = 100
    total = 0

    for i in range(0, len(datasets), chunk_size):
        chunk = datasets[i : i + chunk_size]
        operations = []

        for ds in chunk:
            payload = ds.model_dump(
                exclude={"id", "ingested_at"}, exclude_none=False, mode="json"
            )
            payload["updated_at"] = now.isoformat()

            operations.append(
                UpdateOne(
                    filter={"source": ds.source, "source_id": ds.source_id},
                    update={
                        "$set": payload,
                        "$setOnInsert": {"ingested_at": now.isoformat()},
                    },
                    upsert=True,
                )
            )

        if operations:
            try:
                await db[COLLECTION_NAME].bulk_write(operations, ordered=False)
                total += len(chunk)
            except BulkWriteError as exc:
                failed = len(exc.details.get("writeErrors", []))
                total += len(chunk) - failed
                logger.error("bulk_upsert: %d writes failed in a chunk: %s", failed, exc.details)

    logger.info("bulk_upsert: %d datasets in %d chunks", len(datasets), (len(datasets) + chunk_size - 1) // chunk_size)
    return total


async def upsert_many(datasets: list[Dataset]) -> int:
    # ponytail: parallel upserts — N separate network roundtrips fire at once.
    import asyncio
    await asyncio.gather(*[upsert_dataset(ds) for ds in datasets])
    return len(datasets)


async def find_datasets_by_identity(pairs: list[tuple[str, str]]) -> list[Dataset]:
    """
    Fetch the canonical Mongo documents for a set of ``(source, source_id)``
    identities and return them as Dataset models with ``id`` populated from
    the document ``_id``.

    Used by repository-search after Stage 7 publication: the pipeline
    returns in-memory Datasets without ``_id``; this re-query returns the
    persisted canonical documents (post canonical-merge identities, since
    ``bulk_upsert`` mutates ``source``/``source_id`` when a URL/DOI merge
    re-targets a record) so the API response carries Mongo-backed records
    (``_id``, provenance, quality score) — the same shape subsequent cache
    hits and ``getById`` serve.

    Result order follows input order; duplicate identities collapse to one
    document.
    """
    if not pairs:
        return []
    order: dict[tuple[str, str], int] = {}
    keys: list[tuple[str, str]] = []
    for pair in pairs:
        if not pair[0] or not pair[1]:
            continue
        if pair not in order:
            order[pair] = len(order)
            keys.append(pair)
    if not keys:
        return []

    db = get_db()
    cursor = db[COLLECTION_NAME].find(
        {"$or": [{"source": s, "source_id": i} for s, i in keys]}
    )
    docs = await cursor.to_list(length=len(keys))
    out: list[Dataset] = []
    for doc in docs:
        # Repository boundary: stored `_id` is a bson.ObjectId; normalize to str
        # so the pure domain Dataset model validates. Project convention:
        # swallow + log — one invalid stored document never aborts the whole
        # re-query (and with it the entire repository-search response).
        try:
            out.append(Dataset.model_validate(_normalize_doc(doc)))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "find_datasets_by_identity: skipping invalid doc (%s:%s): %s",
                doc.get("source"),
                doc.get("source_id"),
                exc,
            )
    out.sort(key=lambda d: order.get((d.source, d.source_id), len(order)))
    return out


async def find_datasets_for_reverification(stale_before: datetime, limit: int) -> list[Dataset]:
    """
    Return verified/stale datasets whose links are due for scheduled re-check.
    Unverified fallback candidates are intentionally excluded.
    """
    db = get_db()
    cursor = (
        db[COLLECTION_NAME]
        .find(
            {
                "trust_tier": {"$in": [TrustTier.VERIFIED.value, TrustTier.STALE.value]},
                "$or": [
                    {"last_verified_at": None},
                    {"last_verified_at": {"$lt": stale_before.isoformat()}},
                    {"last_verified_at": {"$exists": False}},
                ],
            }
        )
        .limit(limit)
    )
    docs = await cursor.to_list(length=limit)
    # Repository boundary: stored `_id` is a bson.ObjectId; normalize to str so
    # the pure domain Dataset model validates.
    return [Dataset.model_validate(_normalize_doc(doc)) for doc in docs]


async def update_verification_status(dataset: Dataset, trust_tier: TrustTier, verified_at: datetime) -> None:
    """
    Update the existing document after cron revalidation. This is deliberately
    not an upsert; the cron job only processes documents already in MongoDB.
    """
    db = get_db()
    await db[COLLECTION_NAME].update_one(
        {"source": dataset.source, "source_id": dataset.source_id},
        {
            "$set": {
                "trust_tier": trust_tier.value,
                "last_verified_at": verified_at.isoformat(),
                "updated_at": verified_at.isoformat(),
            }
        },
    )


async def bulk_update_verification_status(
    records: list[tuple[Dataset, TrustTier, datetime]],
) -> None:
    """
    Batch-update verification statuses for multiple datasets in a single
    MongoDB bulkWrite command.

    Each element of *records* is a tuple of
    (dataset, trust_tier, verified_at).  The bulk write replaces what
    would otherwise be N individual ``update_one`` calls.

    Chunks at 100 documents to avoid oversized write commands.
    """
    db = get_db()
    chunk_size = 100

    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        operations = []

        for dataset, trust_tier, verified_at in chunk:
            iso = verified_at.isoformat()
            operations.append(
                UpdateOne(
                    filter={
                        "source": dataset.source,
                        "source_id": dataset.source_id,
                    },
                    update={
                        "$set": {
                            "trust_tier": trust_tier.value,
                            "last_verified_at": iso,
                            "updated_at": iso,
                        }
                    },
                )
            )

        if operations:
            await db[COLLECTION_NAME].bulk_write(operations, ordered=False)
