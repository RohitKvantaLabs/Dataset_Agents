"""
Canonical persistence — idempotent, single-record-per-dataset writes.

Covers the stabilization requirement that the same dataset is never stored more
than once even when discovered from multiple repositories, mirrors, repeated
searches, or future synchronizations: a write whose normalized URL or DOI
matches an existing document re-targets to that canonical record (refresh)
instead of inserting a duplicate.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.db.repositories.dataset_repository import (
    PROVENANCE_HISTORY_CAP,
    bulk_upsert,
    find_datasets_by_identity,
    merge_provenance,
    normalize_doi,
    normalize_url_key,
    upsert_dataset,
)
from app.models.dataset import Dataset


def _full_provenance(
    source: str = "openneuro",
    harvested_at: str = "2026-08-01T00:00:00+00:00",
    method: str = "batch_sync",
    harvest_query: str = "fMRI",
) -> dict:
    """A provenance snapshot as built by the quality pipeline Stage 7:
    latest-snapshot fields + discovery_history + top-level counters."""
    return {
        "source_repository": source,
        "source_api": "openneuro.org",
        "harvest_query": harvest_query,
        "harvested_at": harvested_at,
        "pipeline_version": "v2",
        "enrichment_sources": [],
        "dedup_key": f"{source}:ds000001",
        "enrichment": {"sources": [], "started_at": None, "completed_at": None},
        "first_seen_at": harvested_at,
        "last_seen_at": harvested_at,
        "discovery_count": 1,
        "discovery_history": [
            {
                "source": source,
                "discovery_method": method,
                "source_api": "openneuro.org",
                "harvest_query": harvest_query,
                "harvested_at": harvested_at,
                "pipeline_version": "v2",
            }
        ],
    }


async def _fake_cursor(docs):
    for doc in docs:
        yield doc


def _dataset(
    source: str = "web_search",
    source_id: str = "sha1abc123",
    url: str = "https://openneuro.org/datasets/ds000001",
    doi: str | None = None,
    title: str = "Test dataset",
) -> Dataset:
    return Dataset(
        title=title,
        source=source,
        source_id=source_id,
        url=url,
        doi=doi,
        provenance={"dedup_key": f"{source}:{source_id}", "source_repository": source},
    )


def _mock_collection(existing_docs: list[dict]) -> MagicMock:
    """A collection whose find() returns existing docs and whose writes are
    recorded; the get_db() patch is applied by the caller via the returned db."""
    mock_collection = MagicMock()
    # find() is NOT awaited in _find_canonical_matches (Motor-style cursor), so
    # a plain MagicMock returns the async generator directly.
    mock_collection.find = MagicMock(side_effect=lambda *a, **k: _fake_cursor(existing_docs))
    mock_collection.bulk_write = AsyncMock(return_value=MagicMock(bulk_api_result={}))
    mock_collection.find_one_and_update = AsyncMock(
        return_value={"_id": "canonical", "source": "openneuro", "source_id": "ds000001"}
    )
    return mock_collection


def _mock_db(existing_docs: list[dict]) -> tuple[MagicMock, MagicMock]:
    collection = _mock_collection(existing_docs)
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=collection)
    return db, collection


class _ListCursor:
    """Motor-style cursor supporting both ``async for`` and ``await to_list()``."""

    def __init__(self, docs: list[dict]):
        self._docs = docs

    def __aiter__(self):
        self._it = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration

    async def to_list(self, length: int | None = None):
        return self._docs[:length] if length else self._docs


class _MergeCollection:
    """Faithful in-memory Mongo for the canonical-merge full-flow tests.

    ``find`` matches the ``$or`` clauses built by ``_find_canonical_matches``
    (url ``$in`` / doi ``$in`` / identity) and ``find_datasets_by_identity``
    (identity only); ``bulk_write`` applies real upsert semantics — an
    existing ``(source, source_id)`` document is updated in place, otherwise a
    new document is inserted with a generated ``_id``. No application function
    is mocked: the real ``bulk_upsert`` / ``find_datasets_by_identity`` code
    runs against this harness.
    """

    def __init__(self, seed_docs: list[dict]):
        self.docs: dict[str, dict] = {d["_id"]: dict(d) for d in seed_docs}
        self.writes: list[dict] = []
        self._oid = 100

    def find(self, query, projection=None):
        clauses = query.get("$or") or []
        matched = [dict(doc) for doc in self.docs.values() if self._matches(doc, clauses)]
        return _ListCursor(matched)

    @staticmethod
    def _matches(doc: dict, clauses: list[dict]) -> bool:
        for c in clauses:
            if "url" in c:
                vals = c["url"].get("$in") or []
                if str(doc.get("url") or "") in vals or normalize_url_key(str(doc.get("url") or "")) in vals:
                    return True
            elif "doi" in c:
                vals = c["doi"].get("$in") or []
                if doc.get("doi") in vals or normalize_doi(doc.get("doi")) in vals:
                    return True
            elif "source" in c:
                if doc.get("source") == c["source"] and doc.get("source_id") == c["source_id"]:
                    return True
        return False

    async def bulk_write(self, operations, ordered=False):
        for op in operations:
            f = op._filter
            update = op._doc
            self.writes.append({"filter": f, "update": update, "upsert": op._upsert})
            existing = self._by_key(f["source"], f["source_id"])
            if existing is not None:
                existing.update(update.get("$set", {}))
                for k, v in (update.get("$setOnInsert") or {}).items():
                    existing.setdefault(k, v)
            else:
                doc = dict(update.get("$set", {}))
                doc.update(update.get("$setOnInsert") or {})
                doc["_id"] = f"fake_oid_{self._oid}"
                self._oid += 1
                self.docs[doc["_id"]] = doc

    def _by_key(self, source: str, source_id: str) -> dict | None:
        for doc in self.docs.values():
            if doc.get("source") == source and doc.get("source_id") == source_id:
                return doc
        return None


# ---------------------------------------------------------------------------
# Key normalization
# ---------------------------------------------------------------------------


class TestKeyNormalization:
    def test_url_key_normalization(self) -> None:
        assert (
            normalize_url_key("https://www.OpenNeuro.org/datasets/ds000001/")
            == "http://openneuro.org/datasets/ds000001"
        )
        assert (
            normalize_url_key("http://openneuro.org/datasets/ds000001?x=1#y")
            == "http://openneuro.org/datasets/ds000001"
        )
        assert normalize_url_key("https://example.edu/a") == "http://example.edu/a"

    def test_doi_normalization(self) -> None:
        assert normalize_doi("10.5281/zenodo.12345") == "10.5281/zenodo.12345"
        assert normalize_doi("https://doi.org/10.5061/dryad.x") == "10.5061/dryad.x"
        assert normalize_doi("DOI:10.1234/ABC") == "10.1234/abc"
        assert normalize_doi(None) is None
        assert normalize_doi("") is None


# ---------------------------------------------------------------------------
# find_datasets_by_identity — canonical re-query (repository-search contract)
# ---------------------------------------------------------------------------


class TestFindDatasetsByIdentity:
    """Re-query used by repository-search after Stage 7 publication: returns the
    persisted canonical documents (with `_id`) in input order — never transient
    in-memory previews."""

    @staticmethod
    def _persisted_doc(source: str, source_id: str, oid: str, url: str, title: str) -> dict:
        return {
            "_id": oid,
            "source": source,
            "source_id": source_id,
            "url": url,
            "title": title,
            "modality": ["fmri"],
            "species": [],
        }

    def _collection(self, docs: list[dict]) -> MagicMock:
        collection = MagicMock()
        cursor = MagicMock()
        cursor.to_list = AsyncMock(return_value=docs)
        collection.find = MagicMock(return_value=cursor)
        return collection

    @pytest.mark.asyncio
    async def test_returns_persisted_docs_in_input_order_with_id(self) -> None:
        docs = [
            self._persisted_doc(
                "zenodo", "4938058", "66f0aaa",
                "https://zenodo.org/records/4938058", "Resting brain ALFF",
            ),
            self._persisted_doc(
                "figshare", "33150869", "66f0bbb",
                "https://figshare.com/articles/dataset/x/33150869", "Some figshare dataset",
            ),
        ]
        collection = self._collection(docs)
        db = MagicMock()
        db.__getitem__ = MagicMock(return_value=collection)

        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            result = await find_datasets_by_identity(
                [("figshare", "33150869"), ("zenodo", "4938058")]
            )

        # `_id` populated; response order follows the input identities
        assert [d.id for d in result] == ["66f0bbb", "66f0aaa"]
        assert [d.source for d in result] == ["figshare", "zenodo"]
        query = collection.find.call_args.args[0]
        assert query == {
            "$or": [
                {"source": "figshare", "source_id": "33150869"},
                {"source": "zenodo", "source_id": "4938058"},
            ]
        }

    @pytest.mark.asyncio
    async def test_duplicate_identities_collapse_to_one_query_clause(self) -> None:
        doc = self._persisted_doc(
            "zenodo", "4938058", "66f0aaa",
            "https://zenodo.org/records/4938058", "Resting brain ALFF",
        )
        collection = self._collection([doc])
        db = MagicMock()
        db.__getitem__ = MagicMock(return_value=collection)

        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            result = await find_datasets_by_identity(
                [("zenodo", "4938058"), ("zenodo", "4938058")]
            )

        assert len(result) == 1
        query = collection.find.call_args.args[0]
        assert len(query["$or"]) == 1

    @pytest.mark.asyncio
    async def test_empty_pairs_short_circuits_without_mongo(self) -> None:
        db = MagicMock()
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            result = await find_datasets_by_identity([])
        assert result == []
        db.__getitem__.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_stored_doc_is_skipped_not_abort(self) -> None:
        """A stored document that fails schema validation (e.g. malformed url)
        is skipped and logged — the rest of the pool is still returned."""
        bad = self._persisted_doc("bad", "x", "66f0ccc", "not a url", "Broken doc")
        good = self._persisted_doc(
            "openneuro", "ds000001", "66f0ddd",
            "https://openneuro.org/datasets/ds000001", "Healthy dataset",
        )
        collection = self._collection([bad, good])
        db = MagicMock()
        db.__getitem__ = MagicMock(return_value=collection)

        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            result = await find_datasets_by_identity([("bad", "x"), ("openneuro", "ds000001")])

        assert [d.source for d in result] == ["openneuro"]
        assert result[0].id == "66f0ddd"


# ---------------------------------------------------------------------------
# bulk_upsert — canonical merge
# ---------------------------------------------------------------------------


class TestBulkUpsertCanonical:
    @pytest.mark.asyncio
    async def test_web_candidate_retargeted_to_existing_repo_record(self) -> None:
        existing = {
            "source": "openneuro",
            "source_id": "ds000001",
            "url": "https://openneuro.org/datasets/ds000001",
            "doi": None,
        }
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            web = _dataset()
            written = await bulk_upsert([web])

        assert written == 1
        # The web-discovered dataset refreshed the canonical repository record
        assert (web.source, web.source_id) == ("openneuro", "ds000001")
        ops = collection.bulk_write.await_args.args[0]
        assert ops[0]._filter == {"source": "openneuro", "source_id": "ds000001"}
        assert web.provenance["dedup_key"] == "openneuro:ds000001"

    @pytest.mark.asyncio
    async def test_cross_format_url_variants_merge(self) -> None:
        """http/https + www + trailing slash variants reconcile via the
        normalized key even though the stored URL differs byte-for-byte."""
        existing = {
            "source": "openneuro",
            "source_id": "ds000001",
            "url": "https://www.openneuro.org/datasets/ds000001/",
            "doi": None,
        }
        web = _dataset(url="http://openneuro.org/datasets/ds000001")
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            await bulk_upsert([web])

        assert (web.source, web.source_id) == ("openneuro", "ds000001")
        ops = collection.bulk_write.await_args.args[0]
        assert ops[0]._filter == {"source": "openneuro", "source_id": "ds000001"}

    @pytest.mark.asyncio
    async def test_doi_cross_source_merge(self) -> None:
        existing = {
            "source": "zenodo",
            "source_id": "12345",
            "url": "https://zenodo.org/records/12345",
            "doi": "10.5281/zenodo.12345",
        }
        web = _dataset(
            url="https://some-mirror.example/record",  # different URL
            doi="10.5281/ZENODO.12345",  # same DOI, different casing
        )
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            await bulk_upsert([web])

        assert (web.source, web.source_id) == ("zenodo", "12345")
        ops = collection.bulk_write.await_args.args[0]
        assert ops[0]._filter == {"source": "zenodo", "source_id": "12345"}

    @pytest.mark.asyncio
    async def test_distinct_dataset_keeps_own_identity(self) -> None:
        existing = {
            "source": "openneuro",
            "source_id": "ds000001",
            "url": "https://openneuro.org/datasets/ds000001",
            "doi": None,
        }
        distinct = _dataset(
            source="web_search",
            source_id="sha1other",
            url="https://example.edu/sub-01_T1w.nii.gz",
        )
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            await bulk_upsert([distinct])

        assert (distinct.source, distinct.source_id) == ("web_search", "sha1other")
        ops = collection.bulk_write.await_args.args[0]
        assert ops[0]._filter == {"source": "web_search", "source_id": "sha1other"}

    @pytest.mark.asyncio
    async def test_repeat_write_is_idempotent(self) -> None:
        """The same web discovery written twice yields one record (same key)."""
        existing = {
            "source": "web_search",
            "source_id": "sha1abc123",
            "url": "https://example.edu/data",
            "doi": None,
        }
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            ds = _dataset(url="https://example.edu/data")
            await bulk_upsert([ds])

        assert (ds.source, ds.source_id) == ("web_search", "sha1abc123")
        ops = collection.bulk_write.await_args.args[0]
        assert ops[0]._filter == {"source": "web_search", "source_id": "sha1abc123"}


# ---------------------------------------------------------------------------
# Provenance discovery history — append-only, FIFO-capped merge
# ---------------------------------------------------------------------------


class TestProvenanceDiscoveryHistory:
    def test_merge_appends_event_and_preserves_first_seen(self) -> None:
        existing = _full_provenance(harvested_at="2026-08-01T00:00:00+00:00", method="batch_sync")
        incoming = _full_provenance(harvested_at="2026-08-02T00:00:00+00:00", method="web_search")

        merged = merge_provenance(existing, incoming)

        assert merged["discovery_count"] == 2
        assert len(merged["discovery_history"]) == 2
        assert merged["discovery_history"][0]["discovery_method"] == "batch_sync"
        assert merged["discovery_history"][1]["discovery_method"] == "web_search"
        # First-seen survives the merge; last-seen advances to the newest event
        assert merged["first_seen_at"] == "2026-08-01T00:00:00+00:00"
        assert merged["last_seen_at"] == "2026-08-02T00:00:00+00:00"
        # Latest-snapshot fields come from the incoming write
        assert merged["source_repository"] == "openneuro"

    def test_merge_without_incoming_history_derives_event(self) -> None:
        existing = _full_provenance(harvested_at="2026-08-01T00:00:00+00:00")
        # Legacy caller: provenance dict without discovery_history
        incoming = {"source_repository": "openneuro", "dedup_key": "openneuro:ds000001"}

        merged = merge_provenance(existing, incoming)

        assert merged["discovery_count"] == 2
        assert len(merged["discovery_history"]) == 2
        assert merged["first_seen_at"] == "2026-08-01T00:00:00+00:00"

    def test_fifo_cap_keeps_most_recent_20(self) -> None:
        base = _full_provenance(harvested_at="2026-08-01T00:00:00+00:00")
        # 20 existing events (already at cap)
        existing = dict(base)
        existing["discovery_history"] = [
            {
                "source": "openneuro",
                "discovery_method": "batch_sync",
                "source_api": "openneuro.org",
                "harvest_query": "q",
                "harvested_at": f"2026-07-{day:02d}T00:00:00+00:00",
                "pipeline_version": "v2",
            }
            for day in range(1, 21)
        ]
        existing["discovery_count"] = 20
        existing["first_seen_at"] = "2026-07-01T00:00:00+00:00"
        incoming = _full_provenance(harvested_at="2026-08-02T00:00:00+00:00")

        merged = merge_provenance(existing, incoming)

        assert len(merged["discovery_history"]) == PROVENANCE_HISTORY_CAP
        # Oldest (July 1) was trimmed; newest (Aug 2) retained
        assert merged["discovery_history"][0]["harvested_at"] == "2026-07-02T00:00:00+00:00"
        assert merged["discovery_history"][-1]["harvested_at"] == "2026-08-02T00:00:00+00:00"
        # Count is NOT capped — total discoveries keep accumulating
        assert merged["discovery_count"] == 21
        # First-seen survives trimming (top-level counter, not history-bound)
        assert merged["first_seen_at"] == "2026-07-01T00:00:00+00:00"

    @pytest.mark.asyncio
    async def test_re_discovery_via_bulk_upsert_appends_event(self) -> None:
        """A repeated sync of the same record merges history instead of
        overwriting it — same key, same URL, later timestamp."""
        existing = {
            "source": "openneuro",
            "source_id": "ds000001",
            "url": "https://openneuro.org/datasets/ds000001",
            "doi": None,
            "provenance": _full_provenance(
                harvested_at="2026-08-01T00:00:00+00:00", method="batch_sync"
            ),
        }
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            ds = _dataset()  # provenance without history (legacy-style incoming)
            ds.provenance = _full_provenance(
                harvested_at="2026-08-02T00:00:00+00:00", method="repository_search"
            )
            await bulk_upsert([ds])

        assert (ds.source, ds.source_id) == ("openneuro", "ds000001")
        prov = ds.provenance
        assert prov["discovery_count"] == 2
        assert len(prov["discovery_history"]) == 2
        assert prov["first_seen_at"] == "2026-08-01T00:00:00+00:00"
        assert prov["last_seen_at"] == "2026-08-02T00:00:00+00:00"

    @pytest.mark.asyncio
    async def test_cross_source_merge_combines_history_and_retargets(self) -> None:
        """Web discovery of an existing OpenNeuro dataset merges into the
        canonical record: history combines, key is retargeted to OpenNeuro."""
        existing = {
            "source": "openneuro",
            "source_id": "ds000001",
            "url": "https://openneuro.org/datasets/ds000001",
            "doi": None,
            "provenance": _full_provenance(
                harvested_at="2026-08-01T00:00:00+00:00", method="batch_sync"
            ),
        }
        web = _dataset(  # web-origin candidate, same URL
            source="web_search", source_id="sha1xyz",
            url="https://openneuro.org/datasets/ds000001",
        )
        web.provenance = _full_provenance(
            source="web_search",
            harvested_at="2026-08-02T00:00:00+00:00",
            method="web_search",
            harvest_query="resting state fMRI",
        )
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            await bulk_upsert([web])

        assert (web.source, web.source_id) == ("openneuro", "ds000001")
        ops = collection.bulk_write.await_args.args[0]
        assert ops[0]._filter == {"source": "openneuro", "source_id": "ds000001"}
        prov = web.provenance
        assert prov["dedup_key"] == "openneuro:ds000001"
        assert prov["discovery_count"] == 2
        assert [e["discovery_method"] for e in prov["discovery_history"]] == [
            "batch_sync",
            "web_search",
        ]
        # History records the discovery SOURCE faithfully (web_search origin)
        assert prov["discovery_history"][1]["source"] == "web_search"
        assert prov["discovery_history"][1]["harvest_query"] == "resting state fMRI"

    @pytest.mark.asyncio
    async def test_legacy_doc_without_provenance_upgraded_gracefully(self) -> None:
        """Existing docs written before discovery_history get a derived event
        on the next write — never a crash, never a lost history."""
        existing = {
            "source": "openneuro",
            "source_id": "ds000001",
            "url": "https://openneuro.org/datasets/ds000001",
            "doi": None,
            # legacy provenance: no discovery_history, no counters
            "provenance": {
                "source_repository": "openneuro",
                "harvested_at": "2026-07-15T00:00:00+00:00",
                "dedup_key": "openneuro:ds000001",
            },
        }
        incoming = _full_provenance(harvested_at="2026-08-02T00:00:00+00:00")
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            ds = _dataset()
            ds.provenance = incoming
            await bulk_upsert([ds])

        prov = ds.provenance
        assert prov["discovery_count"] == 2
        assert len(prov["discovery_history"]) == 2
        assert prov["first_seen_at"] == "2026-07-15T00:00:00+00:00"  # legacy preserved
        assert prov["last_seen_at"] == "2026-08-02T00:00:00+00:00"


# ---------------------------------------------------------------------------
# Canonical merge full flow — repository-search persistence contract
# ---------------------------------------------------------------------------


class TestCanonicalMergeFullFlow:
    """The zenodo→dryad same-DOI scenario through the REAL persistence code:
    a repository record mirroring an existing canonical record (same DOI,
    different source/URL) is merged into the existing document — never
    inserted as a duplicate — and the re-query returns the surviving canonical
    document with its original `_id`."""

    @pytest.mark.asyncio
    async def test_zenodo_mirror_merges_into_existing_dryad_doc(self) -> None:
        existing_dryad = {
            "_id": "66f0d0ad0000000000000dry",
            "source": "dryad",
            "source_id": "dryad.4kb75",
            "url": "https://datadryad.org/stash/dataset/doi:10.5061/dryad.4kb75",
            "doi": "10.5061/dryad.4kb75",
            "title": "Data from: Low-frequency fluctuations of the resting brain...",
            "modality": [],
            "species": [],
            "keywords": [],
            "trust_tier": "verified",
            "quality_score": 0.5,
            "provenance": {
                "source_repository": "dryad",
                "harvest_query": "resting state fMRI",
                "harvested_at": "2026-07-15T00:00:00+00:00",
                "pipeline_version": "v2",
                "dedup_key": "dryad:dryad.4kb75",
                "enrichment_sources": [],
                "first_seen_at": "2026-07-15T00:00:00+00:00",
                "last_seen_at": "2026-07-15T00:00:00+00:00",
                "discovery_count": 1,
                "discovery_history": [
                    {
                        "source": "dryad",
                        "discovery_method": "batch_sync",
                        "source_api": "datadryad.org",
                        "harvest_query": "resting state fMRI",
                        "harvested_at": "2026-07-15T00:00:00+00:00",
                        "pipeline_version": "v2",
                    }
                ],
            },
            "ingested_at": "2026-07-15T00:00:00+00:00",
            "updated_at": "2026-07-15T00:00:00+00:00",
        }
        collection = _MergeCollection([existing_dryad])
        db = MagicMock()
        db.__getitem__ = MagicMock(return_value=collection)

        # The zenodo connector record: same dataset, same DOI, different URL.
        zenodo = _dataset(
            source="zenodo",
            source_id="4938058",
            url="https://zenodo.org/records/4938058",
            doi="10.5061/dryad.4kb75",
        )
        zenodo.provenance = {
            "source_repository": "zenodo",
            "harvest_query": "resting state fMRI",
            "harvested_at": "2026-08-06T00:00:00+00:00",
            "pipeline_version": "v2",
            "dedup_key": "zenodo:4938058",
            "enrichment_sources": [],
            "first_seen_at": "2026-08-06T00:00:00+00:00",
            "last_seen_at": "2026-08-06T00:00:00+00:00",
            "discovery_count": 1,
            "discovery_history": [
                {
                    "source": "zenodo",
                    "discovery_method": "repository_search",
                    "source_api": "zenodo.org",
                    "harvest_query": "resting state fMRI",
                    "harvested_at": "2026-08-06T00:00:00+00:00",
                    "pipeline_version": "v2",
                }
            ],
        }

        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            # 1. bulk_upsert merges — re-targets the candidate to the dryad identity
            written = await bulk_upsert([zenodo])
            assert written == 1
            assert (zenodo.source, zenodo.source_id) == ("dryad", "dryad.4kb75")

            # the single write op targets the surviving dryad document
            assert len(collection.writes) == 1
            assert collection.writes[0]["filter"] == {"source": "dryad", "source_id": "dryad.4kb75"}

            # 2. the dryad document survives; NO zenodo document exists
            assert len(collection.docs) == 1
            survivor = next(iter(collection.docs.values()))
            assert survivor["_id"] == "66f0d0ad0000000000000dry"
            assert survivor["source"] == "dryad"
            assert survivor["source_id"] == "dryad.4kb75"
            # refresh semantics: incoming payload wins, identity + _id survive
            assert survivor["url"] == "https://zenodo.org/records/4938058"
            assert survivor["doi"] == "10.5061/dryad.4kb75"
            assert survivor["ingested_at"] == "2026-07-15T00:00:00+00:00"  # $setOnInsert kept

            # 3. re-query by the post-merge identity returns the surviving doc
            requery = await find_datasets_by_identity([(zenodo.source, zenodo.source_id)])
            assert len(requery) == 1
            assert requery[0].id == "66f0d0ad0000000000000dry"
            assert requery[0].source == "dryad"
            assert requery[0].source_id == "dryad.4kb75"

            # provenance history merged: batch_sync + repository_search
            methods = [e["discovery_method"] for e in requery[0].provenance["discovery_history"]]
            assert methods == ["batch_sync", "repository_search"]
            assert requery[0].provenance["discovery_count"] == 2

            # 4. the object returned to Node is the surviving dryad document
            assert requery[0].id == survivor["_id"]
            assert requery[0].source_id == survivor["source_id"]

    @pytest.mark.asyncio
    async def test_no_existing_doc_inserts_new_record_with_own_oid(self) -> None:
        """Baseline: without a URL/DOI/identity match, the repo record is
        inserted as a new document and the re-query returns its own _id."""
        collection = _MergeCollection([])
        db = MagicMock()
        db.__getitem__ = MagicMock(return_value=collection)

        zenodo = _dataset(
            source="zenodo",
            source_id="4938058",
            url="https://zenodo.org/records/4938058",
            doi="10.5281/zenodo.4938058",
        )
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            written = await bulk_upsert([zenodo])
            requery = await find_datasets_by_identity([(zenodo.source, zenodo.source_id)])

        assert written == 1
        assert (zenodo.source, zenodo.source_id) == ("zenodo", "4938058")  # no merge
        assert len(collection.docs) == 1
        assert requery[0].source == "zenodo"
        assert requery[0].source_id == "4938058"
        assert requery[0].id is not None  # its own freshly inserted _id
        assert requery[0].id == next(iter(collection.docs.values()))["_id"]


# ---------------------------------------------------------------------------
# upsert_dataset — canonical merge (single-record path)
# ---------------------------------------------------------------------------


class TestUpsertDatasetCanonical:
    @pytest.mark.asyncio
    async def test_retargets_web_candidate_to_repo_record(self) -> None:
        existing = {
            "source": "openneuro",
            "source_id": "ds000001",
            "url": "https://openneuro.org/datasets/ds000001",
            "doi": None,
        }
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            web = _dataset()
            await upsert_dataset(web)

        assert (web.source, web.source_id) == ("openneuro", "ds000001")
        fau_filter = collection.find_one_and_update.await_args.args[0]
        assert fau_filter == {"source": "openneuro", "source_id": "ds000001"}
        assert web.provenance["dedup_key"] == "openneuro:ds000001"

    @pytest.mark.asyncio
    async def test_same_key_refresh_merges_history(self) -> None:
        """The single-record path (used by upsert_many) also appends discovery
        history on a same-key re-discovery instead of overwriting it."""
        existing = {
            "source": "openneuro",
            "source_id": "ds000001",
            "url": "https://openneuro.org/datasets/ds000001",
            "doi": None,
            "provenance": _full_provenance(
                harvested_at="2026-08-01T00:00:00+00:00", method="batch_sync"
            ),
        }
        db, collection = _mock_db([existing])
        with patch("app.db.repositories.dataset_repository.get_db", return_value=db):
            ds = _dataset()
            ds.provenance = _full_provenance(
                harvested_at="2026-08-02T00:00:00+00:00", method="repository_search"
            )
            await upsert_dataset(ds)

        assert (ds.source, ds.source_id) == ("openneuro", "ds000001")
        prov = ds.provenance
        assert prov["discovery_count"] == 2
        assert [e["discovery_method"] for e in prov["discovery_history"]] == [
            "batch_sync",
            "repository_search",
        ]
        assert prov["first_seen_at"] == "2026-08-01T00:00:00+00:00"
        assert prov["last_seen_at"] == "2026-08-02T00:00:00+00:00"
