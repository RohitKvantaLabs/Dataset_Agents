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
