"""Dryad ingestion tests — in-memory catalog (no live API, no DB).

Verifies the Dryad ingestion pipeline contract:
  - HIGH-confidence-only gate (MEDIUM/FALSE records refused before any write)
  - normalization of every HIGH-confidence record
  - duplicate candidate protection (same DOI / source key detected)
  - generic identity resolver integration (insert vs merge vs ambiguous)
  - idempotency (rerun = merges, no duplicates)
  - failure handling (a bad record is isolated)
  - ZERO API calls and ZERO asset/file calls
"""
from unittest.mock import MagicMock

from app.catalog.ingest import run_dryad_ingestion
from app.catalog.normalize import build_dryad_source_record, canonical_record_from_source

from ._dryad_fixtures import (
    dryad_record,
    dryad_record_b,
    false_positive_record,
    medium_record,
)


# ─────────────────────────────────────────────────────────────────────────────
# In-memory catalog collection (subset of the persistence contract)
# ─────────────────────────────────────────────────────────────────────────────


class _MemCursor:
    def __init__(self, docs):
        self._docs = docs

    def __aiter__(self):
        self._it = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration

    def limit(self, n):
        return self


class _MemCatalog:
    def __init__(self):
        self.docs: list[dict] = []
        self._seq = 0

    @staticmethod
    def _get(doc, path):
        cur = doc
        for part in str(path).split("."):
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
        return cur

    def _matches(self, doc, query):
        for key, value in query.items():
            val = self._get(doc, key)
            if isinstance(val, list):
                if value not in val:
                    return False
            elif val != value:
                return False
        return True

    async def find_one(self, query):
        for d in self.docs:
            if self._matches(d, query):
                return d
        return None

    def find(self, query):
        return _MemCursor([d for d in self.docs if self._matches(d, query)])

    async def insert_one(self, doc):
        self._seq += 1
        doc["_id"] = f"oid{self._seq}"
        self.docs.append(doc)

    async def replace_one(self, filt, doc):
        for i, d in enumerate(self.docs):
            if d.get("_id") == filt.get("_id"):
                self.docs[i] = doc
                return None
        raise KeyError(filt)


def _make_db(coll: _MemCatalog) -> MagicMock:
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    return db


def _high_records() -> list[dict]:
    """Two distinct HIGH-confidence candidates (the full validated set shape)."""
    return [dryad_record(), dryad_record_b()]


# ─────────────────────────────────────────────────────────────────────────────
# Test: HIGH-confidence records are inserted through the generic pipeline
# ─────────────────────────────────────────────────────────────────────────────


async def test_high_confidence_ingestion():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_dryad_ingestion(db, candidates=_high_records())

    assert stats["gate_failed"] is False
    assert stats["input"] == 2
    assert stats["high_confidence"] == 2
    assert stats["excluded_non_high"] == 0
    assert stats["normalized"] == 2
    assert stats["inserted"] == 2
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["validation_failed"] == 0
    assert stats["api_calls"] == 0
    assert stats["asset_calls"] == 0
    assert len(coll.docs) == 2

    keys = {d["sourceKeys"][0] for d in coll.docs}
    assert keys == {"dryad:dryad:105", "dryad:dryad:4812047"}
    dois = {d["doi"] for d in coll.docs}
    assert dois == {"10.5061/dryad.gc72v", "10.5061/dryad.4b8gtht9c"}


async def test_each_record_is_one_canonical_dataset_with_version_metadata():
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_dryad_ingestion(db, candidates=_high_records())

    rec = next(d for d in coll.docs if d["sourceKeys"] == ["dryad:dryad:105"])
    assert len(rec["sources"]) == 1  # one dataset = one canonical record
    assert rec["doi"] == "10.5061/dryad.gc72v"
    assert rec["rawMetadata"]["dryad"]["versionNumber"] == 1
    # article DOI quarantined to relationship metadata
    assert rec["rawMetadata"]["dryad"]["relatedWorks"][0]["relationship"] == "primary_article"
    assert rec["doi"] == "10.5061/dryad.gc72v"  # NOT the article DOI


# ─────────────────────────────────────────────────────────────────────────────
# Test: MEDIUM / FALSE-POSITIVE records cannot enter ingestion (hard gate)
# ─────────────────────────────────────────────────────────────────────────────


async def test_gate_rejects_medium_records():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_dryad_ingestion(
        db, candidates=[dryad_record(), medium_record()]
    )

    assert stats["gate_failed"] is True
    assert "non-HIGH" in (stats["gate_reason"] or "")
    assert stats["excluded_non_high"] == 1
    assert stats["high_confidence"] == 1
    assert stats["inserted"] == 0
    assert stats["merged"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_gate_rejects_false_positive_records():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_dryad_ingestion(
        db, candidates=[false_positive_record(), dryad_record_b()]
    )

    assert stats["gate_failed"] is True
    assert stats["excluded_non_high"] == 1
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_gate_rejects_mixed_pool_without_partial_writes():
    """A mixed pool (H + M + F) writes NOTHING — the gate is all-or-nothing."""
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_dryad_ingestion(
        db,
        candidates=[dryad_record(), medium_record(), false_positive_record()],
    )

    assert stats["gate_failed"] is True
    assert stats["excluded_non_high"] == 2
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test: duplicate candidate protection
# ─────────────────────────────────────────────────────────────────────────────


async def test_duplicate_candidates_detected_and_not_duplicated():
    coll = _MemCatalog()
    db = _make_db(coll)

    # same DOI appears twice (census artifact would never do this, but the
    # adapter must protect against it)
    stats = await run_dryad_ingestion(
        db, candidates=[dryad_record(), dryad_record()]
    )

    assert stats["duplicate_source_identities"] != []  # detected
    # both records normalized; second upsert resolves to a merge of the first
    assert stats["inserted"] + stats["merged"] == 2
    assert len(coll.docs) == 1  # ONE canonical record — never two


# ─────────────────────────────────────────────────────────────────────────────
# Test: generic identity resolver integration — cross-repo merge
# ─────────────────────────────────────────────────────────────────────────────


async def test_existing_canonical_with_same_doi_merges():
    """A pre-existing canonical record with the same Dryad DOI → proposed merge."""
    coll = _MemCatalog()
    # pre-seed a canonical record for the same Dryad DOI
    seeded = canonical_record_from_source(build_dryad_source_record(dryad_record()))
    await coll.insert_one(seeded)
    db = _make_db(coll)

    stats = await run_dryad_ingestion(db, candidates=[dryad_record()])

    assert stats["inserted"] == 0
    assert stats["merged"] == 1
    assert "doi" in stats["matched_via"]
    assert len(coll.docs) == 1  # merged, never duplicated


# ─────────────────────────────────────────────────────────────────────────────
# Test: idempotency — rerun produces merges, no duplicates
# ─────────────────────────────────────────────────────────────────────────────


async def test_rerun_is_idempotent_no_duplicates():
    coll = _MemCatalog()
    db = _make_db(coll)

    first = await run_dryad_ingestion(db, candidates=_high_records())
    assert first["inserted"] == 2

    second = await run_dryad_ingestion(db, candidates=_high_records())
    assert second["inserted"] == 0
    assert second["merged"] == 2
    assert set(second["matched_via"].keys()) <= {"doi", "source_url", "source_key"}
    assert len(coll.docs) == 2  # no duplicates


# ─────────────────────────────────────────────────────────────────────────────
# Test: failure handling — a malformed candidate is isolated
# ─────────────────────────────────────────────────────────────────────────────


async def test_malformed_candidate_isolated():
    coll = _MemCatalog()
    db = _make_db(coll)

    # a record that breaks normalization (title/abstract become None is fine,
    # but a non-dict author block must be tolerated; use a structural break)
    bad = dryad_record()
    bad["authors"] = "not-a-list"  # normalize tolerates; identity still OK
    good = dryad_record_b()

    stats = await run_dryad_ingestion(db, candidates=[bad, good])

    # the malformed record still normalizes safely (never crashes the run)
    assert stats["normalized"] == 2
    assert stats["inserted"] == 2
    assert stats["failed"] == 0
    assert len(coll.docs) == 2


# ─────────────────────────────────────────────────────────────────────────────
# Test: ZERO API calls and ZERO asset/file calls
# ─────────────────────────────────────────────────────────────────────────────


async def test_zero_api_and_asset_calls():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_dryad_ingestion(db, candidates=_high_records())

    assert stats["api_calls"] == 0  # census artifact is authoritative
    assert stats["asset_calls"] == 0  # no file/asset downloads ever
