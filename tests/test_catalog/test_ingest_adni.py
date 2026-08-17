"""ADNI ingestion tests — in-memory catalog (no live API, no DB).

Verifies the ADNI ingestion pipeline contract (Option A identity model):
  - EXACT 122-record gate (fewer/more refused before any write)
  - stable_identifier completeness gate
  - normalization + generic identity resolver integration
  - duplicate candidate protection
  - shared documentation/news URLs never collapse: products sharing the SAME
    sourceUrl stay distinct canonical records (URL is NOT an ADNI identity
    signal — identity is the deterministic repo:sourceDatasetId/sourceKey)
  - canonical DOI null; publication DOI stays relationship metadata
  - idempotency (rerun = merges via source_key, no duplicates)
  - failure isolation
  - ZERO API calls and ZERO asset/file calls
"""
import json
from unittest.mock import MagicMock

from app.catalog.ingest import run_adni_ingestion
from app.catalog.normalize import (
    ADNI_CENSUS_EXPECTED,
    build_adni_source_record,
    canonical_record_from_source,
)

from ._adni_fixtures import (
    adni_doc_record,
    adni_doc_record_same_url,
    adni_news_shared_record,
    adni_news_shared_record_b,
    adni_records,
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


# ─────────────────────────────────────────────────────────────────────────────
# EXACT 122-record gate
# ─────────────────────────────────────────────────────────────────────────────


async def test_exact_122_input_passes_gate_and_inserts_all():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_adni_ingestion(db, records=adni_records(ADNI_CENSUS_EXPECTED))

    assert stats["gate_failed"] is False
    assert stats["input"] == ADNI_CENSUS_EXPECTED
    assert stats["normalized"] == ADNI_CENSUS_EXPECTED
    assert stats["inserted"] == ADNI_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["validation_failed"] == 0
    assert len(coll.docs) == ADNI_CENSUS_EXPECTED
    assert stats["api_calls"] == 0
    assert stats["asset_calls"] == 0


async def test_input_below_122_is_refused_before_any_write():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_adni_ingestion(db, records=adni_records(ADNI_CENSUS_EXPECTED - 1))

    assert stats["gate_failed"] is True
    assert "exactly" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert stats["merged"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_input_above_122_is_refused_before_any_write():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_adni_ingestion(db, records=adni_records(ADNI_CENSUS_EXPECTED + 1))

    assert stats["gate_failed"] is True
    assert stats["inserted"] == 0
    assert stats["merged"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_records_missing_stable_identifier_refuse_the_run():
    coll = _MemCatalog()
    db = _make_db(coll)

    records = adni_records(ADNI_CENSUS_EXPECTED)
    records[0]["stable_identifier"] = None

    stats = await run_adni_ingestion(db, records=records)

    assert stats["gate_failed"] is True
    assert "stable_identifier" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


# ─────────────────────────────────────────────────────────────────────────────
# Canonical record contract
# ─────────────────────────────────────────────────────────────────────────────


async def test_each_product_is_one_canonical_record_with_null_doi():
    records = adni_records(ADNI_CENSUS_EXPECTED)
    records[0]["publication_doi"] = "10.3233/JAD-221272"
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_adni_ingestion(db, records=records)

    # every canonical record has null DOI + one source + one sourceKey
    for doc in coll.docs:
        assert doc["doi"] is None
        assert len(doc["sources"]) == 1
        assert len(doc["sourceKeys"]) == 1
        assert doc["sourceKeys"][0].startswith("adni:adni:")
    # the publication DOI stayed relationship metadata only
    seeded = next(d for d in coll.docs if "adni:adni:adni-product-1" in d["sourceKeys"])
    assert seeded["rawMetadata"]["adni"]["publication_doi"] == "10.3233/JAD-221272"
    assert seeded["doi"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Shared documentation/news URLs → distinct canonical records (no collapse)
# ─────────────────────────────────────────────────────────────────────────────


async def test_shared_doc_and_news_urls_never_collapse():
    # two doc products sharing bioflood.html + one news product; pad to 122
    records = adni_records(ADNI_CENSUS_EXPECTED)
    records[0] = adni_doc_record()
    records[1] = adni_doc_record_same_url()
    records[2] = adni_news_shared_record()
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_adni_ingestion(db, records=records)

    assert stats["gate_failed"] is False
    assert stats["inserted"] == ADNI_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert stats["ambiguous_candidates"] == 0
    assert len(coll.docs) == ADNI_CENSUS_EXPECTED
    ids = [d["sourceKeys"][0] for d in coll.docs]
    assert "adni:adni:adni-diadem---alzosure-predict-(plasma-u-p53az)" in ids
    assert "adni:adni:adni-csf-local-lab-results-(protein,-glucose,-wbc/rbc-counts)" in ids
    assert "adni:adni:adni-blennow-lab--csf-gap-43" in ids
    # all three co-located products present as DISTINCT canonical records
    assert len(set(ids)) == ADNI_CENSUS_EXPECTED
    # and their canonical ids are all distinct too
    assert len({d["canonicalDatasetId"] for d in coll.docs}) == ADNI_CENSUS_EXPECTED


async def test_products_with_exact_same_source_url_remain_distinct():
    """Two products sharing the EXACT SAME official news URL still produce two
    distinct canonical records — a shared URL never merges ADNI products."""
    records = adni_records(ADNI_CENSUS_EXPECTED)
    records[0] = adni_news_shared_record()
    records[1] = adni_news_shared_record_b()
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_adni_ingestion(db, records=records)

    assert stats["gate_failed"] is False
    assert stats["inserted"] == ADNI_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert len(coll.docs) == ADNI_CENSUS_EXPECTED
    a = next(
        d for d in coll.docs
        if "adni:adni:adni-blennow-lab--csf-gap-43" in d["sourceKeys"]
    )
    b = next(
        d for d in coll.docs
        if "adni:adni:adni-janssen-plasma-p217-+-tau-simoa-assay-[adni2,3]" in d["sourceKeys"]
    )
    # same shared URL preserved verbatim on both...
    assert a["sources"][0]["sourceUrl"] == b["sources"][0]["sourceUrl"]
    # ...but different sourceDatasetIds, sourceKeys, canonical ids
    assert a["sourceKeys"] != b["sourceKeys"]
    assert a["canonicalDatasetId"] != b["canonicalDatasetId"]
    # and no ADNI URL identity signal anywhere
    for d in coll.docs:
        assert d["provenance"]["identity"]["sourceUrlNorm"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Duplicate candidate protection
# ─────────────────────────────────────────────────────────────────────────────


async def test_duplicate_stable_identity_detected_and_never_duplicated():
    records = adni_records(ADNI_CENSUS_EXPECTED)
    records[0] = adni_records(1)[0]
    records[1] = adni_records(1)[0]  # same stable_identifier twice
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_adni_ingestion(db, records=records)

    assert stats["duplicate_source_identities"] != []  # detected up front
    assert "adni:adni:adni-product-1" in stats["duplicate_source_identities"][0]["identity"]
    # second occurrence resolves to a merge of the first — ONE canonical record
    assert stats["inserted"] + stats["merged"] == ADNI_CENSUS_EXPECTED
    assert len(coll.docs) == ADNI_CENSUS_EXPECTED - 1


# ─────────────────────────────────────────────────────────────────────────────
# Generic identity resolver integration
# ─────────────────────────────────────────────────────────────────────────────


async def test_rerun_is_idempotent_no_duplicates():
    records = adni_records(ADNI_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    first = await run_adni_ingestion(db, records=records)
    assert first["inserted"] == ADNI_CENSUS_EXPECTED

    second = await run_adni_ingestion(db, records=records)
    assert second["inserted"] == 0
    assert second["merged"] == ADNI_CENSUS_EXPECTED
    # every rerun match is via the deterministic source_key — the URL layer is
    # skipped for ADNI (shared official URLs are never an identity signal)
    assert set(second["matched_via"].keys()) == {"source_key"}
    assert len(coll.docs) == ADNI_CENSUS_EXPECTED  # no duplicates


async def test_same_title_cross_repo_record_is_ambiguous_not_merged():
    """A non-ADNI canonical record with an identical title must NOT be merged —
    the generic resolver reports it ambiguous and inserts the ADNI product."""
    records = adni_records(ADNI_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    # pre-seed a canonical record from ANOTHER repository with the same title
    seeded = canonical_record_from_source(
        build_adni_source_record(records[0])
    )
    # replace the identity-bearing fields with a true foreign (OpenNeuro) source
    seeded["sourceKeys"] = ["openneuro:datasets/ds000001"]
    seeded["sources"] = [
        {
            "repository": "openneuro",
            "sourceDatasetId": "datasets/ds000001",
            "sourceUrl": "https://openneuro.org/datasets/ds000001",
            "title": "ADNI product 1",
            "modality": [],
            "participantCount": None,
        }
    ]
    seeded["provenance"]["identity"]["sourceUrlNorm"] = "http://openneuro.org/datasets/ds000001"
    seeded["_id"] = "seeded"
    coll.docs.append(seeded)

    stats = await run_adni_ingestion(db, records=records)

    # title matches but no strong cross-repo signal → never force-merged
    assert stats["inserted"] == ADNI_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert len(coll.docs) == ADNI_CENSUS_EXPECTED + 1  # seeded + all ADNI products
    # the ADNI product got its own canonical record (identity via source_key,
    # never the title/URL)
    adni_docs = [d for d in coll.docs if d["sourceKeys"][0].startswith("adni:adni:")]
    assert len(adni_docs) == ADNI_CENSUS_EXPECTED


# ─────────────────────────────────────────────────────────────────────────────
# Failure handling — malformed input is refused, never partially written
# ─────────────────────────────────────────────────────────────────────────────


async def test_malformed_record_refuses_the_whole_run():
    records = adni_records(ADNI_CENSUS_EXPECTED)
    records[0] = "not-a-dict"  # corrupted record
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_adni_ingestion(db, records=records)

    assert stats["gate_failed"] is True
    assert "stable_identifier" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_builder_tolerates_absent_optional_fields():
    """A record missing optional fields still normalizes (never fabricated)."""
    record = adni_records(1)[0]
    del record["version_update_info"]
    del record["first_announced"]
    source = build_adni_source_record(record)
    assert source["sourceDatasetId"] == "adni:adni-product-1"
    assert source["snapshot"]["versionUpdateInfo"] is None
    assert source["snapshot"]["firstAnnounced"] is None
    assert source["title"] == "ADNI product 1"
