"""UK Biobank ingestion tests — in-memory catalog (no live API, no DB).

Verifies the UK Biobank ingestion pipeline contract:
  - EXACT 21-record gate (fewer/more refused before any write)
  - stable_identifier completeness gate
  - normalization + generic identity resolver integration
  - duplicate candidate protection
  - shared Showcase URLs never collapse: products sharing the SAME sourceUrl
    stay distinct canonical records (URL is NOT a UK Biobank identity signal —
    identity is the deterministic repo:sourceDatasetId/sourceKey)
  - canonical DOI null; publication DOI stays relationship metadata
  - child Data-Fields remain metadata on the parent product (never datasets)
  - IDP families remain grouped (field_count is metadata, not dataset count)
  - releases never become separate datasets
  - returned datasets remain excluded (none are ever part of the input)
  - idempotency (rerun = merges via source_key, no duplicates)
  - failure isolation
  - ZERO API calls and ZERO asset/file calls
"""
from unittest.mock import MagicMock

from app.catalog.ingest import run_ukbiobank_ingestion
from app.catalog.normalize import (
    UKB_CENSUS_EXPECTED,
    build_ukbiobank_source_record,
    canonical_record_from_source,
)

from ._ukbiobank_fixtures import (
    ukb_records,
    ukb_shared_url_record,
    ukb_shared_url_record_b,
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
# EXACT 21-record gate
# ─────────────────────────────────────────────────────────────────────────────


async def test_exact_21_input_passes_gate_and_inserts_all():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ukbiobank_ingestion(db, records=ukb_records(UKB_CENSUS_EXPECTED))

    assert stats["gate_failed"] is False
    assert stats["input"] == UKB_CENSUS_EXPECTED
    assert stats["normalized"] == UKB_CENSUS_EXPECTED
    assert stats["inserted"] == UKB_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["validation_failed"] == 0
    assert len(coll.docs) == UKB_CENSUS_EXPECTED
    assert stats["api_calls"] == 0
    assert stats["asset_calls"] == 0


async def test_input_below_21_is_refused_before_any_write():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ukbiobank_ingestion(db, records=ukb_records(UKB_CENSUS_EXPECTED - 1))

    assert stats["gate_failed"] is True
    assert "exactly" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert stats["merged"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_input_above_21_is_refused_before_any_write():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ukbiobank_ingestion(db, records=ukb_records(UKB_CENSUS_EXPECTED + 1))

    assert stats["gate_failed"] is True
    assert stats["inserted"] == 0
    assert stats["merged"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_records_missing_stable_identifier_refuse_the_run():
    coll = _MemCatalog()
    db = _make_db(coll)

    records = ukb_records(UKB_CENSUS_EXPECTED)
    records[0]["stable_identifier"] = None

    stats = await run_ukbiobank_ingestion(db, records=records)

    assert stats["gate_failed"] is True
    assert "stable_identifier" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_malformed_record_refuses_the_whole_run():
    records = ukb_records(UKB_CENSUS_EXPECTED)
    records[0] = "not-a-dict"  # corrupted record
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ukbiobank_ingestion(db, records=records)

    assert stats["gate_failed"] is True
    assert "stable_identifier" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


# ─────────────────────────────────────────────────────────────────────────────
# Canonical record contract
# ─────────────────────────────────────────────────────────────────────────────


async def test_each_product_is_one_canonical_record_with_null_doi():
    records = ukb_records(UKB_CENSUS_EXPECTED)
    records[0]["publication_doi"] = "10.1101/2025.01.01.25320000"
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_ukbiobank_ingestion(db, records=records)

    for doc in coll.docs:
        assert doc["doi"] is None
        assert len(doc["sources"]) == 1
        assert len(doc["sourceKeys"]) == 1
        assert doc["sourceKeys"][0].startswith("ukbiobank:ukbiobank:")
    # the publication DOI stayed relationship metadata only
    seeded = next(
        d for d in coll.docs if "ukbiobank:ukbiobank:ukbiobank-product-1" in d["sourceKeys"]
    )
    assert seeded["rawMetadata"]["ukbiobank"]["publication_doi"] == "10.1101/2025.01.01.25320000"
    assert seeded["doi"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Child Data-Fields / IDP families / releases — metadata, never datasets
# ─────────────────────────────────────────────────────────────────────────────


async def test_child_fields_and_releases_stay_metadata_not_datasets():
    records = ukb_records(UKB_CENSUS_EXPECTED)
    # product 1: a big IDP family (1,445 child fields) + release history
    records[0]["field_count"] = 1445
    records[0]["child_data_field_ids"] = [f"2500{i}" for i in range(1445)]
    records[0]["version_release_information"] = (
        "Field debut 2015-10-09; Mar 2025 tranche +15,000 participants "
        "(release tranches are NOT separate datasets)."
    )
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ukbiobank_ingestion(db, records=records)

    assert stats["inserted"] == UKB_CENSUS_EXPECTED
    assert len(coll.docs) == UKB_CENSUS_EXPECTED  # 21 products — NOT 1,445+ datasets

    big = next(
        d for d in coll.docs if "ukbiobank:ukbiobank:ukbiobank-product-1" in d["sourceKeys"]
    )
    snap = big["sources"][0]["snapshot"]
    # child Data-Field IDs + field counts live on the parent product
    assert snap["fieldCount"] == 1445
    assert len(snap["childDataFieldIds"]) == 1445
    assert "release tranches are NOT separate datasets" in snap["versionReleaseInformation"]
    # exactly ONE canonical record per product — never per-field / per-release
    assert len(big["sources"]) == 1
    assert len(big["sourceKeys"]) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Shared Showcase URLs → distinct canonical records (no collapse)
# ─────────────────────────────────────────────────────────────────────────────


async def test_products_with_exact_same_source_url_remain_distinct():
    """Two products sharing the EXACT SAME official Showcase/docs URL still
    produce two distinct canonical records — a shared URL never merges UK
    Biobank products."""
    records = ukb_records(UKB_CENSUS_EXPECTED)
    records[0] = ukb_shared_url_record()
    records[1] = ukb_shared_url_record_b()
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ukbiobank_ingestion(db, records=records)

    assert stats["gate_failed"] is False
    assert stats["inserted"] == UKB_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert len(coll.docs) == UKB_CENSUS_EXPECTED
    a = next(
        d for d in coll.docs
        if "ukbiobank:ukbiobank:ukbiobank-nervous-system-disorders" in d["sourceKeys"]
    )
    b = next(
        d for d in coll.docs
        if "ukbiobank:ukbiobank:ukbiobank-sleep" in d["sourceKeys"]
    )
    # same shared URL preserved verbatim on both...
    assert a["sources"][0]["sourceUrl"] == b["sources"][0]["sourceUrl"]
    # ...but different sourceDatasetIds, sourceKeys, canonical ids
    assert a["sourceKeys"] != b["sourceKeys"]
    assert a["canonicalDatasetId"] != b["canonicalDatasetId"]
    # and no UK Biobank URL identity signal anywhere
    for d in coll.docs:
        assert d["provenance"]["identity"]["sourceUrlNorm"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Duplicate candidate protection
# ─────────────────────────────────────────────────────────────────────────────


async def test_duplicate_stable_identity_detected_and_never_duplicated():
    records = ukb_records(UKB_CENSUS_EXPECTED)
    records[0] = ukb_records(1)[0]
    records[1] = ukb_records(1)[0]  # same stable_identifier twice
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ukbiobank_ingestion(db, records=records)

    assert stats["duplicate_source_identities"] != []  # detected up front
    assert "ukbiobank:ukbiobank:ukbiobank-product-1" in (
        stats["duplicate_source_identities"][0]["identity"]
    )
    # second occurrence resolves to a merge of the first — ONE canonical record
    assert stats["inserted"] + stats["merged"] == UKB_CENSUS_EXPECTED
    assert len(coll.docs) == UKB_CENSUS_EXPECTED - 1


# ─────────────────────────────────────────────────────────────────────────────
# Generic identity resolver integration
# ─────────────────────────────────────────────────────────────────────────────


async def test_rerun_is_idempotent_no_duplicates():
    records = ukb_records(UKB_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    first = await run_ukbiobank_ingestion(db, records=records)
    assert first["inserted"] == UKB_CENSUS_EXPECTED

    second = await run_ukbiobank_ingestion(db, records=records)
    assert second["inserted"] == 0
    assert second["merged"] == UKB_CENSUS_EXPECTED
    # every rerun match is via the deterministic source_key — the URL layer is
    # skipped for UK Biobank (shared Showcase URLs are never an identity signal)
    assert set(second["matched_via"].keys()) == {"source_key"}
    assert len(coll.docs) == UKB_CENSUS_EXPECTED  # no duplicates


async def test_same_title_cross_repo_record_is_ambiguous_not_merged():
    """A non-UKB canonical record with an identical title must NOT be merged —
    the generic resolver reports it ambiguous and inserts the UK Biobank
    product (no fuzzy matching, no force-merge)."""
    records = ukb_records(UKB_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    # pre-seed a canonical record from ANOTHER repository with the same title
    seeded = canonical_record_from_source(
        build_ukbiobank_source_record(records[0])
    )
    seeded["sourceKeys"] = ["openneuro:datasets/ds000001"]
    seeded["sources"] = [
        {
            "repository": "openneuro",
            "sourceDatasetId": "datasets/ds000001",
            "sourceUrl": "https://openneuro.org/datasets/ds000001",
            "title": "UK Biobank product 1",
            "modality": [],
            "participantCount": None,
        }
    ]
    seeded["provenance"]["identity"]["sourceUrlNorm"] = "http://openneuro.org/datasets/ds000001"
    # the foreign record only shares a TITLE — no participant count, no
    # modality, no other strong signal (title alone is never enough to merge)
    seeded["participantCount"] = None
    seeded["modality"] = []
    seeded["_id"] = "seeded"
    coll.docs.append(seeded)

    stats = await run_ukbiobank_ingestion(db, records=records)

    # title matches but no strong cross-repo signal → never force-merged
    assert stats["inserted"] == UKB_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert len(coll.docs) == UKB_CENSUS_EXPECTED + 1  # seeded + all UKB products
    # the UK Biobank product got its own canonical record (identity via
    # source_key, never the title/URL)
    ukb_docs = [d for d in coll.docs if d["sourceKeys"][0].startswith("ukbiobank:ukbiobank:")]
    assert len(ukb_docs) == UKB_CENSUS_EXPECTED


# ─────────────────────────────────────────────────────────────────────────────
# Returned datasets stay excluded / no access / no downloads
# ─────────────────────────────────────────────────────────────────────────────


async def test_returned_datasets_never_ingested():
    """The Returns catalogue (158 neuroscience-related returned datasets) is
    not part of the approved input and no returned-dataset identity ever
    appears in the catalog."""
    records = ukb_records(UKB_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_ukbiobank_ingestion(db, records=records)

    # every canonical record is one of the 21 approved products
    assert len(coll.docs) == UKB_CENSUS_EXPECTED
    for d in coll.docs:
        key = d["sourceKeys"][0]
        assert key.startswith("ukbiobank:ukbiobank:ukbiobank-")
        assert "return" not in key  # no returned-dataset records


async def test_no_api_calls_no_asset_downloads_no_participant_access():
    records = ukb_records(UKB_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ukbiobank_ingestion(db, records=records)

    assert stats["api_calls"] == 0     # census artifact is authoritative
    assert stats["asset_calls"] == 0   # no bulk/image/file downloads
    # no per-participant, per-scan or per-file records were written
    for d in coll.docs:
        assert d.get("datasetSizeBytes") is None
        assert d.get("subjectIds") is None
        assert d["sources"][0].get("subjectIds") is None


# ─────────────────────────────────────────────────────────────────────────────
# Failure handling — builder tolerates absent optional fields
# ─────────────────────────────────────────────────────────────────────────────


async def test_builder_tolerates_absent_optional_fields():
    """A record missing optional fields still normalizes (never fabricated)."""
    record = ukb_records(1)[0]
    del record["version_release_information"]
    del record["dataset_unit_rationale"]
    source = build_ukbiobank_source_record(record)
    assert source["sourceDatasetId"] == "ukbiobank:ukbiobank-product-1"
    assert source["snapshot"]["versionReleaseInformation"] is None
    assert source["snapshot"]["datasetUnitRationale"] is None
    assert source["title"] == "UK Biobank product 1"
