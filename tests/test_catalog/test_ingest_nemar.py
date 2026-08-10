"""NEMAR ingestion tests — mocked HTTP + in-memory catalog.

Verifies the full pipeline contract WITHOUT hitting the real NEMAR API:
list (offset/limit) → detail → normalize → validate → dedup → persist.

Covers the required cases:
  CASE A — OpenNeuro mirror on004504 merges into the existing OpenNeuro
           canonical record (ds004504) — NO new canonical dataset.
  CASE B — NEMAR-native nm000103 inserts a new canonical dataset.
  CASE C — mirror on007221 whose OpenNeuro source (ds007221) is not in the
           catalog inserts as a new candidate (no fuzzy matching).
  CASE D — version drift: source_id stays authoritative, no duplicate.
  CASE E — NEMAR DOI 10.82901/nemar.on004504 never replaces the OpenNeuro
           canonical DOI.
"""
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

from app.catalog.ingest import run_nemar_ingestion
from app.catalog.normalize import canonical_record_from_source

from ._nemar_fixtures import list_record, mirror_detail, missing_mirror_detail, native_detail


# ─────────────────────────────────────────────────────────────────────────────
# Fake transport
# ─────────────────────────────────────────────────────────────────────────────


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class _FakeClient:
    """Drop-in httpx.AsyncClient serving NEMAR list pages + dataset details."""

    def __init__(self, pages: list[dict], details: dict):
        self.pages = pages
        self.details = details
        self.requested_urls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, url: str) -> _FakeResponse:
        self.requested_urls.append(url)
        if "/datasets?" in url:
            offset = int(parse_qs(urlparse(url).query).get("offset", ["0"])[0])
            page = next((p for p in self.pages if p.get("offset") == offset), self.pages[0])
            return _FakeResponse(page)
        ds_id = url.split("/datasets/")[1]
        return _FakeResponse(self.details.get(ds_id, {}))


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
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _openneuro_source(ds_id: str, doi: str, title: str) -> dict:
    """Minimal OpenNeuro source for seeding canonical records (CASE A/D)."""
    return {
        "repository": "openneuro",
        "sourceDatasetId": ds_id,
        "sourceUrl": f"https://openneuro.org/datasets/{ds_id}",
        "doi": doi,
        "title": title,
        "description": None,
        "readme": None,
        "license": "CC0",
        "licenseNormalized": "cc0",
        "datasetType": "raw",
        "availability": "open",
        "authors": ["Cassani, Raymundo", "Falk, Tiago H."],
        "modality": ["eeg"],
        "modalityRaw": ["eeg"],
        "participantCount": 88,
        "datasetSizeBytes": 12345,
        "derived": {"publicationYear": 2022, "availability": "open"},
        "provenance": {"source": "openneuro", "retrievedAt": None},
    }


async def _seed_openneuro(coll: _MemCatalog, ds_id: str, doi: str, title: str) -> dict:
    doc = canonical_record_from_source(_openneuro_source(ds_id, doi, title))
    await coll.insert_one(doc)
    return doc


def _single_page(records: list[dict]) -> list[dict]:
    return [
        {
            "count": len(records),
            "total_count": len(records),
            "limit": 200,
            "offset": 0,
            "datasets": records,
        }
    ]


# ─────────────────────────────────────────────────────────────────────────────
# CASE A — mirror merges into existing OpenNeuro canonical record
# ─────────────────────────────────────────────────────────────────────────────


async def test_case_a_mirror_merges_into_existing_openneuro():
    coll = _MemCatalog()
    await _seed_openneuro(
        coll,
        "ds004504",
        "10.18112/openneuro.ds004504.v1.0.9",
        mirror_detail()["name"],
    )
    client = _FakeClient(
        _single_page([list_record("on004504", source="openneuro", source_id="ds004504")]),
        {"on004504": {"dataset": mirror_detail()}},
    )
    db = _make_db(coll)

    stats = await run_nemar_ingestion(db, target=1, client=client, page_delay=0)

    assert stats["retrieved"] == 1
    assert stats["inserted"] == 0
    assert stats["merged"] == 1
    assert stats["matched_via"].get("cross_reference") == 1

    # ONE canonical record, BOTH sources preserved
    assert len(coll.docs) == 1
    doc = coll.docs[0]
    repos = [(s["repository"], s["sourceDatasetId"]) for s in doc["sources"]]
    assert repos == [
        ("openneuro", "ds004504"),
        ("nemar", "on004504"),
    ]
    assert "openneuro:ds004504" in doc["sourceKeys"]
    assert "nemar:on004504" in doc["sourceKeys"]
    assert set(doc["rawMetadata"].keys()) == {"openneuro", "nemar"}


async def test_case_e_nemar_doi_never_replaces_openneuro_doi():
    coll = _MemCatalog()
    await _seed_openneuro(
        coll,
        "ds004504",
        "10.18112/openneuro.ds004504.v1.0.9",
        mirror_detail()["name"],
    )
    client = _FakeClient(
        _single_page([list_record("on004504", source="openneuro", source_id="ds004504")]),
        {"on004504": {"dataset": mirror_detail()}},   # payload carries 10.82901/nemar.on004504
    )
    db = _make_db(coll)

    stats = await run_nemar_ingestion(db, target=1, client=client, page_delay=0)
    assert stats["merged"] == 1

    doc = coll.docs[0]
    # NEMAR concept DOI must NOT be the canonical DOI
    assert doc["doi"] == "10.18112/openneuro.ds004504.v1.0.9"
    assert "10.82901/nemar.on004504" not in (doc["doi"] or "")
    # NEMAR DOI survives only as NEMAR-side provenance in rawMetadata
    assert doc["rawMetadata"]["nemar"]["concept_doi"] == "10.82901/nemar.on004504"


# ─────────────────────────────────────────────────────────────────────────────
# CASE B — NEMAR-native inserts a new canonical dataset
# ─────────────────────────────────────────────────────────────────────────────


async def test_case_b_native_inserts_new_canonical():
    coll = _MemCatalog()
    client = _FakeClient(
        _single_page([list_record("nm000103")]),
        {"nm000103": {"dataset": native_detail()}},
    )
    db = _make_db(coll)

    stats = await run_nemar_ingestion(db, target=1, client=client, page_delay=0)

    assert stats["inserted"] == 1
    assert stats["merged"] == 0
    assert len(coll.docs) == 1
    doc = coll.docs[0]
    assert doc["canonicalDatasetId"].startswith("ns-")
    assert doc["doi"] == "10.82901/nemar.nm000103"        # concept DOI identity
    assert doc["sourceKeys"] == ["nemar:nm000103"]
    assert doc["sources"][0]["repository"] == "nemar"
    assert doc["ages"] is None and doc["ageGroup"] == []  # age range not fabricated


# ─────────────────────────────────────────────────────────────────────────────
# CASE C — mirror whose OpenNeuro source is missing → new candidate, no fuzzy match
# ─────────────────────────────────────────────────────────────────────────────


async def test_case_c_missing_openneuro_snapshot_inserts_new_candidate():
    coll = _MemCatalog()
    client = _FakeClient(
        _single_page([list_record("on007221", source="openneuro", source_id="ds007221")]),
        {"on007221": {"dataset": missing_mirror_detail()}},
    )
    db = _make_db(coll)

    stats = await run_nemar_ingestion(db, target=1, client=client, page_delay=0)

    assert stats["inserted"] == 1
    assert stats["merged"] == 0
    assert len(coll.docs) == 1
    doc = coll.docs[0]
    assert doc["sourceKeys"] == ["nemar:on007221"]
    assert doc["doi"] is None                            # NEMAR DOI never used
    assert doc["sources"][0]["sourceDatasetId"] == "on007221"
    assert doc["sources"][0]["snapshot"]["openNeuroSourceId"] == "ds007221"


# ─────────────────────────────────────────────────────────────────────────────
# CASE D — version drift: source_id stays authoritative, no duplicate
# ─────────────────────────────────────────────────────────────────────────────


async def test_case_d_version_drift_merges_via_source_id():
    coll = _MemCatalog()
    await _seed_openneuro(
        coll,
        "ds004504",
        "10.18112/openneuro.ds004504.v1.0.9",   # catalog has v1.0.9
        mirror_detail()["name"],
    )
    # NEMAR mirror references an OLDER OpenNeuro version (v1.0.8)
    drifted = mirror_detail()
    drifted["enrichment_json"] = (
        '{"version":"2.0","pipeline_stage":"validated","license":"CC0","dataset_type":"raw",'
        '"modalities":["eeg"],"related_identifiers":['
        '{"identifier":"10.18112/openneuro.ds004504.v1.0.8","identifier_type":"DOI","relation_type":"IsDerivedFrom"}'
        '],"resource_type_specific":"EEG Dataset"}'
    )
    client = _FakeClient(
        _single_page([list_record("on004504", source="openneuro", source_id="ds004504")]),
        {"on004504": {"dataset": drifted}},
    )
    db = _make_db(coll)

    stats = await run_nemar_ingestion(db, target=1, client=client, page_delay=0)

    assert stats["merged"] == 1                       # source_id cross-reference wins
    assert stats["inserted"] == 0
    assert len(coll.docs) == 1                        # NO duplicate canonical dataset
    doc = coll.docs[0]
    assert doc["doi"] == "10.18112/openneuro.ds004504.v1.0.9"   # canonical DOI untouched
    assert ("nemar", "on004504") in [(s["repository"], s["sourceDatasetId"]) for s in doc["sources"]]


# ─────────────────────────────────────────────────────────────────────────────
# Full small-sample contract + idempotency + failure handling
# ─────────────────────────────────────────────────────────────────────────────


def _mixed_run(limit: int = 10):
    """Mirrors + natives; seeds the 4 existing OpenNeuro sources so the smoke
    expectation (merge existing, insert missing/native) is exercised."""
    mirrors = {
        "on004504": mirror_detail(),
        "on004584": mirror_detail(dataset_id="on004584", source_id="ds004584",
                                  name="Rest eyes open", latest_version="v1.0.0"),
        "on005274": mirror_detail(dataset_id="on005274", source_id="ds005274",
                                  name="UV_EEG", latest_version="v1.0.0"),
        "on007763": mirror_detail(dataset_id="on007763", source_id="ds007763",
                                  name="BCCWJ-MEG", modalities="anat,meg", latest_version="v1.0.0"),
        "on007221": missing_mirror_detail(),
    }
    natives = {
        "nm000103": native_detail(),
        "nm000114": native_detail(dataset_id="nm000114", name="MDD Patients and Healthy Controls EEG Data",
                                  concept_doi="10.82901/nemar.nm000114",
                                  latest_version_doi="10.82901/nemar.nm000114.v1.0.0",
                                  license="CC-BY-4.0", participants=64),
        "nm000108": native_detail(dataset_id="nm000108", name="hyser_bids",
                                  concept_doi="10.82901/nemar.nm000108",
                                  latest_version_doi="10.82901/nemar.nm000108.v1.0.1",
                                  latest_version="v1.0.1"),
        "nm000276": native_detail(dataset_id="nm000276", name="iEEG dataset", modalities="ieeg",
                                  concept_doi="10.82901/nemar.nm000276",
                                  latest_version_doi="10.82901/nemar.nm000276.v1.0.0"),
        "nm000232": native_detail(dataset_id="nm000232", name="EEG dataset v2",
                                  concept_doi="10.82901/nemar.nm000232",
                                  latest_version_doi="10.82901/nemar.nm000232.v1.1.0",
                                  latest_version="v1.1.0"),
    }
    ids = ["on004504", "on004584", "on005274", "on007763", "on007221",
           "nm000103", "nm000114", "nm000108", "nm000276", "nm000232"][:limit]
    pages = _single_page([list_record(i, source="openneuro", source_id=f"ds{i[2:]}") if i.startswith("on") else list_record(i) for i in ids])
    details = {}
    for i in ids:
        details[i] = {"dataset": mirrors.get(i) or natives.get(i)}
    return ids, pages, details, mirrors


async def test_small_sample_ingestion_contract():
    limit = 10
    ids, pages, details, _ = _mixed_run(limit)
    coll = _MemCatalog()
    # seed the 4 existing OpenNeuro sources
    seeds = {
        "ds004504": "10.18112/openneuro.ds004504.v1.0.9",
        "ds004584": "10.18112/openneuro.ds004584.v1.0.0",
        "ds005274": "10.18112/openneuro.ds005274.v1.0.0",
        "ds007763": "10.18112/openneuro.ds007763.v1.0.0",
    }
    titles = {
        "ds004504": "A dataset of EEG recordings from: Alzheimer's disease, Frontotemporal dementia and Healthy subjects",
        "ds004584": "Rest eyes open",
        "ds005274": "UV_EEG",
        "ds007763": "BCCWJ-MEG",
    }
    for ds, doi in seeds.items():
        await _seed_openneuro(coll, ds, doi, titles[ds])

    client = _FakeClient(pages, details)
    db = _make_db(coll)

    stats = await run_nemar_ingestion(db, target=limit, client=client, page_delay=0)

    # 10 discovered/retrieved/normalized/validated
    assert stats["discovered"] == limit
    assert stats["retrieved"] == limit
    assert stats["normalized"] == limit
    assert stats["validated_ok"] == limit
    # 4 existing mirrors merged, 6 new (1 missing mirror + 5 natives) inserted
    assert stats["merged"] == 4
    assert stats["inserted"] == 6
    assert stats["failed"] == 0
    assert stats["validation_failed"] == 0
    assert stats["matched_via"].get("cross_reference") == 4

    # ZERO asset/file calls; 1 list + 10 detail requests
    assert stats["asset_calls"] == 0
    assert stats["list_requests"] == 1
    assert stats["detail_requests"] == limit
    assert stats["total_api_calls"] == limit + 1
    assert not any(("/summary.json" in u or "/manifest.json" in u or "/records.json" in u) for u in client.requested_urls)

    # 4 seeded + 6 new = 10 canonical docs; merged docs carry both sources
    assert len(coll.docs) == 10
    merged_docs = [d for d in coll.docs if len(d["sources"]) == 2]
    assert len(merged_docs) == 4
    for d in merged_docs:
        assert {"openneuro", "nemar"} == {s["repository"] for s in d["sources"]}
        assert {"openneuro", "nemar"} == set(d["rawMetadata"].keys())
    native_docs = [d for d in coll.docs if d["sources"][0]["repository"] == "nemar" and len(d["sources"]) == 1]
    assert len(native_docs) == 6
    assert all(d["doi"] is None or d["doi"].startswith("10.82901/nemar.") for d in native_docs)


async def test_rerun_is_idempotent_no_duplicates():
    limit = 6
    ids, pages, details, _ = _mixed_run(limit)
    coll = _MemCatalog()
    seeds = {
        "ds004504": ("10.18112/openneuro.ds004504.v1.0.9", "A dataset of EEG recordings from: Alzheimer's disease, Frontotemporal dementia and Healthy subjects"),
    }
    for ds, (doi, title) in seeds.items():
        await _seed_openneuro(coll, ds, doi, title)

    client = _FakeClient(pages, details)
    db = _make_db(coll)
    first = await run_nemar_ingestion(db, target=limit, client=client, page_delay=0)
    assert first["inserted"] + first["merged"] == limit

    client2 = _FakeClient(pages, details)
    second = await run_nemar_ingestion(db, target=limit, client=client2, page_delay=0)
    assert second["inserted"] == 0
    assert second["merged"] == limit
    assert len(coll.docs) == limit            # no duplicate canonical records
    keys = [d["canonicalDatasetId"] for d in coll.docs]
    assert len(set(keys)) == limit


async def test_failed_detail_fetch_is_counted_not_aborting():
    limit = 3
    ids, pages, details, _ = _mixed_run(3)
    client = _FakeClient(pages, details)
    client.details.pop(ids[1], None)          # one detail returns empty (broken record)
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_nemar_ingestion(db, target=limit, client=client, page_delay=0)

    # the empty payload yields a record with no id → clean validation failure
    assert stats["validation_failed"] >= 1
    assert stats["retrieved"] == limit
    assert stats["asset_calls"] == 0
    assert len(coll.docs) >= 1                # healthy records still persisted
