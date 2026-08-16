"""NeuroMorpho ingestion tests — mocked HTTP + in-memory catalog.

Verifies the full pipeline contract WITHOUT hitting the real NeuroMorpho
API: Solr pagination → group neurons → normalize → validate → dedup →
persist.

Covers:
  - one contribution = one canonical dataset
  - neuron count aggregation
  - idempotency (rerun = all merges, no duplicates)
  - failure handling (broken neuron page, no abort)
  - ZERO asset/SWC file calls
"""
from unittest.mock import MagicMock

from app.catalog.ingest import run_neuromorpho_ingestion
from app.catalog.normalize import (
    build_neuromorpho_source_record,
    canonical_record_from_source,
    group_neuromorpho_neurons,
)

from ._neuromorpho_fixtures import (
    neuron,
    single_archive_one_publication,
    single_archive_two_publications,
    placeholders_only,
    many_neurons_same_contribution,
    doi_only_contribution,
    solr_list_page,
)


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
    """Drop-in httpx.AsyncClient serving Solr /select pages by page number."""

    def __init__(self, pages: list[dict]):
        self.pages = pages
        self.requested_urls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, url: str) -> _FakeResponse:
        self.requested_urls.append(url)
        if "page=" in url:
            page = int(url.split("page=")[1])
            if 0 <= page < len(self.pages):
                return _FakeResponse(self.pages[page])
            return _FakeResponse(solr_list_page([]))
        return _FakeResponse(self.pages[0])


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
# Helper: seed an existing canonical NeuroMorpho record
# ─────────────────────────────────────────────────────────────────────────────


async def _seed_neuromorpho(coll: _MemCatalog, neurons: list[dict]) -> dict:
    groups = group_neuromorpho_neurons(neurons)
    src = build_neuromorpho_source_record(groups[0])
    doc = canonical_record_from_source(src)
    await coll.insert_one(doc)
    return doc


# ─────────────────────────────────────────────────────────────────────────────
# Test: one contribution → one canonical dataset
# ─────────────────────────────────────────────────────────────────────────────


async def test_single_contribution_inserts_one_canonical():
    neurons = single_archive_one_publication()
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)

    assert stats["discovered"] == 3          # 3 neurons
    assert stats["neuron_count"] == 3
    assert stats["groups_formed"] == 1       # 1 contribution group
    assert stats["pmid_backed_groups"] == 1
    assert stats["doi_only_groups"] == 0
    assert stats["retrieved"] == 1
    assert stats["normalized"] == 1
    assert stats["inserted"] == 1
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["asset_calls"] == 0

    assert len(coll.docs) == 1
    doc = coll.docs[0]
    assert doc["canonicalDatasetId"].startswith("ns-")
    assert doc["sourceKeys"] == ["neuromorpho:neuromorpho:Mallick:pmid:21228908"]
    assert doc["sources"][0]["neuronCount"] == 3
    assert doc["sources"][0]["repository"] == "neuromorpho"


# ─────────────────────────────────────────────────────────────────────────────
# Test: multiple contributions from multiple archives
# ─────────────────────────────────────────────────────────────────────────────


async def test_multiple_archives_multiple_contributions():
    neurons = (
        single_archive_one_publication()      # Mallick: 1 group
        + single_archive_two_publications()   # Jacobs: 2 groups
        + placeholders_only()                 # Siegert: 1 DOI-only group
        + doi_only_contribution()             # Kuddannaya: 1 DOI-only group
    )
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)

    # 4 groups: 2 PMID (Mallick, Jacobs-12204204... wait 2 Jacobs groups)
    assert stats["groups_formed"] == 5  # Mallick(1) + Jacobs(2) + Siegert(1) + Kuddannaya(1)
    assert stats["pmid_backed_groups"] == 3
    assert stats["doi_only_groups"] == 2
    assert stats["inserted"] == 5
    assert stats["failed"] == 0
    assert len(coll.docs) == 5


# ─────────────────────────────────────────────────────────────────────────────
# Test: same archive + same PMID across many neurons → ONE dataset
# ─────────────────────────────────────────────────────────────────────────────


async def test_many_neurons_one_dataset():
    neurons = many_neurons_same_contribution()  # 50 neurons, Chiang:21129968
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)

    assert stats["groups_formed"] == 1
    assert stats["inserted"] == 1
    assert len(coll.docs) == 1
    assert coll.docs[0]["sources"][0]["neuronCount"] == 50


# ─────────────────────────────────────────────────────────────────────────────
# Test: idempotency — rerun produces merges, no duplicates
# ─────────────────────────────────────────────────────────────────────────────


async def test_rerun_is_idempotent_no_duplicates():
    neurons = single_archive_one_publication()
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    first = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)
    assert first["inserted"] == 1

    client2 = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    second = await run_neuromorpho_ingestion(db, neurons=neurons, client=client2,
                                             page_delay=0, group_limit=10)
    assert second["inserted"] == 0
    assert second["merged"] == 1
    assert len(coll.docs) == 1  # no duplicate


# ─────────────────────────────────────────────────────────────────────────────
# Test: Solr pagination across multiple pages
# ─────────────────────────────────────────────────────────────────────────────


async def test_solr_pagination_multiple_pages():
    neurons_a = single_archive_one_publication()      # 3 neurons, Mallick
    neurons_b = doi_only_contribution()               # 2 neurons, Kuddannaya
    pages = [
        solr_list_page(neurons_a, page=0, size=500, total=5),
        solr_list_page(neurons_b, page=1, size=500, total=5),
    ]
    coll = _MemCatalog()
    client = _FakeClient(pages)
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, client=client, page_delay=0,
                                            page_size=500, group_limit=10)

    assert stats["pages"] == 2
    assert stats["neuron_count"] == 5
    assert stats["groups_formed"] == 2
    assert stats["inserted"] == 2
    assert len(client.requested_urls) == 2
    assert all("/select?q=" in u for u in client.requested_urls)


# ─────────────────────────────────────────────────────────────────────────────
# Test: broken neuron page is counted, not aborting
# ─────────────────────────────────────────────────────────────────────────────


class _BrokenResponse(_FakeResponse):
    def raise_for_status(self):
        raise RuntimeError("boom")


class _FailingClient(_FakeClient):
    async def get(self, url: str):
        self.requested_urls.append(url)
        if "page=0" in url:
            return _BrokenResponse({})
        return _FakeResponse(solr_list_page([]))


async def test_broken_page_counted_not_aborting():
    neurons = single_archive_one_publication()
    coll = _MemCatalog()
    client = _FailingClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, client=client, page_delay=0,
                                            page_size=500, group_limit=10)

    assert stats["api_failures"] >= 1
    assert stats["asset_calls"] == 0
    # no neurons were fetched (page 0 failed) → no groups
    assert stats["neuron_count"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test: group_limit caps contributions
# ─────────────────────────────────────────────────────────────────────────────


async def test_group_limit_caps_contributions():
    neurons = (
        single_archive_one_publication()
        + single_archive_two_publications()
        + placeholders_only()
        + doi_only_contribution()
    )
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=3)

    assert stats["groups_formed"] == 3
    assert stats["inserted"] == 3
    assert len(coll.docs) == 3


# ─────────────────────────────────────────────────────────────────────────────
# Test: no asset/file calls ever
# ─────────────────────────────────────────────────────────────────────────────


async def test_zero_asset_calls():
    neurons = many_neurons_same_contribution()
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)

    assert stats["asset_calls"] == 0
    assert stats["total_api_calls"] == stats["neuron_requests"]
    # no swc/asc or file endpoints requested
    assert not any((".swc" in u or ".asc" in u or "/morphometry" in u) for u in client.requested_urls)
    # only /select pages requested
    assert all("/select" in u for u in client.requested_urls)


# ─────────────────────────────────────────────────────────────────────────────
# Regression: same archive + different PMID must NOT merge into one canonical
# doc. This was the production blocker: the shared per-archive sourceUrl made
# the generic resolver merge distinct publications (27 wrongly absorbed).
# ─────────────────────────────────────────────────────────────────────────────


async def test_same_archive_different_pmid_stay_separate():
    neurons = single_archive_two_publications()  # Jacobs: PMIDs 12204204 + 9230750
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)

    assert stats["groups_formed"] == 2
    assert stats["inserted"] == 2
    assert stats["merged"] == 0
    assert len(coll.docs) == 2  # two canonical datasets, not one

    # Distinct canonical IDs and distinct sourceKeys (no collision).
    cids = [d["canonicalDatasetId"] for d in coll.docs]
    keys = [d["sourceKeys"][0] for d in coll.docs]
    assert len(set(cids)) == 2
    assert len(set(keys)) == 2
    assert "neuromorpho:neuromorpho:Jacobs:pmid:12204204" in keys
    assert "neuromorpho:neuromorpho:Jacobs:pmid:9230750" in keys

    # Each doc carries exactly ONE neuromorpho source (no absorption).
    for d in coll.docs:
        nm = [s for s in d["sources"] if s["repository"] == "neuromorpho"]
        assert len(nm) == 1


async def test_within_archive_doi_artifact_not_merged():
    """Ascoli pattern: 3 PMID groups sharing one within-archive DOI stay
    separate; the DOI-only group keeps its DOI identity."""
    neurons = [
        neuron(archive="Ascoli", pmid="2007659", doi="10.1016/j.neucom.2004.10.105",
               species="rat", brain_region=["neocortex"], nid_offset=0),
        neuron(archive="Ascoli", pmid="2329188", doi="10.1016/j.neucom.2004.10.105",
               species="rat", brain_region=["hippocampus"], nid_offset=1),
        neuron(archive="Ascoli", pmid="3401733", doi="10.1016/j.neucom.2004.10.105",
               species="rat", brain_region=["striatum"], nid_offset=2),
    ]
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)

    assert stats["groups_formed"] == 3
    assert stats["inserted"] == 3
    assert stats["merged"] == 0
    assert len(coll.docs) == 3
    # No PMID-backed source carries the artifact DOI as its doi field.
    for d in coll.docs:
        src = d["sources"][0]
        assert src["doi"] is None  # artifact cleared
        assert src["snapshot"]["pmid"] is not None


async def test_cross_archive_same_publication_merges():
    """The 6 legitimate cross-archive merges must be PRESERVED: same
    publication (same DOI/PMID) deposited under two archive labels resolves
    to ONE canonical dataset with two sources."""
    neurons = [
        neuron(archive="Wilson_R", pmid="", doi="10.1101/666073",
               species="mouse", brain_region=["hippocampus"], nid_offset=0),
        neuron(archive="Scimemi", pmid="", doi="10.1101/666073",
               species="mouse", brain_region=["hippocampus"], nid_offset=1),
    ]
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)

    assert stats["groups_formed"] == 2
    assert stats["inserted"] == 1
    assert stats["merged"] == 1
    assert len(coll.docs) == 1  # ONE canonical dataset
    nm = [s for s in coll.docs[0]["sources"] if s["repository"] == "neuromorpho"]
    assert len(nm) == 2  # both sources preserved
    assert {s["sourceDatasetId"].split(":")[1] for s in nm} == {"Wilson_R", "Scimemi"}


# ─────────────────────────────────────────────────────────────────────────────
# Test: full small-sample contract (mixed scenario)
# ─────────────────────────────────────────────────────────────────────────────


async def test_full_small_sample_contract():
    neurons = (
        single_archive_one_publication()      # 1 PMID group, 3 neurons
        + single_archive_two_publications()   # 2 PMID groups, 4 neurons
        + placeholders_only()                 # 1 DOI group, 3 neurons
        + many_neurons_same_contribution()    # 1 PMID group, 50 neurons
        + doi_only_contribution()             # 1 DOI group, 2 neurons
    )
    coll = _MemCatalog()
    client = _FakeClient([solr_list_page(neurons, total=len(neurons))])
    db = _make_db(coll)

    stats = await run_neuromorpho_ingestion(db, neurons=neurons, client=client,
                                            page_delay=0, group_limit=10)

    assert stats["neuron_count"] == 3 + 4 + 3 + 50 + 2
    assert stats["groups_formed"] == 6
    assert stats["pmid_backed_groups"] == 4   # Mallick + Jacobsx2 + Chiang
    assert stats["doi_only_groups"] == 2      # Siegert + Kuddannaya
    assert stats["inserted"] == 6
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["asset_calls"] == 0
    assert len(coll.docs) == 6

    # sourceKeys all unique
    keys = [d["sourceKeys"][0] for d in coll.docs]
    assert len(set(keys)) == 6
    assert all(k.startswith("neuromorpho:neuromorpho:") for k in keys)