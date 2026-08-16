"""HCP ingestion tests — mocked HTTP + in-memory catalog.

Verifies the full pipeline contract WITHOUT hitting the live CCF site:
index-page discovery → study page parse → releases/publications enrichment →
normalize → validate → dedup → persist.

Covers:
  - 20-study discovery via the two index pages
  - discovery gate: partial catalog and duplicate slugs STOP before writes
  - one Study = one canonical dataset
  - releases aggregated, never separate records
  - idempotency (rerun = all merges, no duplicates)
  - ids restriction (smoke-test path)
  - failure handling (broken study page is isolated)
  - ZERO image/file calls (ConnectomeDB/BALSA/file URLs never requested)
"""
from unittest.mock import MagicMock
from urllib.parse import urlparse

from app.catalog.ingest import run_hcp_ingestion
from app.catalog.normalize import build_hcp_source_record, canonical_record_from_source

from ._hcp_fixtures import (
    HCP_STUDY_SLUGS,
    _slug_title,
    index_html,
    publications_html,
    releases_html,
    study_dict,
    study_html,
)

# 14 releases for HCP Young Adult (mirrors the live page).
_HCP_YA_RELEASES = [
    ("HCP-Young Adult 2025", "08/11/2025"),
    ("S1200 Extensively Processed fMRI Data", "07/21/2017"),
    ("1200 Subjects Data Release", "03/01/2017"),
    ("900 Subjects Data Release Reference", "12/08/2015"),
    ("500 Subjects Plus MEG2", "11/25/2014"),
    ("HCP Lifespan Pilot Data Release", "08/01/2014"),
    ("MGH Adult Diffusion Data Release", "08/01/2014"),
    ("March 2014 MR Data Patch", "03/25/2014"),
    ("MEG1 Initial Data Release", "03/04/2014"),
    ("Q1-Q3 Diffusion Data Update", "01/31/2014"),
    ("Q3 Subjects Data Release", "09/23/2013"),
    ("Q2 Subjects Data Release", "06/13/2013"),
    ("Q1 Subjects Data Release", "03/05/2013"),
]


# ─────────────────────────────────────────────────────────────────────────────
# Fake transport — serves the Drupal pages by URL (metadata HTML only)
# ─────────────────────────────────────────────────────────────────────────────


class _FakeResponse:
    def __init__(self, text: str):
        self.text = text
        self.status_code = 200

    def raise_for_status(self):
        pass


class _FakeClient:
    """Drop-in httpx.AsyncClient serving the CCF pages by URL path."""

    def __init__(self, slugs: list[str], *, fail_study: str | None = None):
        self.slugs = slugs
        self.fail_study = fail_study
        self.requested_urls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, url: str, **kwargs) -> _FakeResponse:
        self.requested_urls.append(url)
        path = urlparse(url).path

        if path in ("/lifespan-studies", "/disease-studies"):
            return _FakeResponse(index_html(self.slugs))

        m = urlparse(url).path.split("/")
        if len(m) >= 3 and m[1] == "study" and m[2] in self.slugs:
            slug = m[2]
            if slug == self.fail_study:
                raise RuntimeError("boom")
            if len(m) == 3:
                return _FakeResponse(study_html(slug))
            if len(m) == 4 and m[3] == "data-releases":
                if slug == "hcp-young-adult":
                    return _FakeResponse(releases_html(_HCP_YA_RELEASES))
                return _FakeResponse(releases_html([]))
            if len(m) == 4 and m[3] == "publications":
                return _FakeResponse(
                    publications_html(["10.1038/nature18933", "10.1038/sdata.2017.10"])
                )
        raise RuntimeError(f"unhandled url: {url}")


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
# Test: full 20-study discovery + one study = one canonical dataset
# ─────────────────────────────────────────────────────────────────────────────


async def test_full_20_study_ingestion():
    coll = _MemCatalog()
    client = _FakeClient(HCP_STUDY_SLUGS)
    db = _make_db(coll)

    stats = await run_hcp_ingestion(db, client=client, page_delay=0)

    assert stats["gate_failed"] is False
    assert stats["discovered"] == 20
    assert stats["expected_studies"] == 20
    assert stats["inserted"] == 20
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["asset_calls"] == 0
    assert stats["index_requests"] == 2
    assert stats["study_requests"] == 20
    assert stats["release_requests"] == 20
    assert stats["publication_requests"] == 20
    assert stats["total_api_calls"] == 62

    assert len(coll.docs) == 20
    keys = {d["sourceKeys"][0] for d in coll.docs}
    assert len(keys) == 20  # all unique
    assert "hcp:hcp:hcp-young-adult" in keys


async def test_each_study_is_one_canonical_dataset_with_releases():
    coll = _MemCatalog()
    client = _FakeClient(HCP_STUDY_SLUGS)
    db = _make_db(coll)

    await run_hcp_ingestion(db, client=client, page_delay=0)

    ya = next(
        d for d in coll.docs if d["sourceKeys"] == ["hcp:hcp:hcp-young-adult"]
    )
    assert len(ya["sources"]) == 1  # one study = one canonical record
    assert len(ya["rawMetadata"]["hcp"]["dataReleases"]) == 13  # aggregated, NOT 13 records
    assert ya["rawMetadata"]["hcp"]["publications"] == [
        "10.1038/nature18933", "10.1038/sdata.2017.10",
    ]
    assert ya["doi"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Test: discovery gate — partial catalog STOPS before writes
# ─────────────────────────────────────────────────────────────────────────────


async def test_gate_rejects_partial_catalog():
    coll = _MemCatalog()
    partial = HCP_STUDY_SLUGS[:5]
    client = _FakeClient(partial)
    db = _make_db(coll)

    stats = await run_hcp_ingestion(db, client=client, page_delay=0)

    assert stats["gate_failed"] is True
    assert "discovered=5" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert stats["merged"] == 0
    assert len(coll.docs) == 0  # NOTHING written
    assert stats["asset_calls"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test: discovery gate — duplicate slugs STOPS before writes
# ─────────────────────────────────────────────────────────────────────────────


class _DuplicateIndexClient(_FakeClient):
    """Returns the first study twice on one index page (duplicate slugs)."""

    async def get(self, url: str, **kwargs):
        self.requested_urls.append(url)
        path = urlparse(url).path
        if path == "/lifespan-studies":
            dupes = self.slugs + [self.slugs[0]]  # first slug repeated
            return _FakeResponse(index_html(dupes))
        return await super().get(url, **kwargs)


async def test_gate_rejects_duplicate_slugs():
    coll = _MemCatalog()
    client = _DuplicateIndexClient(HCP_STUDY_SLUGS)
    db = _make_db(coll)

    stats = await run_hcp_ingestion(db, client=client, page_delay=0)

    assert stats["gate_failed"] is True
    assert stats["duplicate_slugs"] == [HCP_STUDY_SLUGS[0]]
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


# ─────────────────────────────────────────────────────────────────────────────
# Test: idempotency — rerun produces merges, no duplicates
# ─────────────────────────────────────────────────────────────────────────────


async def test_rerun_is_idempotent_no_duplicates():
    coll = _MemCatalog()
    db = _make_db(coll)

    first = await run_hcp_ingestion(db, client=_FakeClient(HCP_STUDY_SLUGS), page_delay=0)
    assert first["inserted"] == 20

    second = await run_hcp_ingestion(db, client=_FakeClient(HCP_STUDY_SLUGS), page_delay=0)
    assert second["inserted"] == 0
    assert second["merged"] == 20
    assert set(second["matched_via"].keys()) <= {"source_url", "source_key"}
    assert len(coll.docs) == 20  # no duplicates


# ─────────────────────────────────────────────────────────────────────────────
# Test: ids restriction (smoke-test path)
# ─────────────────────────────────────────────────────────────────────────────


async def test_ids_restriction():
    coll = _MemCatalog()
    client = _FakeClient(HCP_STUDY_SLUGS)
    db = _make_db(coll)

    stats = await run_hcp_ingestion(
        db, client=client, page_delay=0, ids=["hcp-young-adult", "hcp-lifespan-aging"]
    )

    assert stats["discovered"] == 20          # full discovery still runs
    assert stats["retrieved"] == 2            # only the requested studies processed
    assert stats["inserted"] == 2
    assert len(coll.docs) == 2


# ─────────────────────────────────────────────────────────────────────────────
# Test: broken study page is isolated (does not abort the run)
# ─────────────────────────────────────────────────────────────────────────────


async def test_broken_study_isolated():
    coll = _MemCatalog()
    client = _FakeClient(HCP_STUDY_SLUGS, fail_study="hcp-lifespan-aging")
    db = _make_db(coll)

    stats = await run_hcp_ingestion(db, client=client, page_delay=0)

    assert stats["api_failures"] >= 1
    assert stats["inserted"] == 19            # the other 19 still inserted
    assert stats["failed"] == 0
    assert len(coll.docs) == 19
    assert stats["gate_failed"] is False      # discovery unaffected


# ─────────────────────────────────────────────────────────────────────────────
# Test: ZERO image/file calls — only the three metadata HTML pages
# ─────────────────────────────────────────────────────────────────────────────


async def test_zero_image_file_calls():
    coll = _MemCatalog()
    client = _FakeClient(HCP_STUDY_SLUGS)
    db = _make_db(coll)

    stats = await run_hcp_ingestion(db, client=client, page_delay=0)

    assert stats["asset_calls"] == 0
    assert stats["total_api_calls"] == stats["index_requests"] + stats["study_requests"] + \
        stats["release_requests"] + stats["publication_requests"]
    for u in client.requested_urls:
        path = urlparse(u).path
        # ONLY metadata pages: index pages + /study/<slug>[/data-releases|/publications]
        assert path in ("/lifespan-studies", "/disease-studies") or path.startswith("/study/")
        assert "/data" not in path or path.endswith(("data-releases",))
        assert "connectomedb" not in path.lower() and "balsa" not in u.lower()
        assert not any(ext in u.lower() for ext in (".jpg", ".png", ".tif", ".pdf", ".zip", ".nii"))
