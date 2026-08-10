"""DANDI small-sample ingestion test — mocked HTTP + in-memory catalog.

Verifies the full pipeline contract WITHOUT hitting the real DANDI API:
list → draft version → normalize → validate → dedup → persist, with
``target`` acting as the ``--limit`` smoke-test cap. Asserts ZERO asset-level
API calls and no duplicate source records.

NOTE: no ``@pytest.mark.asyncio`` — pytest.ini sets ``asyncio_mode = auto``.
"""
from unittest.mock import MagicMock

from app.catalog.ingest import run_dandi_ingestion

from ._dandi_fixtures import list_record, published_version, version_record


def _version_for(i: int) -> dict:
    v = version_record(identifier=f"DANDI:{i:06d}")
    v["url"] = f"https://dandiarchive.org/dandiset/{i:06d}/draft"
    return v


def _list_rec_for(i: int, published=None) -> dict:
    rec = list_record(identifier=f"{i:06d}", name=f"DANDI title {i}", published=published)
    return rec


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
    """Drop-in httpx.AsyncClient serving one list page + a version per dandiset."""

    def __init__(self, list_page: dict, versions: dict):
        self.list_page = list_page
        self.versions = versions
        self.requested_urls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, url: str) -> _FakeResponse:
        self.requested_urls.append(url)
        if "/versions/draft/" in url:
            ds_id = url.split("/dandisets/")[1].split("/versions/draft/")[0]
            return _FakeResponse(self.versions.get(ds_id, {}))
        return _FakeResponse(self.list_page)


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


def _build_run(limit: int = 10) -> tuple[dict, _MemCatalog, _FakeClient]:
    versions = {f"{i:06d}": _version_for(i) for i in range(1, limit + 1)}
    # first list record carries a real published version (provenance check)
    list_page = {
        "count": limit,
        "next": None,
        "results": [
            _list_rec_for(i, published=published_version() if i == 1 else None)
            for i in range(1, limit + 1)
        ],
    }
    client = _FakeClient(list_page, versions)
    coll = _MemCatalog()
    return client, coll, versions


async def test_small_sample_ingestion_contract():
    limit = 10
    client, coll, _ = _build_run(limit)
    db = _make_db(coll)

    stats = await run_dandi_ingestion(db, target=limit, client=client, page_delay=0, list_page_delay=0)

    # 10 DANDI records retrieved + normalized + validated
    assert stats["discovered"] == 10
    assert stats["retrieved"] == 10
    assert stats["normalized"] == 10
    assert stats["validated_ok"] == 10
    assert stats["inserted"] == 10
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["validation_failed"] == 0

    # ZERO asset-level API calls; 1 list + 10 version calls
    assert stats["asset_calls"] == 0
    assert stats["list_requests"] == 1
    assert stats["version_requests"] == 10
    assert stats["total_api_calls"] == 11
    assert not any("/assets/" in u for u in client.requested_urls)

    # 10 canonical records, each with a unique DANDI source key
    assert len(coll.docs) == 10
    keys = sorted(d["sourceKeys"][0] for d in coll.docs)
    assert len(set(keys)) == 10
    assert all(k.startswith("dandi:") for k in keys)

    for doc in coll.docs:
        src = doc["sources"][0]
        assert src["repository"] == "dandi"
        assert src["sourceDatasetId"].isdigit()
        assert src["sourceUrl"].startswith("https://dandiarchive.org/dandiset/")
        assert doc["doi"] is None                       # no placeholder DOI stored
        assert list(doc["rawMetadata"].keys()) == ["dandi"]
        assert doc["rawMetadata"]["dandi"]["assetsSummary"]["numberOfFiles"] is not None

    # published DOI is provenance-only on the dandi source of record 000001
    record_1 = next(d for d in coll.docs if d["sourceKeys"] == ["dandi:000001"])
    dandi_src = record_1["sources"][0]
    assert dandi_src["snapshot"]["publishedDoi"] == "10.48324/dandi.000003/0.260218.2052"
    assert record_1["doi"] is None


async def test_rerun_is_idempotent_no_duplicates():
    limit = 5
    client, coll, _ = _build_run(limit)
    db = _make_db(coll)

    first = await run_dandi_ingestion(db, target=limit, client=client, page_delay=0, list_page_delay=0)
    assert first["inserted"] == limit

    # Fresh client (same payloads) against the SAME catalog → all merges.
    client2, _, _ = _build_run(limit)
    second = await run_dandi_ingestion(db, target=limit, client=client2, page_delay=0, list_page_delay=0)

    assert second["inserted"] == 0
    assert second["merged"] == limit
    assert len(coll.docs) == limit            # no duplicate records
    keys = [d["sourceKeys"][0] for d in coll.docs]
    assert len(set(keys)) == limit


async def test_failed_version_fetch_is_counted_not_aborting():
    limit = 3
    client, coll, _ = _build_run(limit)
    # force dandiset 000003 to fail (returns empty dict → normalization yields
    # missing id → validation failure), others succeed.
    client.versions["000003"] = {"unexpected": True}
    db = _make_db(coll)

    stats = await run_dandi_ingestion(db, target=limit, client=client, page_delay=0, list_page_delay=0)

    assert stats["retrieved"] == limit
    assert stats["version_requests"] == limit
    assert stats["validation_failed"] >= 1     # broken version rejected cleanly
    assert stats["asset_calls"] == 0
    assert len(coll.docs) >= 1                 # healthy records still persisted
