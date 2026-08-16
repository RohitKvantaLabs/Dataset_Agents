"""Allen ingestion tests — mocked HTTP + in-memory catalog.

Verifies the full pipeline contract WITHOUT hitting the real Allen RMA API:
Product enumeration → Age lookup → per-product include fetch → normalize →
validate → dedup → persist.

Covers:
  - 64-product enumeration and the live-count policy fields
  - one Product = one canonical dataset
  - child statistics aggregation (arrays never persisted)
  - idempotency (rerun = all merges, no duplicates)
  - ids restriction (smoke-test path)
  - failure handling (broken enumeration, no abort)
  - ZERO image/file calls (SectionImages never requested)
"""
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

from app.catalog.ingest import run_allen_ingestion
from app.catalog.normalize import (
    build_allen_source_record,
    canonical_record_from_source,
)

from ._allen_fixtures import (
    all_64_products,
    ages_payload,
    dataset_row,
    donor_row,
    enumeration_payload,
    include_payload,
    product,
    specimen_row,
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
    """Drop-in httpx.AsyncClient serving RMA query.json routes by criteria."""

    def __init__(self, products: list[dict], data_sets=None, specimens=None, donors=None):
        self.products = {int(p["id"]): p for p in products}
        self.data_sets = data_sets or []
        self.specimens = specimens or []
        self.donors = donors or []
        self.requested_urls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def get(self, url: str, **kwargs) -> _FakeResponse:
        # httpx passes query params separately — merge them into the URL for
        # criteria-based routing, exactly like the real client would.
        params = kwargs.get("params") or {}
        if params:
            sep = "&" if "?" in url else "?"
            url = url + sep + "&".join(f"{k}={v}" for k, v in params.items())
        self.requested_urls.append(url)
        qs = parse_qs(urlparse(url).query)
        criteria = (qs.get("criteria") or [""])[0]
        if criteria == "model::Product":
            return _FakeResponse(enumeration_payload(list(self.products.values())))
        if criteria == "model::Age":
            return _FakeResponse(ages_payload())
        if criteria.startswith("model::Product[id$eq"):
            pid = int(criteria.split("$eq")[1].rstrip("]"))
            if pid not in self.products:
                return _FakeResponse({"success": True, "msg": [], "total_rows": 0})
            payload = include_payload(
                self.products[pid],
                data_sets=self.data_sets,
                specimens=self.specimens,
                donors=self.donors,
            )
            return _FakeResponse(payload)
        return _FakeResponse({"success": False, "msg": "unhandled criteria"})


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


def _products(*ids: int) -> list[dict]:
    pool = {p["id"]: p for p in all_64_products()}
    return [pool[i] for i in ids]


# ─────────────────────────────────────────────────────────────────────────────
# Test: 64-product enumeration + one product = one canonical dataset
# ─────────────────────────────────────────────────────────────────────────────


async def test_single_product_inserts_one_canonical():
    coll = _MemCatalog()
    client = _FakeClient(_products(62))
    db = _make_db(coll)

    stats = await run_allen_ingestion(db, client=client, page_delay=0, target=1)

    assert stats["enumerated_total"] == 1
    assert stats["expected_products"] == 64
    assert stats["discovered"] == 1
    assert stats["retrieved"] == 1
    assert stats["normalized"] == 1
    assert stats["inserted"] == 1
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["asset_calls"] == 0
    assert stats["enumeration_requests"] == 1
    assert stats["age_requests"] == 1
    assert stats["product_requests"] == 1
    assert stats["total_api_calls"] == 3

    assert len(coll.docs) == 1
    doc = coll.docs[0]
    assert doc["canonicalDatasetId"].startswith("ns-")
    assert doc["sourceKeys"] == ["allen:allen:62"]
    assert doc["sources"][0]["repository"] == "allen"
    assert doc["sources"][0]["sourceDatasetId"] == "allen:62"


async def test_full_64_product_enumeration():
    coll = _MemCatalog()
    client = _FakeClient(_products(*range(1, 65)))
    db = _make_db(coll)

    stats = await run_allen_ingestion(db, client=client, page_delay=0)

    assert stats["enumerated_total"] == 64
    assert stats["discovered"] == 64
    assert stats["retrieved"] == 64
    assert stats["inserted"] == 64
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert len(coll.docs) == 64
    keys = [d["sourceKeys"][0] for d in coll.docs]
    assert len(set(keys)) == 64  # all unique


# ─────────────────────────────────────────────────────────────────────────────
# Test: child statistics aggregated, arrays never persisted
# ─────────────────────────────────────────────────────────────────────────────


async def test_child_statistics_aggregated_not_stored():
    coll = _MemCatalog()
    client = _FakeClient(
        _products(33),
        data_sets=[dataset_row(ds_id=i) for i in range(2679)],
        specimens=[specimen_row(specimen_id=i) for i in range(1923)],
        donors=[donor_row(donor_id=i, age_id=62) for i in range(921)],
    )
    db = _make_db(coll)

    stats = await run_allen_ingestion(db, client=client, page_delay=0, target=1)

    assert stats["inserted"] == 1
    doc = coll.docs[0]
    src = doc["sources"][0]
    assert src["snapshot"]["dataSetCount"] == 2679
    assert src["snapshot"]["specimenCount"] == 1923
    assert src["snapshot"]["donorCount"] == 921
    assert stats["child_stats_total"]["dataSetCount"] == 2679
    raw = doc["rawMetadata"]["allen"]
    assert "data_sets" not in raw
    assert "specimens" not in raw
    assert "donors" not in raw
    assert raw["childStats"]["dataSetCount"] == 2679


# ─────────────────────────────────────────────────────────────────────────────
# Test: ids restriction (smoke-test path)
# ─────────────────────────────────────────────────────────────────────────────


async def test_ids_restriction():
    coll = _MemCatalog()
    client = _FakeClient(_products(62, 26, 28, 33, 34))
    db = _make_db(coll)

    stats = await run_allen_ingestion(db, client=client, page_delay=0, ids=[62, 26, 28])

    assert stats["discovered"] == 5          # full enumeration still runs
    assert stats["retrieved"] == 3           # only the requested ids processed
    assert stats["inserted"] == 3
    assert len(coll.docs) == 3
    assert {d["sourceKeys"][0] for d in coll.docs} == {
        "allen:allen:62", "allen:allen:26", "allen:allen:28",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Test: idempotency — rerun produces merges, no duplicates
# ─────────────────────────────────────────────────────────────────────────────


async def test_rerun_is_idempotent_no_duplicates():
    coll = _MemCatalog()
    client = _FakeClient(_products(62))
    db = _make_db(coll)

    first = await run_allen_ingestion(db, client=client, page_delay=0, target=1)
    assert first["inserted"] == 1

    client2 = _FakeClient(_products(62))
    second = await run_allen_ingestion(db, client=client2, page_delay=0, target=1)
    assert second["inserted"] == 0
    assert second["merged"] == 1
    # confident match via the distinct per-product sourceUrl (or source key)
    assert set(second["matched_via"].keys()) <= {"source_url", "source_key"}
    assert len(coll.docs) == 1  # no duplicate


# ─────────────────────────────────────────────────────────────────────────────
# Test: broken enumeration is counted, not aborting
# ─────────────────────────────────────────────────────────────────────────────


class _BrokenResponse(_FakeResponse):
    def raise_for_status(self):
        raise RuntimeError("boom")


class _FailingEnumerationClient(_FakeClient):
    async def get(self, url: str, **kwargs):
        params = kwargs.get("params") or {}
        if params:
            sep = "&" if "?" in url else "?"
            url = url + sep + "&".join(f"{k}={v}" for k, v in params.items())
        self.requested_urls.append(url)
        qs = parse_qs(urlparse(url).query)
        criteria = (qs.get("criteria") or [""])[0]
        if criteria == "model::Product":
            raise _BrokenResponse({})
        return _FakeResponse(ages_payload())


async def test_broken_enumeration_counted_not_aborting():
    coll = _MemCatalog()
    client = _FailingEnumerationClient(_products(62))
    db = _make_db(coll)

    stats = await run_allen_ingestion(db, client=client, page_delay=0)

    assert stats["api_failures"] >= 1
    assert stats["enumerated_total"] == 0
    assert stats["retrieved"] == 0
    assert stats["asset_calls"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test: per-product failure is isolated
# ─────────────────────────────────────────────────────────────────────────────


class _FailingProductClient(_FakeClient):
    async def get(self, url: str, **kwargs):
        params = kwargs.get("params") or {}
        if params:
            sep = "&" if "?" in url else "?"
            url = url + sep + "&".join(f"{k}={v}" for k, v in params.items())
        qs = parse_qs(urlparse(url).query)
        criteria = (qs.get("criteria") or [""])[0]
        if criteria.startswith("model::Product[id$eq"):
            pid = int(criteria.split("$eq")[1].rstrip("]"))
            if pid == 26:
                raise RuntimeError("boom")
        # delegate to the parent for every other route
        return await _FakeClient.get(self, url, **kwargs)


async def test_broken_product_isolated():
    coll = _MemCatalog()
    client = _FailingProductClient(_products(62, 26))
    db = _make_db(coll)

    stats = await run_allen_ingestion(db, client=client, page_delay=0)

    assert stats["api_failures"] >= 1
    assert stats["inserted"] == 1       # product 62 still inserted
    assert stats["failed"] == 0
    assert len(coll.docs) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Test: ZERO image/file calls
# ─────────────────────────────────────────────────────────────────────────────


async def test_zero_image_file_calls():
    coll = _MemCatalog()
    client = _FakeClient(_products(62, 26))
    db = _make_db(coll)

    stats = await run_allen_ingestion(db, client=client, page_delay=0)

    assert stats["asset_calls"] == 0
    assert stats["total_api_calls"] == stats["enumeration_requests"] + \
        stats["age_requests"] + stats["product_requests"]
    assert stats["total_api_calls"] == 1 + 1 + 2
    # ONLY the metadata query.json endpoint is ever requested — never
    # SectionImage/image-file endpoints.
    for u in client.requested_urls:
        assert "/data/query.json" in u
        assert "SectionImage" not in u and "section_image" not in u
        assert not any(ext in u.lower() for ext in (".jpg", ".png", ".tif", ".swc", ".asc"))
