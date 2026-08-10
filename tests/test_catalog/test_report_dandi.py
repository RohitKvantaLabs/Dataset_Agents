"""Test the catalog report builder for a DANDI Phase-1 run.

Verifies the output contract the standalone runner
(``trace_tools.ingest_dandi_catalog.py``) relies on: the DANDI-specific
``A_INGESTION`` fields (list/version/total API calls, ``asset_calls=0``) and
the DANDI dedup note — with an in-memory catalog and a mocked production
collection so nothing touches real Mongo.
"""

from app.catalog.ingest import run_dandi_ingestion
from app.catalog.report import build_report

from ._dandi_fixtures import list_record, version_record
from .test_ingest_dandi import _FakeClient, _MemCatalog, _make_db


class _ProdCollection:
    """Mimics the production ``datasets`` collection: count only, never written."""

    def __init__(self):
        self.name = "datasets"
        self.docs = []

    async def count_documents(self, _filter):
        if _filter.get("source") == "openneuro":
            return 3
        return 12


async def _run_small(limit: int = 3):
    versions = {}
    for i in range(1, limit + 1):
        v = version_record(identifier=f"DANDI:{i:06d}")
        v["url"] = f"https://dandiarchive.org/dandiset/{i:06d}/draft"
        versions[f"{i:06d}"] = v
    page = {
        "count": limit,
        "next": None,
        "results": [list_record(identifier=f"{i:06d}", name=f"Dataset {i}") for i in range(1, limit + 1)],
    }
    client = _FakeClient(page, versions)
    coll = _MemCatalog()
    db = _make_db(coll)
    stats = await run_dandi_ingestion(
        db, target=limit, client=client, page_delay=0, list_page_delay=0
    )
    return coll, stats


class TestReportDandi:
    async def test_a_ingestion_reports_dandi_stats(self):
        coll, stats = await _run_small(limit=3)
        db = _make_db(coll)
        docs = coll.docs
        report = await build_report(db, stats, docs, _ProdCollection())

        a = report["A_INGESTION"]
        assert a["repository"] == "dandi"
        assert a["list_api_requests"] == 1
        assert a["version_api_requests"] == 3
        assert a["total_api_calls"] == 4
        assert a["asset_level_api_calls"] == 0

    async def test_b_dedup_note_is_dandi_specific(self):
        coll, stats = await _run_small(limit=2)
        db = _make_db(coll)
        docs = coll.docs
        report = await build_report(db, stats, docs, _ProdCollection())

        note = report["B_DEDUPLICATION"]["note"]
        assert "DANDI Phase-1 run" in note
        assert "source identity is repository='dandi'" in note
        assert "preserved as provenance only" in note

    async def test_e_integrity_proves_production_untouched(self):
        coll, stats = await _run_small(limit=2)
        db = _make_db(coll)
        docs = coll.docs
        prod = _ProdCollection()
        report = await build_report(db, stats, docs, prod)

        integrity = report["E_DATA_INTEGRITY"]
        assert integrity["no_duplicate_source_keys"] is True
        assert integrity["no_dataset_files_downloaded"] is True
        untouched = integrity["production_datasets_untouched"]
        assert untouched["collection"] == "datasets"
        assert untouched["count_after_ingestion"] == 12
        assert untouched["openneuro_count_after_ingestion"] == 3
