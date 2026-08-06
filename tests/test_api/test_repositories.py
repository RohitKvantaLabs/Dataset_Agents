"""
Tests for the Phase-4 repository endpoints (§4.6):
    POST /api/v1/agents/repository-search
    POST /api/v1/agents/repository-sync
    GET  /api/v1/agents/repository-health
    GET  /api/v1/cron/ingest-repositories

Covers:
- internal-secret endpoints reject missing (422) / wrong (401) / accept correct (200)
- cron endpoint rejects missing/wrong cron secret, accepts correct
- response shapes match §4.6
- repository-search runs quality pipeline with publish=True, re-queries Mongo,
  and returns the persisted canonical documents (with `_id`)
- repository-sync returns per-source PipelineResult

All connectors, quality pipeline, and Mongo counts are mocked — no real I/O.
"""
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from app.ingestion.quality_pipeline import PipelineRunResult
from app.ingestion.pipeline import PipelineResult
from app.models.dataset import Dataset
from app.services.repository_retrieval import AggregateResult

# conftest.py provides `client`, `auth_headers`, `bad_headers`,
# `cron_headers`, `bad_cron_headers` fixtures.

_SAMPLE_FILTERS = {
    "modality": ["fMRI"],
    "species": ["human"],
    "age_range": "pediatric",
    "condition": ["ADHD"],
    "task": "resting-state",
    "format": [],
    "keywords": [],
    "raw_query": "resting state fMRI in kids with ADHD",
}


def _aggregate() -> AggregateResult:
    return AggregateResult(
        query="resting state fMRI in kids with ADHD",
        records=[],
        per_source={},
        sources_queried=["dandi", "openneuro"],
        errors=[],
        elapsed_ms=12,
        total_available=0,
    )


def _pipeline(datasets: list[Dataset] | None = None) -> PipelineRunResult:
    return PipelineRunResult(
        stages={},
        datasets=datasets or [],
        dedup_groups=[],
        errors=[],
        elapsed_ms=8,
    )


# ---------------------------------------------------------------------------
# POST /api/v1/agents/repository-search
# ---------------------------------------------------------------------------

class TestRepositorySearchAuth:
    def test_missing_secret_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/api/v1/agents/repository-search",
            json={"query": "fmri", "filters": _SAMPLE_FILTERS},
        )
        assert resp.status_code == 422

    def test_wrong_secret_returns_401(self, client: TestClient, bad_headers: dict) -> None:
        resp = client.post(
            "/api/v1/agents/repository-search",
            headers=bad_headers,
            json={"query": "fmri", "filters": _SAMPLE_FILTERS},
        )
        assert resp.status_code == 401


class TestRepositorySearchResponse:
    def test_response_shape_and_publish_mongo_documents(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        """§4.6 canonical-persistence contract.

        publish=True → re-query Mongo by the discovered identities → response
        datasets are the persisted canonical documents (with ``_id``, provenance,
        quality score), never transient in-memory pipeline previews.
        """
        dataset = Dataset(
            title="ADHD resting fMRI",
            description="desc",
            source="dandi",
            source_id="DANDI:000003",
            url="https://dandiarchive.org/dandiset/3",
            modality=["fMRI"],
            species=["human"],
        )
        persisted = dataset.model_copy(
            update={
                "id": "66f0deadbeef000000000001",
                "provenance": {"dedup_key": "dandi:DANDI:000003"},
                "quality_score": 0.42,
            }
        )
        with (
            patch(
                "app.api.v1.repositories.aggregate_repository_search",
                new=AsyncMock(return_value=_aggregate()),
            ),
            patch(
                "app.api.v1.repositories.run_quality_pipeline",
                new=AsyncMock(return_value=_pipeline([dataset])),
            ) as mock_pipeline,
            patch(
                "app.api.v1.repositories.find_datasets_by_identity",
                new=AsyncMock(return_value=[persisted]),
            ) as mock_find,
        ):
            resp = client.post(
                "/api/v1/agents/repository-search",
                headers=auth_headers,
                json={"query": "fmri", "filters": _SAMPLE_FILTERS},
            )
        assert resp.status_code == 200
        body = resp.json()
        for field in ("query_id", "sources_queried", "total_found", "elapsed_ms", "datasets"):
            assert field in body, f"Missing field: {field}"
        assert body["sources_queried"] == ["dandi", "openneuro"]
        assert body["total_found"] == 1
        assert len(body["datasets"]) == 1
        assert body["datasets"][0]["source"] == "dandi"
        # Mongo-backed record: _id present, provenance attached, quality score.
        assert body["datasets"][0]["_id"] == "66f0deadbeef000000000001"
        assert body["datasets"][0]["provenance"]["dedup_key"] == "dandi:DANDI:000003"
        assert body["datasets"][0]["quality_score"] == 0.42
        # Stage 7 publication is the persistence contract.
        assert mock_pipeline.await_args.kwargs["publish"] is True
        assert mock_pipeline.await_args.kwargs["discovery_method"] == "repository_search"
        # Re-query used the discovered identities (post-run canonical keys).
        assert mock_find.await_args.args[0] == [("dandi", "DANDI:000003")]

    def test_filters_are_passed_with_raw_query_injected(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        mock_aggregate = AsyncMock(return_value=_aggregate())
        mock_pipeline = AsyncMock(return_value=_pipeline())
        with (
            patch("app.api.v1.repositories.aggregate_repository_search", new=mock_aggregate),
            patch("app.api.v1.repositories.run_quality_pipeline", new=mock_pipeline),
            patch(
                "app.api.v1.repositories.find_datasets_by_identity",
                new=AsyncMock(return_value=[]),
            ),
        ):
            client.post(
                "/api/v1/agents/repository-search",
                headers=auth_headers,
                json={"query": "fmri", "filters": _SAMPLE_FILTERS},
            )
        call_filters = mock_aggregate.call_args.kwargs["filters"]
        assert call_filters.raw_query == "fmri"  # body query overrides the dict raw_query


# ---------------------------------------------------------------------------
# POST /api/v1/agents/repository-sync
# ---------------------------------------------------------------------------

class TestRepositorySync:
    def test_missing_secret_returns_422(self, client: TestClient) -> None:
        resp = client.post("/api/v1/agents/repository-sync", json={"source": "dandi"})
        assert resp.status_code == 422

    def test_wrong_secret_returns_401(self, client: TestClient, bad_headers: dict) -> None:
        resp = client.post(
            "/api/v1/agents/repository-sync",
            headers=bad_headers,
            json={"source": "dandi"},
        )
        assert resp.status_code == 401

    def test_correct_secret_returns_per_source_results(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        mock_sync = AsyncMock(
            return_value={
                "dandi": PipelineResult(source="dandi", fetched=3, normalized=3, upserted=3),
            }
        )
        with patch("app.api.v1.repositories.run_repository_sync", new=mock_sync):
            resp = client.post(
                "/api/v1/agents/repository-sync",
                headers=auth_headers,
                json={"source": "dandi", "embed": False},
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["results"]["dandi"]["source"] == "dandi"
        assert body["results"]["dandi"]["upserted"] == 3
        mock_sync.assert_awaited_once_with(source="dandi", limit_per_source=None, embed=False)


# ---------------------------------------------------------------------------
# GET /api/v1/cron/ingest-repositories
# ---------------------------------------------------------------------------

class TestCronIngestRepositories:
    def test_missing_cron_secret_returns_422(self, client: TestClient) -> None:
        resp = client.get("/api/v1/cron/ingest-repositories")
        assert resp.status_code == 422

    def test_wrong_cron_secret_returns_401(self, client: TestClient, bad_cron_headers: dict) -> None:
        resp = client.get("/api/v1/cron/ingest-repositories", headers=bad_cron_headers)
        assert resp.status_code == 401

    def test_correct_cron_secret_runs_full_sync(
        self, client: TestClient, cron_headers: dict
    ) -> None:
        mock_sync = AsyncMock(
            return_value={
                "dandi": PipelineResult(source="dandi", fetched=2, upserted=2),
                "openneuro": PipelineResult(source="openneuro", fetched=1, upserted=0),
            }
        )
        with patch("app.api.v1.repositories.run_repository_sync", new=mock_sync):
            resp = client.get("/api/v1/cron/ingest-repositories", headers=cron_headers)
        assert resp.status_code == 200
        body = resp.json()
        assert set(body["results"]) == {"dandi", "openneuro"}


# ---------------------------------------------------------------------------
# GET /api/v1/agents/repository-health
# ---------------------------------------------------------------------------

class TestRepositoryHealth:
    def test_missing_secret_returns_422(self, client: TestClient) -> None:
        resp = client.get("/api/v1/agents/repository-health")
        assert resp.status_code == 422

    def test_wrong_secret_returns_401(self, client: TestClient, bad_headers: dict) -> None:
        resp = client.get("/api/v1/agents/repository-health", headers=bad_headers)
        assert resp.status_code == 401

    def test_correct_secret_returns_per_source_snapshot(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_collection.count_documents = AsyncMock(return_value=7)
        mock_db.__getitem__ = MagicMock(return_value=mock_collection)

        with (
            patch("app.api.v1.repositories.get_db", return_value=mock_db),
            patch("app.api.v1.repositories.get_circuit_state", return_value="closed"),
            patch(
                "app.api.v1.repositories.get_enabled_sources",
                return_value=["dandi", "openneuro"],
            ),
        ):
            resp = client.get("/api/v1/agents/repository-health", headers=auth_headers)
        assert resp.status_code == 200
        body = resp.json()
        sources = body["sources"]
        assert len(sources) == 2
        assert sources[0]["source"] == "dandi"
        assert sources[0]["circuit_state"] == "closed"
        assert sources[0]["record_count"] == 7
        assert "last_search_at" in sources[0]
        assert "last_sync_at" in sources[0]
