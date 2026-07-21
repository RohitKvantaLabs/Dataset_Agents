"""
Tests for the two Node-facing endpoints:
  POST /api/v1/agents/parse-query
  POST /api/v1/agents/fallback-search

Covers (per CLAUDE.md checklist):
- Both endpoints reject requests without X-Internal-Secret → 401.
- Both endpoints accept requests with the correct secret → 200.
- parse-query response shape matches NODE_INTEGRATION_CONTRACT.md exactly.
- fallback-search response shape matches the contract.
- fallback-search works with empty candidate list (no Mongo/Redis errors).

All LLM calls, Mongo writes, Redis publishes, and HTTP link-checks are
mocked — no real I/O in these tests.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

# conftest.py provides `client` and `auth_headers` fixtures


# ---------------------------------------------------------------------------
# POST /api/v1/agents/parse-query
# ---------------------------------------------------------------------------

class TestParseQueryAuth:

    def test_missing_secret_returns_422(self, client: TestClient) -> None:
        # FastAPI validates required Header fields before the dependency runs,
        # so a completely absent X-Internal-Secret is a 422 validation error.
        resp = client.post(
            "/api/v1/agents/parse-query",
            json={"query": "fMRI in adults"},
        )
        assert resp.status_code == 422

    def test_wrong_secret_returns_401(self, client: TestClient, bad_headers: dict) -> None:
        resp = client.post(
            "/api/v1/agents/parse-query",
            headers=bad_headers,
            json={"query": "fMRI in adults"},
        )
        assert resp.status_code == 401

    def test_correct_secret_returns_200(self, client: TestClient, auth_headers: dict) -> None:
        resp = client.post(
            "/api/v1/agents/parse-query",
            headers=auth_headers,
            json={"query": "resting state fMRI in kids with ADHD"},
        )
        assert resp.status_code == 200


class TestParseQueryResponse:
    """Response shape must match NODE_INTEGRATION_CONTRACT.md exactly."""

    def test_response_has_filters_key(self, client: TestClient, auth_headers: dict) -> None:
        resp = client.post(
            "/api/v1/agents/parse-query",
            headers=auth_headers,
            json={"query": "fMRI human adults"},
        )
        body = resp.json()
        assert "filters" in body

    def test_filters_has_all_contract_fields(self, client: TestClient, auth_headers: dict) -> None:
        """Every field in the contract must be present (even if empty/null)."""
        resp = client.post(
            "/api/v1/agents/parse-query",
            headers=auth_headers,
            json={"query": "resting state fMRI in kids with ADHD"},
        )
        filters = resp.json()["filters"]
        # Fields specified in NODE_INTEGRATION_CONTRACT.md
        for field in ("modality", "species", "age_range", "condition", "task", "format", "keywords", "raw_query"):
            assert field in filters, f"Missing field: {field}"

    def test_raw_query_is_echoed(self, client: TestClient, auth_headers: dict) -> None:
        query = "resting state fMRI ADHD pediatric"
        resp = client.post(
            "/api/v1/agents/parse-query",
            headers=auth_headers,
            json={"query": query},
        )
        assert resp.json()["filters"]["raw_query"] == query

    def test_heuristic_modality_detection(self, client: TestClient, auth_headers: dict) -> None:
        """In no-LLM mode the heuristic parser should detect fmri."""
        resp = client.post(
            "/api/v1/agents/parse-query",
            headers=auth_headers,
            json={"query": "fmri resting state dataset"},
        )
        filters = resp.json()["filters"]
        assert "fmri" in filters["modality"]

    def test_query_too_short_returns_422(self, client: TestClient, auth_headers: dict) -> None:
        """min_length=2 on the query field — single char should 422."""
        resp = client.post(
            "/api/v1/agents/parse-query",
            headers=auth_headers,
            json={"query": "x"},
        )
        assert resp.status_code == 422

    def test_query_too_long_returns_422(self, client: TestClient, auth_headers: dict) -> None:
        resp = client.post(
            "/api/v1/agents/parse-query",
            headers=auth_headers,
            json={"query": "a" * 501},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/v1/agents/fallback-search
# ---------------------------------------------------------------------------

# We need to patch Mongo upsert and Redis publish so no real I/O fires.
MOCK_PATCHES = [
    "app.api.v1.agents.upsert_many",
    "app.api.v1.agents.publish_fallback_result",
    "app.agents.verification_agent.VerificationAgent.verify",
    "app.agents.fallback_agent.FallbackAgent.discover",
]

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

_FALLBACK_PAYLOAD = {
    "query_id": "sess_abc123",
    "query": "resting state fMRI in kids with ADHD",
    "filters": _SAMPLE_FILTERS,
}


class TestFallbackSearchAuth:

    def test_missing_secret_returns_422(self, client: TestClient) -> None:
        # Header absent entirely → FastAPI validation error (422)
        resp = client.post(
            "/api/v1/agents/fallback-search",
            json=_FALLBACK_PAYLOAD,
        )
        assert resp.status_code == 422

    def test_wrong_secret_returns_401(self, client: TestClient, bad_headers: dict) -> None:
        resp = client.post(
            "/api/v1/agents/fallback-search",
            headers=bad_headers,
            json=_FALLBACK_PAYLOAD,
        )
        assert resp.status_code == 401


class TestFallbackSearchResponse:
    """Response shape and behaviour with all I/O mocked."""

    def test_correct_secret_with_zero_candidates_returns_200(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        with (
            patch("app.agents.fallback_agent.FallbackAgent.discover", new=AsyncMock(return_value=[])),
            patch("app.agents.verification_agent.VerificationAgent.verify", new=AsyncMock(return_value=[])),
            patch("app.services.redis_publisher.get_redis"),
            patch("app.api.v1.agents.upsert_many", new=AsyncMock(return_value=0)),
            patch("app.api.v1.agents.publish_fallback_result", new=AsyncMock()),
        ):
            resp = client.post(
                "/api/v1/agents/fallback-search",
                headers=auth_headers,
                json=_FALLBACK_PAYLOAD,
            )
        assert resp.status_code == 200
        body = resp.json()
        assert body["query_id"] == "sess_abc123"
        assert body["datasets_found"] == 0
        assert body["published"] is True
        assert body["datasets"] == []

    def test_response_shape_matches_contract(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        """NODE_INTEGRATION_CONTRACT §2 response: query_id, datasets_found, published."""
        with (
            patch("app.agents.fallback_agent.FallbackAgent.discover", new=AsyncMock(return_value=[])),
            patch("app.agents.verification_agent.VerificationAgent.verify", new=AsyncMock(return_value=[])),
            patch("app.api.v1.agents.upsert_many", new=AsyncMock(return_value=0)),
            patch("app.api.v1.agents.publish_fallback_result", new=AsyncMock()),
        ):
            resp = client.post(
                "/api/v1/agents/fallback-search",
                headers=auth_headers,
                json=_FALLBACK_PAYLOAD,
            )
        body = resp.json()
        for field in ("query_id", "datasets_found", "published", "datasets"):
            assert field in body, f"Missing contract field: {field}"

    def test_upsert_not_called_when_no_datasets(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        """upsert_many must only be called when there are verified datasets."""
        mock_upsert = AsyncMock(return_value=0)
        with (
            patch("app.agents.fallback_agent.FallbackAgent.discover", new=AsyncMock(return_value=[])),
            patch("app.agents.verification_agent.VerificationAgent.verify", new=AsyncMock(return_value=[])),
            patch("app.api.v1.agents.upsert_many", new=mock_upsert),
            patch("app.api.v1.agents.publish_fallback_result", new=AsyncMock()),
        ):
            client.post(
                "/api/v1/agents/fallback-search",
                headers=auth_headers,
                json=_FALLBACK_PAYLOAD,
            )
        mock_upsert.assert_not_called()

    def test_redis_always_published_even_with_empty_results(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        """Redis must be published once regardless of dataset count
        so Node always gets a 'nothing found' signal rather than silence."""
        mock_publish = AsyncMock()
        with (
            patch("app.agents.fallback_agent.FallbackAgent.discover", new=AsyncMock(return_value=[])),
            patch("app.agents.verification_agent.VerificationAgent.verify", new=AsyncMock(return_value=[])),
            patch("app.api.v1.agents.upsert_many", new=AsyncMock(return_value=0)),
            patch("app.api.v1.agents.publish_fallback_result", new=mock_publish),
        ):
            client.post(
                "/api/v1/agents/fallback-search",
                headers=auth_headers,
                json=_FALLBACK_PAYLOAD,
            )
        mock_publish.assert_called_once()
        call_args = mock_publish.call_args
        assert call_args[0][0] == "sess_abc123"   # first positional = query_id

    def test_missing_query_id_returns_422(
        self, client: TestClient, auth_headers: dict
    ) -> None:
        resp = client.post(
            "/api/v1/agents/fallback-search",
            headers=auth_headers,
            json={"query": "fmri", "filters": _SAMPLE_FILTERS},  # no query_id
        )
        assert resp.status_code == 422
