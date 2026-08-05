"""
EBRAINS connector tests — current KG Core Query API (Issue 3).

Verifies:
- The connector targets ``https://core.kg.ebrains.eu/v3/queries`` (the retired
  ``kg.ebrains.eu`` endpoint is gone).
- Missing ``EBRAINS_API_KEY`` → ``offline`` status with a reason (no silent skip).
- Bearer authentication is attached when the key is configured.
- JSON-LD query payload (meta.type DatasetVersion + documented REGEX filters).
- Pagination via ``from``/``size`` and the ``data``/``total`` response shape.
- Normalization of JSON-LD instance docs (expanded IRIs and compact keys).
"""
import pytest
from unittest.mock import AsyncMock, patch

import httpx

from app.config import Settings
from app.connectors.base import SearchRequest
from app.connectors.ebrains_connector import (
    EBRAINSConnector,
    EBRAINS_QUERY_TEMPLATE,
    EBRAINS_QUERY_URL,
    _build_query_payload,
)
from app.ingestion.normalizer import _repository_ebrains
from app.models.query_filters import QueryFilters

QUERY_URL = "https://core.kg.ebrains.eu/v3/queries"


def _settings(**overrides) -> Settings:
    base = dict(
        INTERNAL_API_SECRET="test-secret",
        CRON_SECRET="test-cron-secret",
        GROQ_API_KEY="gsk_test",
        TAVILY_API_KEY="tvly-test",
        MONGO_URI="mongodb://localhost:27017",
        REDIS_URL="redis://localhost:6379/0",
    )
    base.update(overrides)
    return Settings(**base)


def _json_response(payload: dict, status: int = 200) -> httpx.Response:
    return httpx.Response(status, json=payload, request=httpx.Request("POST", QUERY_URL))


def _instance_doc(
    uuid: str = "00000000-0000-0000-0000-000000000001",
    title: str = "MEG Parkinson Dataset",
) -> dict:
    return {
        "@id": f"https://core.kg.ebrains.eu/instances/{uuid}",
        "@type": ["https://openminds.ebrains.eu/core/DatasetVersion"],
        "https://openminds.ebrains.eu/vocab/fullName": title,
        "https://openminds.ebrains.eu/vocab/description": (
            "Magnetoencephalography recordings from Parkinson's disease patients."
        ),
        "https://openminds.ebrains.eu/vocab/firstReleasedAt": "2023-11-17T00:00:00.000Z",
        "https://openminds.ebrains.eu/vocab/custodian": [
            {"https://openminds.ebrains.eu/vocab/fullName": "Lundqvist, D."}
        ],
        "https://openminds.ebrains.eu/vocab/license": [
            {"@id": "https://core.kg.ebrains.eu/instances/00000000-0000-0000-0000-00000000aaaa"}
        ],
    }


def _req(query: str = "Find Parkinson disease MEG datasets", limit: int = 10) -> SearchRequest:
    return SearchRequest(query=query, filters=QueryFilters(raw_query=query), limit=limit)


# ---------------------------------------------------------------------------
# Query payload construction
# ---------------------------------------------------------------------------


class TestQueryPayload:
    def test_endpoint_is_current_kg_core_query_api(self) -> None:
        assert EBRAINS_QUERY_URL == QUERY_URL
        # The retired endpoint host (https://kg.ebrains.eu/...) is gone.
        assert not EBRAINS_QUERY_URL.startswith("https://kg.ebrains.eu")

    def test_template_targets_openminds_dataset_version(self) -> None:
        assert EBRAINS_QUERY_TEMPLATE["meta"]["type"] == "https://openminds.ebrains.eu/core/DatasetVersion"

    def test_filters_attached_only_to_text_properties(self) -> None:
        payload = _build_query_payload("MEG Parkinson disease")
        filtered = [e for e in payload["structure"] if "filter" in e]
        assert len(filtered) == 2
        for entry in filtered:
            assert entry["filter"]["op"] == "REGEX"
            assert entry["path"].endswith("fullName") or entry["path"].endswith("description")

    def test_no_terms_no_filters(self) -> None:
        assert _build_query_payload("") == EBRAINS_QUERY_TEMPLATE


# ---------------------------------------------------------------------------
# Missing API key → offline with reason (Issue 3, §2.7)
# ---------------------------------------------------------------------------


class TestNoApiKey:
    @pytest.mark.asyncio
    async def test_offline_with_reason(self) -> None:
        # Patch the connector module's own get_settings binding (it imports
        # ``from app.config import get_settings`` at module load).
        with patch("app.connectors.ebrains_connector.get_settings", return_value=_settings()):
            conn = EBRAINSConnector(http_client=AsyncMock(spec=httpx.AsyncClient))
        result = await conn.search(_req())
        assert result.status == "offline"
        assert result.error == "EBRAINS_API_KEY not configured"
        assert result.records == []


# ---------------------------------------------------------------------------
# Search against the current Query API
# ---------------------------------------------------------------------------


class TestSearch:
    @pytest.mark.asyncio
    async def test_bearer_authentication_attached(self) -> None:
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        with (
            patch("app.connectors.ebrains_connector.get_settings", return_value=_settings(EBRAINS_API_KEY="tok-123")),
            patch("httpx.AsyncClient", return_value=mock_client) as factory,
        ):
            EBRAINSConnector()
        assert factory.call_args.kwargs["headers"]["Authorization"] == "Bearer tok-123"

    @pytest.mark.asyncio
    async def test_current_endpoint_params_and_normalization(self) -> None:
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post = AsyncMock(
            return_value=_json_response({"data": [_instance_doc()], "total": 1, "size": 25, "from": 0})
        )
        with patch("app.connectors.ebrains_connector.get_settings", return_value=_settings(EBRAINS_API_KEY="tok")):
            conn = EBRAINSConnector(http_client=mock_client)

        result = await conn.search(_req())
        assert result.status == "ok"
        assert result.total_available == 1
        assert len(result.records) == 1

        call = mock_client.post.await_args
        assert str(call.args[0]) == QUERY_URL
        assert call.kwargs["params"]["from"] == 0
        assert call.kwargs["params"]["size"] == 10
        assert call.kwargs["params"]["stage"] == "RELEASED"
        assert call.kwargs["params"]["returnTotalResults"] == "true"
        assert call.kwargs["json"]["meta"]["type"] == "https://openminds.ebrains.eu/core/DatasetVersion"

        ds = result.records[0]
        assert ds.source == "ebrains"
        assert ds.source_id == "00000000-0000-0000-0000-000000000001"
        assert ds.url == "https://search.kg.ebrains.eu/instances/00000000-0000-0000-0000-000000000001"
        assert ds.title == "MEG Parkinson Dataset"
        assert ds.description and "Magnetoencephalography" in ds.description
        assert ds.authors == ["Lundqvist, D."]
        assert ds.published_at is not None
        assert ds.license  # license instance IRI preserved, never fabricated

    @pytest.mark.asyncio
    async def test_pagination_uses_from_size(self) -> None:
        mock_client = AsyncMock(spec=httpx.AsyncClient)

        def page_docs(offset: int) -> list:
            return [_instance_doc(uuid=f"...{offset + i:03d}", title=f"DS {offset + i}") for i in range(25)]

        mock_client.post = AsyncMock(
            side_effect=[
                _json_response({"data": page_docs(0), "total": 50, "size": 25, "from": 0}),
                _json_response({"data": page_docs(25), "total": 50, "size": 25, "from": 25}),
            ]
        )
        with patch("app.connectors.ebrains_connector.get_settings", return_value=_settings(EBRAINS_API_KEY="tok")):
            conn = EBRAINSConnector(http_client=mock_client)

        result = await conn.search(_req(limit=50))
        assert result.status == "ok"
        assert len(result.records) == 50
        assert result.truncated is False
        offsets = [c.kwargs["params"]["from"] for c in mock_client.post.call_args_list]
        assert offsets == [0, 25]

    @pytest.mark.asyncio
    async def test_truncated_flag_when_more_available(self) -> None:
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post = AsyncMock(
            return_value=_json_response(
                {"data": [_instance_doc(uuid="...001")], "total": 500, "size": 25, "from": 0}
            )
        )
        with patch("app.connectors.ebrains_connector.get_settings", return_value=_settings(EBRAINS_API_KEY="tok")):
            conn = EBRAINSConnector(http_client=mock_client)
        result = await conn.search(_req(limit=1))
        assert result.status == "ok"
        assert len(result.records) == 1
        assert result.truncated is True

    @pytest.mark.asyncio
    async def test_filter_rejected_falls_back_to_unfiltered_query(self) -> None:
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        bad = httpx.Response(400, request=httpx.Request("POST", QUERY_URL))
        good = _json_response({"data": [_instance_doc()], "total": 1, "size": 25, "from": 0})
        mock_client.post = AsyncMock(side_effect=[bad, good])
        with patch("app.connectors.ebrains_connector.get_settings", return_value=_settings(EBRAINS_API_KEY="tok")):
            conn = EBRAINSConnector(http_client=mock_client)

        result = await conn.search(_req())
        assert result.status == "ok"
        assert len(result.records) == 1
        bodies = [c.kwargs["json"] for c in mock_client.post.call_args_list]
        assert bodies[1] == EBRAINS_QUERY_TEMPLATE  # retried without filters


# ---------------------------------------------------------------------------
# Normalizer — JSON-LD instance docs (expanded IRIs and compact keys)
# ---------------------------------------------------------------------------


class TestNormalizerEbrains:
    def test_expanded_iri_keys(self) -> None:
        ds = _repository_ebrains(_instance_doc())
        assert ds is not None
        assert ds.source == "ebrains"
        assert ds.title == "MEG Parkinson Dataset"
        assert ds.authors == ["Lundqvist, D."]
        assert ds.url == "https://search.kg.ebrains.eu/instances/00000000-0000-0000-0000-000000000001"

    def test_compact_keys(self) -> None:
        raw = {
            "@id": "https://core.kg.ebrains.eu/instances/abc123",
            "fullName": "Compact Title",
            "description": "A compact description",
            "custodian": [{"fullName": "Doe, J."}],
            "firstReleasedAt": "2023-11-17",
        }
        ds = _repository_ebrains(raw)
        assert ds is not None
        assert ds.title == "Compact Title"
        assert ds.description == "A compact description"
        assert ds.authors == ["Doe, J."]
        assert ds.published_at is not None

    def test_missing_id_dropped(self) -> None:
        assert _repository_ebrains({}) is None

    def test_dataset_never_downgraded_to_paper(self) -> None:
        """Issue 4 — a Dataset(DatasetVersion) instance must normalize as a
        dataset regardless of any related publication; the classifier reads the
        repo-native @type declaration and keeps it a dataset."""
        from app.ingestion.quality_pipeline import _repo_native_class, _classify_content_type
        from app.ingestion.quality_pipeline import _Record
        from app.ingestion.quality_pipeline import WEB_DISCOVERY_SOURCE

        raw = _instance_doc(
            uuid="natmeg-pd",
            title="The Swedish National Facility for Magnetoencephalography Parkinson's Disease Dataset",
        )
        assert _repo_native_class(raw) == "dataset"
        ds = _repository_ebrains(raw)
        assert ds is not None and ds.source == "ebrains"
        rec = _Record(candidate=ds)
        # A repo-origin EBRAINS record must classify as dataset, never paper.
        assert _classify_content_type(rec, _settings()) == "dataset"
        assert WEB_DISCOVERY_SOURCE == "web_search"  # sanity: constant unchanged
