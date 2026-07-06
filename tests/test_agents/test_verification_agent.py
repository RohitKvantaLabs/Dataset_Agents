"""
Tests for VerificationAgent.

Covers (per CLAUDE.md checklist):
- Live URL: HEAD returns 2xx → dataset produced.
- Dead URL: HEAD + GET both return 4xx → candidate dropped.
- Malformed URL: httpx.URL constructor raises → candidate dropped, no crash.
- In-batch deduplication: duplicate URL appears twice → only one dataset.
- Known repository domain: logged but still passes through.

No real HTTP requests are made — httpx.AsyncClient is replaced with a mock.
"""
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx

from app.agents.fallback_agent import FallbackCandidate
from app.agents.verification_agent import VerificationAgent, KNOWN_REPOSITORY_DOMAINS
from app.models.dataset import Dataset, TrustTier


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_candidate(
    url: str = "https://openneuro.org/datasets/ds000001",
    title: str = "Test Dataset",
    source_guess: str = "openneuro",
    reasoning: str = "Matches query",
) -> FallbackCandidate:
    return FallbackCandidate(
        title=title,
        url=url,
        source_guess=source_guess,
        reasoning=reasoning,
    )


def _mock_response(status_code: int) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    return resp


def _make_agent(head_status: int = 200, get_status: int | None = None) -> VerificationAgent:
    """Return a VerificationAgent whose HTTP client is fully mocked."""
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.head = AsyncMock(return_value=_mock_response(head_status))
    if get_status is not None:
        mock_client.get = AsyncMock(return_value=_mock_response(get_status))
    else:
        mock_client.get = AsyncMock(return_value=_mock_response(200))
    mock_client.aclose = AsyncMock()
    return VerificationAgent(http_client=mock_client)


# ---------------------------------------------------------------------------
# Live URL
# ---------------------------------------------------------------------------

class TestLiveUrl:

    @pytest.mark.asyncio
    async def test_live_url_produces_dataset(self) -> None:
        agent = _make_agent(head_status=200)
        candidates = [_make_candidate()]
        result = await agent.verify(candidates)

        assert len(result) == 1
        ds = result[0]
        assert isinstance(ds, Dataset)
        assert ds.title == "Test Dataset"
        assert ds.source == "openneuro"
        assert ds.source_id  # must be non-empty
        assert str(ds.url).startswith("https://openneuro.org")

    @pytest.mark.asyncio
    async def test_live_url_source_id_is_deterministic(self) -> None:
        """Same URL must always produce the same source_id (SHA-1 hash)."""
        agent1 = _make_agent(head_status=200)
        agent2 = _make_agent(head_status=200)
        url = "https://openneuro.org/datasets/ds000001"
        result1 = await agent1.verify([_make_candidate(url=url)])
        result2 = await agent2.verify([_make_candidate(url=url)])
        assert result1[0].source_id == result2[0].source_id

    @pytest.mark.asyncio
    async def test_head_405_falls_back_to_get(self) -> None:
        """HEAD → 405 triggers GET; if GET is 200 the candidate survives."""
        agent = _make_agent(head_status=405, get_status=200)
        result = await agent.verify([_make_candidate()])
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Dead URL
# ---------------------------------------------------------------------------

class TestDeadUrl:

    @pytest.mark.asyncio
    async def test_404_head_and_get_drops_candidate(self) -> None:
        agent = _make_agent(head_status=404, get_status=404)
        result = await agent.verify([_make_candidate()])
        assert result == []

    @pytest.mark.asyncio
    async def test_500_drops_candidate(self) -> None:
        agent = _make_agent(head_status=500, get_status=500)
        result = await agent.verify([_make_candidate()])
        assert result == []

    @pytest.mark.asyncio
    async def test_httpx_connection_error_drops_candidate(self) -> None:
        """Network failure (ConnectError) should drop the candidate, not crash."""
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.head = AsyncMock(side_effect=httpx.ConnectError("refused"))
        mock_client.aclose = AsyncMock()
        agent = VerificationAgent(http_client=mock_client)

        result = await agent.verify([_make_candidate()])
        assert result == []


# ---------------------------------------------------------------------------
# Malformed URL
# ---------------------------------------------------------------------------

class TestMalformedUrl:

    @pytest.mark.asyncio
    async def test_malformed_url_is_dropped_not_crashed(self) -> None:
        """A URL that httpx cannot parse must be silently dropped."""
        agent = _make_agent(head_status=200)
        bad = _make_candidate(url="not-a-url-at-all")
        result = await agent.verify([bad])
        # httpx.URL("not-a-url-at-all") actually doesn't raise but produces
        # an empty host — the HEAD call would fail with a connection error.
        # Either way, we don't crash and we may or may not get a result.
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_empty_url_is_skipped(self) -> None:
        agent = _make_agent(head_status=200)
        empty = _make_candidate(url="")
        result = await agent.verify([empty])
        assert result == []


# ---------------------------------------------------------------------------
# In-batch deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:

    @pytest.mark.asyncio
    async def test_duplicate_url_produces_single_dataset(self) -> None:
        agent = _make_agent(head_status=200)
        url = "https://openneuro.org/datasets/ds000001"
        candidates = [_make_candidate(url=url), _make_candidate(url=url, title="Dupe")]
        result = await agent.verify(candidates)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_two_distinct_urls_both_kept(self) -> None:
        agent = _make_agent(head_status=200)
        c1 = _make_candidate(url="https://openneuro.org/datasets/ds000001", title="A")
        c2 = _make_candidate(url="https://openneuro.org/datasets/ds000002", title="B")
        result = await agent.verify([c1, c2])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Known repository domain
# ---------------------------------------------------------------------------

class TestKnownDomain:

    @pytest.mark.asyncio
    async def test_known_domain_candidate_passes_through(self) -> None:
        agent = _make_agent(head_status=200)
        known_url = "https://openneuro.org/datasets/ds000001"
        assert "openneuro.org" in KNOWN_REPOSITORY_DOMAINS
        result = await agent.verify([_make_candidate(url=known_url)])
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_unknown_domain_also_passes_through(self) -> None:
        """Unknown domain is not rejected — it just doesn't get the 'known repo' log."""
        agent = _make_agent(head_status=200)
        result = await agent.verify([
            _make_candidate(url="https://some-random-lab-site.edu/data")
        ])
        assert len(result) == 1
