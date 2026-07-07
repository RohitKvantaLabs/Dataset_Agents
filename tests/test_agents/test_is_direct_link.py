"""
Tests for is_direct_link determination in VerificationAgent.

Three cases:
- URL ending in .nii.gz → is_direct_link True, no headers needed.
- HEAD returns text/html, no matching extension → is_direct_link False.
- HEAD returns Content-Disposition: attachment → is_direct_link True.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock

import httpx

from app.agents.fallback_agent import FallbackCandidate
from app.agents.verification_agent import VerificationAgent


def _make_candidate(url: str, source_guess: str = "openneuro") -> FallbackCandidate:
    return FallbackCandidate(
        title="Test",
        url=url,
        source_guess=source_guess,
        reasoning="test",
    )


def _make_agent_with_headers(status: int = 200, content_type: str = "", content_disposition: str = "") -> VerificationAgent:
    """Build a VerificationAgent whose HTTP client returns a response with given headers."""
    resp = MagicMock()
    resp.status_code = status
    resp.headers = {
        "content-type": content_type,
        "content-disposition": content_disposition,
    }
    mock_client = AsyncMock(spec=httpx.AsyncClient)
    mock_client.head = AsyncMock(return_value=resp)
    mock_client.get = AsyncMock(return_value=resp)
    mock_client.aclose = AsyncMock()
    return VerificationAgent(http_client=mock_client)


class TestIsDirectLink:

    @pytest.mark.asyncio
    async def test_nii_gz_extension_is_direct_without_headers(self) -> None:
        """.nii.gz in URL path → is_direct_link True, no header inspection needed."""
        agent = _make_agent_with_headers(status=200, content_type="text/html")
        url = "https://openneuro.org/datasets/ds000001/sub-01/anat/sub-01_T1w.nii.gz"
        result = await agent.verify([_make_candidate(url=url)])
        assert len(result) == 1
        assert result[0].is_direct_link is True

    @pytest.mark.asyncio
    async def test_html_landing_page_no_extension_is_not_direct(self) -> None:
        """text/html response with no neuro extension → is_direct_link False."""
        # Use a known-domain URL so the candidate isn't dropped as unknown-domain landing page.
        agent = _make_agent_with_headers(status=200, content_type="text/html; charset=utf-8")
        url = "https://openneuro.org/datasets/ds000001"
        result = await agent.verify([_make_candidate(url=url, source_guess="openneuro")])
        assert len(result) == 1
        assert result[0].is_direct_link is False

    @pytest.mark.asyncio
    async def test_content_disposition_attachment_is_direct(self) -> None:
        """Content-Disposition: attachment → is_direct_link True regardless of content-type."""
        agent = _make_agent_with_headers(
            status=200,
            content_type="application/octet-stream",
            content_disposition="attachment; filename=data.bin",
        )
        url = "https://openneuro.org/crn/datasets/ds000001/snapshots/1.0.0/files"
        result = await agent.verify([_make_candidate(url=url)])
        assert len(result) == 1
        assert result[0].is_direct_link is True

    @pytest.mark.asyncio
    async def test_bids_marker_url_is_direct(self) -> None:
        """URL containing dataset_description.json → is_direct_link True."""
        agent = _make_agent_with_headers(status=200, content_type="application/json")
        url = "https://openneuro.org/datasets/ds000001/dataset_description.json"
        result = await agent.verify([_make_candidate(url=url)])
        assert len(result) == 1
        assert result[0].is_direct_link is True
