from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from app.agents.search_provider import TavilySearchProvider


def _response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.raise_for_status = MagicMock()
    response.json = MagicMock(return_value=payload)
    return response


@pytest.mark.asyncio
async def test_tavily_success_maps_results() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(
        return_value=_response(
            {
                "results": [
                    {"title": "OpenNeuro", "url": "https://openneuro.org/datasets/ds1", "content": "fMRI"},
                    {"title": "Missing URL", "content": "ignored"},
                ]
            }
        )
    )

    provider = TavilySearchProvider(http_client=client)
    results = await provider.search("fmri", max_results=5)

    assert results == [
        {
            "title": "OpenNeuro",
            "url": "https://openneuro.org/datasets/ds1",
            "snippet": "fMRI",
        }
    ]
    client.post.assert_awaited_once()


@pytest.mark.asyncio
async def test_tavily_timeout_returns_empty_list() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(side_effect=httpx.TimeoutException("slow"))

    provider = TavilySearchProvider(http_client=client)
    assert await provider.search("fmri") == []


@pytest.mark.asyncio
async def test_tavily_malformed_response_returns_empty_list() -> None:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.post = AsyncMock(return_value=_response({"results": {"url": "not-a-list"}}))

    provider = TavilySearchProvider(http_client=client)
    assert await provider.search("fmri") == []
