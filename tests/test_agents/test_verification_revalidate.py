from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from app.agents.verification_agent import VerificationAgent
from app.models.dataset import Dataset, TrustTier


def _mock_response(status_code: int) -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    return response


def _agent(head_status: int, get_status: int | None = None) -> VerificationAgent:
    client = AsyncMock(spec=httpx.AsyncClient)
    client.head = AsyncMock(return_value=_mock_response(head_status))
    client.get = AsyncMock(return_value=_mock_response(get_status or 200))
    client.aclose = AsyncMock()
    return VerificationAgent(http_client=client)


def _dataset() -> Dataset:
    return Dataset(
        title="Dataset",
        source="openneuro",
        source_id="ds1",
        url="https://openneuro.org/datasets/ds1",
    )


@pytest.mark.asyncio
async def test_live_dataset_revalidates_as_verified() -> None:
    assert await _agent(head_status=200).revalidate(_dataset()) == TrustTier.VERIFIED


@pytest.mark.asyncio
async def test_dead_dataset_revalidates_as_stale() -> None:
    assert await _agent(head_status=404, get_status=404).revalidate(_dataset()) == TrustTier.STALE
