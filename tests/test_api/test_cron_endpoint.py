from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient

from app.models.dataset import Dataset, TrustTier


def _dataset(source_id: str) -> Dataset:
    return Dataset(
        title=f"Dataset {source_id}",
        source="openneuro",
        source_id=source_id,
        url=f"https://openneuro.org/datasets/{source_id}",
        trust_tier=TrustTier.VERIFIED,
    )


def test_reverify_links_missing_auth_returns_422(client: TestClient) -> None:
    response = client.get("/api/v1/cron/reverify-links")
    assert response.status_code == 422


def test_reverify_links_wrong_auth_returns_401(client: TestClient, bad_cron_headers: dict) -> None:
    response = client.get("/api/v1/cron/reverify-links", headers=bad_cron_headers)
    assert response.status_code == 401


def test_reverify_links_with_correct_auth_returns_counts(
    client: TestClient, cron_headers: dict
) -> None:
    datasets = [_dataset("ds1"), _dataset("ds2")]
    verifier = MagicMock()
    verifier.revalidate = AsyncMock(side_effect=[TrustTier.VERIFIED, TrustTier.STALE])
    update_status = AsyncMock()

    with (
        patch("app.api.v1.cron.find_datasets_for_reverification", new=AsyncMock(return_value=datasets)),
        patch("app.api.v1.cron.VerificationAgent", return_value=verifier),
        patch("app.api.v1.cron.bulk_update_verification_status", new=update_status),
    ):
        response = client.get("/api/v1/cron/reverify-links", headers=cron_headers)

    assert response.status_code == 200
    assert response.json() == {"checked": 2, "verified": 1, "stale": 1, "errors": 0}
    assert verifier.revalidate.await_count == 2
    assert update_status.await_count == 1
