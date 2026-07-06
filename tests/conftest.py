"""
Shared test fixtures.

Design rules (from CLAUDE.md):
- Never hit real HuggingFace, Tavily, MongoDB, or Redis in tests.
- Override INTERNAL_API_SECRET by patching get_settings in every module
  that imports it directly (security.py, agents, etc.).
- Mock Motor with AsyncMock so async collection operations don't crash.
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch

from app.config import get_settings, Settings

# ---------------------------------------------------------------------------
# A fixed secret used in every test that needs auth headers.
# ---------------------------------------------------------------------------
TEST_SECRET = "test-secret-do-not-use-in-prod"
TEST_CRON_SECRET = "test-cron-secret-do-not-use-in-prod"


def _make_test_settings() -> Settings:
    """Return Settings with test-safe values (no real keys)."""
    return Settings(
        INTERNAL_API_SECRET=TEST_SECRET,
        CRON_SECRET=TEST_CRON_SECRET,
        HF_TOKEN="hf_test",
        TAVILY_API_KEY="tvly-test",
        MONGO_URI="mongodb://localhost:27017",
        REDIS_URL="redis://localhost:6379/0",
    )


def _make_mock_db() -> MagicMock:
    """
    Return a MagicMock that behaves like an AsyncIOMotorDatabase.
    Collection operations that are awaited need to be AsyncMock.
    """
    mock_collection = MagicMock()
    mock_collection.create_index = AsyncMock(return_value="index_name")
    mock_collection.find_one_and_update = AsyncMock(return_value=None)

    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    return mock_db


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    return _make_test_settings()


@pytest.fixture(scope="session")
def client(test_settings: Settings) -> TestClient:
    """
    FastAPI TestClient with:
    - get_settings lru_cache cleared and replaced with test settings everywhere.
    - MongoDB patched with AsyncMock so startup index creation doesn't crash.
    """
    # Clear lru_cache so our settings are used from the first call.
    get_settings.cache_clear()

    mock_db = _make_mock_db()
    mock_motor_client = MagicMock()
    mock_motor_client.close = MagicMock()

    with (
        patch("app.config.get_settings", return_value=test_settings),
        patch("app.core.security.get_settings", return_value=test_settings),
        patch("app.agents.verification_agent.get_settings", return_value=test_settings),
        patch("app.agents.search_provider.get_settings", return_value=test_settings),
        patch("app.agents.query_understanding_agent.LLMClient", side_effect=ValueError("mocked no llm")),
        patch("app.api.v1.agents.get_settings", return_value=test_settings),
        patch("app.api.v1.cron.get_settings", return_value=test_settings),
        # Patch Motor so no real connection is attempted.
        patch("app.db.indexes.get_db", return_value=mock_db),
        patch("app.db.mongo.get_client", return_value=mock_motor_client),
        patch("app.db.mongo.get_db", return_value=mock_db),
    ):
        from app.main import create_app
        app = create_app()
        app.dependency_overrides[get_settings] = lambda: test_settings

        with TestClient(app, raise_server_exceptions=False) as c:
            yield c

    get_settings.cache_clear()


@pytest.fixture()
def auth_headers() -> dict[str, str]:
    return {"X-Internal-Secret": TEST_SECRET}


@pytest.fixture()
def bad_headers() -> dict[str, str]:
    return {"X-Internal-Secret": "wrong-secret"}


@pytest.fixture()
def cron_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {TEST_CRON_SECRET}"}


@pytest.fixture()
def bad_cron_headers() -> dict[str, str]:
    return {"Authorization": "Bearer wrong-secret"}
