"""
Quality pipeline — web-origin candidate handling (§3.0 + approved decision).

Covers the stabilization decision:
- Web candidates (source="web_search") are NOT rejected by Stage 1's repository
  allowlist; blocklist and required-field checks still apply.
- Stage 4 derives trust from the verified destination URL + validated metadata,
  never from source_guess: direct data links pass; landing pages on supported
  repository domains are promoted to repository datasets; unknown-domain landing
  pages are dropped.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.config import Settings
from app.ingestion.quality_pipeline import (
    WEB_DISCOVERY_SOURCE,
    _Record,
    _classify_content_type,
    _is_repository_landing_page,
    _known_domain_host,
    _repo_identity_from_url,
    _repo_native_class,
    _stage_filter,
    _stage_verify,
    run_quality_pipeline,
)
from app.models.repository_dataset import RepositoryDataset

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _web_candidate(
    url: str = "https://example.edu/data",
    title: str = "Web dataset",
    source_id: str = "sha1abc123",
) -> RepositoryDataset:
    return RepositoryDataset(
        source=WEB_DISCOVERY_SOURCE,
        source_id=source_id,
        url=url,
        title=title,
        raw={"source_guess": "openneuro", "discovery": "web"},
    )


def _record(candidate: RepositoryDataset) -> _Record:
    return _Record(candidate=candidate)


def _live_response() -> MagicMock:
    """A link-check response that is live and performs no redirect."""
    resp = MagicMock()
    resp.url = None
    return resp


# ---------------------------------------------------------------------------
# Stage 1 — web-origin passthrough (blocklist + required fields only)
# ---------------------------------------------------------------------------


class TestStage1WebOrigin:
    def test_web_candidate_outside_allowlist_accepted(self) -> None:
        settings = _settings()
        kept, stats = _stage_filter([_record(_web_candidate(url="https://example.edu/data"))], settings)
        assert stats.accepted == 1
        assert kept[0].candidate.source == WEB_DISCOVERY_SOURCE

    def test_web_candidate_on_repository_domain_accepted(self) -> None:
        settings = _settings()
        kept, _ = _stage_filter(
            [_record(_web_candidate(url="https://openneuro.org/datasets/ds000001"))], settings
        )
        assert len(kept) == 1

    def test_web_candidate_blocklisted_dropped(self) -> None:
        settings = _settings(REPOSITORY_BLOCKLIST=["example.edu"])
        _, stats = _stage_filter([_record(_web_candidate(url="https://example.edu/data"))], settings)
        assert stats.accepted == 0
        assert stats.dropped.get("blocklisted") == 1

    def test_web_candidate_missing_required_fields_dropped(self) -> None:
        settings = _settings()
        bad = _web_candidate(url="")
        _, stats = _stage_filter([_record(bad)], settings)
        assert stats.accepted == 0
        assert stats.dropped.get("missing_required") == 1

    def test_repository_candidate_still_allowlisted(self) -> None:
        settings = _settings()
        repo = RepositoryDataset(
            source="openneuro", source_id="ds000001",
            url="https://openneuro.org/datasets/ds000001", title="Repo dataset",
        )
        _, stats = _stage_filter([_record(repo)], settings)
        assert stats.accepted == 1

    def test_repository_candidate_outside_allowlist_still_dropped(self) -> None:
        settings = _settings()
        repo = RepositoryDataset(
            source="openneuro", source_id="x",
            url="https://random-lab.example/dataset", title="Repo dataset",
        )
        _, stats = _stage_filter([_record(repo)], settings)
        assert stats.accepted == 0
        assert stats.dropped.get("domain_not_allowlisted") == 1


# ---------------------------------------------------------------------------
# URL → repository identity extraction
# ---------------------------------------------------------------------------


class TestRepoIdentityFromUrl:
    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://openneuro.org/datasets/ds000001", ("openneuro", "ds000001")),
            ("https://www.openneuro.org/datasets/ds000002", ("openneuro", "ds000002")),
            ("https://dandiarchive.org/dandiset/3", ("dandi", "DANDI:000003")),
            ("https://dandiarchive.org/dandiset/000003", ("dandi", "DANDI:000003")),
            ("https://neurovault.org/collections/42/", ("neurovault", "42")),
            ("https://zenodo.org/records/12345", ("zenodo", "12345")),
            ("https://datadryad.org/stash/dataset/10.5061/dryad.abc123", ("dryad", "dryad.abc123")),
            ("https://osf.io/abcd1234/", ("osf", "abcd1234")),
            ("https://www.nitrc.org/projects/abcd", ("nitrc", "abcd")),
            ("https://example.edu/data", None),
            ("https://openneuro.org/not-a-dataset-path", ("openneuro", "")),
        ],
    )
    def test_extraction(self, url: str, expected) -> None:
        assert _repo_identity_from_url(url) == expected


# ---------------------------------------------------------------------------
# Stage 4 — trust from verified destination URL, web→repo promotion
# ---------------------------------------------------------------------------


class TestStage4WebTrust:
    @pytest.mark.asyncio
    async def test_web_candidate_on_repo_domain_promoted_to_repository(self) -> None:
        rec = _record(_web_candidate(url="https://openneuro.org/datasets/ds000001"))
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "openneuro.org", _live_response())),
            ),
        ):
            kept, stats = await _stage_verify([rec], _settings())
        assert stats.accepted == 1
        assert kept[0].candidate.source == "openneuro"
        assert kept[0].candidate.source_id == "ds000001"

    @pytest.mark.asyncio
    async def test_web_candidate_dandi_url_gets_native_id(self) -> None:
        rec = _record(_web_candidate(url="https://dandiarchive.org/dandiset/3"))
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "dandiarchive.org", _live_response())),
            ),
        ):
            kept, _ = await _stage_verify([rec], _settings())
        assert kept[0].candidate.source == "dandi"
        assert kept[0].candidate.source_id == "DANDI:000003"

    @pytest.mark.asyncio
    async def test_web_unknown_domain_landing_page_dropped(self) -> None:
        """source_guess must NOT grant trust — an unknown-domain landing page
        with a guess is still dropped (trust comes from verified URL only)."""
        rec = _record(_web_candidate(url="https://example.edu/data"))
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "example.edu", _live_response())),
            ),
        ):
            _, stats = await _stage_verify([rec], _settings())
        assert stats.accepted == 0
        assert stats.dropped.get("unverifiable") == 1

    @pytest.mark.asyncio
    async def test_web_direct_data_link_unknown_domain_passes(self) -> None:
        rec = _record(_web_candidate(url="https://example.edu/sub-01_T1w.nii.gz"))
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "example.edu", _live_response())),
            ),
        ):
            kept, stats = await _stage_verify([rec], _settings())
        assert stats.accepted == 1
        assert kept[0].candidate.source == WEB_DISCOVERY_SOURCE  # stays web-discovered
        assert kept[0].is_direct_link is True

    @pytest.mark.asyncio
    async def test_dead_web_link_dropped(self) -> None:
        rec = _record(_web_candidate(url="https://openneuro.org/datasets/ds000001"))
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(False, "openneuro.org", None)),
            ),
        ):
            _, stats = await _stage_verify([rec], _settings())
        assert stats.accepted == 0
        assert stats.dropped.get("dead_link") == 1

    @pytest.mark.asyncio
    async def test_repository_homepage_never_promoted(self) -> None:
        """Issue 2 — the OpenNeuro homepage (https://openneuro.org/) is a
        landing page, NOT a dataset. It must be dropped at Stage 4 and can
        never be promoted to a repository dataset / reach persistence."""
        rec = _record(_web_candidate(url="https://openneuro.org/", title="OpenNeuro"))
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "openneuro.org", _live_response())),
            ),
        ):
            kept, stats = await _stage_verify([rec], _settings())
        assert stats.accepted == 0
        assert stats.dropped.get("repo_landing_page") == 1
        assert kept == []

    @pytest.mark.asyncio
    async def test_repository_search_and_support_pages_dropped(self) -> None:
        rec = _record(_web_candidate(url="https://openneuro.org/about", title="About OpenNeuro"))
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "openneuro.org", _live_response())),
            ),
        ):
            _, stats = await _stage_verify([rec], _settings())
        assert stats.accepted == 0
        assert stats.dropped.get("repo_landing_page") == 1


# ---------------------------------------------------------------------------
# End-to-end: web candidate through the full pipeline (publish=True)
# ---------------------------------------------------------------------------


class TestWebCandidateFullPipeline:
    @pytest.mark.asyncio
    async def test_web_candidate_published_as_repository_dataset(self) -> None:
        web = _web_candidate(url="https://openneuro.org/datasets/ds000001")
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "openneuro.org", _live_response())),
            ),
            patch("app.ingestion.quality_pipeline.bulk_upsert", new=AsyncMock(return_value=1)),
        ):
            result = await run_quality_pipeline([web], publish=True)
        assert len(result.datasets) == 1
        ds = result.datasets[0]
        # Promoted to a repository dataset after verification
        assert ds.source == "openneuro"
        assert ds.source_id == "ds000001"
        assert ds.trust_tier.value == "verified"
        assert ds.provenance is not None
        assert result.stages.get("publish") is not None
        # Provenance discovery history: one event, correct method + origin
        prov = ds.provenance
        assert prov["discovery_count"] == 1
        assert len(prov["discovery_history"]) == 1
        event = prov["discovery_history"][0]
        assert event["discovery_method"] == "web_search"
        assert event["source"] == WEB_DISCOVERY_SOURCE  # origin kept even after promotion
        assert event["harvest_query"] == ""
        assert event["pipeline_version"] == ds.provenance["pipeline_version"]
        assert prov["first_seen_at"] == prov["last_seen_at"] == prov["harvested_at"]

    @pytest.mark.asyncio
    async def test_web_direct_link_stays_web_discovered_but_verified(self) -> None:
        web = _web_candidate(url="https://example.edu/sub-01_T1w.nii.gz")
        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "example.edu", _live_response())),
            ),
            patch("app.ingestion.quality_pipeline.bulk_upsert", new=AsyncMock(return_value=1)),
        ):
            result = await run_quality_pipeline([web], publish=True)
        assert len(result.datasets) == 1
        assert result.datasets[0].source == WEB_DISCOVERY_SOURCE
        assert result.datasets[0].trust_tier.value == "verified"
        # Discovery history reflects the web-search discovery
        prov = result.datasets[0].provenance
        assert prov["discovery_count"] == 1
        assert prov["discovery_history"][0]["discovery_method"] == "web_search"
        assert prov["discovery_history"][0]["source"] == WEB_DISCOVERY_SOURCE

    @pytest.mark.asyncio
    async def test_homepage_never_reaches_persistence(self) -> None:
        """Issue 2/5 — with publish=True, a homepage candidate must be dropped
        at Stage 4 so bulk_upsert is NEVER called for it (no Canonical
        Persistence of landing pages), while a real dataset still publishes."""
        homepage = _web_candidate(url="https://openneuro.org/", title="OpenNeuro")
        real = _web_candidate(url="https://openneuro.org/datasets/ds000001", title="Real dataset")
        calls: list[list] = []

        async def fake_upsert(datasets: list):
            calls.append(datasets)
            return len(datasets)

        with (
            patch("app.config.get_settings", return_value=_settings()),
            patch(
                "app.agents.verification_agent.VerificationAgent._check_link",
                new=AsyncMock(return_value=(True, "openneuro.org", _live_response())),
            ),
            patch("app.ingestion.quality_pipeline.bulk_upsert", new=fake_upsert),
        ):
            result = await run_quality_pipeline([homepage, real], publish=True)
        assert len(result.datasets) == 1
        assert result.datasets[0].source_id == "ds000001"
        assert len(calls) == 1
        assert [d.source_id for d in calls[0]] == ["ds000001"]
        # The homepage is rejected at Stage 2 (classified as documentation);
        # the real dataset is the only record that reaches persistence.
        assert result.stages["classify"].dropped.get("documentation") == 1
        assert result.stages["verify"].dropped.get("repo_landing_page", 0) == 0


# ---------------------------------------------------------------------------
# Repository integrity — landing pages, known-domain matching, @type
# ---------------------------------------------------------------------------


class TestRepositoryIntegrity:
    def test_known_domain_host_matches_subdomains(self) -> None:
        # search.kg.ebrains.eu is the public KG Search host (under ebrains.eu);
        # figshare portals are branded subdomains — both are known domains.
        assert _known_domain_host("search.kg.ebrains.eu") is True
        assert _known_domain_host("karger.figshare.com") is True
        assert _known_domain_host("openneuro.org") is True
        assert _known_domain_host("random-lab.example.com") is False

    def test_repo_native_class_jsonld_atype(self) -> None:
        """Issue 4 — the current EBRAINS Query API declares types via @type
        (openMINDS Dataset / DatasetVersion IRIs). These must classify as
        ``dataset`` — a related publication never downgrades them."""
        assert _repo_native_class({"@type": ["https://openminds.ebrains.eu/core/DatasetVersion"]}) == "dataset"
        assert _repo_native_class({"@type": "https://openminds.ebrains.eu/core/Dataset"}) == "dataset"
        assert _repo_native_class({"@type": "https://openminds.ebrains.eu/core/SoftwareVersion"}) == "software"
        assert _repo_native_class({"@type": "https://openminds.ebrains.eu/core/Model"}) is None

    def test_repo_origin_homepage_classified_documentation(self) -> None:
        rec = _record(
            RepositoryDataset(
                source="openneuro", source_id="home",
                url="https://openneuro.org/", title="OpenNeuro",
            )
        )
        assert _classify_content_type(rec, _settings()) == "documentation"

    @pytest.mark.parametrize(
        "url,expected",
        [
            # landing pages on supported repository domains → True
            ("https://openneuro.org/", True),
            ("https://openneuro.org/about", True),
            ("https://openneuro.org/documentation/overview", True),
            ("https://dandiarchive.org/", True),
            ("https://www.nitrc.org/", True),
            ("https://osf.io/", True),
            ("https://figshare.com/", True),
            ("https://figshare.com/search?q=meg", True),
            ("https://search.kg.ebrains.eu/", True),
            # actual dataset URLs → False
            ("https://openneuro.org/datasets/ds000001", False),
            ("https://dandiarchive.org/dandiset/000003", False),
            ("https://zenodo.org/records/12345", False),
            ("https://search.kg.ebrains.eu/instances/some-uuid", False),
            ("https://osf.io/abcd1234/", False),
            ("https://karger.figshare.com/articles/dataset/12345678", False),
            # direct data links → False
            ("https://openneuro.org/datasets/ds000001/files/sub-01_T1w.nii.gz", False),
            # unknown domains → False (not a repository landing page)
            ("https://example.edu/data", False),
        ],
    )
    def test_is_repository_landing_page(self, url: str, expected: bool) -> None:
        assert _is_repository_landing_page(url) is expected

    @pytest.mark.asyncio
    async def test_osf_and_figshare_homepages_never_promoted(self) -> None:
        """Bare repository roots on osf.io / figshare.com are homepages — they
        must be dropped, never promoted to a repository dataset."""
        for url in ("https://osf.io/", "https://figshare.com/"):
            rec = _record(_web_candidate(url=url, title=url))
            with (
                patch("app.config.get_settings", return_value=_settings()),
                patch(
                    "app.agents.verification_agent.VerificationAgent._check_link",
                    new=AsyncMock(return_value=(True, "osf.io", _live_response())),
                ),
            ):
                kept, stats = await _stage_verify([rec], _settings())
            assert stats.accepted == 0, url
            assert stats.dropped.get("repo_landing_page") == 1, url
            assert kept == [], url
