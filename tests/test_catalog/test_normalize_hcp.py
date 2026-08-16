"""Unit tests for HCP Study → canonical source normalization (pure, no I/O).

Covers the 14 required test cases:
 1. HCP Study normalization
 2. sourceDatasetId = hcp:<slug>
 3. sourceKeys = ["hcp:hcp:<slug>"]
 4. sourceUrl = https://www.humanconnectome.org/study/<slug>
 5. DOI remains null (publication DOIs quarantined)
 6. Releases stored as metadata, NOT datasets
 7. Publication DOIs do not become canonical DOI
 8. Two different slugs cannot collide
 9. Generic identity resolver receives the normalized record
10. Discovery gate rejects a partial catalog
11. Discovery gate rejects duplicate Study slugs
12. Asset/file fetchers never invoked (asserted in the ingest tests)
13. rawMetadata.hcp is populated correctly
14. Existing repository behavior remains unaffected
"""

from app.catalog.dedup import evaluate_identity, merge_source_into_canonical
from app.catalog.normalize import (
    HCP_REPOSITORY,
    HCP_STUDY_URL,
    build_hcp_source_record,
    canonical_record_from_source,
    hcp_index_slugs,
    hcp_parse_publications_page,
    hcp_parse_releases_page,
    hcp_parse_study_page,
)
from app.catalog.schema import normalize_url_key, validate_canonical_record

from ._hcp_fixtures import (
    HCP_STUDY_SLUGS,
    index_html,
    publications_html,
    releases_html,
    study_dict,
    study_html,
)


def _source(slug: str = "hcp-young-adult", **overrides) -> dict:
    study = study_dict(slug)
    study.update(overrides)
    return build_hcp_source_record(study)


# ─────────────────────────────────────────────────────────────────────────────
# Case 1: HCP Study normalization (identity + core fields)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase1StudyNormalization:
    def test_repository_is_hcp(self):
        assert HCP_REPOSITORY == "hcp"
        assert HCP_REPOSITORY not in ("openneuro", "dandi", "nemar", "neuromorpho", "allen")

    def test_source_record_identity(self):
        src = _source("hcp-young-adult")
        assert src["repository"] == "hcp"
        assert src["sourceDatasetId"] == "hcp:hcp-young-adult"
        assert src["doi"] is None
        assert src["datasetType"] == "study"
        assert src["availability"] == "open"

    def test_title_and_description_from_study_page(self):
        src = _source("hcp-young-adult")
        assert src["title"] == "HCP Young Adult"
        assert "Human Connectome Project" in (src["description"] or "")

    def test_parsed_study_page(self):
        html = study_html("hcp-young-adult")
        study = hcp_parse_study_page(html, "hcp-young-adult")
        assert study["name"] == "HCP Young Adult"
        assert len(study["principalInvestigators"]) == 2
        assert study["principalInvestigators"][0]["institution"] == "UMinn"
        assert study["protocols"] == [
            "https://www.humanconnectome.org/study/hcp-young-adult/project-protocols"
        ]

    def test_canonical_record_validates(self):
        src = _source("hcp-young-adult")
        rec = canonical_record_from_source(src)
        assert rec["canonicalDatasetId"].startswith("ns-")
        assert validate_canonical_record(rec) == []

    def test_index_slugs_extraction(self):
        html = index_html(HCP_STUDY_SLUGS)
        assert hcp_index_slugs(html) == sorted(HCP_STUDY_SLUGS)


# ─────────────────────────────────────────────────────────────────────────────
# Case 2: sourceDatasetId = hcp:<slug>
# ─────────────────────────────────────────────────────────────────────────────


class TestCase2SourceDatasetId:
    def test_deterministic_per_slug(self):
        assert _source("hcp-young-adult")["sourceDatasetId"] == "hcp:hcp-young-adult"
        assert _source("hcp-lifespan-aging")["sourceDatasetId"] == "hcp:hcp-lifespan-aging"

    def test_same_study_same_id(self):
        a = _source("hcp-young-adult")
        b = _source("hcp-young-adult")
        assert a["sourceDatasetId"] == b["sourceDatasetId"] == "hcp:hcp-young-adult"


# ─────────────────────────────────────────────────────────────────────────────
# Case 3: sourceKeys = ["hcp:hcp:<slug>"]
# ─────────────────────────────────────────────────────────────────────────────


class TestCase3SourceKeys:
    def test_canonical_source_keys(self):
        rec = canonical_record_from_source(_source("hcp-young-adult"))
        assert rec["sourceKeys"] == ["hcp:hcp:hcp-young-adult"]

    def test_source_key_identity_signal(self):
        src = _source("hcp-lifespan-development")
        assert f"{src['repository']}:{src['sourceDatasetId']}" == "hcp:hcp:hcp-lifespan-development"


# ─────────────────────────────────────────────────────────────────────────────
# Case 4: sourceUrl = https://www.humanconnectome.org/study/<slug>
# ─────────────────────────────────────────────────────────────────────────────


class TestCase4SourceUrl:
    def test_study_landing_page_url(self):
        src = _source("hcp-young-adult")
        assert src["sourceUrl"] == "https://www.humanconnectome.org/study/hcp-young-adult"
        assert src["sourceUrl"] == HCP_STUDY_URL.format(slug="hcp-young-adult")

    def test_url_normalizes_distinctly_per_study(self):
        """The normalized URL must differ per study so the generic resolver can
        never merge two Studies through the source_url signal."""
        u_ya = normalize_url_key(_source("hcp-young-adult")["sourceUrl"])
        u_age = normalize_url_key(_source("hcp-lifespan-aging")["sourceUrl"])
        assert u_ya != u_age
        assert u_ya == "http://humanconnectome.org/study/hcp-young-adult"

    def test_never_repo_homepage_or_balsa(self):
        src = _source("hcp-young-adult")
        assert "humanconnectome.org/study/" in src["sourceUrl"]
        assert "balsa" not in src["sourceUrl"]
        assert src["sourceUrl"] != "https://www.humanconnectome.org"


# ─────────────────────────────────────────────────────────────────────────────
# Case 5: DOI remains null
# ─────────────────────────────────────────────────────────────────────────────


class TestCase5DoiNull:
    def test_doi_null(self):
        src = _source("hcp-young-adult")
        assert src["doi"] is None
        rec = canonical_record_from_source(src)
        assert rec["doi"] is None

    def test_canonical_identity_falls_back_to_source_url(self):
        """No DOI and a distinct per-study URL → canonical ID derives from the
        normalized source URL (priority 2), never a fabricated DOI."""
        rec = canonical_record_from_source(_source("hcp-young-adult"))
        prov = rec["provenance"]["identity"]
        assert prov["doi"] is None
        assert prov["sourceUrlNorm"] == "http://humanconnectome.org/study/hcp-young-adult"


# ─────────────────────────────────────────────────────────────────────────────
# Case 6: Releases are metadata, NOT datasets
# ─────────────────────────────────────────────────────────────────────────────


class TestCase6ReleasesAreMetadata:
    def test_releases_aggregated_under_raw_metadata(self):
        study = study_dict(
            "hcp-young-adult",
            releases=[
                ("Q1 Subjects Data Release", "03/05/2013"),
                ("900 Subjects Data Release Reference", "12/08/2015"),
                ("1200 Subjects Data Release", "03/01/2017"),
            ],
        )
        src = build_hcp_source_record(study)
        rec = canonical_record_from_source(src)
        raw = rec["rawMetadata"]["hcp"]["dataReleases"]
        assert len(raw) == 3
        assert raw[0]["name"] == "Q1 Subjects Data Release"
        assert raw[0]["date"] == "03/05/2013"
        # snapshot carries a bounded count only
        assert src["snapshot"]["dataReleaseCount"] == 3

    def test_one_study_one_canonical_dataset(self):
        """14 releases on one Study → still exactly ONE source record, never 14."""
        study = study_dict(
            "hcp-young-adult",
            releases=[(f"Release {i}", f"0{i}/01/2020") for i in range(1, 15)],
        )
        src = build_hcp_source_record(study)
        rec = canonical_record_from_source(src)
        assert len(rec["sources"]) == 1
        assert len(rec["rawMetadata"]["hcp"]["dataReleases"]) == 14
        assert rec["sourceKeys"] == ["hcp:hcp:hcp-young-adult"]

    def test_release_page_parser_excludes_cta_block(self):
        html = releases_html([
            ("1200 Subjects Data Release", "03/01/2017"),
            ("MEG1 Initial Data Release", "03/04/2014"),
        ])
        releases = hcp_parse_releases_page(html)
        assert len(releases) == 2
        assert all("Let me explore" not in r["name"] for r in releases)
        assert all(r["date"] for r in releases)


# ─────────────────────────────────────────────────────────────────────────────
# Case 7: Publication DOIs do not become canonical DOI
# ─────────────────────────────────────────────────────────────────────────────


class TestCase7PublicationDoisQuarantined:
    def test_publication_dois_stored_in_raw_metadata_only(self):
        pubs = ["10.1038/nature18933", "10.1038/sdata.2017.10"]
        src = _source("hcp-young-adult", publications=pubs)
        assert src["doi"] is None
        assert canonical_record_from_source(src)["rawMetadata"]["hcp"]["publications"] == pubs
        assert canonical_record_from_source(src)["doi"] is None

    def test_publication_page_parser_extracts_dois(self):
        html = publications_html(["10.1038/nature18933", "10.1038/sdata.2017.10"])
        assert hcp_parse_publications_page(html) == ["10.1038/nature18933", "10.1038/sdata.2017.10"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 8: Two different slugs cannot collide
# ─────────────────────────────────────────────────────────────────────────────


class TestCase8NoSlugCollision:
    def test_two_studies_two_canonical_records(self):
        r1 = canonical_record_from_source(_source("hcp-young-adult"))
        r2 = canonical_record_from_source(_source("hcp-lifespan-aging"))
        assert r1["canonicalDatasetId"] != r2["canonicalDatasetId"]
        assert r1["sourceKeys"] != r2["sourceKeys"]

    def test_identity_never_matches_across_studies(self):
        r1 = canonical_record_from_source(_source("hcp-young-adult"))
        s2 = _source("hcp-lifespan-aging")
        result = evaluate_identity(r1, s2)
        assert result["match"] is False
        assert result["ambiguous"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Case 9: Generic identity resolver receives the normalized record
# ─────────────────────────────────────────────────────────────────────────────


class TestCase9GenericIdentity:
    def test_same_study_resolves_as_match(self):
        src = _source("hcp-young-adult")
        rec = canonical_record_from_source(src)
        again = _source("hcp-young-adult")
        result = evaluate_identity(rec, again)
        assert result["match"] is True
        assert result["matchedVia"] in ("source_url", "source_key")

    def test_canonical_identity_is_deterministic(self):
        a = canonical_record_from_source(_source("hcp-young-adult"))
        b = canonical_record_from_source(_source("hcp-young-adult"))
        assert a["canonicalDatasetId"] == b["canonicalDatasetId"]

    def test_no_fuzzy_title_merge(self):
        """Exact title + different slug → repository ID is authoritative; the
        generic pipeline must NOT multi-field-merge two same-repo studies."""
        r1 = canonical_record_from_source(_source("hcp-young-adult", name="Identical Title"))
        s2 = _source("hcp-lifespan-aging", name="Identical Title")
        result = evaluate_identity(r1, s2)
        # same repo (hcp) attached → repo ID is authoritative → never fuzzy-merge
        assert result["match"] is False
        assert result["ambiguous"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Case 10 + 11: Discovery gate (implemented in the ingestion runner; the
# pure slug helpers below are the inputs it validates)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase10_11DiscoveryGate:
    def test_partial_index_yields_fewer_slugs(self):
        html = index_html(HCP_STUDY_SLUGS[:5])
        assert len(hcp_index_slugs(html)) == 5

    def test_duplicate_slugs_collapse_to_unique_set(self):
        html = index_html(["hcp-young-adult", "hcp-young-adult", "hcp-lifespan-aging"])
        assert hcp_index_slugs(html) == ["hcp-lifespan-aging", "hcp-young-adult"]

    def test_full_index_yields_all_20(self):
        html = index_html(HCP_STUDY_SLUGS)
        assert len(hcp_index_slugs(html)) == 20


# ─────────────────────────────────────────────────────────────────────────────
# Case 13: rawMetadata.hcp is populated correctly
# ─────────────────────────────────────────────────────────────────────────────


class TestCase13RawMetadataHcp:
    def test_raw_metadata_structure(self):
        study = study_dict(
            "hcp-young-adult",
            releases=[("1200 Subjects Data Release", "03/01/2017")],
            publications=["10.1038/nature18933"],
        )
        src = build_hcp_source_record(study)
        rec = canonical_record_from_source(src)
        assert list(rec["rawMetadata"].keys()) == ["hcp"]
        raw = rec["rawMetadata"]["hcp"]
        assert raw["slug"] == "hcp-young-adult"
        assert raw["name"] == "HCP Young Adult"
        assert raw["url"] == "https://www.humanconnectome.org/study/hcp-young-adult"
        assert "Human Connectome Project" in (raw["description"] or "")
        assert raw["principalInvestigators"][0]["name"] == "Kamil Ugurbil"
        assert raw["dataReleases"][0]["name"] == "1200 Subjects Data Release"
        assert raw["publications"] == ["10.1038/nature18933"]
        assert raw["dataUseTerms"] == (
            "https://www.humanconnectome.org/study/hcp-young-adult/data-use-terms"
        )

    def test_merge_keeps_hcp_raw_metadata_repo_scoped(self):
        src = _source("hcp-young-adult")
        rec = canonical_record_from_source(src)
        rec["rawMetadata"]["allen"] = {"other": True}
        dup = _source("hcp-young-adult")
        merged = merge_source_into_canonical(rec, dup, "source_key")
        assert set(merged["rawMetadata"].keys()) == {"hcp", "allen"}
        assert merged["sourceKeys"] == ["hcp:hcp:hcp-young-adult"]
        assert validate_canonical_record(merged) == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 14: Existing repository behavior remains unaffected
# ─────────────────────────────────────────────────────────────────────────────


class TestCase14RegressionSafety:
    def test_hcp_isolation(self):
        src = _source("hcp-young-adult")
        assert src["repository"] == "hcp"
        assert "humanconnectome.org" in src["sourceUrl"]
        for other in ("openneuro.org", "dandiarchive", "nemar.org", "neuromorpho.org",
                      "brain-map.org", "balsa.wustl.edu"):
            assert other not in src["sourceUrl"]

    def test_availability_by_repository_other_values_unchanged(self):
        from app.catalog.schema import AVAILABILITY_BY_REPOSITORY
        assert AVAILABILITY_BY_REPOSITORY["openneuro"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["dandi"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["nemar"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["neuromorpho"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["allen"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["hcp"] == "open"

    def test_unavailable_fields_stay_null(self):
        src = _source("hcp-young-adult")
        assert src["modality"] == []
        assert src["species"] is None
        assert src["disease"] is None
        assert src["brainRegions"] is None
        assert src["ages"] is None
        assert src["ageGroup"] is None
        assert src["participantCount"] is None
        assert src["license"] is None
        assert src["licenseNormalized"] is None
        assert src["tasks"] == []
        assert src["sessions"] == []
        assert src["datasetSizeBytes"] is None
        assert src["studyType"] is None
        assert src["studyDesign"] is None
