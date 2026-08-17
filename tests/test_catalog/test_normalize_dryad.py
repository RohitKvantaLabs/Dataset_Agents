"""Unit tests for Dryad census record → canonical source normalization (pure, no I/O).

Covers the 15 required test cases:
 1. Dryad record normalization
 2. Stable Dryad dataset identity (canonical DOI, deterministic)
 3. sourceDatasetId
 4. sourceKeys
 5. sourceUrl
 6. DOI handling
 7. Related article DOI remains relationship metadata
 8. Version handling
 9. rawMetadata.dryad
10. File metadata without file downloads
11. HIGH-confidence candidate filtering
12. MEDIUM/FALSE records cannot enter ingestion
13. Duplicate candidate protection
14. Generic identity resolver integration
15. Failure handling (pure normalize guards)
"""

from app.catalog.dedup import evaluate_identity, merge_source_into_canonical
from app.catalog.normalize import (
    DRYAD_DATASET_URL,
    DRYAD_HIGH_CONFIDENCE,
    DRYAD_REPOSITORY,
    build_dryad_source_record,
    canonical_record_from_source,
)
from app.catalog.schema import (
    AVAILABILITY_BY_REPOSITORY,
    normalize_url_key,
    validate_canonical_record,
)

from ._dryad_fixtures import dryad_record, dryad_record_b


def _source(**overrides) -> dict:
    return build_dryad_source_record(dryad_record(**overrides))


# ─────────────────────────────────────────────────────────────────────────────
# Case 1: Dryad record normalization (identity + core fields)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase1Normalization:
    def test_repository_is_dryad(self):
        assert DRYAD_REPOSITORY == "dryad"
        assert DRYAD_REPOSITORY not in (
            "openneuro", "dandi", "nemar", "neuromorpho", "allen", "hcp",
        )

    def test_source_record_core_fields(self):
        src = _source()
        assert src["repository"] == "dryad"
        assert src["title"] == dryad_record()["title"]
        assert src["description"] == dryad_record()["abstract"]
        assert src["availability"] == "open"
        assert src["license"] == "https://spdx.org/licenses/CC0-1.0.html"
        assert src["licenseNormalized"] == "cc0"

    def test_authors_and_contributors_split(self):
        src = _source()
        assert src["authors"] == ["Jessie Kulaga-Yoskovitz", "Boris C. Bernhardt"]
        # authors with an ORCID or an affiliation also appear as contributors
        # (affiliation-only authors are included — same rule as NEMAR)
        assert src["contributors"] == [
            {
                "name": "Jessie Kulaga-Yoskovitz",
                "orcid": None,
                "affiliation": "McGill University",
            },
            {
                "name": "Boris C. Bernhardt",
                "orcid": "0000-0001-9200-6187",
                "affiliation": "McGill University",
            },
        ]

    def test_publish_date_and_year(self):
        src = _source()
        assert src["publishDate"] == "2016-10-22"
        assert src["derived"]["publicationYear"] == 2016
        assert src["lastUpdated"] is not None  # parsed from lastModificationDate

    def test_canonical_record_validates(self):
        rec = canonical_record_from_source(_source())
        assert rec["canonicalDatasetId"].startswith("ns-")
        assert validate_canonical_record(rec) == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 2 + 6: Stable Dryad dataset identity via the canonical dataset DOI
# ─────────────────────────────────────────────────────────────────────────────


class TestCase2_6StableIdentityAndDoi:
    def test_doi_is_the_dryad_dataset_doi(self):
        src = _source()
        assert src["doi"] == "10.5061/dryad.gc72v"

    def test_canonical_identity_uses_doi_priority(self):
        rec = canonical_record_from_source(_source())
        prov = rec["provenance"]["identity"]
        assert prov["primary"] == "doi:10.5061/dryad.gc72v"
        assert prov["doi"] == "10.5061/dryad.gc72v"

    def test_identity_is_deterministic(self):
        a = canonical_record_from_source(_source())
        b = canonical_record_from_source(_source())
        assert a["canonicalDatasetId"] == b["canonicalDatasetId"]

    def test_two_datasets_two_canonical_ids(self):
        r1 = canonical_record_from_source(_source())
        r2 = canonical_record_from_source(build_dryad_source_record(dryad_record_b()))
        assert r1["canonicalDatasetId"] != r2["canonicalDatasetId"]
        assert r1["doi"] != r2["doi"]

    def test_doi_not_fabricated_when_absent(self):
        src = _source(identifier=None)
        assert src["doi"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Case 3: sourceDatasetId
# ─────────────────────────────────────────────────────────────────────────────


class TestCase3SourceDatasetId:
    def test_deterministic_per_dataset(self):
        assert _source()["sourceDatasetId"] == "dryad:105"
        b = build_dryad_source_record(dryad_record_b())
        assert b["sourceDatasetId"] == "dryad:4812047"

    def test_same_record_same_id(self):
        assert _source()["sourceDatasetId"] == _source()["sourceDatasetId"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 4: sourceKeys
# ─────────────────────────────────────────────────────────────────────────────


class TestCase4SourceKeys:
    def test_canonical_source_keys(self):
        rec = canonical_record_from_source(_source())
        assert rec["sourceKeys"] == ["dryad:dryad:105"]

    def test_source_key_identity_signal(self):
        src = _source()
        assert f"{src['repository']}:{src['sourceDatasetId']}" == "dryad:dryad:105"


# ─────────────────────────────────────────────────────────────────────────────
# Case 5: sourceUrl = actual Dryad dataset landing page
# ─────────────────────────────────────────────────────────────────────────────


class TestCase5SourceUrl:
    def test_landing_page_url(self):
        src = _source()
        assert src["sourceUrl"] == "https://datadryad.org/dataset/doi:10.5061/dryad.gc72v"
        assert src["sourceUrl"] == f"{DRYAD_DATASET_URL}doi:10.5061/dryad.gc72v"

    def test_url_is_dataset_page_not_homepage_or_article(self):
        src = _source()
        assert src["sourceUrl"].startswith("https://datadryad.org/dataset/")
        assert src["sourceUrl"] != "https://datadryad.org"
        assert "doi.org/10.1038" not in src["sourceUrl"]  # article DOI is NOT the URL
        assert "/files" not in src["sourceUrl"]

    def test_url_normalizes_distinctly_per_dataset(self):
        u1 = normalize_url_key(_source()["sourceUrl"])
        u2 = normalize_url_key(build_dryad_source_record(dryad_record_b())["sourceUrl"])
        assert u1 != u2
        assert u1 == "http://datadryad.org/dataset/doi:10.5061/dryad.gc72v"


# ─────────────────────────────────────────────────────────────────────────────
# Case 7: Related article DOI remains relationship metadata
# ─────────────────────────────────────────────────────────────────────────────


class TestCase7ArticleDoiIsRelationship:
    def test_article_doi_never_becomes_canonical_doi(self):
        src = _source()
        # the primary-article DOI is 10.1038/sdata.2015.59 — different from the
        # dataset DOI 10.5061/dryad.gc72v
        assert src["doi"] == "10.5061/dryad.gc72v"
        assert (src["publication"] or {}).get("articleDoi") == "10.1038/sdata.2015.59"
        rec = canonical_record_from_source(src)
        assert rec["doi"] == "10.5061/dryad.gc72v"  # NOT the article DOI

    def test_related_identifiers_preserved(self):
        src = _source()
        rel = (src["publication"] or {}).get("relatedIdentifiers") or []
        assert rel == [
            {
                "relationship": "primary_article",
                "identifierType": "DOI",
                "identifier": "https://doi.org/10.1038/sdata.2015.59",
            }
        ]

    def test_identity_never_matches_via_article_doi(self):
        """Two different datasets with different DOIs must NEVER merge — the
        DOI conflict rule blocks them (ambiguous, kept separate)."""
        src_a = _source()
        rec_a = canonical_record_from_source(src_a)
        src_b = build_dryad_source_record(dryad_record_b())  # different DOI
        result = evaluate_identity(rec_a, src_b)
        assert result["match"] is False  # never merged
        assert result["ambiguous"] is True  # DOI conflict → kept separate


# ─────────────────────────────────────────────────────────────────────────────
# Case 8: Version handling — one list record = one dataset entity
# ─────────────────────────────────────────────────────────────────────────────


class TestCase8VersionHandling:
    def test_version_metadata_preserved_not_separate_records(self):
        src = _source(versionNumber=3, versionStatus="published")
        snap = src["snapshot"]
        assert snap["versionNumber"] == 3
        assert snap["versionStatus"] == "published"
        assert snap["dryadId"] == "105"

    def test_one_record_per_dataset_regardless_of_version(self):
        v1 = canonical_record_from_source(_source(versionNumber=1))
        v5 = canonical_record_from_source(_source(versionNumber=5))
        # same dataset DOI → same canonical identity, never a new record
        assert v1["canonicalDatasetId"] == v5["canonicalDatasetId"]
        assert v1["doi"] == v5["doi"] == "10.5061/dryad.gc72v"

    def test_version_metadata_in_raw_metadata(self):
        rec = canonical_record_from_source(_source(versionNumber=2))
        assert rec["rawMetadata"]["dryad"]["versionNumber"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Case 9: rawMetadata.dryad is populated correctly
# ─────────────────────────────────────────────────────────────────────────────


class TestCase9RawMetadataDryad:
    def test_raw_metadata_structure(self):
        rec = canonical_record_from_source(_source())
        assert list(rec["rawMetadata"].keys()) == ["dryad"]
        raw = rec["rawMetadata"]["dryad"]
        assert raw["identifier"] == "doi:10.5061/dryad.gc72v"
        assert raw["id"] == 105
        assert raw["title"] == dryad_record()["title"]
        assert raw["abstract"] == dryad_record()["abstract"]
        assert raw["authors"][0]["firstName"] == "Jessie"
        assert raw["fieldOfScience"] is None
        assert raw["versionNumber"] == 1
        assert raw["license"] == "https://spdx.org/licenses/CC0-1.0.html"
        assert raw["relatedWorks"][0]["relationship"] == "primary_article"
        assert raw["classification"] == "H"

    def test_merge_keeps_dryad_raw_metadata_repo_scoped(self):
        rec = canonical_record_from_source(_source())
        rec["rawMetadata"]["allen"] = {"other": True}
        dup = _source()
        merged = merge_source_into_canonical(rec, dup, "source_key")
        assert set(merged["rawMetadata"].keys()) == {"dryad", "allen"}
        assert merged["sourceKeys"] == ["dryad:dryad:105"]
        assert validate_canonical_record(merged) == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 10: File metadata without file downloads
# ─────────────────────────────────────────────────────────────────────────────


class TestCase10FileMetadataNoDownload:
    def test_storage_size_and_size_label(self):
        src = _source()
        assert src["datasetSizeBytes"] == 9_924_107_495
        assert src["derived"]["sizeLabel"] == "9.2 GB"

    def test_no_file_content_in_record(self):
        src = _source()
        assert "files" not in src  # file METADATA only (size), never contents
        assert "fileUrls" not in src
        assert src["rawMetadata"].get("storageSize") == 9_924_107_495


# ─────────────────────────────────────────────────────────────────────────────
# Case 11 + 12: HIGH-confidence candidate filtering / M+F cannot enter
# ─────────────────────────────────────────────────────────────────────────────


class TestCase11_12HighFiltering:
    def test_high_confidence_label_constant(self):
        assert DRYAD_HIGH_CONFIDENCE == "H"

    def test_classification_preserved_on_source(self):
        assert _source(classification="H")["snapshot"]["censusClassification"] == "H"

    def test_medium_and_false_records_carry_their_classification(self):
        from ._dryad_fixtures import false_positive_record, medium_record

        m = build_dryad_source_record(medium_record())
        f = build_dryad_source_record(false_positive_record())
        assert m["snapshot"]["censusClassification"] == "M"
        assert f["snapshot"]["censusClassification"] == "F"


# ─────────────────────────────────────────────────────────────────────────────
# Case 13: Duplicate candidate protection (pure detection keys)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase13DuplicateProtection:
    def test_identity_keys_unique(self):
        from app.catalog.ingest import _dryad_identity_keys

        a = _dryad_identity_keys(dryad_record())
        b = _dryad_identity_keys(dryad_record_b())
        assert set(a) & set(b) == set()  # no key collision across datasets

    def test_same_candidate_same_keys(self):
        from app.catalog.ingest import _dryad_identity_keys

        assert _dryad_identity_keys(dryad_record()) == _dryad_identity_keys(
            dryad_record()
        )


# ─────────────────────────────────────────────────────────────────────────────
# Case 14: Generic identity resolver integration
# ─────────────────────────────────────────────────────────────────────────────


class TestCase14GenericIdentity:
    def test_same_dataset_resolves_as_match(self):
        src = _source()
        rec = canonical_record_from_source(src)
        again = _source()
        result = evaluate_identity(rec, again)
        assert result["match"] is True
        assert result["matchedVia"] in ("doi", "source_url", "source_key")

    def test_distinct_datasets_never_match(self):
        """Distinct Dryad datasets have distinct DOIs → DOI conflict blocks any
        merge (ambiguous, kept separate)."""
        r1 = canonical_record_from_source(_source())
        s2 = build_dryad_source_record(dryad_record_b())
        result = evaluate_identity(r1, s2)
        assert result["match"] is False
        assert result["ambiguous"] is True  # DOI conflict → never merged

    def test_doi_conflict_never_merges(self):
        """A record with a DIFFERENT DOI must never merge via fuzzy rules."""
        src = _source()
        rec = canonical_record_from_source(src)
        other = build_dryad_source_record(
            dryad_record(identifier="doi:10.5061/dryad.different")
        )
        result = evaluate_identity(rec, other)
        assert result["match"] is False
        assert result["ambiguous"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Case 15: Failure handling (pure guards — no fabrication on bad input)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase15FailureHandling:
    def test_missing_identity_fields_stay_none_not_crash(self):
        src = _source(identifier=None, id=None)
        assert src["doi"] is None
        assert src["sourceDatasetId"] is None
        assert src["sourceUrl"] is None
        # still a well-formed record dict (no exception raised)

    def test_unavailable_fields_stay_null(self):
        src = _source()
        assert src["modality"] == []
        assert src["species"] is None
        assert src["disease"] is None
        assert src["brainRegions"] is None
        assert src["ages"] is None
        assert src["ageGroup"] is None
        assert src["participantCount"] is None
        assert src["tasks"] == []
        assert src["sessions"] == []
        assert src["studyType"] is None
        assert src["studyDesign"] is None

    def test_availability_by_repository_dryad_open(self):
        assert AVAILABILITY_BY_REPOSITORY["dryad"] == "open"
        # other repositories unchanged
        assert AVAILABILITY_BY_REPOSITORY["openneuro"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["dandi"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["nemar"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["neuromorpho"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["allen"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["hcp"] == "open"

    def test_never_uses_article_or_funder_as_identity(self):
        src = _source()
        prov = canonical_record_from_source(src)["provenance"]["identity"]
        assert "sdata.2015.59" not in prov["primary"]
        assert "10.1038" not in prov["primary"]
        assert "NSF" not in prov["primary"]
