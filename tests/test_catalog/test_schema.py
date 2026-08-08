"""Unit tests for the canonical catalog schema rules (pure logic, no I/O)."""

from app.catalog.schema import (
    KNOWN_NORMALIZED_LICENSES,
    derive_age_groups,
    make_canonical_id,
    normalize_doi,
    normalize_license,
    normalize_title,
    normalize_url_key,
    size_label,
    validate_canonical_record,
)


class TestAgeGroups:
    def test_children_0_12(self):
        assert derive_age_groups([3, 7, 12]) == ["Children"]

    def test_adolescents_13_17(self):
        assert derive_age_groups([13, 15, 17]) == ["Adolescents"]

    def test_adults_18_64(self):
        assert derive_age_groups([18, 30, 64]) == ["Adults"]

    def test_older_adults_65_plus(self):
        assert derive_age_groups([65, 80, 95]) == ["Older Adults"]

    def test_multi_group_preserved(self):
        groups = derive_age_groups([8, 15, 25, 70])
        assert set(groups) == {"Children", "Adolescents", "Adults", "Older Adults"}

    def test_boundary_12_13(self):
        assert derive_age_groups([12]) == ["Children"]
        assert derive_age_groups([13]) == ["Adolescents"]

    def test_missing_returns_none(self):
        assert derive_age_groups(None) is None
        assert derive_age_groups([]) is None

    def test_garbage_ignored_not_fabricated(self):
        assert derive_age_groups(["unknown", None, "adult"]) is None

    def test_age_zero_counts_as_child(self):
        assert derive_age_groups([0]) == ["Children"]


class TestLicenseNormalization:
    def test_cc0_variants(self):
        for raw in ["CC0", "CCO", "Creative Commons CC0 1.0", "CC0 1.0", "Public Domain (CC0)"]:
            assert normalize_license(raw) == "cc0", raw

    def test_pddl_variants(self):
        for raw in [
            "PDDL",
            "PPDL",
            "Public Domain Dedication and License v1.0",
            "This dataset is made available under the Public Domain Dedication and License",
            "PD",
        ]:
            assert normalize_license(raw) == "pddl", raw

    def test_cc_by(self):
        assert normalize_license("Creative Commons Attribution 4.0 International Public License") == "cc-by-4.0"
        assert normalize_license("CC BY 4.0") == "cc-by-4.0"

    def test_cc_by_nc(self):
        assert normalize_license("CC BY-NC 4.0i") == "cc-by-nc-4.0"

    def test_unknown_returns_none(self):
        assert normalize_license(None) is None
        assert normalize_license("") is None
        assert normalize_license("Not Specified") is None
        assert normalize_license("some random free text") is None

    def test_all_normalized_values_known(self):
        assert {"cc0", "pddl", "cc-by-4.0", "cc-by-nc-4.0"} <= KNOWN_NORMALIZED_LICENSES


class TestSizeLabel:
    def test_human_labels(self):
        assert size_label(1024) == "1 KB"
        assert size_label(2416199965) == "2.3 GB"
        assert size_label(500) == "500 B"

    def test_none_and_zero(self):
        assert size_label(None) is None
        assert size_label(0) is None


class TestIdentityNormalization:
    def test_doi(self):
        assert normalize_doi("10.18112/openneuro.ds000001.v1.0.0") == "10.18112/openneuro.ds000001.v1.0.0"
        assert normalize_doi("HTTPS://DOI.ORG/10.5555/ABC") == "10.5555/abc"
        assert normalize_doi("doi:10.5555/abc") == "10.5555/abc"
        assert normalize_doi(None) is None

    def test_doi_rejects_placeholders(self):
        # Live regression: OpenNeuro stores "mockdoi" as a placeholder.
        assert normalize_doi("mockdoi") is None
        assert normalize_doi("to be assigned") is None
        assert normalize_doi("10.1/abc") is None  # no real registry prefix

    def test_url(self):
        assert normalize_url_key("https://openneuro.org/datasets/ds000001") == "http://openneuro.org/datasets/ds000001"
        assert normalize_url_key("https://www.OpenNeuro.org/datasets/ds000001/") == "http://openneuro.org/datasets/ds000001"

    def test_title(self):
        # lowercase, whitespace collapsed, punctuation stripped (no hyphenation
        # — hyphens could merge distinct words)
        assert normalize_title("  Resting  State  fMRI!  ") == "resting state fmri"
        assert normalize_title(None) is None

    def test_canonical_id_deterministic(self):
        assert make_canonical_id("doi:10.1/abc") == make_canonical_id("doi:10.1/abc")
        assert make_canonical_id("doi:10.1/abc").startswith("ns-")
        assert make_canonical_id("doi:10.1/abc") != make_canonical_id("doi:10.1/abd")


class TestValidation:
    def _valid_record(self):
        return {
            "canonicalDatasetId": "ns-abcdef1234567890",
            "title": "Test Dataset",
            "doi": "10.18112/openneuro.ds000001.v1.0.0",
            "license": "cc0",
            "ages": [25, 30],
            "ageGroup": ["Adults"],
            "participantCount": 2,
            "sources": [
                {
                    "repository": "openneuro",
                    "sourceDatasetId": "ds000001",
                    "sourceUrl": "https://openneuro.org/datasets/ds000001",
                }
            ],
        }

    def test_valid_record(self):
        assert validate_canonical_record(self._valid_record()) == []

    def test_missing_canonical_id(self):
        rec = self._valid_record()
        rec["canonicalDatasetId"] = "not-ns"
        assert validate_canonical_record(rec) != []

    def test_missing_source_fields(self):
        rec = self._valid_record()
        rec["sources"] = [{"repository": "openneuro"}]
        assert validate_canonical_record(rec) != []

    def test_age_group_without_ages_is_fabrication(self):
        rec = self._valid_record()
        rec["ages"] = None
        rec["ageGroup"] = ["Adults"]
        assert validate_canonical_record(rec) != []

    def test_age_group_inconsistent_with_ages(self):
        rec = self._valid_record()
        rec["ageGroup"] = ["Children"]  # ages are [25, 30] → Adults
        assert validate_canonical_record(rec) != []

    def test_negative_participant_count(self):
        rec = self._valid_record()
        rec["participantCount"] = -1
        assert validate_canonical_record(rec) != []

    def test_unknown_license_rejected(self):
        rec = self._valid_record()
        rec["license"] = "made-up-license"
        assert validate_canonical_record(rec) != []
