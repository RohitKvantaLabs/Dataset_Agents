"""Unit tests for NEMAR → canonical source normalization (pure logic, no I/O)."""

from app.catalog.normalize import (
    build_nemar_source_record,
    canonical_record_from_source,
    _split_tasks,
)
from app.catalog.schema import (
    normalize_doi,
    normalize_license,
    normalize_url_key,
    validate_canonical_record,
)

from ._nemar_fixtures import list_record, mirror_detail, missing_mirror_detail, native_detail


class TestNemarSourceRecord:
    def test_identity_fields(self):
        src = build_nemar_source_record(native_detail())
        assert src["repository"] == "nemar"
        assert src["sourceDatasetId"] == "nm000103"
        assert src["sourceUrl"] == "https://nemar.org/dataset/nm000103"

    def test_source_url_normalizes_uniquely_per_dataset(self):
        """Query-param URLs would collapse to one identity key — the path form
        must stay unique after normalize_url_key (mirrors the OpenNeuro
        ``/datasets/<id>`` convention)."""
        a = build_nemar_source_record(native_detail())["sourceUrl"]
        b = build_nemar_source_record(mirror_detail())["sourceUrl"]
        assert normalize_url_key(a) != normalize_url_key(b)
        assert normalize_url_key(a).endswith("/dataset/nm000103")

    def test_mirror_doi_is_none(self):
        """CRITICAL: a mirror's source doi is None — the NEMAR concept DOI must
        never become the canonical DOI (DOI-conflict rule would block merges)."""
        src = build_nemar_source_record(mirror_detail())
        assert src["doi"] is None
        # the NEMAR concept DOI exists in the payload but is NOT used as identity
        assert mirror_detail()["concept_doi"] == "10.82901/nemar.on004504"

    def test_mirror_cross_reference_url_present(self):
        """The generic cross-reference mechanism (extract_cross_references)
        parses openneuro.org/datasets/<id> from publication.referencesAndLinks."""
        src = build_nemar_source_record(mirror_detail())
        assert "https://openneuro.org/datasets/ds004504" in src["publication"]["referencesAndLinks"]

    def test_mirror_openneuro_provenance_in_snapshot(self):
        src = build_nemar_source_record(mirror_detail())
        assert src["snapshot"]["openNeuroSourceId"] == "ds004504"
        assert src["snapshot"]["openNeuroDoi"] == "10.18112/openneuro.ds004504.v1.0.9"
        # version DOI stays version metadata — never the canonical doi
        assert src["doi"] is None

    def test_native_uses_concept_doi(self):
        src = build_nemar_source_record(native_detail())
        assert src["doi"] == "10.82901/nemar.nm000103"
        assert src["snapshot"]["openNeuroSourceId"] is None

    def test_native_never_uses_version_doi(self):
        src = build_nemar_source_record(native_detail())
        assert src["doi"] == "10.82901/nemar.nm000103"          # concept
        assert src["snapshot"]["versionDoi"] == "10.82901/nemar.nm000103.v2.0.0"
        assert src["doi"] != src["snapshot"]["versionDoi"]       # version ≠ identity

    def test_title_description_readme(self):
        src = build_nemar_source_record(native_detail())
        assert src["title"] == "Healthy Brain Network EEG - Not for Commercial Use"
        assert src["description"].startswith("HBN-EEG NC")
        assert src["readme"].startswith("README for HBN EEG")

    def test_authors_and_contributors_from_enrichment(self):
        src = build_nemar_source_record(native_detail())
        assert "Seyed Yahya Shirazi" in src["authors"]
        assert "Alexandre Franco" in src["authors"]
        orcid = next(c for c in src["contributors"] if c["name"] == "Seyed Yahya Shirazi")
        assert orcid["orcid"] == "0000-0001-5557-259X"
        assert orcid["affiliations"] == ["UCSD"]

    def test_keywords_extracted_from_term_objects(self):
        src = build_nemar_source_record(native_detail())
        assert src["keywords"] == ["EEG", "child and adolescent mental health"]

    def test_modality_bids_tokens_preserved_no_conversion(self):
        d = mirror_detail()
        d["modalities"] = "anat,meg,func,beh,dwi,fmap,emg,nirs,motion,ieeg,eeg,perf"
        src = build_nemar_source_record(d)
        # normalize_modalities preserves unknown BIDS datatype tokens unchanged —
        # never converted to smri/fmri/dti (verified against MODALITY_VOCAB).
        assert src["modality"] == [
            "anat", "meg", "func", "beh", "dwi", "fmap", "emg", "nirs",
            "motion", "ieeg", "eeg", "perf",
        ]
        assert src["modalityRaw"] == src["modality"]

    def test_tasks_comma_string_split_defensively(self):
        src = build_nemar_source_record(native_detail())
        assert src["tasks"] == [
            "DespicableMe", "DiaryOfAWimpyKid", "FunwithFractals",
            "RestingState", "ThePresent",
        ]
        # garbage values are preserved, not dropped
        d = mirror_detail(tasks="")
        assert build_nemar_source_record(d)["tasks"] == []
        d = mirror_detail(tasks="P300,eyesClosed,eyesOpen")
        assert build_nemar_source_record(d)["tasks"] == ["P300", "eyesClosed", "eyesOpen"]
        assert _split_tasks(["a,b", "c"]) == ["a", "b", "c"]

    def test_participant_count_and_size_and_files(self):
        src = build_nemar_source_record(mirror_detail())
        assert src["participantCount"] == 88
        assert src["datasetSizeBytes"] == 5781001822
        assert src["snapshot"]["totalFiles"] == 361
        assert src["derived"]["sizeLabel"] == "5.4 GB"

    def test_age_range_stays_on_source_only(self):
        """age_min/age_max are dataset-level ranges — canonical ages/ageGroup
        stay null so derive_age_groups() is never fed a fabricated range."""
        src = build_nemar_source_record(native_detail())
        assert src["ageMin"] == 5.0059
        assert src["ageMax"] == 21.8166
        assert src["ages"] is None
        assert src["ageGroup"] is None
        assert src["derived"]["ageGroup"] is None

    def test_license_normalization(self):
        src = build_nemar_source_record(mirror_detail())      # CC0
        assert src["license"] == "CC0"
        assert src["licenseNormalized"] == "cc0"
        native = build_nemar_source_record(native_detail())   # CC-BY-NC-SA 4.0
        assert native["license"] == "CC-BY-NC-SA 4.0"
        # pre-existing substring quirk: NC-SA/NC-ND match the `cc-by-nc`
        # pattern → normalized as cc-by-nc-4.0. Documented, NOT fixed here
        # (raw license is always preserved).
        assert native["licenseNormalized"] == "cc-by-nc-4.0"

    def test_license_vocabulary_current_behavior(self):
        # verified real normalize_license() behavior on NEMAR's actual values
        assert normalize_license("CC0") == "cc0"
        assert normalize_license("CC-BY-4.0") == "cc-by-4.0"
        assert normalize_license("CC-BY-NC-4.0") == "cc-by-nc-4.0"
        assert normalize_license("PD") == "pddl"
        for raw in ("CC-BY-SA-4.0", "ODC-By-1.0", "ODbL v1.0", "GPL-3.0", "CDLA-Permissive-2.0"):
            assert normalize_license(raw) is None
        # known pre-existing substring quirk — documented, NOT fixed here
        assert normalize_license("CC-BY-NC-ND-4.0") == "cc-by-nc-4.0"

    def test_availability_open_for_public_active(self):
        assert build_nemar_source_record(mirror_detail())["availability"] == "open"
        d = mirror_detail(visibility="private")
        assert build_nemar_source_record(d)["availability"] is None

    def test_dates_parsed(self):
        src = build_nemar_source_record(mirror_detail())
        assert src["createdAt"] is not None
        assert src["lastUpdated"] is not None
        assert src["publishDate"] == "2026-06-20 00:36:53"

    def test_citations_in_publication(self):
        src = build_nemar_source_record(mirror_detail())
        assert src["publication"]["citations"] == 12
        assert src["publication"]["numDatasetCitations"] == 5
        assert src["publication"]["numDatapaperCitations"] == 7

    def test_raw_metadata_preserved_verbatim(self):
        d = mirror_detail()
        src = build_nemar_source_record(d)
        assert src["rawMetadata"] is d
        assert d["enrichment_json"] in src["rawMetadata"]["enrichment_json"]

    def test_unavailable_fields_stay_null(self):
        src = build_nemar_source_record(native_detail())
        assert src["species"] is None
        assert src["disease"] is None
        assert src["brainRegions"] is None
        assert src["studyType"] is None
        assert src["studyDesign"] is None
        assert src["sessions"] == []

    def test_missing_enrichment_is_not_fatal(self):
        d = mirror_detail()
        d["enrichment_json"] = None
        src = build_nemar_source_record(d)
        assert src["title"] == mirror_detail()["name"]
        assert src["keywords"] == []
        assert src["datasetType"] is None


class TestNemarCanonicalRecord:
    def test_native_canonical_record(self):
        src = build_nemar_source_record(native_detail())
        rec = canonical_record_from_source(src)
        assert rec["canonicalDatasetId"].startswith("ns-")
        assert rec["doi"] == "10.82901/nemar.nm000103"
        assert rec["sourceKeys"] == ["nemar:nm000103"]
        # native license CC-BY-NC-SA 4.0 → cc-by-nc-4.0 (pre-existing quirk)
        assert rec["license"] == "cc-by-nc-4.0"
        assert rec["ages"] is None
        assert rec["ageGroup"] == []
        assert list(rec["rawMetadata"].keys()) == ["nemar"]
        assert validate_canonical_record(rec) == []

    def test_native_identity_primary_is_concept_doi(self):
        src = build_nemar_source_record(native_detail())
        rec = canonical_record_from_source(src)
        assert rec["provenance"]["identity"]["primary"] == "doi:10.82901/nemar.nm000103"

    def test_mirror_canonical_record_never_uses_nemar_doi(self):
        """CASE E: 10.82901/nemar.on004504 must never be the canonical DOI —
        even when the mirror has no OpenNeuro record to merge into."""
        src = build_nemar_source_record(missing_mirror_detail())
        rec = canonical_record_from_source(src)
        assert rec["doi"] is None
        assert "10.82901/nemar.on007221" not in (rec["doi"] or "")
        assert validate_canonical_record(rec) == []

    def test_mirror_identity_is_stable(self):
        a = canonical_record_from_source(build_nemar_source_record(missing_mirror_detail()))
        b = canonical_record_from_source(build_nemar_source_record(missing_mirror_detail()))
        assert a["canonicalDatasetId"] == b["canonicalDatasetId"]

    def test_list_record_fallback_for_source_id(self):
        """Detail normally carries source/source_id; the list record is a
        fallback if a detail ever omits it."""
        d = mirror_detail()
        d.pop("source")
        d.pop("source_id")
        src = build_nemar_source_record(d, list_record("on004504", source="openneuro", source_id="ds004504"))
        assert src["doi"] is None
        assert "https://openneuro.org/datasets/ds004504" in src["publication"]["referencesAndLinks"]
        assert src["snapshot"]["openNeuroSourceId"] == "ds004504"


class TestNemarDoiBehavior:
    def test_normalize_doi_accepts_nemar_and_openneuro(self):
        assert normalize_doi("10.82901/nemar.nm000103") == "10.82901/nemar.nm000103"
        assert normalize_doi("10.82901/nemar.nm000103.v2.0.0") == "10.82901/nemar.nm000103.v2.0.0"
        assert normalize_doi("10.18112/openneuro.ds004504.v1.0.9") == "10.18112/openneuro.ds004504.v1.0.9"

    def test_nemar_namespace_never_equals_openneuro(self):
        assert normalize_doi("10.82901/nemar.on004504") != normalize_doi("10.18112/openneuro.ds004504.v1.0.9")
