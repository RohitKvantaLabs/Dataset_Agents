"""Unit tests for DANDI → canonical source normalization (pure logic, no I/O)."""

from app.catalog.normalize import (
    build_dandi_source_record,
    build_source_record,
    canonical_record_from_source,
    _dandi_id,
    _dandi_modality,
)
from app.catalog.schema import (
    normalize_doi,
    normalize_license,
    validate_canonical_record,
)

from ._dandi_fixtures import list_record, published_version, version_record


class TestDandiSourceRecord:
    def test_identity_fields(self):
        src = build_dandi_source_record(version_record())
        assert src["repository"] == "dandi"
        assert src["sourceDatasetId"] == "000003"
        assert src["sourceUrl"] == "https://dandiarchive.org/dandiset/000003/draft"

    def test_source_id_normalization_dandi_prefix(self):
        v = version_record(identifier="DANDI:000003")
        assert build_dandi_source_record(v)["sourceDatasetId"] == "000003"
        v2 = version_record(identifier="000003")
        assert build_dandi_source_record(v2)["sourceDatasetId"] == "000003"
        assert _dandi_id("DANDI:000123") == "000123"
        assert _dandi_id(None) is None

    def test_title_and_description(self):
        src = build_dandi_source_record(version_record())
        assert src["title"] == "Physiological Properties and Behavioral Correlates"
        assert src["description"].startswith("Extracellular recordings")

    def test_license_spdx_normalized_raw_preserved(self):
        src = build_dandi_source_record(version_record())
        assert src["license"] == "spdx:CC-BY-4.0"          # raw preserved
        assert src["licenseNormalized"] == "cc-by-4.0"     # canonical normalized
        assert normalize_license("spdx:CC0-1.0") == "cc0"
        assert normalize_license("spdx:PDDL-1.0") == "pddl"
        assert normalize_license("spdx:CC-BY-NC-4.0") == "cc-by-nc-4.0"

    def test_malformed_license_kept_raw_not_fabricated(self):
        src = build_dandi_source_record(version_record(license=["some-garbage"]))
        assert src["license"] == "some-garbage"
        assert src["licenseNormalized"] is None            # canonical stays unavailable

    def test_contributor_role_separation(self):
        src = build_dandi_source_record(version_record())
        # dcite:Author → authors[]
        assert src["authors"] == ["Doe, Jane"]
        # funders and other roles → contributors[] with roles preserved
        names = [c["name"] for c in src["contributors"]]
        assert "Funder, Phil" in names
        assert "Maintainer, A." in names
        funder = next(c for c in src["contributors"] if c["name"] == "Funder, Phil")
        assert funder["roleName"] == ["dcite:Funder"]
        assert "Doe, Jane" not in names

    def test_species_prefers_taxonomy_identifier(self):
        src = build_dandi_source_record(version_record())
        assert src["species"] == ["NCBITaxon_10090"]
        # equivalent display names collapse to the same taxon
        v = version_record()
        v["assetsSummary"]["species"] = [
            {"name": "Mus musculus - House mouse", "identifier": "http://purl.obolibrary.org/obo/NCBITaxon_10090"},
            {"name": "House mouse", "identifier": "http://purl.obolibrary.org/obo/NCBITaxon_10090"},
        ]
        assert build_dandi_source_record(v)["species"] == ["NCBITaxon_10090"]

    def test_species_without_identifier_uses_name(self):
        v = version_record()
        v["assetsSummary"]["species"] = [{"name": "House mouse"}]
        assert build_dandi_source_record(v)["species"] == ["House mouse"]

    def test_participant_count_direct_source_value(self):
        src = build_dandi_source_record(version_record())
        assert src["participantCount"] == 16
        assert src["derived"]["participantCount"] == 16

    def test_dataset_size_bytes(self):
        src = build_dandi_source_record(version_record())
        assert src["datasetSizeBytes"] == 2559248010229
        assert src["derived"]["sizeLabel"] == "2.3 TB"

    def test_file_count_preserved(self):
        src = build_dandi_source_record(version_record())
        assert src["snapshot"]["totalFiles"] == 101

    def test_modality_derivation(self):
        src = build_dandi_source_record(version_record())
        assert src["modality"] == ["electrophysiology", "behavior"]
        assert _dandi_modality([], []) == []

    def test_modality_optogenetics_and_imaging(self):
        v = version_record()
        v["assetsSummary"]["approach"] = [
            {"name": "optogenetic approach", "schemaKey": "ApproachType"},
            {"name": "microscopy approach; cell population imaging", "schemaKey": "ApproachType"},
        ]
        v["assetsSummary"]["measurementTechnique"] = [
            {"name": "two-photon microscopy technique", "schemaKey": "MeasurementTechniqueType"},
        ]
        assert set(build_dandi_source_record(v)["modality"]) == {"optogenetics", "imaging"}

    def test_no_reliable_mapping_leaves_modality_empty(self):
        v = version_record()
        v["assetsSummary"]["approach"] = [{"name": "unknown approach", "schemaKey": "ApproachType"}]
        v["assetsSummary"]["measurementTechnique"] = []
        assert build_dandi_source_record(v)["modality"] == []

    def test_anatomy_to_brain_regions(self):
        src = build_dandi_source_record(version_record())
        assert src["brainRegions"] == ["Hippocampus"]

    def test_disorder_to_disease(self):
        src = build_dandi_source_record(version_record())
        assert src["disease"] == ["Alzheimer's disease"]

    def test_about_entries_do_not_leak_across_schema_keys(self):
        v = version_record()
        v["about"] = [
            {"name": "Hippocampus", "schemaKey": "Anatomy"},
            {"name": "Behavioral task", "schemaKey": "ExperimentalApproach"},
        ]
        src = build_dandi_source_record(v)
        assert src["brainRegions"] == ["Hippocampus"]
        assert src["disease"] is None

    def test_placeholder_doi_rejected(self):
        v = version_record(doi="10.48324/dandi.000003/draft")
        src = build_dandi_source_record(v)
        assert src["doi"] is None          # placeholder never stored as canonical DOI

    def test_published_doi_preserved_as_provenance_only(self):
        pub = published_version()
        src = build_dandi_source_record(version_record(), published=pub)
        assert src["doi"] is None                          # not canonical identity
        assert src["snapshot"]["publishedDoi"] == "10.48324/dandi.000003/0.260218.2052"
        assert src["publication"]["publishedDoi"] == "10.48324/dandi.000003/0.260218.2052"
        assert src["snapshot"]["publishedVersion"]["version"] == "0.260218.2052"

    def test_draft_version_handling(self):
        src = build_dandi_source_record(version_record())
        assert src["snapshot"]["version"] == "draft"
        assert src["snapshot"]["latestTag"] == "draft"
        assert src["snapshot"]["versionIdentifier"] == "DANDI:000003"

    def test_publication_year_only_from_real_date_published(self):
        src = build_dandi_source_record(version_record(datePublished="2024-03-01T00:00:00+00:00"))
        assert src["derived"]["publicationYear"] == 2024
        # dateCreated is NOT a publication date
        src2 = build_dandi_source_record(version_record(dateCreated="2021-04-07T23:31:12+00:00"))
        assert src2["derived"]["publicationYear"] is None

    def test_last_updated_from_date_modified_only(self):
        src = build_dandi_source_record(version_record())
        assert src["lastUpdated"] is not None             # dateModified present
        v = version_record(dateModified=None)
        assert build_dandi_source_record(v)["lastUpdated"] is None  # never dateCreated
        assert build_dandi_source_record(v)["createdAt"] is not None

    def test_access_open(self):
        src = build_dandi_source_record(version_record())
        assert src["availability"] == "open"
        v = version_record(access=[{"status": "dandi:EmbargoedAccess", "schemaKey": "AccessRequirements"}])
        assert build_dandi_source_record(v)["availability"] is None

    def test_keywords_preserved_and_not_invented(self):
        src = build_dandi_source_record(version_record())
        assert src["keywords"] == ["electrophysiology", "hippocampus", "sleep"]
        empty = build_dandi_source_record(version_record(keywords=[]))
        assert empty["keywords"] == []

    def test_missing_metadata_stays_null(self):
        v = version_record()
        v["assetsSummary"]["species"] = []
        v["assetsSummary"]["numberOfSubjects"] = None
        v["assetsSummary"]["numberOfBytes"] = None
        v["about"] = []
        v["license"] = []
        v["contributor"] = []
        src = build_dandi_source_record(v)
        assert src["species"] is None
        assert src["participantCount"] is None
        assert src["datasetSizeBytes"] is None
        assert src["disease"] is None
        assert src["brainRegions"] is None
        assert src["license"] is None
        assert src["authors"] == []
        assert src["ages"] is None
        assert src["ageGroup"] is None
        assert src["studyType"] is None
        assert src["sessions"] == []
        assert src["tasks"] == []

    def test_dataset_type_not_derived_from_data_standard(self):
        src = build_dandi_source_record(version_record())
        assert src["datasetType"] is None          # NWB is NOT a datasetType
        assert src["dataStandard"][0]["name"] == "Neurodata Without Borders (NWB)"

    def test_variable_measured_and_related_resource_preserved(self):
        src = build_dandi_source_record(version_record())
        assert src["variableMeasured"] == ["LFP", "Units", "Position"]
        assert src["relatedResource"][0]["relation"] == "dcite:IsDescribedBy"
        assert src["publication"]["relatedResource"][0]["name"].startswith("High-Density")

    def test_raw_version_payload_preserved_verbatim(self):
        v = version_record()
        src = build_dandi_source_record(v)
        assert src["rawMetadata"] is v
        assert src["rawMetadata"]["assetsSummary"]["numberOfFiles"] == 101

    def test_provenance_direct_derived_split(self):
        src = build_dandi_source_record(version_record())
        assert src["provenance"]["source"] == "dandi"
        assert "participantCount" in src["provenance"]["derived"]
        assert "title" in src["provenance"]["direct"]
        assert src["provenance"]["sourceApi"] == "dandiarchive.org"


class TestDandiCanonicalRecord:
    def test_canonical_wrapping(self):
        src = build_dandi_source_record(version_record())
        rec = canonical_record_from_source(src)
        assert rec["canonicalDatasetId"].startswith("ns-")
        assert rec["sourceKeys"] == ["dandi:000003"]
        assert rec["title"] == "Physiological Properties and Behavioral Correlates"
        assert rec["license"] == "cc-by-4.0"
        assert rec["doi"] is None
        assert rec["keywords"] == ["electrophysiology", "hippocampus", "sleep"]
        assert rec["participantCount"] == 16
        assert rec["datasetSizeBytes"] == 2559248010229
        assert rec["brainRegions"] == ["Hippocampus"]
        assert rec["disease"] == ["Alzheimer's disease"]
        assert rec["modality"] == ["electrophysiology", "behavior"]

    def test_raw_metadata_keyed_by_repository(self):
        src = build_dandi_source_record(version_record())
        rec = canonical_record_from_source(src)
        assert list(rec["rawMetadata"].keys()) == ["dandi"]
        assert rec["rawMetadata"]["dandi"]["identifier"] == "DANDI:000003"

    def test_canonical_id_stable_across_runs(self):
        a = canonical_record_from_source(build_dandi_source_record(version_record()))
        b = canonical_record_from_source(build_dandi_source_record(version_record()))
        assert a["canonicalDatasetId"] == b["canonicalDatasetId"]

    def test_valid_canonical_record(self):
        src = build_dandi_source_record(version_record())
        rec = canonical_record_from_source(src)
        assert validate_canonical_record(rec) == []


class TestDandiIdListExtraction:
    def test_normalize_list_id(self):
        from app.catalog.ingest import _normalize_dandi_list_id

        assert _normalize_dandi_list_id("DANDI:000003") == "000003"
        assert _normalize_dandi_list_id("000003") == "000003"
        assert _normalize_dandi_list_id(None) is None
        assert _normalize_dandi_list_id("") is None

    def test_list_record_shape_used(self):
        """The list endpoint carries NO rich metadata — the id comes only from
        ``identifier`` (regression guard against reading science from the list)."""
        rec = list_record(identifier="000623", name="Some title")
        assert rec["identifier"] == "000623"
        assert "metadata" not in rec
        assert rec["draft_version"]["name"] == "Some title"
