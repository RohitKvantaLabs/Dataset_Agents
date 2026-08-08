"""Unit tests for catalog normalization (pure mapping, no I/O)."""

from app.catalog.normalize import (
    build_source_record,
    canonical_record_from_source,
    normalize_modalities,
    normalize_species,
)


def _openneuro_node(**overrides):
    node = {
        "id": "ds000001",
        "name": "ds001",
        "created": "2016-10-12T23:40:22.670Z",
        "publishDate": "2016-10-12T23:40:22.670Z",
        "public": True,
        "uploader": {"id": "u1", "name": "Uploader"},
        "metadata": {
            "datasetId": "ds000001",
            "datasetName": "Balloon Analog Risk-taking Task",
            "datasetUrl": None,
            "modalities": ["mri"],
            "species": "Human",
            "ages": [26, 24, 27, 20, 22],
            "studyDesign": None,
            "studyDomain": None,
            "studyLongitudinal": None,
            "tasksCompleted": ["balloon analog risk task"],
            "trialCount": None,
            "associatedPaperDOI": None,
            "grantIdentifier": None,
            "grantFunderName": None,
            "dataProcessed": None,
            "dxStatus": None,
            "firstSnapshotCreatedAt": "2018-07-14T01:16:46.922Z",
            "latestSnapshotCreatedAt": "2020-05-14T14:56:55.000Z",
        },
        "latestSnapshot": {
            "id": "ds000001:1.0.0",
            "tag": "1.0.0",
            "created": "2020-05-14T14:56:55.000Z",
            "hexsha": "f8e27ac909e50b5b5e311f6be271f0b1757ebb7b",
            "size": 2416199965,
            "readme": "README text",
            "description": {
                "Name": "Balloon Analog Risk-taking Task",
                "Authors": ["Tom Schonberg", "Russell A. Poldrack"],
                "BIDSVersion": "1.0.0",
                "DatasetDOI": "10.18112/openneuro.ds000001.v1.0.0",
                "DatasetType": "raw",
                "EthicsApprovals": None,
                "Funding": None,
                "HowToAcknowledge": None,
                "License": "CC0",
                "ReferencesAndLinks": ["some ref"],
                "SeniorAuthor": "Russell A. Poldrack",
            },
            "contributors": [
                {"name": "Tom Schonberg", "givenName": None, "familyName": None,
                 "contributorType": "Researcher", "orcid": None}
            ],
            "summary": {
                "dataProcessed": False,
                "modalities": ["mri"],
                "primaryModality": "mri",
                "secondaryModalities": ["mri_functional", "mri_structural"],
                "sessions": [],
                "size": 2416199965,
                "subjects": ["01", "02", "03", "04", "05"],
                "tasks": ["balloon analog risk task"],
                "totalFiles": 133,
            },
            "related": None,
        },
        "snapshots": [{"id": "ds000001:1.0.0", "tag": "1.0.0", "created": "2020-05-14T14:56:55.000Z"}],
    }
    for k, v in overrides.items():
        if k == "metadata":
            node["metadata"].update(v)
        elif k == "latestSnapshot":
            node["latestSnapshot"].update(v)
        else:
            node[k] = v
    return node


class TestBuildSourceRecord:
    def test_basic_mapping(self):
        src = build_source_record(_openneuro_node())
        assert src["repository"] == "openneuro"
        assert src["sourceDatasetId"] == "ds000001"
        assert src["sourceUrl"] == "https://openneuro.org/datasets/ds000001"
        assert src["title"] == "Balloon Analog Risk-taking Task"
        assert src["doi"] == "10.18112/openneuro.ds000001.v1.0.0"
        assert src["license"] == "CC0"  # raw preserved
        assert src["licenseNormalized"] == "cc0"
        assert src["authors"] == ["Tom Schonberg", "Russell A. Poldrack"]
        assert src["modality"] == ["mri"]
        assert src["species"] == ["human"]
        assert src["datasetType"] == "raw"
        # OpenNeuro has no structured study-type field: studyType stays null
        # (honest) while free-text studyDesign is preserved.
        assert src["studyType"] is None
        assert src["studyDesign"] is None  # node fixture has no studyDesign

    def test_derived_fields(self):
        src = build_source_record(_openneuro_node())
        assert src["participantCount"] == 5
        assert src["ageGroup"] == ["Adults"]
        assert src["ages"] == [26, 24, 27, 20, 22]
        assert src["derived"]["sizeLabel"] == "2.3 GB"
        assert src["derived"]["publicationYear"] == 2016
        assert src["derived"]["availability"] == "open"

    def test_license_pddl_long_text(self):
        node = _openneuro_node()
        node["latestSnapshot"]["description"]["License"] = (
            "This dataset is made available under the Public Domain Dedication and License "
            "v1.0, whose full text can be found at http://www.opendatacommons.org/licenses/pddl/1.0/"
        )
        src = build_source_record(node)
        assert src["license"] != "cc0"
        assert src["licenseNormalized"] == "pddl"

    def test_missing_metadata_is_null_not_fabricated(self):
        node = _openneuro_node()
        node["metadata"]["species"] = None
        node["metadata"]["ages"] = None
        node["metadata"]["modalities"] = None
        node["latestSnapshot"]["description"]["DatasetDOI"] = None
        node["latestSnapshot"]["description"]["License"] = None
        src = build_source_record(node)
        assert src["species"] is None
        assert src["ages"] is None
        assert src["ageGroup"] is None
        assert src["doi"] is None
        assert src["license"] is None
        assert src["licenseNormalized"] is None
        assert src["disease"] is None  # never invented
        assert src["brainRegions"] is None  # never invented
        assert src["keywords"] == []
        assert src["description"] is None

    def test_raw_metadata_preserved_verbatim(self):
        node = _openneuro_node()
        src = build_source_record(node)
        assert src["rawMetadata"]["id"] == "ds000001"
        assert src["rawMetadata"]["metadata"]["modalities"] == ["mri"]

    def test_provenance_direct_derived_split(self):
        src = build_source_record(_openneuro_node())
        assert src["provenance"]["source"] == "openneuro"
        assert "participantCount" in src["provenance"]["derived"]
        assert "title" in src["provenance"]["direct"]
        assert "ageGroup" in src["provenance"]["derived"]

    def test_ages_and_age_group_consistent(self):
        node = _openneuro_node()
        node["metadata"]["ages"] = [8, 30, 70]
        src = build_source_record(node)
        assert set(src["ageGroup"]) == {"Children", "Adults", "Older Adults"}


class TestModalitySpeciesNormalization:
    def test_modality_synonyms(self):
        assert normalize_modalities(["MRI"]) == ["mri"]
        assert normalize_modalities(["magnetoencephalography"]) == ["meg"]
        assert normalize_modalities(["beh", "mri"]) == ["beh", "mri"]  # unknown preserved

    def test_modality_drops_numeric_bids_indices(self):
        # Live regression: OpenNeuro summary.modalities leaks BIDS file/subject
        # indices ("01", "13") — never modalities.
        assert normalize_modalities(["mri", "01", "13", "events"]) == ["mri", "events"]

    def test_species_normalization(self):
        assert normalize_species("Human") == ["human"]
        assert normalize_species("Mouse") == ["mouse"]
        assert normalize_species(None) is None

    def test_species_blank_is_absent(self):
        # Live regression: OpenNeuro returns species="" for some datasets —
        # blank must count as absent, never as populated.
        assert normalize_species("") is None
        assert normalize_species(["Human", ""]) == ["human"]


class TestCanonicalRecord:
    def test_canonical_from_source(self):
        src = build_source_record(_openneuro_node())
        rec = canonical_record_from_source(src)
        assert rec["canonicalDatasetId"].startswith("ns-")
        assert rec["doi"] == "10.18112/openneuro.ds000001.v1.0.0"
        assert rec["license"] == "cc0"
        assert rec["sourceKeys"] == ["openneuro:ds000001"]
        assert rec["sources"][0]["repository"] == "openneuro"
        assert "openneuro" in rec["rawMetadata"]
        assert rec["provenance"]["identity"]["matchedVia"] == "new"

    def test_canonical_id_stable_across_runs(self):
        src1 = build_source_record(_openneuro_node())
        src2 = build_source_record(_openneuro_node())
        assert (
            canonical_record_from_source(src1)["canonicalDatasetId"]
            == canonical_record_from_source(src2)["canonicalDatasetId"]
        )
