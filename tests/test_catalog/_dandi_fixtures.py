"""Shared DANDI fixtures for catalog tests (pure data — no I/O)."""


def version_record(**overrides) -> dict:
    """A realistic DANDI draft VERSION endpoint payload (shape verified live)."""
    v = {
        "identifier": "DANDI:000003",
        "version": "draft",
        "name": "Physiological Properties and Behavioral Correlates",
        "description": "Extracellular recordings during a behavioral task.",
        "url": "https://dandiarchive.org/dandiset/000003/draft",
        "dateCreated": "2021-04-07T23:31:12.831000+00:00",
        "dateModified": "2023-06-20T00:56:18.859500+00:00",
        "datePublished": None,
        "doi": None,
        "license": ["spdx:CC-BY-4.0"],
        "keywords": ["electrophysiology", "hippocampus", "sleep"],
        "access": [
            {"status": "dandi:OpenAccess", "schemaKey": "AccessRequirements"}
        ],
        "contributor": [
            {
                "name": "Doe, Jane",
                "roleName": ["dcite:Author"],
                "schemaKey": "Person",
                "includeInCitation": True,
            },
            {
                "name": "Funder, Phil",
                "roleName": ["dcite:Funder"],
                "schemaKey": "Organization",
                "includeInCitation": False,
            },
            {
                "name": "Maintainer, A.",
                "roleName": ["dcite:ContactPerson"],
                "schemaKey": "Person",
                "includeInCitation": False,
            },
        ],
        "about": [
            {
                "name": "Hippocampus",
                "schemaKey": "Anatomy",
                "identifier": "http://purl.obolibrary.org/obo/UBERON_0002421",
            },
            {
                "name": "Alzheimer's disease",
                "schemaKey": "Disorder",
                "identifier": "http://purl.obolibrary.org/obo/DOID_10652",
            },
        ],
        "relatedResource": [
            {
                "url": "https://pubmed.ncbi.nlm.nih.gov/30502044/",
                "name": "High-Density Polymer Probe Recordings",
                "relation": "dcite:IsDescribedBy",
                "identifier": "DOI: 10.1016/j.neuron.2018.11.002",
            }
        ],
        "assetsSummary": {
            "approach": [
                {"name": "electrophysiological approach", "schemaKey": "ApproachType"},
                {"name": "behavioral approach", "schemaKey": "ApproachType"},
            ],
            "measurementTechnique": [
                {"name": "spike sorting technique", "schemaKey": "MeasurementTechniqueType"},
                {"name": "behavioral technique", "schemaKey": "MeasurementTechniqueType"},
            ],
            "dataStandard": [
                {"name": "Neurodata Without Borders (NWB)", "identifier": "RRID:SCR_015242"}
            ],
            "species": [
                {
                    "name": "House mouse",
                    "identifier": "http://purl.obolibrary.org/obo/NCBITaxon_10090",
                }
            ],
            "numberOfSubjects": 16,
            "numberOfBytes": 2559248010229,
            "numberOfFiles": 101,
            "variableMeasured": ["LFP", "Units", "Position"],
        },
        "citation": "A citation string",
    }
    for key, val in overrides.items():
        v[key] = val
    return v


def list_record(identifier="000003", name="Real DANDI title", published=None) -> dict:
    """A realistic DANDI /dandisets/ list record (no rich metadata)."""
    return {
        "identifier": identifier,
        "created": "2021-04-07T23:31:12.831000Z",
        "modified": "2023-06-20T00:56:18.859500Z",
        "contact_person": "Someone",
        "draft_version": {
            "version": "draft",
            "name": name,
            "asset_count": 42,
            "size": 123456,
            "status": "Valid",
            "created": "2021-04-07T23:31:12.831000Z",
            "modified": "2023-06-20T00:56:18.859500Z",
        },
        "most_recent_published_version": published,
        "embargo_status": "open",
        "star_count": 0,
        "is_starred": False,
    }


def published_version(version="0.260218.2052", doi="10.48324/dandi.000003/0.260218.2052") -> dict:
    return {
        "version": version,
        "name": "Physiological Properties and Behavioral Correlates",
        "created": "2022-05-10T00:00:00.000000+00:00",
        "modified": "2022-05-10T00:00:00.000000+00:00",
        "status": "Published",
        "size": 2559248010229,
        "doi": doi,
    }
