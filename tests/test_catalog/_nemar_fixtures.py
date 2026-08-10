"""Shared NEMAR fixtures for catalog tests (pure data — no I/O).

Payload shapes mirror the LIVE api.nemar.org/datasets/{id} responses captured
during the 2026-08-10 investigation (detail_on004504.api.json etc.).
"""


def mirror_detail(**overrides) -> dict:
    """A realistic OpenNeuro-mirror detail payload (on004504 → ds004504)."""
    d = {
        "dataset_id": "on004504",
        "id": "on004504",
        "name": "A dataset of EEG recordings from: Alzheimer's disease, Frontotemporal dementia and Healthy subjects",
        "description": "This dataset comprises resting-state, eyes-closed scalp EEG recordings from 88 subjects.",
        "status": "active",
        "visibility": "public",
        "github_repo": "nemarDatasets/on004504",
        "concept_doi": "10.82901/nemar.on004504",
        "latest_version_doi": "10.82901/nemar.on004504.v1.0.0",
        "created_at": "2026-06-20 00:29:41",
        "updated_at": "2026-07-10 22:32:39",
        "source": "openneuro",
        "source_id": "ds004504",
        "subject_count": 88,
        "modalities": "eeg",
        "age_min": 44,
        "age_max": 79,
        "file_size": 5781001822,
        "total_files": 361,
        "tasks": "eyesclosed",
        "authors": "Cassani, Raymundo, Estarellas, Martín, San-Martin, Rodrigo, Falk, Tiago H., Fraiwan, Luay",
        "license": "CC0",
        "readme": "[![DOI](https://img.shields.io/badge/DOI-10.82901%2Fnemar.on004504-blue)](https://doi.org/10.82901/nemar.on004504)\nThis dataset contains the EEG rest...",
        "bids_version": "v1.2.1",
        "sessions_count": None,
        "publish_date": "2026-06-20 00:36:53",
        "file_size_formatted": "5.38 GB",
        "n_channels": 19,
        "electrode_system": "10-20",
        "has_hed": 0,
        "hed_version": None,
        "num_citations": 12,
        "num_dataset_citations": 5,
        "num_datapaper_citations": 7,
        "participants": 88,
        "latest_version": "v1.0.0",
        "enrichment_json": (
            '{"version":"2.0","pipeline_stage":"validated",'
            '"title":"A dataset of EEG recordings from: Alzheimer\'s disease, Frontotemporal dementia and Healthy subjects",'
            '"description":"This dataset comprises resting-state, eyes-closed scalp EEG recordings from 88 subjects.",'
            '"license":"CC0","dataset_type":"raw",'
            '"modalities":["eeg"],'
            '"keywords":[{"term":"EEG"},{"term":"dementia"}],'
            '"related_identifiers":['
            '{"identifier":"10.18112/openneuro.ds004504.v1.0.9","identifier_type":"DOI","relation_type":"IsDerivedFrom"},'
            '{"identifier":"10.1007/s11571-026-10464-w","identifier_type":"DOI","relation_type":"References"}'
            '],'
            '"resource_type_specific":"EEG Dataset",'
            '"authors":{"Cassani, Raymundo":{"orcid":"0000-0001-1111-2222","affiliations":[{"name":"Unit","identifier":null,"scheme":null}]}}'
            "}"
        ),
    }
    for key, val in overrides.items():
        d[key] = val
    return d


def native_detail(**overrides) -> dict:
    """A realistic NEMAR-native detail payload (nm000103 — no source_id)."""
    d = {
        "dataset_id": "nm000103",
        "id": "nm000103",
        "name": "Healthy Brain Network EEG - Not for Commercial Use",
        "description": "HBN-EEG NC - Healthy Brain Network EEG data",
        "status": "active",
        "visibility": "public",
        "github_repo": "nemarDatasets/nm000103",
        "concept_doi": "10.82901/nemar.nm000103",
        "latest_version_doi": "10.82901/nemar.nm000103.v2.0.0",
        "created_at": "2026-01-19 03:25:23",
        "updated_at": "2026-07-10 21:42:34",
        "source": None,
        "source_id": None,
        "subject_count": 447,
        "modalities": "eeg",
        "age_min": 5.0059,
        "age_max": 21.8166,
        "file_size": 268698800306,
        "total_files": 21142,
        "tasks": "DespicableMe,DiaryOfAWimpyKid,FunwithFractals,RestingState,ThePresent",
        "authors": "Seyed Yahya Shirazi, Alexandre Franco",
        "license": "CC-BY-NC-SA 4.0",
        "readme": "README for HBN EEG",
        "bids_version": "1.9.0",
        "sessions_count": None,
        "publish_date": None,
        "n_channels": 129,
        "electrode_system": "Custom",
        "has_hed": 1,
        "hed_version": "8.3.0",
        "num_citations": 215,
        "participants": 447,
        "latest_version": "v2.0.0",
        "enrichment_json": (
            '{"version":"2.0","pipeline_stage":"validated",'
            '"title":"Healthy Brain Network EEG - Not for Commercial Use",'
            '"description":"The Healthy Brain Network EEG dataset comprises high-resolution electroencephalogram recordings.",'
            '"license":"CC-BY-NC-SA 4.0","dataset_type":"raw",'
            '"modalities":["eeg"],'
            '"keywords":[{"term":"EEG"},{"term":"child and adolescent mental health"}],'
            '"related_identifiers":[{"identifier":"10.1038/sdata.2017.181","identifier_type":"DOI","relation_type":"References"}],'
            '"resource_type_specific":"EEG Dataset",'
            '"authors":{'
            '"Seyed Yahya Shirazi":{"orcid":"0000-0001-5557-259X","affiliations":[{"name":"UCSD","identifier":"https://ror.org/0168r3w48","scheme":"ROR"}]},'
            '"Alexandre Franco":{"orcid":"0000-0002-1552-1090","affiliations":[{"name":"Child Mind Institute","identifier":null,"scheme":null}]}'
            '}'
            "}"
        ),
    }
    for key, val in overrides.items():
        d[key] = val
    return d


def missing_mirror_detail(**overrides) -> dict:
    """Mirror whose OpenNeuro source (ds007221) is NOT in our catalog snapshot."""
    d = mirror_detail()
    d.update(
        {
            "dataset_id": "on007221",
            "id": "on007221",
            "name": "A NEMAR mirror whose OpenNeuro snapshot is not yet cataloged",
            "concept_doi": "10.82901/nemar.on007221",
            "latest_version_doi": "10.82901/nemar.on007221.v1.0.0",
            "github_repo": "nemarDatasets/on007221",
            "source": "openneuro",
            "source_id": "ds007221",
            "participants": 40,
            "subject_count": 40,
            "tasks": "rest",
            "latest_version": "v1.0.0",
            "enrichment_json": (
                '{"version":"2.0","pipeline_stage":"validated",'
                '"title":"A NEMAR mirror whose OpenNeuro snapshot is not yet cataloged",'
                '"license":"CC0","dataset_type":"raw","modalities":["eeg"],'
                '"related_identifiers":['
                '{"identifier":"10.18112/openneuro.ds007221.v1.0.0","identifier_type":"DOI","relation_type":"IsDerivedFrom"}'
                '],"resource_type_specific":"EEG Dataset"}'
            ),
        }
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def list_record(dataset_id: str, source=None, source_id=None) -> dict:
    """A rich NEMAR API list record."""
    return {
        "dataset_id": dataset_id,
        "id": dataset_id,
        "name": f"Dataset {dataset_id}",
        "description": None,
        "status": "active",
        "visibility": "public",
        "github_repo": f"nemarDatasets/{dataset_id}",
        "concept_doi": f"10.82901/nemar.{dataset_id}",
        "doi": f"10.82901/nemar.{dataset_id}",
        "created_at": "2026-06-20 00:29:41",
        "updated_at": "2026-07-10 22:32:39",
        "source": source,
        "source_id": source_id,
        "modalities": "eeg",
        "participants": 20,
        "tasks": None,
        "authors": "A. Author",
        "license": "CC0",
        "file_size": 1000,
        "total_files": 5,
        "n_channels": 19,
        "electrode_system": "10-20",
        "has_hed": 0,
        "hed_version": None,
        "num_citations": 0,
        "data_complete": 1,
        "bytes_present": 900,
        "source_type": "managed",
        "latest_version": "v1.0.0",
    }
