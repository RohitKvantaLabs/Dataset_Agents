"""Shared EBRAINS census-artifact fixtures for catalog tests (pure data — no I/O).

Payload shapes mirror the live ebrains_candidates.jsonl records produced by
the 2026-08-18 census (trace_artifacts/ebrains_census_20260818/).
"""

# The four AUDITED exact DANDI/OpenNeuro matches (identity audit 2026-08-18,
# A-class). ``dataset_id`` is the stable EBRAINS Dataset ID; the census DOI is
# the external-repo dataset DOI; the canonical records already exist in the
# live catalog with these sourceKeys (DANDI doi=null, OpenNeuro doi set).
EBRAINS_EXACT_MATCHES = [
    {
        "dataset_id": "d278f032-f941-48a5-8ebb-0396ef12d610",
        "title": "Physiological Properties and Behavioral Correlates of Hippocampal Granule Cells and Mossy Cells (0.250624.0409)",
        "category": "HIGH",
        "confidence": "high",
        "neuro_relevance": "neuroscience",
        "doi": "10.48324/dandi.000003/0.250624.0409",
        "external_doi": True,
        "multi_version": False,
        "n_versions": 1,
        "n_indexed_versions": 1,
        "first_release": "2025-06-24",
        "latest_release": "2025-06-24",
        "accessibility": "free access",
        "species": "Mus musculus",
        "technique": "multi-electrode extracellular electrophysiology;spike sorting;temporal filtering",
        "experimental_approach": "behavior;electrophysiology",
        "keywords": "cell type;current source density analysis;granule neuron;laminar recordings;local field potential;mossy cells;multi-unit activity;optogenetics;oscillations",
        "version_ids": "d278f032-f941-48a5-8ebb-0396ef12d610",
        "url": "https://search.kg.ebrains.eu/instances/d278f032-f941-48a5-8ebb-0396ef12d610",
    },
    {
        "dataset_id": "eaf47ff8-1500-43cb-b238-25d36a895f13",
        "title": "Electrophysiology data from thalamic and cortical neurons during somatosensation (0.220126.1853)",
        "category": "HIGH",
        "confidence": "high",
        "neuro_relevance": "neuroscience",
        "doi": "10.48324/dandi.000005/0.220126.1853",
        "external_doi": True,
        "multi_version": False,
        "n_versions": 1,
        "n_indexed_versions": 1,
        "first_release": "2022-01-26",
        "latest_release": "2022-01-26",
        "accessibility": "free access",
        "species": "Mus musculus",
        "technique": "current clamp;spike sorting",
        "experimental_approach": "electrophysiology;optogenetics",
        "keywords": "membrane potential;multi-unit activity",
        "version_ids": "eaf47ff8-1500-43cb-b238-25d36a895f13",
        "url": "https://search.kg.ebrains.eu/instances/eaf47ff8-1500-43cb-b238-25d36a895f13",
    },
    {
        "dataset_id": "f63e7d6e-b305-4c54-b3a2-a364474ab80f",
        "title": "MPI-Leipzig Mind-Brain-Body dataset (v1.0.0)",
        "category": "HIGH",
        "confidence": "high",
        "neuro_relevance": "neuroscience",
        "doi": "10.18112/openneuro.ds000221.v1.0.0",
        "external_doi": True,
        "multi_version": False,
        "n_versions": 1,
        "n_indexed_versions": 1,
        "first_release": "2020-07-22",
        "latest_release": "2020-07-22",
        "accessibility": "free access",
        "species": "Homo sapiens",
        "technique": "diffusion-weighted imaging;functional magnetic resonance imaging;gradient-echo pulse sequence;image distortion correction;magnetic resonance imaging;spin echo pulse sequence;T1 pulse sequence;T2 pulse sequence",
        "experimental_approach": "behavior;neural connectivity;neuroimaging",
        "keywords": "cognitive phenotype;emotional phenotype;physiological phenotype",
        "version_ids": "f63e7d6e-b305-4c54-b3a2-a364474ab80f",
        "url": "https://search.kg.ebrains.eu/instances/f63e7d6e-b305-4c54-b3a2-a364474ab80f",
    },
    {
        "dataset_id": "4f6e1509-2e7f-44dd-a45c-c100cd7728a3",
        "title": "Decoding natural sounds in early 'visual' cortex of congenitally blind individuals (v1)",
        "category": "HIGH",
        "confidence": "high",
        "neuro_relevance": "neuroscience",
        "doi": "10.18112/openneuro.ds002715.v1.0.0",
        "external_doi": True,
        "multi_version": False,
        "n_versions": 1,
        "n_indexed_versions": 1,
        "first_release": "2020-06-10",
        "latest_release": "2020-06-10",
        "accessibility": "free access",
        "species": "Homo sapiens",
        "technique": "functional magnetic resonance imaging;magnetic resonance imaging;multi-voxel pattern analysis;natural sound auditory stimulation;reconstruction technique;retinotopic mapping;rigid image registration;rigid motion correction",
        "experimental_approach": "behavior;neuroimaging",
        "keywords": "auditory feedback;blind;brain decoding;visual imagery",
        "version_ids": "4f6e1509-2e7f-44dd-a45c-c100cd7728a3",
        "url": "https://search.kg.ebrains.eu/instances/4f6e1509-2e7f-44dd-a45c-c100cd7728a3",
    },
]

# The existing canonical records (as present in the live catalog) that the
# four exact matches must MERGE into — never duplicate.
EBRAINS_EXACT_CANONICALS = [
    {
        "canonicalDatasetId": "ns-4c610a2fdb7fd9a3",
        "sourceKeys": ["dandi:000003"],
        "doi": None,
        "sources": [
            {
                "repository": "dandi",
                "sourceDatasetId": "dandi:000003",
                "sourceUrl": "https://dandiarchive.org/dandiset/000003/draft",
                "title": "Physiological Properties and Behavioral Correlates of Hippocampal Granule Cells and Mossy Cells",
                "modality": [],
                "participantCount": None,
            }
        ],
        "provenance": {
            "identity": {
                "primary": "url:http://dandiarchive.org/dandiset/000003/draft",
                "matchedVia": "new",
                "doi": None,
                "sourceUrlNorm": "http://dandiarchive.org/dandiset/000003/draft",
            }
        },
    },
    {
        "canonicalDatasetId": "ns-bb8d7726f2f6bf79",
        "sourceKeys": ["dandi:000005"],
        "doi": None,
        "sources": [
            {
                "repository": "dandi",
                "sourceDatasetId": "dandi:000005",
                "sourceUrl": "https://dandiarchive.org/dandiset/000005/draft",
                "title": "Electrophysiology data from thalamic and cortical neurons during somatosensation",
                "modality": [],
                "participantCount": None,
            }
        ],
        "provenance": {
            "identity": {
                "primary": "url:http://dandiarchive.org/dandiset/000005/draft",
                "matchedVia": "new",
                "doi": None,
                "sourceUrlNorm": "http://dandiarchive.org/dandiset/000005/draft",
            }
        },
    },
    {
        "canonicalDatasetId": "ns-78c1f5deb1e86111",
        "sourceKeys": ["openneuro:ds000221"],
        "doi": "10.18112/openneuro.ds000221.v1.0.0",
        "sources": [
            {
                "repository": "openneuro",
                "sourceDatasetId": "datasets/ds000221",
                "sourceUrl": "https://openneuro.org/datasets/ds000221",
                "title": "MPI-Leipzig Mind-Brain-Body dataset",
                "modality": [],
                "participantCount": None,
            }
        ],
        "provenance": {
            "identity": {
                "primary": "doi:10.18112/openneuro.ds000221.v1.0.0",
                "matchedVia": "new",
                "doi": "10.18112/openneuro.ds000221.v1.0.0",
                "sourceUrlNorm": "http://openneuro.org/datasets/ds000221",
            }
        },
    },
    {
        "canonicalDatasetId": "ns-1273531292419a1e",
        "sourceKeys": ["openneuro:ds002715"],
        "doi": "10.18112/openneuro.ds002715.v1.0.0",
        "sources": [
            {
                "repository": "openneuro",
                "sourceDatasetId": "datasets/ds002715",
                "sourceUrl": "https://openneuro.org/datasets/ds002715",
                "title": "Decoding natural sounds in early 'visual' cortex of congenitally blind individuals",
                "modality": [],
                "participantCount": None,
            }
        ],
        "provenance": {
            "identity": {
                "primary": "doi:10.18112/openneuro.ds002715.v1.0.0",
                "matchedVia": "new",
                "doi": "10.18112/openneuro.ds002715.v1.0.0",
                "sourceUrlNorm": "http://openneuro.org/datasets/ds002715",
            }
        },
    },
]


def ebrains_record(**overrides) -> dict:
    """A realistic approved EBRAINS census record (native dataset DOI)."""
    dataset_id = "9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d"
    d = {
        "dataset_id": dataset_id,
        "source": "EBRAINS",
        "title": "MOBILE: Multimodal Whole Brain Imaging in Epilepsy",
        "category": "HIGH",
        "confidence": "high",
        "neuro_relevance": "neuroscience",
        "doi": "10.25493/RPSQ-END",
        "external_doi": False,
        "multi_version": True,
        "n_versions": 2,
        "n_indexed_versions": 1,
        "first_release": "2024-03-15",
        "latest_release": "2026-07-14",
        "accessibility": "free access",
        "species": "Homo sapiens",
        "technique": "functional magnetic resonance imaging;diffusion-weighted imaging;electroencephalography;magnetic resonance imaging",
        "experimental_approach": "behaviour;neuroimaging",
        "keywords": "epilepsy;seizure;multimodal;EEG-fMRI",
        "version_ids": "67eea200-0033-4bae-8dde-9d9f2ed463af",
        "url": f"https://search.kg.ebrains.eu/instances/{dataset_id}",
    }
    for key, val in overrides.items():
        d[key] = val
    return d


def ebrains_record_b(**overrides) -> dict:
    """A SECOND distinct approved EBRAINS census record."""
    d = ebrains_record(
        dataset_id="b2c3d4e5-6f7a-8b9c-0d1e-2f3a4b5c6d7e",
        title="Cell Atlas of the Mouse Primary Motor Cortex",
        doi="10.25493/ABCD-123",
        multi_version=False,
        n_versions=1,
        n_indexed_versions=1,
        first_release="2022-01-10",
        latest_release="2022-01-10",
        accessibility="controlled access",
        species="Mus musculus",
        technique="single-cell RNA sequencing;transcriptomics",
        experimental_approach="transcriptomics",
        keywords="cell atlas;cortex;single cell",
        version_ids="b2c3d4e5-6f7a-8b9c-0d1e-2f3a4b5c6d7e",
        url="https://search.kg.ebrains.eu/instances/b2c3d4e5-6f7a-8b9c-0d1e-2f3a4b5c6d7e",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ebrains_external_record(**overrides) -> dict:
    """An approved EBRAINS record whose DOI is an EXTERNAL-repo DOI (G-Node)."""
    dataset_id = "7757e057-1234-4abc-9def-0123456789ab"
    d = ebrains_record(
        dataset_id=dataset_id,
        title="Extracellular recordings of mouse somatosensory cortex",
        doi="10.12751/g-node.l7xbnd",
        external_doi=True,
        multi_version=False,
        n_versions=1,
        n_indexed_versions=1,
        first_release="2023-05-01",
        latest_release="2023-05-01",
        accessibility="free access",
        technique="multi-electrode extracellular electrophysiology;spike sorting",
        experimental_approach="electrophysiology",
        keywords="extracellular;cortex",
        version_ids=dataset_id,
        url=f"https://search.kg.ebrains.eu/instances/{dataset_id}",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ebrains_publication_record(**overrides) -> dict:
    """An approved EBRAINS record whose DOI is a PUBLICATION DOI (C-class)."""
    d = ebrains_external_record(
        dataset_id="13e6ec48-1234-4abc-9def-0123456789ab",
        title="Publication-linked dataset",
        doi="10.1371/journal.pbio.3000678",
        technique="optical imaging;microscopy",
        experimental_approach="optogenetics",
        url="https://search.kg.ebrains.eu/instances/13e6ec48-1234-4abc-9def-0123456789ab",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ebrains_osf_record(**overrides) -> dict:
    """An approved EBRAINS record whose DOI is an OSF PROJECT DOI (D-class)."""
    d = ebrains_external_record(
        dataset_id="5706c36d-1234-4abc-9def-0123456789ab",
        title="OSF project container",
        doi="10.17605/OSF.IO/DVMRB",
        technique="behavioral task",
        experimental_approach="behaviour",
        url="https://search.kg.ebrains.eu/instances/5706c36d-1234-4abc-9def-0123456789ab",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ebrains_multiversion_record(**overrides) -> dict:
    """An approved multi-version EBRAINS parent (n_total_versions > 1)."""
    d = ebrains_record(
        dataset_id="c3d4e5f6-7a8b-9c0d-1e2f-3a4b5c6d7e8f",
        title="Multi-version Dataset (release tranches)",
        doi="10.25493/MULT-001",
        multi_version=True,
        n_versions=5,
        n_indexed_versions=2,
        first_release="2019-01-01",
        latest_release="2025-12-31",
        version_ids="c3d4e5f6-7a8b-9c0d-1e2f-3a4b5c6d7e8f;d4e5f6a7-8b9c-0d1e-2f3a-4b5c6d7e8f90",
        url="https://search.kg.ebrains.eu/instances/c3d4e5f6-7a8b-9c0d-1e2f-3a4b5c6d7e8f",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ebrains_records(n: int) -> list[dict]:
    """``n`` distinct approved EBRAINS records with unique dataset identities.

    Used to build exactly-sized ingestion inputs (the runner requires EXACTLY
    ``EBRAINS_CENSUS_EXPECTED`` records). Deterministic; each record gets a
    unique dataset_id and unique source URL.
    """
    out: list[dict] = []
    for i in range(1, n + 1):
        dataset_id = f"{i:08d}-0000-4000-8000-000000000000"
        out.append(
            ebrains_record(
                dataset_id=dataset_id,
                title=f"EBRAINS dataset {i}",
                doi=f"10.25493/EBR{1000 + i:04d}",
                multi_version=(i % 7 == 0),
                n_versions=3 if i % 7 == 0 else 1,
                n_indexed_versions=1,
                first_release="2021-02-14",
                latest_release="2025-06-30",
                version_ids=dataset_id,
                url=f"https://search.kg.ebrains.eu/instances/{dataset_id}",
            )
        )
    return out
