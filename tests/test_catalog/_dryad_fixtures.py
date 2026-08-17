"""Shared Dryad census-artifact fixtures for catalog tests (pure data — no I/O).

Payload shapes mirror the live dryad_candidates.jsonl records produced by the
2026-08-17 census (trace_artifacts/dryad_census_20260817/dryad_candidates.jsonl).
"""


def dryad_record(**overrides) -> dict:
    """A realistic HIGH-confidence Dryad census record (hippocampal MRI dataset)."""
    d = {
        "identifier": "doi:10.5061/dryad.gc72v",
        "id": 105,
        "title": (
            "Data from: Multi-contrast submillimetric 3-Tesla hippocampal "
            "subfield segmentation protocol and dataset"
        ),
        "abstract": (
            "The hippocampus is composed of distinct anatomical subregions that "
            "participate in multiple cognitive processes and are differentially "
            "affected in prevalent neurological and psychiatric conditions. "
            "Advances in high-field MRI allow for the non-invasive identification "
            "of hippocampal substructure."
        ),
        "keywords": [],
        "fieldOfScience": None,
        "publicationDate": "2016-10-22",
        "lastModificationDate": "2020-06-24",
        "versionNumber": 1,
        "versionStatus": "submitted",
        "curationStatus": "Published",
        "visibility": "public",
        "license": "https://spdx.org/licenses/CC0-1.0.html",
        "storageSize": 9924107495,
        "relatedPublicationISSN": "2052-4463",
        "authors": [
            {
                "firstName": "Jessie",
                "lastName": "Kulaga-Yoskovitz",
                "orcid": None,
                "affiliation": "McGill University",
            },
            {
                "firstName": "Boris C.",
                "lastName": "Bernhardt",
                "orcid": "0000-0001-9200-6187",
                "affiliation": "McGill University",
            },
        ],
        "funders": [
            {"organization": "U.S. National Science Foundation", "awardNumber": None}
        ],
        "relatedWorks": [
            {
                "relationship": "primary_article",
                "identifierType": "DOI",
                "identifier": "https://doi.org/10.1038/sdata.2015.59",
            }
        ],
        "metrics": {"views": 1333, "downloads": 164, "citations": 0},
        "_matched": ["hippocampus", "neuroimaging", "neurosciences"],
        "_groups": ["brain_structure", "core_lexicon", "imaging_modalities"],
        "_noisy_only": False,
        "classification": "H",
        "reviewed": True,
    }
    for key, val in overrides.items():
        d[key] = val
    return d


def dryad_record_b(**overrides) -> dict:
    """A SECOND distinct HIGH-confidence record (EEG epilepsy dataset)."""
    d = dryad_record(
        identifier="doi:10.5061/dryad.4b8gtht9c",
        id=4_812_047,
        title=(
            "Data from: High-density EEG recordings from patients with "
            "temporal lobe epilepsy during a memory task"
        ),
        abstract=(
            "We provide high-density electroencephalography (EEG) recordings "
            "acquired from patients with drug-resistant temporal lobe epilepsy "
            "during a verbal memory encoding and retrieval task."
        ),
        publicationDate="2023-05-11",
        lastModificationDate="2024-01-30",
        versionNumber=2,
        versionStatus="published",
        relatedPublicationISSN="0013-9580",
        storageSize=12_345_678_901,
        authors=[
            {
                "firstName": "Amina",
                "lastName": "Zafar",
                "orcid": "0000-0002-1234-5678",
                "affiliation": "University College London",
            }
        ],
        relatedWorks=[
            {
                "relationship": "primary_article",
                "identifierType": "DOI",
                "identifier": "https://doi.org/10.1111/epi.17890",
            }
        ],
        _matched=["eeg", "epilepsy", "electrophysiology"],
        _groups=["electrophysiology_modalities", "neurological_disorders"],
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def medium_record(**overrides) -> dict:
    """A MEDIUM-confidence record — must NEVER enter ingestion."""
    d = dryad_record(
        identifier="doi:10.5061/dryad.kh189326t",
        id=77_002,
        title="Data from: Comparative genomics of brain-expressed genes across primates",
        classification="M",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def false_positive_record(**overrides) -> dict:
    """A FALSE-POSITIVE record (ecology) — must NEVER enter ingestion."""
    d = dryad_record(
        identifier="doi:10.5061/dryad.7m0cfxppz",
        id=91_331,
        title="Data from: Landscape connectivity and gene flow in riparian plants",
        abstract=(
            "We quantified landscape genetic connectivity across riparian "
            "corridors in three plant species."
        ),
        classification="F",
    )
    for key, val in overrides.items():
        d[key] = val
    return d
