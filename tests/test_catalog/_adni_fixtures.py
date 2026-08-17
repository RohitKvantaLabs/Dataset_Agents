"""Shared ADNI census-artifact fixtures for catalog tests (pure data — no I/O).

Payload shapes mirror the live adni_census.json ``datasets[]`` records produced
by the 2026-08-17 census (trace_artifacts/adni_census_20260817/adni_census.json).
"""

# A documentation-derived product that shares the bioflood page with others.
ADNI_DOC_URL = "https://adni.loni.usc.edu/quick-start-guide-asset101625/bioflood.html#other-biofluid-biomarker-tables"

# A news/announcement index URL shared by many products.
ADNI_NEWS_URL = "https://adni.loni.usc.edu/news-publications/news/"


def adni_record(**overrides) -> dict:
    """A realistic approved ADNI census record (news-derived MRI product)."""
    d = {
        "dataset_name": "ADNI MRI DICOM Image Archive (raw + preprocessed images)",
        "stable_identifier": "adni-adni-mri-dicom-image-archive-(raw-+-preprocessed-images)",
        "category": "MRI",
        "modality": "MRI",
        "phase": "ADNI1, GO, 2, 3, 4",
        "description": (
            "The complete ADNI structural/functional MRI image archive in DICOM "
            "(all sequences), raw through pre/post-processed, passed QC/protocol "
            "compliance."
        ),
        "source_url": "https://adni.loni.usc.edu/data-samples/adni-data/neuroimaging/mri/",
        "first_announced": "2007-10-03",
        "last_update": "2026-05-01",
        "version_update_info": (
            "Includes ADNI1 1.5T/3T Standardized Image Collections "
            "(curation subsets, not separate data products)."
        ),
        "publication_doi": None,
        "access_level": (
            "Controlled-access (LONI IDA; ADNI DUA + DPC approval). "
            "Metadata publicly visible via ADNI news/data pages."
        ),
        "metadata_fields_available": [
            "dataset_name",
            "description",
            "phase",
            "modality",
            "source_url",
        ],
        "classification": "dataset",
        "exclusion_reason": None,
    }
    for key, val in overrides.items():
        d[key] = val
    return d


def adni_record_b(**overrides) -> dict:
    """A SECOND distinct approved ADNI product (PET news-derived, unique URL)."""
    d = adni_record(
        dataset_name="ADNI Amyloid PET Image Archive",
        stable_identifier="adni-adni-amyloid-pet-image-archive",
        category="PET",
        modality="PET",
        description="The complete ADNI amyloid PET image archive.",
        source_url="https://adni.loni.usc.edu/data-samples/adni-data/neuroimaging/pet/",
        first_announced="2009-08-01",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def adni_doc_record(**overrides) -> dict:
    """A documentation-derived biofluid product sharing ADNI_DOC_URL.

    Mirrors the real ``adni-diadem---alzosure-predict-(plasma-u-p53az)`` record:
    a stable identity + shared documentation URL + a publication DOI that must
    stay relationship metadata (never canonical identity).
    """
    d = adni_record(
        dataset_name="ADNI Diadem / AlzoSure Predict (plasma P-tau, U-p53AZ)",
        stable_identifier="adni-diadem---alzosure-predict-(plasma-u-p53az)",
        category="Biomarker",
        modality="Biofluid",
        phase="ADNI3, 4",
        description=(
            "ADNI Diadem / AlzoSure Predict plasma biomarker panel: P-tau217 and "
            "U-p53AZ results. Documentation-derived product (biofluid biomarker "
            "tables)."
        ),
        source_url=ADNI_DOC_URL,
        documentation_section="bioflood.html#other-biofluid-biomarker-tables",
        first_announced=None,
        publication_doi="10.3233/JAD-221272",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def adni_doc_record_same_url(**overrides) -> dict:
    """A SECOND documentation product sharing the SAME bioflood page.

    Mirrors ``adni-csf-local-lab-results-(protein,-glucose,-wbc/rbc-counts)``
    (bioflood.html#important-note). Distinct stable identity, same page base as
    adni_doc_record() after normalization — must never collapse.
    """
    d = adni_record(
        dataset_name="ADNI CSF Local Lab Results (protein, glucose, WBC/RBC counts)",
        stable_identifier="adni-csf-local-lab-results-(protein,-glucose,-wbc/rbc-counts)",
        category="Biomarker",
        modality="Biofluid",
        phase="ADNI1, GO, 2, 3, 4",
        description=(
            "ADNI CSF local laboratory results: protein, glucose, WBC and RBC "
            "counts. Documentation-derived product (biofluid important-note)."
        ),
        source_url="https://adni.loni.usc.edu/quick-start-guide-asset101625/bioflood.html#important-note",
        documentation_section="bioflood.html#important-note",
        first_announced=None,
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def adni_news_shared_record(**overrides) -> dict:
    """A news-derived product whose census URL is the shared news index page.

    Mirrors the real ``adni-blennow-lab--csf-gap-43`` record: many products
    share ``news-publications/news/`` after normalization.
    """
    d = adni_record(
        dataset_name="ADNI Blennow Lab CSF GAP-43",
        stable_identifier="adni-blennow-lab--csf-gap-43",
        category="Biomarker",
        modality="Biofluid",
        phase="ADNI1, GO, 2, 3",
        description="ADNI CSF growth-associated protein 43 (GAP-43) results.",
        source_url=ADNI_NEWS_URL,
        first_announced="2024-06-12",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def adni_news_shared_record_b(**overrides) -> dict:
    """A SECOND product sharing the same news index URL (distinct identity)."""
    d = adni_news_shared_record(
        dataset_name="ADNI Janssen Plasma P-tau217+ Simoa Assay",
        stable_identifier="adni-janssen-plasma-p217-+-tau-simoa-assay-[adni2,3]",
        category="Biomarker",
        modality="Biofluid",
        phase="ADNI2, 3",
        description="ADNI Janssen plasma P-tau217 plus tau Simoa assay results.",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def adni_records(n: int) -> list[dict]:
    """``n`` distinct approved ADNI records with unique stable identities/URLs.

    Used to build exactly-sized ingestion inputs (the runner requires EXACTLY
    ``ADNI_CENSUS_EXPECTED`` records). Deterministic; each record gets a unique
    stable_identifier and a unique per-product source URL.
    """
    out: list[dict] = []
    for i in range(1, n + 1):
        out.append(
            adni_record(
                dataset_name=f"ADNI product {i}",
                stable_identifier=f"adni-product-{i}",
                description=f"Approved ADNI dataset-level product {i}.",
                source_url=f"https://adni.loni.usc.edu/data-samples/adni-data/product-{i}/",
                first_announced="2020-01-01",
            )
        )
    return out
