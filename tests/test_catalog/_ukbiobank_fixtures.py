"""Shared UK Biobank census-artifact fixtures for catalog tests (pure data — no I/O).

Payload shapes mirror the live ukbiobank_candidates.jsonl records produced by
the 2026-08-17 census (trace_artifacts/ukbiobank_census_20260817/).
"""

# Real public UK Biobank Showcase category-page pattern (biobank.ndph.ox.ac.uk).
UKB_SHOWCASE = "https://biobank.ndph.ox.ac.uk/ukb/label.cgi?id="

# A shared documentation URL reused by two distinct products (Showcase pages
# describe related metadata — the URL alone is never identity).
UKB_SHARED_URL = "https://biobank.ndph.ox.ac.uk/ukb/docs.cgi?id=1"


def ukb_record(**overrides) -> dict:
    """A realistic approved UK Biobank census record (T1 structural brain MRI)."""
    d = {
        "name": "T1 structural brain MRI",
        "stable_identifier": "ukbiobank-brain-mri-t1-structural",
        "category": "Brain MRI / T1 structural",
        "category_ids": ["110", "1101", "1102", "190", "191", "192", "193"],
        "modality": "MRI",
        "child_data_field_ids": ["20252", "20263", "25000", "25001", "25731"],
        "field_count": 1445,
        "bulk_field_count": 2,
        "derived_field_count": 1445,
        "participant_count": 87637,
        "source_url": UKB_SHOWCASE + "110",
        "access_level": (
            "Controlled access. Metadata and field definitions are public via the "
            "UK Biobank Showcase; participant-level data are only accessible to "
            "registered researchers on approved applications through UKB-RAP."
        ),
        "classification": "dataset",
        "dataset_unit_rationale": (
            "Dataset unit = Showcase category-level product: one acquisition+"
            "processing domain with its own stable Showcase category ID(s), "
            "images and IDP family. Child Data-Fields (individual regional "
            "measurements, IDPs and file pointers) are variables of this product."
        ),
        "description": (
            "T1-weighted structural brain MRI: raw DICOM, processed NIFTI images, "
            "surface model files, structural segmentations, and T1 image-derived "
            "phenotypes."
        ),
        "doi": None,
        "exclusion_reason": None,
        "metadata_note": (
            "No dataset-level DOI is published by UK Biobank for this product; "
            "publication DOIs are relationship metadata only (doi=null)."
        ),
        "version_release_information": (
            "Field debut 2015-10-09; Showcase field version stamp 2025-08-24 "
            "(successive imaging data releases update the same product - release "
            "tranches are NOT separate datasets)."
        ),
    }
    for key, val in overrides.items():
        d[key] = val
    return d


def ukb_record_b(**overrides) -> dict:
    """A SECOND distinct approved UK Biobank product (Diffusion brain MRI)."""
    d = ukb_record(
        name="Diffusion brain MRI",
        stable_identifier="ukbiobank-brain-mri-diffusion",
        category="Brain MRI / Diffusion",
        category_ids=["107", "134", "135"],
        modality="MRI",
        child_data_field_ids=["20250", "25750", "25751"],
        field_count=690,
        bulk_field_count=1,
        derived_field_count=688,
        participant_count=85662,
        source_url=UKB_SHOWCASE + "107",
        description="Diffusion-weighted brain MRI: raw data and diffusion tensor imaging-derived phenotypes.",
        version_release_information=(
            "Field debut 2015-10-09; successive imaging data releases update the "
            "same product (release tranches are NOT separate datasets)."
        ),
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ukb_nonimaging_record(**overrides) -> dict:
    """A non-imaging neuroscience product (Cognitive function assessments).

    Modality label is outside the canonical vocab (never coerced to a modality).
    """
    d = ukb_record(
        name="Cognitive function assessments (touchscreen & online)",
        stable_identifier="ukbiobank-cognitive-function",
        category="Cognition",
        category_ids=["100026", "100027", "100028", "501", "502"],
        modality="Cognition / Neuropsychological tests",
        child_data_field_ids=["20016", "20023", "20127", "399"],
        field_count=211,
        bulk_field_count=0,
        derived_field_count=23,
        participant_count=498507,
        source_url=UKB_SHOWCASE + "100026",
        description=(
            "Cognitive function assessments: touchscreen reaction time, pairs "
            "matching, fluid intelligence, and online follow-up cognitive tests."
        ),
        version_release_information=(
            "Field debut 2012-01-05; Showcase field version stamp 2025-08-30 "
            "(assessment waves update the same product)."
        ),
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ukb_shared_url_record(**overrides) -> dict:
    """A product whose census source_url is the shared Returns/docs index page."""
    d = ukb_record(
        name="Nervous system disorders (first occurrences)",
        stable_identifier="ukbiobank-nervous-system-disorders",
        category="Health outcomes / Neurological",
        category_ids=["2406"],
        modality="Clinical outcomes",
        child_data_field_ids=["131036", "131037"],
        field_count=136,
        bulk_field_count=0,
        derived_field_count=136,
        participant_count=28900,
        source_url=UKB_SHARED_URL,
        description="Algorithmically-defined first occurrences of nervous system disorders.",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ukb_shared_url_record_b(**overrides) -> dict:
    """A SECOND product sharing the EXACT SAME source URL (distinct identity)."""
    d = ukb_shared_url_record(
        name="Sleep",
        stable_identifier="ukbiobank-sleep",
        category="Sleep",
        category_ids=["205", "206"],
        modality="Sleep / chronotype questionnaires",
        child_data_field_ids=["1160", "1170"],
        field_count=193,
        bulk_field_count=0,
        derived_field_count=0,
        participant_count=501091,
        description="Sleep duration, chronotype and sleep-related questionnaire data.",
    )
    for key, val in overrides.items():
        d[key] = val
    return d


def ukb_records(n: int) -> list[dict]:
    """``n`` distinct approved UK Biobank records with unique stable identities.

    Used to build exactly-sized ingestion inputs (the runner requires EXACTLY
    ``UKB_CENSUS_EXPECTED`` records). Deterministic; each record gets a unique
    stable_identifier and a unique per-product Showcase category URL.
    """
    out: list[dict] = []
    for i in range(1, n + 1):
        out.append(
            ukb_record(
                name=f"UK Biobank product {i}",
                stable_identifier=f"ukbiobank-product-{i}",
                category=f"Category {i}",
                category_ids=[str(1000 + i)],
                modality="MRI",
                child_data_field_ids=[f"field-{i}-1", f"field-{i}-2"],
                field_count=2,
                bulk_field_count=1,
                derived_field_count=2,
                participant_count=1000 + i,
                source_url=UKB_SHOWCASE + str(1000 + i),
                description=f"Approved UK Biobank dataset-level product {i}.",
                version_release_information=(
                    "Successive data releases update the same product "
                    "(release tranches are NOT separate datasets)."
                ),
            )
        )
    return out
