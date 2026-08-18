"""UK Biobank normalization tests — pure builder behavior (no I/O).

Verifies build_ukbiobank_source_record() contract:
  - deterministic stable-identifier identity (sourceDatasetId / sourceKeys)
  - sourceUrl = census source_url VERBATIM (never a synthetic path); shared
    Showcase URLs stay shared and never collapse products into one dataset
  - sourceUrlNorm is None for UK Biobank — URL is NOT an identity signal
  - canonical identity derives from repository:sourceDatasetId, not the URL
  - canonical DOI always null (publication DOI quarantined to relationship)
  - strict modality mapping (MRI → mri; non-imaging labels preserved raw only)
  - child Data-Fields stay METADATA on the parent product (never datasets)
  - IDP families remain grouped under their parent product
  - release/version handling preserved under snapshot/rawMetadata (never
    separate datasets)
  - rawMetadata.ukbiobank preserves the census record verbatim
"""
import json
from collections import Counter

from app.catalog.dedup import evaluate_identity, source_identity
from app.catalog.normalize import (
    UKB_CENSUS_EXPECTED,
    UKB_REPOSITORY,
    build_ukbiobank_source_record,
    canonical_record_from_source,
)

from ._ukbiobank_fixtures import (
    UKB_SHARED_URL,
    UKB_SHOWCASE,
    ukb_nonimaging_record,
    ukb_record,
    ukb_record_b,
    ukb_shared_url_record,
    ukb_shared_url_record_b,
)

_real_candidates = [
    json.loads(line)
    for line in open(
        r"..\trace_artifacts\ukbiobank_census_20260817\ukbiobank_candidates.jsonl",
        encoding="utf-8",
    )
    if line.strip()
]


def _build(record: dict) -> dict:
    return build_ukbiobank_source_record(record)


# ─────────────────────────────────────────────────────────────────────────────
# Stable category/product identity
# ─────────────────────────────────────────────────────────────────────────────


def test_stable_identifier_drives_source_identity():
    record = ukb_record()
    source = _build(record)

    assert source["repository"] == UKB_REPOSITORY
    assert source["sourceDatasetId"] == f"ukbiobank:{record['stable_identifier']}"
    # sourceKeys convention: repo:sourceDatasetId
    assert source_identity(source)["sourceKey"] == (
        f"ukbiobank:ukbiobank:{record['stable_identifier']}"
    )
    assert source["title"] == record["name"]


def test_source_identity_is_deterministic():
    a = _build(ukb_record())
    b = _build(ukb_record())
    assert source_identity(a) == source_identity(b)
    assert canonical_record_from_source(a)["canonicalDatasetId"] == (
        canonical_record_from_source(b)["canonicalDatasetId"]
    )


def test_distinct_products_get_distinct_canonical_ids():
    a = canonical_record_from_source(_build(ukb_record()))
    b = canonical_record_from_source(_build(ukb_record_b()))
    assert a["canonicalDatasetId"] != b["canonicalDatasetId"]


# ─────────────────────────────────────────────────────────────────────────────
# DOI: canonical always null; publication DOI stays relationship metadata
# ─────────────────────────────────────────────────────────────────────────────


def test_canonical_doi_is_always_null():
    for record in (_build(ukb_record()), _build(ukb_nonimaging_record())):
        assert record["doi"] is None
        assert canonical_record_from_source(record)["doi"] is None


def test_publication_doi_quarantined_to_relationship_metadata():
    record = ukb_record(publication_doi="10.1101/2025.01.01.25320000")
    source = _build(record)

    assert source["doi"] is None                       # never canonical
    assert source["publication"]["articleDoi"] == "10.1101/2025.01.01.25320000"
    assert source["publication"]["relatedIdentifiers"] == [
        {"identifier": "10.1101/2025.01.01.25320000"}
    ]
    # preserved verbatim in rawMetadata (the census record, spec §11)
    assert source["rawMetadata"]["publication_doi"] == "10.1101/2025.01.01.25320000"
    # and the canonical record is still DOI-less
    assert canonical_record_from_source(source)["doi"] is None


# ─────────────────────────────────────────────────────────────────────────────
# sourceUrl — census URL verbatim; shared Showcase URLs are NEVER identity
# ─────────────────────────────────────────────────────────────────────────────


def test_source_url_kept_verbatim():
    record = ukb_record()
    source = _build(record)
    assert source["sourceUrl"] == record["source_url"] == UKB_SHOWCASE + "110"
    assert source["documentationUrl"] == record["source_url"]


def test_url_is_not_a_ukbiobank_identity_signal():
    """sourceUrlNorm is always None — shared Showcase pages never merge
    co-located products (SHARED_SOURCE_URL_REPOSITORIES policy)."""
    for record in (ukb_record(), ukb_shared_url_record(), ukb_shared_url_record_b()):
        assert source_identity(_build(record))["sourceUrlNorm"] is None
    canonical = canonical_record_from_source(_build(ukb_record()))
    assert canonical["provenance"]["identity"]["sourceUrlNorm"] is None


def test_two_products_with_same_source_url_remain_distinct():
    """Exact same census URL (shared docs index) ⇒ distinct datasets."""
    a = _build(ukb_shared_url_record())
    b = _build(ukb_shared_url_record_b())

    assert a["sourceUrl"] == b["sourceUrl"] == UKB_SHARED_URL  # shared, verbatim
    assert a["sourceDatasetId"] != b["sourceDatasetId"]
    assert a["sourceDatasetId"] == f"ukbiobank:{ukb_shared_url_record()['stable_identifier']}"
    assert b["sourceDatasetId"] == f"ukbiobank:{ukb_shared_url_record_b()['stable_identifier']}"
    assert source_identity(a)["sourceKey"] != source_identity(b)["sourceKey"]

    ca = canonical_record_from_source(a)
    cb = canonical_record_from_source(b)
    assert ca["canonicalDatasetId"] != cb["canonicalDatasetId"]

    # shared URL must NOT trigger a merge
    assert evaluate_identity(ca, b)["match"] is False
    assert evaluate_identity(ca, b)["ambiguous"] is False


def test_same_stable_identity_resolves_to_same_canonical():
    """Same stable identity ⇒ same sourceDatasetId/sourceKey/canonical ID
    (idempotent rerun resolves to the same canonical dataset)."""
    a = _build(ukb_record())
    b = _build(ukb_record())

    assert a["sourceDatasetId"] == b["sourceDatasetId"]
    assert source_identity(a)["sourceKey"] == source_identity(b)["sourceKey"]
    assert canonical_record_from_source(a)["canonicalDatasetId"] == (
        canonical_record_from_source(b)["canonicalDatasetId"]
    )
    assert evaluate_identity(canonical_record_from_source(a), b)["match"] is True
    assert evaluate_identity(canonical_record_from_source(a), b)["matchedVia"] == (
        "source_key"
    )


def test_full_census_set_distinct_canonical_identities_and_no_synthetic_urls():
    """All 21 approved records: verbatim URLs only, zero synthetic URLs, and
    all 21 distinct canonical identities (category-level products)."""
    assert len(_real_candidates) == UKB_CENSUS_EXPECTED
    canonical_ids = Counter()
    synthetic = []
    for r in _real_candidates:
        source = _build(r)
        if source["sourceUrl"] != r["source_url"]:
            synthetic.append(r["stable_identifier"])
        assert source_identity(source)["sourceUrlNorm"] is None
        assert source["sourceUrl"].startswith("https://biobank.ndph.ox.ac.uk/ukb/")
        canonical_ids[
            canonical_record_from_source(source)["canonicalDatasetId"]
        ] += 1

    assert synthetic == []  # no synthetic URL anywhere in the UK Biobank records
    assert all(count == 1 for count in canonical_ids.values())
    assert len(canonical_ids) == UKB_CENSUS_EXPECTED


# ─────────────────────────────────────────────────────────────────────────────
# Modality — strict map only (MRI → mri), non-imaging labels preserved raw
# ─────────────────────────────────────────────────────────────────────────────


def test_modality_strict_mapping():
    assert _build(ukb_record(modality="MRI"))["modality"] == ["mri"]
    # non-imaging labels outside the canonical vocab stay out of the canonical list
    source = _build(ukb_nonimaging_record(modality="Cognition / Neuropsychological tests"))
    assert source["modality"] == []
    assert source["modalityRaw"] == ["Cognition / Neuropsychological tests"]
    assert source["snapshot"]["modalityLabel"] == "Cognition / Neuropsychological tests"


# ─────────────────────────────────────────────────────────────────────────────
# Child Data-Fields stay METADATA — never separate datasets
# ─────────────────────────────────────────────────────────────────────────────


def test_child_data_fields_are_metadata_not_datasets():
    record = ukb_record()
    source = _build(record)

    # the full child Data-Field ID list lives on the parent product snapshot
    assert source["snapshot"]["childDataFieldIds"] == record["child_data_field_ids"]
    assert source["snapshot"]["fieldCount"] == record["field_count"]
    assert source["snapshot"]["bulkFieldCount"] == record["bulk_field_count"]
    assert source["snapshot"]["derivedFieldCount"] == record["derived_field_count"]
    # …and the census record (with the child-field list) is preserved verbatim
    assert source["rawMetadata"]["child_data_field_ids"] == record["child_data_field_ids"]
    # ONE canonical record per product — child fields never become records
    canonical = canonical_record_from_source(source)
    assert len(canonical["sources"]) == 1
    assert len(canonical["sourceKeys"]) == 1


def test_idp_families_remain_grouped_under_parent_product():
    """Thousands of brain IDPs group under their modality product: the T1
    product carries 1,445 child fields (incl. IDPs) as ONE canonical record."""
    record = ukb_record(field_count=1445, child_data_field_ids=[f"2500{i}" for i in range(1445)])
    source = _build(record)
    assert source["snapshot"]["fieldCount"] == 1445
    assert len(source["snapshot"]["childDataFieldIds"]) == 1445
    canonical = canonical_record_from_source(source)
    assert len(canonical["sources"]) == 1  # one product, not 1,445 datasets
    # field count is METADATA — never confused with a dataset count
    assert source["snapshot"]["fieldCount"] != 1


# ─────────────────────────────────────────────────────────────────────────────
# Release / version handling — ONE canonical record, history in snapshot/raw
# ─────────────────────────────────────────────────────────────────────────────


def test_releases_do_not_become_separate_datasets():
    record = ukb_record(
        version_release_information=(
            "Field debut 2015-10-09; successive imaging data releases update "
            "the same product - e.g. Mar 2025 tranche +15,000 participants, "
            "Nov 2025 tranche +20,800 to ~90,000 (release tranches are NOT "
            "separate datasets)."
        )
    )
    source = _build(record)

    assert source["snapshot"]["versionReleaseInformation"] == (
        record["version_release_information"]
    )
    assert source["rawMetadata"]["version_release_information"] == (
        record["version_release_information"]
    )
    # one canonical record, never per-release records
    assert len(canonical_record_from_source(source)["sources"]) == 1


def test_dataset_unit_rationale_and_classification_preserved():
    source = _build(ukb_record())
    assert source["snapshot"]["classification"] == "dataset"
    assert source["snapshot"]["datasetUnitRationale"] == (
        ukb_record()["dataset_unit_rationale"]
    )
    assert source["snapshot"]["accessLevel"].startswith("Controlled access")


# ─────────────────────────────────────────────────────────────────────────────
# rawMetadata.ukbiobank — census record preserved verbatim
# ─────────────────────────────────────────────────────────────────────────────


def test_raw_metadata_ukbiobank_preserves_census_record():
    record = ukb_nonimaging_record()
    source = _build(record)
    # on the source record, rawMetadata IS the census record (the canonical
    # record wraps it under rawMetadata.ukbiobank later — tested in the ingest file)
    for key in record:
        assert source["rawMetadata"][key] == record[key]


# ─────────────────────────────────────────────────────────────────────────────
# Controlled access — never reported open, never fabricated fields
# ─────────────────────────────────────────────────────────────────────────────


def test_controlled_access_never_reported_open():
    source = _build(ukb_record())
    assert source["availability"] is None
    assert source["ages"] is None
    assert source["ageGroup"] is None
    assert source["disease"] is None
    assert source["brainRegions"] is None
    assert source["analysisMethods"] is None
    assert source["license"] is None
    assert source["authors"] == []


def test_participant_count_is_documented_showcase_number():
    source = _build(ukb_record())
    assert source["participantCount"] == 87637
    assert source["snapshot"]["participantCount"] == 87637
    canonical = canonical_record_from_source(source)
    assert canonical["participantCount"] == 87637
