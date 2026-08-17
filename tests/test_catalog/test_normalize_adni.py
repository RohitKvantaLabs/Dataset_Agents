"""ADNI normalization tests — pure builder behavior (no I/O).

Verifies build_adni_source_record() contract (Option A identity model):
  - deterministic stable-identifier identity (sourceDatasetId / sourceKeys)
  - sourceUrl = census source_url VERBATIM (never a synthetic path); shared
    official URLs stay shared and never collapse products into one dataset
  - sourceUrlNorm is None for ADNI — URL is NOT an identity signal
  - canonical identity derives from repository:sourceDatasetId, not the URL
  - canonical DOI always null (publication DOI quarantined to relationship)
  - strict modality mapping (MRI → mri, PET → pet; others preserved raw only)
  - phase/version handling preserved under snapshot/rawMetadata
  - rawMetadata.adni preserves the census record verbatim
"""
import json
from collections import Counter

import pytest

from app.catalog.dedup import evaluate_identity, source_identity
from app.catalog.normalize import (
    ADNI_CENSUS_EXPECTED,
    ADNI_REPOSITORY,
    build_adni_source_record,
    canonical_record_from_source,
)

from ._adni_fixtures import (
    ADNI_DOC_URL,
    ADNI_NEWS_URL,
    adni_doc_record,
    adni_doc_record_same_url,
    adni_news_shared_record,
    adni_news_shared_record_b,
    adni_record,
    adni_record_b,
    adni_records,
)

_real_records = json.load(
    open(
        r"..\trace_artifacts\adni_census_20260817\adni_census.json",
        encoding="utf-8",
    )
)["datasets"]


def _build(record: dict) -> dict:
    return build_adni_source_record(record)


# ─────────────────────────────────────────────────────────────────────────────
# Stable-identifier identity
# ─────────────────────────────────────────────────────────────────────────────


def test_stable_identifier_drives_source_identity():
    record = adni_record()
    source = _build(record)

    assert source["repository"] == ADNI_REPOSITORY
    assert source["sourceDatasetId"] == f"adni:{record['stable_identifier']}"
    # sourceKeys convention: repo:sourceDatasetId
    assert source_identity(source)["sourceKey"] == (
        f"adni:adni:{record['stable_identifier']}"
    )
    assert source["title"] == record["dataset_name"]


def test_source_identity_is_deterministic():
    a = _build(adni_record())
    b = _build(adni_record())
    assert source_identity(a) == source_identity(b)
    assert canonical_record_from_source(a)["canonicalDatasetId"] == (
        canonical_record_from_source(b)["canonicalDatasetId"]
    )


def test_distinct_products_get_distinct_canonical_ids():
    a = canonical_record_from_source(_build(adni_record()))
    b = canonical_record_from_source(_build(adni_record_b()))
    assert a["canonicalDatasetId"] != b["canonicalDatasetId"]


# ─────────────────────────────────────────────────────────────────────────────
# DOI: canonical always null; publication DOI stays relationship metadata
# ─────────────────────────────────────────────────────────────────────────────


def test_canonical_doi_is_always_null():
    for record in (_build(adni_record()), _build(adni_doc_record())):
        assert record["doi"] is None
        assert canonical_record_from_source(record)["doi"] is None


def test_publication_doi_quarantined_to_relationship_metadata():
    record = adni_doc_record(publication_doi="10.3233/JAD-221272")
    source = _build(record)

    assert source["doi"] is None                       # never canonical
    assert source["publication"]["articleDoi"] == "10.3233/jad-221272"
    assert source["publication"]["relatedIdentifiers"] == [
        {"identifier": "10.3233/jad-221272"}
    ]
    # preserved verbatim in rawMetadata (the census record, spec §11)
    assert source["rawMetadata"]["publication_doi"] == "10.3233/JAD-221272"
    # and the canonical record is still DOI-less
    assert canonical_record_from_source(source)["doi"] is None


# ─────────────────────────────────────────────────────────────────────────────
# sourceUrl — census URL verbatim; shared URLs are expected, NEVER identity
# ─────────────────────────────────────────────────────────────────────────────


def test_unique_url_kept_verbatim():
    record = adni_record()
    source = _build(record)
    assert source["sourceUrl"] == record["source_url"]
    assert source["documentationUrl"] == record["source_url"]


def test_source_url_is_verbatim_for_doc_products():
    for record in (adni_doc_record(), adni_doc_record_same_url()):
        source = _build(record)
        assert source["sourceUrl"] == record["source_url"]  # never synthesized
        assert source["documentationUrl"] == record["source_url"]


def test_url_is_not_an_adni_identity_signal():
    """ADNI sourceUrlNorm is always None — shared official URLs never merge
    co-located products (SHARED_SOURCE_URL_REPOSITORIES policy)."""
    for record in (
        adni_doc_record(),
        adni_doc_record_same_url(),
        adni_news_shared_record(),
        adni_news_shared_record_b(),
    ):
        assert source_identity(_build(record))["sourceUrlNorm"] is None
    # canonical provenance also records no URL identity for ADNI
    canonical = canonical_record_from_source(_build(adni_doc_record()))
    assert canonical["provenance"]["identity"]["sourceUrlNorm"] is None


def test_two_products_with_same_source_url_remain_distinct():
    """Exact same census URL (news index) ⇒ distinct datasets."""
    a = _build(adni_news_shared_record())
    b = _build(adni_news_shared_record_b())

    assert a["sourceUrl"] == b["sourceUrl"] == ADNI_NEWS_URL  # shared, verbatim
    assert a["sourceDatasetId"] != b["sourceDatasetId"]
    assert a["sourceDatasetId"] == f"adni:{adni_news_shared_record()['stable_identifier']}"
    assert b["sourceDatasetId"] == f"adni:{adni_news_shared_record_b()['stable_identifier']}"
    assert source_identity(a)["sourceKey"] != source_identity(b)["sourceKey"]

    ca = canonical_record_from_source(a)
    cb = canonical_record_from_source(b)
    assert ca["canonicalDatasetId"] != cb["canonicalDatasetId"]

    # shared URL must NOT trigger a merge
    assert evaluate_identity(ca, b)["match"] is False
    assert evaluate_identity(ca, b)["ambiguous"] is False


def test_two_products_on_same_doc_page_remain_distinct():
    """Co-located documentation pages (different anchors, same page) ⇒
    distinct datasets; both sourceUrls stay verbatim."""
    a = _build(adni_doc_record())
    b = _build(adni_doc_record_same_url())

    assert a["sourceUrl"] == adni_doc_record()["source_url"]
    assert b["sourceUrl"] == adni_doc_record_same_url()["source_url"]
    assert a["sourceUrl"] != b["sourceUrl"]  # anchors differ — both verbatim
    assert a["documentationUrl"] == ADNI_DOC_URL

    assert source_identity(a)["sourceUrlNorm"] is None
    assert source_identity(b)["sourceUrlNorm"] is None
    assert canonical_record_from_source(a)["canonicalDatasetId"] != (
        canonical_record_from_source(b)["canonicalDatasetId"]
    )
    assert evaluate_identity(canonical_record_from_source(a), b)["match"] is False


def test_same_stable_identity_resolves_to_same_canonical():
    """Same ADNI stable identity ⇒ same sourceDatasetId/sourceKey/canonical ID
    (idempotent rerun resolves to the same canonical dataset)."""
    a = _build(adni_record())
    b = _build(adni_record())

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
    """All 122 approved records: verbatim URLs only, zero synthetic URLs, and
    all 122 distinct canonical identities."""
    assert len(_real_records) == ADNI_CENSUS_EXPECTED
    canonical_ids = Counter()
    synthetic = []
    for r in _real_records:
        source = _build(r)
        if source["sourceUrl"] != r["source_url"]:
            synthetic.append(r["stable_identifier"])
        assert source_identity(source)["sourceUrlNorm"] is None
        canonical_ids[
            canonical_record_from_source(source)["canonicalDatasetId"]
        ] += 1

    assert synthetic == []  # no synthetic URL anywhere in the ADNI records
    assert all(count == 1 for count in canonical_ids.values())
    assert len(canonical_ids) == ADNI_CENSUS_EXPECTED


# ─────────────────────────────────────────────────────────────────────────────
# Modality — strict map only (MRI → mri, PET → pet), others preserved raw
# ─────────────────────────────────────────────────────────────────────────────


def test_modality_strict_mapping():
    assert _build(adni_record(modality="MRI"))["modality"] == ["mri"]
    assert _build(adni_record(modality="PET"))["modality"] == ["pet"]
    # labels outside the canonical vocab stay out of the canonical list
    source = _build(adni_record(modality="Biofluid"))
    assert source["modality"] == []
    assert source["modalityRaw"] == ["Biofluid"]
    assert source["snapshot"]["modalityLabel"] == "Biofluid"


# ─────────────────────────────────────────────────────────────────────────────
# Phase / version handling — ONE canonical record, history in snapshot/raw
# ─────────────────────────────────────────────────────────────────────────────


def test_phase_and_version_history_preserved_not_split():
    record = adni_record(
        phase="ADNI1, GO, 2, 3, 4",
        version_update_info="Includes ADNI1 1.5T/3T Standardized Image Collections.",
        first_announced="2007-10-03",
        last_update="2026-05-01",
    )
    source = _build(record)

    assert source["snapshot"]["phase"] == "ADNI1, GO, 2, 3, 4"
    assert source["snapshot"]["versionUpdateInfo"] == (
        "Includes ADNI1 1.5T/3T Standardized Image Collections."
    )
    assert source["snapshot"]["firstAnnounced"] == "2007-10-03"
    assert source["snapshot"]["lastUpdate"] == "2026-05-01"
    # one canonical record, never per-phase records
    assert len(canonical_record_from_source(source)["sources"]) == 1


# ─────────────────────────────────────────────────────────────────────────────
# rawMetadata.adni — census record preserved verbatim
# ─────────────────────────────────────────────────────────────────────────────


def test_raw_metadata_adni_preserves_census_record():
    record = adni_doc_record_same_url()
    source = _build(record)
    # on the source record, rawMetadata IS the census record (the canonical
    # record wraps it under rawMetadata.adni later — tested in the ingest file)
    for key in record:
        assert source["rawMetadata"][key] == record[key]


# ─────────────────────────────────────────────────────────────────────────────
# Controlled access — never reported open, never fabricated fields
# ─────────────────────────────────────────────────────────────────────────────


def test_controlled_access_never_reported_open():
    source = _build(adni_record())
    assert source["availability"] is None
    assert source["participantCount"] is None
    assert source["ages"] is None
    assert source["ageGroup"] is None
    assert source["disease"] is None
    assert source["brainRegions"] is None
    assert source["analysisMethods"] is None
    assert source["license"] is None
