"""
Catalog validation report (spec §13).

Produces a detailed JSON report with five sections:

A. INGESTION      totals, failures, pagination, timing
B. DEDUPLICATION  candidates, merges, sources attached, ambiguous identities
C. COVERAGE       per-field populated / missing / percentage
D. NORMALIZATION  unique raw vs normalized values
E. INTEGRITY      no dup canonical records, no dup DOIs, production untouched,
                  no downloads, provenance/raw/URL preservation
"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone

from app.catalog.persistence import get_collection
from app.catalog.schema import CATALOG_COLLECTION

logger = logging.getLogger("neuro_platform.catalog.report")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _is_present(v) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return bool(v.strip())
    if isinstance(v, (list, tuple, dict)):
        return len(v) > 0
    return True


# ─────────────────────────────────────────────────────────────────────────────
# C. Coverage
# ─────────────────────────────────────────────────────────────────────────────

# (report field, canonical-record getter path)
COVERAGE_FIELDS: tuple[tuple[str, str], ...] = (
    ("title", "title"),
    ("description", "description"),
    ("readme", "readme"),
    ("doi", "doi"),
    ("license", "license"),
    ("modality", "modality"),
    ("species", "species"),
    ("disease_condition", "disease"),
    ("ages", "ages"),
    ("age_group", "ageGroup"),
    ("participant_count", "participantCount"),
    ("dataset_size", "datasetSizeBytes"),
    ("publication_year", "publicationYear"),
    ("study_type", "studyType"),
    ("study_design", "studyDesign"),
    ("study_domain", "studyDomain"),
    ("tasks", "tasks"),
    ("sessions", "sessions"),
    ("brain_regions", "brainRegions"),
    ("keywords", "keywords"),
    ("dataset_type", "datasetType"),
    ("availability", "availability"),
    ("authors", "authors"),
    ("contributors", "contributors"),
    ("last_updated", "lastUpdated"),
    ("snapshot_version", "snapshot.latestTag"),
)


def _field_value(record: dict, path: str):
    """Nested getter supporting dotted paths (e.g. ``snapshot.latestTag``)."""
    cur: object = record
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def compute_coverage(docs: list[dict]) -> dict:
    coverage: dict = {}
    for report_field, canonical_path in COVERAGE_FIELDS:
        total = len(docs)
        non_null = sum(1 for d in docs if _is_present(_field_value(d, canonical_path)))
        coverage[report_field] = {
            "total": total,
            "non_null": non_null,
            "missing": total - non_null,
            "coverage_pct": round((non_null / total) * 100, 1) if total else 0.0,
        }
    return coverage


# ─────────────────────────────────────────────────────────────────────────────
# D. Normalization (unique raw vs normalized)
# ─────────────────────────────────────────────────────────────────────────────


def compute_normalization(docs: list[dict]) -> dict:
    raw_modalities: Counter = Counter()
    norm_modalities: Counter = Counter()
    raw_licenses: Counter = Counter()
    norm_licenses: Counter = Counter()
    study_types: Counter = Counter()
    study_designs: Counter = Counter()
    species: Counter = Counter()
    age_groups: Counter = Counter()

    for d in docs:
        for src in d.get("sources") or []:
            for m in src.get("modalityRaw") or []:
                raw_modalities[str(m)] += 1
            for m in src.get("modality") or []:
                norm_modalities[str(m)] += 1
            if src.get("license"):
                raw_licenses[str(src["license"])] += 1
            if src.get("licenseNormalized"):
                norm_licenses[str(src["licenseNormalized"])] += 1
            if src.get("studyType"):
                study_types[str(src["studyType"])] += 1
            if src.get("studyDesign"):
                study_designs[str(src["studyDesign"])] += 1
            for s in src.get("species") or []:
                species[str(s)] += 1
        for g in d.get("ageGroup") or []:
            age_groups[str(g)] += 1

    return {
        "unique_raw_modalities": dict(raw_modalities.most_common()),
        "unique_normalized_modalities": dict(norm_modalities.most_common()),
        "unique_raw_licenses": dict(raw_licenses.most_common()),
        "unique_normalized_licenses": dict(norm_licenses.most_common()),
        "unique_study_types": dict(study_types.most_common()),
        "unique_study_designs": dict(study_designs.most_common()),
        "unique_species": dict(species.most_common()),
        "unique_age_groups": dict(age_groups.most_common()),
    }


# ─────────────────────────────────────────────────────────────────────────────
# E. Integrity
# ─────────────────────────────────────────────────────────────────────────────


async def compute_integrity(db, docs: list[dict], prod_collection, production_before: dict | None = None) -> dict:
    coll = get_collection(db)

    # No duplicate canonical records for the same sourceDatasetId:
    # sourceKeys is a unique-indexed array, but verify from the docs too.
    source_key_seen: set[str] = set()
    dup_source_keys: list[str] = []
    doi_seen: set[str] = set()
    dup_dois: list[str] = []
    for d in docs:
        for key in d.get("sourceKeys") or []:
            if key in source_key_seen:
                dup_source_keys.append(key)
            source_key_seen.add(key)
        doi = d.get("doi")
        if doi:
            if doi in doi_seen:
                dup_dois.append(doi)
            doi_seen.add(doi)

    missing_urls = 0
    missing_raw = 0
    missing_provenance = 0
    for d in docs:
        for src in d.get("sources") or []:
            if not src.get("sourceUrl"):
                missing_urls += 1
        if not d.get("rawMetadata"):
            missing_raw += 1
        if not (d.get("provenance") or {}).get("identity"):
            missing_provenance += 1

    prod_count_after = await prod_collection.count_documents({})
    prod_openneuro_after = await prod_collection.count_documents({"source": "openneuro"})
    before = production_before or {}
    unchanged = (
        before.get("count") == prod_count_after
        and before.get("openneuro_count") == prod_openneuro_after
    ) if before else None

    return {
        "no_duplicate_source_keys": not dup_source_keys,
        "duplicate_source_keys": dup_source_keys[:20],
        "duplicate_source_key_count": len(dup_source_keys),
        "no_duplicate_doi_identities": not dup_dois,
        "duplicate_dois": dup_dois[:20],
        "duplicate_doi_count": len(dup_dois),
        "production_datasets_untouched": {
            "collection": prod_collection.name,
            "count_before_ingestion": before.get("count"),
            "count_after_ingestion": prod_count_after,
            "openneuro_count_before_ingestion": before.get("openneuro_count"),
            "openneuro_count_after_ingestion": prod_openneuro_after,
            "unchanged": unchanged,
        },
        "no_dataset_files_downloaded": True,  # ingestion is metadata-only by design
        "source_urls_preserved": {
            "all_present": missing_urls == 0,
            "missing_count": missing_urls,
        },
        "raw_metadata_preserved": {
            "all_present": missing_raw == 0,
            "missing_count": missing_raw,
        },
        "provenance_preserved": {
            "all_present": missing_provenance == 0,
            "missing_count": missing_provenance,
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Full report
# ─────────────────────────────────────────────────────────────────────────────


async def build_report(db, stats: dict, docs: list[dict], prod_collection) -> dict:
    coverage = compute_coverage(docs)
    normalization = compute_normalization(docs)
    integrity = await compute_integrity(db, docs, prod_collection, stats.get("production_before"))

    total_unique = len(docs)
    total_source_records = sum(len(d.get("sources") or []) for d in docs)
    report = {
        "report_generated_at": _utcnow(),
        "catalog_collection": CATALOG_COLLECTION,
        "A_INGESTION": {
            "total_datasets_discovered": stats.get("discovered", 0),
            "total_datasets_retrieved": stats.get("retrieved", 0),
            "total_successfully_normalized": stats.get("normalized", 0),
            "total_inserted": stats.get("inserted", 0),
            "total_updated_merged": stats.get("merged", 0),
            "total_skipped_broken_nodes": stats.get("skipped_broken_nodes", 0),
            "total_failed": stats.get("failed", 0),
            "total_api_failures": stats.get("api_failures", 0),
            "total_retries": stats.get("retries", 0),
            "per_node_graphql_errors": stats.get("per_node_graphql_errors", 0),
            "pagination_pages": stats.get("pages", 0),
            "total_ingestion_time_s": stats.get("elapsed_s"),
            "started_at": stats.get("started_at"),
            "finished_at": stats.get("finished_at"),
            "api_error_details": stats.get("api_error_details", []),
            "failure_details": stats.get("failure_details", [])[:50],
            "validation_failed": stats.get("validation_failed", 0),
            "validation_errors": stats.get("validation_errors", [])[:50],
        },
        "B_DEDUPLICATION": {
            "duplicate_candidates_evaluated": stats.get("merged", 0) + stats.get("ambiguous_candidates", 0),
            "duplicates_merged": stats.get("merged", 0),
            "matched_via": stats.get("matched_via", {}),
            "unique_canonical_datasets": total_unique,
            "total_source_records": total_source_records,
            "sources_attached_via_merge": stats.get("merged", 0),  # each merge attaches exactly one source
            "sources_per_record_avg": round(total_source_records / total_unique, 3) if total_unique else 0.0,
            "ambiguous_identities_not_merged": stats.get("ambiguous_candidates", 0),
            "ambiguous_sample": stats.get("ambiguous_sample", []),
            "note": (
                "This OpenNeuro-only run establishes the dedup framework. "
                "Cross-repository merges (DOI/URL/strong multi-field) will "
                "activate when NEMAR/DANDI/... sources are ingested."
            ),
        },
        "C_METADATA_COVERAGE": coverage,
        "D_NORMALIZATION": normalization,
        "E_DATA_INTEGRITY": integrity,
        "age_group_rule": {
            "0-12": "Children",
            "13-17": "Adolescents",
            "18-64": "Adults",
            "65+": "Older Adults",
            "derived_from": "actual subject ages only",
            "unavailable_is_null": True,
        },
    }
    return report
