"""
Catalog normalization — raw repository payload → per-source canonical record.

This module maps a raw repository node (e.g. the OpenNeuro GraphQL ``node``
validated in the Phase 1/2 experiments) into a *per-source* canonical record.

Direct vs derived distinction (spec §2):
- Direct:   values explicitly provided by the repository (title, modality,
            ages, license, ...) — stored untouched under the source record.
- Derived:  values computed by documented deterministic rules
            (participantCount, ageGroup, sizeLabel, publicationYear,
            availability) — stored under ``derived`` AND as canonical fields.
- Unavailable: fields the source does not provide reliably (species for
            many OpenNeuro datasets, disease, brainRegions) stay None.

Never fabricate: a missing value is None/[] — never guessed, never LLM.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from app.catalog.schema import (
    availability_for,
    derive_age_groups,
    derive_publication_year,
    make_canonical_id,
    normalize_doi,
    normalize_license,
    normalize_title,
    normalize_url_key,
    size_label,
)
from app.data.vocab import MODALITY_VOCAB, SPECIES_VOCAB

REPOSITORY = "openneuro"

# Modality raw token → canonical label (reverse of MODALITY_VOCAB). Tokens
# with no approved mapping are preserved lowercase (e.g. OpenNeuro's "beh").
_MODALITY_LABEL: dict[str, str] = {}
for _label, _tokens in MODALITY_VOCAB.items():
    for _tok in _tokens:
        _MODALITY_LABEL[_tok] = _label

# Species raw token → canonical label.
_SPECIES_LABEL: dict[str, str] = {}
for _label, _tokens in SPECIES_VOCAB.items():
    for _tok in _tokens:
        _SPECIES_LABEL[_tok] = _label


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# Purely numeric tokens observed in OpenNeuro ``summary.modalities`` are BIDS
# subject/file indices ("01", "13"), never modalities — deterministic drop.
_NUMERIC_ONLY = re.compile(r"^\d+$")


def normalize_modalities(raw: list | None) -> list[str]:
    """Map raw modality tokens to canonical labels (lowercase, deduped).

    Unknown tokens are preserved as-is (lowercased) — never dropped, never
    invented. The raw list stays on the source record. Purely numeric tokens
    (BIDS file/subject indices) are deterministically excluded.
    """
    out: list[str] = []
    for token in raw or []:
        if not isinstance(token, str) or not token.strip():
            continue
        t = token.strip().lower()
        if _NUMERIC_ONLY.match(t):
            continue  # BIDS index, not a modality
        label = _MODALITY_LABEL.get(t, t)
        if label not in out:
            out.append(label)
    return out


def normalize_species(raw: str | list | None) -> list[str] | None:
    """Map raw species values to canonical labels; None when absent.

    Empty/blank values are treated as absent (never counted as populated).
    """
    values: list[str] = []
    if isinstance(raw, str):
        values = [raw]
    elif isinstance(raw, list):
        values = [v for v in raw if isinstance(v, str)]
    if not values:
        return None
    out: list[str] = []
    for v in values:
        t = v.strip().lower()
        if not t:
            continue  # blank species = absent, not populated
        label = _SPECIES_LABEL.get(t, t)
        if label not in out:
            out.append(label)
    return out or None


def _first_str(*values) -> str | None:
    for v in values:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _str_list(value) -> list[str]:
    if not isinstance(value, list):
        return []
    return [v for v in value if isinstance(v, str) and v.strip()]


def build_source_record(node: dict) -> dict:
    """Map a raw OpenNeuro GraphQL node → per-source canonical record.

    The returned dict is the ``sources[]`` entry shape plus ``derived`` and
    ``rawMetadata`` (the untouched node). Deterministic; no fabrication.
    """
    ds_id = str(node.get("id") or "").strip()
    metadata = node.get("metadata") or {}
    snapshot = node.get("latestSnapshot") or {}
    description = snapshot.get("description") or {}
    summary = snapshot.get("summary") or {}
    all_snapshots = node.get("snapshots") or []

    url = f"https://openneuro.org/datasets/{ds_id}"

    # ── Direct source metadata ────────────────────────────────────────────
    title = _first_str(description.get("Name"), metadata.get("datasetName"), node.get("name"))
    ages_raw = metadata.get("ages") if isinstance(metadata.get("ages"), list) else None
    ages: list[float] | None = None
    if ages_raw:
        ages = []
        for a in ages_raw:
            try:
                ages.append(float(a))
            except (TypeError, ValueError):
                continue
        if not ages:
            ages = None

    subjects = _str_list(summary.get("subjects"))
    size_bytes = snapshot.get("size")
    if size_bytes is None:
        size_bytes = summary.get("size")
    try:
        size_bytes = int(size_bytes) if size_bytes is not None else None
    except (TypeError, ValueError):
        size_bytes = None

    last_updated_raw = (
        metadata.get("latestSnapshotCreatedAt")
        or snapshot.get("created")
        or node.get("created")
    )
    last_updated = None
    if last_updated_raw:
        try:
            last_updated = datetime.fromisoformat(str(last_updated_raw).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            last_updated = None

    published = node.get("publishDate")

    # ── Derived NeuroSearch fields (documented deterministic rules) ───────
    derived = {
        "participantCount": len(subjects) if subjects else None,
        "ageGroup": derive_age_groups(ages),
        "sizeLabel": size_label(size_bytes),
        "publicationYear": derive_publication_year(published, node.get("created")),
        "availability": availability_for(REPOSITORY, node.get("public")),
    }

    doi = normalize_doi(_first_str(description.get("DatasetDOI")))
    license_raw = _first_str(description.get("License"))
    modality_raw = _str_list(metadata.get("modalities")) or _str_list(summary.get("modalities"))
    species_raw = metadata.get("species")

    source = {
        # provenance / identity
        "repository": REPOSITORY,
        "sourceDatasetId": ds_id,
        "sourceUrl": url,
        "doi": doi,
        # core
        "title": title,
        "description": None,  # OpenNeuro GraphQL exposes no free-text body
        "readme": snapshot.get("readme"),
        "license": license_raw,             # raw — never destroyed
        "licenseNormalized": normalize_license(license_raw),
        "datasetType": _first_str(description.get("DatasetType")),  # raw/derivative
        "availability": derived["availability"],
        "lastUpdated": last_updated,
        "createdAt": node.get("created"),
        "publishDate": published,
        "authors": _str_list(description.get("Authors")),
        "contributors": [
            {
                "name": c.get("name"),
                "contributorType": c.get("contributorType"),
                "orcid": c.get("orcid"),
            }
            for c in (snapshot.get("contributors") or [])
            if isinstance(c, dict) and c.get("name")
        ],
        # scientific metadata
        "modality": normalize_modalities(modality_raw),
        "modalityRaw": modality_raw,
        "species": normalize_species(species_raw),
        "speciesRaw": species_raw,
        "disease": None,                    # OpenNeuro: no reliable field
        "brainRegions": None,               # OpenNeuro: no reliable field
        "ages": ages,                       # original source ages preserved
        "ageGroup": derived["ageGroup"],
        "participantCount": derived["participantCount"],
        "subjectIds": subjects,
        # OpenNeuro exposes NO structured study-type field: studyType stays
        # null (honest), while the free-text studyDesign is preserved below.
        "studyType": None,
        "studyDesign": _first_str(metadata.get("studyDesign")),
        "studyDomain": _first_str(metadata.get("studyDomain")),
        "studyLongitudinal": metadata.get("studyLongitudinal"),
        "tasks": _str_list(metadata.get("tasksCompleted")) or _str_list(summary.get("tasks")),
        "sessions": summary.get("sessions") or [],
        "trialCount": metadata.get("trialCount"),
        "keywords": [],                     # OpenNeuro exposes no keyword field
        "analysisMethods": None,
        "dxStatus": metadata.get("dxStatus"),
        "dataProcessed": metadata.get("dataProcessed"),
        # snapshot / version information
        "snapshot": {
            "latestTag": snapshot.get("tag"),
            "latestSnapshotId": snapshot.get("id"),
            "latestSnapshotCreated": snapshot.get("created"),
            "hexsha": snapshot.get("hexsha"),
            "totalFiles": summary.get("totalFiles"),
            "snapshotCount": len(all_snapshots),
            "tags": [s.get("tag") for s in all_snapshots],
        },
        "datasetSizeBytes": size_bytes,
        # publication information
        "publication": {
            "publishDate": published,
            "associatedPaperDOI": metadata.get("associatedPaperDOI"),
            "funding": _str_list(description.get("Funding")),
            "ethicsApprovals": _str_list(description.get("EthicsApprovals")),
            "referencesAndLinks": _str_list(description.get("ReferencesAndLinks")),
            "bidsVersion": _first_str(description.get("BIDSVersion")),
        },
        # explicit per-field provenance bookkeeping
        "provenance": {
            "source": REPOSITORY,
            "sourceApi": "openneuro.org",
            "retrievedAt": _utcnow(),
            "direct": ["title", "modality", "species", "ages", "license", "tasks"],
            "derived": list(derived.keys()),
        },
        # derived summary (explicit direct-vs-derived split, spec §2)
        "derived": derived,
        # untouched raw node — preserved verbatim (spec §11)
        "rawMetadata": node,
    }
    return source


def canonical_record_from_source(source: dict) -> dict:
    """Wrap a single per-source record into a new canonical record.

    Identity priority for the canonical ID (spec §9):
    1. DOI  2. canonical source URL  3. repository:sourceDatasetId.
    """
    doi = source.get("doi")
    url_norm = normalize_url_key(source.get("sourceUrl")) or ""
    if doi:
        primary = f"doi:{doi}"
    elif url_norm:
        primary = f"url:{url_norm}"
    else:
        primary = f"{source.get('repository')}:{source.get('sourceDatasetId')}"

    return {
        "canonicalDatasetId": make_canonical_id(primary),
        "title": source.get("title"),
        "description": source.get("description"),
        "readme": source.get("readme"),
        "doi": doi,
        "license": source.get("licenseNormalized"),
        "modality": list(source.get("modality") or []),
        "species": list(source.get("species") or []),
        "disease": source.get("disease"),
        "brainRegions": source.get("brainRegions"),
        "ages": source.get("ages"),
        "ageGroup": list(source.get("ageGroup") or []),
        "participantCount": source.get("participantCount"),
        "datasetSizeBytes": source.get("datasetSizeBytes"),
        "sizeLabel": source.get("derived", {}).get("sizeLabel"),
        "publicationYear": source.get("derived", {}).get("publicationYear"),
        "studyType": source.get("studyType"),
        "studyDesign": source.get("studyDesign"),
        "studyDomain": source.get("studyDomain"),
        "longitudinal": source.get("studyLongitudinal"),
        "tasks": list(source.get("tasks") or []),
        "sessions": list(source.get("sessions") or []),
        "keywords": [],
        "analysisMethods": None,
        "datasetType": source.get("datasetType"),
        "availability": source.get("availability"),
        "authors": list(source.get("authors") or []),
        "contributors": list(source.get("contributors") or []),
        "lastUpdated": source.get("lastUpdated"),
        "createdAt": source.get("createdAt"),
        "snapshot": source.get("snapshot"),
        "sources": [_source_slim(source)],
        "rawMetadata": {REPOSITORY: source.get("rawMetadata")},
        "sourceKeys": [f"{source.get('repository')}:{source.get('sourceDatasetId')}"],
        "provenance": {
            "identity": {
                "primary": primary,
                "matchedVia": "new",
                "doi": doi,
                "sourceUrlNorm": url_norm or None,
                "titleNorm": normalize_title(source.get("title")),
            },
            "pipeline": "catalog-0.1.0",
            "firstSeenAt": _utcnow(),
            "updatedAt": _utcnow(),
        },
        "syncedAt": _utcnow(),
    }


def _source_slim(source: dict) -> dict:
    """Compact source entry stored in the canonical ``sources[]`` array.

    Carries all repository-specific metadata (spec §10: preserve info one
    repo has and another lacks) — the full detail is retained, raw node
    lives under ``rawMetadata.<repository>``.
    """
    slim = dict(source)
    slim.pop("rawMetadata", None)  # stored once per repo on the canonical doc
    return slim
