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

import json
import re
from datetime import datetime, timezone

from html import unescape as _html_unescape

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
DANDI_REPOSITORY = "dandi"

# ─────────────────────────────────────────────────────────────────────────────
# DANDI modality derivation — deterministic rules ONLY (never LLM, never
# inferred from title/description). Mapped from assetsSummary.approach[] and
# assetsSummary.measurementTechnique[] (spec §2.5/C for DANDI).
# ─────────────────────────────────────────────────────────────────────────────

# (canonical modality label, matching raw approach/technique tokens)
DANDI_MODALITY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "electrophysiology",
        (
            "electrophysiological approach",
            "spike sorting technique",
            "multi electrode extracellular electrophysiology recording technique",
        ),
    ),
    (
        "behavior",
        (
            "behavioral approach",
            "behavioral technique",
        ),
    ),
    (
        "imaging",
        (
            "microscopy approach; cell population imaging",
            "two-photon microscopy technique",
            "one-photon microscopy technique",
        ),
    ),
    (
        "optogenetics",
        (
            "optogenetic approach",
        ),
    ),
)

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


# ─────────────────────────────────────────────────────────────────────────────
# DANDI catalog normalization
# ─────────────────────────────────────────────────────────────────────────────


def _as_str(value) -> str | None:
    """Best-effort string cast (strings pass through; datetimes/ints stringify)."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    try:
        return str(value).strip() or None
    except Exception:  # noqa: BLE001
        return None


def _parse_timestamp(value: str | None) -> datetime | None:
    """Best-effort ISO8601 → naive-UTC datetime (mirrors _parse_iso)."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


def _dandi_id(identifier) -> str | None:
    """Normalize a DANDI dandiset identifier ('DANDI:000003' or '000003') → '000003'."""
    raw = _as_str(identifier)
    if not raw:
        return None
    return raw.replace("DANDI:", "").strip() or None


def _dandi_modality(approach: list | None, measurement_technique: list | None) -> list[str]:
    """Derive canonical modalities deterministically from DANDI approach[] +
    measurementTechnique[] name tokens. Empty when nothing maps (never guessed)."""
    names: set[str] = set()
    for group in (approach, measurement_technique):
        for item in group or []:
            if isinstance(item, dict) and item.get("name"):
                names.add(str(item["name"]).strip().lower())
    out: list[str] = []
    for label, tokens in DANDI_MODALITY_RULES:
        if any(tok in names for tok in tokens):
            out.append(label)
    return out


def _dandi_species(entries: list | None) -> list[str] | None:
    """Map DANDI assetsSummary.species[] to canonical species values.

    Prefers the taxonomy identifier when present (e.g. ``NCBITaxon_10090``) so
    equivalent display names ("House mouse" vs "Mus musculus - House mouse")
    collapse to ONE canonical identity. Falls back to the display name.
    """
    out: list[str] = []
    for e in entries or []:
        if not isinstance(e, dict):
            continue
        identifier = _as_str(e.get("identifier"))
        name = _as_str(e.get("name"))
        if identifier:
            taxon = identifier.rstrip("/").split("/")[-1].strip()
            if taxon and taxon not in out:
                out.append(taxon)
        elif name and name not in out:
            out.append(name)
    return out or None


def _dandi_about_names(about: list | None, schema_key: str) -> list[str] | None:
    """Names from about[] entries with the given schemaKey (Anatomy / Disorder).

    Only exact schemaKey matches count — never inferred from title/description.
    """
    out: list[str] = []
    for item in about or []:
        if not isinstance(item, dict):
            continue
        if (item.get("schemaKey") or "") != schema_key:
            continue
        name = _as_str(item.get("name"))
        if name and name not in out:
            out.append(name)
    return out or None


def _dandi_contributors(contributor: list | None) -> tuple[list[str], list[dict]]:
    """Split DANDI contributor[] into (authors, contributors).

    ``dcite:Author`` role → authors[] (never funders); every other role stays
    in contributors[] with its roleName preserved.
    """
    authors: list[str] = []
    contributors: list[dict] = []
    for c in contributor or []:
        if not isinstance(c, dict):
            continue
        name = _as_str(c.get("name"))
        if not name:
            continue
        roles = c.get("roleName") or []
        role_names = [str(r) for r in roles] if isinstance(roles, list) else [str(roles)]
        entry = {
            "name": name,
            "roleName": role_names,
            "includeInCitation": c.get("includeInCitation"),
            "identifier": _as_str(c.get("identifier")),
        }
        if any("dcite:Author" in r for r in role_names):
            if name not in authors:
                authors.append(name)
        else:
            contributors.append(entry)
    return authors, contributors


def _dandi_access_status(version: dict) -> str | None:
    """DANDI access[].status 'dandi:OpenAccess' → 'open' (deterministic)."""
    for a in version.get("access") or []:
        if isinstance(a, dict) and (a.get("status") or "").endswith("OpenAccess"):
            return "open"
    return None


def build_dandi_source_record(version: dict, published: dict | None = None) -> dict:
    """Map a full DANDI draft VERSION response → per-source canonical record.

    ``published`` (optional) is the list endpoint's ``most_recent_published_version``
    dict and is used ONLY for provenance (version id / published DOI). The DANDI
    version DOI is deliberately NOT stored as the canonical ``doi`` — it is
    per-version, the draft DOI is a placeholder, and it must never drive
    canonical identity resolution.
    """
    ds_id = _dandi_id(version.get("identifier")) or _dandi_id(version.get("id"))
    assets_summary = version.get("assetsSummary") or {}
    approach = assets_summary.get("approach") or []
    techniques = assets_summary.get("measurementTechnique") or []
    about = version.get("about") or []
    access = version.get("access") or []
    contributor = version.get("contributor") or []

    license_values = _str_list(version.get("license"))
    license_raw = license_values[0] if license_values else None
    availability = _dandi_access_status(version)
    species_entries = assets_summary.get("species") or []
    authors, contributors = _dandi_contributors(contributor)

    size_bytes = assets_summary.get("numberOfBytes")
    try:
        size_bytes = int(size_bytes) if size_bytes is not None else None
    except (TypeError, ValueError):
        size_bytes = None
    participant_count = assets_summary.get("numberOfSubjects")

    date_created = _as_str(version.get("dateCreated"))
    date_modified = _as_str(version.get("dateModified"))
    date_published = _as_str(version.get("datePublished"))
    source_url = _as_str(version.get("url"))

    published = published or {}
    published_doi = _as_str(published.get("doi"))

    # ── Derived NeuroSearch fields (documented deterministic rules) ─────────
    # publicationYear comes ONLY from a real datePublished — never dateCreated.
    derived = {
        "participantCount": participant_count if isinstance(participant_count, int) else None,
        "ageGroup": None,
        "sizeLabel": size_label(size_bytes),
        "publicationYear": derive_publication_year(date_published),
        "availability": availability,
    }

    modality_raw = [
        str(x.get("name"))
        for x in (approach + techniques)
        if isinstance(x, dict) and x.get("name")
    ]

    source = {
        # provenance / identity
        "repository": DANDI_REPOSITORY,
        "sourceDatasetId": ds_id,
        "sourceUrl": source_url,
        "doi": None,  # DANDI version DOI is never the canonical/global identity
        # core
        "title": _as_str(version.get("name")),
        "description": _as_str(version.get("description")),
        "readme": None,
        "license": license_raw,             # raw — never destroyed
        "licenseNormalized": normalize_license(license_raw),
        "datasetType": None,                # dataStandard is NOT datasetType
        "availability": availability,
        "lastUpdated": _parse_timestamp(date_modified),  # real update stamp only
        "createdAt": _parse_timestamp(date_created),
        "publishDate": date_published,
        "authors": authors,
        "contributors": contributors,
        # scientific metadata
        "modality": _dandi_modality(approach, techniques),
        "modalityRaw": modality_raw,
        "species": _dandi_species(species_entries),
        "speciesRaw": species_entries,
        "disease": _dandi_about_names(about, "Disorder"),
        "brainRegions": _dandi_about_names(about, "Anatomy"),
        "ages": None,                       # Phase 1: no asset-level age extraction
        "ageGroup": None,
        "participantCount": derived["participantCount"],
        "subjectIds": None,
        # Phase 1 unavailable fields stay null/empty (never invented).
        "studyType": None,
        "studyDesign": None,
        "studyDomain": None,
        "studyLongitudinal": None,
        "tasks": [],
        "sessions": [],                    # wasGeneratedBy is NOT experimental sessions
        "trialCount": None,
        "keywords": _str_list(version.get("keywords")),
        "analysisMethods": None,
        # preserved DANDI structures (also kept verbatim in rawMetadata)
        "variableMeasured": assets_summary.get("variableMeasured") or [],
        "dataStandard": assets_summary.get("dataStandard") or [],
        "relatedResource": version.get("relatedResource") or [],
        "accessRaw": access,
        # snapshot / version information
        "snapshot": {
            "latestTag": _as_str(version.get("version")),
            "version": _as_str(version.get("version")),
            "versionIdentifier": _as_str(version.get("identifier")),
            "dateCreated": date_created,
            "dateModified": date_modified,
            "datePublished": date_published,
            "assetCount": version.get("assetCount"),
            "totalFiles": assets_summary.get("numberOfFiles"),
            "publishedVersion": {
                k: published.get(k)
                for k in ("version", "name", "created", "modified", "status", "size", "doi")
                if published.get(k) is not None
            },
            "publishedDoi": published_doi,
        },
        "datasetSizeBytes": size_bytes,
        # publication information
        "publication": {
            "publishDate": date_published,
            "citation": _as_str(version.get("citation")),
            "relatedResource": version.get("relatedResource") or [],
            "publishedDoi": published_doi,
        },
        # explicit per-field provenance bookkeeping
        "provenance": {
            "source": DANDI_REPOSITORY,
            "sourceApi": "dandiarchive.org",
            "retrievedAt": _utcnow(),
            "direct": [
                "title", "description", "keywords", "species", "license",
                "access", "about", "relatedResource", "variableMeasured",
            ],
            "derived": ["participantCount", "sizeLabel", "publicationYear", "availability", "modality"],
        },
        # derived summary (explicit direct-vs-derived split, spec §2)
        "derived": derived,
        # untouched full DANDI version response — preserved verbatim (spec §11)
        "rawMetadata": version,
    }
    return source


# ─────────────────────────────────────────────────────────────────────────────
# NEMAR catalog normalization (Phase 1 — investigation 2026-08-10)
# ─────────────────────────────────────────────────────────────────────────────

NEMAR_REPOSITORY = "nemar"

# Canonical human dataset page (verified live: https://nemar.org/dataset/<id>).
# The legacy dataexplorer URL redirects here. The path form is REQUIRED for
# identity: ``normalize_url_key()`` strips query params, so a query-param URL
# would collapse every NEMAR dataset to one normalized identity key.
NEMAR_DATASET_PAGE = "https://nemar.org/dataset/"

# Pattern for OpenNeuro versioned DOIs inside related_identifiers (provenance).
_OPENNEURO_DOI_RE = re.compile(r"10\.18112/openneuro\.ds\d{4,6}\.v[0-9.]+", re.IGNORECASE)


def _parse_enrichment(raw: str | None) -> dict:
    """Best-effort parse of ``enrichment_json`` (a JSON string)."""
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:  # noqa: BLE001 — malformed enrichment is never fatal
        return {}


def _nemar_timestamp(value: str | None) -> datetime | None:
    """Parse NEMAR timestamps (``'2026-01-19 03:25:23'``, no tz) → naive UTC."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace(" ", "T").replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


def _split_tasks(raw: str | list | None) -> list[str]:
    """Defensive NEMAR task parsing.

    The API exposes tasks as a comma-joined STRING (e.g.
    ``"P300,eyesClosed,eyesOpen"``). Split on commas, strip, drop empties,
    preserve order and original labels (BIDS task labels are never collapsed).
    Embedded comma-runs (e.g. "DespicableMe,DiaryOfAWimpyKid,…") are split the
    same way. Garbage values ("", "1", "unnamed") are preserved verbatim.
    """
    values: list[str] = []
    if isinstance(raw, str):
        values = [raw]
    elif isinstance(raw, list):
        values = [v for v in raw if isinstance(v, str)]
    out: list[str] = []
    for v in values:
        for part in v.split(","):
            t = part.strip()
            if t and t not in out:
                out.append(t)
    return out


def _nemar_authors(detail: dict, enrichment: dict) -> tuple[list[str], list[dict]]:
    """Extract (authors, contributors) from the NEMAR detail payload.

    Prefers the structured ``enrichment_json.authors`` (dict name →
    {orcid, affiliations} or list of {name, …}); falls back to the plain
    comma-separated ``authors`` string on the detail.
    """
    enr_authors = enrichment.get("authors")
    names: list[str] = []
    contributors: list[dict] = []
    if isinstance(enr_authors, dict):
        for name, info in enr_authors.items():
            if not isinstance(name, str) or not name.strip():
                continue
            names.append(name.strip())
            if isinstance(info, dict) and (info.get("orcid") or info.get("affiliations")):
                contributors.append(
                    {
                        "name": name.strip(),
                        "orcid": info.get("orcid"),
                        "affiliations": [
                            a.get("name") for a in (info.get("affiliations") or []) if isinstance(a, dict)
                        ],
                    }
                )
    elif isinstance(enr_authors, list):
        for a in enr_authors:
            if isinstance(a, dict) and a.get("name"):
                names.append(str(a["name"]).strip())
    if not names and isinstance(detail.get("authors"), str):
        names = [n.strip() for n in detail["authors"].split(",") if n.strip()]
    return names, contributors


def _nemar_keywords(enrichment: dict) -> list[str]:
    """NEMAR keywords are objects ``[{term: …}]`` (some are plain strings)."""
    out: list[str] = []
    for k in enrichment.get("keywords") or []:
        if isinstance(k, dict):
            t = k.get("term")
        elif isinstance(k, str):
            t = k
        else:
            continue
        if isinstance(t, str) and t.strip() and t.strip() not in out:
            out.append(t.strip())
    return out


def _nemar_openneuro_doi(enrichment: dict) -> str | None:
    """First OpenNeuro versioned DOI in related_identifiers (provenance only)."""
    for r in enrichment.get("related_identifiers") or []:
        if not isinstance(r, dict):
            continue
        identifier = str(r.get("identifier") or "")
        if _OPENNEURO_DOI_RE.search(identifier):
            return normalize_doi(identifier)
    return None


def build_nemar_source_record(detail: dict, list_record: dict | None = None) -> dict:
    """Map a NEMAR API detail payload (``dataset`` object) → per-source record.

    Identity rules (verified against the real catalog in the compatibility
    verdict):
    - OpenNeuro mirrors (``source='openneuro'`` + ``source_id='ds…'``) get
      ``doi = None`` (never the NEMAR DOI — the DOI-conflict rule would block
      the merge; never the versioned OpenNeuro DOI as identity — version drift
      would split the dataset). Resolution happens via the generic
      cross-reference mechanism: the synthesized ``openneuro.org/datasets/…``
      URL in ``publication.referencesAndLinks`` is parsed by
      ``extract_cross_references()`` into ``openneuro:ds…``.
    - NEMAR-native (``nm…``, no source_id) use the NEMAR concept DOI as their
      canonical identity (existing identity priority: DOI > URL > repo:id).
    - age_min/age_max stay on the SOURCE record only — canonical ``ages`` /
      ``ageGroup`` stay null (a min/max range is never treated as subject ages).
    """
    ds_id = str(detail.get("dataset_id") or detail.get("id") or "").strip()
    enrichment = _parse_enrichment(detail.get("enrichment_json"))

    # Provenance / identity
    source_repo = detail.get("source") or (list_record or {}).get("source")
    source_id = detail.get("source_id") or (list_record or {}).get("source_id")
    is_mirror = bool(source_repo == "openneuro" and source_id)
    url = f"{NEMAR_DATASET_PAGE}{ds_id}"

    # DOI: mirrors → None (identity comes from the OpenNeuro source_id
    # cross-reference); natives → NEMAR concept DOI (concept, NEVER version).
    if is_mirror:
        doi = None
    else:
        doi = normalize_doi(detail.get("concept_doi"))
    openneuro_doi = _nemar_openneuro_doi(enrichment) if is_mirror else None

    # Modality: BIDS-datatype tokens pass through normalize_modalities()
    # unchanged (eeg/meg/ieeg/emg/nirs/motion/beh/anat/func/dwi/fmap/perf).
    modality_raw = _split_tasks(detail.get("modalities"))
    modality = normalize_modalities(modality_raw)

    participants = detail.get("participants")
    if participants is None:
        participants = detail.get("subject_count")
    try:
        participants = int(participants) if participants is not None else None
    except (TypeError, ValueError):
        participants = None

    size_bytes = detail.get("file_size")
    try:
        size_bytes = int(size_bytes) if size_bytes is not None else None
    except (TypeError, ValueError):
        size_bytes = None

    authors, contributors = _nemar_authors(detail, enrichment)
    publish_date = _as_str(detail.get("publish_date"))

    derived = {
        "participantCount": participants,
        "ageGroup": None,                       # never derived from min/max range
        "sizeLabel": size_label(size_bytes),
        "publicationYear": derive_publication_year(publish_date),
        "availability": availability_for(
            NEMAR_REPOSITORY,
            detail.get("visibility") == "public" and detail.get("status") == "active",
        ),
    }

    license_raw = _as_str(detail.get("license")) or _as_str(enrichment.get("license"))
    dataset_type = _as_str(enrichment.get("dataset_type"))
    version = _as_str(detail.get("latest_version"))
    version_doi = _as_str(detail.get("latest_version_doi"))

    # Cross-reference for mirrors — drives the existing generic
    # ``cross_reference`` identity path (extract_cross_references parses
    # ``openneuro.org/datasets/<id>``). Never a NEMAR-only dedup rule.
    references = [f"https://openneuro.org/datasets/{source_id}"] if is_mirror else []

    source = {
        # provenance / identity
        "repository": NEMAR_REPOSITORY,
        "sourceDatasetId": ds_id,
        "sourceUrl": url,
        "doi": doi,
        # core
        "title": _as_str(detail.get("name")) or _as_str(enrichment.get("title")),
        "description": _as_str(detail.get("description")) or _as_str(enrichment.get("description")),
        "readme": _as_str(detail.get("readme")),
        "license": license_raw,                 # raw — never destroyed
        "licenseNormalized": normalize_license(license_raw),
        "datasetType": dataset_type,
        "availability": derived["availability"],
        "lastUpdated": _nemar_timestamp(detail.get("updated_at")),
        "createdAt": _nemar_timestamp(detail.get("created_at")),
        "publishDate": publish_date,
        "authors": authors,
        "contributors": contributors,
        # scientific metadata
        "modality": modality,
        "modalityRaw": modality_raw,
        "species": None,                        # NEMAR: no structured species
        "speciesRaw": None,
        "disease": None,                        # free text only (description/readme/keywords)
        "brainRegions": None,
        "ages": None,                           # age range is NOT subject ages
        "ageGroup": None,
        "ageMin": detail.get("age_min"),       # dataset-level range — source only
        "ageMax": detail.get("age_max"),
        "participantCount": participants,
        "subjectIds": None,
        "studyType": None,
        "studyDesign": None,
        "studyDomain": None,
        "studyLongitudinal": None,
        "tasks": _split_tasks(detail.get("tasks")),
        "sessions": [],                         # NEMAR exposes a count, not labels
        "trialCount": None,
        "keywords": _nemar_keywords(enrichment),
        "analysisMethods": None,
        # snapshot / version information (NEMAR versions are NEMAR-side;
        # latest version only — never one canonical dataset per version)
        "snapshot": {
            "latestTag": version,
            "versionDoi": version_doi,
            "totalFiles": detail.get("total_files"),
            "bidsVersion": _as_str(detail.get("bids_version")),
            "sessionsCount": detail.get("sessions_count"),
            "nChannels": detail.get("n_channels"),
            "electrodeSystem": _as_str(detail.get("electrode_system")),
            "hasHed": detail.get("has_hed"),
            "hedVersion": _as_str(detail.get("hed_version")),
            "githubRepo": _as_str(detail.get("github_repo")),
            "resourceTypeSpecific": _as_str(enrichment.get("resource_type_specific")),
            "openNeuroSourceId": source_id if is_mirror else None,
            "openNeuroDoi": openneuro_doi,      # provenance only — never identity
        },
        "datasetSizeBytes": size_bytes,
        # publication information
        "publication": {
            "publishDate": publish_date,
            "citations": detail.get("num_citations"),
            "numDatasetCitations": detail.get("num_dataset_citations"),
            "numDatapaperCitations": detail.get("num_datapaper_citations"),
            "funding": enrichment.get("funding_references") or [],
            "relatedIdentifiers": enrichment.get("related_identifiers") or [],
            "referencesAndLinks": references,
        },
        # explicit per-field provenance bookkeeping
        "provenance": {
            "source": NEMAR_REPOSITORY,
            "sourceApi": "api.nemar.org",
            "retrievedAt": _utcnow(),
            "direct": ["title", "description", "modality", "license", "authors", "tasks", "keywords"],
            "derived": list(derived.keys()),
        },
        # derived summary (explicit direct-vs-derived split, spec §2)
        "derived": derived,
        # untouched full NEMAR API detail payload — preserved verbatim (spec §11)
        "rawMetadata": detail,
    }
    return source


def canonical_record_from_source(source: dict) -> dict:
    """Wrap a single per-source record into a new canonical record.

    Identity priority for the canonical ID (spec §9):
    1. DOI  2. canonical source URL  3. repository:sourceDatasetId.

    Repositories in ``SHARED_SOURCE_URL_REPOSITORIES`` (e.g. ADNI) skip the
    URL layer entirely: their sourceUrl is a shared documentation URL, so the
    deterministic ``repository:sourceDatasetId`` is the identity and
    ``provenance.identity.sourceUrlNorm`` stays None.
    """
    doi = source.get("doi")
    repository = source.get("repository")
    if repository in SHARED_SOURCE_URL_REPOSITORIES:
        # ADNI: sourceUrl is shared documentation — never identity. Co-located
        # products must stay distinct even when they point at the same page.
        url_norm = None
    else:
        url_norm = normalize_url_key(source.get("sourceUrl")) or ""
    if doi:
        primary = f"doi:{doi}"
    elif url_norm:
        primary = f"url:{url_norm}"
    else:
        primary = f"{repository}:{source.get('sourceDatasetId')}"

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
        "keywords": list(source.get("keywords") or []),
        "analysisMethods": None,
        "datasetType": source.get("datasetType"),
        "availability": source.get("availability"),
        "authors": list(source.get("authors") or []),
        "contributors": list(source.get("contributors") or []),
        "lastUpdated": source.get("lastUpdated"),
        "createdAt": source.get("createdAt"),
        "snapshot": source.get("snapshot"),
        "sources": [_source_slim(source)],
        "rawMetadata": {source.get("repository") or REPOSITORY: source.get("rawMetadata")},
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


# ─────────────────────────────────────────────────────────────────────────────
# NeuroMorpho.Org catalog normalization (Phase 1 — investigation 2026-08-10)
#
# Dataset unit = Archive × Publication contribution.
# Grouping algorithm:
#   1. Collect all neuron metadata records from the Solr index.
#   2. Group by (archive, real_PMID). Real PMIDs are positive integers;
#      negative/sentinel values (e.g. ``-42``) are placeholders, NOT real.
#   3. Records with no real PMID use (archive, real_DOI) as the group key.
#      This covers the 54 DOI-only groups found in the full corpus.
#   4. Within each group, aggregate species/brain regions/cell types
#      deterministically (union of unique values).
#
# Identity:
#   sourceDatasetId = "neuromorpho:<archive>:pmid:<PMID>" or
#                     "neuromorpho:<archive>:doi:<normalized_DOI>"
#   sourceUrl = contribution-UNIQUE (archive + contribution key in the path).
#               The bare archive URL is shared by every contribution in an
#               archive and would collapse distinct publications through the
#               generic resolver's source_url signal — observed live: 27
#               contributions wrongly merged. The path form survives
#               normalize_url_key() (query/fragment are stripped), so each
#               contribution gets a distinct sourceUrlNorm. The plain archive
#               URL is preserved under rawMetadata.neuromorpho.archiveUrl.
#   DOI = group's literature DOI (NOT cross-repo dedup identity). DOIs shared
#         by 2+ distinct PMID groups WITHIN one archive are archive-level
#         artifacts (e.g. Ascoli: 3 PMIDs all carrying 10.1016/...105) and are
#         cleared from the PMID groups so the generic DOI signal cannot merge
#         distinct publications; DOI-only groups always keep their DOI (it IS
#         their contribution identity).
#
# ZERO asset/file crawling — metadata only.
# ─────────────────────────────────────────────────────────────────────────────

NEUROMORPHO_REPOSITORY = "neuromorpho"

# Regular expression to distinguish real PMIDs (positive integer strings)
# from placeholders like "-42", "-4", or other sentinel values.
_REAL_PMID_RE = re.compile(r"^\d+$")

NEUROMORPHO_ARCHIVE_URL = "https://neuromorpho.org/archive"


def _real_pmid(pmids: list | None) -> str | None:
    """Return the first real (positive integer) PMID from the list, or None."""
    for p in (pmids or []):
        if p is not None and _REAL_PMID_RE.match(str(p)):
            return str(p)
    return None


def _real_doi(dois: list | None) -> str | None:
    """Return the first recognizable DOI from the list, or None."""
    for d in (dois or []):
        normalized = normalize_doi(str(d)) if d else None
        if normalized:
            return normalized
    return None


# The live Solr API exposes publication identity under ``reference_pmid`` /
# ``reference_doi`` (lists) and the record id under ``neuron_id``; the
# captured corpus (nm_neurons.jsonl) was normalized by the scan script to
# ``pmids`` / ``dois`` / ``id``. Accept BOTH conventions (canonical first,
# live API second) so grouping behaves identically on the raw API payload
# and on the captured corpus.
_NEURON_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "pmids": ("pmids", "reference_pmid"),
    "dois": ("dois", "reference_doi"),
    "id": ("id", "neuron_id"),
}


def _neuron_field(neuron: dict, canonical: str):
    """Read a neuron field, trying canonical then live-API key names."""
    for key in _NEURON_FIELD_ALIASES[canonical]:
        if key in neuron:
            return neuron.get(key)
    return neuron.get(canonical)


def group_neuromorpho_neurons(neurons: list[dict]) -> list[dict]:
    """Group raw neuron metadata records into Archive × Publication
    contribution groups — the canonical dataset-level unit.

    Input: list of raw neuron dicts (from the Solr /neuron/select API).
    Output: list of grouped contribution dicts, each ready for
    ``build_neuromorpho_source_record()``.

    Field-name compatibility: the live Solr API uses ``reference_pmid`` /
    ``reference_doi`` / ``neuron_id``; the captured corpus uses ``pmids`` /
    ``dois`` / ``id``. Both are accepted (see ``_NEURON_FIELD_ALIASES``).

    Grouping rules:
    - Primary group key: ``(archive, real_PMID)`` where PMID is a positive
      integer string. One group per distinct (archive, PMID) pair.
    - Fallback: ``(archive, real_DOI)`` when no real PMID exists. This
      covers the 54 DOI-only groups found in the full corpus.
    - Neurons with neither real PMID nor real DOI are discarded (1 found
      in the full corpus of 298,339 — a 0.0003% edge case).
    - Within each group, aggregate: neuron_count, species (union),
      brain_regions (union), cell_types (union), genders (union),
      age_min (min of min_ages), age_max (max of max_ages),
      strains (union if present).
    """
    groups: dict[str, dict] = {}

    for n in neurons:
        archive = str(n.get("archive") or "").strip()
        if not archive:
            continue

        pmid = _real_pmid(_neuron_field(n, "pmids"))
        doi = _real_doi(_neuron_field(n, "dois"))

        if pmid:
            key = f"{archive}|real_pmid|{pmid}"
            group_pmid = pmid
            group_doi = doi or ""
        elif doi:
            key = f"{archive}|doi|{doi}"
            group_pmid = ""
            group_doi = doi
        else:
            continue  # no identity — 0.0003% edge case

        if key not in groups:
            groups[key] = {
                "archive": archive,
                "pmid": group_pmid,
                "doi": group_doi,
                "neuron_count": 0,
                "species": set(),
                "brain_regions": set(),
                "cell_types": set(),
                "genders": set(),
                "age_min": float("inf"),
                "age_max": float("-inf"),
                "strains": set(),
            }

        g = groups[key]
        g["neuron_count"] += 1

        if n.get("species"):
            g["species"].add(str(n["species"]).strip().lower())
        if n.get("brain_region"):
            for br in n["brain_region"]:
                if isinstance(br, str) and br.strip():
                    g["brain_regions"].add(br.strip())
        if n.get("cell_type"):
            for ct in n["cell_type"]:
                if isinstance(ct, str) and ct.strip():
                    g["cell_types"].add(ct.strip())
        if n.get("gender"):
            gt = str(n["gender"]).strip()
            if gt and gt.lower() != "not reported":
                g["genders"].add(gt)
        if n.get("min_age") is not None:
            try:
                g["age_min"] = min(g["age_min"], float(n["min_age"]))
            except (TypeError, ValueError):
                pass
        if n.get("max_age") is not None:
            try:
                g["age_max"] = max(g["age_max"], float(n["max_age"]))
            except (TypeError, ValueError):
                pass
        if n.get("strain"):
            st = str(n["strain"]).strip()
            if st:
                g["strains"].add(st)

    # Finalize groups: convert sets to sorted lists, handle empty ages.
    result = []
    for g in groups.values():
        g["species"] = sorted(g["species"])
        g["brain_regions"] = sorted(g["brain_regions"])
        g["cell_types"] = sorted(g["cell_types"])
        g["genders"] = sorted(g["genders"])
        g["strains"] = sorted(g["strains"])
        if g["age_min"] == float("inf"):
            g["age_min"] = None
        if g["age_max"] == float("-inf"):
            g["age_max"] = None
        result.append(g)

    # Archive-level DOI artifact guard: a DOI shared by 2+ distinct PMID
    # groups in the SAME archive is an archive-level artifact (the archive's
    # primary-paper DOI stamped on unrelated records). Clear it from those
    # PMID groups so the generic DOI signal cannot merge distinct
    # publications. DOI-only groups are untouched (their DOI IS their
    # contribution identity).
    _clear_within_archive_doi_artifacts(result)

    return result


def _clear_within_archive_doi_artifacts(groups: list[dict]) -> None:
    """Clear DOIs shared by 2+ distinct PMID groups within one archive.

    Mutates ``groups`` in place (deterministic). A DOI that appears on
    multiple PMID groups inside a single archive is an archive-level
    artifact, not a per-contribution identity — clearing it prevents the
    generic resolver's DOI signal from merging distinct publications.
    DOI-only groups (empty ``pmid``) always keep their DOI.
    """
    by_archive: dict[str, list[dict]] = {}
    for g in groups:
        by_archive.setdefault(g["archive"], []).append(g)

    for gs in by_archive.values():
        doi_pmids: dict[str, set] = {}
        for g in gs:
            if g.get("pmid") and g.get("doi"):
                doi_pmids.setdefault(g["doi"], set()).add(g["pmid"])
        for doi, pmids in doi_pmids.items():
            if len(pmids) < 2:
                continue
            for g in gs:
                if g.get("pmid") in pmids and g.get("doi") == doi:
                    g["doi_artifact"] = g["doi"]
                    g["doi"] = ""


def build_neuromorpho_source_record(group: dict) -> dict:
    """Map a grouped NeuroMorpho contribution → per-source canonical record.

    ``group`` is one element of the list returned by
    ``group_neuromorpho_neurons()``.

    Identity:
    - PMID-backed groups: sourceDatasetId = ``neuromorpho:<archive>:pmid:<PMID>``
    - DOI-only groups: ``neuromorpho:<archive>:doi:<DOI>``
    - sourceUrl = ``{NEUROMORPHO_ARCHIVE_URL}/{archive}``
    - DOI = the group's real DOI (from PMID-backed or DOI-only group)
    - title = archive name (the most stable repository-provided label)
    """
    archive = group.get("archive") or "unknown"
    pmid = group.get("pmid") or ""
    doi = group.get("doi") or ""

    if pmid:
        source_ds_id = f"neuromorpho:{archive}:pmid:{pmid}"
        source_url = f"{NEUROMORPHO_ARCHIVE_URL}/{archive}/pmid/{pmid}"
    else:
        doi_slug = doi.replace("/", "__")
        source_ds_id = f"neuromorpho:{archive}:doi:{doi_slug}"
        source_url = f"{NEUROMORPHO_ARCHIVE_URL}/{archive}/doi/{doi_slug}"

    archive_url = f"{NEUROMORPHO_ARCHIVE_URL}/{archive}"

    title = archive  # archive name is the closest thing to a dataset label

    normalized_doi = normalize_doi(doi) if doi else None

    derived = {
        "participantCount": None,   # not applicable (neurons != participants)
        "ageGroup": None,           # min/max is range, not subject ages
        "sizeLabel": None,          # no dataset size bytes available
        "publicationYear": None,    # no date from neuron API
        "availability": availability_for(NEUROMORPHO_REPOSITORY, True),
    }

    species_raw = group.get("species") or []
    species_normalized = normalize_species(species_raw)

    age_min = group.get("age_min")
    age_max = group.get("age_max")

    source = {
        # provenance / identity
        "repository": NEUROMORPHO_REPOSITORY,
        "sourceDatasetId": source_ds_id,
        "sourceUrl": source_url,
        "doi": normalized_doi,
        # core
        "title": title,
        "description": None,
        "readme": None,
        "license": None,
        "licenseNormalized": None,
        "datasetType": "morphology",
        "availability": derived["availability"],
        "lastUpdated": None,
        "createdAt": None,
        "publishDate": None,
        "authors": [],
        "contributors": [],
        # scientific metadata
        "modality": [],
        "modalityRaw": [],
        "species": species_normalized or None,
        "speciesRaw": species_raw if species_raw else None,
        "disease": None,
        "brainRegions": group.get("brain_regions") or None,
        "ages": None,
        "ageGroup": None,
        "ageMin": age_min,
        "ageMax": age_max,
        "participantCount": None,
        "subjectIds": None,
        "studyType": None,
        "studyDesign": None,
        "studyDomain": None,
        "studyLongitudinal": None,
        "tasks": [],
        "sessions": [],
        "trialCount": None,
        "keywords": [],
        "analysisMethods": None,
        "neuronCount": group.get("neuron_count"),
        # snapshot / version information
        "snapshot": {
            "archive": archive,
            "pmid": pmid or None,
            "doi": normalized_doi or None,
            "neuronCount": group.get("neuron_count"),
            "speciesList": group.get("species") or [],
            "brainRegionList": group.get("brain_regions") or [],
            "cellTypeList": group.get("cell_types") or [],
            "genders": group.get("genders") or [],
            "strains": group.get("strains") or [],
            "ageMin": age_min,
            "ageMax": age_max,
        },
        "datasetSizeBytes": None,
        # publication information
        "publication": {
            "publishDate": None,
            "pmid": pmid or None,
            "doi": normalized_doi or None,
        },
        # explicit per-field provenance bookkeeping
        "provenance": {
            "source": NEUROMORPHO_REPOSITORY,
            "sourceApi": "neuromorpho.org/api/neuron/select",
            "retrievedAt": _utcnow(),
            "direct": ["species", "brainRegions", "cellTypes", "pmid", "doi"],
            "derived": list(derived.keys()),
        },
        # derived summary (explicit direct-vs-derived split, spec §2)
        "derived": derived,
        # untouched grouped contribution data — preserved verbatim
        "rawMetadata": {
            "archive": archive,
            "archiveUrl": archive_url,
            "pmid": pmid or None,
            "doi": normalized_doi or None,
            "doiArtifact": group.get("doi_artifact"),
            "neuronCount": group.get("neuron_count"),
            "species": group.get("species"),
            "brainRegions": group.get("brain_regions"),
            "cellTypes": group.get("cell_types"),
            "genders": group.get("genders"),
            "strains": group.get("strains"),
            "ageMin": age_min,
            "ageMax": age_max,
        },
    }
    return source


# ─────────────────────────────────────────────────────────────────────────────
# Allen Brain Atlas catalog normalization (Phase 1 — implementation 2026-08-16)
#
# Dataset unit = ONE Allen Product (locked decision from the investigation).
# 64 unique Products exist in the live RMA API (verified: total_rows=64).
# Lower-level entities (170,036 SectionDataSets, 6,703 MicroarrayDataSets,
# 22 AtlasDataSets, SectionImages, Specimens, Donors) are child/content
# entities and are NEVER ingested as canonical datasets — only their
# aggregated statistics are preserved.
#
# Identity:
#   sourceDatasetId = "allen:<productId>"  (deterministic, locked decision)
#   sourceKeys      = ["allen:allen:<productId>"]
#   DOI             = None (Allen Products carry no reliable dataset-level
#                     DOI; publication DOIs are never used as dataset identity)
#   sourceUrl       = deterministic per-product resource URL. The RMA query
#                     URL (https://api.brain-map.org/api/v2/data/query.json?
#                     criteria=model::Product[id$eqN]) is the live locator but
#                     is query-based: normalize_url_key() strips the query,
#                     collapsing every product to ONE normalized URL — the
#                     NeuroMorpho shared-URL trap that merged 27 contributions.
#                     The stored sourceUrl therefore uses the documented RMA
#                     per-record resource path (model/id/query.json) so each
#                     product keeps a distinct sourceUrlNorm; the working
#                     criteria URL is preserved under rawMetadata.allen.apiUrl.
#
# Canonical field mapping (never inferred from free text):
#   title         = product name
#   description   = product description
#   species       = product species (Mouse/Human/NHP → canonical labels)
#   brainRegions  = None (no structured anatomy on Product; never inferred)
#   modality      = []    (no explicit Allen modality; nothing invented)
#   ages/ageGroup = None  (donor ages are species-specific units — never fed
#                     into derive_age_groups(); preserved as summaries)
#   sex           = donor sex distribution (snapshot.genders, explicit only)
#   doi/license   = None  (no reliable dataset-level values)
#
# Child statistics (from the include response; arrays NEVER stored):
#   dataSetCount / specimenCount / donorCount are exact. Per-type counts
#   (sectionDataSetCount / microarrayDataSetCount / atlasDataSetCount) are
#   NOT supported by the RMA (no type discriminator on included DataSet rows,
#   no product filter on the type models) and stay null; the repo-global
#   verified totals are recorded by the ingestion report.
#
# ZERO asset/file crawling — metadata only.
# ─────────────────────────────────────────────────────────────────────────────

ALLEN_REPOSITORY = "allen"

# RMA per-record resource URL (documented convention, deterministic path).
ALLEN_PRODUCT_URL = "https://api.brain-map.org/api/v2/data/Product/{product_id}/query.json"

# The working RMA criteria URL — preserved verbatim under rawMetadata.allen.apiUrl.
ALLEN_PRODUCT_CRITERIA_URL = (
    "https://api.brain-map.org/api/v2/data/query.json?criteria=model::Product[id$eq{product_id}]"
)

# Donor sex values that are NOT explicit subject metadata (never preserved).
_ALLEN_UNKNOWN_SEX = frozenset({"unknown", "not reported", "unspecified", ""})
_ALLEN_UNKNOWN_STRAIN = frozenset({"unknown", "not reported", "unspecified", "", "none"})


def _allen_strip_product(product: dict) -> dict:
    """Product-level payload WITHOUT the child arrays (never stored)."""
    return {
        k: product.get(k)
        for k in ("id", "name", "abbreviation", "description", "resource", "species", "tags")
    }


def allen_child_stats(product: dict) -> dict:
    """Aggregated child/entity statistics from the include payload.

    Pure function of the product dict returned by the RMA include request
    (``data_sets`` / ``specimens`` / ``donors`` arrays present or absent).
    Never returns the child rows themselves — only counts and bounded
    distributions.

    Returns:
        dataSetCount, specimenCount, donorCount (exact),
        sectionDataSetCount / microarrayDataSetCount / atlasDataSetCount
        (null — type discrimination unsupported), plus donor sex / strain /
        age-id distributions and condition descriptions (explicit values only).
    """
    data_sets = product.get("data_sets") or []
    specimens = product.get("specimens") or []
    donors = product.get("donors") or []

    sexes: list[str] = []
    strains: list[str] = []
    age_ids: list[int] = []
    conditions: list[str] = []
    for d in donors:
        if not isinstance(d, dict):
            continue
        sex = str(d.get("sex_full_name") or d.get("sex") or "").strip()
        if sex and sex.lower() not in _ALLEN_UNKNOWN_SEX and sex not in sexes:
            sexes.append(sex)
        strain = str(d.get("strain") or "").strip()
        if strain and strain.lower() not in _ALLEN_UNKNOWN_STRAIN and strain not in strains:
            strains.append(strain)
        age_id = d.get("age_id")
        if age_id is not None:
            try:
                age_id = int(age_id)
            except (TypeError, ValueError):
                age_id = None
            if age_id is not None and age_id not in age_ids:
                age_ids.append(age_id)
        condition = str(d.get("condition_description") or "").strip()
        if condition and condition.lower() not in _ALLEN_UNKNOWN_STRAIN and condition not in conditions:
            conditions.append(condition)

    return {
        "dataSetCount": len(data_sets),
        "sectionDataSetCount": None,  # type discrimination unsupported by RMA
        "microarrayDataSetCount": None,
        "atlasDataSetCount": None,
        "specimenCount": len(specimens),
        "donorCount": len(donors),
        "donorSexes": sorted(sexes),
        "donorStrains": sorted(strains),
        "donorAgeIds": sorted(age_ids),
        "donorConditions": sorted(conditions),
    }


def _allen_age_summaries(age_ids: list[int], age_map: dict | None) -> list[dict]:
    """Resolve donor age ids against the Age model map (pure).

    ``age_map`` maps age_id → {id, name, days, age_group_id, embryonic,
    organism_id} (fetched once by the ingestion). Returns bounded summaries;
    unknown ids are preserved as ``{ageId: N}`` without fabrication.
    """
    out: list[dict] = []
    for age_id in age_ids:
        row = (age_map or {}).get(age_id) if isinstance(age_map, dict) else None
        if row:
            out.append(
                {
                    "ageId": age_id,
                    "name": row.get("name"),
                    "days": row.get("days"),
                    "ageGroupId": row.get("age_group_id"),
                    "embryonic": row.get("embryonic"),
                    "organismId": row.get("organism_id"),
                }
            )
        else:
            out.append({"ageId": age_id})
    return out


def build_allen_source_record(product: dict, age_map: dict | None = None) -> dict:
    """Map an Allen Product (RMA include payload) → per-source canonical record.

    ``product`` is the full product dict from ``model::Product`` with optional
    ``data_sets`` / ``specimens`` / ``donors`` arrays (the include response).
    ``age_map`` (optional) maps Allen Age ids → Age model rows for donor-age
    summaries. Pure and deterministic; never fabricates values.

    Child arrays are aggregated into statistics (``allen_child_stats``) and
    the arrays themselves are NEVER stored — rawMetadata.allen contains only
    the stripped product-level payload plus aggregated summaries.
    """
    product_id = str(product.get("id") or "").strip()
    stats = allen_child_stats(product)
    species_raw = product.get("species")
    species = normalize_species(species_raw)

    derived = {
        "participantCount": None,  # Products are not participant-based
        "ageGroup": None,          # donor ages are species-specific — never grouped
        "sizeLabel": None,
        "publicationYear": None,
        "availability": availability_for(ALLEN_REPOSITORY, True),
    }

    source = {
        # provenance / identity
        "repository": ALLEN_REPOSITORY,
        "sourceDatasetId": f"allen:{product_id}",
        "sourceUrl": ALLEN_PRODUCT_URL.format(product_id=product_id),
        "doi": None,  # no reliable dataset-level DOI — never fabricated
        # core
        "title": _as_str(product.get("name")),
        "description": _as_str(product.get("description")),
        "readme": None,
        "license": None,             # no reliable dataset-level license
        "licenseNormalized": None,
        "datasetType": "product",    # Allen dataset-level unit = Product
        "availability": derived["availability"],
        "lastUpdated": None,
        "createdAt": None,
        "publishDate": None,
        "authors": [],               # no structured author list on Product
        "contributors": [],
        # scientific metadata
        "modality": [],              # no explicit Allen modality — never invented
        "modalityRaw": [],
        "species": species or None,
        "speciesRaw": [species_raw] if species_raw else None,
        "disease": None,             # condition_description is donor-level, not dataset-level
        "brainRegions": None,        # no structured anatomy on Product
        "ages": None,                # never fed into derive_age_groups
        "ageGroup": None,
        "ageMin": None,
        "ageMax": None,
        "participantCount": None,
        "subjectIds": None,
        "studyType": None,
        "studyDesign": None,
        "studyDomain": None,
        "studyLongitudinal": None,
        "tasks": [],
        "sessions": [],
        "trialCount": None,
        "keywords": [],              # Product.tags is null for all 64 products
        "analysisMethods": None,
        # snapshot / product information
        "snapshot": {
            "productId": product_id,
            "abbreviation": product.get("abbreviation"),
            "resource": product.get("resource"),
            "tags": product.get("tags"),
            "dataSetCount": stats["dataSetCount"],
            "sectionDataSetCount": stats["sectionDataSetCount"],
            "microarrayDataSetCount": stats["microarrayDataSetCount"],
            "atlasDataSetCount": stats["atlasDataSetCount"],
            "specimenCount": stats["specimenCount"],
            "donorCount": stats["donorCount"],
            "genders": stats["donorSexes"],
            "strains": stats["donorStrains"],
            "donorConditions": stats["donorConditions"],
            "donorAgeSummaries": _allen_age_summaries(stats["donorAgeIds"], age_map),
        },
        "datasetSizeBytes": None,
        # publication information — none reliably available on the Product model
        "publication": {
            "publishDate": None,
            "notes": "Allen Product model exposes no publication structure",
        },
        # explicit per-field provenance bookkeeping
        "provenance": {
            "source": ALLEN_REPOSITORY,
            "sourceApi": "api.brain-map.org/api/v2/data",
            "retrievedAt": _utcnow(),
            "direct": ["title", "description", "species", "resource"],
            "derived": list(derived.keys()),
        },
        # derived summary (explicit direct-vs-derived split, spec §2)
        "derived": derived,
        # stripped product-level payload + aggregated child summaries — the
        # child ARRAYS (data_sets/specimens/donors) are NEVER stored.
        "rawMetadata": {
            "product": _allen_strip_product(product),
            "apiUrl": ALLEN_PRODUCT_CRITERIA_URL.format(product_id=product_id),
            "childStats": stats,
        },
    }
    return source


# ─────────────────────────────────────────────────────────────────────────────
# HCP / Connectome Coordination Facility ingestion (investigation 2026-08-16)
#
# Source: the CCF Drupal site (www.humanconnectome.org). No public JSON API is
# exposed (verified: no /jsonapi, no REST routes; BALSA/ConnectomeDB is
# login-gated and its /studies.json returns an HTML SPA shell). Metadata is
# read from the public study pages — metadata only, zero asset/file crawling.
#
# Discovery (the two authoritative index pages, verified live):
#   https://www.humanconnectome.org/lifespan-studies
#   https://www.humanconnectome.org/disease-studies
#   → exactly 20 unique study slugs (the sitemap is NOT authoritative — it
#     was observed to omit two Studies, e.g. HCP-DES and Dual Mechanisms).
#
# Dataset unit = ONE HCP Study. 1 Study = 1 NeuroSearch dataset.
# Releases are VERSIONS of a study (HCP-YA: 14 releases; HCP-Aging: 4) and are
# NEVER separate datasets — they are aggregated under
# rawMetadata.hcp.dataReleases[].
#
# Identity:
#   sourceDatasetId = "hcp:<slug>"          (deterministic, locked decision)
#   sourceKeys      = ["hcp:hcp:<slug>"]
#   DOI             = None — HCP study pages expose no dataset-level DOI;
#                     publication DOIs (hundreds per study) are preserved under
#                     rawMetadata.hcp.publications and NEVER used as identity.
#   sourceUrl       = https://www.humanconnectome.org/study/<slug> — the Study
#                     landing page itself. Path-distinct per study so
#                     normalize_url_key() keeps 20 distinct sourceUrlNorm
#                     values (never the shared repo homepage, never a release/
#                     document/publication URL, never BALSA).
#
# Canonical field mapping (never inferred from free text):
#   title         = study name (h1)
#   description   = Study Overview body (fallback: meta description)
#   authors       = explicit Principal Investigator names (structured blocks)
#   species       = None (no structured species field on the study pages)
#   brainRegions  = None  (none structured; never inferred from text)
#   modality      = []    (no structured modality field; MRI/MEG only appear
#                     in free text — never converted to canonical labels)
#   ages/ageGroup = None  (no structured ages)
#   participantCount = None (subject counts appear only in release prose)
#   doi/license   = None  (no dataset-level DOI; Data Use Terms is a
#                     restricted-access agreement, not an open license)
#
# ZERO asset/file crawling — the fetcher requests ONLY the study page, the
# data-releases page, and the publications page (metadata HTML only).
# ─────────────────────────────────────────────────────────────────────────────

HCP_REPOSITORY = "hcp"

# The Study landing page — the canonical per-study sourceUrl.
HCP_STUDY_URL = "https://www.humanconnectome.org/study/{slug}"

# Authoritative discovery pages (verified live: 20 unique slugs each).
HCP_INDEX_PAGES = (
    "https://www.humanconnectome.org/lifespan-studies",
    "https://www.humanconnectome.org/disease-studies",
)

# Sub-page URL patterns used for metadata enrichment (never ingested as units).
HCP_DATA_RELEASES_URL = "https://www.humanconnectome.org/study/{slug}/data-releases"
HCP_PUBLICATIONS_URL = "https://www.humanconnectome.org/study/{slug}/publications"
HCP_DATA_USE_TERMS_URL = "https://www.humanconnectome.org/study/{slug}/data-use-terms"
HCP_PROTOCOLS_URL = "https://www.humanconnectome.org/study/{slug}/project-protocols"

# Study slugs are lowercase [a-z0-9-]+ — a bare /study/<slug> link ends right
# after the slug (sub-pages continue with "/" and are excluded).
_HCP_SLUG_RE = re.compile(r'href="/study/([a-z0-9][a-z0-9-]*)"')

# Study CARDS live inside <div class="study-content"><h3><a href="/study/<slug>">
# (verified live). The nav dropdown / sidebar also link studies — those are
# navigation chrome, NOT catalog entries. The discovery gate therefore counts
# duplicates WITHIN the card region only, so nav repetition is never mistaken
# for a corrupted catalog.
_HCP_CARD_RE = re.compile(
    r'<div class="study-content">\s*<h3[^>]*>\s*<a href="/study/([a-z0-9][a-z0-9-]*)"'
)


class _HcpParseError(ValueError):
    """Raised when a study page lacks the required structural elements."""


def _hcp_clean_text(fragment: str | None) -> str | None:
    """Strip tags + unescape HTML entities + collapse whitespace (pure)."""
    if not fragment:
        return None
    text = re.sub(r"<[^>]+>", " ", fragment)
    text = _html_unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def hcp_index_slugs(html: str) -> list[str]:
    """Unique study slugs linked from an index page (deterministic, sorted).

    Only bare ``/study/<slug>`` links match — sub-page links (e.g.
    ``/study/hcp-young-adult/data-releases``) and the sitemap are excluded.
    Nav/sidebar chrome is included in the raw link scan (each page lists the
    full 20 via the "Studies" dropdown); duplicates collapse via the set.
    """
    return sorted(set(hcp_index_slugs_raw(html)))


def hcp_index_slugs_raw(html: str) -> list[str]:
    """All ``/study/<slug>`` links in document order (duplicates preserved).

    Used by the discovery gate to size the page and detect corruption.
    """
    return [m.group(1) for m in _HCP_SLUG_RE.finditer(html or "")]


def hcp_index_card_slugs(html: str) -> list[str]:
    """Slugs listed as study CARDS (the authoritative listing region).

    Only ``study-content`` h3-anchored cards count as catalog entries; the
    nav dropdown and sidebar are navigation chrome and are excluded. Used by
    the discovery gate: a slug appearing in MORE than one card on a single
    page is a corrupted catalog (duplicate study entry).
    """
    return [m.group(1) for m in _HCP_CARD_RE.finditer(html or "")]


def hcp_parse_study_page(html: str, slug: str) -> dict:
    """Parse a Study landing page → structured study metadata (pure).

    Returns the explicit fields the page provides: name (h1), description
    (Study Overview body, falling back to the meta description),
    principalInvestigators (structured blocks), protocol-page presence,
    data-use-terms URL, and any parseable last-modified marker.

    Raises ``_HcpParseError`` when the page is not a recognizable study page.
    """
    html = html or ""
    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
    name = _hcp_clean_text(h1.group(1)) if h1 else None
    if not name:
        raise _HcpParseError(f"no <h1> title on /study/{slug}")

    # Description: prefer the Study Overview section body; fall back to the
    # meta description (both are explicit page content, never inferred).
    overview = re.search(
        r"<h2[^>]*>Study Overview</h2>(.*?)(?:<h2[^>]*>|<footer)", html, re.S
    )
    description = _hcp_clean_text(overview.group(1)) if overview else None
    if not description:
        meta = re.search(r'<meta name="description" content="([^"]*)"', html)
        description = _hcp_clean_text(meta.group(1)) if meta else None

    # Principal Investigators — structured <div class="investigator principal">
    # blocks with an <img alt="Name"> and <h3>… - Institution<span>role</span>.
    # Each principal block is matched as a whole (the inner
    # ``investigator__image`` div must not terminate the match).
    investigators: list[dict] = []
    for blk in re.finditer(
        r'<div class="investigator principal">.*?<img[^>]*alt="([^"]+)"[^>]*>.*?<h3[^>]*>(.*?)</h3>',
        html, re.S,
    ):
        pi_name = blk.group(1).strip()
        h3_raw = blk.group(2)
        role = None
        role_m = re.search(r"<span[^>]*>(.*?)</span>", h3_raw, re.S)
        if role_m:
            role = _hcp_clean_text(role_m.group(1))
        pre = re.sub(r"<[^>]+>", " ", h3_raw)
        pre = _html_unescape(pre)
        pre = re.sub(r"\s+", " ", pre).strip()
        # "Kamil Ugurbil, Ph.D. - UMinn Principal Investigator" → institution.
        # Drop the role words if they leaked into the text (span-less pages),
        # then take the segment after the last " - " as the institution.
        inst = None
        if role and pre.endswith(role):
            pre = pre[: -len(role)].strip()
        pre = re.sub(r"\s+-", "-", pre)
        dash = re.search(r"-\s*([A-Za-z0-9&.]+)\s*$", pre)
        if dash:
            inst = dash.group(1).strip()
        investigators.append(
            {"name": pi_name, "institution": inst, "role": role or "Principal Investigator"}
        )

    protocols: list[str] = []
    if f"/study/{slug}/project-protocols" in html:
        protocols.append(HCP_PROTOCOLS_URL.format(slug=slug))

    lastmod = None
    lm = re.search(r'<meta[^>]*name="(?:article:modified_time|lastmod)"[^>]*content="([^"]+)"', html)
    if lm:
        lastmod = lm.group(1).strip()

    return {
        "slug": slug,
        "name": name,
        "url": HCP_STUDY_URL.format(slug=slug),
        "description": description,
        "principalInvestigators": investigators,
        "protocols": protocols,
        "dataUseTermsUrl": HCP_DATA_USE_TERMS_URL.format(slug=slug),
        "lastmod": lastmod,
    }


def hcp_parse_releases_page(html: str) -> list[dict]:
    """Parse a Study data-releases page → aggregated release metadata (pure).

    Releases are VERSIONS of the parent Study — this returns an aggregated
    list [{name, date, url}] that is stored under rawMetadata.hcp.dataReleases;
    releases are NEVER ingested as separate canonical datasets. The
    "Let me explore the dataset" CTA block (no release date) is excluded.
    """
    out: list[dict] = []
    for blk in re.finditer(
        r'<div class="investigator">\s*<div class="investigator__image"><img[^>]*alt="([^"]+)"[^>]*></div>\s*<h3>(.*?)</h3>',
        html or "", re.S,
    ):
        name = blk.group(1).strip()
        h3 = blk.group(2)
        if not name or name.lower().startswith("let me explore"):
            continue
        url_m = re.search(r'href="([^"]+)"', h3)
        date_m = re.search(r"Released on\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", h3)
        if not date_m:
            continue  # not a real release entry
        out.append(
            {
                "name": name,
                "date": date_m.group(1),
                "url": url_m.group(1) if url_m else None,
            }
        )
    return out


def hcp_parse_publications_page(html: str) -> list[str]:
    """Extract publication DOIs from a Study publications page (pure).

    These are PUBLICATION identifiers — preserved under
    rawMetadata.hcp.publications and never used as the canonical dataset DOI.
    """
    dois: list[str] = []
    for m in re.finditer(r"10\.\d{4,9}/[A-Za-z0-9._\-/]+", html or ""):
        doi = m.group(0).rstrip(".;,")
        if doi not in dois:
            dois.append(doi)
    return dois


def build_hcp_source_record(study: dict) -> dict:
    """Map a parsed HCP Study → per-source canonical record.

    ``study`` is the structured dict produced by ``hcp_parse_study_page``
    plus the aggregated ``dataReleases`` / ``publications`` lists from the
    corresponding sub-pages. Pure and deterministic; never fabricates values.

    Releases are aggregated into ``rawMetadata.hcp.dataReleases`` (and a
    bounded ``snapshot``), NEVER as separate canonical records.
    """
    slug = str(study.get("slug") or "").strip()
    name = _as_str(study.get("name"))
    investigators = study.get("principalInvestigators") or []
    releases = study.get("dataReleases") or []
    publications = study.get("publications") or []

    authors = [
        _as_str(pi.get("name")) for pi in investigators if _as_str(pi.get("name"))
    ]

    derived = {
        "participantCount": None,  # subject counts appear only in release prose
        "ageGroup": None,          # no structured ages — never fabricated
        "sizeLabel": None,
        "publicationYear": None,
        "availability": availability_for(HCP_REPOSITORY, True),
    }

    source = {
        # provenance / identity
        "repository": HCP_REPOSITORY,
        "sourceDatasetId": f"hcp:{slug}",
        "sourceUrl": HCP_STUDY_URL.format(slug=slug),
        "doi": None,  # no dataset-level DOI — publication DOIs are never identity
        # core
        "title": name,
        "description": _as_str(study.get("description")),
        "readme": None,
        "license": None,             # Data Use Terms is a restricted-access agreement
        "licenseNormalized": None,
        "datasetType": "study",      # HCP dataset-level unit = Study
        "availability": derived["availability"],
        "lastUpdated": None,
        "createdAt": None,
        "publishDate": None,
        "authors": authors,          # explicit PI names (structured blocks)
        "contributors": [],
        # scientific metadata
        "modality": [],              # no structured modality — never invented
        "modalityRaw": [],
        "species": None,             # no structured species field on the pages
        "speciesRaw": None,
        "disease": None,             # disease only appears in study title/prose
        "brainRegions": None,        # no structured anatomy on the pages
        "ages": None,
        "ageGroup": None,
        "ageMin": None,
        "ageMax": None,
        "participantCount": None,
        "subjectIds": None,
        "studyType": None,
        "studyDesign": None,
        "studyDomain": None,
        "studyLongitudinal": None,
        "tasks": [],
        "sessions": [],
        "trialCount": None,
        "keywords": [],
        "analysisMethods": None,
        # snapshot / study information (releases are metadata, never datasets)
        "snapshot": {
            "studySlug": slug,
            "name": name,
            "url": HCP_STUDY_URL.format(slug=slug),
            "principalInvestigators": investigators,
            "dataReleaseCount": len(releases),
            "publicationCount": len(publications),
            "lastmod": study.get("lastmod"),
        },
        "datasetSizeBytes": None,
        # publication information — publication DOIs live under rawMetadata.hcp
        "publication": {
            "publishDate": None,
            "notes": "HCP study pages expose publication DOIs (not a dataset DOI); "
            "they are preserved under rawMetadata.hcp.publications",
        },
        # explicit per-field provenance bookkeeping
        "provenance": {
            "source": HCP_REPOSITORY,
            "sourceApi": "www.humanconnectome.org (Drupal study pages)",
            "retrievedAt": _utcnow(),
            "direct": ["title", "description", "authors", "principalInvestigators"],
            "derived": list(derived.keys()),
        },
        # derived summary (explicit direct-vs-derived split, spec §2)
        "derived": derived,
        # structured study metadata + aggregated releases/publications. The
        # raw HTML is never stored — only the parsed, bounded metadata. The
        # canonical wrapper keys this under ``rawMetadata.hcp`` (per-repo).
        "rawMetadata": {
            "slug": slug,
            "name": name,
            "url": HCP_STUDY_URL.format(slug=slug),
            "description": _as_str(study.get("description")),
            "principalInvestigators": investigators,
            "dataReleases": releases,
            "publications": publications,
            "dataUseTerms": study.get("dataUseTermsUrl"),
            "protocols": study.get("protocols") or [],
            "lastmod": study.get("lastmod"),
        },
    }
    return source


# ─────────────────────────────────────────────────────────────────────────────
# Dryad catalog normalization (implementation + dry-run 2026-08-17)
#
# Source of truth: the validated census artifact
# ``trace_artifacts/dryad_census_20260817/dryad_candidates.jsonl`` — each line
# is ONE Dryad dataset (the list API returns one record per dataset, latest
# version only; versions are NEVER separate canonical records).
#
# Identity (locked):
#   sourceDatasetId = "dryad:<numeric-id>"   (Dryad package id)
#   sourceUrl       = https://datadryad.org/dataset/doi:10.5061/dryad.<slug>
#                     (the dataset landing page — verified live, HTTP 200)
#   DOI             = the Dryad dataset DOI (10.5061/dryad.*) — the CANONICAL
#                     dataset identity (DOI priority 1 in the generic
#                     resolver). Reused verbatim from the census artifact's
#                     ``identifier``, never rediscovered.
#   The related ARTICLE DOI (``relatedWorks`` relationship=primary_article)
#   stays relationship metadata under ``publication.relatedIdentifiers`` — it
#   is NEVER the canonical DOI and never drives identity.
#
# Version handling: one list record = one dataset-level entity.
# versionNumber/versionStatus are preserved under rawMetadata.dryad and
# snapshot — never separate canonical records.
#
# ZERO asset/file calls: only census metadata is used (no downloads).
# ─────────────────────────────────────────────────────────────────────────────

DRYAD_REPOSITORY = "dryad"

# Dryad dataset landing page — the actual dataset URL (never the Dryad
# homepage, a journal-article URL, or an individual file URL).
DRYAD_DATASET_URL = "https://datadryad.org/dataset/"

# Census classification label for the validated HIGH-confidence ingestion set.
DRYAD_HIGH_CONFIDENCE = "H"


def _dryad_doi(identifier) -> str | None:
    """Census artifact identifier is ``doi:10.5061/dryad.gc72v`` → DOI."""
    if not identifier:
        return None
    raw = str(identifier).strip()
    if raw.lower().startswith("doi:"):
        raw = raw[4:].strip()
    return normalize_doi(raw)


def _dryad_related_identifiers(record: dict) -> list[dict]:
    """Related works preserved verbatim as relationship metadata.

    The primary-article DOI lives here (relationship metadata only) — it must
    never become the canonical dataset DOI.
    """
    out: list[dict] = []
    for rw in record.get("relatedWorks") or []:
        if isinstance(rw, dict) and rw.get("identifier"):
            out.append(dict(rw))
    return out


def _dryad_article_doi(record: dict) -> str | None:
    """The primary-article DOI — relationship metadata, never identity."""
    for rw in record.get("relatedWorks") or []:
        if isinstance(rw, dict) and rw.get("relationship") == "primary_article":
            return normalize_doi(str(rw.get("identifier") or ""))
    return None


def _dryad_funders(record: dict) -> list[dict]:
    """Census funders are ``[{organization, awardNumber}]`` — preserved."""
    out: list[dict] = []
    for f in record.get("funders") or []:
        if isinstance(f, dict) and (f.get("organization") or f.get("awardNumber")):
            out.append(
                {
                    "organization": f.get("organization"),
                    "awardNumber": f.get("awardNumber"),
                }
            )
    return out


def _dryad_authors(record: dict) -> tuple[list[str], list[dict]]:
    """Split census author blocks (firstName/lastName/orcid/affiliation)
    into (authors, contributors)."""
    authors: list[str] = []
    contributors: list[dict] = []
    for a in record.get("authors") or []:
        if not isinstance(a, dict):
            continue
        name = " ".join(
            p for p in (a.get("firstName"), a.get("lastName"))
            if isinstance(p, str) and p.strip()
        ).strip()
        if not name:
            continue
        if name not in authors:
            authors.append(name)
        if a.get("orcid") or a.get("affiliation"):
            contributors.append(
                {
                    "name": name,
                    "orcid": a.get("orcid"),
                    "affiliation": a.get("affiliation"),
                }
            )
    return authors, contributors


def build_dryad_source_record(record: dict) -> dict:
    """Map one census-artifact Dryad dataset → per-source canonical record.

    ``record`` is one line of dryad_candidates.jsonl (metadata only — never
    a file payload). Pure and deterministic; no fabrication.
    """
    ds_id = str(record.get("id") or "").strip()
    identifier = record.get("identifier") or ""
    doi = _dryad_doi(identifier)
    source_url = f"{DRYAD_DATASET_URL}{identifier}" if identifier else None

    authors, contributors = _dryad_authors(record)

    storage_size = record.get("storageSize")
    try:
        storage_size = int(storage_size) if storage_size is not None else None
    except (TypeError, ValueError):
        storage_size = None

    publish_date = _as_str(record.get("publicationDate"))
    last_mod = _as_str(record.get("lastModificationDate"))
    license_raw = _as_str(record.get("license"))

    # ── Derived NeuroSearch fields (documented deterministic rules) ─────────
    derived = {
        "participantCount": None,  # no structured participant count on Dryad
        "ageGroup": None,          # no structured ages on Dryad
        "sizeLabel": size_label(storage_size),
        "publicationYear": derive_publication_year(publish_date),
        "availability": availability_for(
            DRYAD_REPOSITORY, record.get("visibility") == "public"
        ),
    }

    source = {
        # provenance / identity
        "repository": DRYAD_REPOSITORY,
        "sourceDatasetId": f"dryad:{ds_id}" if ds_id else None,
        "sourceUrl": source_url,
        "doi": doi,  # canonical Dryad dataset DOI (identity)
        # core
        "title": _as_str(record.get("title")),
        "description": _as_str(record.get("abstract")),
        "readme": None,
        "license": license_raw,             # raw — never destroyed
        "licenseNormalized": normalize_license(license_raw),
        "datasetType": None,                # no reliable Dryad dataset-type field
        "availability": derived["availability"],
        "lastUpdated": _parse_timestamp(last_mod),
        "createdAt": None,
        "publishDate": publish_date,
        "authors": authors,
        "contributors": contributors,
        # scientific metadata — Dryad exposes no structured modality/species/
        # disease/anatomy/ages; never invented from census keyword signals
        # (those signals stay in rawMetadata.dryad for audit).
        "modality": [],
        "modalityRaw": [],
        "species": None,
        "speciesRaw": None,
        "disease": None,
        "brainRegions": None,
        "ages": None,
        "ageGroup": None,
        "participantCount": None,
        "subjectIds": None,
        "studyType": None,
        "studyDesign": None,
        "studyDomain": None,
        "studyLongitudinal": None,
        "tasks": [],
        "sessions": [],
        "trialCount": None,
        "keywords": _str_list(record.get("keywords")),
        "analysisMethods": None,
        # snapshot / version information (one list record = one dataset)
        "snapshot": {
            "dryadId": ds_id,
            "versionNumber": record.get("versionNumber"),
            "versionStatus": record.get("versionStatus"),
            "curationStatus": record.get("curationStatus"),
            "visibility": record.get("visibility"),
            "fieldOfScience": record.get("fieldOfScience"),
            "storageSize": storage_size,
            "metrics": record.get("metrics"),
            "relatedPublicationISSN": record.get("relatedPublicationISSN"),
            "censusClassification": record.get("classification"),
        },
        "datasetSizeBytes": storage_size,
        # publication information — the ARTICLE DOI is relationship metadata,
        # quarantined from identity (never the canonical DOI).
        "publication": {
            "publishDate": publish_date,
            "articleDoi": _dryad_article_doi(record),
            "relatedIdentifiers": _dryad_related_identifiers(record),
            "funding": _dryad_funders(record),
            "relatedPublicationISSN": record.get("relatedPublicationISSN"),
        },
        # explicit per-field provenance bookkeeping
        "provenance": {
            "source": DRYAD_REPOSITORY,
            "sourceApi": "datadryad.org/api/v2 (census artifact 2026-08-17)",
            "retrievedAt": _utcnow(),
            "direct": [
                "title", "description", "authors", "keywords", "license",
                "publishDate", "storageSize",
            ],
            "derived": list(derived.keys()),
        },
        # derived summary (explicit direct-vs-derived split, spec §2)
        "derived": derived,
        # untouched census-artifact record — preserved verbatim (spec §11)
        "rawMetadata": record,
    }
    return source


# ─────────────────────────────────────────────────────────────────────────────
# ADNI ingestion normalization (implementation + dry-run 2026-08-17)
#
# Dataset unit = ONE named, approved ADNI dataset-level product.
# Approved input = the 122 records of the validated ADNI census artifact
# (trace_artifacts/adni_census_20260817/adni_census.json). A hard gate
# requires EXACTLY 122 records — every extra/missing record refuses the run.
#
# Identity:
#   sourceDatasetId = "adni:<stable_identifier>"  (deterministic; the artifact
#                   stable identifier IS the repository identity — one named
#                   product, one canonical record).
#   sourceKeys      = ["adni:adni:<stable_identifier>"]
#   DOI             = None. ADNI products carry NO dataset-level DOI — the
#                   canonical doi is ALWAYS null. Census publication_doi is
#                   relationship metadata only (source["publication"]["articleDoi"]),
#                   never canonical identity.
#   sourceUrl       = the census source_url VERBATIM — always the real official
#                   ADNI/LONI page, never a generated path. Shared official
#                   URLs are legitimate and expected: many approved products
#                   co-locate on a Documentation section page (bioflood.html /
#                   pet.html / filetour.html / schars.html) or on a news/
#                   announcement index (e.g. news-publications/news). Sharing
#                   a URL does NOT make two products the same dataset.
#   sourceUrlNorm   = None for ADNI. The normalized URL is NEVER an identity
#                   signal for ADNI — a co-located product would otherwise
#                   collapse into the shared-URL merge trap (the NeuroMorpho
#                   bug that merged 27 wrong contributions, see the
#                   neuroMorpho section note). This is enforced globally via
#                   SHARED_SOURCE_URL_REPOSITORIES (below): both the
#                   canonical-ID derivation (canonical_record_from_source) and
#                   the generic identity resolver (dedup.source_identity) skip
#                   the URL layer for those repositories and identify ADNI
#                   products by the deterministic repository:sourceDatasetId.
#                   The plain official ADNI page is also preserved verbatim
#                   under source["documentationUrl"] and rawMetadata.adni.
#
# Canonical field mapping (never inferred from free text):
#   modality  = strict map of the EXPLICIT census modality label into the
#               canonical vocab: MRI → mri, PET → pet. Labels outside the
#               vocab (Biofluid, Clinical, Genetics, Metabolomics, ...) stay
#               out of the canonical list — they are preserved raw under
#               modalityRaw and snapshot.modalityLabel, never coerced.
#   availability = None: ADNI is Controlled-access (LONI IDA; DUA + DPC
#               approval) — never reported as open.
#   participantCount / ages / ageGroup = None: the census records no
#               participant counts or ages; nothing is invented.
#   disease/brainRegions/species/analysisMethods = None: no structured
#               dataset-level values exist on the census.
#   phases / updates: ADNI1/GO/2/3/4 phases and update announcements are
#               grouped under ONE canonical record — preserved in the phase /
#               versionUpdateInfo fields of snapshot and rawMetadata.adni.
#
# ZERO asset/file crawling — metadata only (api_calls and asset_calls stay 0).
# ─────────────────────────────────────────────────────────────────────────────

ADNI_REPOSITORY = "adni"

# Repositories whose sourceUrl is a SHARED documentation/locator URL rather
# than a per-product locator. For these the normalized URL is NEVER an
# identity signal: two products co-located on one official page are distinct
# datasets, identified by the deterministic repository:sourceDatasetId. Both
# the canonical-ID derivation (canonical_record_from_source) and the generic
# identity resolver (dedup.source_identity) honor this set, so ADNI products
# sharing a URL never merge. Other repositories are untouched.
SHARED_SOURCE_URL_REPOSITORIES: frozenset[str] = frozenset({ADNI_REPOSITORY})

# The ADNI census artifact contains exactly 122 approved dataset-level records.
# This is a hard gate: input != 122 refuses the entire run before any write.
ADNI_CENSUS_EXPECTED = 122

# Strict map of the explicit census modality label → canonical modality vocab.
# MRI → mri, PET → pet; every other census label is out of the canonical vocab.
_ADNI_MODALITY_MAP: dict[str, str] = {"MRI": "mri", "PET": "pet"}


def _adni_modality(modality_raw) -> list[str]:
    """Census modality label → canonical modality vocab (strict map only)."""
    label = None
    if isinstance(modality_raw, list) and modality_raw:
        label = str(modality_raw[0]).strip()
    elif isinstance(modality_raw, str):
        label = modality_raw.strip()
    mapped = _ADNI_MODALITY_MAP.get((label or "").upper())
    return [mapped] if mapped else []


def build_adni_source_record(record: dict) -> dict:
    """Map one ADNI census artifact record → per-source canonical record.

    ``record`` is one dict of the approved ADNI census artifact
    (adni_census.json → datasets[], metadata only — never a file payload).
    sourceUrl is the census source_url VERBATIM (the real official ADNI/LONI
    page) — never a generated path. Products sharing a URL remain distinct
    datasets: identity comes from the deterministic sourceDatasetId /
    sourceKeys (see SHARED_SOURCE_URL_REPOSITORIES). Pure and deterministic;
    no fabrication of missing values.
    """
    stable_id = _as_str(record.get("stable_identifier"))
    ds_id = f"{ADNI_REPOSITORY}:{stable_id}" if stable_id else None
    doc_url = _as_str(record.get("source_url"))
    publication_doi = normalize_doi(record.get("publication_doi"))

    # ── Derived NeuroSearch fields (documented deterministic rules) ─────────
    derived = {
        "participantCount": None,  # census records no participant counts
        "ageGroup": None,          # no structured ages on the census
        "sizeLabel": None,         # census carries no dataset size
        "publicationYear": None,   # census carries no publication year
        "availability": None,      # ADNI = Controlled-access (DUA + DPC) — never open
    }

    source = {
        # provenance / identity
        "repository": ADNI_REPOSITORY,
        "sourceDatasetId": ds_id,
        "sourceUrl": doc_url,  # census source_url verbatim — the real official page
        "doi": None,  # ADNI products have NO dataset-level DOI — never fabricated
        # core
        "title": _as_str(record.get("dataset_name")),
        "description": _as_str(record.get("description")),
        "readme": None,
        "license": None,             # distribution governed by ADNI DUA/DPC — no dataset-level license
        "licenseNormalized": None,
        "datasetType": None,         # census "dataset" classification kept in snapshot
        "availability": derived["availability"],
        "lastUpdated": _parse_timestamp(record.get("last_update")),
        "createdAt": None,
        "publishDate": None,
        "authors": [],               # census lists no authors — never invented
        "contributors": [],
        # scientific metadata — strict/circumscribed (see section note)
        "modality": _adni_modality(record.get("modality")),
        "modalityRaw": _str_list([record.get("modality")]) if record.get("modality") else [],
        "species": None,
        "speciesRaw": None,
        "disease": None,
        "brainRegions": None,
        "ages": None,
        "ageGroup": None,
        "participantCount": None,
        "subjectIds": None,
        "studyType": None,
        "studyDesign": None,
        "studyDomain": None,
        "studyLongitudinal": None,
        "tasks": [],
        "sessions": [],
        "trialCount": None,
        "keywords": [],              # census carries no keyword list
        "analysisMethods": None,
        # snapshot / product information
        "snapshot": {
            "stableIdentifier": stable_id,
            "category": record.get("category"),
            "modalityLabel": record.get("modality"),
            "phase": record.get("phase"),
            "accessLevel": record.get("access_level"),
            "classification": record.get("classification"),
            "firstAnnounced": record.get("first_announced"),
            "lastUpdate": record.get("last_update"),
            "versionUpdateInfo": record.get("version_update_info"),
            "exclusionReason": record.get("exclusion_reason"),
            "metadataFieldsAvailable": _str_list(record.get("metadata_fields_available")),
        },
        "datasetSizeBytes": None,
        # official ADNI product/documentation page — verbatim census URL; the
        # deterministic sourceDatasetId/sourceKeys, not the URL, are identity.
        "documentationUrl": doc_url,
        # publication information — the census publication DOI is relationship
        # metadata ONLY, quarantined from identity (canonical doi stays null).
        "publication": {
            "publishDate": None,
            "articleDoi": publication_doi,
            "relatedIdentifiers": (
                [{"identifier": publication_doi}] if publication_doi else []
            ),
            "funding": [],
        },
        # explicit per-field provenance bookkeeping
        "provenance": {
            "source": ADNI_REPOSITORY,
            "sourceApi": "adni.loni.usc.edu census artifact 2026-08-17",
            "retrievedAt": _utcnow(),
            "direct": [
                "dataset_name", "description", "category", "modality", "phase",
                "source_url", "access_level", "first_announced", "last_update",
                "version_update_info", "publication_doi", "metadata_fields_available",
                "classification", "exclusion_reason", "documentation_section",
            ],
            "derived": list(derived.keys()),
        },
        # derived summary (explicit direct-vs-derived split, spec §2)
        "derived": derived,
        # untouched ADNI census-artifact record — preserved verbatim (spec §11)
        "rawMetadata": record,
    }
    return source
