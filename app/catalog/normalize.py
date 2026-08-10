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
