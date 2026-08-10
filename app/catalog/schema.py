"""
Canonical NeuroSearch dataset catalog schema (§1–§6).

The canonical record represents ONE underlying scientific dataset with
MULTIPLE provenance/access sources (OpenNeuro + NEMAR + DANDI + ...). It is
deliberately repository-agnostic: missing fields stay null/empty and every
value keeps its provenance.

Field naming follows the canonical schema spec (camelCase for canonical
fields such as ``canonicalDatasetId`` / ``sourceDatasetId`` / ``ageGroup``),
with ``rawMetadata`` and ``provenance`` preserved verbatim per repository.

Derived NeuroSearch fields (deterministic, documented rules — never LLM):
- ``participantCount``   from source subject lists
- ``ageGroup``           from actual source ages (see ``derive_age_groups``)
- ``sizeLabel``          human-readable bucket from dataset size bytes
- ``publicationYear``    from publish date
- ``availability``       deterministic per-repository (OpenNeuro → "open")
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any

# ─────────────────────────────────────────────────────────────────────────────
# Canonical constants
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_VERSION = "catalog-0.1.0"

# Collection the canonical catalog lives in — NEVER the production `datasets`.
CATALOG_COLLECTION = "neurosearch_dataset_catalog"

# Repository source key → stable identity prefix (dedup identity).
REPOSITORY = "repository"

# §3 — NeuroSearch age-group rule (derived ONLY from actual subject ages).
AGE_GROUP_RULES: tuple[tuple[int, int, str], ...] = (
    (0, 12, "Children"),
    (13, 17, "Adolescents"),
    (18, 64, "Adults"),
    (65, 10_000, "Older Adults"),
)

# Canonical normalized licenses (§6). Raw values are always preserved.
LICENSE_CC0 = "cc0"
LICENSE_PDDL = "pddl"
LICENSE_CC_BY_4 = "cc-by-4.0"
LICENSE_CC_BY_NC_4 = "cc-by-nc-4.0"
KNOWN_NORMALIZED_LICENSES: frozenset[str] = frozenset(
    {LICENSE_CC0, LICENSE_PDDL, LICENSE_CC_BY_4, LICENSE_CC_BY_NC_4}
)

# Deterministic per-repository availability (mirrors ACCESS_TIER_STATIC).
# OpenNeuro public datasets are always open access.
AVAILABILITY_BY_REPOSITORY: dict[str, str] = {
    "openneuro": "open",
    "dandi": "open",
    "neurovault": "open",
    "zenodo": "open",
    "figshare": "open",
    "dryad": "open",
    "osf": "open",
    "nitrc": "open",
    # "ebrains": registered-or-open per record — left unset (no guessing).
}

# Pattern for deterministic dataset-size labels (mirrors quality_pipeline).
_SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]


# ─────────────────────────────────────────────────────────────────────────────
# String helpers (normalization)
# ─────────────────────────────────────────────────────────────────────────────


def _fold(text: str) -> str:
    """Lowercase + NFKC fold + collapse whitespace (identity comparison)."""
    if not text:
        return ""
    return " ".join(unicodedata.normalize("NFKC", text).lower().split())


def normalize_title(title: str | None) -> str | None:
    """Deterministic title normalization used by the multi-field identity.

    Lowercase, NFKC fold, collapse whitespace, strip punctuation — but never
    stemmed or truncated (no fuzzy matching).
    """
    if not title:
        return None
    folded = _fold(title)
    if not folded:
        return None
    cleaned = re.sub(r"[^\w\s-]", "", folded)
    return re.sub(r"\s+", " ", cleaned).strip() or None


# A real DOI resolves from a 10.xxxx registry prefix (Crossref/DataCite).
# Placeholders like "mockdoi" / "to be assigned" are NOT identity keys —
# treating them as DOIs merges unrelated datasets (observed live: 5 unrelated
# OpenNeuro datasets all carried the literal string "mockdoi").
_DOI_PATTERN = re.compile(r"^10\.\d{4,9}/")


def normalize_doi(doi: str | None) -> str | None:
    """Case-insensitive DOI normalization; strips common prefixes.

    Returns None for absent OR implausible values (e.g. the placeholder
    ``mockdoi``) so fake DOIs are never used as identity keys. The raw
    value is always preserved in the source record.
    """
    if not doi:
        return None
    d = doi.strip().lower()
    for prefix in (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "doi:",
    ):
        if d.startswith(prefix):
            d = d[len(prefix):]
            break
    d = d.strip()
    if not d or not _DOI_PATTERN.match(d):
        return None
    return d


def normalize_url_key(url: str | None) -> str | None:
    """Identity URL normalization: lowercase host, strip www./trailing
    slash/query/fragment, http/https merged (mirrors dataset_repository)."""
    if not url:
        return None
    try:
        from urllib.parse import urlparse

        p = urlparse(url)
        host = (p.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "").rstrip("/")
        return f"http://{host}{path}" or None
    except Exception:  # noqa: BLE001
        return (url or "").lower().strip().rstrip("/") or None


def make_canonical_id(primary_identity: str) -> str:
    """Deterministic canonical dataset ID: ``ns-`` + sha1 of the primary
    identity key (DOI > source URL > repository:sourceDatasetId)."""
    digest = hashlib.sha1(primary_identity.encode("utf-8")).hexdigest()[:16]
    return f"ns-{digest}"


# ─────────────────────────────────────────────────────────────────────────────
# §3 — Age group derivation (deterministic, from actual ages only)
# ─────────────────────────────────────────────────────────────────────────────


def derive_age_groups(ages: list[float] | None) -> list[str] | None:
    """Map actual subject ages to NeuroSearch age groups.

    - Uses ONLY numeric age values — never title/disease/study-name inference.
    - A dataset spanning several groups keeps every applicable group.
    - Returns None when no usable age data exists (never fabricates).
    """
    if not ages:
        return None
    groups: list[str] = []
    for age in ages:
        try:
            age_f = float(age)
        except (TypeError, ValueError):
            continue
        if age_f < 0:
            continue
        for lo, hi, label in AGE_GROUP_RULES:
            if lo <= age_f <= hi:
                if label not in groups:
                    groups.append(label)
                break
    return groups or None


# ─────────────────────────────────────────────────────────────────────────────
# §6 — License normalization (raw value always preserved in source metadata)
# ─────────────────────────────────────────────────────────────────────────────

_LICENSE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    # CC0 / CCO / equivalent variants → cc0. Matches plain text ("CC0 1.0")
    # AND SPDX ids ("spdx:CC0-1.0" — "\bcc0\b" is a word boundary before the
    # colon/hyphen).
    (LICENSE_CC0, re.compile(r"\bcc0\b|\bcco\b|creative commons 0|cc 0 universal|public domain \(cc0\)")),
    # PDDL / PPDL / Public Domain Dedication variants → pddl
    (
        LICENSE_PDDL,
        re.compile(r"\bpddl\b|\bppdl\b|public domain dedication|opendatacommons\.org/licenses/pddl|odc-pddl"),
    ),
    # CC BY-NC (4.0 and relatives) → cc-by-nc-4.0. Handles hyphenated SPDX
    # ("spdx:CC-BY-NC-4.0") and space/hyphen prose ("CC BY-NC 4.0").
    (LICENSE_CC_BY_NC_4, re.compile(r"cc[- ]by[- ]nc|creative commons (attribution[- ]non[ -]commercial|noncommercial)")),
    # CC BY → cc-by-4.0. "\bcc0\b"-style SPDX ("spdx:CC-BY-4.0") is covered by
    # the "cc[- ]by[- ]4" branch; plain "CC BY" prose keeps matching "cc by\b".
    (LICENSE_CC_BY_4, re.compile(r"cc by\b|cc[- ]by[- ]4|creative commons attribution|creative commons by\b")),
    # Plain public domain / PD → pddl (closest canonical public-domain label)
    (LICENSE_PDDL, re.compile(r"^public domain$|^pd$|\bpublic domain\b")),
)

_NON_LICENSE_MARKERS: tuple[re.Pattern[str], ...] = (
    re.compile(r"not specified|unspecified|n/?a\b|unknown|no license|none"),
)


def normalize_license(raw: str | None) -> str | None:
    """Map a raw license string to a canonical NeuroSearch license.

    Returns None for absent OR unrecognized raw values (raw is never
    destroyed — it stays in the source record's ``license`` field).
    """
    if not raw:
        return None
    folded = _fold(raw)
    if not folded:
        return None
    if any(m.search(folded) for m in _NON_LICENSE_MARKERS) and len(folded) < 40:
        return None
    for canonical, pattern in _LICENSE_PATTERNS:
        if pattern.search(folded):
            return canonical
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Derived helpers
# ─────────────────────────────────────────────────────────────────────────────


def size_label(num_bytes: int | None) -> str | None:
    """Human-readable dataset size label (e.g. '1.2 GB') — deterministic
    bucket from bytes, mirroring the quality-pipeline convention."""
    if not num_bytes or num_bytes <= 0:
        return None
    size = float(num_bytes)
    for unit in _SIZE_UNITS:
        if size < 1024 or unit == _SIZE_UNITS[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}".replace(".0 ", " ")
        size /= 1024
    return None


def derive_publication_year(publish_date: str | None, created: str | None = None) -> int | None:
    """Publication year from the publish date (fallback: created)."""
    for raw in (publish_date, created):
        if not raw:
            continue
        m = re.search(r"\b(19\d\d|20\d\d)\b", raw)
        if m:
            return int(m.group(1))
    return None


def availability_for(repository: str, public: bool | None = True) -> str | None:
    """Deterministic availability tier for a repository source."""
    if not public:
        return None
    return AVAILABILITY_BY_REPOSITORY.get(repository)


def normalize_string_list(values: Any) -> list[str]:
    """Lowercase/trim a list of strings, preserving order, dropping empties."""
    out: list[str] = []
    for v in values or []:
        if isinstance(v, str) and v.strip():
            s = _fold(v)
            if s and s not in out:
                out.append(s)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_SOURCE_FIELDS = ("repository", "sourceDatasetId", "sourceUrl")


def validate_canonical_record(record: dict) -> list[str]:
    """Return a list of validation errors (empty = valid).

    Enforces the canonical invariants:
    - canonicalDatasetId is present and deterministic-looking (ns-…)
    - at least one source with repository / sourceDatasetId / sourceUrl
    - ageGroup, if present, is consistent with the stored ages (never
      fabricated independently)
    - participantCount, if present, is a non-negative int
    - doi, if present, is normalized
    - license, if present, is a known canonical license
    """
    errors: list[str] = []

    cid = record.get("canonicalDatasetId")
    if not cid or not str(cid).startswith("ns-"):
        errors.append("canonicalDatasetId must be present and start with 'ns-'")

    sources = record.get("sources") or []
    if not sources:
        errors.append("record must have at least one source")
    else:
        for i, src in enumerate(sources):
            for field in REQUIRED_SOURCE_FIELDS:
                if not (src or {}).get(field):
                    errors.append(f"sources[{i}].{field} is required")

    ages = record.get("ages")
    age_group = record.get("ageGroup")
    if age_group and not ages:
        errors.append("ageGroup present without source ages (fabrication guard)")
    if age_group:
        derived = derive_age_groups(ages)
        if not derived or set(age_group) != set(derived):
            errors.append(f"ageGroup {age_group} inconsistent with ages {ages}")

    pc = record.get("participantCount")
    if pc is not None and (not isinstance(pc, int) or pc < 0):
        errors.append("participantCount must be a non-negative integer")

    doi = record.get("doi")
    if doi is not None:
        normalized = normalize_doi(doi)
        if normalized != str(doi).strip().lower():
            errors.append(f"doi {doi!r} is not normalized ({normalized!r})")

    license_ = record.get("license")
    if license_ is not None and license_ not in KNOWN_NORMALIZED_LICENSES:
        errors.append(f"license {license_!r} is not a known canonical license")

    return errors
