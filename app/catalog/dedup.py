"""
Cross-repository deduplication (spec §8–§10).

One canonical record per UNDERLYING dataset; duplicate repository sources
are merged into the existing canonical record instead of creating a new one.

Identity priority (spec §9) — resolved strictly in this order:
    1. DOI / canonical dataset DOI
    2. Strong repository-provided cross-reference (same dataset, other repo)
    3. Canonical source URL
    4. Strong multi-field identity (normalized title + authors +
       participant count + modality + publication info)

Hard rules:
- Repository-specific IDs alone are NEVER global identity.
- No fuzzy title matching. Multi-field identity requires an EXACT
  normalized-title match AND ≥2 additional strong signals.
- DOI conflict (both sides have a DOI and they differ) ⇒ NEVER merge.
- When identity is uncertain ⇒ keep separate records (counted as
  ambiguous) — a false-positive merge is worse than a possible duplicate.
- Merging NEVER discards the second source: its provenance is attached to
  the canonical record (spec §10) and raw metadata is kept per repository
  (spec §11).

All functions here are pure (no DB) so the matching rules are unit-tested.
"""

from __future__ import annotations

from app.catalog.schema import derive_age_groups, normalize_doi, normalize_title, normalize_url_key

# ─────────────────────────────────────────────────────────────────────────────
# Identity keys
# ─────────────────────────────────────────────────────────────────────────────

# Repository dataset-URL patterns that make a cross-reference trustworthy
# (spec §9 key 2). Only exact recognized dataset URLs count.
CROSS_REF_PATTERNS: tuple[tuple[str, str], ...] = (
    ("dandi", "dandiarchive.org/dandiset/"),
    ("openneuro", "openneuro.org/datasets/"),
    ("nemar", "nemar.org/"),
    ("nitrc", "nitrc.org/projects/"),
    ("brainlife", "brainlife.io/datasets/"),
    ("ebrains", "ebrains.eu/"),
)


def source_identity(source: dict) -> dict:
    """Deterministic identity keys for a per-source record (pure)."""
    return {
        "doi": normalize_doi(source.get("doi")),
        "sourceUrlNorm": normalize_url_key(source.get("sourceUrl")),
        "sourceKey": f"{source.get('repository')}:{source.get('sourceDatasetId')}",
        "titleNorm": normalize_title(source.get("title")),
        "authorsNorm": {
            normalize_title(a)
            for a in (source.get("authors") or [])
            if normalize_title(a)
        },
        "participantCount": source.get("participantCount"),
        "modalityNorm": set(source.get("modality") or []),
        "publicationYear": source.get("derived", {}).get("publicationYear"),
    }


def extract_cross_references(source: dict) -> list[dict]:
    """Repository-provided references to the SAME dataset on other platforms.

    Conservative: parses known repository dataset-URL patterns from the raw
    ``related`` objects and reference links. Returns [] when nothing is
    recognizable — a string that merely *mentions* a repo never counts.
    """
    refs: list[dict] = []
    seen: set[str] = set()
    raw = (source.get("rawMetadata") or {})
    snapshot = raw.get("latestSnapshot") or {}
    related = snapshot.get("related") or []
    descriptions: list[str] = []
    for r in related if isinstance(related, list) else []:
        if isinstance(r, dict) and r.get("description"):
            descriptions.append(str(r["description"]))
    for link in (source.get("publication") or {}).get("referencesAndLinks") or []:
        if isinstance(link, str):
            descriptions.append(link)

    for text in descriptions:
        low = text.lower()
        for repo, pattern in CROSS_REF_PATTERNS:
            if pattern in low:
                # extract the id right after the pattern
                rest = low.split(pattern, 1)[1].strip()
                ds_id = rest.split("/")[0].split("?")[0].strip() or None
                key = (repo, ds_id)
                if ds_id and key not in seen:
                    seen.add(key)
                    refs.append({"repository": repo, "sourceDatasetId": ds_id})
    return refs


# ─────────────────────────────────────────────────────────────────────────────
# Matching (pure)
# ─────────────────────────────────────────────────────────────────────────────


def _doi_conflict(existing: dict, incoming: dict) -> bool:
    """True when both records have DOIs and they differ → never merge."""
    a = normalize_doi(existing.get("doi"))
    b = normalize_doi(incoming.get("doi"))
    return bool(a and b and a != b)


def _multi_field_strong_match(existing: dict, incoming: dict) -> bool:
    """Strong multi-field identity (spec §9 key 4).

    CROSS-REPOSITORY ONLY: within a single repository the repository ID is
    authoritative, so two distinct source IDs are different datasets even if
    their titles/counts match (observed live: distinct ds001566/ds003714
    datasets both titled "Test"). Multi-field identity therefore requires
    the incoming repository to NOT already be attached to the existing
    record.

    Then requires an EXACT normalized-title match AND ≥2 of:
    - ≥1 common normalized author
    - equal non-null participant count
    - ≥1 common modality
    - equal non-null publication year
    """
    in_repo = incoming.get("repository")
    existing_repos = {s.get("repository") for s in (existing.get("sources") or [])}
    if in_repo in existing_repos:
        return False  # same-repo: repo ID is authoritative, never fuzzy-merge

    in_id = source_identity(incoming)
    ex_title = normalize_title(existing.get("title"))
    if not ex_title or ex_title != in_id["titleNorm"]:
        return False

    signals = 0
    ex_authors = {
        normalize_title(a) for a in (existing.get("authors") or []) if normalize_title(a)
    }
    if ex_authors and (ex_authors & in_id["authorsNorm"]):
        signals += 1
    if (
        existing.get("participantCount") is not None
        and existing.get("participantCount") == in_id["participantCount"]
        and in_id["participantCount"] is not None
    ):
        signals += 1
    ex_modality = set(existing.get("modality") or [])
    if ex_modality and in_id["modalityNorm"] and (ex_modality & in_id["modalityNorm"]):
        signals += 1
    if (
        existing.get("publicationYear") is not None
        and existing.get("publicationYear") == in_id["publicationYear"]
        and in_id["publicationYear"] is not None
    ):
        signals += 1
    return signals >= 2


def evaluate_identity(existing: dict, incoming: dict) -> dict:
    """Evaluate whether *incoming* is the same underlying dataset as
    *existing* and, if so, how confidently.

    Returns::

        {"match": bool, "matchedVia": str|None, "ambiguous": bool}

    ``ambiguous=True`` means a candidate was found but identity is NOT
    certain enough to merge — the caller keeps both records.
    """
    in_id = source_identity(incoming)
    ex_id = {
        "doi": normalize_doi(existing.get("doi")),
        "sourceUrlNorm": normalize_url_key(
            next(
                (
                    s.get("sourceUrl")
                    for s in (existing.get("sources") or [])
                    if s.get("sourceUrl")
                ),
                None,
            )
        ),
        "sourceKeys": set(existing.get("sourceKeys") or []),
    }

    # 1) DOI — strongest signal, but conflicting DOIs block everything.
    if _doi_conflict(existing, incoming):
        return {"match": False, "matchedVia": None, "ambiguous": True}

    if in_id["doi"] and ex_id["doi"] == in_id["doi"]:
        return {"match": True, "matchedVia": "doi", "ambiguous": False}

    # 2) Strong repository-provided cross-reference.
    for ref in extract_cross_references(incoming):
        key = f"{ref['repository']}:{ref['sourceDatasetId']}"
        if key in ex_id["sourceKeys"]:
            return {"match": True, "matchedVia": "cross_reference", "ambiguous": False}

    # 3) Canonical source URL.
    if in_id["sourceUrlNorm"] and in_id["sourceUrlNorm"] == ex_id["sourceUrlNorm"]:
        return {"match": True, "matchedVia": "source_url", "ambiguous": False}

    # Same source key (same repo + id) — same record, refresh it.
    if in_id["sourceKey"] in ex_id["sourceKeys"]:
        return {"match": True, "matchedVia": "source_key", "ambiguous": False}

    # 4) Strong multi-field identity — conservative by design.
    if _multi_field_strong_match(existing, incoming):
        # DOI conflict already excluded above.
        return {"match": True, "matchedVia": "multi_field", "ambiguous": False}

    # Title/author overlap that did NOT reach the strong threshold is an
    # ambiguous candidate — kept separate, never merged.
    if (
        in_id["titleNorm"]
        and normalize_title(existing.get("title")) == in_id["titleNorm"]
    ):
        return {"match": False, "matchedVia": None, "ambiguous": True}

    return {"match": False, "matchedVia": None, "ambiguous": False}


# ─────────────────────────────────────────────────────────────────────────────
# Merge (pure) — spec §10: attach, never discard
# ─────────────────────────────────────────────────────────────────────────────


def _merge_union(existing: list, incoming: list) -> list:
    out = list(existing or [])
    for item in incoming or []:
        if item not in out:
            out.append(item)
    return out


def merge_source_into_canonical(canonical: dict, incoming: dict, matched_via: str) -> dict:
    """Merge a duplicate source INTO the existing canonical record.

    - Attaches the second source (never discarded).
    - Unions list-valued fields (modality, species, tasks, sessions,
      authors, ageGroup, ...) so repository-specific metadata survives.
    - First-non-null wins for scalars; raw metadata is kept per repository.
    - Recomputes ageGroup from the merged ages to stay consistent.
    """
    repo = incoming.get("repository")
    src_id = incoming.get("sourceDatasetId")
    key = f"{repo}:{src_id}"

    sources = list(canonical.get("sources") or [])
    existing_idx = next(
        (i for i, s in enumerate(sources) if f"{s.get('repository')}:{s.get('sourceDatasetId')}" == key),
        None,
    )
    slim = dict(incoming)
    slim.pop("rawMetadata", None)
    if existing_idx is None:
        sources.append(slim)
    else:
        sources[existing_idx] = slim  # refresh same-source entry

    # Scalars — first non-null wins (existing preserved).
    for field in (
        "title", "description", "readme", "doi", "license", "datasetType",
        "availability", "studyType", "studyDesign", "studyDomain",
        "longitudinal", "datasetSizeBytes", "sizeLabel", "publicationYear",
        "participantCount", "lastUpdated", "createdAt",
    ):
        if field in incoming and incoming.get(field) is not None:
            if canonical.get(field) in (None, [], ""):
                canonical[field] = incoming.get(field)

    # Lists — union.
    for field in ("modality", "species", "tasks", "sessions", "authors", "keywords"):
        canonical[field] = _merge_union(canonical.get(field), incoming.get(field))
    canonical["ageGroup"] = _merge_union(canonical.get("ageGroup"), incoming.get("ageGroup"))
    canonical["ages"] = _merge_union(canonical.get("ages"), incoming.get("ages"))
    # Recompute from merged ages for consistency with the validation rule.
    canonical["ageGroup"] = derive_age_groups(canonical.get("ages")) or canonical.get("ageGroup") or []
    canonical["ageGroup"] = list(dict.fromkeys(canonical["ageGroup"]))

    for c in incoming.get("contributors") or []:
        if c not in (canonical.get("contributors") or []):
            canonical.setdefault("contributors", []).append(c)

    # BrainRegions / disease from a repo that provides them (spec §10 example).
    if incoming.get("brainRegions") and not canonical.get("brainRegions"):
        canonical["brainRegions"] = list(incoming["brainRegions"])
    if incoming.get("disease") and not canonical.get("disease"):
        canonical["disease"] = list(incoming["disease"])
    if incoming.get("analysisMethods") and not canonical.get("analysisMethods"):
        canonical["analysisMethods"] = list(incoming["analysisMethods"])

    # Source keys + raw metadata (per repository — never overwrite others).
    keys = list(canonical.get("sourceKeys") or [])
    if key not in keys:
        keys.append(key)
    canonical["sourceKeys"] = keys
    raw = dict(canonical.get("rawMetadata") or {})
    raw[repo] = incoming.get("rawMetadata")  # repo-scoped, never clobbers others
    canonical["rawMetadata"] = raw
    canonical["sources"] = sources

    # Provenance — record how identity was established.
    prov = dict(canonical.get("provenance") or {})
    identity = dict(prov.get("identity") or {})
    identity["matchedVia"] = matched_via
    prov["identity"] = identity
    prov["updatedAt"] = incoming.get("provenance", {}).get("retrievedAt")
    canonical["provenance"] = prov
    return canonical
