"""
Normalizer — raw upstream records → Common Schema.

Two entry points:

- ``normalize(raw, source_name)`` — batch ingestion path (existing), converts
  raw records into the current ``Dataset`` schema.
- ``normalize_repository(raw, source_name)`` — online retrieval path (§2.2),
  converts raw records into the intermediate ``RepositoryDataset`` schema
  produced by ``connector.search()`` before the quality pipeline runs.

Adding a new source
-------------------
1. Write a ``_normalize_<source>(raw)`` and/or ``_repository_<source>(raw)``
   mapper.
2. Register it in ``NORMALIZER_MAP`` / ``REPOSITORY_NORMALIZER_MAP``.
3. The pipeline / connectors pick it up automatically via source_name.

Mapping rules (§2.2):
- ``source`` / ``source_id`` / ``url`` / ``title`` are mandatory for
  ``RepositoryDataset``; a record missing them is dropped (returns None).
- ``modality`` / ``species`` come from repository-native metadata first;
  if absent they are filled by Stage 3 vocabulary matching (no LLM).
- ``access_tier`` is NOT guessed here — it is populated from the static
  per-source lookup (§2.17) at Stage 3.
- ``raw`` preserves the full upstream payload for Stage 7 provenance.
"""
import logging
from datetime import datetime
from typing import Callable

from app.models.dataset import Dataset
from app.models.repository_dataset import RepositoryDataset, RepositoryFile

logger = logging.getLogger("neuro_platform.ingestion.normalizer")


def _parse_iso(value: str | None) -> datetime | None:
    """Best-effort ISO8601 → datetime; returns None on any failure."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _first_str(d: dict | None, *keys: str) -> str | None:
    """First non-empty string value among *keys* (case-insensitive dict keys)."""
    if not d:
        return None
    for key in keys:
        for k, v in d.items():
            if k.lower() == key.lower() and isinstance(v, str) and v.strip():
                return v.strip()
    return None


def _list_of_str(value: object) -> list[str]:
    """
    Normalize a value that may be a str, list[str], list[dict], or None into
    list[str]. Dict entries (e.g. Dryad/EBRAINS author objects) are unwrapped
    via their name-ish keys instead of being stringified into garbage.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, (list, tuple)):
        out: list[str] = []
        for v in value:
            if isinstance(v, dict):
                name = (
                    v.get("name")
                    or v.get("fullName")
                    or v.get("title")
                    or _join_name_parts(v)
                )
                if name and str(name).strip():
                    out.append(str(name).strip())
            elif str(v).strip():
                out.append(str(v).strip())
        return out
    return []


def _join_name_parts(d: dict) -> str | None:
    """Combine firstName/lastName (or givenName/familyName) into one string."""
    first = d.get("firstName") or d.get("givenName") or ""
    last = d.get("lastName") or d.get("familyName") or ""
    joined = f"{first} {last}".strip()
    return joined or None


def _map_zenodo_access_right(value: str | None) -> str | None:
    """Zenodo ``metadata.access_right`` → access_tier (record-level override)."""
    mapping = {
        "open": "open",
        "restricted": "registered",
        "closed": "restricted",
        "embargoed": "registered",
    }
    return mapping.get((value or "").lower())


# ---------------------------------------------------------------------------
# Batch mappers (raw → Dataset) — current schema, P4-1 prerequisite fix
# ---------------------------------------------------------------------------

def _normalize_dandi(raw: dict) -> Dataset:
    meta = raw.get("metadata") or raw
    identifier = raw.get("identifier") or meta.get("identifier", "")

    title = (
        _first_str(meta, "name")
        or _first_str(raw, "name")
        or f"DANDI Dataset {identifier}"
    )
    description = _first_str(meta, "description") or ""

    num = str(identifier).replace("DANDI:", "").lstrip("0") or identifier
    url = f"https://dandiarchive.org/dandiset/{num}"

    licenses = meta.get("license") or []
    license_str: str | None = None
    if isinstance(licenses, list) and licenses:
        first = licenses[0] if isinstance(licenses[0], dict) else {}
        license_str = first.get("identifier") or first.get("name")

    species_list = meta.get("species") or []
    species = []
    for s in species_list if isinstance(species_list, list) else []:
        if isinstance(s, dict):
            name = s.get("name")
        else:
            name = s
        if name:
            species.append(str(name))

    subject_count: int | None = meta.get("numberOfSubjects") or meta.get("number_of_subjects")

    techniques = meta.get("measurementTechnique") or []
    modalities = [
        t.get("name", "") for t in techniques if isinstance(t, dict) and t.get("name")
    ]

    return Dataset(
        title=title,
        description=description,
        source="dandi",
        source_id=str(identifier),
        url=url,
        modality=[str(m) for m in modalities],
        species=species,
        subject_count=subject_count,
        license=license_str,
    )


def _normalize_openneuro(raw: dict) -> Dataset:
    desc = raw.get("description") or {}
    meta = raw.get("metadata") or {}
    ds_id = raw.get("id", "")

    title = (
        _first_str(desc, "Name")
        or _first_str(raw, "name")
        or f"OpenNeuro Dataset {ds_id}"
    )
    description = ""
    url = f"https://openneuro.org/datasets/{ds_id}"

    modalities = _list_of_str(meta.get("modalities"))
    subject_count: int | None = meta.get("subjectCount")
    species = _list_of_str(meta.get("species"))
    license_str: str | None = _first_str(desc, "License")

    return Dataset(
        title=title,
        description=description,
        source="openneuro",
        source_id=str(ds_id),
        url=url,
        modality=[str(m) for m in modalities],
        species=species,
        subject_count=subject_count,
        license=license_str,
    )


NORMALIZER_MAP: dict[str, Callable[[dict], Dataset]] = {
    "dandi": _normalize_dandi,
    "openneuro": _normalize_openneuro,
}


def normalize(raw: dict, source_name: str) -> Dataset | None:
    """
    Convert a raw upstream record into a validated Dataset (batch path).

    Returns None if the record cannot be normalised (e.g. missing URL)
    so the pipeline can skip it without crashing.
    """
    mapper = NORMALIZER_MAP.get(source_name.lower())
    if mapper is None:
        logger.warning("No normalizer registered for source %r — skipping", source_name)
        return None

    try:
        return mapper(raw)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to normalize record from %r: %s | raw keys: %s",
            source_name,
            exc,
            list(raw.keys())[:10],
        )
        return None


# ---------------------------------------------------------------------------
# Repository mappers (raw → RepositoryDataset) — §2.2, used by connector.search()
# ---------------------------------------------------------------------------

def _repository_dandi(raw: dict) -> RepositoryDataset | None:
    meta = raw.get("metadata") or raw
    identifier = raw.get("identifier") or meta.get("identifier")
    if not identifier:
        return None

    num = str(identifier).replace("DANDI:", "").lstrip("0") or identifier
    url = f"https://dandiarchive.org/dandiset/{num}"
    title = (
        _first_str(meta, "name")
        or _first_str(raw, "name")
        or f"DANDI Dataset {identifier}"
    )
    if not title or not url:
        return None

    licenses = meta.get("license") or []
    license_str: str | None = None
    if isinstance(licenses, list) and licenses:
        first = licenses[0] if isinstance(licenses[0], dict) else {}
        license_str = first.get("identifier") or first.get("name")

    species = []
    for s in (meta.get("species") or []):
        if isinstance(s, dict):
            name = s.get("name")
        else:
            name = s
        if name:
            species.append(str(name))

    techniques = meta.get("measurementTechnique") or []
    modalities = [
        t.get("name", "") for t in techniques if isinstance(t, dict) and t.get("name")
    ]

    return RepositoryDataset(
        source="dandi",
        source_id=str(identifier),
        url=url,
        title=title,
        description=_first_str(meta, "description"),
        modality=[str(m) for m in modalities],
        species=species,
        subject_count=meta.get("numberOfSubjects") or meta.get("number_of_subjects"),
        license=license_str,
        version=_first_str(raw, "version"),
        updated_at=_parse_iso(raw.get("modified") or raw.get("created")),
        published_at=_parse_iso(raw.get("created")),
        raw=raw,
    )


def _repository_openneuro(raw: dict) -> RepositoryDataset | None:
    desc = raw.get("description") or {}
    meta = raw.get("metadata") or {}
    ds_id = raw.get("id")
    if not ds_id:
        return None

    url = f"https://openneuro.org/datasets/{ds_id}"
    title = _first_str(desc, "Name") or _first_str(raw, "name") or f"OpenNeuro Dataset {ds_id}"
    if not title:
        return None

    authors = _list_of_str(desc.get("Authors"))

    return RepositoryDataset(
        source="openneuro",
        source_id=str(ds_id),
        url=url,
        title=title,
        description=None,  # OpenNeuro description object has no free-text body field
        modality=[str(m) for m in _list_of_str(meta.get("modalities"))],
        species=_list_of_str(meta.get("species")),
        subject_count=meta.get("subjectCount"),
        license=_first_str(desc, "License"),
        doi=_first_str(desc, "DatasetDOI"),
        authors=authors,
        version=_first_str(raw, "version"),
        published_at=_parse_iso(raw.get("created")),
        updated_at=_parse_iso(raw.get("modified") or raw.get("created")),
        raw=raw,
    )


def _repository_neurovault(raw: dict) -> RepositoryDataset | None:
    cid = raw.get("id")
    if cid is None:
        return None
    url = f"https://neurovault.org/collections/{cid}/"
    title = _first_str(raw, "name") or f"NeuroVault Collection {cid}"
    if not title:
        return None

    return RepositoryDataset(
        source="neurovault",
        source_id=str(cid),
        url=url,
        title=title,
        description=_first_str(raw, "description"),
        subject_count=raw.get("number_of_images"),
        doi=_first_str(raw, "DOI"),
        authors=_list_of_str(raw.get("authors")),
        updated_at=_parse_iso(raw.get("modify_date") or raw.get("modified")),
        published_at=_parse_iso(raw.get("create_date") or raw.get("created")),
        raw=raw,
    )


def _repository_ebrains(raw: dict) -> RepositoryDataset | None:
    inst_id = raw.get("id")
    if not inst_id:
        return None
    # KG instance id may be a full URL or a bare UUID — keep the tail.
    tail = str(inst_id).rstrip("/").split("/")[-1]
    url = f"https://search.kg.ebrains.eu/instances/{tail}"
    title = (
        _first_str(raw, "displayName")
        or _first_str(raw, "name")
        or _first_str(raw, "title")
        or f"EBRAINS Dataset {tail}"
    )
    if not title:
        return None

    return RepositoryDataset(
        source="ebrains",
        source_id=tail,
        url=url,
        title=title,
        description=_first_str(raw, "description"),
        license=_first_str(raw, "license"),
        authors=_list_of_str(raw.get("contributors") or raw.get("authors")),
        updated_at=_parse_iso(raw.get("lastModified") or raw.get("modified")),
        published_at=_parse_iso(raw.get("firstReleased") or raw.get("created")),
        raw=raw,
    )


def _repository_zenodo(raw: dict) -> RepositoryDataset | None:
    rec_id = raw.get("id")
    if rec_id is None:
        return None
    meta = raw.get("metadata") or {}
    links = raw.get("links") or {}
    url = links.get("html") or f"https://zenodo.org/records/{rec_id}"
    title = _first_str(meta, "title") or f"Zenodo Record {rec_id}"
    if not title:
        return None

    license_info = meta.get("license") or {}
    license_str = license_info.get("id") if isinstance(license_info, dict) else None

    files = [
        {
            "name": f.get("key", ""),
            "size_bytes": f.get("size"),
            "download_url": (f.get("links") or {}).get("self") if isinstance(f, dict) else None,
        }
        for f in (raw.get("files") or [])
        if isinstance(f, dict) and f.get("key")
    ]

    return RepositoryDataset(
        source="zenodo",
        source_id=str(rec_id),
        url=url,
        title=title,
        description=_first_str(meta, "description"),
        keywords=_list_of_str(meta.get("keywords")),
        license=license_str,
        doi=raw.get("doi") or _first_str(meta, "doi"),
        authors=[a for a in (_first_str(c, "name") for c in (meta.get("creators") or []) if isinstance(c, dict)) if a],
        version=_first_str(meta, "version"),
        access_tier=_map_zenodo_access_right(_first_str(meta, "access_right")),
        published_at=_parse_iso(raw.get("created") or raw.get("published")),
        updated_at=_parse_iso(raw.get("updated")),
        files=[RepositoryFile(**f) for f in files],
        raw=raw,
    )


def _repository_figshare(raw: dict) -> RepositoryDataset | None:
    art_id = raw.get("id")
    if art_id is None:
        return None
    url = raw.get("url_public_html") or f"https://figshare.com/articles/dataset/{art_id}"
    title = _first_str(raw, "title") or f"Figshare Article {art_id}"
    if not title:
        return None

    files = [
        {
            "name": f.get("name", ""),
            "size_bytes": f.get("size"),
            "download_url": f.get("download_url"),
        }
        for f in (raw.get("files") or [])
        if isinstance(f, dict) and f.get("name")
    ]

    return RepositoryDataset(
        source="figshare",
        source_id=str(art_id),
        url=url,
        title=title,
        description=_first_str(raw, "description"),
        keywords=_list_of_str(raw.get("tags")),
        license=(raw.get("license") or {}).get("name") if isinstance(raw.get("license"), dict) else None,
        doi=raw.get("doi"),
        authors=[a for a in (_first_str(a, "name") for a in (raw.get("authors") or []) if isinstance(a, dict)) if a],
        published_at=_parse_iso(raw.get("published_date") or raw.get("published_at")),
        updated_at=_parse_iso(raw.get("modified_date") or raw.get("modified_at")),
        files=[RepositoryFile(**f) for f in files],
        raw=raw,
    )


def _repository_dryad(raw: dict) -> RepositoryDataset | None:
    ident = raw.get("identifier")
    if not ident:
        return None
    doi = str(ident)
    # DOI suffix is the repository-native id (package identifier).
    suffix = doi.split("/")[-1] if "/" in doi else doi
    url = f"https://datadryad.org/stash/dataset/{doi}"
    title = _first_str(raw, "title") or f"Dryad Dataset {suffix}"
    if not title:
        return None

    return RepositoryDataset(
        source="dryad",
        source_id=suffix,
        url=url,
        title=title,
        description=_first_str(raw, "abstract"),
        license=_first_str(raw, "license"),
        doi=doi,
        authors=[a for a in (_first_str(a, "name") for a in (raw.get("authors") or []) if isinstance(a, dict)) if a]
        or _list_of_str(raw.get("authors")),
        published_at=_parse_iso(raw.get("publicationDate") or raw.get("created")),
        updated_at=_parse_iso(raw.get("modified")),
        raw=raw,
    )


def _repository_osf(raw: dict) -> RepositoryDataset | None:
    node_id = raw.get("id")
    if not node_id:
        return None
    attrs = raw.get("attributes") or {}
    url = raw.get("links", {}).get("html") if isinstance(raw.get("links"), dict) else None
    url = url or f"https://osf.io/{node_id}/"
    title = _first_str(attrs, "title") or f"OSF Project {node_id}"
    if not title:
        return None

    return RepositoryDataset(
        source="osf",
        source_id=str(node_id),
        url=url,
        title=title,
        description=_first_str(attrs, "description"),
        keywords=_list_of_str(attrs.get("tags")),
        authors=_list_of_str(attrs.get("contributors")),
        updated_at=_parse_iso(attrs.get("date_modified") or attrs.get("modified")),
        published_at=_parse_iso(attrs.get("date_created") or attrs.get("created")),
        raw=raw,
    )


def _repository_nitrc(raw: dict) -> RepositoryDataset | None:
    pid = raw.get("id")
    if pid is None:
        return None
    slug = raw.get("slug") or str(pid)
    url = f"https://www.nitrc.org/projects/{slug}"
    title = _first_str(raw, "name") or _first_str(raw, "title") or f"NITRC Project {pid}"
    if not title:
        return None

    return RepositoryDataset(
        source="nitrc",
        source_id=str(pid),
        url=url,
        title=title,
        description=_first_str(raw, "description"),
        keywords=_list_of_str(raw.get("keywords") or raw.get("tags")),
        license=_first_str(raw, "license"),
        updated_at=_parse_iso(raw.get("modified")),
        published_at=_parse_iso(raw.get("created")),
        raw=raw,
    )


REPOSITORY_NORMALIZER_MAP: dict[str, Callable[[dict], RepositoryDataset | None]] = {
    "openneuro": _repository_openneuro,
    "dandi": _repository_dandi,
    "neurovault": _repository_neurovault,
    "ebrains": _repository_ebrains,
    "zenodo": _repository_zenodo,
    "figshare": _repository_figshare,
    "dryad": _repository_dryad,
    "osf": _repository_osf,
    "nitrc": _repository_nitrc,
}


def normalize_repository(raw: dict, source_name: str) -> RepositoryDataset | None:
    """
    Convert a raw upstream record into a validated RepositoryDataset (§2.2).

    Returns None if the source has no mapper or the record is missing a
    mandatory field (source/source_id/url/title), so connectors can drop it.
    """
    mapper = REPOSITORY_NORMALIZER_MAP.get(source_name.lower())
    if mapper is None:
        logger.warning("No repository normalizer registered for source %r — skipping", source_name)
        return None

    try:
        return mapper(raw)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Failed to normalize repository record from %r: %s | raw keys: %s",
            source_name,
            exc,
            list(raw.keys())[:10],
        )
        return None
