"""
Normalizer — raw upstream records → Common Schema (Dataset).

Each source has its own raw field layout; this module contains
source-specific mappers that convert the raw dict produced by a
connector into a validated Dataset instance.

Adding a new source
-------------------
1. Write a ``_normalize_<source>(raw: dict) -> Dataset`` function.
2. Register it in ``NORMALIZER_MAP``.
3. The pipeline will pick it up automatically via source_name.
"""
import logging
from typing import Callable

from app.models.dataset import Dataset, DatasetFlags

logger = logging.getLogger("neuro_platform.ingestion.normalizer")


# ---------------------------------------------------------------------------
# Source-specific mappers
# ---------------------------------------------------------------------------

def _normalize_dandi(raw: dict) -> Dataset:
    """
    Map a DANDI API dandiset record to Dataset.

    DANDI record fields of interest
    --------------------------------
    identifier       : str  e.g. "DANDI:000003"
    created          : ISO8601 str
    modified         : ISO8601 str
    metadata         : dict
      name           : str  (dataset title)
      description    : str
      license        : list[dict]  [{identifier: "spdx:CC-BY-4.0"}]
      species        : list[dict]  [{identifier: "...", name: "Homo sapiens"}]
      numberOfSubjects : int
      measurementTechnique : list[dict]  [{name: "electrophysiology"}]
    """
    meta = raw.get("metadata") or raw  # some versions inline metadata
    identifier = raw.get("identifier") or meta.get("identifier", "")

    title = (
        meta.get("name")
        or raw.get("name")
        or f"DANDI Dataset {identifier}"
    )
    description = meta.get("description") or ""
    url = f"https://dandiarchive.org/dandiset/{identifier.replace('DANDI:', '').lstrip('0') or identifier}"

    # License
    licenses = meta.get("license") or []
    license_str: str | None = None
    if licenses:
        first_lic = licenses[0]
        license_str = first_lic.get("identifier") or first_lic.get("name")

    # Species
    species_list = meta.get("species") or []
    species: str | None = species_list[0].get("name") if species_list else None

    # Subject count
    subject_count: int | None = meta.get("numberOfSubjects") or meta.get("number_of_subjects")

    # Modalities from measurementTechnique
    techniques = meta.get("measurementTechnique") or []
    modalities = [t.get("name", "") for t in techniques if t.get("name")]

    return Dataset(
        title=title,
        description=description,
        source_repository="dandi",
        original_url=url,  # type: ignore[arg-type]
        modalities=modalities,
        subject_count=subject_count,
        species=species,
        license=license_str,
        is_bids_compliant=False,
        flags=DatasetFlags(),
    )


def _normalize_openneuro(raw: dict) -> Dataset:
    """
    Map an OpenNeuro GraphQL node to Dataset.

    GraphQL node fields of interest
    --------------------------------
    id          : str  e.g. "ds000003"
    description : dict
      Name      : str
      Authors   : list[str]
      License   : str
      DatasetDOI: str
    metadata    : dict
      modalities     : list[str]
      subjectCount   : int
      species        : str
    """
    desc = raw.get("description") or {}
    meta = raw.get("metadata") or {}
    ds_id = raw.get("id", "")

    title = desc.get("Name") or raw.get("name") or f"OpenNeuro Dataset {ds_id}"
    description = ""  # OpenNeuro description object has no free-text body field
    url = f"https://openneuro.org/datasets/{ds_id}"

    modalities: list[str] = meta.get("modalities") or []
    subject_count: int | None = meta.get("subjectCount")
    species: str | None = meta.get("species")
    license_str: str | None = desc.get("License")

    return Dataset(
        title=title,
        description=description,
        source_repository="openneuro",
        original_url=url,  # type: ignore[arg-type]
        modalities=modalities,
        subject_count=subject_count,
        species=species,
        license=license_str,
        is_bids_compliant=True,  # OpenNeuro enforces BIDS compliance
        flags=DatasetFlags(green=["BIDS compliant (OpenNeuro)"]),
    )


# ---------------------------------------------------------------------------
# Registry & public API
# ---------------------------------------------------------------------------

NORMALIZER_MAP: dict[str, Callable[[dict], Dataset]] = {
    "dandi": _normalize_dandi,
    "openneuro": _normalize_openneuro,
}


def normalize(raw: dict, source_name: str) -> Dataset | None:
    """
    Convert a raw upstream record into a validated Dataset.

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
