"""
Repository Dataset — normalized retrieval schema (§2.2).

The intermediate schema produced by ``connector.search()`` before the
quality pipeline runs. Field names reuse the existing ``Dataset`` common
schema wherever possible.

Mapping rules (§2.2):
- ``source`` / ``source_id`` / ``url`` / ``title`` / ``description`` are
  mandatory; a record missing them is dropped at Stage 1.
- ``modality`` / ``species`` come from repository-native metadata first;
  if absent they are filled by Stage 3 vocabulary matching (no LLM).
- ``access_tier`` is populated from the static per-source lookup (§2.17)
  at Stage 3, **not** guessed by a connector. A connector only sets it
  when the record itself explicitly declares a value.
- ``raw`` preserves the full upstream payload for Stage 7 provenance
  and debugging.
"""
from datetime import datetime

from pydantic import BaseModel, Field


class RepositoryFile(BaseModel):
    name: str
    size_bytes: int | None = None
    format: str | None = None          # e.g. "nifti", "edf", "zip"
    checksum: str | None = None
    download_url: str | None = None


class RepositoryDataset(BaseModel):
    source: str                        # registry key: "openneuro" | "dandi" | ...
    source_id: str                     # repository-native id (stringified)
    url: str                           # canonical landing/access URL
    title: str
    description: str | None = None
    modality: list[str] = Field(default_factory=list)
    species: list[str] = Field(default_factory=list)
    subject_count: int | None = None
    keywords: list[str] = Field(default_factory=list)
    license: str | None = None         # SPDX id when resolvable
    doi: str | None = None
    authors: list[str] = Field(default_factory=list)
    version: str | None = None
    published_at: datetime | None = None
    updated_at: datetime | None = None
    access_tier: str | None = None     # 'open' | 'registered' | 'restricted'
    size_label: str | None = None
    # Enrichment outputs populated by Stage 3 (§3.3 item 5 — deterministic
    # vocabulary/pattern match, never LLM). Kept through to publication
    # (§2.14: "keeping … region, age_group, disease when populated by Stage 3").
    region: str | None = None
    age_group: str | None = None
    disease: str | None = None
    # Retrieval V2 Phase 1 — canonical TASK_VOCAB label populated by Stage 3
    # enrichment from dataset-owned evidence only; None stays None.
    task: str | None = None
    files: list[RepositoryFile] = Field(default_factory=list)
    is_direct_link: bool = False
    raw: dict = Field(default_factory=dict)  # full upstream record (provenance/debug)
