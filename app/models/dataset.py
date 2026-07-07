"""
The common schema every source connector normalizes its data into.
This is the shape stored in MongoDB and returned by the search API.
"""
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, HttpUrl


class TrustTier(str, Enum):
    VERIFIED = "verified"        # link checked recently, from an official connector
    UNVERIFIED = "unverified"    # surfaced by the fallback agent, not yet checked
    STALE = "stale"              # previously verified, link check failed on re-check


class Dataset(BaseModel):
    id: str | None = Field(default=None, alias="_id")

    title: str
    description: str | None = None
    source: str                       # e.g. "openneuro", "dandi"
    source_id: str                    # the ID within that source
    url: HttpUrl

    modality: list[str] = Field(default_factory=list)   # e.g. ["fMRI", "EEG"]
    species: list[str] = Field(default_factory=list)     # e.g. ["human", "mouse"]
    subject_count: int | None = None
    keywords: list[str] = Field(default_factory=list)

    license: str | None = None

    is_direct_link: bool = False
    """True if the URL points at an actual data file/archive rather than a repository landing page."""

    trust_tier: TrustTier = TrustTier.UNVERIFIED
    confidence_score: float | None = None   # set by the ranking service

    embedding: list[float] | None = Field(default=None, exclude=True)  # never serialize to API responses

    last_verified_at: datetime | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    model_config = {"populate_by_name": True}
