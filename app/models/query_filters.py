from pydantic import BaseModel, Field


class QueryFilters(BaseModel):
    """
    Strict structured output of the Query Understanding Agent.
    No open-ended generation - every field is either a known category,
    a list of strings, or null. This is what gets sent back to Node so
    Node can build its own Mongo query.
    """
    modality: list[str] = Field(default_factory=list)      # e.g. ["fMRI", "EEG"]
    species: list[str] = Field(default_factory=list)        # e.g. ["human"]
    age_range: str | None = None                            # e.g. "pediatric", "0-12"
    condition: list[str] = Field(default_factory=list)      # e.g. ["ADHD"]
    task: str | None = None                                  # e.g. "resting-state"
    format: list[str] = Field(default_factory=list)          # e.g. ["NIfTI", "BIDS"]
    keywords: list[str] = Field(default_factory=list)        # free-text fallback terms
    raw_query: str


class ParseQueryRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=500)


class ParseQueryResponse(BaseModel):
    filters: QueryFilters
