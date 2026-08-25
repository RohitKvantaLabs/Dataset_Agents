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
    age_range: str | None = None                            # canonical AGE_TERMS label ("child", "adolescent", …) or numeric "0-12"
    region: str | None = None                               # e.g. "hippocampus", "amygdala"
    condition: list[str] = Field(default_factory=list)      # e.g. ["ADHD"]
    task: str | None = None                                  # e.g. "resting-state"
    format: list[str] = Field(default_factory=list)          # e.g. ["NIfTI", "BIDS"]
    keywords: list[str] = Field(default_factory=list)        # free-text fallback terms
    raw_query: str
    # ponytail: domain guard — False means no neuroscience signal was found; Node
    # must stop retrieval immediately. Defaults True so keyword-only fallback
    # (API down) never incorrectly blocks legitimate queries.
    in_domain: bool = True


class ParseQueryRequest(BaseModel):
    query: str = Field(..., min_length=2, max_length=500)


class ParseQueryResponse(BaseModel):
    filters: QueryFilters
