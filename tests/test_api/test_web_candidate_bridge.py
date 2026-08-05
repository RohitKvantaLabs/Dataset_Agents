"""
Web-candidate bridge integrity (Issue 1).

``_web_candidate_to_repository_dataset()`` must NEVER copy structured metadata
from the user's query (modality/species/condition/task/format/keywords) onto a
discovered dataset. Structured metadata may only originate from repository
metadata, verified enrichment, ontology normalization, or explicit dataset
content — never from the search query. Unknown fields stay ``[]``/``None``.
"""
from app.agents.fallback_agent import FallbackCandidate
from app.api.v1.agents import _web_candidate_to_repository_dataset
from app.models.query_filters import QueryFilters


def _candidate(title: str = "Some discovered page", url: str = "https://example.org/result") -> FallbackCandidate:
    return FallbackCandidate(
        title=title,
        url=url,
        source_guess="openneuro",
        reasoning="A web result about the topic",
    )


def _filters(**overrides) -> QueryFilters:
    base = dict(
        modality=["MEG"],
        species=["human"],
        age_range=None,
        region="motor cortex",
        condition=["Parkinson disease"],
        task="resting-state",
        format=["NIfTI"],
        keywords=["pd"],
        raw_query="Find Parkinson disease MEG datasets",
    )
    base.update(overrides)
    return QueryFilters(**base)


def test_no_structured_metadata_copied_from_filters() -> None:
    ds = _web_candidate_to_repository_dataset(_candidate(), _filters())
    assert ds is not None
    # The user query must never fabricate structured metadata on the candidate.
    assert ds.modality == []
    assert ds.species == []
    assert ds.keywords == []
    assert ds.region is None
    assert ds.disease is None
    assert ds.age_group is None


def test_known_fields_still_carried() -> None:
    ds = _web_candidate_to_repository_dataset(
        _candidate(title="  Real dataset title  ", url="https://openneuro.org/datasets/ds000001"),
        _filters(),
    )
    assert ds is not None
    assert ds.title == "Real dataset title"
    assert ds.url == "https://openneuro.org/datasets/ds000001"
    assert ds.source == "web_search"
    # source_guess stays provenance-only in raw; never becomes the source.
    assert ds.raw == {"source_guess": "openneuro", "discovery": "web"}


def test_empty_url_returns_none() -> None:
    assert _web_candidate_to_repository_dataset(_candidate(url=""), _filters()) is None
