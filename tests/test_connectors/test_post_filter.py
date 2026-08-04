"""
post_filter — Issue 4 evidence gate (query-first stabilization).

When a specific modality is explicitly requested, a candidate that declares
NO modality is kept only if it carries neuroscience evidence in its
title/description/keywords (Stage 3 can still enrich it). Candidates with
neither modality nor neuroscience evidence are unrelated to the request and
filtered — this removes loosely-related generic-repository records without
reducing coverage for anything with a neuroscience signal.
"""
from app.connectors.base import post_filter
from app.models.query_filters import QueryFilters
from app.models.repository_dataset import RepositoryDataset


def _dataset(**overrides) -> RepositoryDataset:
    base = dict(
        source="zenodo",
        source_id="z1",
        url="https://zenodo.org/records/1",
        title="Some dataset",
        description=None,
        modality=[],
        species=[],
        keywords=[],
    )
    base.update(overrides)
    return RepositoryDataset(**base)


def _meg_filters() -> QueryFilters:
    return QueryFilters(raw_query="find MEG datasets", modality=["MEG"])


class TestModalityEvidenceGate:
    def test_no_modality_no_neuroscience_evidence_filtered(self) -> None:
        ds = _dataset(title="Helicopter parenting and adjustment in emerging adults")
        assert post_filter(ds, _meg_filters()) is False

    def test_no_modality_but_neuroscience_evidence_kept(self) -> None:
        ds = _dataset(
            title="Resting-state magnetoencephalography recordings",
            description="Neuromagnetic activity from healthy volunteers.",
        )
        assert post_filter(ds, _meg_filters()) is True

    def test_neuroscience_evidence_in_description_kept(self) -> None:
        ds = _dataset(
            title="Data from an ecological field study",
            description="Includes EEG and fMRI recordings of the auditory cortex.",
        )
        assert post_filter(ds, _meg_filters()) is True

    def test_declared_meg_kept(self) -> None:
        ds = _dataset(title="Anything at all", modality=["MEG"])
        assert post_filter(ds, _meg_filters()) is True

    def test_declared_non_matching_modality_filtered(self) -> None:
        ds = _dataset(title="Anything at all", modality=["mri"])
        assert post_filter(ds, _meg_filters()) is False

    def test_declared_synonym_modality_kept(self) -> None:
        # magnetoencephalography is a full-family synonym of MEG
        ds = _dataset(title="Anything at all", modality=["magnetoencephalography"])
        assert post_filter(ds, _meg_filters()) is True

    def test_no_modality_requested_keeps_everything(self) -> None:
        filters = QueryFilters(raw_query="neuroscience datasets")
        ds = _dataset(title="Soil and vegetation data from a field site")
        assert post_filter(ds, filters) is True


class TestSpeciesFilterUnchanged:
    def test_species_mismatch_filtered(self) -> None:
        filters = QueryFilters(raw_query="mouse datasets", species=["mouse"])
        ds = _dataset(title="Anything", species=["human"])
        assert post_filter(ds, filters) is False

    def test_species_match_kept(self) -> None:
        filters = QueryFilters(raw_query="human datasets", species=["human"])
        ds = _dataset(title="Anything", species=["Human"])
        assert post_filter(ds, filters) is True

    def test_modality_gate_and_species_gate_combine(self) -> None:
        filters = QueryFilters(raw_query="human MEG", modality=["MEG"], species=["human"])
        # No modality + no neuro evidence → filtered even though species matches
        ds = _dataset(title="Helicopter parenting", species=["human"])
        assert post_filter(ds, filters) is False
        # Declared MEG + species match → kept
        ds2 = _dataset(title="Anything", modality=["MEG"], species=["human"])
        assert post_filter(ds2, filters) is True
