"""
Stage 3 enrichment — populated controlled vocabularies (Issue 3).

The approved vocabularies (app/data/vocab.py, populated 2026-08-04) drive
Stage 3's deterministic word-boundary matching: repository candidates that
declare ``modality = []`` (etc.) get their structured fields filled from
title/description/keywords. No LLM, no new normalization system — the
existing enrichment pipeline is reused.
"""
import pytest

from app.config import Settings
from app.ingestion.quality_pipeline import _Record, _stage_enrich
from app.models.repository_dataset import RepositoryDataset

from app.data.vocab import (
    AGE_TERMS,
    DISEASE_TERMS,
    MODALITY_VOCAB,
    REGION_TERMS,
    SPECIES_VOCAB,
)


def _settings(**overrides) -> Settings:
    base = dict(
        INTERNAL_API_SECRET="test-secret",
        CRON_SECRET="test-cron-secret",
        GROQ_API_KEY="gsk_test",
        TAVILY_API_KEY="tvly-test",
        MONGO_URI="mongodb://localhost:27017",
        REDIS_URL="redis://localhost:6379/0",
    )
    base.update(overrides)
    return Settings(**base)


def _dataset(title: str, description: str | None = None, **overrides) -> RepositoryDataset:
    base = dict(
        source="zenodo",
        source_id="z1",
        url="https://zenodo.org/records/1",
        title=title,
        description=description,
        modality=[],
        species=[],
        keywords=[],
    )
    base.update(overrides)
    return RepositoryDataset(**base)


@pytest.mark.asyncio
async def test_enrich_fills_modality_meg_from_full_name() -> None:
    rec = _Record(candidate=_dataset(
        "Neuromagnetic recordings of motor cortex",
        description="Magnetoencephalography data collected at 300 Hz.",
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    assert kept[0].candidate.modality == ["meg"]


@pytest.mark.asyncio
async def test_enrich_fills_modality_eeg_from_title() -> None:
    rec = _Record(candidate=_dataset("EEG motor imagery BCI dataset"))
    kept, _ = await _stage_enrich([rec], _settings())
    assert "eeg" in kept[0].candidate.modality


@pytest.mark.asyncio
async def test_enrich_fills_disease_and_region_and_species() -> None:
    rec = _Record(candidate=_dataset(
        "Resting-state recordings in Parkinson disease",
        description="Magnetoencephalography from hippocampus regions of human participants.",
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    c = kept[0].candidate
    assert c.disease == "parkinson"
    assert c.region == "hippocampus"
    assert "human" in c.species
    # enrichment_sources is tracked on the _Record carrier, not the candidate
    assert "vocab_modality" in kept[0].enrichment_sources
    assert "vocab_region" in kept[0].enrichment_sources
    assert "vocab_disease" in kept[0].enrichment_sources
    assert "vocab_species" in kept[0].enrichment_sources


@pytest.mark.asyncio
async def test_enrich_fills_age_group() -> None:
    rec = _Record(candidate=_dataset("Pediatric resting-state fMRI"))
    kept, _ = await _stage_enrich([rec], _settings())
    assert kept[0].candidate.age_group == "child"


@pytest.mark.asyncio
async def test_enrich_does_not_overwrite_declared_modality() -> None:
    rec = _Record(candidate=_dataset("fMRI study", modality=["mri"]))
    kept, _ = await _stage_enrich([rec], _settings())
    # Declared values win — Stage 3 fills only empty fields (existing contract).
    assert kept[0].candidate.modality == ["mri"]


@pytest.mark.asyncio
async def test_enrich_leaves_empty_when_no_vocab_token() -> None:
    rec = _Record(candidate=_dataset("Soil and vegetation data from a field site"))
    kept, _ = await _stage_enrich([rec], _settings())
    assert kept[0].candidate.modality == []
    assert kept[0].candidate.disease is None
    assert kept[0].candidate.species == []


class TestVocabData:
    def test_modality_vocab_populated(self) -> None:
        assert "meg" in MODALITY_VOCAB
        assert "eeg" in MODALITY_VOCAB
        assert "mri" in MODALITY_VOCAB
        assert "fmri" in MODALITY_VOCAB
        assert "pet" in MODALITY_VOCAB
        # labels must be lowercase strings
        assert all(isinstance(k, str) and k.islower() for k in MODALITY_VOCAB)

    def test_disease_and_region_vocab_populated(self) -> None:
        assert any("parkinson" in k for k in DISEASE_TERMS)
        assert any("alzheimer" in k for k in DISEASE_TERMS)
        assert "hippocampus" in REGION_TERMS

    def test_species_and_age_vocab_populated(self) -> None:
        assert "human" in SPECIES_VOCAB
        assert "adult" in AGE_TERMS
