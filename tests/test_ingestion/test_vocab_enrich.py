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


# ── Metadata Integrity: context-aware extraction (Issue 2) ──────────────────


@pytest.mark.asyncio
async def test_enrich_ignores_incidental_portal_mentions() -> None:
    # ds005892-style pollution (generic, no hardcoding): the title declares MRI;
    # the description mentions MEG/EEG/iEEG ONLY inside the NEMAR viewing-portal
    # blurb. The dataset must remain MRI.
    rec = _Record(candidate=_dataset(
        "Resting State MRI data from healthy control and Parkinson's disease cohorts",
        description=(
            "Resting-state fMRI data were collected at multiple sites. "
            "View this dataset on the NEMAR OpenNeuro portal for MEG, iEEG, and EEG data."
        ),
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    assert kept[0].candidate.modality == ["mri"]
    assert kept[0].candidate.disease == "parkinson"


@pytest.mark.asyncio
async def test_enrich_ignores_software_and_viewer_mentions() -> None:
    rec = _Record(candidate=_dataset(
        "Clinical scores from a longitudinal cohort",
        description=(
            "The software supports MEG visualization and EEG analysis tools; "
            "data are compatible with the NEMAR portal for viewing MEG signals."
        ),
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    # "Supports MEG" / "MEG visualization" / "EEG analysis tools" / "portal for
    # viewing MEG signals" describe tooling — never dataset modality.
    assert kept[0].candidate.modality == []


@pytest.mark.asyncio
async def test_enrich_accepts_acquisition_context_in_description() -> None:
    rec = _Record(candidate=_dataset(
        "Auditory oddball paradigm",
        description="MEG recordings were acquired at 1000 Hz from 30 subjects.",
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    assert kept[0].candidate.modality == ["meg"]


@pytest.mark.asyncio
async def test_enrich_keeps_multiple_labels_within_one_source() -> None:
    rec = _Record(candidate=_dataset("MEG and EEG recordings of resting state"))
    kept, _ = await _stage_enrich([rec], _settings())
    assert set(kept[0].candidate.modality) == {"meg", "eeg"}


@pytest.mark.asyncio
async def test_enrich_accepts_analysis_and_library_phrasing() -> None:
    # Ambiguous tooling words (analysis/library) are NOT signals — legitimate
    # acquisition phrasing must not be false-rejected.
    rec = _Record(candidate=_dataset(
        "Error-related negativity study",
        description="Single-trial EEG analysis of error-related negativity.",
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    assert "eeg" in kept[0].candidate.modality

    rec2 = _Record(candidate=_dataset(
        "Resting-state neuromagnetic recordings",
        description="This dataset is part of an open library of MEG recordings.",
    ))
    kept2, _ = await _stage_enrich([rec2], _settings())
    assert "meg" in kept2[0].candidate.modality


# ── Metadata Integrity: confidence hierarchy (Issues 1/3/4) ─────────────────


@pytest.mark.asyncio
async def test_enrich_title_evidence_beats_description() -> None:
    rec = _Record(candidate=_dataset(
        "MEG recordings during motor imagery",
        description="EEG and fMRI data from the same participants.",
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    # title (90) fills the field; lower-confidence description (40) evidence is
    # never added.
    assert kept[0].candidate.modality == ["meg"]
    assert kept[0].enrichment["modality_source"] == "title"


@pytest.mark.asyncio
async def test_enrich_keywords_beat_description() -> None:
    rec = _Record(candidate=_dataset(
        "Multi-site resting-state study",
        keywords=["magnetoencephalography"],
        description="EEG recordings from patients.",
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    # keywords (85) > description (40)
    assert kept[0].candidate.modality == ["meg"]
    assert kept[0].enrichment["modality_source"] == "keywords"


@pytest.mark.asyncio
async def test_enrich_json_metadata_beats_free_text() -> None:
    rec = _Record(candidate=_dataset(
        "MEG study of motor cortex",
        raw={"metadata": {"modalities": ["mri"]}},
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    # repository JSON metadata (95) outranks the title (90)
    assert kept[0].candidate.modality == ["mri"]
    assert kept[0].enrichment["modality_source"] == "json"


@pytest.mark.asyncio
async def test_enrich_json_species_fallback() -> None:
    rec = _Record(candidate=_dataset(
        "Resting-state recordings",
        raw={"metadata": {"species": ["Human"]}},
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    assert "human" in kept[0].candidate.species
    assert kept[0].enrichment["species_source"] == "json"


@pytest.mark.asyncio
async def test_enrich_never_overrides_declared_fields() -> None:
    rec = _Record(candidate=_dataset(
        "fMRI and MEG in human volunteers",
        description="EEG data and PET imaging, compatible with the NEMAR portal.",
        modality=["mri"],
        species=["mouse"],
        disease="parkinson",
        region="hippocampus",
    ))
    kept, _ = await _stage_enrich([rec], _settings())
    c = kept[0].candidate
    # Repository-declared values (confidence 100) are never overwritten.
    assert c.modality == ["mri"]
    assert c.species == ["mouse"]
    assert c.disease == "parkinson"
    assert c.region == "hippocampus"
    assert "vocab_modality" not in kept[0].enrichment_sources


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
