"""
Regression test for functional nuclear magnetic resonance modality mapping.
"""

from app.connectors.base import MODALITY_SYNONYMS
from app.data.vocab import MODALITY_VOCAB


def test_functional_nuclear_magnetic_resonance_in_vocab() -> None:
    assert "functional nuclear magnetic resonance" in MODALITY_VOCAB["mri"]
    assert "functional nuclear magnetic resonance imaging" in MODALITY_VOCAB["mri"]


def test_functional_nuclear_magnetic_resonance_in_synonyms() -> None:
    assert "functional nuclear magnetic resonance" in MODALITY_SYNONYMS["mri"]
    assert "functional nuclear magnetic resonance imaging" in MODALITY_SYNONYMS["mri"]
