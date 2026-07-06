"""
Tests for QueryUnderstandingAgent.

Covers (per CLAUDE.md checklist):
- JSON-parse failure fallback path: LLMClient raises LLMJSONParseError →
  agent returns a degraded QueryFilters from heuristic parse, never a 500.
- Heuristic parser correctness for key field mappings (modality, species,
  age_range, condition, task).
- LLM happy path: valid JSON from LLM → correct QueryFilters.
"""
import pytest

from unittest.mock import MagicMock, patch

from app.agents.query_understanding_agent import QueryUnderstandingAgent
from app.llm.client import LLMClient, LLMJSONParseError
from app.models.query_filters import QueryFilters


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_agent_with_mock_llm(generate_json_side_effect=None, generate_json_return=None) -> tuple[QueryUnderstandingAgent, MagicMock]:
    """Return (agent, mock_llm) with generate_json pre-configured."""
    mock_llm = MagicMock(spec=LLMClient)
    if generate_json_side_effect is not None:
        mock_llm.generate_json.side_effect = generate_json_side_effect
    else:
        mock_llm.generate_json.return_value = generate_json_return
    agent = QueryUnderstandingAgent(llm_client=mock_llm)
    return agent, mock_llm


# ---------------------------------------------------------------------------
# JSON-parse failure: the critical fallback path
# ---------------------------------------------------------------------------

class TestJsonParseFailureFallback:
    """When LLM returns garbage, the agent must degrade gracefully, not 500."""

    def test_llm_json_parse_error_falls_back_to_heuristic(self) -> None:
        """LLMJSONParseError → heuristic parse, never raises."""
        agent, mock_llm = _make_agent_with_mock_llm(
            generate_json_side_effect=LLMJSONParseError("bad json")
        )
        result = agent.parse("resting state fMRI in kids with ADHD")

        # Must return a QueryFilters, not raise
        assert isinstance(result, QueryFilters)
        # Heuristic parser should have caught fmri
        assert "fmri" in result.modality
        # raw_query must be preserved
        assert result.raw_query == "resting state fMRI in kids with ADHD"

    def test_llm_type_error_falls_back_to_heuristic(self) -> None:
        """TypeError (e.g. LLM returns wrong shape) also triggers fallback."""
        agent, _ = _make_agent_with_mock_llm(
            generate_json_side_effect=TypeError("not a dict")
        )
        result = agent.parse("EEG mouse resting-state")
        assert isinstance(result, QueryFilters)
        assert "eeg" in result.modality

    def test_fallback_populates_keywords_from_query(self) -> None:
        """Heuristic fallback never silently returns an empty QueryFilters."""
        agent, _ = _make_agent_with_mock_llm(
            generate_json_side_effect=LLMJSONParseError("unparseable")
        )
        # A query with no recognised modality/species terms still has raw_query
        result = agent.parse("some obscure query with no known terms")
        assert result.raw_query == "some obscure query with no known terms"

    def test_no_llm_client_uses_heuristic_only(self) -> None:
        """When HF_TOKEN is absent and LLMClient raises ValueError, agent
        still works via heuristic — this is the no-key-local-dev path."""
        with patch("app.agents.query_understanding_agent.LLMClient",
                   side_effect=ValueError("no token")):
            agent = QueryUnderstandingAgent()
        result = agent.parse("fMRI human adult")
        assert isinstance(result, QueryFilters)
        assert "fmri" in result.modality


# ---------------------------------------------------------------------------
# Heuristic parser field correctness
# ---------------------------------------------------------------------------

class TestHeuristicParser:

    def _parse(self, text: str) -> QueryFilters:
        """Parse via heuristic mode (no LLM)."""
        agent = QueryUnderstandingAgent(llm_client=None)
        return agent._heuristic_parse(text)

    def test_modality_fmri(self) -> None:
        result = self._parse("resting state fmri data")
        assert "fmri" in result.modality

    def test_modality_eeg(self) -> None:
        result = self._parse("EEG recordings during sleep")
        assert "eeg" in result.modality

    def test_species_human(self) -> None:
        result = self._parse("human fMRI dataset")
        assert result.species == ["human"]

    def test_species_mouse(self) -> None:
        result = self._parse("mouse electrophysiology")
        assert result.species == ["mouse"]

    def test_species_none(self) -> None:
        result = self._parse("fMRI dataset")
        assert result.species == []

    def test_age_range_pediatric(self) -> None:
        result = self._parse("fMRI in kids")
        assert result.age_range == "pediatric"

    def test_age_range_adult(self) -> None:
        result = self._parse("adult healthy controls")
        assert result.age_range == "adult"

    def test_condition_adhd(self) -> None:
        result = self._parse("ADHD fMRI resting state")
        assert "adhd" in result.condition

    def test_task_resting_state(self) -> None:
        result = self._parse("resting state fmri")
        assert result.task == "resting-state"

    def test_multiple_modalities(self) -> None:
        result = self._parse("fmri and eeg combined dataset")
        assert "fmri" in result.modality
        assert "eeg" in result.modality


# ---------------------------------------------------------------------------
# LLM happy path
# ---------------------------------------------------------------------------

class TestLLMHappyPath:

    def test_valid_llm_output_returns_correct_filters(self) -> None:
        llm_output = {
            "modality": ["fMRI"],
            "species": ["human"],
            "age_range": "pediatric",
            "condition": ["ADHD"],
            "task": "resting-state",
            "format": [],
            "keywords": [],
        }
        agent, mock_llm = _make_agent_with_mock_llm(generate_json_return=llm_output)
        result = agent.parse("resting state fMRI ADHD kids")

        assert result.modality == ["fMRI"]
        assert result.species == ["human"]
        assert result.age_range == "pediatric"
        assert result.condition == ["ADHD"]
        assert result.task == "resting-state"
        assert result.raw_query == "resting state fMRI ADHD kids"
        mock_llm.generate_json.assert_called_once()
