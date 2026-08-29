"""
Tests for QueryUnderstandingAgent.

Covers:
- JSON-parse failure fallback path: LLMClient raises LLMJSONParseError →
  agent returns a degraded QueryFilters from heuristic parse, never a 500.
- Network failure fallback path: LLMClient raises GroqAPIError →
  agent returns keyword-only QueryFilters, never a 500.
- Heuristic parser correctness for key field mappings (modality, species,
  age_range, condition, task).
- LLM happy path: valid JSON from LLM → correct QueryFilters.
"""
import pytest

from unittest.mock import MagicMock, patch

from groq import APIError as GroqAPIError

from app.agents.query_understanding_agent import QueryUnderstandingAgent
from app.llm.client import LLMClient, LLMJSONParseError
from app.models.query_filters import QueryFilters


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_agent_with_mock_llm(generate_json_side_effect=None, generate_json_return=None) -> tuple[QueryUnderstandingAgent, MagicMock]:
    """Return (agent, mock_llm) with generate_json pre-configured."""
    mock_llm = MagicMock(spec=LLMClient)
    # parse() logs self._llm._model at debug level; spec'd MagicMock rejects
    # private attributes unless explicitly set (pre-existing suite failure).
    mock_llm._model = "mock-model"
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
        """When LLMClient init fails, agent still works via heuristic."""
        with patch("app.agents.query_understanding_agent.LLMClient",
                   side_effect=Exception("init failed")):
            agent = QueryUnderstandingAgent()
        result = agent.parse("fMRI human adult")
        assert isinstance(result, QueryFilters)
        assert "fmri" in result.modality


# ---------------------------------------------------------------------------
# Network failure: the critical graceful-degradation path
# ---------------------------------------------------------------------------

class TestNetworkFailureFallback:
    """
    When the LLM endpoint is unreachable (network error, timeout, HTTP error),
    parse() must return heuristic QueryFilters, not propagate the exception
    as a 500.
    """

    def test_groq_api_error_falls_back_to_heuristic(self) -> None:
        """GroqAPIError (base class for all Groq API errors) → heuristic fallback."""
        import httpx as _httpx
        mock_request = _httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
        agent, _ = _make_agent_with_mock_llm(
            generate_json_side_effect=GroqAPIError("API connection failed", request=mock_request, body=None)
        )
        result = agent.parse("resting state fMRI ADHD")

        assert isinstance(result, QueryFilters)
        assert result.raw_query == "resting state fMRI ADHD"
        # Heuristic fallback path: modality and condition recovered
        assert "fmri" in result.modality
        assert "adhd" in result.condition
        assert result.task == "resting-state"

    def test_groq_api_error_recovers_alzheimer_condition(self) -> None:
        """Query 'Alzheimer's disease longitudinal neuroimaging' recovers 'alzheimer' condition on GroqAPIError."""
        import httpx as _httpx
        mock_request = _httpx.Request("POST", "https://api.groq.com/openai/v1/chat/completions")
        agent, _ = _make_agent_with_mock_llm(
            generate_json_side_effect=GroqAPIError("JSON validation failed", request=mock_request, body=None)
        )
        result = agent.parse("Alzheimer's disease longitudinal neuroimaging")

        assert isinstance(result, QueryFilters)
        assert "alzheimer" in result.condition

    def test_unexpected_exception_still_propagates(self) -> None:
        """A non-network, non-parse exception must NOT be swallowed — it should 500."""
        agent, _ = _make_agent_with_mock_llm(
            generate_json_side_effect=RuntimeError("unexpected internal error")
        )
        with pytest.raises(RuntimeError, match="unexpected internal error"):
            agent.parse("any query")


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

    def test_age_range_child_canonical(self) -> None:
        """All child-equivalent forms collapse onto the canonical label "child"
        (mirrors AGE_TERMS), never a separate token like "pediatric"."""
        for query in ["fMRI in kids", "fMRI in children", "fMRI in child patients",
                      "pediatric fMRI dataset"]:
            result = self._parse(query)
            assert result.age_range == "child", f"{query!r} → {result.age_range}"

    def test_age_range_adolescent(self) -> None:
        """Adolescent forms stay distinct from child — never collapsed together."""
        for query in ["fMRI in adolescents", "fMRI in teenagers", "youth EEG dataset"]:
            result = self._parse(query)
            assert result.age_range == "adolescent", f"{query!r} → {result.age_range}"

    def test_age_range_adult_plural(self) -> None:
        result = self._parse("fMRI in adults")
        assert result.age_range == "adult"

    def test_age_range_infant(self) -> None:
        result = self._parse("EEG in newborns")
        assert result.age_range == "infant"

    def test_age_range_child_takes_precedence_over_adult(self) -> None:
        """Existing branch precedence preserved: child terms win over adult."""
        result = self._parse("children and adults fMRI")
        assert result.age_range == "child"

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
            "age_range": "child",
            "condition": ["ADHD"],
            "task": "resting-state",
            "format": [],
            "keywords": [],
        }
        agent, mock_llm = _make_agent_with_mock_llm(generate_json_return=llm_output)
        result = agent.parse("resting state fMRI ADHD kids")

        assert result.modality == ["fMRI"]
        assert result.species == ["human"]
        assert result.age_range == "child"
        assert result.condition == ["ADHD"]
        assert result.task == "resting-state"
        assert result.raw_query == "resting state fMRI ADHD kids"
        mock_llm.generate_json.assert_called_once()


# ---------------------------------------------------------------------------
# Canonical age vocabulary contract (FR: query intent ↔ filter vocabulary)
# ---------------------------------------------------------------------------

class TestCanonicalAgeVocabularyContract:
    """
    The prompt must emit the dataset-metadata age labels (AGE_TERMS), not a
    parallel vocabulary. Regression guard for the false-conflict bug where the
    parser emitted "pediatric" while facets expose "Child".
    """

    def test_prompt_uses_canonical_age_labels(self) -> None:
        from app.llm.prompts import QUERY_UNDERSTANDING_SYSTEM_PROMPT

        assert '"child"' in QUERY_UNDERSTANDING_SYSTEM_PROMPT
        assert '"adolescent"' in QUERY_UNDERSTANDING_SYSTEM_PROMPT
        assert '"adult"' in QUERY_UNDERSTANDING_SYSTEM_PROMPT

    def test_prompt_no_longer_mandates_pediatric_as_canonical(self) -> None:
        from app.llm.prompts import QUERY_UNDERSTANDING_SYSTEM_PROMPT

        # "pediatric" may appear only as an input synonym to be mapped onto
        # "child", never as an emitted canonical value.
        assert '→"pediatric"' not in QUERY_UNDERSTANDING_SYSTEM_PROMPT
        assert '"pediatric" (age < 18)' not in QUERY_UNDERSTANDING_SYSTEM_PROMPT
