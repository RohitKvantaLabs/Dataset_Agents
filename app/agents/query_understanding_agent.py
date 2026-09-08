import logging

from groq import APIError as GroqAPIError

from app.config import get_settings
from app.core.circuit_breaker import CircuitBreakerOpenError
from app.llm.client import LLMClient, LLMJSONParseError
from app.llm.prompts import QUERY_UNDERSTANDING_SYSTEM_PROMPT, query_understanding_user_prompt
from app.models.query_filters import QueryFilters

logger = logging.getLogger("neuro_platform.agents.query_understanding")


class QueryUnderstandingAgent:
    """
    Phase 1 synchronous agent. It parses natural language for Node/MERN and
    does not retrieve data, scrape the web, or launch fallback workflows.
    """

    def __init__(self, llm_client: LLMClient | None = None):
        self._provider = "heuristic"
        self._llm = llm_client
        self._last_usage: dict | None = None
        self._last_model_used: str | None = None
        if self._llm is None:
            try:
                settings = get_settings()
                self._llm = LLMClient(model=settings.GROQ_QUERY_MODEL)
                self._provider = "groq"
            except Exception:
                self._llm = None

    @property
    def provider(self) -> str:
        return self._provider

    @property
    def last_usage(self) -> dict | None:
        return getattr(self, "_last_usage", None)

    @property
    def last_model_used(self) -> str | None:
        return getattr(self, "_last_model_used", None)

    def parse(self, raw_query: str) -> QueryFilters:
        raw_query = raw_query.strip()
        
        # Log the attempt to understand query
        logger.debug("QueryUnderstandingAgent parsing query: %r", raw_query)
        
        if self._llm is None:
            logger.warning("LLMClient not available, falling back to heuristic parsing")
            self._last_usage = None
            self._last_model_used = None
            return self._heuristic_parse(raw_query)

        try:
            # Log the model being used for parsing
            logger.debug("Using Groq model for query parsing: %s", self._llm._model)
            parsed, usage = self._llm.generate_json_with_usage(
                system_prompt=QUERY_UNDERSTANDING_SYSTEM_PROMPT,
                user_prompt=query_understanding_user_prompt(raw_query),
                max_tokens=500,
            )
            # Store usage for API layer to read without changing return type
            self._last_usage = usage
            self._last_model_used = usage.get("model") if usage else self._llm._model
            
            # Log successful parsing with extracted fields for debugging
            logger.debug("Query parsing successful. Extracted fields: %s", {
                "modality": parsed.get("modality", []),
                "condition": parsed.get("condition", []),
                "species": parsed.get("species", []),
                "region": parsed.get("region"),
                "task": parsed.get("task"),
                "format": parsed.get("format", []),
                "age_range": parsed.get("age_range"),
                "keywords": parsed.get("keywords", [])
            })
            
            has_signal = bool(
                parsed.get("modality") or parsed.get("condition") or parsed.get("task")
                or parsed.get("region") or parsed.get("species") or parsed.get("age_range")
                or parsed.get("format")
            )
            result = QueryFilters(raw_query=raw_query, in_domain=has_signal, **parsed)
            return result
        except (LLMJSONParseError, TypeError, ValueError) as exc:
            logger.error("Query parsing failed with LLM error: %s", exc)
            logger.error("QueryUnderstandingAgent falling back to heuristic parsing")
            self._last_usage = None
            self._last_model_used = None
            return self._heuristic_parse(raw_query)
        except (GroqAPIError, CircuitBreakerOpenError) as exc:
            logger.error("Groq API error during query parsing: %s (type: %s)", exc, type(exc).__name__)
            logger.error("QueryUnderstandingAgent falling back to heuristic parsing due to API failure")
            self._last_usage = None
            self._last_model_used = None
            return self._heuristic_parse(raw_query)

    def _heuristic_parse(self, raw_query: str) -> QueryFilters:
        text = raw_query.lower()
        modalities = [
            term
            for term in ["fmri", "eeg", "meg", "smri", "dti", "pet", "ieeg"]
            if term in text
        ]

        species = None
        for candidate in ["human", "mouse", "macaque", "rat"]:
            if candidate in text:
                species = candidate
                break

        age_range = None
        # Canonical labels mirror AGE_TERMS (app/data/vocab.py) — the same
        # vocabulary the ingestion pipeline writes into dataset age_group
        # metadata, so intent, facets and conflict detection share one concept
        # space ("children"/"pediatric" → "child", never a separate token).
        if any(term in text for term in ["kid", "child", "children", "pediatric", "under 12"]):
            age_range = "child"
        elif any(term in text for term in ["adolescent", "teenager", "teenagers", "youth"]):
            age_range = "adolescent"
        elif any(term in text for term in ["adult", "adults"]):
            age_range = "adult"
        elif any(term in text for term in ["elderly", "geriatric", "older adults"]):
            age_range = "elderly"
        elif any(term in text for term in ["infant", "infants", "newborn", "newborns",
                                           "neonatal", "neonates"]):
            age_range = "infant"

        region = None
        for candidate in ["hippocampus", "amygdala", "cerebellum", "thalamus", "striatum",
                          "prefrontal cortex", "motor cortex", "visual cortex", "brainstem",
                          "basal ganglia", "insula", "caudate", "putamen", "cingulate",
                          "corpus callosum", "hypothalamus"]:
            if candidate in text:
                region = candidate
                break

        condition = [
            label
            for label in ["adhd", "alzheimer", "dementia", "autism", "parkinson", "depression"]
            if label in text
        ]
        formats = [fmt for fmt in ["bids", "nifti", "dicom"] if fmt in text]

        task = None
        if "resting" in text or "resting-state" in text:
            task = "resting-state"
        elif "working memory" in text:
            task = "working memory"

        has_signal = bool(modalities or condition or task or region or species or age_range or formats)
        return QueryFilters(
            raw_query=raw_query,
            modality=modalities,
            species=[species] if species else [],
            age_range=age_range,
            region=region,
            condition=condition,
            task=task,
            format=formats,
            in_domain=has_signal,
        )
