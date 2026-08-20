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

    def parse(self, raw_query: str) -> QueryFilters:
        raw_query = raw_query.strip()
        
        # Log the attempt to understand query
        logger.debug("QueryUnderstandingAgent parsing query: %r", raw_query)
        
        if self._llm is None:
            logger.warning("LLMClient not available, falling back to heuristic parsing")
            return self._heuristic_parse(raw_query)

        try:
            # Log the model being used for parsing
            logger.debug("Using Groq model for query parsing: %s", self._llm._model)
            parsed = self._llm.generate_json(
                system_prompt=QUERY_UNDERSTANDING_SYSTEM_PROMPT,
                user_prompt=query_understanding_user_prompt(raw_query),
                max_tokens=500,
            )
            
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
            return self._heuristic_parse(raw_query)
        except (GroqAPIError, CircuitBreakerOpenError) as exc:
            logger.error("Groq API error during query parsing: %s (type: %s)", exc, type(exc).__name__)
            logger.error("QueryUnderstandingAgent falling back to keyword-only filters due to API failure")
            return QueryFilters(raw_query=raw_query, keywords=raw_query.split())

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
        if any(term in text for term in ["kid", "child", "children", "pediatric", "under 12"]):
            age_range = "pediatric"
        elif any(term in text for term in ["adult", "adults"]):
            age_range = "adult"

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
