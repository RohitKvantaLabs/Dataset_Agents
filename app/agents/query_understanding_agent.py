import logging

from app.config import get_settings
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
                # Mistral-7B-Instruct: optimised for tight structured-JSON
                # output with minimal latency — ideal for Phase-1 parsing.
                settings = get_settings()
                self._llm = LLMClient(model=settings.HF_QUERY_MODEL)
                self._provider = "huggingface"
            except ValueError:
                self._llm = None

    @property
    def provider(self) -> str:
        return self._provider

    def parse(self, raw_query: str) -> QueryFilters:
        raw_query = raw_query.strip()
        if self._llm is None:
            return self._heuristic_parse(raw_query)

        try:
            parsed = self._llm.generate_json(
                system_prompt=QUERY_UNDERSTANDING_SYSTEM_PROMPT,
                user_prompt=query_understanding_user_prompt(raw_query),
                max_tokens=500,
            )
            return QueryFilters(raw_query=raw_query, **parsed)
        except (LLMJSONParseError, TypeError, ValueError) as exc:
            logger.warning("Query parsing fell back to heuristic mode: %s", exc)
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
        if any(term in text for term in ["kid", "child", "children", "pediatric", "under 12"]):
            age_range = "pediatric"
        elif any(term in text for term in ["adult", "adults"]):
            age_range = "adult"

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

        return QueryFilters(
            raw_query=raw_query,
            modality=modalities,
            species=[species] if species else [],
            age_range=age_range,
            condition=condition,
            task=task,
            format=formats,
            is_bids_compliant=True if "bids" in formats else None,
        )
