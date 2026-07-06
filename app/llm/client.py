"""
Thin wrapper around the LLM provider. Everything else in the codebase
calls `LLMClient`, never huggingface_hub directly - so switching to a
paid provider later (Anthropic/OpenAI) means rewriting this one file,
not every agent that uses an LLM.
"""
import json
import logging

from huggingface_hub import InferenceClient
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import get_settings

logger = logging.getLogger("neuro_platform.llm")


class LLMJSONParseError(Exception):
    """Raised when the model doesn't return valid JSON after retries."""


class LLMClient:
    def __init__(self, model: str | None = None) -> None:
        settings = get_settings()
        if not settings.HF_TOKEN:
            raise ValueError(
                "HF_TOKEN is not set — LLMClient requires a Hugging Face API token. "
                "Set HF_TOKEN in your environment or .env file."
            )
        # If no model is specified, default to the query-understanding model.
        # Callers that need a different model (e.g. FallbackAgent) pass it
        # explicitly so the right endpoint is used without touching settings.
        self._model = model or settings.HF_QUERY_MODEL
        self._client = InferenceClient(model=self._model, token=settings.HF_TOKEN)
        logger.debug("LLMClient initialised with model=%s", self._model)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True)
    def _chat(self, system_prompt: str, user_prompt: str, max_tokens: int = 512) -> str:
        response = self._client.chat_completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=max_tokens,
            temperature=0.1,  # low temp: this pipeline needs consistency, not creativity
        )
        return response.choices[0].message.content

    def generate_json(self, system_prompt: str, user_prompt: str, max_tokens: int = 512) -> dict:
        """
        Free-tier HF models don't reliably support native function-calling,
        so we enforce structure via prompting + strict parsing rather than
        a tool-call API. If/when you move to a paid provider with real
        structured-output support, swap the internals here - callers don't
        need to change.
        """
        raw = self._chat(system_prompt, user_prompt, max_tokens=max_tokens)
        cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as exc:
            logger.error("LLM returned non-JSON output: %s", raw[:500])
            raise LLMJSONParseError(f"Could not parse LLM output as JSON: {exc}") from exc