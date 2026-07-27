"""
Thin wrapper around the LLM provider. Everything else in the codebase
calls `LLMClient`, never the Groq SDK directly - so switching providers
later means rewriting this one file, not every agent that uses an LLM.
"""
import json
import logging

from groq import Groq
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import get_settings
from app.core.circuit_breaker import CircuitBreaker

logger = logging.getLogger("neuro_platform.llm")

# Module-level circuit breaker for LLM calls.
# 5 failures within 30s → open circuit for 30s of fast-fail.
_llm_circuit_breaker = CircuitBreaker(
    name="groq-llm",
    failure_threshold=5,
    recovery_timeout=30.0,
)


class LLMJSONParseError(Exception):
    """Raised when the model doesn't return valid JSON after retries."""


class LLMClient:
    def __init__(self, model: str | None = None) -> None:
        settings = get_settings()
        self._model = model or settings.GROQ_QUERY_MODEL
        self._client = Groq(api_key=settings.GROQ_API_KEY)
        logger.debug("LLMClient initialised with model=%s", self._model)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8), reraise=True)
    def _chat(self, system_prompt: str, user_prompt: str, max_tokens: int = 512, response_format: dict | None = None) -> str:
        kwargs = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": 0.1,
        }
        if response_format:
            kwargs["response_format"] = response_format
        response = self._client.chat.completions.create(**kwargs)
        return response.choices[0].message.content

    def generate_json(self, system_prompt: str, user_prompt: str, max_tokens: int = 512) -> dict:
        """
        Uses Groq's JSON mode (response_format={"type": "json_object"}) to
        enforce structured output. The prompt must still instruct the model
        to produce JSON — Groq's JSON mode requires it.

        Circuit breaker wraps the ENTIRE call (including tenacity retries)
        so that a known-down LLM is fast-failed without exhausting retries.
        """
        with _llm_circuit_breaker:
            raw = self._chat(
                system_prompt,
                user_prompt,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
        cleaned = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as exc:
            logger.error("LLM returned non-JSON output: %s", raw[:500])
            raise LLMJSONParseError(f"Could not parse LLM output as JSON: {exc}") from exc
