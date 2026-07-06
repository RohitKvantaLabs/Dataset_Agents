"""
Embedder — text → vector embedding for Atlas Vector Search.

Produces a float32 embedding vector for a Dataset's searchable text
using the Hugging Face Inference API (feature-extraction task).

Architecture notes
------------------
- The embedding model is called via the HF InferenceClient, so no local
  model weights are downloaded. This keeps the Vercel bundle minimal.
- If HF_TOKEN is absent the embedder returns None, and the pipeline will
  skip the vector field. Text-search still works without it.
- Embeddings are stored in the ``embedding`` field of the Mongo document.
  The Atlas Vector Search index should be configured to target this field.
"""
import logging
from typing import Optional

from app.config import get_settings

logger = logging.getLogger("neuro_platform.ingestion.embedder")

# Default model — a lightweight sentence-transformer hosted on HF Hub.
# Override via HF_EMBEDDING_MODEL in config if needed.
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


class Embedder:
    """
    Wraps the HF feature-extraction inference endpoint.

    Returns a list[float] embedding vector or None if the call fails or
    the token is not configured.
    """

    def __init__(self):
        settings = get_settings()
        self._token: Optional[str] = settings.HF_TOKEN
        self._model: str = getattr(settings, "HF_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL)
        self._client = None

        if self._token:
            try:
                from huggingface_hub import InferenceClient  # lazy import

                self._client = InferenceClient(model=self._model, token=self._token)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Could not initialise HF InferenceClient for embeddings: %s", exc)

    def embed(self, text: str) -> Optional[list[float]]:
        """
        Generate an embedding vector for *text*.

        Returns None on any failure so callers can skip gracefully.
        """
        if not self._client:
            return None

        text = text.strip()
        if not text:
            return None

        try:
            result = self._client.feature_extraction(text, model=self._model)
            # result shape can be (1, seq_len, hidden) or (hidden,);
            # we want a flat 1-D vector representing the whole input.
            if hasattr(result, "tolist"):
                flat = result.tolist()
            else:
                flat = result  # already a list

            # If 2-D (seq_len × hidden), take the CLS / first token vector.
            if flat and isinstance(flat[0], list):
                flat = flat[0]
            return flat  # type: ignore[return-value]

        except Exception as exc:  # noqa: BLE001
            logger.warning("Embedding call failed for text=%r: %s", text[:80], exc)
            return None

    def embed_dataset_text(self, title: str, description: str) -> Optional[list[float]]:
        """Convenience wrapper that concatenates title + description."""
        combined = f"{title}. {description}".strip(" .")
        return self.embed(combined)
