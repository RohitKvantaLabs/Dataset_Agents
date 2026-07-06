from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    APP_NAME: str = "Neuro Data Discovery - Python Services"
    ENV: str = "development"
    API_V1_PREFIX: str = "/api/v1"

    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB_NAME: str = "neuro_data_platform"
    MONGO_DATASET_COLLECTION: str = "datasets"

    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_RESULT_CHANNEL_PREFIX: str = "fallback-result:"  # + query_id

    # --- Hugging Face API token (shared across all HF Inference endpoints) ---
    # Required at startup. Query parsing has a heuristic fallback for LLM
    # response failures, but this service is not configured to run without HF.
    HF_TOKEN: str

    # Model for Phase-1 Query Understanding Agent (structured JSON extraction
    # from natural language). Mistral-7B-Instruct is instruction-tuned for
    # tight JSON output and has low latency on the free HF Inference tier.
    HF_QUERY_MODEL: str = "mistralai/Mistral-7B-Instruct-v0.3"

    # Model for the Fallback / Discovery Agent (open-ended reasoning, URL
    # candidate generation). Qwen2.5-7B-Instruct excels at long-context
    # reasoning and multi-step tool-like generation.
    HF_FALLBACK_MODEL: str = "Qwen/Qwen2.5-7B-Instruct"

    # Model for vector embeddings (feature-extraction). all-MiniLM-L6-v2
    # produces 384-dim sentence vectors; fast, lightweight, and free-tier.
    HF_EMBEDDING_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"

    # Legacy alias kept for any caller that hasn't been updated yet.
    # Points to the query-understanding model so old code stays functional.
    @property
    def HF_LLM_MODEL(self) -> str:  # noqa: N802
        return self.HF_QUERY_MODEL

    HTTP_CHECK_TIMEOUT_SECONDS: int = 8
    REQUEST_TIMEOUT_SECONDS: int = 20

    INTERNAL_API_SECRET: str | None = None
    CRON_SECRET: str | None = None

    # Search provider — Tavily (https://tavily.com). Required at startup because
    # fallback-search is expected to use real web search before verification.
    TAVILY_API_KEY: str

    # Fallback pipeline: max dataset candidates to surface per query.
    MAX_FALLBACK_CANDIDATES: int = 10

    # Scheduled link re-verification controls.
    CRON_STALE_THRESHOLD_DAYS: int = 30
    CRON_BATCH_SIZE: int = 100


@lru_cache
def get_settings() -> Settings:
    return Settings()
