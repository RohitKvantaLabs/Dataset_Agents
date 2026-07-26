from functools import lru_cache

from pydantic import field_validator
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

    # --- Groq API key (required) ---
    GROQ_API_KEY: str

    # Model for Phase-1 Query Understanding Agent (structured JSON extraction
    # from natural language). Llama 3.1 8B Instant is fast and reliable for
    # structured JSON output via Groq's JSON mode.
    GROQ_QUERY_MODEL: str = "llama-3.1-8b-instant"

    # Model for the Fallback / Discovery Agent (open-ended reasoning, URL
    # candidate generation). Llama 3.3 70B Versatile excels at long-context
    # reasoning and multi-step generation.
    GROQ_FALLBACK_MODEL: str = "llama-3.3-70b-versatile"

    # Retained for the embedder (optional — if absent, embedding is skipped).
    HF_TOKEN: str | None = None
    HF_EMBEDDING_MODEL: str = "sentence-transformers/all-MiniLM-L6-v2"

    HTTP_CHECK_TIMEOUT_SECONDS: int = 8
    REQUEST_TIMEOUT_SECONDS: int = 20

    INTERNAL_API_SECRET: str | None = None
    CRON_SECRET: str | None = None

    # Search provider — Tavily (https://tavily.com). Required at startup because
    # fallback-search is expected to use real web search before verification.
    TAVILY_API_KEY: str

    # Fallback pipeline: max dataset candidates to surface per query.
    MAX_FALLBACK_CANDIDATES: int = 10

    # Comma-separated list of allowed CORS origins.
    # Browsers reject allow_origins=["*"] with allow_credentials=True,
    # so this must always be an explicit list in production.
    ALLOWED_ORIGINS: list[str] = ["https://neuro-frontend-two.vercel.app", "https://neuro-server.vercel.app"]

    @field_validator("ALLOWED_ORIGINS", mode="before")
    @classmethod
    def split_origins(cls, v):
        """Accept both JSON array (pydantic-settings default) and comma-sep string."""
        if isinstance(v, str):
            return [o.strip() for o in v.split(",") if o.strip()]
        return v

    # Scheduled link re-verification controls.
    CRON_STALE_THRESHOLD_DAYS: int = 30
    CRON_BATCH_SIZE: int = 100


@lru_cache
def get_settings() -> Settings:
    return Settings()
