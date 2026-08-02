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

    # ── Phase 2 — Repository Retrieval Architecture (§3.8 config, subset used by Phase 2) ──
    REPOSITORY_ENABLED_SOURCES: list[str] = [
        "openneuro", "dandi", "neurovault", "ebrains",
        "zenodo", "figshare", "dryad", "osf", "nitrc",
    ]
    REPO_RATE_LIMIT_PER_SOURCE: float = 2.0   # default req/s per connector (§2.3.1)
    REPO_MAX_PAGES: int = 5                    # pagination cap per connector (§2.3.1)
    REPO_SEARCH_LIMIT_PER_SOURCE: int = 10     # default records per source in repository-search

    # EBRAINS Knowledge Graph API key (bearer). Optional — when missing the
    # ebrains connector returns an offline status with a reason instead of
    # being skipped silently (§2.7).
    EBRAINS_API_KEY: str | None = None

    @field_validator("REPOSITORY_ENABLED_SOURCES", mode="before")
    @classmethod
    def split_enabled_sources(cls, v):
        """Accept both JSON array (pydantic-settings default) and comma-sep string."""
        if isinstance(v, str):
            return [s.strip().lower() for s in v.split(",") if s.strip()]
        return v

    # ── Phase 3 — Dataset Quality Pipeline (§3.8) ──
    # Stage 1 allow/block lists. Empty allowlist means "auto-derive" from
    # each enabled source's canonical host + KNOWN_REPOSITORY_DOMAINS (§3.1).
    REPOSITORY_ALLOWLIST: list[str] = []
    REPOSITORY_BLOCKLIST: list[str] = []

    # Stage 2 — classifier
    ALLOW_SOFTWARE: bool = False            # False → software candidates dropped

    # Stage 3 — metadata enrichment
    ENRICHMENT_ENABLED: bool = True
    CROSSREF_TIMEOUT_MS: int = 5000

    # Stage 4 — download verification
    MAX_CONCURRENT_CHECKS: int = 10         # semaphore, mirrors cron concurrency

    # Stage 5 — quality scoring
    SCORING_SCHEME: str = "legacy"          # "legacy" | "extended" (§3.5 optional)
    ENRICH_QUALITY_BONUS_CAP: float = 0.15  # extended scheme bonus cap (+0.04 × 6 fields)

    # Stage 6 — deduplication
    DEDUP_URL_NORMALIZE: bool = True
    DEDUP_DOI: bool = True
    DEDUP_TITLE_SIM: float = 0.95           # Jaccard threshold for title merges

    # Stage 7 — provenance & publication
    PUBLISH_CHUNK_SIZE: int = 100           # matches bulk_upsert chunking
    PIPELINE_VERSION: str = "v2"

    @field_validator("REPOSITORY_ALLOWLIST", "REPOSITORY_BLOCKLIST", mode="before")
    @classmethod
    def split_host_lists(cls, v):
        """Accept both JSON array (pydantic-settings default) and comma-sep string."""
        if isinstance(v, str):
            return [h.strip().lower() for h in v.split(",") if h.strip()]
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()
