# Node <-> Python Agent Service Contract

All requests require header: `X-Internal-Secret: <INTERNAL_API_SECRET>`
(shared secret, set in both services' env vars - not the same as any user-facing auth).

---

## 1. POST /api/v1/agents/parse-query
**Blocking.** Node must await this before querying Mongo.

**Request**
```json
{ "query": "resting state fMRI in kids with ADHD" }
```

**Response `200`**
```json
{
  "filters": {
    "modality": ["fMRI"],
    "species": ["human"],
    "age_range": "pediatric",
    "condition": ["ADHD"],
    "task": "resting-state",
    "format": [],
    "keywords": [],
    "raw_query": "resting state fMRI in kids with ADHD"
  }
}
```
Node builds its own Mongo query from `filters`. If the LLM output fails to parse, this endpoint
still returns `200` with a degraded object (`keywords` populated from a raw split, everything else
empty) rather than erroring - Node can still attempt a plain-text search.

---

## 2. POST /api/v1/agents/fallback-search
**Fire-and-forget from Node's side** - Node does not need to read the response body, but the
request itself must stay open; Python does not use background tasks (see code comments in
`app/api/v1/agents.py` for why - Vercel-specific). Give the HTTP client a generous timeout
(60s+) or set it to not wait on the body at all.

**Request**
```json
{
  "query_id": "sess_abc123",
  "query": "resting state fMRI in kids with ADHD",
  "filters": { "...": "the exact `filters` object returned by /parse-query" }
}
```
`query_id` is whatever Node uses to correlate the eventual Redis message back to the right
frontend connection (websocket/SSE session, request id, etc.) - Python does not generate this.

**Response `200`** (safe to ignore)
```json
{ "query_id": "sess_abc123", "datasets_found": 3, "published": true }
```

**What actually matters: the Redis message.**
Python publishes to channel `fallback-result:{query_id}` (prefix configurable via
`REDIS_RESULT_CHANNEL_PREFIX`) exactly once, when the job finishes:
```json
{
  "query_id": "sess_abc123",
  "query": "resting state fMRI in kids with ADHD",
  "datasets": [
    {
      "title": "...",
      "description": "...",
      "source": "web_search",
      "source_id": "...",
      "url": "https://...",
      "modality": [],
      "species": [],
      "is_direct_link": false,
      "trust_tier": "unverified",
      "last_verified_at": null,
      "ingested_at": "2026-07-03T12:00:00Z",
      "updated_at": "2026-07-03T12:00:00Z"
    }
  ]
}
```
`datasets` can be an empty array - that's a valid "nothing found" outcome, not an error. Node
should handle that in the UI rather than treating it as a failure.

**Relevance filter (applied before any network check):** candidates whose title or URL contain
spec/documentation signals (`specification`, `documentation`, `changelog`, `manual`, `user guide`,
`white paper`, `readme.pdf`, `spec.pdf`) are dropped entirely, as are bare `.pdf` URLs. This means
Node will never receive BIDS spec PDFs, standards documents, or README files as dataset results —
they are filtered before the link-liveness check runs, not just ranked lower.

**`is_direct_link`:** `true` when the URL resolves to a data file or archive (detected via URL
extension, `Content-Type: application/zip/octet-stream/…`, or `Content-Disposition: attachment`).
`false` for repository landing pages. Node can use this to display a "Direct download" badge.

**`trust_tier`:** newly-found candidates always arrive as `"unverified"`. The scheduled
`/api/v1/cron/reverify-links` job re-checks live URLs and upgrades them to `"verified"` or
downgrades to `"stale"`. Node should not display `"stale"` links to users without a warning.

These same datasets are also upserted into Mongo (`datasets` collection, keyed on
`source + source_id`) at the same time, so the next identical query is a cache hit on Node's side.

---

## Open items before this is production-ready
- Fallback Agent has no real web-search provider wired in yet (`app/agents/search_provider.py`) -
  currently LLM-only guesses, verified by the Verification Agent's link check. Needs a real
  provider (Tavily/Serper/Bing) before Phase 2 fallback quality is trustworthy.
- Confirm Vercel plan supports the `maxDuration: 60` set in `vercel.json` for the fallback
  endpoint - LLM + verification can take a while.

---















# Security & Infrastructure Audit (Python repo only)

**Audit date:** 2026-07-18  
**Scope:** `d:\Neuro-Agents` (Python FastAPI agent service only). Node.js backend is in a separate repository — Node-specific checks (`npm audit`, `errorHandler.js`, Express limiters, Mongoose/JWT routes) are marked N/A or mapped to Python equivalents where applicable.  
**Method:** Grep + file reads across the repo; `pip list --outdated` in `.venv`; runtime probes via `TestClient` (temporary probe script, deleted after run). No code fixes applied.

---

## 1. Rate limiting coverage

**Verdict: DOES NOT EXIST**

No rate-limiting middleware or library is present in this Python repo. Grep for `rate_limit`, `RateLimit`, `slowapi`, and `limiter` across `*.py` files returns only unrelated matches (none in application code). `app/main.py` registers only CORS middleware — no throttling.

The audit checklist referenced Node limiter names (`generalLimiter`, `searchLimiter`, `otpVerifyLimiter`, etc.). Those do not exist here. This service exposes **four** HTTP routes total (prefix `/api/v1` from `app/config.py`).

### Route table (Python API — actual routes)

| Method | Full path | Auth dependency | Rate limiter |
|--------|-----------|-----------------|--------------|
| `GET` | `/api/v1/health` | none | **none** |
| `POST` | `/api/v1/agents/parse-query` | `require_internal_secret` (`X-Internal-Secret`) | **none** |
| `POST` | `/api/v1/agents/fallback-search` | `require_internal_secret` (`X-Internal-Secret`) | **none** |
| `GET` | `/api/v1/cron/reverify-links` | `require_cron_secret` (`Authorization: Bearer <CRON_SECRET>`) | **none** |

**Router wiring** (`app/api/v1/router.py`):

```1:8:app/api/v1/router.py
from fastapi import APIRouter

from app.api.v1 import agents, cron, health

router = APIRouter()
router.include_router(health.router, tags=["health"])
router.include_router(agents.router, tags=["agents"])
router.include_router(cron.router, tags=["cron"])
```

### Routes from the audit checklist that do not exist in this repo

| Audit item | Verdict |
|------------|---------|
| auth routes (e.g. `POST /auth/login`) | **DOES NOT EXIST** |
| user routes | **DOES NOT EXIST** |
| admin routes (e.g. `POST /admin/login`) | **DOES NOT EXIST** |
| dataset read/search routes | **DOES NOT EXIST** (Mongo writes only via fallback upsert) |
| query-logs routes | **DOES NOT EXIST** |
| stream / SSE / websocket routes | **DOES NOT EXIST** |

**Specific confirmations:**
- `POST /auth/login` — **DOES NOT EXIST** in this repo. No login endpoint, no OTP limiter, no `generalLimiter`.
- `POST /admin/login` — **DOES NOT EXIST** in this repo.

---

## 2. Dependency audit

**Verdict: PARTIAL** (Python outdated list captured; `pip-audit` not installed; `npm audit` N/A)

### 2a. `npm audit` (Node project)

**SKIPPED — N/A.** No `package.json` in this repository. Node backend is in a separate repo per audit scope.

### 2b. `pip list --outdated` (Python project)

Run: `.venv\Scripts\python.exe -m pip list --outdated` from repo root on 2026-07-18.

**Full output:**

```
Package            Version  Latest  Type
------------------ -------- ------- -----
anyio              4.14.1   4.14.2  wheel
charset-normalizer 3.4.7    3.4.9   wheel
coverage           7.15.0   7.15.2  wheel
fastapi            0.115.14 0.139.2 wheel
filelock           3.29.5   3.30.3  wheel
hf-xet             1.5.1    1.5.2   wheel
huggingface_hub    1.22.0   1.24.0  wheel
pydantic_core      2.46.4   2.47.0  wheel
pytest             8.4.2    9.1.1   wheel
pytest-asyncio     0.25.3   1.4.0   wheel
pytest-cov         5.0.0    7.1.0   wheel
redis              5.3.1    8.0.1   wheel
starlette          0.46.2   1.3.1   wheel
structlog          24.4.0   26.1.0  wheel
tqdm               4.68.3   4.69.0  wheel
tzdata             2026.2   2026.3  wheel
uvicorn            0.34.3   0.51.0  wheel
websockets         16.0     16.1.1  wheel
```

(System-wide `pip list --outdated` outside `.venv` reported only `uv 0.11.21 → 0.11.29`; the pinned app dependencies live in `.venv`.)

### 2c. `pip-audit`

**`pip-audit` is not installed** in this environment.

- Global shell: `pip-audit` → `CommandNotFoundException`
- Project venv: `python -m pip_audit` → `No module named pip_audit`

No vulnerability scan output is available from this pass.

---

## 3. Error handling / information leakage

**Verdict: PARTIAL**

- Python equivalent of Node's `errorHandler.js` is **`app/core/exceptions.py`** — there is **no** `errorHandler.js` in this repo.
- Custom exception handlers are **defined but not registered** on the app (`register_exception_handlers` is never called from `app/main.py`).
- Runtime behavior therefore mixes FastAPI/Starlette defaults with explicit `HTTPException` detail strings from auth dependencies.

### 3a. Full contents of `app/core/exceptions.py` (Python error handler module)

```1:37:app/core/exceptions.py
import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

logger = logging.getLogger("neuro_platform")


class UpstreamServiceError(Exception):
    """Raised when a connector, LLM call, or link check fails after retries."""

    def __init__(self, service: str, detail: str):
        self.service = service
        self.detail = detail
        super().__init__(f"{service}: {detail}")


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(UpstreamServiceError)
    async def upstream_error_handler(request: Request, exc: UpstreamServiceError):
        logger.error("Upstream service failed: %s - %s", exc.service, exc.detail)
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={
                "error": "upstream_service_error",
                "service": exc.service,
                "detail": exc.detail,
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception):
        logger.exception("Unhandled error on %s", request.url.path)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "internal_server_error"},
        )
```

**`errorHandler.js`:** **DOES NOT EXIST** in this repository.

### 3b. Registration status

`app/main.py` creates the FastAPI app, adds CORS, mounts the v1 router — it **does not** import or call `register_exception_handlers`:

```35:60:app/main.py
def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title=settings.APP_NAME,
        description=(
            "Aggregated search and discovery engine for global "
            "neuroscience and neuroimaging datasets."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS — allow the MERN/Next.js frontend and Vercel preview URLs.
    # Tighten allow_origins in production to your specific domain(s).
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],   # tighten in prod
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(api_v1_router, prefix=settings.API_V1_PREFIX)

    return app
```

Grep for `register_exception_handlers` across `*.py`: defined only in `app/core/exceptions.py`, **zero call sites**.

### 3c. Does the client ever receive `err.message`, `err.stack`, or raw driver/library text?

| Error source | Handlers registered? | Client response | Includes raw message/stack? |
|--------------|---------------------|-----------------|------------------------------|
| `HTTPException` (auth failures in `app/core/security.py`) | N/A (FastAPI built-in) | JSON `{"detail": "<fixed string>"}` e.g. `"Invalid internal secret."` | **No** stack; **yes** fixed `detail` string (not raw exception) |
| Pydantic / FastAPI validation (`422`) | Built-in | JSON `{"detail": […]}` with field paths, constraint types, `msg` | **No** stack; **yes** validation messages (not driver errors) |
| Unhandled `Exception` (e.g. `RuntimeError`, `DuplicateKeyError`) | **No** (Starlette default) | **`500`** body **`Internal Server Error`** (`text/plain`) | **No** — generic plain text, no JSON, no stack |
| `UpstreamServiceError` | **No** (current prod wiring) | **`500`** `Internal Server Error` (plain text) | **No** |
| `UpstreamServiceError` | **Yes** (if `register_exception_handlers` were wired) | **`502`** JSON `{"error","service","detail"}` | **No** stack; **yes** `exc.detail` string passed through |

Auth dependency raises fixed-detail `HTTPException` responses (`app/core/security.py`):

```25:29:app/core/security.py
    if not hmac.compare_digest(x_internal_secret, configured_secret):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid internal secret.",
        )
```

### 3d. Runtime traces (TestClient, current `create_app()` wiring)

Probed 2026-07-18 with mocked settings/I/O; `raise_server_exceptions=False`.

| Scenario | Route / trigger | Status | Response body |
|----------|-----------------|--------|---------------|
| **(a) Invalid ObjectId in admin route param** | `GET /api/v1/admin/users/not-a-valid-objectid/something` | **404** | `{"detail":"Not Found"}` |
| **(a) note** | No `admin/users` routes exist — request hits FastAPI 404, not ObjectId parsing | — | — |
| **(b) Mongo duplicate-key (`DuplicateKeyError` / E11000)** | `POST /api/v1/agents/fallback-search` with `upsert_many` patched to raise `DuplicateKeyError("E11000 duplicate key error …")` | **500** | `Internal Server Error` (plain text) — E11000 text **not** returned to client |
| **(c) Unhandled exception in handler** | `POST /api/v1/agents/parse-query` with `QueryUnderstandingAgent.parse` raising `RuntimeError("raw driver boom …")` | **500** | `Internal Server Error` (plain text) — no stack in body |
| Wrong internal secret | `POST /api/v1/agents/parse-query` | **401** | `{"detail":"Invalid internal secret."}` |
| Validation error | `POST /api/v1/agents/parse-query` with `{"query":"x"}` | **422** | `{"detail":[{"type":"string_too_short","loc":["body","query"],"msg":"String should have at least 2 characters",…}]}` |
| `UpstreamServiceError` (handlers **registered** manually in probe only) | same parse-query route | **502** | `{"error":"upstream_service_error","service":"llm","detail":"provider timeout detail text"}` |

**Mongoose / JWT:** **N/A** — this service uses Motor/PyMongo, not Mongoose; no JWT auth routes (`pyjwt` is listed in `requirements.txt` but no application code imports or uses it).

### 3e. Process-level `unhandledRejection` / `uncaughtException` handlers

**DOES NOT EXIST** (Node terminology). Grep for `unhandledRejection`, `uncaughtException`, `excepthook`, and asyncio global exception hooks across the repo: **no matches** in application code.

`app/main.py` lifespan only closes the Motor client on shutdown — no process-level exception hooks.

There is no Express-style `asyncHandler` wrapper; FastAPI runs async route functions directly. Unhandled exceptions inside route handlers surface as HTTP 500 via Starlette's default error middleware (plain text when custom handlers are not registered).

---

## 4. File upload functionality

**Verdict: DOES NOT EXIST**

### Grep results (Python application + tests + scripts)

| Pattern | Hits in `app/`, `tests/`, `scripts/`, `api/` |
|---------|---------------------------------------------|
| `multer` | **0** |
| `upload` | **0** |
| `multipart` | **0** (application code) |
| `fileFilter` | **0** |
| `storage.diskStorage` / `diskStorage` | **0** |
| `UploadFile` / `File(` (FastAPI upload types) | **0** |
| `avatar` / `image upload` | **0** |

**Only related hit in the repo:**

| File | Line | Content |
|------|------|---------|
| `requirements.txt` | 51 | `python-multipart==0.0.32` |

`python-multipart` is pinned as a dependency but **no route or agent code** imports `UploadFile`, `File`, or `Form` for multipart handling. Likely a transitive/FastAPI-related install with no active upload endpoints.

### Plain confirmation

**No file upload functionality exists anywhere in this Python codebase right now.**

---

*End of audit report.*

