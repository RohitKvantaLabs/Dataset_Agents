# CLAUDE.md — Neuro Data Discovery Platform: Python Agent Service

This file is the source of truth for any AI coding agent (or human) picking up this repo.
Read this fully before writing code. It encodes decisions that were made deliberately —
don't "clean up" or "simplify" them without understanding why they're here first.

## 1. What this service is (and isn't)

This is **one piece of a larger platform**. The full system is:

```
Frontend (Next.js) -> Node.js API -> checks MongoDB Atlas
                                       |
                          hit? --------+--------  miss?
                          |                          |
                   return from DB         calls THIS Python service
                                                      |
                                     parse-query (blocking) -> JSON filters -> back to Node
                                     Node queries Mongo with those filters
                                          |
                                     still a miss? -> fallback-search (fire-and-forget)
                                          |
                              Python: search web -> verify links -> upsert Mongo -> publish Redis
                                          |
                              Node subscribed to Redis -> pushes to frontend
```

**This repo is ONLY the Python agent service.** It is not the primary read API, it has no
end-user-facing routes, and it is not where the frontend talks to directly. Node owns:
- All reads from MongoDB
- All end-user authentication
- The websocket/SSE connection to the frontend
- Redis subscription and forwarding to the frontend

Python owns:
- Turning natural language into structured filters (`QueryUnderstandingAgent`)
- Finding new datasets on the open web when Node's DB comes up empty (`FallbackAgent`)
- Verifying those candidates are real and reachable before anything is trusted (`VerificationAgent`)
- Writing newly-found datasets back to Mongo (write-only from this service's perspective)
- Publishing the result to Redis so Node can push it to the user

If you're about to add an endpoint the frontend would call directly, or a read-heavy search
endpoint — stop. That's Node's job. Check `NODE_INTEGRATION_CONTRACT.md` first.

## 2. Non-negotiable constraints

These aren't style preferences — violating them will cause real production bugs.

1. **No `BackgroundTasks` / fire-and-forget-after-response.** Vercel freezes a serverless
   function the moment it sends an HTTP response. Anything that needs to happen after a
   response is sent (Redis publish, Mongo write) is NOT SAFE unless it's guaranteed to have
   already run *before* the response goes out. `fallback_search()` in `app/api/v1/agents.py`
   does everything synchronously inside the request handler for exactly this reason. Keep it
   that way, even though the endpoint is conceptually "fire-and-forget" from Node's side.

2. **Never read-then-write to check for duplicates.** Two fallback workers can find the same
   dataset at the same time. `app/db/repositories/dataset_repository.py::upsert_dataset` uses a
   single atomic `find_one_and_update(upsert=True)` keyed on `(source, source_id)`. There is
   also a unique index on that pair (`scripts/ensure_indexes.py`) as a hard backstop. Do not
   "optimize" this into a `find()` followed by an `insert()`.

3. **Every Node-facing endpoint requires the `X-Internal-Secret` header.** See
   `app/core/security.py`. This is a cost control (every call can trigger a paid-eventually LLM
   call) as much as a security measure. Don't remove it or make it optional.

4. **The Verification Agent has no LLM in it.** Deterministic only — HTTP liveness check,
   domain trust-tiering, in-batch dedupe. The doc is explicit that fallback candidates must be
   independently verified, not just trusted because an LLM said so.

5. **LLM calls only ever go through `app/llm/client.py`.** We're on Hugging Face's free
   Inference API now, moving to a paid provider later. Every agent calls `LLMClient`, never a
   provider SDK directly, so that swap is a one-file change. Hugging Face's free tier doesn't
   reliably support native structured/function-calling output — `LLMClient.generate_json()`
   enforces JSON via prompting + strict parsing instead. Keep that pattern even after switching
   providers unless the new provider's structured output is verified reliable.

6. **All Pydantic models, all async.** Every request/response body, every internal data shape
   (`QueryFilters`, `Dataset`) is a Pydantic model — no raw dicts crossing a function boundary
   if a model already exists for that shape. All I/O (Mongo, Redis, HTTP, LLM calls) is `async`.

7. **`HF_TOKEN` and `TAVILY_API_KEY` are currently REQUIRED fields in `app/config.py`** (no
   default value) — the app will fail to start without them via pydantic-settings validation.
   If `README.md` or any other doc claims the service starts without them in a degraded
   "heuristic mode," that claim has NOT been verified against the actual code and should not
   be trusted until someone runs the app with those vars unset and confirms real behavior. Do
   not silently change this to `Optional[str] = None` to match outdated docs — if a no-token
   degraded mode is wanted, it needs its own real code path and needs to be flagged as a
   deliberate change, not a docs-matching patch.

## 3. Final folder structure

```
neuro-data-platform/
├── api/
│   └── index.py                        # Vercel entrypoint — imports `app` from app.main
│
├── app/
│   ├── main.py                         # FastAPI() instance, CORS, exception handlers, router mount
│   ├── config.py                       # Settings — all env vars, read once via lru_cache
│   │
│   ├── api/v1/
│   │   ├── router.py                   # aggregates health + agents routers
│   │   ├── health.py                   # GET /health — checks Mongo reachability
│   │   └── agents.py                   # POST /agents/parse-query, POST /agents/fallback-search
│   │
│   ├── agents/
│   │   ├── query_understanding_agent.py  # blocking — raw text -> QueryFilters (1 LLM call)
│   │   ├── fallback_agent.py             # web search + LLM -> candidate URLs (never a final answer)
│   │   ├── verification_agent.py         # deterministic — link check, trust tier, dedupe
│   │   └── search_provider.py            # SearchProvider interface — NOT wired to a real API yet
│   │
│   ├── llm/
│   │   ├── client.py                   # LLMClient — the ONLY place that calls huggingface_hub
│   │   └── prompts.py                  # every system/user prompt template, kept out of agent logic
│   │
│   ├── db/
│   │   ├── mongo.py                    # Motor client, lazy connect, serverless-safe
│   │   └── repositories/
│   │       └── dataset_repository.py   # upsert_dataset() — atomic, no read-then-write
│   │
│   ├── services/
│   │   └── redis_publisher.py          # publish_fallback_result() -> Node's subscribed channel
│   │
│   ├── models/
│   │   ├── dataset.py                  # Dataset — the common schema, also the Mongo document shape
│   │   └── query_filters.py            # QueryFilters + the parse-query request/response models
│   │
│   ├── core/
│   │   ├── security.py                 # require_internal_secret dependency
│   │   └── exceptions.py               # UpstreamServiceError + global exception handlers
│   │
│   └── utils/                          # (empty — shared helpers land here as they're needed)
│
├── scripts/
│   └── ensure_indexes.py               # run once against Atlas: unique index on (source, source_id)
│
├── tests/
│   ├── conftest.py                     # shared fixtures (mock LLMClient, mock Mongo, etc.)
│   ├── test_agents/                    # one test file per agent, LLM calls mocked
│   ├── test_api/                       # endpoint tests via FastAPI TestClient
│   └── test_services/                  # redis_publisher, etc.
│
├── .env.example                        # every required env var, documented
├── .gitignore
├── vercel.json                         # routes to api/index.py, maxDuration: 60 for fallback-search
├── requirements.txt                    # ranged versions (>=,<) — see note below
└── NODE_INTEGRATION_CONTRACT.md        # exact request/response/Redis schemas for the Node team
```

Note on `requirements.txt`: use ranged versions (`>=x,<y`), never hard `==` pins, and re-verify
`uv pip install --dry-run -r requirements.txt` in a clean venv before considering a dependency
change done. `motor` and `pymongo` have a narrow compatible band — check that pairing
specifically if either version changes.
Note on `requirements.txt`: use ranged versions (`>=x,<y`), never hard `==` pins, and re-verify
`pip install --dry-run -r requirements.txt` in a clean venv before considering a dependency
change done. `motor` and `pymongo` have a narrow compatible band — check that pairing
specifically if either version changes.

## 4. Build order (do these in this order — later steps assume earlier ones work)

**Already built and manually verified (imports cleanly, routes registered, auth guard tested):**
- [x] `app/config.py`, `app/main.py`, `app/core/*`
- [x] `app/models/dataset.py`, `app/models/query_filters.py`
- [x] `app/db/mongo.py`, `app/db/repositories/dataset_repository.py`, `scripts/ensure_indexes.py`
- [x] `app/llm/client.py`, `app/llm/prompts.py`
- [x] `app/agents/query_understanding_agent.py`
- [x] `app/agents/fallback_agent.py`, `app/agents/search_provider.py` (Tavily-backed, wired into `app/api/v1/agents.py`)
- [x] `app/agents/verification_agent.py`
- [x] `app/services/redis_publisher.py`
- [x] `app/api/v1/health.py`, `app/api/v1/agents.py`, `app/api/v1/cron.py`, `api/index.py`, `vercel.json`

**Remaining, in priority order for the July 9 deadline:**

1. **Fix `README.md` — it currently contains an unverified claim.** It says the service starts
   without `HF_TOKEN`/`TAVILY_API_KEY` in a "deterministic heuristic parser" mode. Per
   Section 2, constraint 7, this contradicts the actual `config.py`. Run the app with those
   vars unset, observe the real behavior, then rewrite the README section to match reality —
   don't just reword it without testing. Do NOT change the code to match the README's claim
   without flagging that as a deliberate decision first.

2. **Write tests.** Mock `LLMClient` (don't hit real HF in tests — subclass or monkeypatch
   `generate_json`) and mock Motor/Redis/Tavily. Priority: `QueryUnderstandingAgent` JSON-parse-failure
   fallback path, `VerificationAgent` link-check logic and `revalidate()`, `TavilySearchProvider`
   with a mocked `httpx` response (success, timeout, and malformed-response cases), and all three
   endpoints (`parse-query`, `fallback-search`, `cron/reverify-links`) via `TestClient` with and
   without their respective auth headers.

3. **Run `scripts/ensure_indexes.py` against your actual Atlas cluster** before any real
   fallback traffic hits it — the unique index is the backstop for concurrent writes.

4. **Deploy to Vercel.** Set every var from `.env.example` (including `TAVILY_API_KEY` and
   `CRON_SECRET`) in Vercel's Environment Variables. Confirm your Vercel plan actually supports
   the `maxDuration: 60` and the `crons` schedule set in `vercel.json` — check current plan
   limits before assuming this works as configured.

5. **Coordinate with Node on `NODE_INTEGRATION_CONTRACT.md`.** Specifically confirm: the exact
   Redis channel naming Node subscribes to, and what `query_id` format Node will send (must be
   safe to interpolate into a Redis channel name).

**Explicitly out of scope for this service (do not build unless the team decides otherwise):**
- Dataset ingestion connectors (OpenNeuro, DANDI, etc.) — the earlier draft had these in
  Python; ownership between Node and Python here was never confirmed. Don't build until decided.
- Anything JWT/end-user-auth related — Node owns this entirely.

## 4a. Scheduled link re-verification (cron)

Python owns this. `GET /api/v1/cron/reverify-links`, triggered monthly by Vercel Cron
(`vercel.json` → `crons`), protected by `require_cron_secret` (checks
`Authorization: Bearer <CRON_SECRET>` — Vercel sends this automatically when `CRON_SECRET` is
set in the project's env vars; this is a **different secret and header** from Node's
`X-Internal-Secret`, don't conflate them).

Each run: finds datasets with `trust_tier` in `verified`/`stale` whose `last_verified_at` is
either null or older than `CRON_STALE_THRESHOLD_DAYS` (default 30), capped at `CRON_BATCH_SIZE`
(default 100) per invocation, re-checks each link via `VerificationAgent.revalidate()`
(sequentially, not concurrently — deliberate, so a monthly job doesn't look like a burst/DoS to
external hosts), and updates `trust_tier` + `last_verified_at` via a single-document
`update_one` (not a full upsert — we already know these documents exist).

`UNVERIFIED` datasets are excluded from this job on purpose — that tier means "found via
fallback, never checked at all," which is what the *fallback-search* verification path handles,
not this one. A successful re-check always sets `VERIFIED` (the re-check itself is the
confirmation), never `UNVERIFIED`; a failed re-check sets `STALE`. If the dataset backlog grows
past what one monthly run clears, raise `CRON_BATCH_SIZE` or the schedule frequency — don't
remove the batch cap, since Vercel will kill a run that exceeds `maxDuration`.

## 5. Coding conventions

- Type hints on every function signature, always.
- Logging via the standard `logging` module, one logger per file (`logging.getLogger("neuro_platform.<area>")`), never `print()`.
- External calls (LLM, HTTP, Mongo) wrapped in `tenacity` retries where transient failure is
  plausible (see `app/llm/client.py::_chat` for the pattern) — use `reraise=True` on every
  `@retry` decorator, or the specific exception type your `except` clause expects will get
  wrapped in `tenacity.RetryError` instead and silently fail to be caught (this was a real bug,
  found and fixed in `app/agents/search_provider.py` and `app/llm/client.py` — don't reintroduce
  it in new retry-wrapped code).
- Degrade gracefully where the doc calls for it (e.g. `QueryUnderstandingAgent` falls back to
  keyword-only filters rather than 500ing Node), but never degrade silently — always log a
  `warning` when you do, and never claim a degraded mode exists in docs without a test proving it.
- Keep agents "narrow": one agent, one job. If you're adding a second responsibility to an
  existing agent file, it probably belongs in a new agent or a `services/` helper instead.