# Neuro Data Discovery — Python Agent Service

The Python microservice in the Neuro Data Platform. It sits behind the Node.js API and does three things:

1. **Parse query** (`POST /api/v1/agents/parse-query`) — Converts a researcher's natural-language question into structured `QueryFilters` that Node uses to query MongoDB.
2. **Fallback search** (`POST /api/v1/agents/fallback-search`) — When MongoDB has no match, searches the open web (Tavily), verifies links, upserts results into MongoDB, and publishes to Redis so Node can push them to the frontend.
3. **Health check** (`GET /api/v1/health`) — Reports MongoDB connectivity.

See [`NODE_INTEGRATION_CONTRACT.md`](./NODE_INTEGRATION_CONTRACT.md) for exact request/response shapes. See [`CLAUDE.md`](./CLAUDE.md) for architecture constraints (read before modifying anything).

---

## Prerequisites

| Tool | Version |
|---|---|
| Python | 3.11 + |
| pip | latest |
| MongoDB | local (`mongodb://localhost:27017`) **or** Atlas URI |
| Redis | local (`redis://localhost:6379`) **or** managed instance |

---

## Local Setup

### 1. Clone & create a virtual environment

```bash
git clone <repo-url>
cd neuro-data-platform
python -m venv .venv

# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

Verify the install resolves without conflicts:

```bash
pip install --dry-run -r requirements.txt
```

> **Note on `motor` + `pymongo` versions**: they have a narrow compatible band. If you change either version in `requirements.txt`, re-verify the pairing with `pip install --dry-run` in a clean venv.

### 3. Configure environment variables

Copy `.env` and fill in the real values:

```bash
cp .env .env.local   # optional — .env is gitignored
```

Then edit `.env`:

```ini
# Required for all LLM calls (Query Understanding + Fallback Discovery)
GROQ_API_KEY=gsk_xxxx                     # Get a key at https://console.groq.com/keys

# Required for real web search in the Fallback Agent
TAVILY_API_KEY=tvly-xxxx                  # Get a free key at https://app.tavily.com/

# Required for the internal auth guard on every endpoint
INTERNAL_API_SECRET=some-long-random-string   # Must match what Node sends

# MongoDB (local or Atlas)
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=neuro_data_platform

# Redis (local or managed)
REDIS_URL=redis://localhost:6379/0
```

`GROQ_API_KEY` and `TAVILY_API_KEY` are required startup configuration. Verified
behavior in this workspace:

- With the required values present in `.env`, the app imports successfully and
  the server responds normally on the health endpoint.
- If either required value is unset, `pydantic-settings` raises validation errors while
  importing `app.main`, so the FastAPI app does not start.

`parse-query` has a deterministic fallback for LLM output/parse failures
after the service has started with a valid `GROQ_API_KEY`; that fallback is not a
no-token operating mode.

A retained `HF_TOKEN` is optional — it is only used by the ingestion
embedder (vector embeddings for Atlas Search). If absent, embeddings are
skipped gracefully.

### 4. Create MongoDB indexes (run once, before production traffic)

```bash
python scripts/ensure_indexes.py
```

This creates the unique index on `(source, source_id)` that prevents duplicate upserts. Safe to re-run — `create_index` is idempotent.

### 5. Start the server

```bash
uvicorn app.main:app --reload
```

The API is available at `http://localhost:8000`. Docs at `http://localhost:8000/docs`.

---

## Running Tests

```bash
pytest tests/ -v
```

All tests mock LLM calls, MongoDB, and Redis — no real credentials needed to run the suite.

```bash
# Coverage report
pytest tests/ --cov=app --cov-report=term-missing
```

---

## Smoke Testing the Endpoints

Set your secret in a shell variable:

```bash
SECRET=your-internal-secret
```

**Health check** (no auth required):
```bash
curl http://localhost:8000/api/v1/health
# {"status":"degraded","mongo_connected":false}  ← expected locally without Atlas
```

**Parse query** (requires secret):
```bash
curl -X POST http://localhost:8000/api/v1/agents/parse-query \
  -H "Content-Type: application/json" \
  -H "X-Internal-Secret: $SECRET" \
  -d '{"query": "resting state fMRI in kids with ADHD"}'
```

Expected response shape (matches `NODE_INTEGRATION_CONTRACT.md §1`):
```json
{
  "filters": {
    "modality": ["fmri"],
    "species": [],
    "age_range": "pediatric",
    "condition": ["adhd"],
    "task": "resting-state",
    "format": [],
    "keywords": [],
    "raw_query": "resting state fMRI in kids with ADHD"
  }
}
```

**Auth failure** (wrong secret → 401):
```bash
curl -X POST http://localhost:8000/api/v1/agents/parse-query \
  -H "Content-Type: application/json" \
  -H "X-Internal-Secret: wrong" \
  -d '{"query": "test"}'
# {"detail":"Invalid internal secret."}
```

**Fallback search** (requires secret + running Redis + running Mongo):
```bash
curl -X POST http://localhost:8000/api/v1/agents/fallback-search \
  -H "Content-Type: application/json" \
  -H "X-Internal-Secret: $SECRET" \
  -d '{
    "query_id": "sess_test01",
    "query": "resting state fMRI in kids with ADHD",
    "filters": {
      "modality": ["fmri"], "species": [], "age_range": "pediatric",
      "condition": ["adhd"], "task": "resting-state", "format": [],
      "keywords": [], "raw_query": "resting state fMRI in kids with ADHD"
    }
  }'
```

---

## Deployment (Vercel)

1. Push the repo to GitHub / GitLab.
2. Connect it to a new Vercel project.
3. Set every variable from `.env` under **Settings → Environment Variables**.
4. Confirm your Vercel plan supports `maxDuration: 60` (set in `vercel.json` for the fallback endpoint). Check current plan limits before assuming — Hobby plan caps at 10s.
5. The Vercel entrypoint is `api/index.py` → imports `app` from `app.main`.

---

## Architecture Decision Record

Key constraints (full details in `CLAUDE.md`):

| Constraint | Why |
|---|---|
| No `BackgroundTasks` / fire-and-forget | Vercel freezes the function on response send |
| Atomic `find_one_and_update(upsert=True)` only | Two fallback workers can race on the same dataset |
| `X-Internal-Secret` on every endpoint | Each call may trigger a paid LLM call — cost control |
| No LLM in `VerificationAgent` | Candidates must be independently verified, not LLM-trusted |
| All LLM calls through `app/llm/client.py` | Provider swap is a single-file change (now uses Groq) |

---

## What Still Needs Your Input

| Item | Status |
|---|---|
| `TAVILY_API_KEY` | Required in `.env` — the app fails startup validation without it |
| `GROQ_API_KEY` | Required in `.env` — the app fails startup validation without it |
| `INTERNAL_API_SECRET` | Set the same value in Node's env |
| Vercel plan `maxDuration` | Confirm your plan supports 60s — Hobby plan doesn't |
| Redis channel name with Node | Confirm Node subscribes to `fallback-result:{query_id}` (default) |
| `query_id` format from Node | Confirm it's safe for Redis channel interpolation (no spaces/special chars) |
| Atlas index creation | Run `python scripts/ensure_indexes.py` against your Atlas cluster once |
