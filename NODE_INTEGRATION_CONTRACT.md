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
