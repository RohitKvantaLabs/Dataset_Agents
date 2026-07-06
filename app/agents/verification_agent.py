"""
Verification Agent — deterministic, no LLM calls ever.

Every candidate URL produced by the Fallback Agent passes through here
before it is stored in Mongo or shown to a user. Three checks:
  1. URL liveness (HEAD → GET fallback)
  2. In-batch deduplication (seen_urls set)
  3. Domain trust-tier annotation (known repositories get a log note)

A stable source_id is derived from a SHA-1 hash of the URL so the atomic
(source, source_id) upsert key in dataset_repository is always populated.
"""
import hashlib
import logging

import httpx

from app.agents.fallback_agent import FallbackCandidate
from app.config import get_settings
from app.models.dataset import Dataset, TrustTier

logger = logging.getLogger("neuro_platform.agents.verification")

# Domains we already trust as official connectors elsewhere in the
# platform. A fallback hit on one of these is more credible than a
# random domain — bump it in ranking rather than treating everything
# from the fallback path identically.
KNOWN_REPOSITORY_DOMAINS = {
    "openneuro.org",
    "dandiarchive.org",
    "nitrc.org",
    "humanconnectome.org",
    "adni.loni.usc.edu",
    "ebrains.eu",
}


class VerificationAgent:
    """
    Deterministic — no LLM. Every candidate URL from the Fallback Agent
    passes through here before it can be stored or shown to a user.
    """

    def __init__(self, http_client: httpx.AsyncClient | None = None) -> None:
        settings = get_settings()
        self._owns_client = http_client is None
        self._client = http_client or httpx.AsyncClient(
            timeout=settings.HTTP_CHECK_TIMEOUT_SECONDS, follow_redirects=True
        )

    async def verify(self, candidates: list[FallbackCandidate]) -> list[Dataset]:
        verified: list[Dataset] = []
        seen_urls: set[str] = set()

        for candidate in candidates:
            if not candidate.url or candidate.url in seen_urls:
                continue  # in-batch dedupe; DB-level dedupe happens via the upsert key
            seen_urls.add(candidate.url)

            is_live, domain = await self._check_link(candidate.url)
            if not is_live:
                logger.info("Dropping dead/unreachable candidate: %s", candidate.url)
                continue

            # Derive a stable, unique source_id from the URL so the
            # (source, source_id) upsert key is always populated.
            source_id = hashlib.sha1(candidate.url.encode()).hexdigest()[:16]
            source = candidate.source_guess or "web_search"

            if domain in KNOWN_REPOSITORY_DOMAINS:
                logger.info("Candidate %s is from a known repository domain", candidate.url)

            try:
                dataset = Dataset(
                    title=candidate.title,
                    description=candidate.reasoning or "Fallback candidate pending review",
                    source=source,
                    source_id=source_id,
                    url=candidate.url,
                )
            except Exception as exc:  # invalid URL, malformed data, etc.
                logger.info(
                    "Dropping candidate that failed schema validation: %s (%s)",
                    candidate.url,
                    exc,
                )
                continue

            verified.append(dataset)

        if self._owns_client:
            await self._client.aclose()

        return verified

    async def revalidate(self, dataset: Dataset) -> TrustTier:
        """
        Re-check an existing dataset URL for scheduled link maintenance.
        A successful re-check confirms VERIFIED; a failed check marks STALE.
        """
        is_live, _ = await self._check_link(str(dataset.url))
        if self._owns_client:
            await self._client.aclose()
        return TrustTier.VERIFIED if is_live else TrustTier.STALE

    async def _check_link(self, url: str) -> tuple[bool, str]:
        """Return (is_live, domain). Handles malformed URLs gracefully."""
        try:
            parsed = httpx.URL(url)
        except Exception:
            logger.info("Malformed URL, skipping: %s", url)
            return False, ""

        domain = parsed.host or ""
        try:
            resp = await self._client.head(url)
            if resp.status_code >= 400:
                # Some servers reject HEAD — retry with a lightweight GET before giving up.
                resp = await self._client.get(url)
            return resp.status_code < 400, domain
        except httpx.HTTPError as exc:
            logger.info("Link check failed for %s: %s", url, exc)
            return False, domain
