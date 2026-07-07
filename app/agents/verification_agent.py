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

# Extensions that unambiguously identify an actual neuro data file/archive.
# ponytail: flat set, no class hierarchy needed.
KNOWN_DATASET_EXTENSIONS = {
    ".dcm", ".nii", ".nii.gz", ".mnc", ".edf", ".bdf",
    ".vhdr", ".vmrk", ".eeg", ".fif", ".nwb", ".h5", ".hdf5",
}

# Content-Type values that mean "binary blob / archive" (i.e. a download).
_BINARY_CONTENT_TYPES = {
    "application/octet-stream",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/x-tar",
}

# Signals in title/URL that identify specification or documentation documents —
# never actual datasets, so drop outright (no network check needed).
# ponytail: flat set, checked case-insensitively.
NON_DATASET_SIGNAL_TERMS = {
    "specification", "spec.pdf", "documentation", "changelog",
    "manual", "readme.pdf", "white paper", "user guide",
}

# URL suffixes that are almost never datasets themselves.
EXCLUDED_EXTENSIONS = {".pdf"}


def _is_non_dataset(title: str, url: str) -> bool:
    """Return True when the candidate is clearly a spec/doc, not a dataset."""
    needle = (title + " " + url).lower()
    if any(term in needle for term in NON_DATASET_SIGNAL_TERMS):
        return True
    lower_url = url.lower().split("?")[0].split("#")[0]  # strip query/fragment
    return any(lower_url.endswith(ext) for ext in EXCLUDED_EXTENSIONS)


def _is_direct_link_by_path(url: str) -> bool:
    """Check the URL path alone — no network needed."""
    lower = url.lower()
    if "dataset_description.json" in lower:
        return True  # BIDS root marker
    # Multi-char extensions like .nii.gz must be checked before .nii
    for ext in sorted(KNOWN_DATASET_EXTENSIONS, key=len, reverse=True):
        if lower.endswith(ext) or f"{ext}?" in lower or f"{ext}#" in lower:
            return True
    return False


def _is_direct_link_by_headers(response: httpx.Response) -> bool:
    """Inspect Content-Type / Content-Disposition without re-requesting."""
    ct = response.headers.get("content-type", "").split(";")[0].strip().lower()
    if ct in _BINARY_CONTENT_TYPES:
        return True
    cd = response.headers.get("content-disposition", "").lower()
    return "attachment" in cd


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

            if _is_non_dataset(candidate.title, candidate.url):
                logger.info("Dropping spec/doc candidate: %s", candidate.url)
                continue

            is_live, domain, response = await self._check_link(candidate.url)
            if not is_live:
                logger.info("Dropping dead/unreachable candidate: %s", candidate.url)
                continue

            # Determine is_direct_link: path first (free), then headers.
            if _is_direct_link_by_path(candidate.url):
                is_direct = True
            elif response is not None:
                is_direct = _is_direct_link_by_headers(response)
            else:
                is_direct = False

            # Only drop unknown-domain landing-page candidates with no credibility signal.
            if (
                not is_direct
                and domain not in KNOWN_REPOSITORY_DOMAINS
                and not candidate.source_guess
            ):
                logger.info(
                    "Dropping unknown-domain landing page with no credibility signal: %s",
                    candidate.url,
                )
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
                    is_direct_link=is_direct,
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
        is_live, _, _ = await self._check_link(str(dataset.url))
        if self._owns_client:
            await self._client.aclose()
        return TrustTier.VERIFIED if is_live else TrustTier.STALE

    async def _check_link(self, url: str) -> tuple[bool, str, httpx.Response | None]:
        """Return (is_live, domain, response). Handles malformed URLs gracefully."""
        try:
            parsed = httpx.URL(url)
        except Exception:
            logger.info("Malformed URL, skipping: %s", url)
            return False, "", None

        domain = parsed.host or ""
        try:
            resp = await self._client.head(url)
            if resp.status_code >= 400:
                # Some servers reject HEAD — retry with a lightweight GET before giving up.
                resp = await self._client.get(url)
            return resp.status_code < 400, domain, resp
        except httpx.HTTPError as exc:
            logger.info("Link check failed for %s: %s", url, exc)
            return False, domain, None
