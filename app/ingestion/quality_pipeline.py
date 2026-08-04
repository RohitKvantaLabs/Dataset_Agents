"""
Dataset Quality Pipeline (§3) — seven deterministic stages.

    [1 Filter] → [2 Classify] → [3 Enrich] → [4 Verify] → [5 Score]
    → [6 Dedup] → [7 Provenance & Publish] → datasets

Rules (from the architecture document):
- Every stage is deterministic, independently testable, and never aborts
  the run on a single record failure (project convention: swallow + log).
- Stage 7 (publication) is optional — ``publish=False`` for pure retrieval
  previews (Phase 4 ``/agents/repository-search``).
- Controlled vocabularies come from ``app.data.vocab`` (structure-only,
  unimplemented until approved lists are provided). Empty vocab = no
  backfill; the pipeline degrades gracefully.
"""
import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import urlparse

import httpx

from app.agents.verification_agent import (
    KNOWN_DATASET_EXTENSIONS,
    KNOWN_REPOSITORY_DOMAINS,
    VerificationAgent,
    _BINARY_CONTENT_TYPES,
    _is_direct_link_by_headers,
    _is_direct_link_by_path,
    _is_non_dataset,
)
from app.config import get_settings
from app.data.vocab import (
    AGE_TERMS,
    DISEASE_TERMS,
    LICENSE_SPDX_MAP,
    MODALITY_VOCAB,
    REGION_TERMS,
    SPECIES_VOCAB,
)
from app.db.repositories.dataset_repository import (
    bulk_upsert,
    normalize_doi,
    normalize_url_key,
)
from app.ingestion.scorer import score_dataset
from app.models.dataset import Dataset, TrustTier
from app.models.query_filters import QueryFilters
from app.models.repository_dataset import RepositoryDataset

logger = logging.getLogger("neuro_platform.ingestion.quality_pipeline")

# Canonical source label for web-origin candidates (FallbackAgent/Tavily+LLM).
# Web candidates run the SAME 7-stage pipeline as repository candidates (§3.0);
# Stage 1 allows them without the repository allowlist, and Stage 4 derives
# trust from the verified destination URL + validated metadata — never from
# source_guess alone. When the verified URL resolves to a supported repository
# domain, the candidate is remapped to that repository source (§4.4 decision).
WEB_DISCOVERY_SOURCE: str = "web_search"

# §2.17 static per-source access-tier lookup (no LLM). A record-level value
# declared by the connector always wins over this default.
ACCESS_TIER_STATIC: dict[str, str] = {
    "openneuro": "open",
    "dandi": "open",
    "neurovault": "open",
    "ebrains": "registered",   # unless the record itself declares open
    "zenodo": "open",          # per-record access_right override
    "figshare": "open",
    "dryad": "open",
    "osf": "open",
    "nitrc": "open",
}

# Canonical public host per repository source (§2.1/§3.1 — used to derive
# the effective Stage-1 allowlist and for the Stage-2 "known domain" check).
SOURCE_HOSTS: dict[str, str] = {
    "openneuro": "openneuro.org",
    "dandi": "dandiarchive.org",
    "neurovault": "neurovault.org",
    "ebrains": "ebrains.eu",
    "zenodo": "zenodo.org",
    "figshare": "figshare.com",
    "dryad": "datadryad.org",
    "osf": "osf.io",
    "nitrc": "nitrc.org",
}
REPOSITORY_SOURCE_KEYS: frozenset[str] = frozenset(SOURCE_HOSTS)

# §3.2 Stage 2 classifier constants (documented lists — not vocabularies).
SOFTWARE_TERMS: frozenset[str] = frozenset(
    {"tool", "application", "source code", "software", "workflow", "toolbox", "library", "pipeline", "app"}
)

# Stabilization (Phase 3): the classifier must distinguish Dataset / Dataset
# Collection / Paper / Software / Documentation / Blog / Forum / Unknown, and
# only Dataset + Dataset Collection may proceed to persistence. Papers, docs,
# software, blogs, forums, GitHub repos, StackExchange, Neurostars, PubMed,
# Semantic Scholar, and Connected Papers can NEVER be classified as datasets.

# Domains whose content type is unambiguous (web-origin candidates and, where
# the repo-native type is missing, repository-origin candidates).
PAPER_DOMAINS: frozenset[str] = frozenset(
    {
        "pubmed.ncbi.nlm.nih.gov", "ncbi.nlm.nih.gov", "pmc.ncbi.nlm.nih.gov",
        "arxiv.org", "biorxiv.org", "medrxiv.org", "psyarxiv.com",
        "semanticscholar.org", "connectedpapers.com", "sciencedirect.com",
        "nature.com", "springer.com", "wiley.com", "frontiersin.org",
        "plos.org", "academic.oup.com", "jneurosci.org", "cell.com",
        "ieee.org", "elsevier.com", "thelancet.com", "nejm.org",
        "researchgate.net", "academia.edu", "mdpi.com", "sagepub.com",
        "tandfonline.com", "jamanetwork.com", "bmj.com", "science.org",
        "pnas.org", "elifesciences.org", "peerj.com", "f1000research.com",
    }
)
PAPER_TITLE_TERMS: tuple[str, ...] = (
    " paper", "paper ", "-paper", "article", "journal", "publication",
    "preprint", "study", "analysis of", "meta-analysis", "systematic review",
    "manuscript", "abstract", "proceedings", "chapter",
)

BLOG_DOMAINS: frozenset[str] = frozenset(
    {"medium.com", "wordpress.com", "blogger.com", "substack.com", "tumblr.com",
     "hashnode.com", "dev.to", "ghost.io", "wixsite.com", "squarespace.com"}
)

FORUM_DOMAINS: frozenset[str] = frozenset(
    {"stackexchange.com", "stackoverflow.com", "neurostars.org", "discourse.org",
     "reddit.com", "quora.com", "github.com", "gitlab.com"}
)
FORUM_TITLE_TERMS: tuple[str, ...] = (
    " forum", "forum ", "-forum", "discussion", "thread", "question",
    "answers", "issue ", "issues ", "board", "conversation", "community ",
)

DOCUMENTATION_DOMAINS: frozenset[str] = frozenset(
    {"readthedocs.io", "gitbook.io", "gitbook.com", "wikipedia.org",
     "wiktionary.org", "docs.github.com", "developer.mozilla.org",
     "learn.microsoft.com", "kaggle.com/docs", "dandiarchive.org/documentation"}
)

SOFTWARE_DOMAINS: frozenset[str] = frozenset(
    {"github.com", "gitlab.com", "bitbucket.org", "sourceforge.net", "pypi.org",
     "npmjs.com", "crates.io", "docker.com", "hub.docker.com", "anaconda.org"}
)

# Dataset landing/collection URL markers on supported repository domains.
# Stabilization (Phase 1, confirmed live 2026-08-04): OpenNeuro uses the
# plural "/datasets/" path (``/datasets/ds000001``), so both singular and
# plural forms are required — without "/datasets/", every OpenNeuro record
# fell through to "unknown" and was dropped at Stage 4.
DATASET_PATH_MARKERS: tuple[str, ...] = (
    "/dataset/",
    "/datasets/",
    "/dandiset/",
    "/records/",
    "/collections/",
    "/projects/",
)

# Markers that indicate a *collection of datasets* rather than a single one.
# (NeuroVault collections, NITRC projects, OSF category=data registrations.)
COLLECTION_PATH_MARKERS: tuple[str, ...] = ("/collections/", "/projects/")


# ────────────────────────── result / internal state ──────────────────────────


@dataclass
class StageStats:
    """Per-stage accepted/dropped counters (dropped keyed by reason)."""

    accepted: int = 0
    dropped: dict[str, int] = field(default_factory=dict)

    def drop(self, reason: str) -> None:
        self.dropped[reason] = self.dropped.get(reason, 0) + 1


@dataclass
class PipelineRunResult:
    """Returned to every caller (HTTP or cron) for dashboards (§3.9)."""

    stages: dict[str, StageStats] = field(default_factory=dict)
    datasets: list[Dataset] = field(default_factory=list)
    dedup_groups: list[dict] = field(default_factory=list)  # §3.6 loser/winner records
    errors: list[str] = field(default_factory=list)
    elapsed_ms: int = 0


@dataclass
class _Record:
    """Internal carrier — keeps RepositoryDataset pristine while tracking
    per-record pipeline state (class label, enrichment, verification)."""

    candidate: RepositoryDataset
    class_label: str = "unknown"
    reclassified: bool = False
    enrichment_sources: list[str] = field(default_factory=list)
    enrichment: dict[str, str] = field(default_factory=dict)  # source -> status
    enrichment_started_at: datetime | None = None
    enrichment_completed_at: datetime | None = None
    is_direct_link: bool = False
    last_verified_at: datetime | None = None
    dataset: Dataset | None = None
    # Origin discovery source captured at pool build time (BEFORE Stage 4 can
    # promote a web candidate to a repository source). Recorded in the Stage 7
    # provenance discovery event so "found from Web Search then promoted to
    # OpenNeuro" stays faithful to how the dataset actually entered the system.
    origin_source: str = ""


# ───────────────────────────── helpers ─────────────────────────────


def _host_of(url: str) -> str:
    """Lowercased hostname with the ``www.`` prefix normalized away, so
    ``www.nitrc.org`` and ``nitrc.org`` are treated as the same host by the
    Stage-1 allowlist, Stage-2 known-domain check, and Stage-4 trust check."""
    try:
        host = (urlparse(url).hostname or "").lower()
        return host[4:] if host.startswith("www.") else host
    except Exception:  # noqa: BLE001 — malformed URL must not abort
        return ""


def _log_stage(name: str, stats: StageStats) -> None:
    logger.info("Stage %s: accepted=%d dropped=%s", name, stats.accepted, stats.dropped)


def _effective_allowlist(settings) -> frozenset[str]:
    """§3.1 — host of each enabled source's canonical URL + KNOWN_REPOSITORY_DOMAINS."""
    hosts = {SOURCE_HOSTS[s] for s in settings.REPOSITORY_ENABLED_SOURCES if s in SOURCE_HOSTS}
    hosts |= set(KNOWN_REPOSITORY_DOMAINS)
    hosts |= {h.strip().lower() for h in settings.REPOSITORY_ALLOWLIST if h.strip()}
    return frozenset(hosts)


def _human_size_label(num_bytes: int) -> str | None:
    """§3.3 item 4 — human-readable size label (e.g. '184 GB')."""
    if not num_bytes or num_bytes <= 0:
        return None
    size = float(num_bytes)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}".replace(".0 ", " ")
        size /= 1024
    return None


def _match_vocab_labels(text: str, vocab: dict[str, list[str]]) -> list[str]:
    """§3.3 — exact (word-boundary) token match against a label→tokens vocab."""
    labels: list[str] = []
    for label, tokens in (vocab or {}).items():
        for token in tokens or []:
            t = str(token).strip().lower()
            if t and re.search(rf"\b{re.escape(t)}\b", text):
                labels.append(label)
                break
    return labels


def _match_vocab_token(text: str, terms: list[str]) -> str | None:
    """§3.3 — first exact token match against a flat term list (regions)."""
    for term in terms or []:
        t = str(term).strip().lower()
        if t and re.search(rf"\b{re.escape(t)}\b", text):
            return t
    return None


# ───────────────────────────── Stage 1 — Filter ─────────────────────────────


def _stage_filter(records: list[_Record], settings) -> tuple[list[_Record], StageStats]:
    """§3.1 — accept only curated, enabled, allowlisted sources with required fields.

    Web-origin candidates (``WEB_DISCOVERY_SOURCE``) are distinguished from
    repository-origin candidates: they are allowed through the source/allowlist
    gates (they originate outside the repository allowlist by definition) but
    still pass the blocklist and required-field checks. Trust for web candidates
    is derived in Stage 4 from the verified destination URL + validated metadata,
    not from ``source_guess``.
    """
    stats = StageStats()
    enabled = {s.lower() for s in settings.REPOSITORY_ENABLED_SOURCES}
    allowlist = _effective_allowlist(settings)
    blocklist = {h.strip().lower() for h in settings.REPOSITORY_BLOCKLIST if h.strip()}

    kept: list[_Record] = []
    for rec in records:
        c = rec.candidate
        is_web = c.source == WEB_DISCOVERY_SOURCE
        # 1) source allowed: repository-origin must be enabled; web-origin is allowed
        if not is_web and c.source not in enabled:
            stats.drop("source_disabled")
            continue
        # 2) URL host allowlist: repository-origin only (web candidates are not
        #    rejected for originating outside the repository allowlist)
        #
        # Stabilization (Phase 1, confirmed live 2026-08-04): figshare hosts
        # datasets on branded portal subdomains (karger.figshare.com,
        # frontiersin.figshare.com, …) — the exact-host check dropped them.
        # Accept any subdomain of an allowlisted canonical host.
        host = _host_of(c.url)
        host_ok = host in allowlist or any(
            host.endswith("." + h) for h in allowlist if h and "." in h
        )
        if not is_web and (not host or not host_ok):
            stats.drop("domain_not_allowlisted")
            continue
        # 3) URL host ∈ REPOSITORY_BLOCKLIST — applies to all origins
        if host in blocklist:
            stats.drop("blocklisted")
            continue
        # 4) missing required fields — applies to all origins
        if not c.source_id or not c.url or not c.title:
            stats.drop("missing_required")
            continue
        kept.append(rec)

    stats.accepted = len(kept)
    return kept, stats


# ───────────────────────────── Stage 2 — Classify ─────────────────────────────


def _repo_native_class(raw: dict) -> str | None:
    """§3.2 rule 3 — repo-native resource/defined/type declares the record class.

    Returns ``"dataset"``, ``"dataset_collection"``, ``"paper"``,
    ``"software"``, or ``None`` when the payload does not declare a type.
    """
    if not isinstance(raw, dict):
        return None
    needles: list[str] = []
    meta = raw.get("metadata")
    if isinstance(meta, dict):
        rt = meta.get("resource_type")
        if isinstance(rt, dict):
            for k in ("type", "subtype"):
                if isinstance(rt.get(k), str):
                    needles.append(rt[k].lower())
        elif isinstance(rt, str):
            needles.append(rt.lower())
    for key in ("defined_type", "defined_type_name", "type", "resource_type", "kind"):
        v = raw.get(key)
        if isinstance(v, str):
            needles.append(v.lower())
    # OSF nodes/registrations declare category in attributes (e.g. "data").
    attrs = raw.get("attributes")
    if isinstance(attrs, dict) and isinstance(attrs.get("category"), str):
        needles.append(f"category:{attrs['category'].lower()}")
    for key in ("category",):
        v = raw.get(key)
        if isinstance(v, str):
            needles.append(f"category:{v.lower()}")

    for n in needles:
        if not n:
            continue
        if "dataset collection" in n or "data collection" in n or "collection" in n and "dataset" in n:
            return "dataset_collection"
        if "category:data" in n:
            return "dataset_collection"
        if "dataset" in n:
            return "dataset"
        if any(p in n for p in ("paper", "article", "preprint", "publication", "thesis", "journal")):
            return "paper"
        if any(s in n for s in ("software", "code", "tool", "workflow", "app", "library")):
            return "software"
    return None


def _files_declare_dataset(files: list) -> bool:
    """§3.2 rule 2 — any file format/name ∈ KNOWN_DATASET_EXTENSIONS OR
    any file MIME ∈ _BINARY_CONTENT_TYPES."""
    for f in files:
        fmt = str(getattr(f, "format", "") or "").lower()
        name = str(getattr(f, "name", "") or "").lower()
        if fmt and fmt in _BINARY_CONTENT_TYPES:
            return True
        for ext in KNOWN_DATASET_EXTENSIONS:
            if (name and name.endswith(ext)) or (fmt and fmt.endswith(ext)):
                return True
    return False


def _known_domain_host(host: str) -> bool:
    return host in KNOWN_REPOSITORY_DOMAINS or host in set(SOURCE_HOSTS.values())


def _classify_content_type(rec: _Record, settings) -> str:
    """§3.2 — classify a single record into the 8-class taxonomy.

    Returns a class label: dataset | dataset_collection | software | unknown.
    (paper | blog | forum | documentation are DROPPED by the caller — only
    Dataset and Dataset Collection may proceed to persistence.)
    """
    c = rec.candidate
    title_l = (c.title or "").lower()
    host = _host_of(c.url)
    path = (urlparse(c.url).path or "").lower()
    url_l = (c.url or "").lower()

    # 1) documentation / specs are never datasets
    if _is_non_dataset(c.title, c.url):
        return "documentation"

    # 2) unambiguous content-type domains (paper / blog / forum / docs / software)
    if host in PAPER_DOMAINS:
        return "paper"
    if host in BLOG_DOMAINS or ("/blog" in path or "blog." in host or host == "blog"):
        return "blog"
    if host in FORUM_DOMAINS or any(t in title_l for t in FORUM_TITLE_TERMS):
        # GitHub/GitLab hosts are excluded only when they host a software repo;
        # a direct dataset file link on those hosts is handled by Stage 4.
        return "forum" if host not in SOFTWARE_DOMAINS else "software"
    if host in DOCUMENTATION_DOMAINS:
        return "documentation"
    if host in SOFTWARE_DOMAINS or any(term in title_l for term in SOFTWARE_TERMS):
        return "software"

    # 3) repo-native type declaration (Dataset / Dataset Collection / Paper / Software)
    native = _repo_native_class(c.raw)
    if native is not None:
        return native

    # 4) file signals → dataset
    if c.files and _files_declare_dataset(c.files):
        return "dataset"

    # 5) dataset path markers on a known domain → dataset / dataset_collection
    if _known_domain_host(host) and any(m in path for m in DATASET_PATH_MARKERS):
        return "dataset_collection" if any(m in path for m in COLLECTION_PATH_MARKERS) else "dataset"

    # 6) otherwise unknown → Stage 4 decides (direct link reclassifies)
    return "unknown"


def _stage_classify(records: list[_Record], settings) -> tuple[list[_Record], StageStats]:
    """§3.2 — deterministic classifier (no LLM).

    Only ``dataset`` and ``dataset_collection`` may proceed to persistence;
    ``paper``, ``blog``, ``forum``, ``documentation``, and ``software`` are
    dropped here. ``unknown`` survives to Stage 4 for direct-link reclassification.
    """
    stats = StageStats()
    kept: list[_Record] = []
    dropped_labels = {"paper", "blog", "forum", "documentation", "software"}

    for rec in records:
        label = _classify_content_type(rec, settings)
        if label in dropped_labels:
            stats.drop(label)
            continue
        # software is allowed through when ALLOW_SOFTWARE is set (label kept
        # for observability; it never proceeds to persistence unchanged).
        if label == "software" and settings.ALLOW_SOFTWARE:
            rec.class_label = "software"
            kept.append(rec)
            continue
        rec.class_label = label
        kept.append(rec)

    stats.accepted = len(kept)
    return kept, stats


# ───────────────────────────── Stage 3 — Enrich ─────────────────────────────


async def _crossref_backfill(client: httpx.AsyncClient, rec: _Record) -> None:
    """§3.3 item 3 — best-effort DOI backfill; never blocks/aborts."""
    doi = (rec.candidate.doi or "").strip()
    if not doi:
        return
    try:
        resp = await client.get(f"https://api.crossref.org/works/{doi}")
        if resp.status_code >= 400:
            raise RuntimeError(f"crossref returned {resp.status_code}")
        msg = (resp.json() or {}).get("message") or {}
        c = rec.candidate
        if not c.title and isinstance(msg.get("title"), list) and msg["title"]:
            c.title = str(msg["title"][0])[:500]
        if not c.description and msg.get("abstract"):
            abstract = re.sub(r"<[^>]+>", " ", str(msg["abstract"]))
            c.description = re.sub(r"\s+", " ", abstract).strip()[:2000] or None
        if not c.authors:
            for a in msg.get("author", []) or []:
                name = f"{str(a.get('given', '') or '').strip()} {str(a.get('family', '') or '').strip()}".strip()
                if name:
                    c.authors.append(name)
        if not c.license:
            lic = (msg.get("license") or [{}])[0]
            url = str(lic.get("URL", "") or "").strip().lower() if isinstance(lic, dict) else ""
            # Only resolve via the approved SPDX map (unimplemented → no-op).
            if url and url in LICENSE_SPDX_MAP:
                c.license = LICENSE_SPDX_MAP[url]
        rec.enrichment_sources.append("crossref")
        rec.enrichment["crossref"] = "ok"
    except Exception as exc:  # noqa: BLE001 — best-effort enrichment
        rec.enrichment["crossref"] = "failed"
        logger.warning("Crossref backfill failed for doi=%s: %s", doi, exc)


async def _stage_enrich(records: list[_Record], settings) -> tuple[list[_Record], StageStats]:
    """§3.3 — fill metadata gaps; each transformation is best-effort, never blocks."""
    stats = StageStats()
    if not settings.ENRICHMENT_ENABLED:
        stats.accepted = len(records)
        return records, stats

    for rec in records:
        rec.enrichment_started_at = datetime.now(timezone.utc)
        c = rec.candidate
        text = " ".join(
            [c.title or "", c.description or "", " ".join(c.keywords or []), c.doi or ""]
        ).lower()

        # 1) Access tier — static lookup (§2.17); record-level override wins.
        if not c.access_tier:
            c.access_tier = ACCESS_TIER_STATIC.get(c.source)
            if c.access_tier:
                rec.enrichment_sources.append("access_tier_static")

        # 2) License — map raw string → SPDX id via approved map only.
        #    (Map is unimplemented; connector-declared values are preserved.)
        if LICENSE_SPDX_MAP and c.license:
            mapped = LICENSE_SPDX_MAP.get(c.license.strip().lower()) or LICENSE_SPDX_MAP.get(c.license.strip())
            c.license = mapped
            if c.license:
                rec.enrichment_sources.append("license_spdx")

        # 4) Size label — from files[] sizes when present.
        total = sum(int(f.size_bytes or 0) for f in c.files if f.size_bytes)
        if total > 0:
            label = _human_size_label(total)
            if label:
                c.size_label = label
                rec.enrichment_sources.append("size_label")

        # 5) Region / age_group / disease — vocab/pattern match only (no LLM).
        if not c.region:
            region = _match_vocab_token(text, REGION_TERMS)
            if region:
                c.region = region
                rec.enrichment_sources.append("vocab_region")
        if not c.age_group:
            for label, tokens in (AGE_TERMS or {}).items():
                if _match_vocab_token(text, tokens):
                    c.age_group = label
                    rec.enrichment_sources.append("vocab_age")
                    break
        if not c.disease:
            for label, tokens in (DISEASE_TERMS or {}).items():
                if _match_vocab_token(text, tokens):
                    c.disease = label
                    rec.enrichment_sources.append("vocab_disease")
                    break

        # 6) Modality / species fill — exact vocab match only when empty.
        if not c.modality:
            c.modality = _match_vocab_labels(text, MODALITY_VOCAB)
            if c.modality:
                rec.enrichment_sources.append("vocab_modality")
        if not c.species:
            c.species = _match_vocab_labels(text, SPECIES_VOCAB)
            if c.species:
                rec.enrichment_sources.append("vocab_species")

        rec.enrichment_completed_at = datetime.now(timezone.utc)

    # Crossref backfill — bounded timeout + concurrency, parallel, best-effort.
    with_doi = [r for r in records if (r.candidate.doi or "").strip()]
    if with_doi:
        async with httpx.AsyncClient(
            timeout=settings.CROSSREF_TIMEOUT_MS / 1000.0, follow_redirects=True
        ) as client:
            sem = asyncio.Semaphore(max(1, int(getattr(settings, "MAX_CONCURRENT_CHECKS", 10))))

            async def bounded(rec: _Record) -> None:
                async with sem:
                    await _crossref_backfill(client, rec)

            await asyncio.gather(*[bounded(r) for r in with_doi], return_exceptions=True)

    stats.accepted = len(records)
    return records, stats


# ───────────────────────────── Stage 4 — Verify ─────────────────────────────


# §3.4 — supported-repository URL → (source, source_id) extractors. Used to
# promote a verified web candidate whose destination URL belongs to one of the
# nine supported repositories into a repository dataset. When no pattern
# matches, the SHA-1 source_id derived at discovery is kept (persistence-level
# canonical merge still unifies the record).
_REPO_URL_ID_PATTERNS: tuple[tuple[str, re.Pattern, Callable[[re.Match], str]], ...] = (
    ("openneuro", re.compile(r"^https?://(?:www\.)?openneuro\.org/datasets/([^/?#]+)"), lambda m: m.group(1)),
    ("dandi", re.compile(r"^https?://(?:www\.)?dandiarchive\.org/dandiset/(\d+)"), lambda m: f"DANDI:{int(m.group(1)):06d}"),
    ("neurovault", re.compile(r"^https?://(?:www\.)?neurovault\.org/collections/(\d+)"), lambda m: m.group(1)),
    ("ebrains", re.compile(r"^https?://search\.kg\.ebrains\.eu/instances/([^/?#]+)"), lambda m: m.group(1)),
    ("zenodo", re.compile(r"^https?://(?:www\.)?zenodo\.org/records/(\d+)"), lambda m: m.group(1)),
    ("figshare", re.compile(r"^https?://(?:www\.)?figshare\.com/articles/(?:dataset/)?(?:[^/?#]+/)?([^/?#]+)/?$"), lambda m: m.group(1)),
    ("dryad", re.compile(r"^https?://(?:www\.)?datadryad\.org/stash/dataset/([^?#]+)"), lambda m: m.group(1).rstrip("/").split("/")[-1]),
    ("osf", re.compile(r"^https?://osf\.io/([^/?#]+)"), lambda m: m.group(1)),
    ("nitrc", re.compile(r"^https?://(?:www\.)?nitrc\.org/projects/([^/?#]+)"), lambda m: m.group(1)),
)


def _repo_identity_from_url(url: str) -> tuple[str, str] | None:
    """
    Derive a supported repository (source, source_id) from a verified URL.

    Returns ``(source, native_id)`` when the URL encodes a supported
    repository dataset id, ``(source, "")`` when the host is a supported
    repository but no native id can be extracted (keep the SHA-1 id), or
    ``None`` when the URL is not a supported repository dataset URL.
    """
    if not url:
        return None
    for source, pattern, idfn in _REPO_URL_ID_PATTERNS:
        m = pattern.match(url)
        if m:
            return source, idfn(m)
    host = _host_of(url)
    for source, canonical in SOURCE_HOSTS.items():
        if host == canonical or host.endswith("." + canonical):
            return source, ""
    return None


async def _stage_verify(records: list[_Record], settings) -> tuple[list[_Record], StageStats]:
    """§3.4 — parallel link check (reuse VerificationAgent), trust + direct-link.

    Trust is derived from the verified destination URL + validated metadata:
    web-origin candidates are never trusted because of ``source_guess``.
    """
    stats = StageStats()
    agent = VerificationAgent()
    sem = asyncio.Semaphore(max(1, int(getattr(settings, "MAX_CONCURRENT_CHECKS", 10))))
    kept: list[_Record] = []
    try:
        async def check(rec: _Record):
            async with sem:
                return await agent._check_link(rec.candidate.url)  # noqa: SLF001

        results = await asyncio.gather(*[check(r) for r in records], return_exceptions=True)

        for rec, result in zip(records, results):
            if isinstance(result, Exception):
                stats.drop("dead_link")
                logger.info("Stage 4 link check raised for %s: %s", rec.candidate.url, result)
                continue
            is_live, _, response = result
            if not is_live:
                stats.drop("dead_link")
                logger.info("Stage 4 dead link: %s", rec.candidate.url)
                continue

            # Normalize redirect target into the canonical url (Stage 6 key).
            if response is not None and getattr(response, "url", None) is not None:
                final = str(response.url)
                if final and final != rec.candidate.url:
                    rec.candidate.url = final

            direct = _is_direct_link_by_path(rec.candidate.url) or (
                response is not None and _is_direct_link_by_headers(response)
            )
            rec.is_direct_link = direct
            rec.last_verified_at = datetime.now(timezone.utc)

            # Web-origin candidates whose verified destination URL belongs to a
            # supported repository domain become repository datasets (approved
            # decision). Runs before the trust check below, so the remapped
            # record is treated as a known repository source.
            if rec.candidate.source == WEB_DISCOVERY_SOURCE:
                repo_identity = _repo_identity_from_url(rec.candidate.url)
                if repo_identity is not None:
                    repo_source, native_id = repo_identity
                    rec.candidate.source = repo_source
                    if native_id:
                        rec.candidate.source_id = native_id
                    rec.reclassified = True
                    # Promoted: the supported repository itself guarantees the
                    # record is a dataset (rule 3, §3.4) — no path-marker or
                    # direct-link heuristic needed.
                    rec.class_label = "dataset"
                    logger.info(
                        "Stage 4 remapped web candidate to repository source=%s source_id=%s url=%s",
                        rec.candidate.source, rec.candidate.source_id, rec.candidate.url,
                    )

            # Trust from the verified (post-redirect) destination URL:
            # repository sources are allowlisted / known domains — the
            # repository itself guarantees the dataset.
            final_domain = _host_of(rec.candidate.url)
            known = final_domain in KNOWN_REPOSITORY_DOMAINS or rec.candidate.source in REPOSITORY_SOURCE_KEYS
            if not direct and not known:
                stats.drop("unverifiable")
                logger.info("Stage 4 unverifiable landing page: %s", rec.candidate.url)
                continue

            # §3.2 transformation: unknown → direct data link reclassifies.
            if rec.class_label == "unknown" and direct:
                rec.class_label = "dataset"
                rec.reclassified = True
                logger.info("Stage 4 reclassified unknown candidate as dataset: %s", rec.candidate.url)
            if rec.class_label == "unknown":
                stats.drop("unclassifiable")
                continue

            kept.append(rec)
    finally:
        if agent._owns_client:  # noqa: SLF001
            await agent._client.aclose()  # noqa: SLF001

    stats.accepted = len(kept)
    return kept, stats


# ───────────────────────────── Stage 5 — Score ─────────────────────────────


def _to_dataset(rec: _Record) -> Dataset | None:
    """Convert the enriched/verified RepositoryDataset to the common Dataset.

    P4-1 (§4.9): the six optional display fields (``region``, ``age_group``,
    ``disease``, ``access_tier``, ``doi``, ``size_label``) are now fields on the
    common ``Dataset`` model and are carried through from the enriched
    ``RepositoryDataset`` at publication — closing the §1.6 schema drift with the
    Node-side ``dataset.model.js``. Extended scoring (§3.5) reads them from the
    candidate record, and Stage 7 additionally keeps the enrichment trail in
    ``provenance``.
    """
    c = rec.candidate
    now = datetime.now(timezone.utc)
    try:
        return Dataset(
            title=c.title,
            description=c.description,
            source=c.source,
            source_id=c.source_id,
            url=c.url,
            modality=c.modality,
            species=c.species,
            subject_count=c.subject_count,
            keywords=c.keywords,
            license=c.license,
            # P4-1 (§4.9): carry the Stage-3 enriched display fields into the
            # common Dataset so they persist at publication (schema drift closed).
            region=c.region,
            age_group=c.age_group,
            disease=c.disease,
            access_tier=c.access_tier,
            doi=c.doi,
            size_label=c.size_label,
            is_direct_link=rec.is_direct_link,
            trust_tier=TrustTier.VERIFIED,
            last_verified_at=rec.last_verified_at or now,
            updated_at=c.updated_at or now,
        )
    except Exception as exc:  # noqa: BLE001 — invalid URL / malformed record
        logger.info("Stage 5 conversion failed for %s: %s", c.url, exc)
        return None


def _extended_quality_bonus(rec: _Record, cap: float) -> float:
    """§3.5 optional scheme — +0.04 per populated enriched field (cap +0.15)."""
    c = rec.candidate
    filled = sum(
        1
        for v in (c.region, c.age_group, c.disease, c.doi, c.access_tier, c.size_label)
        if v
    )
    return round(min(float(cap), 0.04 * filled), 4)


def _stage_score(records: list[_Record], settings) -> tuple[list[_Record], StageStats]:
    """§3.5 — quality_score (0–1) via the shared scorer + optional extended bonus."""
    stats = StageStats()
    kept: list[_Record] = []
    scores: list[float] = []

    for rec in records:
        ds = _to_dataset(rec)
        if ds is None:
            stats.drop("invalid_record")
            continue
        quality = score_dataset(ds)
        if settings.SCORING_SCHEME == "extended":
            quality = round(
                min(1.0, quality + _extended_quality_bonus(rec, settings.ENRICH_QUALITY_BONUS_CAP)),
                4,
            )
        ds.quality_score = quality
        rec.dataset = ds
        scores.append(quality)
        kept.append(rec)

    stats.accepted = len(kept)
    if scores:
        logger.info(
            "Stage 5 score: mean=%.4f min=%.4f max=%.4f n=%d",
            sum(scores) / len(scores),
            min(scores),
            max(scores),
            len(scores),
        )
    return kept, stats


# ───────────────────────────── Stage 6 — Dedup ─────────────────────────────


def _title_tokens(title: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", (title or "").lower()))


def _jaccard(a: set[str], b: set[str]) -> float:
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def _tld_family(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    parts = [p for p in host.split(".") if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def _identity_keys(rec: _Record, settings) -> dict[str, str]:
    """§3.6 identity keys in priority order: source_id, normalized URL, DOI."""
    keys: dict[str, str] = {}
    c = rec.candidate
    keys["source_id"] = f"{c.source}:{c.source_id}"
    if settings.DEDUP_URL_NORMALIZE and c.url:
        keys["url"] = normalize_url_key(c.url)
    if settings.DEDUP_DOI and c.doi:
        d = normalize_doi(c.doi)
        if d:
            keys["doi"] = d
    return keys


_TRUST_RANK = {"verified": 3, "unverified": 2, "stale": 1}


def _winner_sort_key(rec: _Record) -> tuple:
    """Deterministic winner: trust → quality → repository-source → newer updated_at."""
    ds = rec.dataset
    trust = _TRUST_RANK.get(ds.trust_tier.value if ds and ds.trust_tier else "unverified", 0)
    quality = ds.quality_score if ds and ds.quality_score is not None else 0.0
    repo_pref = 1 if rec.candidate.source in REPOSITORY_SOURCE_KEYS else 0
    updated = rec.candidate.updated_at or datetime.min.replace(tzinfo=timezone.utc)
    return (-trust, -quality, -repo_pref, updated)


def _title_matches(a: _Record, b: _Record, settings) -> bool:
    """§3.6 key 4 — Jaccard ≥ DEDUP_TITLE_SIM AND (same TLD family OR same source)."""
    if not settings.DEDUP_TITLE_SIM:
        return False
    ta, tb = _title_tokens(a.candidate.title), _title_tokens(b.candidate.title)
    if not ta or not tb:
        return False
    if _jaccard(ta, tb) < settings.DEDUP_TITLE_SIM:
        return False
    family_same = _tld_family(a.candidate.url) == _tld_family(b.candidate.url)
    return family_same or a.candidate.source == b.candidate.source


def _stage_dedup(records: list[_Record], settings) -> tuple[list[_Record], StageStats, list[dict]]:
    """§3.6 — remove duplicates within the pool (winner selection deterministic)."""
    stats = StageStats()
    ordered = sorted(records, key=_winner_sort_key)
    accepted: list[_Record] = []
    groups: list[dict] = []
    key_index: dict[str, _Record] = {}

    for rec in ordered:
        keys = _identity_keys(rec, settings)
        winner: _Record | None = None
        match_type = ""
        for ktype, key in keys.items():
            owner = key_index.get(key)
            if owner is not None:
                winner, match_type = owner, ktype
                break
        if winner is None:
            for other in accepted:
                if _title_matches(rec, other, settings):
                    winner, match_type = other, "title"
                    break

        if winner is not None:
            # `ordered` is best-first, so a later record can never beat its
            # already-accepted winner — selection is deterministic.
            stats.drop(match_type)
            groups.append(
                {
                    "winner": f"{winner.candidate.source}:{winner.candidate.source_id}",
                    "loser": f"{rec.candidate.source}:{rec.candidate.source_id}",
                    "key": match_type,
                    "winner_url": winner.candidate.url,
                    "loser_url": rec.candidate.url,
                }
            )
            logger.info(
                "Stage 6 dedup: %s:%s merged into %s:%s via key=%s",
                rec.candidate.source,
                rec.candidate.source_id,
                winner.candidate.source,
                winner.candidate.source_id,
                match_type,
            )
            continue

        accepted.append(rec)
        for ktype, key in keys.items():
            key_index.setdefault(key, rec)

    stats.accepted = len(accepted)
    return accepted, stats, groups


# ───────────────────────────── Stage 7 — Publish ─────────────────────────────


def _build_provenance(rec: _Record, settings, harvest_query: str, discovery_method: str) -> dict:
    """§3.7 — full provenance record (answers where/when/which query/version).

    Latest-snapshot fields (source_repository, source_api, harvest_query,
    harvested_at, pipeline_version, enrichment_sources, dedup_key, enrichment)
    are preserved for backward compatibility, PLUS an append-only
    ``discovery_history`` recording THIS discovery event (origin source,
    discovery method, source API, harvest query, harvested timestamp, pipeline
    version) and top-level ``first_seen_at`` / ``last_seen_at`` /
    ``discovery_count`` for cheap querying. The persistence layer merges
    history across repeated discoveries (FIFO-capped) rather than overwriting.
    """
    now = datetime.now(timezone.utc)
    enrichment = {
        "sources": sorted(set(rec.enrichment_sources)),
        "started_at": rec.enrichment_started_at.isoformat() if rec.enrichment_started_at else None,
        "completed_at": rec.enrichment_completed_at.isoformat() if rec.enrichment_completed_at else None,
    }
    if rec.enrichment:
        enrichment["status"] = dict(rec.enrichment)
    source = rec.candidate.source
    source_api = _host_of(rec.candidate.url)
    event = {
        "source": rec.origin_source or source,
        "discovery_method": discovery_method,
        "source_api": source_api,
        "harvest_query": harvest_query,
        "harvested_at": now.isoformat(),
        "pipeline_version": settings.PIPELINE_VERSION,
    }
    return {
        "source_repository": source,
        "source_api": source_api,
        "harvest_query": harvest_query,
        "harvested_at": now.isoformat(),
        "pipeline_version": settings.PIPELINE_VERSION,
        "enrichment_sources": sorted(set(rec.enrichment_sources)),
        "dedup_key": f"{source}:{rec.candidate.source_id}",
        "enrichment": enrichment,
        "first_seen_at": now.isoformat(),
        "last_seen_at": now.isoformat(),
        "discovery_count": 1,
        "discovery_history": [event],
    }


async def _stage_publish(
    records: list[_Record], settings, harvest_query: str, discovery_method: str
) -> tuple[list[Dataset], StageStats, list[str]]:
    """§3.7 — attach provenance, drop raw, atomic bulk upsert on (source, source_id)."""
    stats = StageStats()
    errors: list[str] = []
    datasets: list[Dataset] = []

    for rec in records:
        if rec.dataset is None:
            continue
        rec.dataset.provenance = _build_provenance(rec, settings, harvest_query, discovery_method)
        datasets.append(rec.dataset)

    if not datasets:
        stats.accepted = 0
        return datasets, stats, errors

    try:
        written = await bulk_upsert(datasets)  # atomic bulkWrite, chunk 100, (source, source_id)
        stats.accepted = written
        if written < len(datasets):
            errors.append(f"publish: {len(datasets) - written} records failed to write")
    except Exception as exc:  # noqa: BLE001 — total write failure → caller decides (502)
        errors.append(f"publish failed: {exc}")
        stats.dropped["publish_failure"] = len(datasets)
        logger.error("Stage 7 bulk publish failed: %s", exc)

    return datasets, stats, errors


# ───────────────────────────── orchestrator ─────────────────────────────


async def run_quality_pipeline(
    candidates: list[RepositoryDataset],
    filters: QueryFilters | None = None,
    *,
    publish: bool = True,  # Stage 7 (False for pure retrieval previews)
    discovery_method: str | None = None,  # batch_sync | repository_search | web_search
) -> PipelineRunResult:
    """
    Run the 7-stage quality pipeline over an aggregated candidate pool.

    Every stage is wrapped so an unexpected crash is logged and recorded
    without aborting the remaining records or the run itself.

    ``discovery_method`` labels the provenance discovery event (Stage 7):
    ``batch_sync`` (repository sync/cron), ``repository_search`` (online
    repository retrieval), or ``web_search`` (fallback web discovery). When
    omitted it is inferred from the pool: ``web_search`` if any candidate
    originated from web discovery, else ``repository_search``.
    """
    start = time.monotonic()
    settings = get_settings()
    result = PipelineRunResult()
    harvest_query = filters.raw_query if filters else ""

    if discovery_method is None:
        discovery_method = (
            "web_search"
            if any(c.source == WEB_DISCOVERY_SOURCE for c in candidates)
            else "repository_search"
        )

    records: list[_Record] = [
        _Record(candidate=c, origin_source=c.source)
        for c in candidates
        if isinstance(c, RepositoryDataset)
    ]

    async def safe_stage(
        name: str, fn, current: list[_Record]
    ) -> tuple[list[_Record], StageStats, list[dict]]:
        """Run a stage (sync or coroutine fn), never raising; returns 3-tuple."""
        try:
            out = fn()
            if asyncio.iscoroutine(out):
                out = await out
            if isinstance(out, tuple) and len(out) == 3:
                return out
            return out[0], out[1], []
        except Exception as exc:  # noqa: BLE001 — stage crash never aborts the run
            result.errors.append(f"stage {name}: {exc}")
            logger.error("Stage %s crashed: %s", name, exc, exc_info=True)
            return current, StageStats(accepted=len(current)), []

    # Stage 1 — Filter
    if records:
        records, st, _ = await safe_stage("filter", lambda: _stage_filter(records, settings), records)
        result.stages["filter"] = st
        _log_stage("filter", st)
    # Stage 2 — Classify
    if records:
        records, st, _ = await safe_stage("classify", lambda: _stage_classify(records, settings), records)
        result.stages["classify"] = st
        _log_stage("classify", st)
    # Stage 3 — Enrich
    if records:
        records, st, _ = await safe_stage("enrich", lambda: _stage_enrich(records, settings), records)
        result.stages["enrich"] = st
        _log_stage("enrich", st)
    # Stage 4 — Verify
    if records:
        records, st, _ = await safe_stage("verify", lambda: _stage_verify(records, settings), records)
        result.stages["verify"] = st
        _log_stage("verify", st)
    # Stage 5 — Score
    if records:
        records, st, _ = await safe_stage("score", lambda: _stage_score(records, settings), records)
        result.stages["score"] = st
        _log_stage("score", st)
    # Stage 6 — Dedup
    if records:
        records, st, groups = await safe_stage("dedup", lambda: _stage_dedup(records, settings), records)
        result.stages["dedup"] = st
        result.dedup_groups.extend(groups)
        _log_stage("dedup", st)

    result.datasets = [r.dataset for r in records if r.dataset is not None]

    # Stage 7 — Provenance & Publish (optional)
    if publish and records:
        try:
            _, st, pub_errors = await _stage_publish(records, settings, harvest_query, discovery_method)
            result.stages["publish"] = st
            result.errors.extend(pub_errors)
            _log_stage("publish", st)
        except Exception as exc:  # noqa: BLE001 — stage crash never aborts the run
            result.errors.append(f"stage publish: {exc}")
            logger.error("Stage 7 crashed: %s", exc, exc_info=True)

    result.elapsed_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "quality_pipeline done: stages=%s datasets=%d errors=%d elapsed_ms=%d",
        list(result.stages),
        len(result.datasets),
        len(result.errors),
        result.elapsed_ms,
    )
    return result
