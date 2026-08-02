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
from app.db.repositories.dataset_repository import bulk_upsert
from app.ingestion.scorer import score_dataset
from app.models.dataset import Dataset, TrustTier
from app.models.query_filters import QueryFilters
from app.models.repository_dataset import RepositoryDataset

logger = logging.getLogger("neuro_platform.ingestion.quality_pipeline")

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
    {"tool", "application", "source code", "software", "workflow"}
)
DATASET_PATH_MARKERS: tuple[str, ...] = (
    "/dataset/",
    "/dandiset/",
    "/records/",
    "/collections/",
    "/projects/",
)


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


# ───────────────────────────── helpers ─────────────────────────────


def _host_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
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
    """§3.1 — accept only curated, enabled, allowlisted sources with required fields."""
    stats = StageStats()
    enabled = {s.lower() for s in settings.REPOSITORY_ENABLED_SOURCES}
    allowlist = _effective_allowlist(settings)
    blocklist = {h.strip().lower() for h in settings.REPOSITORY_BLOCKLIST if h.strip()}

    kept: list[_Record] = []
    for rec in records:
        c = rec.candidate
        # 1) source ∈ REPOSITORY_ENABLED_SOURCES
        if c.source not in enabled:
            stats.drop("source_disabled")
            continue
        # 2) URL host ∈ REPOSITORY_ALLOWLIST
        host = _host_of(c.url)
        if not host or host not in allowlist:
            stats.drop("domain_not_allowlisted")
            continue
        # 3) URL host ∈ REPOSITORY_BLOCKLIST
        if host in blocklist:
            stats.drop("blocklisted")
            continue
        # 4) missing required fields
        if not c.source_id or not c.url or not c.title:
            stats.drop("missing_required")
            continue
        kept.append(rec)

    stats.accepted = len(kept)
    return kept, stats


# ───────────────────────────── Stage 2 — Classify ─────────────────────────────


def _repo_declares_dataset(raw: dict) -> bool:
    """§3.2 rule 3 — repo-native resource/defined/type says 'dataset'."""
    if not isinstance(raw, dict):
        return False
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
    for key in ("defined_type", "type", "resource_type", "kind"):
        v = raw.get(key)
        if isinstance(v, str):
            needles.append(v.lower())
    return any("dataset" in n for n in needles if n)


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


def _stage_classify(records: list[_Record], settings) -> tuple[list[_Record], StageStats]:
    """§3.2 — deterministic classifier (no LLM). 'unknown' survives to Stage 4."""
    stats = StageStats()
    kept: list[_Record] = []

    for rec in records:
        c = rec.candidate
        # 1) docs/specs are never datasets
        if _is_non_dataset(c.title, c.url):
            stats.drop("documentation")
            continue
        # 2) file signals → dataset
        if c.files and _files_declare_dataset(c.files):
            rec.class_label = "dataset"
            kept.append(rec)
            continue
        # 3) repo-native type declaration → dataset
        if _repo_declares_dataset(c.raw):
            rec.class_label = "dataset"
            kept.append(rec)
            continue
        # 4) software terms → drop unless ALLOW_SOFTWARE
        if any(term in c.title.lower() for term in SOFTWARE_TERMS):
            if settings.ALLOW_SOFTWARE:
                rec.class_label = "software"
                kept.append(rec)
            else:
                stats.drop("software")
            continue
        # 5) dataset path markers on a known domain → dataset
        host = _host_of(c.url)
        path = (urlparse(c.url).path or "").lower()
        if _known_domain_host(host) and any(m in path for m in DATASET_PATH_MARKERS):
            rec.class_label = "dataset"
            kept.append(rec)
            continue
        # 6) otherwise unknown → Stage 4 decides (direct link reclassifies)
        rec.class_label = "unknown"
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


async def _stage_verify(records: list[_Record], settings) -> tuple[list[_Record], StageStats]:
    """§3.4 — parallel link check (reuse VerificationAgent), trust + direct-link."""
    stats = StageStats()
    agent = VerificationAgent()
    sem = asyncio.Semaphore(max(1, int(getattr(settings, "MAX_CONCURRENT_CHECKS", 10))))
    kept: list[_Record] = []
    try:
        async def check(rec: _Record):
            async with sem:
                return rec, await agent._check_link(rec.candidate.url)  # noqa: SLF001

        results = await asyncio.gather(*[check(r) for r in records], return_exceptions=True)

        for rec, result in zip(records, results):
            if isinstance(result, Exception):
                stats.drop("dead_link")
                logger.info("Stage 4 link check raised for %s: %s", rec.candidate.url, result)
                continue
            is_live, domain, response = result
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

            # Rule 3: repository sources are already allowlisted — the
            # repository itself guarantees the dataset (known domain).
            known = domain in KNOWN_REPOSITORY_DOMAINS or rec.candidate.source in REPOSITORY_SOURCE_KEYS
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


def _normalize_url_key(url: str) -> str:
    """§3.6 key 2 — lowercase host, http/https merged, strip www., trailing /, query, fragment."""
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        path = (p.path or "").rstrip("/")
        return f"http://{host}{path}"
    except Exception:  # noqa: BLE001
        return (url or "").lower().strip().rstrip("/")


def _normalize_doi(doi: str | None) -> str | None:
    """§3.6 key 3 — case-insensitive, strip common DOI prefixes."""
    if not doi:
        return None
    d = doi.strip().lower()
    for prefix in (
        "https://doi.org/",
        "http://doi.org/",
        "https://dx.doi.org/",
        "http://dx.doi.org/",
        "doi:",
    ):
        if d.startswith(prefix):
            d = d[len(prefix):]
            break
    return d or None


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
        keys["url"] = _normalize_url_key(c.url)
    if settings.DEDUP_DOI and c.doi:
        d = _normalize_doi(c.doi)
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


def _build_provenance(rec: _Record, settings, harvest_query: str) -> dict:
    """§3.7 — full provenance record (answers where/when/which query/version)."""
    now = datetime.now(timezone.utc)
    enrichment = {
        "sources": sorted(set(rec.enrichment_sources)),
        "started_at": rec.enrichment_started_at.isoformat() if rec.enrichment_started_at else None,
        "completed_at": rec.enrichment_completed_at.isoformat() if rec.enrichment_completed_at else None,
    }
    if rec.enrichment:
        enrichment["status"] = dict(rec.enrichment)
    return {
        "source_repository": rec.candidate.source,
        "source_api": _host_of(rec.candidate.url),
        "harvest_query": harvest_query,
        "harvested_at": now.isoformat(),
        "pipeline_version": settings.PIPELINE_VERSION,
        "enrichment_sources": sorted(set(rec.enrichment_sources)),
        "dedup_key": f"{rec.candidate.source}:{rec.candidate.source_id}",
        "enrichment": enrichment,
    }


async def _stage_publish(
    records: list[_Record], settings, harvest_query: str
) -> tuple[list[Dataset], StageStats, list[str]]:
    """§3.7 — attach provenance, drop raw, atomic bulk upsert on (source, source_id)."""
    stats = StageStats()
    errors: list[str] = []
    datasets: list[Dataset] = []

    for rec in records:
        if rec.dataset is None:
            continue
        rec.dataset.provenance = _build_provenance(rec, settings, harvest_query)
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
) -> PipelineRunResult:
    """
    Run the 7-stage quality pipeline over an aggregated candidate pool.

    Every stage is wrapped so an unexpected crash is logged and recorded
    without aborting the remaining records or the run itself.
    """
    start = time.monotonic()
    settings = get_settings()
    result = PipelineRunResult()
    harvest_query = filters.raw_query if filters else ""

    records: list[_Record] = [
        _Record(candidate=c) for c in candidates if isinstance(c, RepositoryDataset)
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
            _, st, pub_errors = await _stage_publish(records, settings, harvest_query)
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
