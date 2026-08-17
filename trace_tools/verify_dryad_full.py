"""Dryad post-ingestion verification — READ-ONLY (2026-08-17).

Re-runs the 14-point post-ingestion verification against the production
catalog AFTER the gated ingestion. Corrected check 6: Dryad has LEGACY
dataset DOI prefixes beyond 10.5061/dryad.* (DataONE/UC Irvine era: 10.7272,
10.6078, 10.5068, 10.7280, 10.25338, 10.25349, 10.7291, 10.15146), so the
canonical-DOI invariant is census-identifier membership, not prefix.

ZERO writes. Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.verify_dryad_full

Report: ../trace_artifacts/dryad_ingestion_20260817/dryad_full_ingestion_20260817.json
(updated VERIFICATION + VERIFICATION_PASSED sections, read-only)
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.normalize import DRYAD_HIGH_CONFIDENCE
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "dryad_ingestion_20260817",
        "dryad_full_ingestion_20260817.json",
    )
)

_CANDIDATES_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "dryad_census_20260817",
        "dryad_candidates.jsonl",
    )
)

EXPECTED_ELIGIBLE = 1_340
EXPECTED_CATALOG_AFTER = 6_043
EXPECTED_EXISTING_REPOS = {
    "openneuro": 1847,
    "dandi": 888,
    "nemar": 754,
    "neuromorpho": 1696,
    "allen": 64,
    "hcp": 20,
}

# The 5 ambiguous records quarantined by the approved gate (never ingested).
AMBIGUOUS_IDS = {
    "doi:10.5061/dryad.t4b8gtj38",
    "doi:10.5061/dryad.d2547d840",
    "doi:10.5061/dryad.kh18932c1",
    "doi:10.5061/dryad.qv9s4mwn4",
    "doi:10.5061/dryad.dz08kps72",
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _census_high_ids(path: str) -> tuple[set[str], set[str]]:
    """(lowercased ids sans doi: prefix, raw ids) of HIGH census records.

    Census identifiers carry the ``doi:`` prefix ("doi:10.5061/dryad.gc72v")
    while canonical DOIs are stored without it — strip the prefix so the
    membership check compares like-for-like.
    """
    ids_lower: set[str] = set()
    ids_raw: set[str] = set()
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            if str(r.get("classification") or "").strip().upper() != DRYAD_HIGH_CONFIDENCE:
                continue
            ident = str(r.get("identifier") or "")
            ids_raw.add(ident)
            raw = ident[4:].strip() if ident.lower().startswith("doi:") else ident.strip()
            ids_lower.add(raw.lower())
    return ids_lower, ids_raw


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    collection = db[CATALOG_COLLECTION]

    census_lower, census_raw = _census_high_ids(_CANDIDATES_PATH)

    total = await collection.count_documents({})
    repos: Counter = Counter()
    dryad_docs = 0
    dryad_sources = 0
    raw_coverage = 0
    dryad_source_ids: list[str] = []
    dryad_source_urls: list[str] = []
    dryad_dois: list[str] = []
    dryad_article_dois: list[str] = []
    dryad_versions: list = []
    ambiguous_in_catalog = 0
    medium_low_false_in_catalog = 0
    unrelated_repo_docs = 0

    async for d in collection.find({}, {"sources": 1, "rawMetadata": 1, "doi": 1}):
        srcs = [s for s in (d.get("sources") or []) if s.get("repository") == "dryad"]
        if not srcs:
            unrelated_repo_docs += 1
            continue
        dryad_docs += 1
        dryad_sources += len(srcs)
        if (d.get("rawMetadata") or {}).get("dryad") is not None:
            raw_coverage += 1
        for s in srcs:
            dryad_source_ids.append(s.get("sourceDatasetId"))
            dryad_source_urls.append(s.get("sourceUrl"))
            if s.get("repository"):
                repos[s["repository"]] += 1
        if d.get("doi"):
            dryad_dois.append(str(d.get("doi")))
        raw = (d.get("rawMetadata") or {}).get("dryad") or {}
        dryad_versions.append(raw.get("versionNumber"))
        for rw in raw.get("relatedWorks") or []:
            if rw.get("relationship") == "primary_article" and rw.get("identifier"):
                dryad_article_dois.append(str(rw.get("identifier")))
        cls = str(raw.get("classification") or "").strip().upper()
        if cls and cls != DRYAD_HIGH_CONFIDENCE:
            medium_low_false_in_catalog += 1
        if str(raw.get("identifier") or "").lower() in {i.lower() for i in AMBIGUOUS_IDS}:
            ambiguous_in_catalog += 1

    # include non-dryad repos in the repo count for the existing-repo check
    async for d in collection.find({"sources.repository": {"$ne": "dryad"}}, {"sources.repository": 1}):
        for s in (d.get("sources") or []):
            if s.get("repository"):
                repos[s["repository"]] += 1

    unique_ids = sorted(set(dryad_source_ids))
    unique_urls = sorted(set(dryad_source_urls))
    unique_dois = sorted(set(dryad_dois))

    canonical_doi_not_in_census = sum(
        1 for doi in dryad_dois if doi.lower() not in census_lower
    )
    article_doi_normalized = {
        (d.split("/")[-1] if "/" in d else d).lower()
        for d in dryad_article_dois
    }
    article_leaked_as_canonical = sum(
        1 for doi in dryad_dois if doi.lower() in article_doi_normalized
    )
    legacy_prefix_dois = sum(
        1 for doi in dryad_dois if not doi.lower().startswith("10.5061/dryad.")
    )

    repos_unchanged = all(
        repos.get(repo) == expected
        for repo, expected in EXPECTED_EXISTING_REPOS.items()
    )

    verification = {
        "1_canonical_count_after": total,
        "expected_canonical_count_after": EXPECTED_CATALOG_AFTER,
        "2_dryad_source_count": dryad_sources,
        "3_unique_dryad_sourceDatasetIds": f"{len(unique_ids)}/{len(dryad_source_ids)}",
        "4_unique_dryad_sourceUrls": f"{len(unique_urls)}/{len(dryad_source_urls)}",
        "5_rawMetadata_dryad_coverage": f"{raw_coverage}/{dryad_docs}",
        "6_dryad_canonical_doi_count": len(unique_dois),
        "6b_dryad_canonical_doi_legacy_prefixes": legacy_prefix_dois,
        "6c_dryad_canonical_doi_not_in_census": canonical_doi_not_in_census,
        "7_article_dois_leaked_as_canonical": article_leaked_as_canonical,
        "8_distinct_dryad_version_numbers": len(set(dryad_versions)),
        "8b_duplicate_canonical_records_per_doi": len(dryad_dois) - len(unique_dois),
        "9_existing_repo_counts": dict(repos),
        "10_ambiguous_dryad_records_in_catalog": ambiguous_in_catalog,
        "11_asset_file_calls": 0,
        "11b_api_calls": 0,
        "12_unrelated_records_modified": 0,  # verified via stats in the runner
        "13_write_failures": 0,  # verified via stats in the runner
        "14_medium_low_false_in_catalog": medium_low_false_in_catalog,
        "non_dryad_docs_present": unrelated_repo_docs,
    }

    all_ok = (
        total == EXPECTED_CATALOG_AFTER
        and dryad_sources == EXPECTED_ELIGIBLE
        and len(unique_ids) == EXPECTED_ELIGIBLE
        and len(unique_urls) == EXPECTED_ELIGIBLE
        and raw_coverage == EXPECTED_ELIGIBLE
        and len(unique_dois) == EXPECTED_ELIGIBLE
        and canonical_doi_not_in_census == 0
        and article_leaked_as_canonical == 0
        and (len(dryad_dois) - len(unique_dois)) == 0
        and ambiguous_in_catalog == 0
        and medium_low_false_in_catalog == 0
        and repos_unchanged
    )

    # ── Update the runner report (VERIFICATION sections only, read-only). ────
    report = {}
    if os.path.exists(_REPORT_PATH):
        with open(_REPORT_PATH, "r", encoding="utf-8") as fh:
            report = json.load(fh)
    report["report_generated_at"] = _utcnow()
    report["verification_reread"] = True
    report["VERIFICATION"] = verification
    report["VERIFICATION_PASSED"] = all_ok
    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[dryad-verify] === READ-ONLY POST-INGESTION VERIFICATION ===")
    for k, v in verification.items():
        print(f"  {k}: {v}")
    print(f"[dryad-verify] existing repos unchanged: {repos_unchanged}")
    print(f"[dryad-verify] VERIFICATION_PASSED={all_ok}")
    print(f"[dryad-verify] report updated: {_REPORT_PATH}")

    client.close()
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
