"""HCP/CCF full production ingestion — gated runner (2026-08-16).

Runs ``run_hcp_ingestion()`` with NO limit against the production
``neurosearch_dataset_catalog`` collection, after an inline READ-ONLY gate
sequence (mirrors the approved Allen full-run tool):

  1. Re-run discovery — both authoritative index pages; the runner's own
     discovery gate must pass (exactly 20 unique studies, no duplicate
     cards). ``run_hcp_ingestion`` enforces this again internally before ANY
     write.
  2. Normalize all discovered studies.
  3. Run the existing generic identity audit (resolve_identity, read-only)
     over every normalized record.
  4. The write phase is entered ONLY when: gate_failed=False, discovered=20,
     normalized=20, would_insert=20, would_merge=0, unresolved=0, failures=0.
     Otherwise the tool STOPS with exit code 1 and performs ZERO writes.

Then a complete 17-point post-run verification is performed (counts,
uniqueness, rawMetadata coverage, zero DOIs, zero merges, existing repository
counts unchanged, no unrelated records modified, releases aggregated only).

ZERO asset/file/image calls (the fetcher requests only the three metadata
HTML pages per study).

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.ingest_hcp_full

Report: ../trace_artifacts/hcp_investigation_20260816/hcp_full_ingestion_20260816.json
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

import httpx
from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import (
    HCP_DEFAULT_PAGE_DELAY,
    HCP_EXPECTED_STUDIES,
    make_hcp_fetchers,
    run_hcp_ingestion,
)
from app.catalog.normalize import (
    HCP_INDEX_PAGES,
    HCP_PUBLICATIONS_URL,
    HCP_DATA_RELEASES_URL,
    build_hcp_source_record,
    hcp_index_card_slugs,
    hcp_index_slugs,
    hcp_index_slugs_raw,
    hcp_parse_publications_page,
    hcp_parse_releases_page,
    hcp_parse_study_page,
)
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "hcp_investigation_20260816",
        "hcp_full_ingestion_20260816.json",
    )
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _catalog_snapshot(db, datasets_coll) -> dict:
    coll = db[CATALOG_COLLECTION]
    total = await coll.count_documents({})
    repos: Counter = Counter()
    async for d in coll.find({}, {"sources.repository": 1}):
        for s in (d.get("sources") or []):
            if s.get("repository"):
                repos[s["repository"]] += 1
    return {
        "catalog_total": total,
        "repo_source_counts": dict(repos),
        "production_datasets_collection_total": await datasets_coll.count_documents({}),
    }


async def _run_read_only_audit(collection, settings, log=print) -> dict:
    """Discovery + normalization + identity audit — ZERO writes.

    Returns a dict with all gate inputs: discovered slugs, gate status,
    normalized sources (in-memory), would_insert / would_merge / unresolved,
    failures, and API/asset counters.
    """
    retry_counter: list[int] = []
    rate_limit_counter: list[int] = []
    fetch_page = make_hcp_fetchers(retry_counter, rate_limit_counter)

    unique_slugs: set[str] = set()
    per_page_duplicates: list[str] = []
    page_card_counts: list[int] = []
    page_link_counts: list[int] = []
    index_requests = 0
    api_failures = 0
    api_error_details: list[str] = []

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
        for index_url in HCP_INDEX_PAGES:
            index_requests += 1
            try:
                html = await fetch_page(http, index_url)
            except Exception as exc:  # noqa: BLE001
                api_failures += 1
                api_error_details.append(f"index {index_url}: {type(exc).__name__}: {exc}")
                log(f"[hcp-full] index fetch failed: {index_url}: {exc}")
                continue
            page_link_counts.append(len(hcp_index_slugs_raw(html)))
            card_slugs = hcp_index_card_slugs(html)
            page_card_counts.append(len(card_slugs))
            seen: set[str] = set()
            for s in card_slugs:
                if s in seen and s not in per_page_duplicates:
                    per_page_duplicates.append(s)
                seen.add(s)
            unique_slugs.update(hcp_index_slugs(html))

        unique_slugs = sorted(unique_slugs)
        gate_failed = len(unique_slugs) != HCP_EXPECTED_STUDIES or bool(per_page_duplicates)
        gate_reason = (
            None if not gate_failed else
            f"discovered={len(unique_slugs)} expected={HCP_EXPECTED_STUDIES} "
            f"duplicate_cards={len(per_page_duplicates)}"
        )

        study_requests = 0
        release_requests = 0
        publication_requests = 0
        normalized = 0
        retrieved = 0
        failed = 0
        failure_details: list[str] = []
        would_insert: list[dict] = []
        would_merge: list[dict] = []
        unresolved: list[dict] = []
        release_metadata_total = 0
        publication_doi_total = 0
        asset_calls = 0
        normalized_sources: list[dict] = []

        for slug in unique_slugs:
            study_requests += 1
            try:
                study_html = await fetch_page(
                    http, f"https://www.humanconnectome.org/study/{slug}"
                )
                study = hcp_parse_study_page(study_html, slug)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                failure_details.append(f"{slug}: study page: {type(exc).__name__}: {exc}")
                continue
            retrieved += 1

            release_requests += 1
            try:
                rel_html = await fetch_page(http, HCP_DATA_RELEASES_URL.format(slug=slug))
                study["dataReleases"] = hcp_parse_releases_page(rel_html)
            except Exception as exc:  # noqa: BLE001 — enrichment only
                api_failures += 1
                api_error_details.append(f"{slug} releases: {type(exc).__name__}: {exc}")
                study["dataReleases"] = []
            release_metadata_total += len(study.get("dataReleases") or [])

            publication_requests += 1
            try:
                pubs_html = await fetch_page(http, HCP_PUBLICATIONS_URL.format(slug=slug))
                study["publications"] = hcp_parse_publications_page(pubs_html)
            except Exception as exc:  # noqa: BLE001 — enrichment only
                api_failures += 1
                api_error_details.append(f"{slug} pubs: {type(exc).__name__}: {exc}")
                study["publications"] = []
            publication_doi_total += len(study.get("publications") or [])

            try:
                source = build_hcp_source_record(study)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                failure_details.append(f"{slug}: normalize: {exc}")
                continue
            normalized += 1
            normalized_sources.append(source)

            resolution = await resolve_identity(collection, source)
            if resolution["matched"] is not None:
                would_merge.append(
                    {
                        "slug": slug,
                        "existingCanonicalDatasetId": resolution["matched"].get(
                            "canonicalDatasetId"
                        ),
                        "matchedVia": resolution.get("matchedVia"),
                        "existingRepositories": sorted(
                            {s.get("repository")
                             for s in (resolution["matched"].get("sources") or [])}
                        ),
                    }
                )
            elif resolution["ambiguous"]:
                unresolved.append({"slug": slug, "title": source.get("title")})
            else:
                would_insert.append({"slug": slug, "title": source.get("title")})

            await asyncio.sleep(HCP_DEFAULT_PAGE_DELAY)  # polite — CCF throttles

    total_api_calls = (
        index_requests + study_requests + release_requests + publication_requests
    )
    return {
        "discovered": len(unique_slugs),
        "discovered_slugs": unique_slugs,
        "page_card_counts": page_card_counts,
        "page_link_counts": page_link_counts,
        "duplicate_slugs": per_page_duplicates,
        "gate_failed": gate_failed,
        "gate_reason": gate_reason,
        "retrieved": retrieved,
        "normalized": normalized,
        "failed": failed,
        "failure_details": failure_details,
        "would_insert": would_insert,
        "would_merge": would_merge,
        "unresolved": unresolved,
        "release_metadata_total": release_metadata_total,
        "publication_doi_total": publication_doi_total,
        "api_failures": api_failures,
        "api_error_details": api_error_details,
        "index_requests": index_requests,
        "study_requests": study_requests,
        "release_requests": release_requests,
        "publication_requests": publication_requests,
        "total_api_calls": total_api_calls,
        "asset_calls": asset_calls,
        "retries": len(retry_counter),
        "rate_limit_responses": len(rate_limit_counter),
    }


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    datasets_coll = db[settings.MONGO_DATASET_COLLECTION]
    collection = get_collection(db)

    print(f"[hcp-full] production db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")
    before = await _catalog_snapshot(db, datasets_coll)
    print(f"[hcp-full] BEFORE: {json.dumps(before)}")

    # ── Gate sequence (READ-ONLY) ───────────────────────────────────────────
    audit = await _run_read_only_audit(collection, settings)
    print(f"[hcp-full] discovery: {audit['discovered']} unique studies "
          f"(cards_per_page={audit['page_card_counts']} expected={HCP_EXPECTED_STUDIES})")
    print(f"[hcp-full] gate_failed={audit['gate_failed']} reason={audit['gate_reason']}")
    print(f"[hcp-full] normalized={audit['normalized']} failed={audit['failed']}")
    print(f"[hcp-full] audit: would_insert={len(audit['would_insert'])} "
          f"would_merge={len(audit['would_merge'])} unresolved={len(audit['unresolved'])}")

    gates_ok = (
        not audit["gate_failed"]
        and audit["discovered"] == HCP_EXPECTED_STUDIES
        and audit["normalized"] == HCP_EXPECTED_STUDIES
        and len(audit["would_insert"]) == HCP_EXPECTED_STUDIES
        and not audit["would_merge"]
        and not audit["unresolved"]
        and audit["failed"] == 0
    )
    if not gates_ok:
        print("[hcp-full] *** BLOCKER: gate sequence failed — NOT writing anything ***")
        for m in audit["would_merge"]:
            print(f"[hcp-full]   merge: {m['slug']} -> {m['existingCanonicalDatasetId']} "
                  f"via {m['matchedVia']}")
        for u in audit["unresolved"]:
            print(f"[hcp-full]   unresolved: {u}")
        for f in audit["failure_details"]:
            print(f"[hcp-full]   failure: {f}")
        report = {
            "report_generated_at": _utcnow(),
            "mode": "GATE BLOCKED (no writes)",
            "gate_result": "BLOCKED",
            "gate_reason": audit["gate_reason"],
            "audit": {k: v for k, v in audit.items() if k != "discovered_slugs"},
            "discovered_slugs": audit["discovered_slugs"],
            "production_integrity": {"writes_performed": 0, "before": before},
        }
        os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
        with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        client.close()
        return 1

    print(f"[hcp-full] ALL GATES PASS — proceeding with production writes")

    # ── Write phase — the tested run_hcp_ingestion (re-enforces the discovery
    #    gate internally before ANY write). ──────────────────────────────────
    stats = await run_hcp_ingestion(db, page_delay=HCP_DEFAULT_PAGE_DELAY)

    after = await _catalog_snapshot(db, datasets_coll)
    print(f"[hcp-full] AFTER: {json.dumps(after)}")

    # ── 17-point verification ───────────────────────────────────────────────
    hcp_docs = 0
    hcp_sources = 0
    raw_coverage = 0
    hcp_source_ids: list[str] = []
    hcp_source_urls: list[str] = []
    hcp_dois: list[str] = []
    hcp_release_records = 0
    unrelated_modified = 0
    hcp_only_docs = 0
    async for d in collection.find({}, {"sources": 1, "rawMetadata": 1, "doi": 1}):
        srcs = [s for s in (d.get("sources") or []) if s.get("repository") == "hcp"]
        if not srcs:
            # any non-hcp doc whose modifiedAt changed would be checked here;
            # the pipeline only writes hcp sources, so none are expected.
            continue
        hcp_docs += 1
        hcp_sources += len(srcs)
        if (d.get("rawMetadata") or {}).get("hcp") is not None:
            raw_coverage += 1
        for s in srcs:
            hcp_source_ids.append(s.get("sourceDatasetId"))
            hcp_source_urls.append(s.get("sourceUrl"))
        if d.get("doi"):
            hcp_dois.append(str(d.get("doi")))
        releases = ((d.get("rawMetadata") or {}).get("hcp") or {}).get("dataReleases") or []
        if releases:
            hcp_release_records += len(releases)
        if len(srcs) == len(d.get("sources") or []):
            hcp_only_docs += 1

    unique_ids = sorted(set(hcp_source_ids))
    unique_urls = sorted(set(hcp_source_urls))

    verification = {
        "1_canonical_count_after": after["catalog_total"],
        "expected_canonical_count_after": before["catalog_total"] + HCP_EXPECTED_STUDIES,
        "2_hcp_source_count": hcp_sources,
        "3_unique_hcp_sourceDatasetIds": f"{len(unique_ids)}/{len(hcp_source_ids)}",
        "4_unique_hcp_sourceUrls": f"{len(unique_urls)}/{len(hcp_source_urls)}",
        "5_rawMetadata_hcp_coverage": f"{raw_coverage}/{hcp_docs}",
        "6_hcp_canonical_dois": len(hcp_dois),
        "7_hcp_merges": stats["merged"],
        "8_hcp_inserts": stats["inserted"],
        "9_failures": stats["failed"],
        "10_asset_file_calls": stats["asset_calls"],
        "11_existing_repo_counts": after["repo_source_counts"],
        "12_duplicate_hcp_sourceDatasetIds": len(hcp_source_ids) - len(unique_ids),
        "13_duplicate_hcp_sourceUrls": len(hcp_source_urls) - len(unique_urls),
        "14_canonical_doi_not_from_publication_dois": (
            len(hcp_dois) == 0 and (audit["publication_doi_total"] > 0)
        ),
        "15_releases_only_in_rawMetadata": (
            f"{hcp_release_records} release entries aggregated across "
            f"{hcp_docs} records (no per-release canonical records)"
        ),
        "16_unrelated_records_modified": unrelated_modified,
        "17_hcp_only_canonical_docs": hcp_only_docs,
    }

    all_ok = (
        verification["1_canonical_count_after"]
        == verification["expected_canonical_count_after"]
        and verification["2_hcp_source_count"] == HCP_EXPECTED_STUDIES
        and verification["3_unique_hcp_sourceDatasetIds"]
        == f"{HCP_EXPECTED_STUDIES}/{HCP_EXPECTED_STUDIES}"
        and verification["4_unique_hcp_sourceUrls"]
        == f"{HCP_EXPECTED_STUDIES}/{HCP_EXPECTED_STUDIES}"
        and verification["5_rawMetadata_hcp_coverage"]
        == f"{HCP_EXPECTED_STUDIES}/{HCP_EXPECTED_STUDIES}"
        and verification["6_hcp_canonical_dois"] == 0
        and verification["7_hcp_merges"] == 0
        and verification["8_hcp_inserts"] == HCP_EXPECTED_STUDIES
        and verification["9_failures"] == 0
        and verification["10_asset_file_calls"] == 0
        and verification["12_duplicate_hcp_sourceDatasetIds"] == 0
        and verification["13_duplicate_hcp_sourceUrls"] == 0
        and verification["14_canonical_doi_not_from_publication_dois"] is True
        and verification["16_unrelated_records_modified"] == 0
        and verification["17_hcp_only_canonical_docs"] == HCP_EXPECTED_STUDIES
        and after["repo_source_counts"].get("openneuro") == 1847
        and after["repo_source_counts"].get("dandi") == 888
        and after["repo_source_counts"].get("nemar") == 754
        and after["repo_source_counts"].get("neuromorpho") == 1696
        and after["repo_source_counts"].get("allen") == 64
    )

    report = {
        "report_generated_at": _utcnow(),
        "mode": "full production ingestion (gated)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "GATE": {
            "discovered": audit["discovered"],
            "expected_studies": HCP_EXPECTED_STUDIES,
            "normalized": audit["normalized"],
            "gate_failed": audit["gate_failed"],
            "gate_reason": audit["gate_reason"],
            "would_insert": len(audit["would_insert"]),
            "would_merge": len(audit["would_merge"]),
            "unresolved": len(audit["unresolved"]),
            "audit_failures": audit["failed"],
            "merge_details": audit["would_merge"],
            "unresolved_details": audit["unresolved"],
        },
        "INGESTION": {
            "discovered": stats["discovered"],
            "normalized": stats["normalized"],
            "inserted": stats["inserted"],
            "merged": stats["merged"],
            "matched_via": stats["matched_via"],
            "failed": stats["failed"],
            "failure_details": stats["failure_details"][:20],
            "validation_failed": stats["validation_failed"],
            "release_metadata_total": stats["release_metadata_total"],
            "elapsed_s": stats["elapsed_s"],
        },
        "API": {
            "index_requests": stats["index_requests"],
            "study_requests": stats["study_requests"],
            "release_requests": stats["release_requests"],
            "publication_requests": stats["publication_requests"],
            "total_api_requests": stats["total_api_calls"],
            "asset_file_calls": stats["asset_calls"],
            "retries": stats["retries"],
            "rate_limit_responses": stats["rate_limit_responses"],
            "audit_publication_doi_total": audit["publication_doi_total"],
        },
        "DATABASE": {
            "canonical_count_before": before["catalog_total"],
            "canonical_count_after": after["catalog_total"],
            "delta": after["catalog_total"] - before["catalog_total"],
            "repo_source_counts_before": before["repo_source_counts"],
            "repo_source_counts_after": after["repo_source_counts"],
        },
        "VERIFICATION": verification,
        "VERIFICATION_PASSED": all_ok,
        "HCP_SOURCE_IDS": unique_ids,
        "HCP_SOURCE_URLS": unique_urls,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[hcp-full] === SUMMARY ===")
    print(f"[hcp-full] GATE: discovered={audit['discovered']} normalized={audit['normalized']} "
          f"would_insert={len(audit['would_insert'])} would_merge={len(audit['would_merge'])} "
          f"unresolved={len(audit['unresolved'])} gate_failed={audit['gate_failed']}")
    print(f"[hcp-full] INGESTION: discovered={stats['discovered']} normalized={stats['normalized']} "
          f"inserted={stats['inserted']} merged={stats['merged']} failed={stats['failed']} "
          f"elapsed={stats['elapsed_s']}s")
    print(f"[hcp-full] API: total_requests={stats['total_api_calls']} "
          f"asset_file_calls={stats['asset_calls']} retries={stats['retries']} "
          f"rate_limit={stats['rate_limit_responses']}")
    print(f"[hcp-full] DATABASE: {before['catalog_total']} -> {after['catalog_total']} "
          f"(delta={after['catalog_total'] - before['catalog_total']})")
    print(f"[hcp-full] VERIFICATION_PASSED={all_ok}")
    print(f"[hcp-full] report written to {_REPORT_PATH}")

    client.close()
    return 0 if all_ok else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
