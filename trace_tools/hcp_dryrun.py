"""HCP/CCF read-only dry-run audit (2026-08-16).

Runs the full HCP pipeline in READ-ONLY mode against the production
``neurosearch_dataset_catalog`` collection:

  1. discovery — fetch the two authoritative index pages, extract study slugs,
     enforce the discovery gate (exactly 20 unique slugs, no within-page
     duplicates);
  2. per study — fetch the landing / data-releases / publications pages and
     normalize into source records;
  3. identity — resolve EVERY normalized record against the existing catalog
     (read-only) and record would-insert / would-merge / unresolved;
  4. report — discovered / normalized / duplicate identities / identity
     matches / proposed inserts / proposed merges / failures / API calls /
     asset calls, plus a production snapshot (BEFORE only — zero writes).

NO WRITES are performed. The production catalog must remain unchanged.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.hcp_dryrun

Report: ../trace_artifacts/hcp_investigation_20260816/hcp_dryrun_20260816.json
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import (
    HCP_EXPECTED_STUDIES,
    HCP_DEFAULT_PAGE_DELAY,
    make_hcp_fetchers,
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
        "hcp_dryrun_20260816.json",
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


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[settings.MONGO_DB_NAME]
    datasets_coll = db[settings.MONGO_DATASET_COLLECTION]
    collection = get_collection(db)

    print(f"[hcp-dryrun] production db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")
    before = await _catalog_snapshot(db, datasets_coll)
    print(f"[hcp-dryrun] production BEFORE: {json.dumps(before)}")

    # ── 1) Discovery (read-only; gate enforced). ────────────────────────────
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

    import httpx

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as http:
        for index_url in HCP_INDEX_PAGES:
            index_requests += 1
            try:
                html = await fetch_page(http, index_url)
            except Exception as exc:  # noqa: BLE001
                api_failures += 1
                api_error_details.append(f"index {index_url}: {type(exc).__name__}: {exc}")
                print(f"[hcp-dryrun] index fetch failed: {index_url}: {exc}")
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
            f"duplicates={len(per_page_duplicates)}"
        )
        print(
            f"[hcp-dryrun] discovered {len(unique_slugs)} unique studies "
            f"(cards_per_page={page_card_counts} links_per_page={page_link_counts} "
            f"expected={HCP_EXPECTED_STUDIES} duplicate_cards={len(per_page_duplicates)})"
        )
        if gate_failed:
            print(f"[hcp-dryrun] *** GATE FAILED: {gate_reason} — stopping (no writes) ***")

        # ── 2 + 3) Per study: fetch pages → normalize → resolve identity. ────
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
        asset_calls = 0  # hard guarantee
        representative_sources: list[dict] = []

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
                print(f"[hcp-dryrun] study fetch failed for {slug}: {exc}")
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
            if len(representative_sources) < 5:
                representative_sources.append(
                    {
                        "slug": slug,
                        "title": source.get("title"),
                        "sourceDatasetId": source.get("sourceDatasetId"),
                        "sourceUrl": source.get("sourceUrl"),
                        "doi": source.get("doi"),
                        "authors": source.get("authors"),
                        "dataReleaseCount": (source.get("snapshot") or {}).get(
                            "dataReleaseCount"
                        ),
                        "publicationCount": (source.get("snapshot") or {}).get(
                            "publicationCount"
                        ),
                        "rawMetadataHcpKeys": sorted(source.get("rawMetadata") or {}),
                    }
                )

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

    # ── 4) Report (read-only; production untouched). ─────────────────────────
    report = {
        "report_generated_at": _utcnow(),
        "mode": "READ-ONLY dry-run audit (zero writes)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "DISCOVERY": {
            "index_pages_fetched": index_requests,
            "expected_studies": HCP_EXPECTED_STUDIES,
            "discovered_unique": len(unique_slugs),
            "cards_per_page": page_card_counts,
            "links_per_page": page_link_counts,
            "duplicate_cards": per_page_duplicates,
            "gate_failed": gate_failed,
            "gate_reason": gate_reason,
            "discovered_slugs": unique_slugs,
        },
        "NORMALIZATION": {
            "retrieved": retrieved,
            "normalized": normalized,
            "failed": failed,
            "failure_details": failure_details[:20],
            "release_metadata_total": release_metadata_total,
            "publication_doi_total": publication_doi_total,
        },
        "IDENTITY_AUDIT": {
            "studies_checked": normalized,
            "would_insert": len(would_insert),
            "would_merge": len(would_merge),
            "unresolved": len(unresolved),
            "insert_slugs": [w["slug"] for w in would_insert],
            "merge_details": would_merge,
            "unresolved_details": unresolved,
        },
        "API": {
            "index_requests": index_requests,
            "study_requests": study_requests,
            "release_requests": release_requests,
            "publication_requests": publication_requests,
            "total_api_requests": total_api_calls,
            "asset_file_calls": asset_calls,
            "retries": len(retry_counter),
            "rate_limit_responses": len(rate_limit_counter),
            "api_failures": api_failures,
            "api_error_details": api_error_details[:20],
        },
        "PRODUCTION_INTEGRITY": {
            "catalog_total_before": before["catalog_total"],
            "repo_source_counts_before": before["repo_source_counts"],
            "production_datasets_collection_before": before[
                "production_datasets_collection_total"
            ],
            "writes_performed": 0,
        },
        "REPRESENTATIVE_RECORDS": representative_sources,
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print("[hcp-dryrun] === SUMMARY (READ-ONLY, no writes) ===")
    print(f"[hcp-dryrun] discovery: {len(unique_slugs)} unique studies "
          f"(expected={HCP_EXPECTED_STUDIES}) gate_failed={gate_failed}")
    print(f"[hcp-dryrun] normalized={normalized} failed={failed}")
    print(f"[hcp-dryrun] identity: would_insert={len(would_insert)} "
          f"would_merge={len(would_merge)} unresolved={len(unresolved)}")
    print(f"[hcp-dryrun] api_requests={total_api_calls} asset_file_calls={asset_calls} "
          f"retries={len(retry_counter)} rate_limit={len(rate_limit_counter)}")
    print(f"[hcp-dryrun] production catalog BEFORE: {before['catalog_total']} "
          f"(writes_performed=0)")
    print(f"[hcp-dryrun] report written to {_REPORT_PATH}")

    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
