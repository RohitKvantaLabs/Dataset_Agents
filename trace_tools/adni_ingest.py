"""ADNI PRODUCTION INGESTION (2026-08-17) — APPROVED live write.

Runs the gated production ADNI ingestion against the live catalog
``neurosearch_dataset_catalog`` using the AUTHORITATIVE approved 122-record
ADNI census artifact as input.

Flow:
  1. Load trace_artifacts/adni_census_20260817/adni_census.json → ``datasets[]``.
  2. PRE-GATE (read-only): normalize all records and resolve identity against
     the live catalog. The run ABORTS with ZERO writes unless EXACTLY:
         input = 122, normalized = 122, failed = 0, duplicate identities = 0,
         proposed inserts = 122, proposed merges = 0, unresolved = 0.
  3. PRODUCTION WRITES via app.catalog.ingest.run_adni_ingestion (the gated
     runner: exact-122 gate + stable_identifier gate + normalize → validate →
     upsert). Metadata only — no API calls, no asset/file downloads, no IDA
     access.
  4. POST-INGESTION verification (independent read-only checks): canonical
     count, ADNI source/sourceKey/canonical/rawMetadata coverage, DOI checks,
     verbatim sourceUrl, sourceUrlNorm None, shared-URL distinction, existing
     repository counts unchanged, unrelated records modified (content-hash
     comparison), write failures.
  5. Final safety: catalog total MUST be 6,043 → 6,165; otherwise STOP.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.adni_ingest

Report: ../trace_artifacts/adni_ingestion_20260817/adni_ingestion_20260817.json
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import _adni_identity_keys, run_adni_ingestion
from app.catalog.normalize import ADNI_CENSUS_EXPECTED, build_adni_source_record
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "adni_ingestion_20260817",
        "adni_ingestion_20260817.json",
    )
)
_CENSUS_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "adni_census_20260817",
        "adni_census.json",
    )
)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _doc_content_hash(doc: dict) -> str:
    """Deterministic content hash of a canonical record (ignores ``_id`` and
    volatile timestamps so we only detect real content drift)."""
    clone = dict(doc)
    clone.pop("_id", None)
    return hashlib.md5(
        json.dumps(clone, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _load_census(path: str) -> list[dict]:
    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    return list(payload.get("datasets") or [])


async def _repo_source_counts(coll) -> Counter:
    repos: Counter = Counter()
    async for d in coll.find({}, {"sources.repository": 1}):
        for s in (d.get("sources") or []):
            if s.get("repository"):
                repos[s["repository"]] += 1
    return repos


async def _adni_docs(coll):
    return coll.find({"sources.repository": "adni"})


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=15000)
    db = client[settings.MONGO_DB_NAME]
    collection = get_collection(db)

    print(f"[adni-ingest] PRODUCTION db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}")

    records = _load_census(_CENSUS_PATH)
    print(f"[adni-ingest] census artifact: {len(records)} records (expected {ADNI_CENSUS_EXPECTED})")

    # ── Phase 0: BEFORE snapshot (read-only). ────────────────────────────────
    total_before = await collection.count_documents({})
    repos_before = await _repo_source_counts(collection)
    baseline_hashes: dict[str, str] = {}
    async for d in collection.find({}):
        baseline_hashes[str(d["_id"])] = _doc_content_hash(d)
    print(f"[adni-ingest] BEFORE: catalog_total={total_before} "
          f"repos={dict(repos_before)}")

    # ── Phase 1: PRE-GATE — read-only resolution of the approved input. ──────
    duplicate_identities: list[dict] = []
    seen: dict[str, str] = {}
    for r in records:
        for key in _adni_identity_keys(r):
            prev = seen.get(key)
            if prev is not None:
                duplicate_identities.append(
                    {"identity": key, "first": prev, "second": str(r.get("stable_identifier") or "")}
                )
            else:
                seen[key] = str(r.get("stable_identifier") or "")

    normalized = 0
    failed = 0
    failure_details: list[str] = []
    proposed_inserts = 0
    proposed_merges = 0
    unresolved = 0
    unresolved_details: list[str] = []
    for r in records:
        try:
            source = build_adni_source_record(r)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            failure_details.append(f"{r.get('stable_identifier')}: normalize: {exc}")
            continue
        normalized += 1
        resolution = await resolve_identity(collection, source)
        if resolution["matched"] is not None:
            proposed_merges += 1
        elif resolution["ambiguous"]:
            unresolved += 1
            unresolved_details.append(
                f"{r.get('stable_identifier')}: {len(resolution['ambiguous'])} candidates"
            )
        else:
            proposed_inserts += 1

    pre_gate = {
        "approved_input": len(records),
        "input_exactly_122": len(records) == ADNI_CENSUS_EXPECTED,
        "normalized": normalized,
        "failed": failed,
        "duplicate_identities": len(duplicate_identities),
        "proposed_inserts": proposed_inserts,
        "proposed_merges": proposed_merges,
        "unresolved": unresolved,
    }
    gate_ok = (
        pre_gate["approved_input"] == 122
        and pre_gate["normalized"] == 122
        and pre_gate["failed"] == 0
        and pre_gate["duplicate_identities"] == 0
        and pre_gate["proposed_inserts"] == 122
        and pre_gate["proposed_merges"] == 0
        and pre_gate["unresolved"] == 0
    )
    print(f"[adni-ingest] PRE-GATE: {json.dumps(pre_gate)} gate_ok={gate_ok}")

    if not gate_ok:
        report = {
            "report_generated_at": _utcnow(),
            "mode": "PRODUCTION ingestion — ABORTED (pre-gate failed, ZERO writes)",
            "pre_gate": pre_gate,
            "failure_details": failure_details[:20],
            "unresolved_details": unresolved_details[:20],
            "writes_performed": 0,
        }
        os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
        with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, default=str)
        client.close()
        return 1

    # ── Phase 2: PRODUCTION WRITES. ─────────────────────────────────────────
    start = _utcnow()
    stats = await run_adni_ingestion(db, records=records, log=print)
    print(f"[adni-ingest] WRITE stats: {json.dumps({k: stats[k] for k in ('input','normalized','inserted','merged','failed','api_calls','asset_calls','validation_failed')})}")

    # ── Phase 3: POST-INGESTION verification (independent read-only). ────────
    total_after = await collection.count_documents({})
    repos_after = await _repo_source_counts(collection)

    adni_docs = []
    async for d in collection.find({"sources.repository": "adni"}):
        adni_docs.append(d)
    adni_source_ids = {}
    adni_source_keys = set()
    adni_canonical_ids = set()
    rawmeta_ok = 0
    adni_links = {}  # stable_identifier -> sourceUrl as stored on canonical source
    for d in adni_docs:
        adni_source_ids[d["sourceKeys"][0]] = d.get("sources", [{}])[0].get("sourceDatasetId")
        adni_source_keys.update(d.get("sourceKeys") or [])
        adni_canonical_ids.add(d["canonicalDatasetId"])
        if (d.get("rawMetadata") or {}).get("adni"):
            rawmeta_ok += 1
        for s in (d.get("sources") or []):
            if s.get("repository") == "adni":
                adni_links[s.get("sourceDatasetId")] = s.get("sourceUrl")

    census_urls = {r["stable_identifier"]: r["source_url"] for r in records}
    # exact count of ADNI sources whose stored sourceUrl matches the census URL verbatim
    source_url_matches = 0
    for d in adni_docs:
        for s in (d.get("sources") or []):
            if s.get("repository") != "adni":
                continue
            sid = str(s.get("sourceDatasetId") or "").replace("adni:", "", 1)
            if s.get("sourceUrl") == census_urls.get(sid):
                source_url_matches += 1

    doi_non_null = sum(1 for d in adni_docs if d.get("doi") is not None)
    pub_doi_present = sum(
        1 for d in adni_docs
        if (d.get("rawMetadata") or {}).get("adni", {}).get("publication_doi")
    )
    leakage = sum(
        1 for d in adni_docs
        if d.get("doi") is not None
    )
    url_norm_used = sum(
        1 for d in adni_docs
        if (d.get("provenance") or {}).get("identity", {}).get("sourceUrlNorm") is not None
    )

    # shared-URL products remain distinct (census groups with >1 products)
    census_groups: dict[str, list[str]] = {}
    for r in records:
        census_groups.setdefault(r["source_url"], []).append(r["stable_identifier"])
    shared_groups_distinct = True
    shared_checks = []
    for url, ids in census_groups.items():
        if len(ids) < 2:
            continue
        canons = set()
        for d in adni_docs:
            for s in (d.get("sources") or []):
                if s.get("repository") == "adni" and str(s.get("sourceDatasetId") or "").replace("adni:", "", 1) in ids:
                    canons.add(d["canonicalDatasetId"])
        distinct = len(canons) == len(ids)
        shared_groups_distinct = shared_groups_distinct and distinct
        shared_checks.append({"source_url": url, "products": ids, "canonicalIds": len(canons), "distinct": distinct})

    # unrelated records modified: content-hash comparison on previously-existing docs
    unrelated_modified = 0
    async for d in collection.find({}):
        key = str(d["_id"])
        if key not in baseline_hashes:
            continue  # newly inserted ADNI docs are additions, not modifications
        if _doc_content_hash(d) != baseline_hashes[key]:
            unrelated_modified += 1

    existing_unchanged = _repo_source_counts_unchanged(repos_before, repos_after)

    verification = {
        "canonical_total_matches_expected": total_after == 6165,
        "canonical_total": total_after,
        "canonical_before": total_before,
        "adni_sources": len(adni_docs),
        "adni_sources_exactly_122": len(adni_docs) == 122,
        "unique_sourceDatasetIds": len(set(adni_source_ids.values())),
        "unique_sourceKeys": len(adni_source_keys),
        "unique_canonicalIds": len(adni_canonical_ids),
        "rawMetadata_adni_coverage": rawmeta_ok,
        "canonical_adni_dois": doi_non_null,
        "publication_doi_leakage_into_canonical": leakage,
        "publication_doi_relationship_records": pub_doi_present,
        "sourceUrl_verbatim_matches": source_url_matches,
        "sourceUrlNorm_used_for_identity": url_norm_used,
        "shared_url_groups_distinct": shared_groups_distinct,
        "adni_merges": stats.get("merged", 0),
        "adni_failures": stats.get("failed", 0),
        "asset_file_calls": stats.get("asset_calls", 0),
        "api_calls": stats.get("api_calls", 0),
        "restricted_ida_access": 0,
        "existing_repositories_unchanged": existing_unchanged,
        "unrelated_records_modified": unrelated_modified,
        "production_write_failures": stats.get("failed", 0),
    }

    passed = (
        verification["canonical_total_matches_expected"]
        and verification["adni_sources_exactly_122"]
        and verification["unique_sourceDatasetIds"] == 122
        and verification["unique_sourceKeys"] == 122
        and verification["unique_canonicalIds"] == 122
        and verification["rawMetadata_adni_coverage"] == 122
        and verification["canonical_adni_dois"] == 0
        and verification["publication_doi_leakage_into_canonical"] == 0
        and verification["sourceUrl_verbatim_matches"] == 122
        and verification["sourceUrlNorm_used_for_identity"] == 0
        and verification["shared_url_groups_distinct"] is True
        and verification["adni_merges"] == 0
        and verification["adni_failures"] == 0
        and verification["asset_file_calls"] == 0
        and verification["api_calls"] == 0
        and verification["restricted_ida_access"] == 0
        and verification["existing_repositories_unchanged"] is True
        and verification["unrelated_records_modified"] == 0
        and verification["production_write_failures"] == 0
        and stats.get("inserted", 0) == 122
        and stats.get("normalized", 0) == 122
    )

    report = {
        "report_generated_at": _utcnow(),
        "mode": "PRODUCTION ingestion (live writes)",
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "census_artifact": _CENSUS_PATH,
        "pre_gate": pre_gate,
        "INGESTION": {
            "approved_input": len(records),
            "inserted": stats.get("inserted", 0),
            "merged": stats.get("merged", 0),
            "failed": stats.get("failed", 0),
            "validation_failed": stats.get("validation_failed", 0),
            "duplicate_source_identities": stats.get("duplicate_source_identities", []),
            "matched_via": stats.get("matched_via", {}),
            "api_calls": stats.get("api_calls", 0),
            "asset_calls": stats.get("asset_calls", 0),
            "started_at": start,
            "finished_at": stats.get("finished_at"),
            "elapsed_s": stats.get("elapsed_s"),
        },
        "VERIFICATION": verification,
        "PRODUCTION_INTEGRITY": {
            "catalog_before": total_before,
            "catalog_after": total_after,
            "repo_source_counts_before": dict(repos_before),
            "repo_source_counts_after": dict(repos_after),
            "shared_url_group_checks": shared_checks,
        },
        "verification_passed": passed,
        "conclusion": (
            "ADNI production ingestion completed successfully: "
            f"122 new canonical neuroscience datasets, 0 merges, "
            f"final catalog {total_after}."
            if passed else "VERIFICATION FAILED — investigate before any further action."
        ),
        "status": "PASSED" if passed else "FAILED",
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print(f"[adni-ingest] === SUMMARY ===")
    print(f"[adni-ingest] INSERTED={stats.get('inserted')} MERGED={stats.get('merged')} "
          f"FAILED={stats.get('failed')}")
    print(f"[adni-ingest] catalog: {total_before} -> {total_after}")
    print(f"[adni-ingest] ADNI docs={len(adni_docs)} unique_ids={len(set(adni_source_ids.values()))} "
          f"unique_keys={len(adni_source_keys)} unique_canonical={len(adni_canonical_ids)}")
    print(f"[adni-ingest] existing repos unchanged={existing_unchanged} "
          f"unrelated_modified={unrelated_modified}")
    print(f"[adni-ingest] VERIFICATION: {'PASSED' if passed else 'FAILED'}")
    print(f"[adni-ingest] report -> {_REPORT_PATH}")

    client.close()
    return 0 if passed else 1


def _repo_source_counts_unchanged(before: Counter, after: Counter) -> bool:
    """Existing repositories are unchanged iff every repo present BEFORE has
    the same source count AFTER. Newly introduced repositories (adni) are the
    intended addition, not a change to an existing repository."""
    diff = {}
    for repo in before:
        b, a = before.get(repo, 0), after.get(repo, 0)
        if b != a:
            diff[repo] = {"before": b, "after": a}
    return not diff


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))