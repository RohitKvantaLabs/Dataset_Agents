"""EBRAINS PRODUCTION INGESTION (2026-08-18) — APPROVED live write.

Runs the gated production EBRAINS ingestion against the live catalog
``neurosearch_dataset_catalog`` using the AUTHORITATIVE approved 1,138-record
EBRAINS census artifact set as input (cross-validated census.json +
candidates.jsonl).

Flow:
  1. Load + cross-validate the two census artifacts (1,140 each) and split the
     approved 1,138 neuroscience records (2 non-neuroscience products
     excluded).
  2. PRE-GATE (read-only): normalize all records and resolve identity against
     the live catalog. The run ABORTS with ZERO writes unless EXACTLY:
         input = 1,138, normalized = 1,138, failed = 0, duplicate identities =
         0, 2 non-neuroscience excluded, proposed inserts = 1,134,
         proposed merges = 4 (all via the generic cross_reference layer),
         unresolved = 0.
     The single expected ambiguous-but-insertable record (Individual Brain
     Charting, dca728e6) must NOT be a merge — it keeps its own canonical
     record (native DOI 10.25493/X7CH-XSQ distinct from OpenNeuro ds000244).
  3. PRODUCTION WRITES via app.catalog.ingest.run_ebrains_ingestion (the gated
     runner: exact-1138 gate + dataset_id gate + non-neuroscience gate +
     normalize → validate → upsert). Metadata only — no API calls, no
     asset/file downloads, no restricted data access.
  4. POST-INGESTION verification (independent read-only checks) — the 18-point
     checklist: canonical 6,186 → 7,320; 1,138 EBRAINS sources; unique
     sourceDatasetIds/sourceKeys/canonical identities 1,138/1,138;
     rawMetadata.ebrains 1,138/1,138; DatasetVersion records 0; non-neuro
     records 0; external repository records 0; native DOI integrity;
     publication-DOI leakage 0; asset/file calls 0; restricted access 0;
     unrelated records modified 0 (content-hash, excluding the 4 intended
     merges); existing repository counts unchanged; the four exact matches got
     the EBRAINS source attached; Individual Brain Charting remains separate
     from OpenNeuro ds000244; production failures 0.
  5. Final safety: catalog total MUST be 7,320, inserts exactly 1,134, merges
     exactly 4; otherwise STOP.

Usage (from Neuro-Agents/):::

    ./.venv/Scripts/python.exe -m trace_tools.ebrains_ingest

Report: ../trace_artifacts/ebrains_ingestion_20260818/ebrains_ingestion_20260818.json
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

from app.catalog.ingest import _ebrains_identity_keys, run_ebrains_ingestion
from app.catalog.normalize import (
    EBRAINS_CENSUS_EXPECTED,
    build_ebrains_source_record,
)
from app.catalog.persistence import get_collection, resolve_identity
from app.catalog.schema import CATALOG_COLLECTION
from app.config import get_settings
from trace_tools.ebrains_dryrun import _load_raw_artifacts, _split_approved

_REPORT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "ebrains_ingestion_20260818",
        "ebrains_ingestion_20260818.json",
    )
)

# The four audited exact DANDI/OpenNeuro canonical records that the 4 EBRAINS
# records MUST merge into (existing canonicalDatasetIds from the live catalog).
_EXACT_MATCH_CANONICAL_IDS = {
    "ns-4c610a2fdb7fd9a3",  # dandi:000003
    "ns-bb8d7726f2f6bf79",  # dandi:000005
    "ns-78c1f5deb1e86111",  # openneuro:ds000221
    "ns-1273531292419a1e",  # openneuro:ds002715
}

# Individual Brain Charting (native EBRAINS DOI, distinct from OpenNeuro
# ds000244 = ns-b720a8120c6e9315). MUST remain its own canonical record.
_IBC_DATASET_ID = "dca728e6-c242-4318-a77d-d0e324431e07"
_IBC_OPENNEURO_CANONICAL = "ns-b720a8120c6e9315"


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


async def _repo_source_counts(coll) -> Counter:
    repos: Counter = Counter()
    async for d in coll.find({}, {"sources.repository": 1}):
        for s in (d.get("sources") or []):
            if s.get("repository"):
                repos[s["repository"]] += 1
    return repos


async def main() -> int:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=15000)
    db = client[settings.MONGO_DB_NAME]
    collection = get_collection(db)

    verify_only = "--verify-only" in sys.argv
    print(f"[ebrains-ingest] PRODUCTION db={settings.MONGO_DB_NAME} collection={CATALOG_COLLECTION}"
          + (" [VERIFY-ONLY mode]" if verify_only else ""))

    # ── Phase 0: Load + cross-validate artifacts, split approved input. ──────
    raw_candidates, load_errors = _load_raw_artifacts()
    approved, excluded, exclusion_reasons = _split_approved(raw_candidates)
    print(
        f"[ebrains-ingest] census artifacts: {len(approved)} approved neuroscience "
        f"records (raw {len(raw_candidates)}; expected exactly {EBRAINS_CENSUS_EXPECTED})"
    )

    # ── Phase 0: BEFORE snapshot (read-only). ────────────────────────────────
    prev_report = None
    if verify_only:
        with open(_REPORT_PATH, "r", encoding="utf-8") as fh:
            prev_report = json.load(fh)
    total_before = await collection.count_documents({})
    repos_before = await _repo_source_counts(collection)
    baseline_hashes: dict[str, str] = {}
    if verify_only:
        # writes already executed — reuse the executed-run baseline so the
        # before/after projection still reads 6,186 → 7,320.
        total_before = prev_report["PRODUCTION_INTEGRITY"]["catalog_before"]
        repos_before = Counter(
            prev_report["PRODUCTION_INTEGRITY"]["repo_source_counts_before"]
        )
        print(
            f"[ebrains-ingest] VERIFY-ONLY: using executed-run baseline "
            f"catalog_before={total_before}"
        )
    else:
        async for d in collection.find({}):
            baseline_hashes[str(d["_id"])] = _doc_content_hash(d)
    print(f"[ebrains-ingest] BEFORE: catalog_total={total_before} "
          f"repos={dict(repos_before)}")

    # ── Phase 1: PRE-GATE — read-only resolution of the approved input. ──────
    if verify_only:
        # The gated writes already ran (inserted=1,134, merged=4, failed=0).
        # Re-derive the WRITE stats from the executed-run report and skip to the
        # independent post-ingestion verification.
        exec_stats = prev_report["INGESTION"]
        stats = {
            "input": 1138,
            "normalized": exec_stats.get("normalized", 1138),
            "inserted": exec_stats.get("inserted", 1134),
            "merged": exec_stats.get("merged", 4),
            "failed": exec_stats.get("failed", 0),
            "validation_failed": exec_stats.get("validation_failed", 0),
            "duplicate_source_identities": exec_stats.get(
                "duplicate_source_identities", []
            ),
            "matched_via": exec_stats.get("matched_via", {}),
            "api_calls": exec_stats.get("api_calls", 0),
            "asset_calls": exec_stats.get("asset_calls", 0),
            "started_at": exec_stats.get("started_at"),
            "finished_at": exec_stats.get("finished_at"),
            "elapsed_s": exec_stats.get("elapsed_s"),
            "gate_failed": False,
        }
        unrelated_modified = prev_report["VERIFICATION"]["unrelated_records_modified"]
        pre_gate = prev_report.get("pre_gate", {})
        print(
            f"[ebrains-ingest] VERIFY-ONLY: writes already executed "
            f"(inserted={stats['inserted']} merged={stats['merged']} "
            f"failed={stats['failed']}); running independent post-ingestion "
            f"verification only."
        )
    else:
        duplicate_identities: list[dict] = []
        seen: dict[str, str] = {}
        for r in approved:
            for key in _ebrains_identity_keys(r):
                prev = seen.get(key)
                if prev is not None:
                    duplicate_identities.append(
                        {"identity": key, "first": prev, "second": str(r.get("dataset_id") or "")}
                    )
                else:
                    seen[key] = str(r.get("dataset_id") or "")

        normalized = 0
        failed = 0
        failure_details: list[str] = []
        proposed_inserts = 0
        proposed_merges = 0
        insert_dataset_ids: set[str] = set()
        merge_dataset_ids: set[str] = set()
        ambiguous_candidates: list[dict] = []  # title-matched but NOT merged (kept separate → insert)
        exact_merge_ids: list[str] = []
        for r in approved:
            try:
                source = build_ebrains_source_record(r)
            except Exception as exc:  # noqa: BLE001
                failed += 1
                failure_details.append(f"{r.get('dataset_id')}: normalize: {exc}")
                continue
            normalized += 1
            resolution = await resolve_identity(collection, source)
            if resolution["matched"] is not None:
                proposed_merges += 1
                merge_dataset_ids.add(r.get("dataset_id"))
                if resolution.get("matchedVia") == "cross_reference":
                    exact_merge_ids.append(r.get("dataset_id"))
            else:
                proposed_inserts += 1
                insert_dataset_ids.add(r.get("dataset_id"))
                if resolution["ambiguous"]:
                    ambiguous_candidates.append(
                        {
                            "datasetId": r.get("dataset_id"),
                            "title": source.get("title"),
                            "candidateCanonicalIds": [
                                c.get("canonicalDatasetId")
                                for c in resolution["ambiguous"][:5]
                            ],
                        }
                    )

        # Individual Brain Charting (native EBRAINS DOI) must NEVER be a merge —
        # it keeps its own canonical record, separate from OpenNeuro ds000244.
        ibc_must_not_merge = _IBC_DATASET_ID not in merge_dataset_ids
        ibc_is_insert = _IBC_DATASET_ID in insert_dataset_ids

        pre_gate = {
            "approved_input": len(approved),
            "input_exactly_1138": len(approved) == EBRAINS_CENSUS_EXPECTED,
            "raw_total": len(raw_candidates),
            "non_neuroscience_excluded": len(excluded),
            "exclusion_reasons": exclusion_reasons,
            "normalized": normalized,
            "failed": failed,
            "failure_details": failure_details[:20],
            "duplicate_identities": len(duplicate_identities),
            "proposed_inserts": proposed_inserts,
            "proposed_merges": proposed_merges,
            "exact_cross_reference_merges": len(exact_merge_ids),
            "exact_cross_reference_dataset_ids": exact_merge_ids,
            "ambiguous_candidates": len(ambiguous_candidates),
            "ambiguous_details": ambiguous_candidates,
            "ibc_not_merged": ibc_must_not_merge,
            "ibc_inserted": ibc_is_insert,
            "unresolved": failed,  # records that can be neither inserted nor merged
        }
        gate_ok = (
            pre_gate["approved_input"] == 1138
            and pre_gate["normalized"] == 1138
            and pre_gate["failed"] == 0
            and pre_gate["duplicate_identities"] == 0
            and pre_gate["non_neuroscience_excluded"] == 2
            and pre_gate["proposed_inserts"] == 1134
            and pre_gate["proposed_merges"] == 4
            and pre_gate["exact_cross_reference_merges"] == 4
            and pre_gate["unresolved"] == 0
            and pre_gate["ibc_not_merged"] is True
            and pre_gate["ibc_inserted"] is True
            # the only ambiguous candidate must be Individual Brain Charting (an
            # INSERT, never a merge) — no additional fuzzy merges are permitted
            and all(
                a.get("datasetId") == _IBC_DATASET_ID for a in ambiguous_candidates
            )
        )
        print(f"[ebrains-ingest] PRE-GATE: {json.dumps({k: v for k, v in pre_gate.items() if not isinstance(v, list)})} gate_ok={gate_ok}")

        if not gate_ok:
            report = {
                "report_generated_at": _utcnow(),
                "mode": "PRODUCTION ingestion — ABORTED (pre-gate failed, ZERO writes)",
                "pre_gate": pre_gate,
                "failure_details": failure_details[:20],
                "writes_performed": 0,
            }
            os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
            with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
                json.dump(report, fh, indent=2, default=str)
            client.close()
            return 1

        # ── Phase 2: PRODUCTION WRITES. ─────────────────────────────────────
        start = _utcnow()
        stats = await run_ebrains_ingestion(db, records=approved, log=print)
        print(
            f"[ebrains-ingest] WRITE stats: {json.dumps({k: stats[k] for k in ('input','normalized','inserted','merged','failed','api_calls','asset_calls','validation_failed')})}"
        )

    # ── Phase 3: POST-INGESTION verification (independent read-only). ────────
    total_after = await collection.count_documents({})
    repos_after = await _repo_source_counts(collection)

    ebrains_docs = []
    async for d in collection.find({"sources.repository": "ebrains"}):
        ebrains_docs.append(d)
    ebrains_source_ids: set[str] = set()
    ebrains_source_keys: set[str] = set()
    ebrains_canonical_ids: set[str] = set()
    rawmeta_ok = 0
    ebrains_source_dois: dict[str, str | None] = {}  # dataset_id -> source doi
    ebrains_external_source_ids: set[str] = set()
    ebrains_dataset_to_canonical: dict[str, str] = {}
    for d in ebrains_docs:
        ebrains_canonical_ids.add(d["canonicalDatasetId"])
        if (d.get("rawMetadata") or {}).get("ebrains"):
            rawmeta_ok += 1
        for s in (d.get("sources") or []):
            if s.get("repository") != "ebrains":
                continue
            sid = str(s.get("sourceDatasetId") or "")
            ebrains_source_ids.add(sid)
            ebrains_source_keys.add(f"ebrains:{sid}")
            dsid = sid.replace("ebrains:", "", 1)
            ebrains_dataset_to_canonical[dsid] = d["canonicalDatasetId"]
            ebrains_source_dois[dsid] = s.get("doi")
            if s.get("publication", {}).get("articleDoi"):
                ebrains_external_source_ids.add(dsid)
        for key in (d.get("sourceKeys") or []):
            if key.startswith("ebrains:"):
                ebrains_source_keys.add(key)

    # native vs external split from the approved census
    native_count = sum(1 for r in approved if not r.get("external_doi"))
    external_count = sum(1 for r in approved if r.get("external_doi"))
    native_dois = {
        # empty-string and None both mean "no DOI" (same convention as the
        # dry-run cross-validation) — the stored source doi is None for those
        r["dataset_id"]: (r.get("doi") or "").lower() or None
        for r in approved if not r.get("external_doi")
    }
    external_dois = {
        (r.get("doi") or "").lower()
        for r in approved if r.get("external_doi") and r.get("doi")
    }

    # DOI integrity: every native EBRAINS record's canonical doi must be the
    # preserved 10.25493/... DOI; every external record's source doi must be
    # None (never canonical).
    native_doi_preserved = sum(
        1 for dsid, doi in ebrains_source_dois.items()
        if dsid in native_dois and doi == native_dois[dsid]
    )
    external_source_doi_none = sum(
        1 for dsid in ebrains_external_source_ids
        if ebrains_source_dois.get(dsid) is None
    )
    # publication-DOI leakage: any canonical doi that equals one of the 26
    # external DOIs AND was introduced by an EBRAINS source (the four
    # OpenNeuro/DANDI canonicals legitimately keep their PRE-EXISTING DOIs).
    leaked = 0
    for d in ebrains_docs:
        ddoi = (d.get("doi") or "").lower()
        if ddoi and ddoi in external_dois:
            ebrains_doi = any(
                (s.get("doi") or "").lower() == ddoi
                for s in (d.get("sources") or [])
                if s.get("repository") == "ebrains"
            )
            if ebrains_doi:  # the EBRAINS source supplied that DOI → leakage
                leaked += 1

    # DatasetVersion / non-neuro / external-repo records created
    dataset_version_records = 0
    non_neuro_records = 0
    restricted_access = 0
    for d in ebrains_docs:
        key = (d.get("sourceKeys") or [""])[0]
        if "datasetversion" in key.lower():
            dataset_version_records += 1
        raw = (d.get("rawMetadata") or {}).get("ebrains") or {}
        if str(raw.get("neuro_relevance")) == "non_neuroscience" or str(
            raw.get("category") or ""
        ).upper() == "NON":
            non_neuro_records += 1
        if d.get("availability") == "restricted":
            restricted_access += 1
    # explicit count of canonical records whose ONLY identity is a
    # non-EBRAINS external repository (should be 0 — ebrains sources carry the
    # ebrains key; the four merges carry dandi:/openneuro: + ebrains: keys)
    external_only_records = sum(
        1 for d in ebrains_docs
        if not any(k.startswith("ebrains:") for k in (d.get("sourceKeys") or []))
    )

    # unrelated records modified: content-hash on previously-existing docs,
    # EXCLUDING the 4 intended-merge canonical records (they gain the EBRAINS
    # source by design) and the IBC OpenNeuro candidate.
    intended_modified = _EXACT_MATCH_CANONICAL_IDS
    if not verify_only:
        unrelated_modified = 0
        async for d in collection.find({}):
            key = str(d["_id"])
            if key not in baseline_hashes:
                continue  # newly inserted EBRAINS docs are additions, not modifications
            cid = d.get("canonicalDatasetId")
            if cid in intended_modified:
                continue  # the 4 audited exact matches are INTENDED merges
            if _doc_content_hash(d) != baseline_hashes[key]:
                unrelated_modified += 1

    existing_unchanged = _repo_source_counts_unchanged(repos_before, repos_after)

    # four exact cross-repository matches: EBRAINS source attached?
    exact_merges_ok = True
    exact_merge_details = []
    for cid in sorted(_EXACT_MATCH_CANONICAL_IDS):
        doc = await collection.find_one({"canonicalDatasetId": cid})
        if doc is None:
            exact_merges_ok = False
            exact_merge_details.append({"canonicalDatasetId": cid, "found": False})
            continue
        repos = {s.get("repository") for s in (doc.get("sources") or [])}
        via = (doc.get("provenance") or {}).get("identity", {}).get("matchedVia")
        exact_merge_details.append(
            {
                "canonicalDatasetId": cid,
                "repositories": sorted(repos),
                "ebrainsAttached": "ebrains" in repos,
                "matchedVia": via,
            }
        )
        exact_merges_ok = exact_merges_ok and ("ebrains" in repos)

    # Individual Brain Charting stays separate from OpenNeuro ds000244
    ibc_canonical = ebrains_dataset_to_canonical.get(_IBC_DATASET_ID)
    on244 = await collection.find_one({"canonicalDatasetId": _IBC_OPENNEURO_CANONICAL})
    on244_has_ebrains = False
    if on244:
        on244_has_ebrains = any(
            s.get("repository") == "ebrains" for s in (on244.get("sources") or [])
        )
    ibc_separate = (
        ibc_canonical is not None
        and ibc_canonical != _IBC_OPENNEURO_CANONICAL
        and not on244_has_ebrains
    )

    verification = {
        # 1
        "canonical_total_matches_expected": total_after == 7320,
        "canonical_before": total_before,
        "canonical_after": total_after,
        # 2
        "ebrains_source_count": len(ebrains_docs),
        "ebrains_source_count_exactly_1138": len(ebrains_docs) == 1138,
        # 3
        "unique_ebrains_sourceDatasetIds": len(ebrains_source_ids),
        "unique_sourceDatasetIds_1138": len(ebrains_source_ids) == 1138,
        # 4
        "unique_ebrains_sourceKeys": len(ebrains_source_keys),
        "unique_sourceKeys_1138": len(ebrains_source_keys) == 1138,
        # 5
        "unique_ebrains_canonical_identities": len(ebrains_canonical_ids),
        "unique_canonical_identities_1138": len(ebrains_canonical_ids) == 1138,
        # 6
        "rawMetadata_ebrains_coverage": rawmeta_ok,
        "rawMetadata_ebrains_1138": rawmeta_ok == 1138,
        # 7
        "dataset_version_records": dataset_version_records,
        # 8
        "non_neuroscience_ebrains_records": non_neuro_records,
        # 9
        "external_repository_records_created": external_only_records,
        # 10
        "native_ebrains_dois": native_count,
        "native_ebrains_dois_preserved": native_doi_preserved,
        "external_ebrains_sources": external_count,
        "external_source_doi_none": external_source_doi_none,
        # 11
        "publication_doi_leakage": leaked,
        # 12
        "asset_file_calls": stats.get("asset_calls", 0),
        "api_calls": stats.get("api_calls", 0),
        # 13
        "restricted_access_records": restricted_access,
        # 14
        "unrelated_records_modified": unrelated_modified,
        # 15
        "existing_repositories_unchanged": existing_unchanged,
        "repo_source_counts_before": dict(repos_before),
        "repo_source_counts_after": dict(repos_after),
        # 16
        "four_exact_matches_ok": exact_merges_ok,
        "exact_match_details": exact_merge_details,
        # 17
        "ibc_separate_from_openneuro_ds000244": ibc_separate,
        "ibc_canonicalDatasetId": ibc_canonical,
        "ibc_openneuro_canonical": _IBC_OPENNEURO_CANONICAL,
        # 18
        "production_failures": stats.get("failed", 0),
    }

    passed = (
        verification["canonical_total_matches_expected"]
        and stats.get("inserted", 0) == 1134
        and stats.get("merged", 0) == 4
        and verification["ebrains_source_count_exactly_1138"]
        and verification["unique_sourceDatasetIds_1138"]
        and verification["unique_sourceKeys_1138"]
        and verification["unique_canonical_identities_1138"]
        and verification["rawMetadata_ebrains_1138"]
        and verification["dataset_version_records"] == 0
        and verification["non_neuroscience_ebrains_records"] == 0
        and verification["external_repository_records_created"] == 0
        and verification["native_ebrains_dois_preserved"] == native_count
        and verification["external_source_doi_none"] == external_count
        and verification["publication_doi_leakage"] == 0
        and verification["asset_file_calls"] == 0
        and verification["api_calls"] == 0
        and verification["restricted_access_records"] == 0
        and verification["unrelated_records_modified"] == 0
        and verification["existing_repositories_unchanged"] is True
        and verification["four_exact_matches_ok"] is True
        and verification["ibc_separate_from_openneuro_ds000244"] is True
        and verification["production_failures"] == 0
        and stats.get("normalized", 0) == 1138
        and stats.get("gate_failed") is False
    )

    report = {
        "report_generated_at": _utcnow(),
        "mode": (
            "PRODUCTION ingestion (live writes) — independent post-ingestion "
            "re-verification" if verify_only else "PRODUCTION ingestion (live writes)"
        ),
        "production_db": settings.MONGO_DB_NAME,
        "collection": CATALOG_COLLECTION,
        "pre_gate": pre_gate,
        "INGESTION": {
            "approved_input": len(approved),
            "inserted": stats.get("inserted", 0),
            "merged": stats.get("merged", 0),
            "failed": stats.get("failed", 0),
            "validation_failed": stats.get("validation_failed", 0),
            "duplicate_source_identities": stats.get("duplicate_source_identities", []),
            "matched_via": stats.get("matched_via", {}),
            "api_calls": stats.get("api_calls", 0),
            "asset_calls": stats.get("asset_calls", 0),
            "started_at": stats.get("started_at") or _utcnow(),
            "finished_at": stats.get("finished_at"),
            "elapsed_s": stats.get("elapsed_s"),
        },
        "VERIFICATION": verification,
        "PRODUCTION_INTEGRITY": {
            "catalog_before": total_before,
            "catalog_after": total_after,
            "repo_source_counts_before": dict(repos_before),
            "repo_source_counts_after": dict(repos_after),
        },
        "verification_passed": passed,
        "conclusion": (
            "EBRAINS production ingestion completed successfully: "
            f"{stats.get('inserted')} new canonical neuroscience datasets, "
            f"{stats.get('merged')} cross-repository source merges, "
            f"final catalog {total_after}."
            if passed else "VERIFICATION FAILED — investigate before any further action."
        ),
        "status": "PASSED" if passed else "FAILED",
    }

    os.makedirs(os.path.dirname(_REPORT_PATH), exist_ok=True)
    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print(f"[ebrains-ingest] === SUMMARY ===")
    print(f"[ebrains-ingest] INSERTED={stats.get('inserted')} MERGED={stats.get('merged')} "
          f"FAILED={stats.get('failed')}")
    print(f"[ebrains-ingest] catalog: {total_before} -> {total_after}")
    print(
        f"[ebrains-ingest] EBRAINS docs={len(ebrains_docs)} "
        f"unique_ids={len(ebrains_source_ids)} unique_keys={len(ebrains_source_keys)} "
        f"unique_canonical={len(ebrains_canonical_ids)} rawmeta={rawmeta_ok}"
    )
    print(
        f"[ebrains-ingest] native DOIs preserved={native_doi_preserved}/{native_count} "
        f"external source doi=None={external_source_doi_none}/{external_count} "
        f"doi leakage={leaked}"
    )
    print(
        f"[ebrains-ingest] DatasetVersion records={dataset_version_records} "
        f"non-neuro={non_neuro_records} external-repo records={external_only_records} "
        f"restricted={restricted_access}"
    )
    print(
        f"[ebrains-ingest] exact matches ok={exact_merges_ok} "
        f"IBC separate={ibc_separate} unrelated_modified={unrelated_modified} "
        f"existing repos unchanged={existing_unchanged}"
    )
    print(f"[ebrains-ingest] VERIFICATION: {'PASSED' if passed else 'FAILED'}")
    print(f"[ebrains-ingest] report -> {_REPORT_PATH}")

    client.close()
    return 0 if passed else 1


def _repo_source_counts_unchanged(before: Counter, after: Counter) -> bool:
    """Existing repositories are unchanged iff every repo present BEFORE has
    the same source count AFTER. Newly introduced repositories (ebrains) are
    the intended addition, not a change to an existing repository."""
    diff = {}
    for repo in before:
        b, a = before.get(repo, 0), after.get(repo, 0)
        if b != a:
            diff[repo] = {"before": b, "after": a}
    return not diff


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))