"""
Retrieval V2 Phase 3 — task-metadata backfill for existing datasets.

Re-runs the SAME Stage-3 enrichment evidence logic (vocabulary word-boundary
matching over dataset-owned text: title > keywords > description) against the
existing `datasets` collection and populates `task` ONLY where:

  - the record has no task value yet (never overwrites), and
  - the dataset's own content contains a TASK_VOCAB token (never fabricated).

Safety (Phase 19):
  - DRY-RUN by default. Pass --write to actually persist.
  - Low-confidence rule: vocabulary matching is deterministic; the only
    accepted evidence tiers are title / keywords / description — identical to
    what Stage 3 accepts at ingest. No weaker inference exists in this script.
  - Provenance: writes provenance.enrichment.task_source /
    provenance.enrichment_sources += ["backfill_vocab_task"] when provenance
    exists, preserving the existing trail (Phase 19 rule 6).
  - No destructive migration: only `$set: {task, updated_at}` (+ optional
    provenance sub-fields); nothing is deleted or rewritten.

Usage:
  python scripts/backfill_task_metadata.py             # dry-run + report JSON
  python scripts/backfill_task_metadata.py --write     # apply + report JSON
  python scripts/backfill_task_metadata.py --limit 50  # cap records processed

Report (stdout summary + artifacts/task_backfill_report_<ts>.json):
  total datasets, task populated before/after, still unknown,
  counts per canonical label, records changed, sample evidence per label.
"""

import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # Neuro-Agents/ so .env resolves

from app.config import get_settings  # noqa: E402
from app.data.vocab import match_task_labels  # noqa: E402
from app.db.mongo import get_db  # noqa: E402

COLLECTION_NAME = "datasets"
BATCH_SIZE = 200


def _evidence_labels(doc: dict) -> tuple[list[str], str]:
    """Same confidence order as Stage 3: title(90) > keywords(85) > description(40).

    Returns (labels, source_name) from the first non-empty tier, or ([], "")."""
    tiers = [
        ("title", str(doc.get("title") or "").lower()),
        ("keywords", " ".join(doc.get("keywords") or []).lower()),
        ("description", re.sub(r"<[^>]+>", " ", str(doc.get("description") or "")).lower()),
    ]
    for name, text in tiers:
        if not text.strip():
            continue
        labels = match_task_labels(text)
        if labels:
            return labels, name
    return [], ""


def _sample_evidence_text(doc: dict, source: str, limit: int = 120) -> str:
    raw = {
        "title": doc.get("title"),
        "keywords": doc.get("keywords"),
        "description": doc.get("description"),
    }.get(source, "")
    text = re.sub(r"\s+", " ", str(raw or ""))[:limit]
    return text


async def run(write: bool, limit: int | None) -> dict:
    settings = get_settings()
    db = get_db()
    collection = db[COLLECTION_NAME]

    total = await collection.count_documents({})
    query = {"$or": [{"task": None}, {"task": {"$exists": False}}, {"task": ""}]}
    cursor = collection.find(query)
    if limit:
        cursor = cursor.limit(limit)

    before_populated = await collection.count_documents(
        {"task": {"$exists": True, "$nin": [None, ""]}}
    )

    stats = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mongo_db": settings.MONGO_DB_NAME,
        "mode": "WRITE" if write else "DRY-RUN",
        "total_datasets": total,
        "task_populated_before": before_populated,
        "candidates_without_task": 0,
        "records_changed": 0,
        "still_unknown_after": 0,
        "task_values_by_label": {},
        "evidence_by_source": {"title": 0, "keywords": 0, "description": 0},
        "samples": [],
    }
    ops = []

    async for doc in cursor:
        stats["candidates_without_task"] += 1
        labels, source = _evidence_labels(doc)
        if not labels:
            continue
        label = labels[0]
        stats["task_values_by_label"][label] = stats["task_values_by_label"].get(label, 0) + 1
        stats["evidence_by_source"][source] = stats["evidence_by_source"].get(source, 0) + 1
        stats["records_changed"] += 1
        if len(stats["samples"]) < 10:
            stats["samples"].append(
                {
                    "source": doc.get("source"),
                    "source_id": doc.get("source_id"),
                    "title": (doc.get("title") or "")[:100],
                    "inferred_task": label,
                    "evidence_source": source,
                    "evidence_text": _sample_evidence_text(doc, source),
                }
            )
        if write:
            from pymongo import UpdateOne

            set_payload = {"task": label, "updated_at": datetime.now(timezone.utc).isoformat()}
            update: dict = {"$set": set_payload}
            prov = doc.get("provenance")
            if isinstance(prov, dict):
                # Preserve the existing enrichment trail (Phase 19 rule 6).
                update["$set"]["provenance.enrichment.task_source"] = f"backfill:{source}"
                sources_list = prov.get("enrichment_sources")
                if isinstance(sources_list, list) and "backfill_vocab_task" not in sources_list:
                    update["$addToSet"] = {"provenance.enrichment_sources": "backfill_vocab_task"}
                elif not isinstance(sources_list, list):
                    update["$set"]["provenance.enrichment_sources"] = ["backfill_vocab_task"]
            ops.append(UpdateOne({"_id": doc["_id"]}, update))

    if write and ops:
        for i in range(0, len(ops), BATCH_SIZE):
            await collection.bulk_write(ops[i : i + BATCH_SIZE], ordered=False)

    after_populated = (
        before_populated + stats["records_changed"]
        if write
        else before_populated + stats["records_changed"]
    )
    stats["task_populated_after_projected"] = after_populated
    stats["still_unknown_after"] = total - after_populated

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="apply changes (default: dry-run)")
    parser.add_argument("--limit", type=int, default=None, help="cap number of records scanned")
    args = parser.parse_args()

    stats = asyncio.run(run(write=args.write, limit=args.limit))

    print(json.dumps(stats, indent=2))
    out_dir = os.path.join("..", "artifacts")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(
        out_dir, f"task_backfill_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(stats, fh, indent=2)
    print(f"\nreport written: {out_path}")


if __name__ == "__main__":
    main()
