"""ADNI sourceUrl verification (2026-08-17) — READ-ONLY web check.

Option A identity model: every ADNI sourceUrl IS the census source_url verbatim
(the real official ADNI/LONI page) — synthetic per-product URLs were removed.
This tool verifies:

  1. NO synthetic URL remains — source["sourceUrl"] == record["source_url"]
     for every one of the 122 approved census records.
  2. URL is NOT an identity signal for ADNI — source_identity()["sourceUrlNorm"]
     is None, so co-located products sharing a page never merge.
  3. Every unique official ADNI/LONI page resolves (bounded reads of public
     documentation pages only).

NO dataset files, NO restricted IDA data, NO MongoDB access, NO writes.

Output: ../trace_artifacts/adni_ingestion_20260817/adni_url_verify_20260817.json
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from datetime import datetime, timezone

import httpx

from app.catalog.dedup import source_identity
from app.catalog.normalize import build_adni_source_record

_CENSUS_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "adni_census_20260817", "adni_census.json",
    )
)
_OUT_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..",
        "trace_artifacts", "adni_ingestion_20260817",
        "adni_url_verify_20260817.json",
    )
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
_MAX_BYTES = 8192  # documentation-page title/context only — bounded read
_DELAY = 0.35


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _title(html: str) -> str | None:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    return re.sub(r"\s+", " ", m.group(1)).strip()[:120] or None


async def _probe(client: httpx.AsyncClient, url: str) -> dict:
    """Bounded GET of a public page — status, final URL, host, title, type."""
    try:
        async with client.stream("GET", url) as resp:
            final = str(resp.url)
            chunks: list[bytes] = []
            async for chunk in resp.aiter_bytes():
                chunks.append(chunk)
                if sum(len(c) for c in chunks) >= _MAX_BYTES:
                    break
            body = b"".join(chunks)
        from urllib.parse import urlparse

        final_host = urlparse(final).hostname or ""
        return {
            "status": resp.status_code,
            "final_url": final,
            "final_host": final_host,
            "content_type": resp.headers.get("content-type"),
            "title": _title(body.decode("utf-8", "ignore")),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": None,
            "final_url": url,
            "final_host": None,
            "content_type": None,
            "title": None,
            "error": f"{type(exc).__name__}: {exc}",
        }


async def main() -> int:
    records = json.load(open(_CENSUS_PATH, encoding="utf-8"))["datasets"]

    # ── 1) No synthetic URLs: every sourceUrl IS the census URL verbatim. ────
    synthetic: list[dict] = []
    identity_signals: list[dict] = []
    unique_urls: dict[str, list[str]] = {}
    for r in records:
        source = build_adni_source_record(r)
        if source["sourceUrl"] != r["source_url"]:
            synthetic.append(
                {
                    "stableIdentifier": r["stable_identifier"],
                    "sourceUrl": source["sourceUrl"],
                    "census_source_url": r["source_url"],
                }
            )
        ident = source_identity(source)
        if ident["sourceUrlNorm"] is not None:
            identity_signals.append(
                {
                    "stableIdentifier": r["stable_identifier"],
                    "sourceUrlNorm": ident["sourceUrlNorm"],
                }
            )
        unique_urls.setdefault(str(r["source_url"]), []).append(
            r["stable_identifier"]
        )

    # ── 2) Probe every UNIQUE official URL (bounded, read-only). ─────────────
    verified: list[dict] = []
    async with httpx.AsyncClient(
        headers=_HEADERS, follow_redirects=True, timeout=25.0
    ) as client:
        for url, ids in unique_urls.items():
            verified.append(
                {
                    "official_url": url,
                    "products": len(ids),
                    "probe": await _probe(client, url),
                }
            )
            await asyncio.sleep(_DELAY)

    ok = [v for v in verified if (v["probe"].get("status") or 0) == 200]
    not_ok = [v for v in verified if (v["probe"].get("status") or 0) != 200]

    report = {
        "report_generated_at": _utcnow(),
        "mode": "READ-ONLY web verification (no DB, no writes, no data files)",
        "model": "Option A — sourceUrl = census source_url verbatim; URL is never an ADNI identity signal",
        "census_records_checked": len(records),
        "synthetic_urls_remaining": len(synthetic),
        "synthetic_url_details": synthetic[:20],
        "url_identity_signals": len(identity_signals),
        "url_identity_signal_details": identity_signals[:20],
        "unique_official_urls": len(unique_urls),
        "shared_official_url_counts": {
            url: len(ids)
            for url, ids in sorted(unique_urls.items(), key=lambda kv: -len(kv[1]))
            if len(ids) > 1
        },
        "verified_urls": verified,
        "SUMMARY": {
            "census_records_checked": len(records),
            "synthetic_urls_remaining": len(synthetic),
            "url_identity_signals": len(identity_signals),
            "unique_official_urls_checked": len(verified),
            "official_resolving_200": len(ok),
            "official_http_failures": len(not_ok),
            "official_failure_details": [
                {"url": v["official_url"], "probe": v["probe"]}
                for v in not_ok
            ],
        },
    }

    os.makedirs(os.path.dirname(_OUT_PATH), exist_ok=True)
    with open(_OUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    print(f"[adni-url-verify] records={len(records)} "
          f"synthetic_urls_remaining={len(synthetic)} "
          f"identity_signals={len(identity_signals)}")
    print(f"[adni-url-verify] unique official URLs checked={len(verified)} "
          f"resolving={len(ok)} failures={len(not_ok)}")
    print(f"[adni-url-verify] report -> {_OUT_PATH}")
    return 0 if not synthetic and not identity_signals else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))