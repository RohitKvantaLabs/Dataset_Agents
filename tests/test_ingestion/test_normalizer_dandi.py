"""
DANDI repository normalizer — live list-endpoint shape (stabilization Phase 1).

Verified live 2026-08-04: ``GET /api/dandisets/?search=...`` returns records
where the title lives on the VERSION OBJECT itself (``draft_version.name``),
not inside ``draft_version.metadata``. Before the fix, every record fell back
to the placeholder title "DANDI Dataset {id}".
"""
from datetime import timezone

from app.ingestion.normalizer import _parse_iso, _repository_dandi


def _list_endpoint_record(identifier="000623", name="Real DANDI title"):
    """Shape returned by the live /api/dandisets/ list & search endpoints."""
    return {
        "identifier": identifier,
        "created": "2023-08-31T18:13:43.895059Z",
        "modified": "2024-01-30T20:30:16.845094Z",
        "contact_person": "Someone",
        "draft_version": {
            "asset_count": 42,
            "created": "2023-08-31T18:13:43.895059Z",
            "modified": "2024-01-30T20:30:16.845094Z",
            "name": name,
            "size": 123456,
            "status": "active",
            "version": "draft",
        },
        "most_recent_published_version": {},
        "embargo_status": "open",
        "star_count": 0,
        "is_starred": False,
    }


def test_list_endpoint_title_from_draft_version_name():
    """draft_version.name (no nested metadata) must produce the real title."""
    ds = _repository_dandi(_list_endpoint_record(name="Multimodal single-neuron fMRI"))
    assert ds is not None
    assert ds.title == "Multimodal single-neuron fMRI"
    assert "DANDI Dataset" not in ds.title


def test_zero_padded_identifier_in_canonical_url():
    """DANDI uses zero-padded dandiset paths (dandiset/000623, not /623)."""
    ds = _repository_dandi(_list_endpoint_record(identifier="000623"))
    assert ds.url == "https://dandiarchive.org/dandiset/000623"


def test_version_and_timestamps_from_version_object():
    ds = _repository_dandi(_list_endpoint_record())
    assert ds.version == "draft"
    assert ds.updated_at is not None
    assert ds.published_at is not None


def test_full_version_endpoint_metadata_still_wins():
    """Full-version endpoints nest under metadata — that path must keep working."""
    rec = _list_endpoint_record()
    rec["draft_version"]["metadata"] = {"name": "Metadata Preferred Name"}
    ds = _repository_dandi(rec)
    assert ds.title == "Metadata Preferred Name"


def test_missing_identifier_returns_none():
    rec = _list_endpoint_record()
    del rec["identifier"]
    assert _repository_dandi(rec) is None


def test_parse_iso_naive_timestamp_becomes_utc_aware():
    """Naive timestamps (e.g. NeuroVault) must be UTC-aware so Stage 6 dedup
    never compares naive vs aware datetimes (stabilization Phase 3)."""
    dt = _parse_iso("2017-06-29 11:20:37.529906")
    assert dt is not None
    assert dt.tzinfo is not None
    assert dt.utcoffset() == timezone.utc.utcoffset(None)


def test_parse_iso_z_suffix_is_aware():
    dt = _parse_iso("2024-01-30T20:30:16.845094Z")
    assert dt is not None and dt.tzinfo is not None


def test_parse_iso_garbage_returns_none():
    assert _parse_iso("not-a-date") is None
    assert _parse_iso(None) is None
    assert _parse_iso("") is None
