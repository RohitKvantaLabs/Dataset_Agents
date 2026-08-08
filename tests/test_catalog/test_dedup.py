"""Unit tests for cross-repository dedup identity + merge (pure logic)."""

from app.catalog.dedup import (
    evaluate_identity,
    extract_cross_references,
    merge_source_into_canonical,
)
from app.catalog.normalize import build_source_record, canonical_record_from_source

# Shared fixture nodes for the two repositories of the SAME dataset.
_OPENNEURO_NODE = {
    "id": "ds000001",
    "name": "ds001",
    "created": "2016-10-12T23:40:22.670Z",
    "publishDate": "2016-10-12T23:40:22.670Z",
    "public": True,
    "metadata": {
        "modalities": ["mri"],
        "species": "Human",
        "ages": [26, 24, 27, 20, 22],
        "tasksCompleted": ["balloon analog risk task"],
    },
    "latestSnapshot": {
        "tag": "1.0.0",
        "created": "2020-05-14T14:56:55.000Z",
        "size": 2416199965,
        "readme": "README",
        "description": {
            "Name": "Balloon Analog Risk-taking Task",
            "Authors": ["Tom Schonberg", "Russell A. Poldrack"],
            "DatasetDOI": "10.18112/openneuro.ds000001.v1.0.0",
            "DatasetType": "raw",
            "License": "CC0",
        },
        "summary": {
            "modalities": ["mri"],
            "subjects": ["01", "02", "03", "04", "05"],
            "tasks": ["balloon analog risk task"],
            "totalFiles": 133,
        },
        "related": None,
    },
    "snapshots": [{"id": "ds000001:1.0.0", "tag": "1.0.0", "created": "2020-05-14T14:56:55.000Z"}],
}

_NEMAR_NODE = {
    "id": "nemar-xyz",
    "name": "balloon",
    "created": "2021-01-01T00:00:00.000Z",
    "publishDate": "2021-01-01T00:00:00.000Z",
    "public": True,
    "metadata": {
        "modalities": ["mri"],
        "species": "Human",
        "ages": [26, 24, 27, 20, 22],
        "tasksCompleted": ["balloon analog risk task"],
    },
    "latestSnapshot": {
        "tag": "1.0",
        "created": "2021-01-01T00:00:00.000Z",
        "size": 2416199965,
        "readme": "README",
        "description": {
            "Name": "Balloon Analog Risk-taking Task",
            "Authors": ["Tom Schonberg", "Russell A. Poldrack"],
            "DatasetDOI": "10.18112/openneuro.ds000001.v1.0.0",  # same DOI
            "DatasetType": "raw",
            "License": "CC0",
        },
        "summary": {
            "modalities": ["mri"],
            "subjects": ["01", "02", "03", "04", "05"],
            "tasks": ["balloon analog risk task"],
            "totalFiles": 133,
        },
        "related": None,
    },
    "snapshots": [{"id": "nemar-xyz:1.0", "tag": "1.0", "created": "2021-01-01T00:00:00.000Z"}],
}

_DIFFERENT_NODE = {
    "id": "ds000999",
    "name": "ds999",
    "created": "2016-10-12T23:40:22.670Z",
    "publishDate": "2016-10-12T23:40:22.670Z",
    "public": True,
    "metadata": {
        "modalities": ["eeg"],
        "species": "Human",
        "ages": [30],
        "tasksCompleted": ["different task"],
    },
    "latestSnapshot": {
        "tag": "1.0.0",
        "created": "2020-05-14T14:56:55.000Z",
        "size": 1000,
        "readme": "R",
        "description": {
            "Name": "A Completely Different Study",
            "Authors": ["Someone Else"],
            "DatasetDOI": "10.18112/openneuro.ds000999.v1.0.0",
            "DatasetType": "raw",
            "License": "PDDL",
        },
        "summary": {"modalities": ["eeg"], "subjects": ["01"], "tasks": ["different task"], "totalFiles": 1},
        "related": None,
    },
    "snapshots": [{"id": "ds000999:1.0.0", "tag": "1.0.0", "created": "2020-05-14T14:56:55.000Z"}],
}


def _source(node, repository="openneuro"):
    src = build_source_record(node)
    src["repository"] = repository
    if repository != "openneuro":
        src["sourceUrl"] = f"https://{repository}.example.org/{src['sourceDatasetId']}"
        src["provenance"]["source"] = repository
    return src


class TestEvaluateIdentity:
    def test_doi_match_merges(self):
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        incoming = _source(_NEMAR_NODE, repository="nemar")
        result = evaluate_identity(existing, incoming)
        assert result["match"] is True
        assert result["matchedVia"] == "doi"

    def test_source_url_match(self):
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        incoming = _source(_OPENNEURO_NODE)  # same repo + url + key, no DOI
        incoming["doi"] = None
        result = evaluate_identity(existing, incoming)
        assert result["match"] is True
        assert result["matchedVia"] in ("source_key", "source_url")

    def test_doi_conflict_never_merges(self):
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        other = _source(_DIFFERENT_NODE)
        result = evaluate_identity(existing, other)
        assert result["match"] is False
        assert result["ambiguous"] is True

    def test_similar_title_alone_is_ambiguous(self):
        # Different source (different repo/id/url), SAME normalized title, no
        # other overlapping signal → ambiguous, NEVER merged.
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        existing["doi"] = None
        incoming = _source(_OPENNEURO_NODE, repository="nemar")
        incoming["sourceDatasetId"] = "nemar-xyz"
        incoming["sourceUrl"] = "https://nemar.example.org/nemar-xyz"
        incoming["doi"] = None
        incoming["authors"] = []
        incoming["participantCount"] = None
        incoming["modality"] = []
        incoming["derived"]["publicationYear"] = None
        incoming["publicationYear"] = None
        existing["authors"] = []
        existing["participantCount"] = None
        existing["modality"] = []
        existing["publicationYear"] = None
        result = evaluate_identity(existing, incoming)
        assert result["match"] is False
        assert result["ambiguous"] is True

    def test_strong_multi_field_match(self):
        # Same title + authors + count + modality across repos, no DOI/URL/key
        # overlap → strong multi-field identity.
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        existing["doi"] = None
        incoming = _source(_OPENNEURO_NODE, repository="nemar")
        incoming["sourceDatasetId"] = "nemar-xyz"
        incoming["sourceUrl"] = "https://nemar.example.org/nemar-xyz"
        incoming["doi"] = None
        result = evaluate_identity(existing, incoming)
        assert result["match"] is True
        assert result["matchedVia"] == "multi_field"

    def test_mockdoi_placeholder_is_not_identity(self):
        """Live regression: OpenNeuro stores the literal string 'mockdoi' in
        DatasetDOI for some datasets. 5 unrelated datasets shared it — fake
        DOIs must never merge."""
        a = _source(_OPENNEURO_NODE)
        a["sourceDatasetId"] = "ds001563"
        a["sourceUrl"] = "https://openneuro.org/datasets/ds001563"
        b = _source(_OPENNEURO_NODE)
        b["sourceDatasetId"] = "ds002181"
        b["sourceUrl"] = "https://openneuro.org/datasets/ds002181"
        for s in (a, b):
            s["doi"] = None
            s["title"] = "Different Dataset"
        a["title"] = "CMRR Workshop"
        b["title"] = "CRYPTO and PROVIDE EEG Baseline Data"
        existing = canonical_record_from_source(a)
        result = evaluate_identity(existing, b)
        assert result["match"] is False
        assert result["matchedVia"] is None
        # normalize_doi rejects the placeholder outright
        from app.catalog.schema import normalize_doi
        assert normalize_doi("mockdoi") is None
        assert normalize_doi("10.18112/openneuro.ds003714.v1.0.1") == "10.18112/openneuro.ds003714.v1.0.1"

    def test_same_repo_distinct_ids_never_multi_field_merge(self):
        """Live regression: ds001566 and ds003714 are distinct OpenNeuro
        datasets both titled 'Test' with same count+modality. Within a repo
        the ID is authoritative → no multi-field merge."""
        a = _source(_OPENNEURO_NODE)
        a["sourceDatasetId"] = "ds001566"
        a["sourceUrl"] = "https://openneuro.org/datasets/ds001566"
        a["doi"] = None
        a["authors"] = []
        b = _source(_OPENNEURO_NODE)
        b["sourceDatasetId"] = "ds003714"
        b["sourceUrl"] = "https://openneuro.org/datasets/ds003714"
        b["doi"] = "10.18112/openneuro.ds003714.v1.0.1"
        b["authors"] = ["Avijit Chowdhury"]
        existing = canonical_record_from_source(a)
        existing["doi"] = None
        result = evaluate_identity(existing, b)
        assert result["match"] is False  # same repo → never fuzzy-merge
        assert result["ambiguous"] is True  # title matches but identity weak

    def test_repository_id_alone_is_not_identity(self):
        # Different repos, same-looking source id, nothing else in common.
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        incoming = _source(_DIFFERENT_NODE, repository="nemar")
        incoming["sourceDatasetId"] = "ds000001"  # same string, different dataset
        incoming["doi"] = None
        existing["doi"] = None
        result = evaluate_identity(existing, incoming)
        assert result["match"] is False  # repo id alone proves nothing
        assert result["ambiguous"] is False  # no title overlap → not even ambiguous


class TestExtractCrossReferences:
    def test_recognizes_dandi_pattern(self):
        node = dict(_OPENNEURO_NODE)
        node["latestSnapshot"] = dict(node["latestSnapshot"])
        node["latestSnapshot"]["related"] = [
            {"description": "Available on DANDI at https://dandiarchive.org/dandiset/000123", "kind": "link", "relation": "seeAlso"}
        ]
        src = _source(node)
        refs = extract_cross_references(src)
        assert ("dandi", "000123") in {(r["repository"], r["sourceDatasetId"]) for r in refs}

    def test_ignores_mentions(self):
        node = dict(_OPENNEURO_NODE)
        node["latestSnapshot"] = dict(node["latestSnapshot"])
        node["latestSnapshot"]["related"] = [
            {"description": "This paper discusses dandiarchive.org in passing", "kind": "paper", "relation": "cites"}
        ]
        src = _source(node)
        assert extract_cross_references(src) == []


class TestMergeSourceIntoCanonical:
    def test_merge_attaches_source_never_discards(self):
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        nemar = _source(_NEMAR_NODE, repository="nemar")
        merged = merge_source_into_canonical(existing, nemar, "doi")
        assert len(merged["sources"]) == 2
        repos = {s["repository"] for s in merged["sources"]}
        assert repos == {"openneuro", "nemar"}
        assert merged["sourceKeys"] == ["openneuro:ds000001", "nemar:nemar-xyz"]

    def test_merge_preserves_raw_metadata_per_repo(self):
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        nemar = _source(_NEMAR_NODE, repository="nemar")
        merged = merge_source_into_canonical(existing, nemar, "doi")
        assert "openneuro" in merged["rawMetadata"]
        assert "nemar" in merged["rawMetadata"]
        assert merged["rawMetadata"]["openneuro"]["id"] == "ds000001"
        assert merged["rawMetadata"]["nemar"]["id"] == "nemar-xyz"

    def test_merge_preserves_repo_specific_metadata(self):
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        nemar = _source(_NEMAR_NODE, repository="nemar")
        nemar["brainRegions"] = ["hippocampus"]  # NEMAR provides region
        merged = merge_source_into_canonical(existing, nemar, "doi")
        assert merged["brainRegions"] == ["hippocampus"]
        assert merged["modality"] == ["mri"]

    def test_merge_updates_provenance_matched_via(self):
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        nemar = _source(_NEMAR_NODE, repository="nemar")
        merged = merge_source_into_canonical(existing, nemar, "doi")
        assert merged["provenance"]["identity"]["matchedVia"] == "doi"

    def test_same_source_refresh_no_duplicate_entry(self):
        existing = canonical_record_from_source(_source(_OPENNEURO_NODE))
        refresh = _source(_OPENNEURO_NODE)
        merged = merge_source_into_canonical(existing, refresh, "source_key")
        assert len(merged["sources"]) == 1  # refreshed, not duplicated
        assert len(merged["sourceKeys"]) == 1
