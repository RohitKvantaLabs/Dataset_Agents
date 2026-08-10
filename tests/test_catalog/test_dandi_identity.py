"""DANDI cross-repository identity + merge behavior (pure dedup rules)."""

from app.catalog.dedup import evaluate_identity, merge_source_into_canonical
from app.catalog.normalize import build_dandi_source_record, canonical_record_from_source

from ._dandi_fixtures import published_version, version_record


def _dandi_source(identifier="000003", **overrides):
    src = build_dandi_source_record(version_record(identifier=f"DANDI:{identifier}"))
    src["sourceDatasetId"] = identifier
    src["sourceUrl"] = f"https://dandiarchive.org/dandiset/{identifier}/draft"
    for k, v in overrides.items():
        src[k] = v
    return src


class TestDandiIdentity:
    def test_same_repo_distinct_ids_remain_distinct(self):
        """Within DANDI the repository ID is authoritative — two distinct IDs
        are distinct datasets even if titles/counts/modality match."""
        a = canonical_record_from_source(_dandi_source("000003", title="Resting State fMRI", modality=["electrophysiology"]))
        b = _dandi_source("000004", title="Resting State fMRI", modality=["electrophysiology"])
        result = evaluate_identity(a, b)
        assert result["match"] is False
        assert result["ambiguous"] is True      # title overlap, never merged
        # distinct canonical identities
        assert a["canonicalDatasetId"] != canonical_record_from_source(b)["canonicalDatasetId"]

    def test_same_dandi_source_reingest_is_refresh_not_duplicate(self):
        existing = canonical_record_from_source(_dandi_source("000003"))
        incoming = _dandi_source("000003")      # same repo + id
        result = evaluate_identity(existing, incoming)
        assert result["match"] is True
        assert result["matchedVia"] in ("source_url", "source_key")
        merged = merge_source_into_canonical(existing, incoming, result["matchedVia"])
        assert len(merged["sources"]) == 1      # refreshed, never duplicated
        assert merged["sourceKeys"] == ["dandi:000003"]

    def test_title_similarity_alone_is_not_identity(self):
        """A DANDI record with the same normalized title as an existing
        OpenNeuro record but no other strong signal must NOT merge."""
        existing = {
            "canonicalDatasetId": "ns-openneuro",
            "title": "Resting State fMRI",
            "doi": None,
            "authors": [],
            "participantCount": None,
            "modality": [],
            "publicationYear": None,
            "sourceKeys": ["openneuro:ds000001"],
            "sources": [
                {"repository": "openneuro", "sourceDatasetId": "ds000001",
                 "sourceUrl": "https://openneuro.org/datasets/ds000001"}
            ],
            "rawMetadata": {"openneuro": {}},
            "provenance": {"identity": {"sourceUrlNorm": "http://openneuro.org/datasets/ds000001"}},
        }
        incoming = _dandi_source("000003", title="Resting State fMRI")
        incoming["authors"] = []
        incoming["participantCount"] = None
        incoming["modality"] = []
        incoming["derived"]["publicationYear"] = None
        result = evaluate_identity(existing, incoming)
        assert result["match"] is False        # weak signals → keep separate
        assert result["ambiguous"] is True

    def test_strong_multi_field_cross_repo_match(self):
        """A DANDI source CAN attach to an existing canonical record from
        another repo when identity is strong (exact title + ≥2 signals)."""
        existing = {
            "canonicalDatasetId": "ns-openneuro",
            "title": "Shared Recording Study",
            "doi": None,
            "authors": ["Doe, Jane"],
            "participantCount": 16,
            "modality": ["electrophysiology"],
            "publicationYear": 2024,
            "sourceKeys": ["openneuro:ds000001"],
            "sources": [
                {"repository": "openneuro", "sourceDatasetId": "ds000001",
                 "sourceUrl": "https://openneuro.org/datasets/ds000001"}
            ],
            "rawMetadata": {"openneuro": {}},
            "provenance": {"identity": {"sourceUrlNorm": "http://openneuro.org/datasets/ds000001"}},
        }
        incoming = _dandi_source(
            "000003",
            title="Shared Recording Study",
            authors=["Doe, Jane"],
            modality=["electrophysiology"],
        )
        incoming["participantCount"] = 16
        incoming["derived"]["publicationYear"] = 2024
        result = evaluate_identity(existing, incoming)
        assert result["match"] is True
        assert result["matchedVia"] == "multi_field"

    def test_dandi_doi_is_never_a_dedup_key(self):
        """Even when a (published) DANDI DOI exists, it is provenance only —
        a DANDI source never merges via DOI nor conflicts via DOI."""
        incoming = _dandi_source("000003")
        incoming["doi"] = None                       # canonical doi always None
        incoming["snapshot"]["publishedDoi"] = "10.48324/dandi.000003/0.260218.2052"
        existing = {
            "canonicalDatasetId": "ns-other",
            "title": "Completely Different Dataset",
            "doi": "10.1016/j.neuron.2018.11.002",
            "authors": [],
            "participantCount": None,
            "modality": [],
            "publicationYear": None,
            "sourceKeys": ["openneuro:ds000999"],
            "sources": [
                {"repository": "openneuro", "sourceDatasetId": "ds000999",
                 "sourceUrl": "https://openneuro.org/datasets/ds000999"}
            ],
            "rawMetadata": {"openneuro": {}},
            "provenance": {"identity": {}},
        }
        result = evaluate_identity(existing, incoming)
        # No DOI on the DANDI side → no DOI conflict, no DOI merge; title
        # differs → not ambiguous either.
        assert result["match"] is False
        assert result["matchedVia"] is None


class TestDandiMerge:
    def test_same_canonical_dataset_multiple_sources(self):
        existing = {
            "canonicalDatasetId": "ns-openneuro",
            "title": "Shared Recording Study",
            "doi": None,
            "authors": ["Doe, Jane"],
            "participantCount": 16,
            "modality": ["electrophysiology", "behavior"],
            "sourceKeys": ["openneuro:ds000001"],
            "sources": [
                {"repository": "openneuro", "sourceDatasetId": "ds000001",
                 "sourceUrl": "https://openneuro.org/datasets/ds000001"}
            ],
            "rawMetadata": {"openneuro": {"id": "ds000001"}},
            "provenance": {"identity": {}},
        }
        incoming = _dandi_source("000003", title="Shared Recording Study")
        merged = merge_source_into_canonical(existing, incoming, "multi_field")

        repos = {s["repository"] for s in merged["sources"]}
        assert repos == {"openneuro", "dandi"}
        assert set(merged["sourceKeys"]) == {"openneuro:ds000001", "dandi:000003"}
        assert set(merged["rawMetadata"].keys()) == {"openneuro", "dandi"}

    def test_merge_preserves_dandi_specific_metadata(self):
        existing = {
            "canonicalDatasetId": "ns-openneuro",
            "title": "Shared Recording Study",
            "doi": None,
            "authors": ["Doe, Jane"],
            "participantCount": 16,
            "modality": ["electrophysiology"],
            "sourceKeys": ["openneuro:ds000001"],
            "sources": [
                {"repository": "openneuro", "sourceDatasetId": "ds000001",
                 "sourceUrl": "https://openneuro.org/datasets/ds000001"}
            ],
            "rawMetadata": {"openneuro": {}},
            "provenance": {"identity": {}},
        }
        incoming = _dandi_source("000003", title="Shared Recording Study")
        incoming["brainRegions"] = ["Hippocampus"]     # DANDI provides regions
        incoming["disease"] = ["Alzheimer's disease"]
        merged = merge_source_into_canonical(existing, incoming, "multi_field")
        assert merged["brainRegions"] == ["Hippocampus"]
        assert merged["disease"] == ["Alzheimer's disease"]

    def test_published_doi_preserved_through_merge(self):
        pub = published_version()
        existing = {
            "canonicalDatasetId": "ns-x",
            "title": "Shared Recording Study",
            "doi": None,
            "authors": [],
            "sourceKeys": ["openneuro:ds000001"],
            "sources": [
                {"repository": "openneuro", "sourceDatasetId": "ds000001",
                 "sourceUrl": "https://openneuro.org/datasets/ds000001"}
            ],
            "rawMetadata": {"openneuro": {}},
            "provenance": {"identity": {}},
        }
        incoming = _dandi_source("000003", title="Shared Recording Study")
        incoming = build_dandi_source_record(version_record(identifier="DANDI:000003"), published=pub)
        merged = merge_source_into_canonical(existing, incoming, "multi_field")
        dandi_src = next(s for s in merged["sources"] if s["repository"] == "dandi")
        assert dandi_src["snapshot"]["publishedDoi"] == "10.48324/dandi.000003/0.260218.2052"
        # canonical DOI stays None — the DANDI version DOI is not global identity
        assert merged.get("doi") is None
