"""Unit tests for NeuroMorpho → canonical source normalization (pure logic, no I/O).

Covers the 20 required test cases:
 1. Archive with one publication
 2. Archive with multiple publications
 3. Archive containing 15 PMIDs, proving separate groups
 4. Negative PMID placeholder
 5. DOI-only contribution
 6. Same archive + same PMID across many neurons → ONE dataset
 7. Same archive + different PMID → DIFFERENT datasets
 8. Same archive + PMID + DOI → PMID is primary contribution identity
 9. Deterministic sourceDatasetId
10. Species aggregation
11. Brain-region aggregation
12. Cell-type aggregation
13. Age-range handling
14. Sex aggregation
15. neuronCount aggregation
16. rawMetadata.neuromorpho preservation
17. existing OpenNeuro/DANDI/NEMAR records unaffected
18. cross-repository identity behavior
19. duplicate source-key detection
20. complete grouping count fixture
"""

from app.catalog.normalize import (
    NEUROMORPHO_REPOSITORY,
    build_neuromorpho_source_record,
    canonical_record_from_source,
    group_neuromorpho_neurons,
    _real_pmid,
    _real_doi,
)
from app.catalog.schema import (
    normalize_doi,
    validate_canonical_record,
    normalize_url_key,
)

from ._neuromorpho_fixtures import (
    neuron,
    single_archive_one_publication,
    single_archive_two_publications,
    placeholders_only,
    mixed_real_and_placeholder,
    many_neurons_same_contribution,
    doi_only_contribution,
)


# ─────────────────────────────────────────────────────────────────────────────
# Case 1: Archive with one publication
# ─────────────────────────────────────────────────────────────────────────────


class TestCase1SingleArchiveOnePublication:
    def test_groups_into_one_contribution(self):
        neurons = single_archive_one_publication()
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 1
        g = groups[0]
        assert g["archive"] == "Mallick"
        assert g["pmid"] == "21228908"
        assert g["neuron_count"] == 3

    def test_source_record(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert src["repository"] == "neuromorpho"
        assert src["sourceDatasetId"] == "neuromorpho:Mallick:pmid:21228908"
        assert src["sourceUrl"] == "https://neuromorpho.org/archive/Mallick/pmid/21228908"
        assert src["title"] == "Mallick"
        assert src["doi"] == "10.1007/s00429-011-0305-0"
        assert src["neuronCount"] == 3

    def test_source_url_is_contribution_unique(self):
        """sourceUrl must be contribution-unique — the bare archive URL is
        shared by every contribution in an archive and collapses distinct
        publications through the generic resolver's source_url signal."""
        groups = group_neuromorpho_neurons(single_archive_two_publications())
        urls = {build_neuromorpho_source_record(g)["sourceUrl"] for g in groups}
        assert len(urls) == 2  # distinct PMIDs → distinct sourceUrls
        assert all("/pmid/" in u for u in urls)
        # archiveUrl (the real page) is preserved under rawMetadata
        src = build_neuromorpho_source_record(groups[0])
        assert src["rawMetadata"]["archiveUrl"] == "https://neuromorpho.org/archive/Jacobs"

    def test_source_url_norm_distinct_for_same_archive_different_pmid(self):
        """The normalized URL keys must differ so the resolver never merges
        two distinct publications from the same archive."""
        groups = group_neuromorpho_neurons(single_archive_two_publications())
        norms = {normalize_url_key(build_neuromorpho_source_record(g)["sourceUrl"]) for g in groups}
        assert len(norms) == 2

    def test_canonical_record(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        rec = canonical_record_from_source(src)
        assert rec["canonicalDatasetId"].startswith("ns-")
        assert rec["sourceKeys"] == ["neuromorpho:neuromorpho:Mallick:pmid:21228908"]
        assert rec["doi"] == "10.1007/s00429-011-0305-0"
        assert validate_canonical_record(rec) == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 2: Archive with multiple publications
# ─────────────────────────────────────────────────────────────────────────────


class TestCase2ArchiveWithMultiplePublications:
    def test_groups_into_two_contributions(self):
        neurons = single_archive_two_publications()
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 2
        pmids = {g["pmid"] for g in groups}
        assert pmids == {"12204204", "9230750"}

    def test_each_group_has_correct_neuron_count(self):
        groups = group_neuromorpho_neurons(single_archive_two_publications())
        groups_by_pmid = {g["pmid"]: g for g in groups}
        assert groups_by_pmid["12204204"]["neuron_count"] == 2  # 2 neurons in Jacobs + 12204204
        assert groups_by_pmid["9230750"]["neuron_count"] == 2   # 2 neurons in Jacobs + 9230750

    def test_each_group_produces_distinct_source_dataset_id(self):
        groups = group_neuromorpho_neurons(single_archive_two_publications())
        ids = {build_neuromorpho_source_record(g)["sourceDatasetId"] for g in groups}
        assert len(ids) == 2
        assert "neuromorpho:Jacobs:pmid:12204204" in ids
        assert "neuromorpho:Jacobs:pmid:9230750" in ids


# ─────────────────────────────────────────────────────────────────────────────
# Case 3: Archive with 15+ PMIDs → separate groups (proven by Jacobs fixture)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase3ArchiveWithManyPmids:
    def test_archive_with_15_pmids_generates_15_groups(self):
        """Simulate an archive with 15 PMIDs — each PMID gets its own group."""
        neurons = []
        for i in range(15):
            pmid = str(12204204 + i)
            neurons.append(
                neuron(archive="Jacobs", pmid=pmid, doi=f"10.1002/cne.{i}",
                       species="rat", nid_offset=100 + i)
            )
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 15
        pmids = {g["pmid"] for g in groups}
        assert len(pmids) == 15


# ─────────────────────────────────────────────────────────────────────────────
# Case 4: Negative PMID placeholder
# ─────────────────────────────────────────────────────────────────────────────


class TestCase4NegativePmidPlaceholder:
    def test_real_pmid_rejects_negative_values(self):
        assert _real_pmid(["-42"]) is None
        assert _real_pmid(["-4"]) is None
        assert _real_pmid(["-1"]) is None
        assert _real_pmid([]) is None
        assert _real_pmid(None) is None

    def test_real_pmid_accepts_positive_values(self):
        assert _real_pmid(["21129968"]) == "21129968"
        assert _real_pmid(["0"]) == "0"  # 0 is technically a positive integer
        assert _real_pmid(["12204204", "-42"]) == "12204204"  # real wins

    def test_placeholder_groups_use_doi_as_key(self):
        neurons = placeholders_only()
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 1
        g = groups[0]
        assert g["pmid"] == ""  # no real pmid — DOI-only
        assert g["doi"] == "10.1038/s41593-024-01776-3"
        assert g["neuron_count"] == 3

    def test_placeholder_group_source_record(self):
        groups = group_neuromorpho_neurons(placeholders_only())
        src = build_neuromorpho_source_record(groups[0])
        # DOI-only key
        assert "neuromorpho:Siegert:doi:" in src["sourceDatasetId"]
        assert src["doi"] == "10.1038/s41593-024-01776-3"
        assert src["neuronCount"] == 3


# ─────────────────────────────────────────────────────────────────────────────
# Case 5: DOI-only contribution
# ─────────────────────────────────────────────────────────────────────────────


class TestCase5DoiOnly:
    def test_doi_only_grouping(self):
        neurons = doi_only_contribution()
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 1
        g = groups[0]
        assert g["pmid"] == ""  # no real PMID
        assert g["doi"] == "10.1002/admi.201700819"

    def test_doi_only_source_record(self):
        groups = group_neuromorpho_neurons(doi_only_contribution())
        src = build_neuromorpho_source_record(groups[0])
        assert "neuromorpho:Kuddannaya:doi:" in src["sourceDatasetId"]
        assert src["doi"] == "10.1002/admi.201700819"
        assert src["neuronCount"] == 2

    def test_doi_only_canonical_record(self):
        groups = group_neuromorpho_neurons(doi_only_contribution())
        src = build_neuromorpho_source_record(groups[0])
        rec = canonical_record_from_source(src)
        assert rec["doi"] == "10.1002/admi.201700819"
        assert validate_canonical_record(rec) == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 6: Same archive + same PMID across many neurons → ONE dataset
# ─────────────────────────────────────────────────────────────────────────────


class TestCase6SameArchiveSamePmidOneDataset:
    def test_50_neurons_one_group(self):
        neurons = many_neurons_same_contribution()
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 1
        g = groups[0]
        assert g["archive"] == "Chiang"
        assert g["pmid"] == "21129968"
        assert g["neuron_count"] == 50

    def test_single_canonical_record(self):
        neurons = many_neurons_same_contribution()
        groups = group_neuromorpho_neurons(neurons)
        src = build_neuromorpho_source_record(groups[0])
        assert src["neuronCount"] == 50
        rec = canonical_record_from_source(src)
        assert rec["sourceKeys"] == ["neuromorpho:neuromorpho:Chiang:pmid:21129968"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 7: Same archive + different PMID → DIFFERENT datasets
# ─────────────────────────────────────────────────────────────────────────────


class TestCase7DifferentPmidDifferentDatasets:
    def test_two_groups_from_same_archive(self):
        neurons = single_archive_two_publications()
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 2
        srcs = [build_neuromorpho_source_record(g) for g in groups]
        ids = [s["sourceDatasetId"] for s in srcs]
        assert len(ids) == len(set(ids))  # distinct


# ─────────────────────────────────────────────────────────────────────────────
# Case 8: Same archive + PMID + DOI → PMID is primary identity
# ─────────────────────────────────────────────────────────────────────────────


class TestCase8PmidIsPrimaryIdentity:
    def test_pmid_key_used_when_both_pmid_and_doi_present(self):
        neurons = [
            neuron(archive="Chiang", pmid="21129968", doi="10.1002/cne.22705",
                   species="fruit fly", nid_offset=0),
            neuron(archive="Chiang", pmid="21129968", doi="10.1002/cne.22705",
                   species="fruit fly", nid_offset=1),
        ]
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 1
        g = groups[0]
        assert g["pmid"] == "21129968"  # PMID is the primary key
        assert g["doi"] == "10.1002/cne.22705"  # DOI preserved but not the key
        src = build_neuromorpho_source_record(g)
        assert "pmid" in src["sourceDatasetId"]
        assert src["doi"] == "10.1002/cne.22705"


# ─────────────────────────────────────────────────────────────────────────────
# Case 9: Deterministic sourceDatasetId
# ─────────────────────────────────────────────────────────────────────────────


class TestCase9DeterministicSourceId:
    def test_same_neurons_produce_same_source_id(self):
        n1 = single_archive_one_publication()
        n2 = single_archive_one_publication()
        g1 = group_neuromorpho_neurons(n1)
        g2 = group_neuromorpho_neurons(n2)
        s1 = build_neuromorpho_source_record(g1[0])
        s2 = build_neuromorpho_source_record(g2[0])
        assert s1["sourceDatasetId"] == s2["sourceDatasetId"]

    def test_same_source_yields_same_canonical_id(self):
        n = single_archive_one_publication()
        g = group_neuromorpho_neurons(n)
        src = build_neuromorpho_source_record(g[0])
        a = canonical_record_from_source(src)
        b = canonical_record_from_source(src)
        assert a["canonicalDatasetId"] == b["canonicalDatasetId"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 10: Species aggregation
# ─────────────────────────────────────────────────────────────────────────────


class TestCase10SpeciesAggregation:
    def test_single_species(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert src["species"] == ["rat"]
        assert src["speciesRaw"] == ["rat"]

    def test_multiple_species_unioned(self):
        """Neurons with different species in same group → union."""
        neurons = [
            neuron(archive="Multi", pmid="11111111", doi="10.1111/test.1",
                   species="mouse", nid_offset=0),
            neuron(archive="Multi", pmid="11111111", doi="10.1111/test.1",
                   species="rat", nid_offset=1),
        ]
        groups = group_neuromorpho_neurons(neurons)
        src = build_neuromorpho_source_record(groups[0])
        assert set(src["species"]) == {"mouse", "rat"}


# ─────────────────────────────────────────────────────────────────────────────
# Case 11: Brain-region aggregation
# ─────────────────────────────────────────────────────────────────────────────


class TestCase11BrainRegionAggregation:
    def test_regions_unioned(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        # 3 neurons: regions union = neocortex, frontal
        assert "neocortex" in src["brainRegions"]
        assert "frontal" in src["brainRegions"]

    def test_multi_region_archive(self):
        neurons = [
            neuron(archive="MultiRegion", pmid="22222222", doi="10.2222/test.2",
                   brain_region=["hippocampus", "CA1"], nid_offset=0),
            neuron(archive="MultiRegion", pmid="22222222", doi="10.2222/test.2",
                   brain_region=["hippocampus", "CA3"], nid_offset=1),
        ]
        groups = group_neuromorpho_neurons(neurons)
        src = build_neuromorpho_source_record(groups[0])
        assert "hippocampus" in src["brainRegions"]
        assert "CA1" in src["brainRegions"]
        assert "CA3" in src["brainRegions"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 12: Cell-type aggregation
# ─────────────────────────────────────────────────────────────────────────────


class TestCase12CellTypeAggregation:
    def test_cell_types_unioned(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert "principal cell" in src["snapshot"]["cellTypeList"]
        assert "pyramidal" in src["snapshot"]["cellTypeList"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 13: Age-range handling
# ─────────────────────────────────────────────────────────────────────────────


class TestCase13AgeRangeHandling:
    def test_age_range_on_source_only(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert src["ageMin"] == 56
        assert src["ageMax"] == 56
        assert src["ages"] is None
        assert src["ageGroup"] is None

    def test_age_range_from_multiple_neurons(self):
        neurons = [
            neuron(archive="AgeTest", pmid="33333333", doi="10.3333/test.3",
                   min_age=10, max_age=20, nid_offset=0),
            neuron(archive="AgeTest", pmid="33333333", doi="10.3333/test.3",
                   min_age=30, max_age=50, nid_offset=1),
        ]
        groups = group_neuromorpho_neurons(neurons)
        src = build_neuromorpho_source_record(groups[0])
        assert src["ageMin"] == 10   # min of min_ages
        assert src["ageMax"] == 50   # max of max_ages

    def test_canonical_ages_and_age_group_stay_null(self):
        neurons = [
            neuron(archive="AgeTest", pmid="44444444", doi="10.4444/test.4",
                   min_age=5, max_age=80, nid_offset=0),
        ]
        groups = group_neuromorpho_neurons(neurons)
        src = build_neuromorpho_source_record(groups[0])
        rec = canonical_record_from_source(src)
        assert rec["ages"] is None
        assert rec["ageGroup"] == []  # not fabricated


# ─────────────────────────────────────────────────────────────────────────────
# Case 14: Sex aggregation
# ─────────────────────────────────────────────────────────────────────────────


class TestCase14SexAggregation:
    def test_genders_collected_in_snapshot(self):
        neurons = [
            neuron(archive="SexTest", pmid="55555555", doi="10.5555/test.5",
                   gender="Male", nid_offset=0),
            neuron(archive="SexTest", pmid="55555555", doi="10.5555/test.5",
                   gender="Female", nid_offset=1),
        ]
        groups = group_neuromorpho_neurons(neurons)
        src = build_neuromorpho_source_record(groups[0])
        assert "Male" in src["snapshot"]["genders"]
        assert "Female" in src["snapshot"]["genders"]

    def test_not_reported_excluded_from_genders(self):
        neurons = [
            neuron(archive="SexTest", pmid="66666666", doi="10.6666/test.6",
                   gender="Not reported", nid_offset=0),
        ]
        groups = group_neuromorpho_neurons(neurons)
        src = build_neuromorpho_source_record(groups[0])
        assert src["snapshot"]["genders"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 15: neuronCount aggregation
# ─────────────────────────────────────────────────────────────────────────────


class TestCase15NeuronCountAggregation:
    def test_neuron_count_reflects_all_neurons(self):
        groups = group_neuromorpho_neurons(many_neurons_same_contribution())
        src = build_neuromorpho_source_record(groups[0])
        assert src["neuronCount"] == 50
        assert src["snapshot"]["neuronCount"] == 50

    def test_neuron_count_in_raw_metadata(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert src["rawMetadata"]["neuronCount"] == 3


# ─────────────────────────────────────────────────────────────────────────────
# Case 16: rawMetadata.neuromorpho preservation
# ─────────────────────────────────────────────────────────────────────────────


class TestCase16RawMetadataPreservation:
    def test_raw_metadata_on_source(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert src["rawMetadata"]["archive"] == "Mallick"
        assert src["rawMetadata"]["pmid"] == "21228908"
        assert src["rawMetadata"]["neuronCount"] == 3
        assert "brainRegions" in src["rawMetadata"]
        assert "cellTypes" in src["rawMetadata"]

    def test_canonical_raw_metadata_keyed_by_repository(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        rec = canonical_record_from_source(src)
        assert list(rec["rawMetadata"].keys()) == ["neuromorpho"]
        assert rec["rawMetadata"]["neuromorpho"]["archive"] == "Mallick"


# ─────────────────────────────────────────────────────────────────────────────
# Case 17: Existing OpenNeuro/DANDI/NEMAR records unaffected
# ─────────────────────────────────────────────────────────────────────────────


class TestCase17ExistingRecordsUnaffected:
    def test_neuromorpho_repository_is_independent(self):
        """The NEUROMORPHO_REPOSITORY constant is literally 'neuromorpho'."""
        assert NEUROMORPHO_REPOSITORY == "neuromorpho"
        assert NEUROMORPHO_REPOSITORY not in ("openneuro", "dandi", "nemar")

    def test_source_url_uses_neuromorpho_domain(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert "neuromorpho.org" in src["sourceUrl"]
        assert "openneuro" not in src["sourceUrl"]
        assert "dandi" not in src["sourceUrl"]
        assert "nemar" not in src["sourceUrl"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 18: Cross-repository identity behavior
# ─────────────────────────────────────────────────────────────────────────────


class TestCase18CrossRepositoryIdentity:
    def test_doi_is_literature_only_not_cross_repo_identity(self):
        """NeuroMorpho DOI is a literature DOI, not a cross-repository dataset
        identity. The doi field on the source is set so identity resolution
        will NOT match OpenNeuro/DANDI/NEMAR DOIs (different namespaces)."""
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        # DOI is a literature DOI, not an OpenNeuro/DANDI/NEMAR one
        doi = src["doi"]
        assert doi is not None
        assert "openneuro" not in doi
        assert "dandi" not in doi
        assert "nemar" not in doi

    def test_no_cross_reference_urls(self):
        """NeuroMorpho records have no referencesAndLinks to other repos."""
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        refs = (src.get("publication") or {}).get("referencesAndLinks") or []
        assert len(refs) == 0  # no cross-references


# ─────────────────────────────────────────────────────────────────────────────
# Case 19: Duplicate source-key detection
# ─────────────────────────────────────────────────────────────────────────────


class TestCase19DuplicateSourceKey:
    def test_same_archive_pmid_produces_same_source_key(self):
        n1 = single_archive_one_publication()
        n2 = single_archive_one_publication()
        g1 = group_neuromorpho_neurons(n1)
        g2 = group_neuromorpho_neurons(n2)
        s1 = build_neuromorpho_source_record(g1[0])
        s2 = build_neuromorpho_source_record(g2[0])
        # Same sourceDatasetId → same sourceKey
        assert s1["sourceDatasetId"] == s2["sourceDatasetId"]
        key1 = f"neuromorpho:{s1['sourceDatasetId']}"
        key2 = f"neuromorpho:{s2['sourceDatasetId']}"
        assert key1 == key2

    def test_different_archives_different_source_keys(self):
        n1 = [neuron(archive="ArchA", pmid="11111111", doi="10.1111/a", nid_offset=0)]
        n2 = [neuron(archive="ArchB", pmid="11111111", doi="10.1111/b", nid_offset=1)]
        g1 = group_neuromorpho_neurons(n1)
        g2 = group_neuromorpho_neurons(n2)
        s1 = build_neuromorpho_source_record(g1[0])
        s2 = build_neuromorpho_source_record(g2[0])
        assert s1["sourceDatasetId"] != s2["sourceDatasetId"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 21: Within-archive DOI artifact — 2+ PMID groups sharing one DOI in
# the SAME archive must keep separate identities (Ascoli pattern). The shared
# DOI is an archive-level artifact, NOT a per-contribution identity.
# ─────────────────────────────────────────────────────────────────────────────


class TestCase21DoiArtifactWithinArchive:
    def _ascioli_style(self):
        """3 distinct PMID groups in one archive all carrying the same DOI."""
        return [
            neuron(archive="Ascoli", pmid="2007659", doi="10.1016/j.neucom.2004.10.105",
                   species="rat", brain_region=["neocortex"], nid_offset=0),
            neuron(archive="Ascoli", pmid="2329188", doi="10.1016/j.neucom.2004.10.105",
                   species="rat", brain_region=["hippocampus"], nid_offset=1),
            neuron(archive="Ascoli", pmid="3401733", doi="10.1016/j.neucom.2004.10.105",
                   species="rat", brain_region=["striatum"], nid_offset=2),
            # DOI-only group in the SAME archive with the SAME DOI keeps it
            neuron(archive="Ascoli", pmid="-42", doi="10.1016/j.neucom.2004.10.105",
                   species="rat", brain_region=["cortex"], nid_offset=3),
        ]

    def test_groups_remain_separate(self):
        neurons = self._ascioli_style()
        groups = group_neuromorpho_neurons(neurons)
        pmids = {g.get("pmid") for g in groups if g.get("pmid")}
        assert pmids == {"2007659", "2329188", "3401733"}  # 3 PMID groups
        assert len(groups) == 4  # + 1 DOI-only group

    def test_shared_doi_cleared_from_pmid_groups(self):
        neurons = self._ascioli_style()
        groups = group_neuromorpho_neurons(neurons)
        pmid_groups = [g for g in groups if g.get("pmid")]
        for g in pmid_groups:
            assert g["doi"] == ""  # artifact cleared — never an identity
            assert g["doi_artifact"] == "10.1016/j.neucom.2004.10.105"  # preserved

    def test_doi_only_group_keeps_doi(self):
        neurons = self._ascioli_style()
        groups = group_neuromorpho_neurons(neurons)
        doi_only = [g for g in groups if not g.get("pmid")]
        assert len(doi_only) == 1
        assert doi_only[0]["doi"] == "10.1016/j.neucom.2004.10.105"  # identity intact

    def test_source_records_have_distinct_identity(self):
        neurons = self._ascioli_style()
        groups = group_neuromorpho_neurons(neurons)
        srcs = [build_neuromorpho_source_record(g) for g in groups]
        ids = [s["sourceDatasetId"] for s in srcs]
        assert len(ids) == len(set(ids))  # 4 distinct contributions
        # PMID groups: doi=None (artifact cleared), sourceUrl unique
        for s in srcs:
            if s["snapshot"]["pmid"]:
                assert s["doi"] is None
                assert "/pmid/" in s["sourceUrl"]
            else:
                assert s["doi"] == "10.1016/j.neucom.2004.10.105"
                assert "/doi/10.1016__j.neucom.2004.10.105" in s["sourceUrl"]
        # raw metadata preserves the artifact DOI verbatim
        assert srcs[0]["rawMetadata"]["doiArtifact"] == "10.1016/j.neucom.2004.10.105"

    def test_cross_archive_shared_doi_still_merges(self):
        """A DOI shared across DIFFERENT archives (same publication under two
        archive labels) is NOT an artifact — both keep the DOI so the generic
        resolver can merge them (observed live: 6 legit cross-archive merges)."""
        neurons = [
            neuron(archive="Wilson_R", pmid="", doi="10.1101/666073",
                   species="mouse", brain_region=["hippocampus"], nid_offset=0),
            neuron(archive="Scimemi", pmid="", doi="10.1101/666073",
                   species="mouse", brain_region=["hippocampus"], nid_offset=1),
        ]
        groups = group_neuromorpho_neurons(neurons)
        assert len(groups) == 2  # two archives, two DOI-only groups
        for g in groups:
            assert g["doi"] == "10.1101/666073"  # never cleared cross-archive
        # Same normalized DOI on both → resolver merges via doi signal
        srcs = [build_neuromorpho_source_record(g) for g in groups]
        assert srcs[0]["doi"] == srcs[1]["doi"] == "10.1101/666073"
        recs = [canonical_record_from_source(s) for s in srcs]
        assert recs[0]["canonicalDatasetId"] == recs[1]["canonicalDatasetId"]

    def test_shared_doi_within_archive_not_cleared_cross_archive(self):
        """Artifact clearing is per-archive: a DOI unique within ITS archive
        stays even if another archive also uses it."""
        neurons = [
            # Ascoli archive: PMID groups 2007659 + 2329188 share the DOI → cleared
            neuron(archive="Ascoli", pmid="2007659", doi="10.1016/j.neucom.2004.10.105", nid_offset=0),
            neuron(archive="Ascoli", pmid="2329188", doi="10.1016/j.neucom.2004.10.105", nid_offset=1),
            # DIADEM archive: same DOI but only ONE group → kept
            neuron(archive="DIADEM", pmid="23077053", doi="10.1016/j.neucom.2004.10.105", nid_offset=2),
        ]
        groups = group_neuromorpho_neurons(neurons)
        by_arch = {g["archive"]: g for g in groups}
        assert by_arch["Ascoli"]["doi"] == ""  # artifact
        assert by_arch["Ascoli"]["doi_artifact"] == "10.1016/j.neucom.2004.10.105"
        assert by_arch["DIADEM"]["doi"] == "10.1016/j.neucom.2004.10.105"  # kept
        assert not by_arch["DIADEM"].get("doi_artifact")


# ─────────────────────────────────────────────────────────────────────────────
# Case 20: Complete grouping count fixture
# ─────────────────────────────────────────────────────────────────────────────


class TestCase20CompleteGroupingCount:
    def test_mixed_scenario_grouping(self):
        """Multiple scenarios merged: 1-pub (1 group) + 2-pub (2 groups) +
        placeholder (1 DOI group) + 50-same (1 group) + DOI-only (1 group)."""
        all_neurons = (
            single_archive_one_publication()
            + single_archive_two_publications()
            + placeholders_only()
            + many_neurons_same_contribution()
            + doi_only_contribution()
        )
        groups = group_neuromorpho_neurons(all_neurons)
        # 1 + 2 + 1 + 1 + 1 = 6 groups
        assert len(groups) == 6
        # Verify exact breakdown
        pmid_groups = [g for g in groups if g.get("pmid")]
        doi_groups = [g for g in groups if not g.get("pmid") and g.get("doi")]
        assert len(pmid_groups) == 4  # Mallick(1) + Jacobs x2(2) + Chiang(1) = 4
        assert len(doi_groups) == 2   # Siegert(1) + Kuddannaya(1) = 2


# ─────────────────────────────────────────────────────────────────────────────
# Utility tests
# ─────────────────────────────────────────────────────────────────────────────


class TestUtilityFunctions:
    def test_real_doi_extraction(self):
        assert _real_doi(["10.1002/cne.22705"]) == "10.1002/cne.22705"
        assert _real_doi(["not-a-doi"]) is None
        assert _real_doi([]) is None
        assert _real_doi(None) is None

    def test_normalize_doi_on_neuromorpho_values(self):
        assert normalize_doi("10.1002/cne.22705") == "10.1002/cne.22705"
        assert normalize_doi("10.1038/s41593-024-01776-3") == "10.1038/s41593-024-01776-3"

    def test_source_url_normalizes_uniquely(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        url = normalize_url_key(src["sourceUrl"])
        assert url is not None
        assert url.endswith("/archive/Mallick/pmid/21228908")

    def test_canonical_record_validates(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        rec = canonical_record_from_source(src)
        assert validate_canonical_record(rec) == []

    def test_unavailable_fields_stay_null(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert src["modality"] == []
        assert src["disease"] is None
        assert src["studyType"] is None
        assert src["license"] is None
        assert src["participantCount"] is None
        assert src["datasetSizeBytes"] is None
        assert src["tasks"] == []
        assert src["sessions"] == []

    def test_availability_open(self):
        groups = group_neuromorpho_neurons(single_archive_one_publication())
        src = build_neuromorpho_source_record(groups[0])
        assert src["availability"] == "open"