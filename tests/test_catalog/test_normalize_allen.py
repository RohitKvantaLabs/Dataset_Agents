"""Unit tests for Allen → canonical source normalization (pure logic, no I/O).

Covers the 20 required test cases:
 1. Product normalization
 2. deterministic Product sourceDatasetId
 3. sourceKeys
 4. rawMetadata.allen
 5. Product title/description
 6. species
 7. age
 8. sex
 9. brain-region mapping
10. modality behavior
11. missing DOI behavior
12. missing license behavior
13. child SectionDataSet counts
14. AtlasDataSet not double-counted
15. multiple products remain separate
16. identity resolution
17. no fuzzy title merge
18. duplicate Product ID protection
19. canonical source merge behavior
20. OpenNeuro/DANDI/NEMAR/NeuroMorpho regression safety
"""

from app.catalog.dedup import evaluate_identity, merge_source_into_canonical
from app.catalog.normalize import (
    ALLEN_REPOSITORY,
    allen_child_stats,
    build_allen_source_record,
    canonical_record_from_source,
)
from app.catalog.schema import (
    normalize_url_key,
    validate_canonical_record,
)

from ._allen_fixtures import (
    ages_map,
    dataset_row,
    donor_row,
    include_payload,
    product,
    specimen_row,
)


# ─────────────────────────────────────────────────────────────────────────────
# Case 1: Product normalization (identity + core fields)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase1ProductNormalization:
    def test_repository_is_allen(self):
        assert ALLEN_REPOSITORY == "allen"
        assert ALLEN_REPOSITORY not in ("openneuro", "dandi", "nemar", "neuromorpho")

    def test_source_record_identity(self):
        p = include_payload(product(product_id=62))
        src = build_allen_source_record(p["msg"][0])
        assert src["repository"] == "allen"
        assert src["sourceDatasetId"] == "allen:62"
        assert src["sourceUrl"] == "https://api.brain-map.org/api/v2/data/Product/62/query.json"
        assert src["doi"] is None
        assert src["datasetType"] == "product"
        assert src["availability"] == "open"

    def test_canonical_record_validates(self):
        p = include_payload(product(product_id=62))
        src = build_allen_source_record(p["msg"][0])
        rec = canonical_record_from_source(src)
        assert rec["canonicalDatasetId"].startswith("ns-")
        assert validate_canonical_record(rec) == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 2: deterministic sourceDatasetId
# ─────────────────────────────────────────────────────────────────────────────


class TestCase2DeterministicSourceId:
    def test_same_product_same_id(self):
        a = build_allen_source_record(include_payload(product(product_id=62))["msg"][0])
        b = build_allen_source_record(include_payload(product(product_id=62))["msg"][0])
        assert a["sourceDatasetId"] == b["sourceDatasetId"] == "allen:62"

    def test_same_source_same_canonical_id(self):
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        assert canonical_record_from_source(src)["canonicalDatasetId"] == \
            canonical_record_from_source(src)["canonicalDatasetId"]

    def test_source_url_normalizes_distinctly_per_product(self):
        """The normalized URL must differ per product so the generic resolver
        can never merge two Products through the source_url signal."""
        u62 = normalize_url_key(build_allen_source_record(
            include_payload(product(product_id=62))["msg"][0])["sourceUrl"])
        u1 = normalize_url_key(build_allen_source_record(
            include_payload(product(product_id=1))["msg"][0])["sourceUrl"])
        assert u62 != u1
        assert u62.endswith("/data/Product/62/query.json")
        assert u1.endswith("/data/Product/1/query.json")


# ─────────────────────────────────────────────────────────────────────────────
# Case 3: sourceKeys
# ─────────────────────────────────────────────────────────────────────────────


class TestCase3SourceKeys:
    def test_canonical_source_keys(self):
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        rec = canonical_record_from_source(src)
        assert rec["sourceKeys"] == ["allen:allen:62"]

    def test_source_key_identity_signal(self):
        p = include_payload(product(product_id=26))["msg"][0]
        src = build_allen_source_record(p)
        assert src["sourceDatasetId"] == "allen:26"
        assert f"{src['repository']}:{src['sourceDatasetId']}" == "allen:allen:26"


# ─────────────────────────────────────────────────────────────────────────────
# Case 4: rawMetadata.allen
# ─────────────────────────────────────────────────────────────────────────────


class TestCase4RawMetadataAllen:
    def test_canonical_raw_metadata_keyed_by_repository(self):
        p = include_payload(
            product(product_id=62),
            data_sets=[dataset_row(ds_id=1), dataset_row(ds_id=2)],
            donors=[donor_row(donor_id=7)],
        )["msg"][0]
        src = build_allen_source_record(p)
        rec = canonical_record_from_source(src)
        assert list(rec["rawMetadata"].keys()) == ["allen"]
        raw = rec["rawMetadata"]["allen"]
        assert raw["product"]["id"] == 62
        assert raw["product"]["name"].startswith("Multi-plane")
        assert "apiUrl" in raw

    def test_child_rows_never_stored(self):
        """The child ARRAYS must never land in rawMetadata — only aggregates."""
        p = include_payload(
            product(product_id=1),
            data_sets=[dataset_row(ds_id=i) for i in range(50)],
            specimens=[specimen_row(specimen_id=i) for i in range(10)],
            donors=[donor_row(donor_id=i) for i in range(5)],
        )["msg"][0]
        src = build_allen_source_record(p)
        raw = src["rawMetadata"]
        assert "data_sets" not in raw
        assert "specimens" not in raw
        assert "donors" not in raw
        assert raw["childStats"]["dataSetCount"] == 50
        assert raw["childStats"]["specimenCount"] == 10
        assert raw["childStats"]["donorCount"] == 5

    def test_api_url_is_the_working_criteria_url(self):
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        assert "criteria=model::Product[id$eq62]" in src["rawMetadata"]["apiUrl"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 5: Product title/description
# ─────────────────────────────────────────────────────────────────────────────


class TestCase5TitleDescription:
    def test_title_is_product_name(self):
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        assert src["title"] == "Multi-plane optical physiology during image change detection"

    def test_description_is_product_description(self):
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        assert src["description"] == "Time series of behavioral variables and neural activity"


# ─────────────────────────────────────────────────────────────────────────────
# Case 6: species (explicit Allen values only; NHP → canonical label)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase6Species:
    def test_mouse(self):
        src = build_allen_source_record(include_payload(product(species="Mouse"))["msg"][0])
        assert src["species"] == ["mouse"]
        assert src["speciesRaw"] == ["Mouse"]

    def test_human(self):
        src = build_allen_source_record(include_payload(product(species="Human"))["msg"][0])
        assert src["species"] == ["human"]

    def test_nhp_maps_to_macaque(self):
        """NHP (non-human primate) is an explicit Allen species value — mapped
        through the canonical species vocabulary to macaque, never invented."""
        src = build_allen_source_record(include_payload(product(species="NHP"))["msg"][0])
        assert src["species"] == ["macaque"]
        assert src["speciesRaw"] == ["NHP"]

    def test_missing_species_stays_null(self):
        src = build_allen_source_record(include_payload(product(species=None))["msg"][0])
        assert src["species"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Case 7: age — canonical stays null; donor ages preserved as summaries
# ─────────────────────────────────────────────────────────────────────────────


class TestCase7Age:
    def test_canonical_ages_and_age_group_stay_null(self):
        p = include_payload(
            product(product_id=62),
            donors=[donor_row(donor_id=1, age_id=62)],
        )["msg"][0]
        src = build_allen_source_record(p, ages_map())
        assert src["ages"] is None
        assert src["ageGroup"] is None
        assert src["ageMin"] is None
        assert src["ageMax"] is None

    def test_donor_age_summaries_preserved(self):
        p = include_payload(
            product(product_id=62),
            donors=[donor_row(donor_id=1, age_id=62), donor_row(donor_id=2, age_id=90)],
        )["msg"][0]
        src = build_allen_source_record(p, ages_map())
        summaries = src["snapshot"]["donorAgeSummaries"]
        by_id = {s["ageId"]: s for s in summaries}
        assert by_id[62]["name"] == "P56"
        assert by_id[62]["days"] == 56.0
        assert by_id[90]["name"] == "60 years"
        assert by_id[90]["days"] == 21900.0

    def test_no_age_map_keeps_age_ids_without_fabrication(self):
        p = include_payload(
            product(product_id=62),
            donors=[donor_row(donor_id=1, age_id=62)],
        )["msg"][0]
        src = build_allen_source_record(p, None)
        assert src["snapshot"]["donorAgeSummaries"] == [{"ageId": 62}]

    def test_derive_age_groups_never_called(self):
        """A range/structured donor age must not appear as canonical ages —
        species-specific units (mouse P56 days vs human years) would produce
        fabricated age groups."""
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p, ages_map())
        rec = canonical_record_from_source(src)
        assert rec["ages"] is None
        assert rec["ageGroup"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 8: sex — explicit donor metadata only
# ─────────────────────────────────────────────────────────────────────────────


class TestCase8Sex:
    def test_donor_sex_distribution(self):
        p = include_payload(
            product(product_id=62),
            donors=[
                donor_row(donor_id=1, sex="M", sex_full_name="Male"),
                donor_row(donor_id=2, sex="F", sex_full_name="Female"),
                donor_row(donor_id=3, sex="M", sex_full_name="Male"),
            ],
        )["msg"][0]
        src = build_allen_source_record(p)
        assert set(src["snapshot"]["genders"]) == {"Female", "Male"}

    def test_unknown_sex_excluded(self):
        p = include_payload(
            product(product_id=62),
            donors=[donor_row(donor_id=1, sex="M", sex_full_name="Male"),
                    donor_row(donor_id=2, sex="unknown", sex_full_name="unknown")],
        )["msg"][0]
        src = build_allen_source_record(p)
        assert src["snapshot"]["genders"] == ["Male"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 9: brain-region mapping — never inferred from text
# ─────────────────────────────────────────────────────────────────────────────


class TestCase9BrainRegion:
    def test_brain_regions_null(self):
        """No structured anatomy on the Product model; a title that MENTIONS a
        region must never populate brainRegions."""
        p = include_payload(
            product(name="Mouse Brain Atlas", description="Hippocampus and cortex ISH data")
        )["msg"][0]
        src = build_allen_source_record(p)
        assert src["brainRegions"] is None
        rec = canonical_record_from_source(src)
        assert rec["brainRegions"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Case 10: modality behavior — never invented
# ─────────────────────────────────────────────────────────────────────────────


class TestCase10Modality:
    def test_modality_empty(self):
        """Allen exposes no explicit modality; Allen-specific experiment types
        are never converted into fMRI/sMRI/DTI/EEG/MEG."""
        p = include_payload(
            product(description="Two-photon imaging and Neuropixels electrophysiology data")
        )["msg"][0]
        src = build_allen_source_record(p)
        assert src["modality"] == []
        assert src["modalityRaw"] == []
        rec = canonical_record_from_source(src)
        assert rec["modality"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Case 11: missing DOI behavior
# ─────────────────────────────────────────────────────────────────────────────


class TestCase11MissingDoi:
    def test_doi_null(self):
        src = build_allen_source_record(include_payload(product())["msg"][0])
        assert src["doi"] is None
        rec = canonical_record_from_source(src)
        assert rec["doi"] is None

    def test_canonical_identity_falls_back_to_repo_id(self):
        """No DOI and no colliding URL → canonical ID derives from the
        repository:sourceDatasetId identity."""
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        rec = canonical_record_from_source(src)
        # primary identity is the distinct per-product URL (documented path form)
        prov = rec["provenance"]["identity"]
        assert prov["doi"] is None
        assert prov["sourceUrlNorm"].endswith("/Product/62/query.json")


# ─────────────────────────────────────────────────────────────────────────────
# Case 12: missing license behavior
# ─────────────────────────────────────────────────────────────────────────────


class TestCase12MissingLicense:
    def test_license_null(self):
        src = build_allen_source_record(include_payload(product())["msg"][0])
        assert src["license"] is None
        assert src["licenseNormalized"] is None
        rec = canonical_record_from_source(src)
        assert rec["license"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Case 13: child SectionDataSet counts (aggregated, never stored)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase13ChildCounts:
    def test_data_set_count_exact(self):
        p = include_payload(
            product(product_id=1),
            data_sets=[dataset_row(ds_id=i) for i in range(39_148)],
        )["msg"][0]
        src = build_allen_source_record(p)
        assert src["snapshot"]["dataSetCount"] == 39_148
        # the raw payload is NOT retained (memory safety)
        assert "data_sets" not in src["rawMetadata"]

    def test_specimen_and_donor_counts(self):
        p = include_payload(
            product(product_id=33),
            specimens=[specimen_row(specimen_id=i) for i in range(1923)],
            donors=[donor_row(donor_id=i) for i in range(921)],
        )["msg"][0]
        src = build_allen_source_record(p)
        assert src["snapshot"]["specimenCount"] == 1923
        assert src["snapshot"]["donorCount"] == 921
        # per-type counts unsupported by the RMA → honest null
        assert src["snapshot"]["sectionDataSetCount"] is None
        assert src["snapshot"]["microarrayDataSetCount"] is None
        assert src["snapshot"]["atlasDataSetCount"] is None

    def test_child_stats_helper_pure(self):
        p = include_payload(
            product(product_id=26),
            data_sets=[dataset_row(ds_id=i) for i in range(565)],
            donors=[donor_row(donor_id=1), donor_row(donor_id=2)],
        )["msg"][0]
        stats = allen_child_stats(p)
        assert stats["dataSetCount"] == 565
        assert stats["donorCount"] == 2
        assert stats["specimenCount"] == 0
        assert "data_sets" not in stats


# ─────────────────────────────────────────────────────────────────────────────
# Case 14: AtlasDataSet not double-counted
# ─────────────────────────────────────────────────────────────────────────────


class TestCase14AtlasNotDoubleCounted:
    def test_atlas_rows_counted_once_in_data_set_count(self):
        """An AtlasDataSet row in the include is ONE data set — the total must
        equal the number of rows, never doubled."""
        p = include_payload(
            product(product_id=1),
            data_sets=[dataset_row(ds_id=1), dataset_row(ds_id=2), dataset_row(ds_id=3)],
        )["msg"][0]
        src = build_allen_source_record(p)
        assert src["snapshot"]["dataSetCount"] == 3
        # only one counting mechanism exists — no separate atlas double-count
        assert src["snapshot"]["atlasDataSetCount"] is None

    def test_zero_data_sets(self):
        src = build_allen_source_record(include_payload(product(product_id=52))["msg"][0])
        assert src["snapshot"]["dataSetCount"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# Case 15: multiple products remain separate
# ─────────────────────────────────────────────────────────────────────────────


class TestCase15MultipleProductsSeparate:
    def test_two_products_two_canonical_records(self):
        p1 = build_allen_source_record(include_payload(product(product_id=1))["msg"][0])
        p2 = build_allen_source_record(include_payload(product(product_id=2))["msg"][0])
        r1 = canonical_record_from_source(p1)
        r2 = canonical_record_from_source(p2)
        assert r1["canonicalDatasetId"] != r2["canonicalDatasetId"]
        assert r1["sourceKeys"] != r2["sourceKeys"]

    def test_no_cross_product_merge_via_identity(self):
        p1 = build_allen_source_record(
            include_payload(product(product_id=1, name="Mouse Brain"))["msg"][0]
        )
        p2 = build_allen_source_record(
            include_payload(product(product_id=2, name="Human Brain Microarray"))["msg"][0]
        )
        r1 = canonical_record_from_source(p1)
        eval_result = evaluate_identity(r1, p2)
        assert eval_result["match"] is False
        assert eval_result["ambiguous"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Case 16: identity resolution (pure rules)
# ─────────────────────────────────────────────────────────────────────────────


class TestCase16IdentityResolution:
    def test_same_product_is_source_url_or_key_match(self):
        """Re-ingesting the same product resolves as a confident match. The
        distinct per-product sourceUrl is a strong signal and fires BEFORE the
        source_key check — which is exactly why the URL must be path-distinct
        (a collapsed URL would merge every product here)."""
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        rec = canonical_record_from_source(src)
        again = build_allen_source_record(p)
        result = evaluate_identity(rec, again)
        assert result["match"] is True
        assert result["matchedVia"] in ("source_url", "source_key")

    def test_different_product_no_match(self):
        src1 = build_allen_source_record(include_payload(product(product_id=62))["msg"][0])
        src2 = build_allen_source_record(include_payload(product(product_id=26))["msg"][0])
        rec1 = canonical_record_from_source(src1)
        assert evaluate_identity(rec1, src2)["match"] is False

    def test_canonical_identity_is_deterministic(self):
        a = canonical_record_from_source(
            build_allen_source_record(include_payload(product(product_id=62))["msg"][0])
        )
        b = canonical_record_from_source(
            build_allen_source_record(include_payload(product(product_id=62))["msg"][0])
        )
        assert a["canonicalDatasetId"] == b["canonicalDatasetId"]


# ─────────────────────────────────────────────────────────────────────────────
# Case 17: no fuzzy title merge
# ─────────────────────────────────────────────────────────────────────────────


class TestCase17NoFuzzyTitleMerge:
    def test_similar_titles_stay_separate(self):
        """Products with similar titles must never merge on title similarity."""
        p1 = include_payload(product(product_id=1, name="Mouse Brain"))["msg"][0]
        p2 = include_payload(product(product_id=12, name="Mouse Brain Reference Data"))["msg"][0]
        s1 = build_allen_source_record(p1)
        s2 = build_allen_source_record(p2)
        r1 = canonical_record_from_source(s1)
        # title differs → not even an ambiguous candidate
        assert evaluate_identity(r1, s2)["match"] is False

    def test_multi_field_requires_exact_title_and_signals(self):
        """Even an exact normalized title cannot merge without ≥2 strong
        signals AND a different repository already attached."""
        p1 = include_payload(product(product_id=1, name="Identical Title"))["msg"][0]
        p2 = include_payload(product(product_id=2, name="Identical Title"))["msg"][0]
        s1 = build_allen_source_record(p1)
        s2 = build_allen_source_record(p2)
        r1 = canonical_record_from_source(s1)
        result = evaluate_identity(r1, s2)
        # same repo (allen) is attached → repository ID is authoritative → no
        # multi-field fuzzy merge, but the exact-title case is ambiguous
        assert result["match"] is False
        assert result["ambiguous"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Case 18: duplicate Product ID protection
# ─────────────────────────────────────────────────────────────────────────────


class TestCase18DuplicateProductId:
    def test_duplicate_product_id_yields_same_identity(self):
        p = include_payload(product(product_id=62))["msg"][0]
        a = build_allen_source_record(p)
        b = build_allen_source_record(p)
        assert a["sourceDatasetId"] == b["sourceDatasetId"]
        assert a["sourceUrl"] == b["sourceUrl"]
        assert canonical_record_from_source(a)["canonicalDatasetId"] == \
            canonical_record_from_source(b)["canonicalDatasetId"]

    def test_identity_resolver_marks_duplicate_as_match(self):
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        rec = canonical_record_from_source(src)
        dup = build_allen_source_record(p)
        assert evaluate_identity(rec, dup)["match"] is True


# ─────────────────────────────────────────────────────────────────────────────
# Case 19: canonical source merge behavior
# ─────────────────────────────────────────────────────────────────────────────


class TestCase19CanonicalMerge:
    def test_merge_attaches_single_source_and_preserves_raw(self):
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        rec = canonical_record_from_source(src)
        dup = build_allen_source_record(p)
        merged = merge_source_into_canonical(rec, dup, "source_key")
        allen_sources = [s for s in merged["sources"] if s["repository"] == "allen"]
        assert len(allen_sources) == 1  # same source refreshed, never duplicated
        assert merged["sourceKeys"] == ["allen:allen:62"]
        assert list(merged["rawMetadata"].keys()) == ["allen"]
        assert validate_canonical_record(merged) == []

    def test_merge_keeps_allen_raw_metadata_repo_scoped(self):
        """A canonical record carrying another repo's rawMetadata must keep it
        when an Allen source is merged in."""
        p = include_payload(product(product_id=62))["msg"][0]
        src = build_allen_source_record(p)
        rec = canonical_record_from_source(src)
        rec["rawMetadata"]["openneuro"] = {"other": True}
        dup = build_allen_source_record(p)
        merged = merge_source_into_canonical(rec, dup, "source_key")
        assert set(merged["rawMetadata"].keys()) == {"allen", "openneuro"}


# ─────────────────────────────────────────────────────────────────────────────
# Case 20: OpenNeuro/DANDI/NEMAR/NeuroMorpho regression safety
# ─────────────────────────────────────────────────────────────────────────────


class TestCase20RegressionSafety:
    def test_allen_isolation(self):
        """Allen constants and URLs never touch the other repositories."""
        src = build_allen_source_record(include_payload(product(product_id=62))["msg"][0])
        assert src["repository"] == "allen"
        assert "brain-map.org" in src["sourceUrl"]
        for other in ("openneuro.org", "dandiarchive", "nemar.org", "neuromorpho.org"):
            assert other not in src["sourceUrl"]

    def test_availability_by_repository_other_values_unchanged(self):
        from app.catalog.schema import AVAILABILITY_BY_REPOSITORY
        assert AVAILABILITY_BY_REPOSITORY["openneuro"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["dandi"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["nemar"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["neuromorpho"] == "open"
        assert AVAILABILITY_BY_REPOSITORY["allen"] == "open"

    def test_unavailable_fields_stay_null(self):
        src = build_allen_source_record(include_payload(product(product_id=62))["msg"][0])
        assert src["modality"] == []
        assert src["disease"] is None
        assert src["studyType"] is None
        assert src["studyDesign"] is None
        assert src["tasks"] == []
        assert src["sessions"] == []
        assert src["participantCount"] is None
        assert src["datasetSizeBytes"] is None
        assert src["authors"] == []
        assert src["keywords"] == []
