"""EBRAINS ingestion tests — in-memory catalog (no live API, no DB).

Verifies the EBRAINS ingestion pipeline contract:
  - EXACT 1,138-record gate (fewer/more refused before any write)
  - dataset_id completeness gate
  - non-neuroscience exclusion gate (the 2 NON census records are never input)
  - Dataset unit = ONE EBRAINS Dataset product (versions/version_ids are
    metadata on the parent, never separate datasets)
  - native dataset DOIs (10.25493/...) are canonical; external-repo DOIs are
    relationship/provenance metadata ONLY (Zenodo, G-Node, OSF, NITRC,
    Mendeley, figshare, Radboud, publication DOIs)
  - publication DOIs and OSF project/container DOIs never become canonical
    identity; no canonical records are ever created for the external
    repositories themselves
  - the four audited exact DANDI/OpenNeuro matches resolve through the generic
    resolver's cross_reference layer and MERGE into the existing canonical
    record (never duplicated; existing canonical DOI never overwritten)
  - per-dataset source URLs are unique identity signals (no shared-page
    collapse needed); idempotent rerun = merges via source_key
  - modality/species mapped strictly from vocab; missing values never fabricated
  - restricted-access records carry no guessed canonical availability
  - failure isolation; duplicate candidate protection
  - ZERO API calls and ZERO asset/file calls
"""
from unittest.mock import MagicMock

from app.catalog.ingest import run_ebrains_ingestion
from app.catalog.normalize import (
    EBRAINS_CENSUS_EXPECTED,
    build_ebrains_source_record,
    canonical_record_from_source,
)

from ._ebrains_fixtures import (
    EBRAINS_EXACT_CANONICALS,
    EBRAINS_EXACT_MATCHES,
    ebrains_external_record,
    ebrains_multiversion_record,
    ebrains_osf_record,
    ebrains_publication_record,
    ebrains_record,
    ebrains_record_b,
    ebrains_records,
)


# ─────────────────────────────────────────────────────────────────────────────
# In-memory catalog collection (subset of the persistence contract)
# ─────────────────────────────────────────────────────────────────────────────


class _MemCursor:
    def __init__(self, docs):
        self._docs = docs

    def __aiter__(self):
        self._it = iter(self._docs)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration

    def limit(self, n):
        return self


class _MemCatalog:
    def __init__(self):
        self.docs: list[dict] = []
        self._seq = 0

    @staticmethod
    def _get(doc, path):
        cur = doc
        for part in str(path).split("."):
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
        return cur

    def _matches(self, doc, query):
        for key, value in query.items():
            val = self._get(doc, key)
            if isinstance(val, list):
                if value not in val:
                    return False
            elif val != value:
                return False
        return True

    async def find_one(self, query):
        for d in self.docs:
            if self._matches(d, query):
                return d
        return None

    def find(self, query):
        return _MemCursor([d for d in self.docs if self._matches(d, query)])

    async def insert_one(self, doc):
        self._seq += 1
        doc["_id"] = f"oid{self._seq}"
        self.docs.append(doc)

    async def replace_one(self, filt, doc):
        for i, d in enumerate(self.docs):
            if d.get("_id") == filt.get("_id"):
                self.docs[i] = doc
                return None
        raise KeyError(filt)


def _make_db(coll: _MemCatalog) -> MagicMock:
    db = MagicMock()
    db.__getitem__ = MagicMock(return_value=coll)
    return db


def _seed(coll: _MemCatalog, docs: list[dict]) -> None:
    for i, doc in enumerate(docs):
        doc["_id"] = f"seed-{i}"
        coll.docs.append(doc)


# ─────────────────────────────────────────────────────────────────────────────
# EXACT 1,138-record gate + non-neuroscience exclusion
# ─────────────────────────────────────────────────────────────────────────────


async def test_exact_1138_input_passes_gate_and_inserts_all():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(
        db, records=ebrains_records(EBRAINS_CENSUS_EXPECTED)
    )

    assert stats["gate_failed"] is False
    assert stats["input"] == EBRAINS_CENSUS_EXPECTED
    assert stats["normalized"] == EBRAINS_CENSUS_EXPECTED
    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert stats["failed"] == 0
    assert stats["validation_failed"] == 0
    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED
    assert stats["api_calls"] == 0
    assert stats["asset_calls"] == 0


async def test_input_below_1138_is_refused_before_any_write():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(
        db, records=ebrains_records(EBRAINS_CENSUS_EXPECTED - 1)
    )

    assert stats["gate_failed"] is True
    assert "exactly" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert stats["merged"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_input_above_1138_is_refused_before_any_write():
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(
        db, records=ebrains_records(EBRAINS_CENSUS_EXPECTED + 1)
    )

    assert stats["gate_failed"] is True
    assert stats["inserted"] == 0
    assert stats["merged"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_records_missing_dataset_id_refuse_the_run():
    coll = _MemCatalog()
    db = _make_db(coll)

    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0]["dataset_id"] = None

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["gate_failed"] is True
    assert "dataset_id" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_malformed_record_refuses_the_whole_run():
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = "not-a-dict"  # corrupted record
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["gate_failed"] is True
    assert "dataset_id" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


async def test_non_neuroscience_record_refuses_the_whole_run():
    """The 2 NON products of the raw census are NEVER part of the approved
    input — if one ever slips in, the entire run is refused."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0]["neuro_relevance"] = "non_neuroscience"
    records[0]["category"] = "NON"
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["gate_failed"] is True
    assert "non-neuroscience" in (stats["gate_reason"] or "")
    assert stats["inserted"] == 0
    assert len(coll.docs) == 0  # NOTHING written


# ─────────────────────────────────────────────────────────────────────────────
# Canonical record contract — dataset unit, identity, DOIs
# ─────────────────────────────────────────────────────────────────────────────


async def test_each_dataset_is_one_canonical_record_with_ebrains_identity():
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_record()
    records[1] = ebrains_record_b()
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_ebrains_ingestion(db, records=records)

    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED
    for doc in coll.docs:
        assert doc["sourceKeys"][0].startswith("ebrains:ebrains:")
        assert len(doc["sourceKeys"]) == 1
        assert len(doc["sources"]) == 1
    seeded = next(
        d for d in coll.docs
        if "ebrains:ebrains:9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d" in d["sourceKeys"]
    )
    # the census sourceUrl is preserved VERBATIM (real, unique per dataset)
    assert (
        seeded["sources"][0]["sourceUrl"]
        == "https://search.kg.ebrains.eu/instances/9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d"
    )
    assert (
        seeded["provenance"]["identity"]["sourceUrlNorm"]
        == "http://search.kg.ebrains.eu/instances/9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d"
    )


async def test_native_dataset_doi_preserved_as_canonical_doi():
    """EBRAINS-minted dataset-level DOIs (10.25493/...) ARE canonical."""
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_ebrains_ingestion(
        db, records=ebrains_records(EBRAINS_CENSUS_EXPECTED)
    )

    for doc in coll.docs:
        assert (doc["doi"] or "").startswith("10.25493/")
        assert doc["provenance"]["identity"]["doi"] == doc["doi"]


async def test_publication_doi_is_not_canonical_identity():
    """4 C-class records carry PUBLICATION DOIs — canonical doi stays null;
    the publication DOI is relationship/provenance metadata only."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_publication_record()
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED
    pub = next(
        d for d in coll.docs
        if "ebrains:ebrains:13e6ec48-1234-4abc-9def-0123456789ab" in d["sourceKeys"]
    )
    assert pub["doi"] is None                                  # NOT canonical
    assert pub["sources"][0]["publication"]["articleDoi"] == "10.1371/journal.pbio.3000678"
    assert pub["sources"][0]["publication"]["relatedIdentifiers"] == [
        {"identifier": "10.1371/journal.pbio.3000678"}
    ]
    assert pub["provenance"]["identity"]["doi"] is None


async def test_external_repo_doi_is_not_canonical_identity():
    """15 related/distinct external-resource records (Zenodo, G-Node, NITRC,
    Mendeley, figshare, Radboud): canonical doi null, external DOI preserved
    as relationship/provenance only — and NO canonical record is ever created
    for the external repository itself."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_external_record()
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED
    ext = next(
        d for d in coll.docs
        if "ebrains:ebrains:7757e057-1234-4abc-9def-0123456789ab" in d["sourceKeys"]
    )
    assert ext["doi"] is None                                  # NOT canonical
    assert ext["sources"][0]["publication"]["articleDoi"] == "10.12751/g-node.l7xbnd"
    assert ext["provenance"]["identity"]["doi"] is None
    assert ext["sources"][0]["publication"]["referencesAndLinks"] == []  # no mirror guess
    # no external-repository source keys anywhere (g-node/zenodo/... records)
    for doc in coll.docs:
        for key in doc["sourceKeys"]:
            assert not key.startswith(("g-node:", "zenodo:", "osf:", "nitrc:",
                                       "mendeley:", "figshare:", "radboud:"))
            assert key.startswith("ebrains:ebrains:")


async def test_osf_project_dois_are_not_datasets():
    """3 D-class OSF project/container DOIs are relationship metadata, never
    canonical identity — and no OSF record is ever created."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_osf_record()
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED
    osf = next(
        d for d in coll.docs
        if "ebrains:ebrains:5706c36d-1234-4abc-9def-0123456789ab" in d["sourceKeys"]
    )
    assert osf["doi"] is None
    assert osf["sources"][0]["publication"]["articleDoi"] == "10.17605/osf.io/dvmrb"
    for doc in coll.docs:
        for key in doc["sourceKeys"]:
            assert not key.startswith("osf:")
    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED  # no extra OSF records


# ─────────────────────────────────────────────────────────────────────────────
# Four audited exact DANDI/OpenNeuro matches → generic resolver cross_reference
# ─────────────────────────────────────────────────────────────────────────────


async def test_exact_dandi_openneuro_matches_merge_never_duplicate():
    """The 4 A-class records carry real mirror-page references
    (publication.referencesAndLinks) that the generic resolver's
    cross_reference layer uses to merge into the EXISTING canonical DANDI/
    OpenNeuro records — 4 merges, 1,134 new inserts, no duplicates."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED - len(EBRAINS_EXACT_MATCHES))
    records = list(EBRAINS_EXACT_MATCHES) + records
    coll = _MemCatalog()
    _seed(coll, EBRAINS_EXACT_CANONICALS)  # existing DANDI/OpenNeuro records
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["gate_failed"] is False
    assert stats["merged"] == 4
    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED - 4
    assert stats["matched_via"].get("cross_reference") == 4
    # no duplicate DANDI/OpenNeuro records; the existing canonicals were merged
    assert len(coll.docs) == 4 + (EBRAINS_CENSUS_EXPECTED - 4)
    merged_ids = {d["canonicalDatasetId"] for d in coll.docs} & {
        c["canonicalDatasetId"] for c in EBRAINS_EXACT_CANONICALS
    }
    assert merged_ids == {
        c["canonicalDatasetId"] for c in EBRAINS_EXACT_CANONICALS
    }
    # every match went through the generic cross_reference identity layer
    for c in EBRAINS_EXACT_CANONICALS:
        merged = next(d for d in coll.docs if d["canonicalDatasetId"] == c["canonicalDatasetId"])
        assert merged["provenance"]["identity"]["matchedVia"] == "cross_reference"
        assert any(s["repository"] == "ebrains" for s in merged["sources"])


async def test_existing_canonical_doi_never_overwritten_by_ebrains_merge():
    """Preserves the identity rules of the existing canonical records: the DANDI
    canonicals keep doi=null and the OpenNeuro canonicals keep their dataset
    DOI — the EBRAINS external DOI never becomes or overwrites canonical DOI."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED - len(EBRAINS_EXACT_MATCHES))
    records = list(EBRAINS_EXACT_MATCHES) + records
    coll = _MemCatalog()
    _seed(coll, EBRAINS_EXACT_CANONICALS)
    db = _make_db(coll)

    await run_ebrains_ingestion(db, records=records)

    for c in EBRAINS_EXACT_CANONICALS:
        merged = next(d for d in coll.docs if d["canonicalDatasetId"] == c["canonicalDatasetId"])
        assert merged["doi"] == c["doi"]  # unchanged (null for DANDI, OpenNeuro DOI for ON)
        if c["doi"] is None:
            assert merged["provenance"]["identity"]["doi"] is None
        else:
            assert merged["provenance"]["identity"]["doi"] == c["doi"]


async def test_four_exact_matches_not_found_without_seeded_canonicals():
    """Without the existing DANDI/OpenNeuro canonical records the resolver
    simply inserts the EBRAINS records as new canonical records (no hard-coded
    match list anywhere)."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED - len(EBRAINS_EXACT_MATCHES))
    records = list(EBRAINS_EXACT_MATCHES) + records
    coll = _MemCatalog()  # EMPTY catalog
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["merged"] == 0
    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED
    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED


# ─────────────────────────────────────────────────────────────────────────────
# Dataset unit — versions are metadata, never separate datasets
# ─────────────────────────────────────────────────────────────────────────────


async def test_multiversion_parent_is_one_canonical_record():
    """212 multi-version EBRAINS parents (n_total_versions > 1) stay ONE
    product; version_ids / n_versions are metadata on the parent."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_multiversion_record()
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED
    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED  # NOT one record per version
    parent = next(
        d for d in coll.docs
        if "ebrains:ebrains:c3d4e5f6-7a8b-9c0d-1e2f-3a4b5c6d7e8f" in d["sourceKeys"]
    )
    snap = parent["sources"][0]["snapshot"]
    assert snap["nVersions"] == 5
    assert snap["nIndexedVersions"] == 2
    assert len(snap["versionIds"]) == 2
    assert snap["multiVersion"] is True
    assert parent["rawMetadata"]["ebrains"]["n_versions"] == 5
    # never a per-version identity
    assert parent["sourceKeys"] == ["ebrains:ebrains:c3d4e5f6-7a8b-9c0d-1e2f-3a4b5c6d7e8f"]


async def test_version_ids_never_become_datasets():
    """DatasetVersion is NEVER a canonical product — each record yields exactly
    one canonical record keyed on the dataset_id, never on version_ids."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_multiversion_record()
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_ebrains_ingestion(db, records=records)

    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED
    for doc in coll.docs:
        assert "DatasetVersion" not in " ".join(doc["sourceKeys"]).lower()
        assert len(doc["sourceKeys"]) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Unique per-dataset source URLs + duplicate protection
# ─────────────────────────────────────────────────────────────────────────────


async def test_unique_source_urls_keep_datasets_distinct():
    """EBRAINS source URLs are unique per dataset (verified 1138/1138), so the
    URL layer is a valid identity signal and distinct datasets never collapse."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED
    assert stats["merged"] == 0
    url_norms = {d["provenance"]["identity"]["sourceUrlNorm"] for d in coll.docs}
    assert len(url_norms) == EBRAINS_CENSUS_EXPECTED  # all distinct


async def test_duplicate_dataset_id_detected_and_never_duplicated():
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_records(1)[0]
    records[1] = ebrains_records(1)[0]  # same dataset_id twice
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["duplicate_source_identities"] != []  # detected up front
    assert "ebrains:ebrains:00000001-0000-4000-8000-000000000000" in (
        stats["duplicate_source_identities"][0]["identity"]
    )
    assert stats["inserted"] + stats["merged"] == EBRAINS_CENSUS_EXPECTED
    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED - 1


# ─────────────────────────────────────────────────────────────────────────────
# Generic identity resolver integration
# ─────────────────────────────────────────────────────────────────────────────


async def test_rerun_is_idempotent_no_duplicates():
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    first = await run_ebrains_ingestion(db, records=records)
    assert first["inserted"] == EBRAINS_CENSUS_EXPECTED

    second = await run_ebrains_ingestion(db, records=records)
    assert second["inserted"] == 0
    assert second["merged"] == EBRAINS_CENSUS_EXPECTED
    # native records carry a canonical dataset DOI, so the DOI layer (the
    # STRONGEST identity signal) refreshes them on rerun — no duplicates
    assert "doi" in second["matched_via"]
    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED  # no duplicates


async def test_same_title_cross_repo_record_is_ambiguous_not_merged():
    """A non-EBRAINS canonical record with an identical title must NOT be
    merged — the generic resolver reports it ambiguous and inserts the EBRAINS
    record (no fuzzy matching, no force-merge)."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    coll = _MemCatalog()
    db = _make_db(coll)

    seeded = canonical_record_from_source(
        build_ebrains_source_record(records[0])
    )
    seeded["sourceKeys"] = ["openneuro:datasets/ds000999"]
    seeded["sources"] = [
        {
            "repository": "openneuro",
            "sourceDatasetId": "datasets/ds000999",
            "sourceUrl": "https://openneuro.org/datasets/ds000999",
            "title": "EBRAINS dataset 1",
            "modality": [],
            "participantCount": None,
        }
    ]
    seeded["provenance"]["identity"]["sourceUrlNorm"] = "http://openneuro.org/datasets/ds000999"
    seeded["participantCount"] = None
    seeded["modality"] = []
    # clear the EBRAINS-derived DOI so only the TITLE overlaps (a title alone
    # is never enough to merge — the DOI/URL/sourceKey layers must not fire)
    seeded["doi"] = None
    seeded["provenance"]["identity"]["doi"] = None
    seeded["_id"] = "seeded"
    coll.docs.append(seeded)

    stats = await run_ebrains_ingestion(db, records=records)

    # title matches but no strong cross-repo signal → never force-merged
    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED
    assert stats["merged"] == 0
    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED + 1  # seeded + all EBRAINS
    ebrains_docs = [d for d in coll.docs if d["sourceKeys"][0].startswith("ebrains:ebrains:")]
    assert len(ebrains_docs) == EBRAINS_CENSUS_EXPECTED


# ─────────────────────────────────────────────────────────────────────────────
# Scientific metadata — strict mapping, no fabrication
# ─────────────────────────────────────────────────────────────────────────────


async def test_modality_mapped_strictly_from_vocab():
    """Only MODALITY_VOCAB technique/experimental-approach tokens map to
    canonical modality; unknown tokens stay in modalityRaw only."""
    coll = _MemCatalog()
    db = _make_db(coll)
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_record(
        technique=(
            "functional magnetic resonance imaging;diffusion-weighted imaging;"
            "electroencephalography;custom analysis protocol"
        ),
        experimental_approach="behaviour;some novel approach",
    )

    await run_ebrains_ingestion(db, records=records)

    seeded = next(
        d for d in coll.docs
        if "ebrains:ebrains:9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d" in d["sourceKeys"]
    )
    assert set(seeded["modality"]) == {"fmri", "dti", "eeg"}
    assert "functional magnetic resonance imaging" in seeded["sources"][0]["modalityRaw"]
    # the unknown tokens never leaked into the canonical list
    assert "custom analysis protocol" not in seeded["modality"]
    assert "some novel approach" not in seeded["modality"]


async def test_blank_species_never_fabricated():
    """36 census records have blank species — canonical species stays empty,
    no fabrication."""
    coll = _MemCatalog()
    db = _make_db(coll)
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_record(species="")

    await run_ebrains_ingestion(db, records=records)

    seeded = next(
        d for d in coll.docs
        if "ebrains:ebrains:9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d" in d["sourceKeys"]
    )
    assert seeded["species"] == []
    assert seeded["sources"][0]["speciesRaw"] == []
    assert seeded["rawMetadata"]["ebrains"]["species"] == ""


async def test_known_species_preserved_not_invented():
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_record(species="Homo sapiens;Mus musculus")
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_ebrains_ingestion(db, records=records)

    seeded = next(
        d for d in coll.docs
        if "ebrains:ebrains:9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d" in d["sourceKeys"]
    )
    assert set(seeded["species"]) == {"homo sapiens", "mus musculus"}


# ─────────────────────────────────────────────────────────────────────────────
# No access guessing / no downloads
# ─────────────────────────────────────────────────────────────────────────────


async def test_restricted_access_preserved_verbatim_no_canonical_guess():
    """16 census records are 'restricted access' — canonical availability is
    never guessed (stays None); the verbatim accessibility value is preserved
    in the snapshot and rawMetadata."""
    coll = _MemCatalog()
    db = _make_db(coll)
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_record(accessibility="restricted access")

    await run_ebrains_ingestion(db, records=records)

    seeded = next(
        d for d in coll.docs
        if "ebrains:ebrains:9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d" in d["sourceKeys"]
    )
    assert seeded["availability"] is None                  # no guessing
    assert seeded["sources"][0]["snapshot"]["accessibility"] == "restricted access"
    assert seeded["rawMetadata"]["ebrains"]["accessibility"] == "restricted access"


async def test_no_api_calls_no_asset_downloads_no_participant_data():
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_record(accessibility="controlled access")
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["api_calls"] == 0     # census artifact is authoritative
    assert stats["asset_calls"] == 0   # no file/asset downloads
    for d in coll.docs:
        assert d.get("datasetSizeBytes") is None
        assert d.get("subjectIds") is None
        assert d.get("participantCount") is None
        assert d["sources"][0].get("subjectIds") is None
        assert d["sources"][0].get("participantCount") is None


# ─────────────────────────────────────────────────────────────────────────────
# Raw metadata preservation + failure handling
# ─────────────────────────────────────────────────────────────────────────────


async def test_rawmetadata_ebrains_preserved_verbatim():
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0] = ebrains_record()
    coll = _MemCatalog()
    db = _make_db(coll)

    await run_ebrains_ingestion(db, records=records)

    seeded = next(
        d for d in coll.docs
        if "ebrains:ebrains:9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d" in d["sourceKeys"]
    )
    raw = seeded["rawMetadata"]["ebrains"]
    assert raw["dataset_id"] == "9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d"
    assert raw["title"] == "MOBILE: Multimodal Whole Brain Imaging in Epilepsy"
    assert raw["doi"] == "10.25493/RPSQ-END"
    assert raw["accessibility"] == "free access"
    assert raw["version_ids"] == "67eea200-0033-4bae-8dde-9d9f2ed463af"
    assert raw["url"] == "https://search.kg.ebrains.eu/instances/9a1b2c3d-4e5f-6a7b-8c9d-0e1f2a3b4c5d"


async def test_builder_tolerates_absent_optional_fields():
    """A record missing optional fields still normalizes (never fabricated)."""
    record = ebrains_records(1)[0]
    del record["experimental_approach"]
    del record["keywords"]
    del record["accessibility"]
    source = build_ebrains_source_record(record)
    assert source["sourceDatasetId"] == "ebrains:00000001-0000-4000-8000-000000000000"
    assert source["snapshot"]["experimentalApproach"] == []
    assert source["keywords"] == []
    assert source["snapshot"]["accessibility"] is None
    assert source["title"] == "EBRAINS dataset 1"


async def test_validation_failure_isolated_other_records_insert():
    """A record that fails per-source validation (no sourceUrl) is counted and
    skipped — the rest of the run continues."""
    records = ebrains_records(EBRAINS_CENSUS_EXPECTED)
    records[0]["url"] = None  # _validate_source: missing sourceUrl
    coll = _MemCatalog()
    db = _make_db(coll)

    stats = await run_ebrains_ingestion(db, records=records)

    assert stats["gate_failed"] is False
    assert stats["validation_failed"] == 1
    assert stats["validated_ok"] == EBRAINS_CENSUS_EXPECTED - 1
    assert stats["inserted"] == EBRAINS_CENSUS_EXPECTED - 1
    assert len(coll.docs) == EBRAINS_CENSUS_EXPECTED - 1
