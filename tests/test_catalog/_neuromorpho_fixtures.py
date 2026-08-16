"""Shared NeuroMorpho fixtures for catalog tests (pure data — no I/O).

Payload shapes mirror the LIVE neuromorpho.org/api/neuron/select Solr
responses captured during the 2026-08-10 full-corpus investigation.

Each fixture returns a list of neuron dicts that can be fed into the
archive × publication grouping function.
"""

import math


def _nid(start: int) -> int:
    return start


def neuron(
    *,
    archive: str = "Mallick",
    pmid: str = "21228908",
    doi: str = "10.1007/s00429-011-0305-0",
    species: str = "rat",
    brain_region: list[str] | None = None,
    cell_type: list[str] | None = None,
    gender: str = "Male",
    min_age: int = 56,
    max_age: int = 56,
    strain: str = "Sprague-Dawley",
    nid_offset: int = 0,
) -> dict:
    """Single neuron record matching the live Solr schema."""
    if brain_region is None:
        brain_region = ["neocortex"]
    if cell_type is None:
        cell_type = ["principal cell", "pyramidal"]
    return {
        "id": _nid(nid_offset),
        "archive": archive,
        "pmids": [pmid] if pmid else [],
        "dois": [doi] if doi else [],
        "species": species,
        "strain": strain,
        "brain_region": brain_region,
        "cell_type": cell_type,
        "gender": gender,
        "min_age": min_age,
        "max_age": max_age,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Scenario fixture builders
# ─────────────────────────────────────────────────────────────────────────────


def single_archive_one_publication() -> list[dict]:
    """Case 1: Archive has one publication, 3 neurons, all same PMID."""
    return [
        neuron(archive="Mallick", pmid="21228908", doi="10.1007/s00429-011-0305-0", nid_offset=0),
        neuron(archive="Mallick", pmid="21228908", doi="10.1007/s00429-011-0305-0", nid_offset=1,
               species="rat", brain_region=["neocortex", "frontal"],
               cell_type=["principal cell"], gender="Female"),
        neuron(archive="Mallick", pmid="21228908", doi="10.1007/s00429-011-0305-0", nid_offset=2,
               species="rat", brain_region=["neocortex"],
               cell_type=["pyramidal"], gender="Not reported"),
    ]


def single_archive_two_publications() -> list[dict]:
    """Case 2: Archive has two publications → 2 groups."""
    return [
        neuron(archive="Jacobs", pmid="12204204", doi="10.1002/cne.10278",
               species="rat", brain_region=["neocortex", "frontal"],
               cell_type=["pyramidal"], nid_offset=10),
        neuron(archive="Jacobs", pmid="12204204", doi="10.1002/cne.10278",
               species="rat", brain_region=["neocortex", "temporal"],
               cell_type=["pyramidal"], nid_offset=11),
        neuron(archive="Jacobs", pmid="9230750", doi="10.1002/(sici)1098-1063(1997)7:5<571::aid-hipo11>3.0.co;2-2",
               species="mouse", brain_region=["hippocampus"],
               cell_type=["granule"], nid_offset=12),
        neuron(archive="Jacobs", pmid="9230750", doi="10.1002/(sici)1098-1063(1997)7:5<571::aid-hipo11>3.0.co;2-2",
               species="mouse", brain_region=["hippocampus", "dentate gyrus"],
               cell_type=["granule", "mossy"], nid_offset=13),
    ]


def placeholders_only() -> list[dict]:
    """Case 3: Archive with only placeholder (negative) PMIDs → DOI-only groups."""
    return [
        neuron(archive="Siegert", pmid="-42", doi="10.1038/s41593-024-01776-3",
               species="mouse", brain_region=["basal ganglia", "substantia nigra"],
               cell_type=["microglia"], gender="Male", min_age=42, max_age=90, nid_offset=50),
        neuron(archive="Siegert", pmid="-42", doi="10.1038/s41593-024-01776-3",
               species="mouse", brain_region=["main olfactory bulb"],
               cell_type=["microglia"], gender="Female", min_age=35, max_age=70, nid_offset=51),
        neuron(archive="Siegert", pmid="-42", doi="10.1038/s41593-024-01776-3",
               species="mouse", brain_region=["basal ganglia"],
               cell_type=["microglia", "Iba1-positive"], gender="Male", min_age=56, max_age=84, nid_offset=52),
    ]


def mixed_real_and_placeholder() -> list[dict]:
    """Case 4: archive has both real PMID and placeholder records."""
    return [
        neuron(archive="Peng", pmid="-34", doi="10.1016/j.neures.2006.05.014",
               species="rat", brain_region=["neocortex"],
               cell_type=["pyramidal"], nid_offset=60),
        neuron(archive="Peng", pmid="-34", doi="10.1016/j.neures.2006.05.014",
               species="rat", brain_region=["neocortex", "somatosensory"],
               cell_type=["pyramidal"], nid_offset=61),
    ]


def many_neurons_same_contribution() -> list[dict]:
    """Case 5: 50 neurons all same archive+PMID → 1 group."""
    base = "Chiang"
    pmid = "21129968"
    doi = "10.1002/cne.22705"
    out = []
    for i in range(50):
        regions = [["Right Mushroom Body", "Right Calyx"],
                   ["protocerebrum"], ["Right Mushroom Body"]][i % 3]
        ctypes = [["interneuron"], ["Embryo-born"], ["Kenyon cell"]][i % 3]
        out.append(
            neuron(archive=base, pmid=pmid, doi=doi,
                   species="fruit fly", brain_region=regions,
                   cell_type=ctypes, gender="Not reported",
                   min_age=1, max_age=21, strain="Canton-S",
                   nid_offset=100 + i)
        )
    return out


def doi_only_contribution() -> list[dict]:
    """Case 6: No PMID at all (empty list) but DOI present."""
    return [
        neuron(archive="Kuddannaya", pmid="", doi="10.1002/admi.201700819",
               species="rat", brain_region=["neocortex", "frontal"],
               cell_type=["principal cell"], gender="Not reported",
               min_age=1, max_age=1, nid_offset=200),
        neuron(archive="Kuddannaya", pmid="", doi="10.1002/admi.201700819",
               species="rat", brain_region=["neocortex"],
               cell_type=["principal cell"], gender="Male",
               min_age=1, max_age=1, nid_offset=201),
    ]


def archive_without_pmid_but_with_doi() -> list[dict]:
    """Case 7: Multiple neurons sharing archive+DOI (no PMID) → DOI group."""
    return doi_only_contribution()


# ─────────────────────────────────────────────────────────────────────────────
# Aggregate (list + detail) page fixtures for ingestion tests
# ─────────────────────────────────────────────────────────────────────────────


def solr_list_page(neurons: list[dict], page: int = 0, size: int = 500,
                   total: int = 0) -> dict:
    """A Solr /neuron/select response page."""
    if total == 0:
        total = len(neurons)
    return {
        "page": {
            "size": size,
            "totalElements": total,
            "totalPages": math.ceil(total / size) if size else 1,
            "number": page,
        },
        "_embedded": {
            "neuronResources": neurons,
        },
    }
