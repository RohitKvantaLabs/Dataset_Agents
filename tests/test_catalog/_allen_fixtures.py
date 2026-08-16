"""Shared Allen Brain Atlas fixtures for catalog tests (pure data — no I/O).

Payload shapes mirror the LIVE Allen RMA query API (verified 2026-08-16):

- ``product()``: a Product row from ``criteria=model::Product`` (base fields:
  id, name, abbreviation, description, resource, species, tags).
- ``include_payload()``: the per-product include response
  (``criteria=model::Product[id$eqN]&include=data_sets,specimens,donors``)
  with ``data_sets`` / ``specimens`` / ``donors`` arrays.
- ``ages_payload()``: the Age model rows (id → name/days/age_group_id/...).
- ``enumeration_payload()``: the 64-product list response.
"""

import math


def product(
    *,
    product_id: int = 62,
    name: str = "Multi-plane optical physiology during image change detection",
    abbreviation: str = "ImageChangeDetectionMultiPlaneOphys",
    description: str = "Time series of behavioral variables and neural activity",
    resource: str = "Allen Brain Observatory",
    species: str = "Mouse",
    tags=None,
) -> dict:
    """Single Product row (base fields only)."""
    return {
        "abbreviation": abbreviation,
        "description": description,
        "id": product_id,
        "name": name,
        "product_name_facet": 823251084,
        "resource": resource,
        "species": species,
        "species_name_facet": 1861523945,
        "tags": tags,
    }


def dataset_row(*, ds_id: int, name: str | None = None, failed: bool = False) -> dict:
    """A DataSet child row (field set mirrors the live RMA DataSet model)."""
    return {
        "ar_association_key_name": "26",
        "blue_channel": None,
        "delegate": False,
        "expression": False,
        "failed": failed,
        "failed_facet": 734881840,
        "green_channel": None,
        "id": ds_id,
        "name": name,
        "plane_of_section_id": 4,
        "qc_date": "2011-12-14T18:20:48Z",
        "red_channel": None,
        "reference_space_id": None,
        "rnaseq_design_id": None,
        "section_thickness": 25.0,
        "specimen_id": 4325,
        "sphinx_id": 50940,
        "storage_directory": None,
        "weight": 5210,
    }


def specimen_row(*, specimen_id: int, name: str = "343-0935", donor_id: int = 9,
                 structure_id: int | None = None, hemisphere: str = "(none)") -> dict:
    """A Specimen child row (mirrors the live RMA Specimen model)."""
    return {
        "cell_prep_sample_id": None,
        "cell_reporter_id": None,
        "cortex_layer_id": None,
        "data": None,
        "donor_id": donor_id,
        "ephys_result_id": None,
        "external_specimen_name": None,
        "failed_facet": 734881840,
        "hemisphere": hemisphere,
        "id": specimen_id,
        "is_cell_specimen": False,
        "is_ish": False,
        "name": name,
        "parent_id": None,
        "parent_x_coord": None,
        "parent_y_coord": None,
        "parent_z_coord": None,
        "pinned_radius": None,
        "rna_integrity_number": None,
        "specimen_id_path": f"/{specimen_id}/",
        "sphinx_id": 1380,
        "structure_id": structure_id,
        "tissue_ph": None,
        "treatment_id": None,
        "weight": 9000,
    }


def donor_row(*, donor_id: int, name: str = "UMB4849", sex: str = "M",
              sex_full_name: str = "Male", strain: str = "unknown",
              age_id: int | None = 62, organism_id: int = 1,
              condition_description: str | None = None) -> dict:
    """A Donor child row (mirrors the live RMA Donor model field set)."""
    return {
        "age_id": age_id,
        "ar_association_key_name": "26",
        "chemotherapy": None,
        "condition_description": condition_description,
        "data": None,
        "date_of_birth": None,
        "donor_condition_description_facet": 4257123317,
        "donor_race_only_facet": 2904991687,
        "donor_sex_facet": 4122955671,
        "donor_strain_facet": 2904991687,
        "donor_strain_only_facet": None,
        "egfr_amplification": None,
        "extent_of_resection": None,
        "external_donor_name": None,
        "full_genotype": None,
        "handedness": "unknown",
        "id": donor_id,
        "initial_kps": None,
        "mgmt_ihc": None,
        "mgmt_methylation": None,
        "molecular_subtype": None,
        "multifocal": None,
        "name": name,
        "organism_id": organism_id,
        "pmi": None,
        "primary_tissue_source": "UCSD",
        "pten_deletion": None,
        "race_only": "unknown",
        "radiation_therapy": None,
        "recurrence_by_six_months": None,
        "sex": sex,
        "sex_full_name": sex_full_name,
        "sleep_state": None,
        "smoker": None,
        "strain": strain,
        "strain_only": None,
        "survival_days": None,
        "tags": None,
        "theiler_stage": None,
        "time_to_progression_or_recurrence": None,
        "transgenic_mouse_id": None,
        "tumor_status": None,
        "weight_grams": None,
    }


def include_payload(
    p: dict,
    data_sets: list[dict] | None = None,
    specimens: list[dict] | None = None,
    donors: list[dict] | None = None,
) -> dict:
    """A per-product include response dict (single row, success)."""
    row = dict(p)
    if data_sets is not None:
        row["data_sets"] = list(data_sets)
    if specimens is not None:
        row["specimens"] = list(specimens)
    if donors is not None:
        row["donors"] = list(donors)
    return {"success": True, "id": 0, "start_row": 0, "num_rows": 1,
            "total_rows": 1, "msg": [row]}


def enumeration_payload(products: list[dict]) -> dict:
    """The full Product enumeration response."""
    return {"success": True, "id": 0, "start_row": 0, "num_rows": len(products),
            "total_rows": len(products), "msg": list(products)}


def ages_payload() -> dict:
    """The Age model response (id → name/days/age_group_id/embryonic/organism)."""
    rows = [
        {"age_group_id": 1, "days": 13.5, "description": None, "embryonic": True,
         "id": 4, "name": "E13.5", "organism_id": 2, "tags": "prenatal embryonic"},
        {"age_group_id": 3, "days": 56.0, "description": None, "embryonic": False,
         "id": 62, "name": "P56", "organism_id": 2, "tags": "postnatal"},
        {"age_group_id": 5, "days": 21900.0, "description": None, "embryonic": False,
         "id": 90, "name": "60 years", "organism_id": 1, "tags": "adult"},
    ]
    return {"success": True, "id": 0, "start_row": 0, "num_rows": len(rows),
            "total_rows": len(rows), "msg": rows}


def ages_map() -> dict:
    """age_id → Age row (the map built by the ingestion from ages_payload())."""
    return {int(r["id"]): r for r in ages_payload()["msg"]}


# ─────────────────────────────────────────────────────────────────────────────
# Representative products across species/categories (for the smoke test)
# ─────────────────────────────────────────────────────────────────────────────

SMOKE_PRODUCT_IDS = [62, 26, 28, 33, 34]


def all_64_products() -> list[dict]:
    """A deterministic 64-product enumeration (ids 1..64, varied species)."""
    resources = [
        "Allen Brain Observatory", "Allen Human Brain Atlas",
        "Allen Mouse Brain Atlas", "Allen Mouse Brain Connectivity Atlas",
        "Mouse Cell Types Database", "NIH Blueprint Non-Human Primate (NHP) Atlas",
        "Allen Spinal Cord Atlas", "BrainSpan Atlas of the Developing Human Brain",
    ]
    species = ["Mouse", "Human", "NHP"]
    out = []
    for i in range(1, 65):
        out.append(
            product(
                product_id=i,
                name=f"Allen Product {i}",
                abbreviation=f"ABBR{i}",
                description=f"Description for product {i}",
                resource=resources[i % len(resources)],
                species=species[i % len(species)],
            )
        )
    return out
