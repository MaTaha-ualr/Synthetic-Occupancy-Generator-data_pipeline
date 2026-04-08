from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.scenario_catalog import get_scenario_catalog_by_id, get_scenario_catalog_entries


def test_scenario_catalog_entries_have_unique_ids() -> None:
    entries = get_scenario_catalog_entries()
    scenario_ids = [str(entry["scenario_id"]).strip() for entry in entries]
    assert len(scenario_ids) == len(set(scenario_ids))


def test_all_canonical_yaml_entries_exist_on_disk() -> None:
    catalog_by_id = get_scenario_catalog_by_id()
    for entry in catalog_by_id.values():
        if entry.get("delivery_mode") != "canonical_yaml":
            continue
        yaml_path = PROJECT_ROOT / str(entry["yaml_path"])
        assert yaml_path.exists(), str(yaml_path)


def test_all_shipped_scenario_yamls_are_listed_in_catalog() -> None:
    catalog_by_id = get_scenario_catalog_by_id()
    yaml_ids = []
    for path in (PROJECT_ROOT / "phase2" / "scenarios").glob("*.yaml"):
        if path.stem.startswith("_") or path.name == "catalog.yaml":
            continue
        yaml_ids.append(path.stem)
    assert set(yaml_ids) == {
        scenario_id
        for scenario_id, entry in catalog_by_id.items()
        if entry.get("delivery_mode") == "canonical_yaml"
    }


def test_supported_parameterized_topologies_are_listed() -> None:
    catalog_by_id = get_scenario_catalog_by_id()
    assert catalog_by_id["custom_single_dataset_dedup"]["status"] == "supported"
    assert catalog_by_id["custom_pairwise_linkage"]["status"] == "supported"
    assert catalog_by_id["custom_multi_dataset_linkage"]["status"] == "supported"


def test_new_canonical_templates_are_marked_supported() -> None:
    catalog_by_id = get_scenario_catalog_by_id()
    for scenario_id in (
        "clean_baseline_linkage",
        "high_noise_identity_drift",
        "high_duplication_dedup",
        "low_overlap_sparse_coverage",
        "asymmetric_source_coverage",
        "three_source_partial_overlap",
    ):
        entry = catalog_by_id[scenario_id]
        assert entry["status"] == "supported"
        assert entry["delivery_mode"] == "canonical_yaml"
        assert entry["runnable"] is True
