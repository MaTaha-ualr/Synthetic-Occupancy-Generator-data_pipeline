from __future__ import annotations

import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "phase2" / "src"))

from sog_phase2.scenario_catalog import get_scenario_catalog_by_id, get_scenario_catalog_entries

RATE_EVENT_FIELDS = {
    "move_rate_pct": "MOVE",
    "cohabit_rate_pct": "COHABIT",
    "birth_rate_pct": "BIRTH",
    "divorce_rate_pct": "DIVORCE",
    "leave_home_rate_pct": "LEAVE_HOME",
    "split_rate_pct": "LEAVE_HOME",
    "death_rate_pct": "DEATH",
    "name_change_rate_pct": "NAME_CHANGE",
    "adoption_rate_pct": "ADOPTION",
}

CATALOG_CARDINALITY_BY_MATCH_MODE = {
    "single_dataset": "dedup",
    "one_to_one": "one_to_one",
    "one_to_many": "one_to_many",
    "many_to_one": "many_to_one",
    "many_to_many": "many_to_many",
}


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


def test_canonical_catalog_metadata_matches_yaml_contract() -> None:
    catalog_by_id = get_scenario_catalog_by_id()
    for scenario_id, entry in catalog_by_id.items():
        if entry.get("delivery_mode") != "canonical_yaml":
            continue
        yaml_path = PROJECT_ROOT / str(entry["yaml_path"])
        scenario = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))

        assert scenario["scenario_id"] == scenario_id

        emission = scenario.get("emission") or {}
        match_mode = str(emission.get("crossfile_match_mode", "")).strip()
        assert entry["cardinality"] == CATALOG_CARDINALITY_BY_MATCH_MODE[match_mode]

        parameters = scenario.get("parameters") or {}
        enabled_events = {
            event
            for field, event in RATE_EVENT_FIELDS.items()
            if float(parameters.get(field, 0.0) or 0.0) > 0.0
        }
        assert set(entry.get("primary_events") or []) == enabled_events


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
        "name_change_lifecycle",
        "death_survivor_persistence",
        "adoption_blended_family",
    ):
        entry = catalog_by_id[scenario_id]
        assert entry["status"] == "supported"
        assert entry["delivery_mode"] == "canonical_yaml"
        assert entry["runnable"] is True
