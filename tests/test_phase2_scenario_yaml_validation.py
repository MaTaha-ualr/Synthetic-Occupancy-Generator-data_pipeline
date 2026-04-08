"""Validate every shipped scenario YAML parses without error and has required fields."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import parse_constraints_config
from sog_phase2.emission import parse_emission_config
from sog_phase2.quality import parse_quality_config
from sog_phase2.selection import parse_selection_config
from sog_phase2.simulator import parse_simulation_config

SCENARIOS_DIR = PROJECT_ROOT / "phase2" / "scenarios"

CANONICAL_SCENARIOS = [
    "single_movers",
    "clean_baseline_linkage",
    "couple_merge",
    "family_birth",
    "divorce_custody",
    "roommates_split",
    "high_noise_identity_drift",
    "low_overlap_sparse_coverage",
    "asymmetric_source_coverage",
    "high_duplication_dedup",
    "three_source_partial_overlap",
]


def _load_scenario(name: str) -> dict:
    path = SCENARIOS_DIR / f"{name}.yaml"
    assert path.exists(), f"Scenario YAML not found: {path}"
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.mark.parametrize("scenario_id", CANONICAL_SCENARIOS)
def test_scenario_yaml_exists_and_parses(scenario_id: str) -> None:
    data = _load_scenario(scenario_id)
    assert isinstance(data, dict)
    assert "scenario_id" in data or "id" in data


@pytest.mark.parametrize("scenario_id", CANONICAL_SCENARIOS)
def test_scenario_has_simulation_section(scenario_id: str) -> None:
    data = _load_scenario(scenario_id)
    sim = data.get("simulation", {})
    cfg = parse_simulation_config(sim)
    assert cfg.granularity in ("monthly", "daily")
    assert cfg.periods > 0


@pytest.mark.parametrize("scenario_id", CANONICAL_SCENARIOS)
def test_scenario_has_valid_emission(scenario_id: str) -> None:
    data = _load_scenario(scenario_id)
    emission = data.get("emission", {})
    cfg = parse_emission_config(emission)
    assert cfg.crossfile_match_mode in (
        "single_dataset", "one_to_one", "one_to_many", "many_to_one", "many_to_many"
    )


@pytest.mark.parametrize("scenario_id", CANONICAL_SCENARIOS)
def test_scenario_has_valid_selection(scenario_id: str) -> None:
    data = _load_scenario(scenario_id)
    selection = data.get("selection", {})
    cfg = parse_selection_config(selection)
    assert cfg.sample_mode in ("all", "count", "pct")


@pytest.mark.parametrize("scenario_id", CANONICAL_SCENARIOS)
def test_scenario_has_valid_constraints(scenario_id: str) -> None:
    data = _load_scenario(scenario_id)
    constraints = data.get("constraints", {})
    cfg = parse_constraints_config(constraints)
    assert cfg.min_marriage_age >= 0
    assert cfg.fertility_age_min >= 0


@pytest.mark.parametrize("scenario_id", CANONICAL_SCENARIOS)
def test_scenario_has_valid_quality(scenario_id: str) -> None:
    data = _load_scenario(scenario_id)
    quality = data.get("quality", {})
    cfg = parse_quality_config(quality)
    assert cfg.household_size_min >= 1


@pytest.mark.parametrize("scenario_id", CANONICAL_SCENARIOS)
def test_scenario_has_required_seed(scenario_id: str) -> None:
    data = _load_scenario(scenario_id)
    assert "seed" in data, f"Scenario {scenario_id} missing 'seed' field"
    assert isinstance(data["seed"], int)


@pytest.mark.parametrize("scenario_id", CANONICAL_SCENARIOS)
def test_scenario_has_phase1_input_or_defaults(scenario_id: str) -> None:
    """Either specifies phase1_input or relies on defaults."""
    data = _load_scenario(scenario_id)
    # Just needs to parse — the pipeline resolves defaults at runtime
    assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# Catalog completeness
# ---------------------------------------------------------------------------

def test_catalog_lists_all_shipped_scenarios() -> None:
    catalog_path = SCENARIOS_DIR / "catalog.yaml"
    assert catalog_path.exists()
    with open(catalog_path, encoding="utf-8") as f:
        catalog = yaml.safe_load(f)
    catalog_ids = {entry.get("scenario_id") for entry in catalog.get("entries", [])}
    for sid in CANONICAL_SCENARIOS:
        assert sid in catalog_ids, f"{sid} not listed in catalog.yaml"


def test_no_orphan_yaml_files() -> None:
    """Every canonical scenario YAML should be in the shipped scenario list.

    Frontend/editor working copies are allowed to live in the same folder when they
    are prefixed with ``_working_`` and should not be treated as shipped scenarios.
    """
    yaml_files = [
        p.stem
        for p in SCENARIOS_DIR.glob("*.yaml")
        if p.stem not in ("catalog",) and not p.stem.startswith("_working_")
    ]
    for name in yaml_files:
        assert name in CANONICAL_SCENARIOS, f"Scenario {name}.yaml not in canonical list"
