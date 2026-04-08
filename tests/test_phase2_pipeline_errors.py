"""Pipeline error handling and invalid config tests."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.pipeline import run_scenario_pipeline


def _write_phase2_params(project_root: Path) -> None:
    params_dir = project_root / "Data" / "phase2_params"
    params_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"metric_id": "moved_past_year_pct", "value_pct": 11.8, "source_id": "acs_2024"}]
    ).to_csv(params_dir / "mobility_overall_acs_2024.csv", index=False)
    pd.DataFrame([
        {"age_cohort_id": "age_0_17", "moved_past_year_pct": 10.0, "population": 1000, "source_id": "acs_2024"},
        {"age_cohort_id": "age_18_24", "moved_past_year_pct": 20.0, "population": 800, "source_id": "acs_2024"},
        {"age_cohort_id": "age_25_34", "moved_past_year_pct": 18.0, "population": 900, "source_id": "acs_2024"},
        {"age_cohort_id": "age_35_64", "moved_past_year_pct": 9.0, "population": 1500, "source_id": "acs_2024"},
        {"age_cohort_id": "age_65_plus", "moved_past_year_pct": 6.0, "population": 700, "source_id": "acs_2024"},
    ]).to_csv(params_dir / "mobility_by_age_cohort_acs_2024.csv", index=False)
    pd.DataFrame([
        {"metric_id": "marriage_rate", "value": 6.0, "source_id": "cdc_2023"},
        {"metric_id": "divorce_rate", "value": 2.4, "source_id": "cdc_2023"},
    ]).to_csv(params_dir / "marriage_divorce_rates_cdc_2023.csv", index=False)
    pd.DataFrame([
        {"age_group": "20-24", "birth_rate_per_1000_women": 60.0, "source_id": "nchs_2024"},
    ]).to_csv(params_dir / "fertility_by_age_nchs_2024.csv", index=False)
    pd.DataFrame([
        {"household_type_id": "solo_house", "share_of_all_households_pct": 50.0, "source_id": "acs_2024"},
        {"household_type_id": "couple", "share_of_all_households_pct": 50.0, "source_id": "acs_2024"},
    ]).to_csv(params_dir / "household_type_shares_acs_2024.csv", index=False)
    (params_dir / "phase2_priors_snapshot.json").write_text(json.dumps({"fertility": {}}), encoding="utf-8")
    (params_dir / "sources.json").write_text(json.dumps({"sources": []}), encoding="utf-8")
    (params_dir / "manifest.json").write_text(json.dumps({"status": "ok"}), encoding="utf-8")


def _write_phase1_baseline(project_root: Path, n: int = 20) -> None:
    phase1_dir = project_root / "phase1" / "outputs_phase1"
    phase1_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "RecordKey": str(i), "PersonKey": str(i), "EntityRecordIndex": "1",
            "AddressKey": f"A{i}", "FormalFirstName": f"First{i}", "MiddleName": "",
            "LastName": f"Last{i}", "Suffix": "", "FormalFullName": f"First{i} Last{i}",
            "Gender": "female" if i % 2 == 0 else "male", "Ethnicity": "White",
            "DOB": f"{1980 + i}-01-01", "Age": str(46 - i),
            "AgeBin": "age_18_34" if (46 - i) <= 34 else "age_35_64",
            "SSN": f"{i:03d}-{i:02d}-{i:04d}", "Phone": f"555-{i:07d}",
            "ResidenceType": "HOUSE", "ResidenceStartDate": "2020-01-01",
        })
    pd.DataFrame(rows).to_csv(phase1_dir / "Phase1_people_addresses.csv", index=False)
    (phase1_dir / "Phase1_people_addresses.manifest.json").write_text(
        json.dumps({"row_count": n}), encoding="utf-8")


def _write_scenario(project_root: Path, scenario_id: str, seed: int, **overrides) -> Path:
    scenarios_dir = project_root / "phase2" / "scenarios"
    scenarios_dir.mkdir(parents=True, exist_ok=True)
    scenario = {
        "scenario_id": scenario_id, "seed": seed,
        "phase1": {
            "data_path": "phase1/outputs_phase1/Phase1_people_addresses.csv",
            "manifest_path": "phase1/outputs_phase1/Phase1_people_addresses.manifest.json",
        },
        "selection": {"sample": {"mode": "count", "value": 10}},
        "simulation": {"granularity": "monthly", "start_date": "2026-01-01", "periods": 4},
        "parameters": {"move_rate_pct": 5.0, "cohabit_rate_pct": 0.0, "birth_rate_pct": 0.0, "divorce_rate_pct": 0.0, "split_rate_pct": 0.0},
        "emission": {
            "crossfile_match_mode": "single_dataset",
            "datasets": [{"dataset_id": "registry", "snapshot": "simulation_end", "appearance_pct": 100.0, "duplication_pct": 0.0, "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0}}],
        },
        "quality": {"household_size_range": {"min": 1, "max": 8}},
        "constraints": {},
    }
    scenario.update(overrides)
    path = scenarios_dir / f"{scenario_id}.yaml"
    path.write_text(yaml.safe_dump(scenario, sort_keys=False), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Missing inputs
# ---------------------------------------------------------------------------

def test_pipeline_raises_on_missing_scenario_yaml(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        run_scenario_pipeline(
            scenario_yaml_path=tmp_path / "nonexistent.yaml",
            runs_root=tmp_path / "runs",
            project_root=tmp_path,
        )


def test_pipeline_raises_on_missing_phase1_csv(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_phase2_params(project_root)
    # Don't write Phase-1 baseline
    yaml_path = _write_scenario(project_root, "bad_phase1", 999)
    with pytest.raises(FileNotFoundError):
        run_scenario_pipeline(
            scenario_yaml_path=yaml_path,
            runs_root=project_root / "phase2" / "runs",
            project_root=project_root,
        )


# ---------------------------------------------------------------------------
# Overwrite protection
# ---------------------------------------------------------------------------

def test_pipeline_raises_if_run_exists_and_no_overwrite(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_phase2_params(project_root)
    _write_phase1_baseline(project_root)
    yaml_path = _write_scenario(project_root, "overwrite_test", 20260501)

    # First run should succeed
    run_scenario_pipeline(
        scenario_yaml_path=yaml_path,
        runs_root=project_root / "phase2" / "runs",
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )
    # Second run without overwrite should raise
    with pytest.raises(FileExistsError):
        run_scenario_pipeline(
            scenario_yaml_path=yaml_path,
            runs_root=project_root / "phase2" / "runs",
            project_root=project_root,
            run_date="2026-04-05",
            overwrite=False,
        )


# ---------------------------------------------------------------------------
# Valid minimal pipeline run
# ---------------------------------------------------------------------------

def test_pipeline_minimal_run_succeeds(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _write_phase2_params(project_root)
    _write_phase1_baseline(project_root)
    yaml_path = _write_scenario(project_root, "minimal", 20260502)
    result = run_scenario_pipeline(
        scenario_yaml_path=yaml_path,
        runs_root=project_root / "phase2" / "runs",
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )
    assert result["validation_valid"] is True
    assert Path(result["paths"]["manifest"]).exists()
    assert Path(result["paths"]["quality_report"]).exists()
