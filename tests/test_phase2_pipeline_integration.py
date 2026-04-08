from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal
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

    pd.DataFrame(
        [
            {"age_cohort_id": "age_0_17", "moved_past_year_pct": 10.0, "population": 1000, "source_id": "acs_2024"},
            {"age_cohort_id": "age_18_24", "moved_past_year_pct": 20.0, "population": 800, "source_id": "acs_2024"},
            {"age_cohort_id": "age_25_34", "moved_past_year_pct": 18.0, "population": 900, "source_id": "acs_2024"},
            {"age_cohort_id": "age_35_64", "moved_past_year_pct": 9.0, "population": 1500, "source_id": "acs_2024"},
            {"age_cohort_id": "age_65_plus", "moved_past_year_pct": 6.0, "population": 700, "source_id": "acs_2024"},
        ]
    ).to_csv(params_dir / "mobility_by_age_cohort_acs_2024.csv", index=False)

    pd.DataFrame(
        [
            {"metric_id": "marriage_rate", "value": 6.0, "source_id": "cdc_2023"},
            {"metric_id": "divorce_rate", "value": 2.4, "source_id": "cdc_2023"},
        ]
    ).to_csv(params_dir / "marriage_divorce_rates_cdc_2023.csv", index=False)

    pd.DataFrame(
        [
            {"age_group": "20-24", "birth_rate_per_1000_women": 60.0, "source_id": "nchs_2024"},
            {"age_group": "25-29", "birth_rate_per_1000_women": 80.0, "source_id": "nchs_2024"},
            {"age_group": "30-34", "birth_rate_per_1000_women": 70.0, "source_id": "nchs_2024"},
        ]
    ).to_csv(params_dir / "fertility_by_age_nchs_2024.csv", index=False)

    pd.DataFrame(
        [
            {"household_type_id": "solo_house", "share_of_all_households_pct": 28.0, "source_id": "acs_2024"},
            {"household_type_id": "couple", "share_of_all_households_pct": 45.0, "source_id": "acs_2024"},
            {"household_type_id": "family", "share_of_all_households_pct": 27.0, "source_id": "acs_2024"},
        ]
    ).to_csv(params_dir / "household_type_shares_acs_2024.csv", index=False)

    (params_dir / "phase2_priors_snapshot.json").write_text(
        json.dumps(
            {
                "fertility": {
                    "birth_rate_per_1000_by_age_group": {
                        "20-24": 60.0,
                        "25-29": 80.0,
                        "30-34": 70.0,
                        "35-39": 30.0,
                    }
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    (params_dir / "sources.json").write_text(json.dumps({"sources": []}, indent=2), encoding="utf-8")
    (params_dir / "manifest.json").write_text(json.dumps({"status": "ok"}, indent=2), encoding="utf-8")


def _write_phase1_baseline(project_root: Path, person_count: int) -> None:
    phase1_dir = project_root / "phase1" / "outputs_phase1"
    phase1_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    for idx in range(1, person_count + 1):
        age = 22 + (idx % 40)
        if age <= 34:
            age_bin = "age_18_34"
        elif age <= 64:
            age_bin = "age_35_64"
        else:
            age_bin = "age_65_plus"
        gender = "female" if idx % 2 == 0 else "male"
        rows.append(
            {
                "RecordKey": str(idx),
                "PersonKey": str(idx),
                "EntityRecordIndex": "1",
                "AddressKey": f"A{idx}",
                "FormalFirstName": f"First{idx}",
                "MiddleName": "M" if idx % 3 == 0 else "",
                "LastName": f"Last{idx}",
                "Suffix": "",
                "FormalFullName": f"First{idx} Last{idx}",
                "Gender": gender,
                "Ethnicity": "White" if idx % 4 else "Black",
                "DOB": f"{1980 + (idx % 20)}-01-01",
                "Age": str(age),
                "AgeBin": age_bin,
                "SSN": f"{idx:03d}-{idx % 100:02d}-{idx % 10000:04d}",
                "Phone": f"555-01{idx:04d}",
                "ResidenceType": "HOUSE" if idx % 2 == 0 else "APARTMENT",
                "ResidenceStreetNumber": str(100 + idx),
                "ResidenceStreetName": f"Test St {idx}",
                "ResidenceUnitType": "Apt" if idx % 2 else "",
                "ResidenceUnitNumber": str((idx % 12) + 1) if idx % 2 else "",
                "ResidenceCity": "Little Rock" if idx % 2 == 0 else "Conway",
                "ResidenceState": "AR",
                "ResidencePostalCode": f"72{200 + (idx % 50):03d}",
                "ResidenceStartDate": "2020-01-01",
            }
        )

    pd.DataFrame(rows).to_csv(phase1_dir / "Phase1_people_addresses.csv", index=False)
    (phase1_dir / "Phase1_people_addresses.manifest.json").write_text(
        json.dumps({"row_count": person_count}, indent=2),
        encoding="utf-8",
    )


def _write_scenario_yaml(project_root: Path, scenario_id: str, seed: int, emission: dict[str, object], sample_count: int) -> Path:
    scenarios_dir = project_root / "phase2" / "scenarios"
    scenarios_dir.mkdir(parents=True, exist_ok=True)
    scenario = {
        "scenario_id": scenario_id,
        "seed": seed,
        "phase1": {
            "data_path": "phase1/outputs_phase1/Phase1_people_addresses.csv",
            "manifest_path": "phase1/outputs_phase1/Phase1_people_addresses.manifest.json",
        },
        "selection": {
            "sample": {"mode": "count", "value": sample_count},
            "filters": {
                "age_bins": [],
                "genders": [],
                "ethnicities": [],
                "residence_types": [],
                "redundancy_profiles": [],
                "mobility_propensity_buckets": [],
            },
            "thresholds": {
                "mobility_low_max": 0.09,
                "mobility_high_min": 0.18,
                "trait_low_max": 0.33,
                "trait_high_min": 0.66,
            },
        },
        "simulation": {
            "granularity": "monthly",
            "start_date": "2026-01-01",
            "periods": 6,
        },
        "parameters": {
            "move_rate_pct": 5.0,
            "cohabit_rate_pct": 0.0,
            "birth_rate_pct": 0.0,
            "divorce_rate_pct": 0.0,
            "split_rate_pct": 0.0,
        },
        "emission": emission,
        "quality": {"household_size_range": {"min": 1, "max": 8}},
        "constraints": {
            "min_marriage_age": 18,
            "max_partner_age_gap": 25,
            "fertility_age_range": {"min": 15, "max": 49},
            "allow_underage_marriage": False,
            "allow_child_lives_alone": False,
            "enforce_non_overlapping_residence_intervals": True,
        },
    }
    path = scenarios_dir / f"{scenario_id}.yaml"
    path.write_text(yaml.safe_dump(scenario, sort_keys=False), encoding="utf-8")
    return path


def _build_project(project_root: Path, person_count: int) -> None:
    _write_phase2_params(project_root)
    _write_phase1_baseline(project_root, person_count)


def _read_run_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=str).fillna("")


def test_run_scenario_pipeline_single_dataset_end_to_end(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    runs_root = project_root / "phase2" / "runs"
    _build_project(project_root, person_count=40)
    scenario_yaml = _write_scenario_yaml(
        project_root,
        "registry_dedup",
        20260410,
        emission={
            "crossfile_match_mode": "single_dataset",
            "datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 10.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                }
            ],
        },
        sample_count=20,
    )

    result = run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml,
        runs_root=runs_root,
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )

    assert result["validation_valid"] is True
    assert result["observed_counts"]["crosswalk_rows"] == 0
    assert result["paths"]["truth_crosswalk"] == ""
    assert Path(result["paths"]["entity_record_map"]).exists()
    registry = _read_run_csv(result["paths"]["datasets"]["registry"])
    assert {"HouseNumber", "StreetName", "StreetAddress", "City", "State", "ZipCode"} <= set(registry.columns)
    assert registry["StreetAddress"].str.strip().ne("").all()
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    assert quality["phase2_quality"]["er_benchmark_metrics"]["topology"]["dataset_count"] == 1
    assert quality["phase2_quality"]["er_benchmark_metrics"]["topology"]["relationship_mode"] == "single_dataset"


def test_run_scenario_pipeline_pairwise_dataset_list_end_to_end(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    runs_root = project_root / "phase2" / "runs"
    _build_project(project_root, person_count=50)
    scenario_yaml = _write_scenario_yaml(
        project_root,
        "registry_claims",
        20260411,
        emission={
            "crossfile_match_mode": "one_to_many",
            "overlap_entity_pct": 100.0,
            "datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_start",
                    "appearance_pct": 100.0,
                    "duplication_pct": 0.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
                {
                    "dataset_id": "claims",
                    "filename": "observed_claims.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 50.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
            ],
        },
        sample_count=20,
    )

    result = run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml,
        runs_root=runs_root,
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )

    assert result["validation_valid"] is True
    assert result["observed_counts"]["crosswalk_rows"] > 0
    assert Path(result["paths"]["truth_crosswalk"]).exists()
    registry = _read_run_csv(result["paths"]["datasets"]["registry"])
    claims = _read_run_csv(result["paths"]["datasets"]["claims"])
    assert {"HouseNumber", "StreetName", "StreetAddress", "City", "State", "ZipCode"} <= set(registry.columns)
    assert {"HouseNumber", "StreetName", "StreetAddress", "City", "State", "ZipCode"} <= set(claims.columns)
    manifest = json.loads(Path(result["paths"]["manifest"]).read_text(encoding="utf-8"))
    assert [item["dataset_id"] for item in manifest["observed_outputs"]["datasets"]] == ["registry", "claims"]
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["relationship_mode"] == "one_to_many"
    assert er["cross_file_overlap"]["dataset_ids"] == ["registry", "claims"]
    assert er["match_cardinality_achieved"]["one_to_many"] >= 1


def test_run_scenario_pipeline_multi_dataset_end_to_end(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    runs_root = project_root / "phase2" / "runs"
    _build_project(project_root, person_count=60)
    scenario_yaml = _write_scenario_yaml(
        project_root,
        "registry_claims_benefits",
        20260414,
        emission={
            "crossfile_match_mode": "many_to_many",
            "overlap_entity_pct": 80.0,
            "datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_start",
                    "appearance_pct": 100.0,
                    "duplication_pct": 0.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
                {
                    "dataset_id": "claims",
                    "filename": "observed_claims.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 50.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
                {
                    "dataset_id": "benefits",
                    "filename": "observed_benefits.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 85.0,
                    "duplication_pct": 25.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
            ],
        },
        sample_count=24,
    )

    result = run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml,
        runs_root=runs_root,
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )

    assert result["validation_valid"] is True
    assert result["observed_counts"]["crosswalk_rows"] == 0
    assert result["paths"]["truth_crosswalk"] == ""
    assert set(result["paths"]["pairwise_crosswalks"].keys()) == {
        "registry__claims",
        "registry__benefits",
        "claims__benefits",
    }
    manifest = json.loads(Path(result["paths"]["manifest"]).read_text(encoding="utf-8"))
    assert [item["dataset_id"] for item in manifest["observed_outputs"]["datasets"]] == ["registry", "claims", "benefits"]
    assert len(manifest["observed_outputs"]["pairwise_crosswalks"]) == 3
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["dataset_count"] == 3
    assert er["topology"]["relationship_mode"] == "many_to_many"
    assert "multi_dataset_overlap" in er
    assert "pairwise_match_cardinality" in er


def test_run_scenario_pipeline_is_reproducible_for_same_seed(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    _build_project(project_root, person_count=40)
    scenario_yaml = _write_scenario_yaml(
        project_root,
        "registry_repro",
        20260412,
        emission={
            "crossfile_match_mode": "single_dataset",
            "datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 80.0,
                    "duplication_pct": 10.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                }
            ],
        },
        sample_count=20,
    )

    result_a = run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml,
        runs_root=project_root / "runs_a",
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )
    result_b = run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml,
        runs_root=project_root / "runs_b",
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )

    assert_frame_equal(
        _read_run_csv(result_a["paths"]["entity_record_map"]),
        _read_run_csv(result_b["paths"]["entity_record_map"]),
    )
    assert_frame_equal(
        _read_run_csv(result_a["paths"]["datasets"]["registry"]),
        _read_run_csv(result_b["paths"]["datasets"]["registry"]),
    )
    pd.testing.assert_frame_equal(
        pd.read_parquet(Path(result_a["run_dir"]) / "truth_events.parquet"),
        pd.read_parquet(Path(result_b["run_dir"]) / "truth_events.parquet"),
    )


def test_run_scenario_pipeline_medium_scale_smoke(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    runs_root = project_root / "phase2" / "runs"
    _build_project(project_root, person_count=300)
    scenario_yaml = _write_scenario_yaml(
        project_root,
        "registry_scale",
        20260413,
        emission={
            "crossfile_match_mode": "single_dataset",
            "datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 10.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                }
            ],
        },
        sample_count=200,
    )

    result = run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml,
        runs_root=runs_root,
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )

    assert result["validation_valid"] is True
    assert result["truth_counts"]["truth_people"] == 200
    assert result["observed_counts"]["datasets"]["registry"] == 220
    assert result["observed_counts"]["entity_record_map_rows"] == 220


def test_run_scenario_pipeline_multi_dataset_scale_smoke(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    runs_root = project_root / "phase2" / "runs"
    _build_project(project_root, person_count=500)
    scenario_yaml = _write_scenario_yaml(
        project_root,
        "multi_dataset_scale",
        20260415,
        emission={
            "crossfile_match_mode": "many_to_many",
            "overlap_entity_pct": 70.0,
            "datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_start",
                    "appearance_pct": 100.0,
                    "duplication_pct": 0.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
                {
                    "dataset_id": "claims",
                    "filename": "observed_claims.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 95.0,
                    "duplication_pct": 25.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
                {
                    "dataset_id": "benefits",
                    "filename": "observed_benefits.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 90.0,
                    "duplication_pct": 15.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
            ],
        },
        sample_count=300,
    )

    result = run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml,
        runs_root=runs_root,
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )

    assert result["validation_valid"] is True
    assert result["truth_counts"]["truth_people"] == 300
    assert result["observed_counts"]["entity_record_map_rows"] > 300
    assert result["observed_counts"]["datasets"]["claims"] > result["observed_counts"]["datasets"]["benefits"]
    assert len(result["observed_counts"]["pairwise_crosswalk_rows"]) == 3
