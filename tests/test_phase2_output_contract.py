from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.output_contract import (
    build_run_id,
    expected_phase2_run_artifact_paths,
    parse_run_id,
    validate_phase2_run,
)


def _write_minimal_valid_run_artifacts(runs_root: Path, run_id: str) -> dict[str, Path]:
    paths = expected_phase2_run_artifact_paths(runs_root, run_id)
    for path in paths.values():
        path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "FormalFirstName": "Ava",
                "MiddleName": "",
                "LastName": "Smith",
                "Suffix": "",
                "FormalFullName": "Ava Smith",
                "Gender": "female",
                "Ethnicity": "White",
                "DOB": "1990-01-01",
                "Age": "36",
                "AgeBin": "age_35_64",
                "SSN": "111-22-3333",
            }
        ]
    ).to_parquet(paths["truth_people"], index=False)

    pd.DataFrame(
        [
            {
                "HouseholdKey": "H1",
                "HouseholdType": "FAMILY",
                "HouseholdStartDate": "2024-01-01",
                "HouseholdEndDate": "",
            }
        ]
    ).to_parquet(paths["truth_households"], index=False)

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "HouseholdKey": "H1",
                "HouseholdRole": "HEAD",
                "MembershipStartDate": "2024-01-01",
                "MembershipEndDate": "",
            }
        ]
    ).to_parquet(paths["truth_household_memberships"], index=False)

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "AddressKey": "A1",
                "ResidenceStartDate": "2024-01-01",
                "ResidenceEndDate": "",
            }
        ]
    ).to_parquet(paths["truth_residence_history"], index=False)

    pd.DataFrame(
        [
            {
                "EventKey": "E1",
                "EventType": "MOVE",
                "EventDate": "2024-01-01",
                "SubjectPersonKey": "1",
                "SubjectHouseholdKey": "",
                "FromAddressKey": "A0",
                "ToAddressKey": "A1",
                "PersonKeyA": "",
                "PersonKeyB": "",
                "NewHouseholdKey": "",
                "CohabitMode": "",
                "ChildPersonKey": "",
                "Parent1PersonKey": "",
                "Parent2PersonKey": "",
                "CustodyMode": "",
            }
        ]
    ).to_parquet(paths["truth_events"], index=False)

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "ScenarioId": "placeholder",
                "SelectionSeed": 0,
                "AgeBin": "age_35_64",
                "Gender": "female",
                "Ethnicity": "White",
                "ResidenceType": "HOUSE",
                "RecordsPerEntity": 1,
                "RedundancyProfile": "single_record",
                "MobilityPropensityScore": 0.12,
                "MobilityPropensityBucket": "medium",
                "PartnershipPropensityScore": 0.60,
                "PartnershipPropensityBucket": "medium",
                "FertilityPropensityScore": 0.30,
                "FertilityPropensityBucket": "low",
            }
        ]
    ).to_parquet(paths["scenario_population"], index=False)

    pd.DataFrame([{"A_RecordKey": "A-1"}]).to_csv(paths["dataset_a"], index=False)
    pd.DataFrame([{"B_RecordKey": "B-1"}]).to_csv(paths["dataset_b"], index=False)
    pd.DataFrame(
        [{"PersonKey": "1", "A_RecordKey": "A-1", "B_RecordKey": "B-1"}]
    ).to_csv(paths["truth_crosswalk"], index=False)

    run_info = parse_run_id(run_id)
    scenario_id = run_info["scenario_id"]
    seed = run_info["seed"]

    scenario = {
        "scenario_id": scenario_id,
        "seed": seed,
        "phase1": {
            "data_path": "outputs_phase1/Phase1_people_addresses.csv",
            "manifest_path": "outputs_phase1/Phase1_people_addresses.manifest.json",
        },
        "selection": {
            "sample": {"mode": "pct", "value": 100.0},
            "filters": {},
            "thresholds": {},
        },
        "emission": {
            "crossfile_match_mode": "one_to_one",
            "overlap_entity_pct": 70.0,
            "appearance_A_pct": 85.0,
            "appearance_B_pct": 90.0,
            "duplication_in_A_pct": 5.0,
            "duplication_in_B_pct": 8.0,
        },
        "constraints": {
            "min_marriage_age": 18,
            "max_partner_age_gap": 25,
            "fertility_age_range": {"min": 15, "max": 49},
            "allow_underage_marriage": False,
            "allow_child_lives_alone": False,
            "enforce_non_overlapping_residence_intervals": True,
        },
    }
    paths["scenario_yaml"].write_text(yaml.safe_dump(scenario, sort_keys=False), encoding="utf-8")

    manifest = {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "seed": seed,
        "phase1_input_csv": "outputs_phase1/Phase1_people_addresses.csv",
        "phase1_input_manifest": "outputs_phase1/Phase1_people_addresses.manifest.json",
    }
    paths["manifest_json"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    quality_report = {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "seed": seed,
        "status": "ok",
    }
    paths["quality_report_json"].write_text(json.dumps(quality_report, indent=2), encoding="utf-8")

    selection_log = {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "selection_seed": seed,
        "counts": {
            "entity_total": 1,
            "candidate_entities": 1,
            "selected_entities": 1,
        },
        "selected_personkey_sha256": "abc123",
    }
    paths["scenario_selection_log_json"].write_text(
        json.dumps(selection_log, indent=2),
        encoding="utf-8",
    )

    # Keep scenario population row aligned with scenario/seed for selection audit checks.
    pop_df = pd.read_parquet(paths["scenario_population"])
    pop_df["ScenarioId"] = scenario_id
    pop_df["SelectionSeed"] = seed
    pop_df.to_parquet(paths["scenario_population"], index=False)
    return paths


def test_phase2_output_contract_valid_minimal_run(tmp_path: Path) -> None:
    runs_root = tmp_path / "phase2" / "runs"
    run_id = build_run_id("single_movers", 20260310, "2026-03-10")
    _write_minimal_valid_run_artifacts(runs_root, run_id)

    result = validate_phase2_run(runs_root=runs_root, run_id=run_id)
    assert result["valid"] is True
    assert result["missing_files"] == []
    assert result["schema_errors"] == {}
    assert result["metadata_errors"] == {}


def test_phase2_output_contract_crosswalk_accepts_entity_key_alias(tmp_path: Path) -> None:
    runs_root = tmp_path / "phase2" / "runs"
    run_id = build_run_id("roommates_split", 20260311, "2026-03-11")
    paths = _write_minimal_valid_run_artifacts(runs_root, run_id)

    pd.DataFrame(
        [{"EntityKey": "1", "A_RecordKey": "A-1", "B_RecordKey": "B-1"}]
    ).to_csv(paths["truth_crosswalk"], index=False)

    result = validate_phase2_run(runs_root=runs_root, run_id=run_id)
    assert result["valid"] is True
    assert result["schema_errors"] == {}


def test_phase2_output_contract_detects_missing_required_columns(tmp_path: Path) -> None:
    runs_root = tmp_path / "phase2" / "runs"
    run_id = build_run_id("family_birth", 20260312, "2026-03-12")
    paths = _write_minimal_valid_run_artifacts(runs_root, run_id)

    pd.DataFrame(
        [{"EventKey": "E1", "EventType": "MOVE", "SubjectPersonKey": "1"}]
    ).to_parquet(paths["truth_events"], index=False)

    result = validate_phase2_run(runs_root=runs_root, run_id=run_id)
    assert result["valid"] is False
    assert "truth_events" in result["schema_errors"]
    assert "EventDate" in result["schema_errors"]["truth_events"]["missing_required_columns"]


def test_phase2_output_contract_detects_seed_mismatch(tmp_path: Path) -> None:
    runs_root = tmp_path / "phase2" / "runs"
    run_id = build_run_id("divorce_custody", 20260313, "2026-03-13")
    paths = _write_minimal_valid_run_artifacts(runs_root, run_id)

    scenario = yaml.safe_load(paths["scenario_yaml"].read_text(encoding="utf-8"))
    scenario["seed"] = 7
    paths["scenario_yaml"].write_text(yaml.safe_dump(scenario, sort_keys=False), encoding="utf-8")

    result = validate_phase2_run(runs_root=runs_root, run_id=run_id)
    assert result["valid"] is False
    assert "reproducibility" in result["metadata_errors"]


def test_phase2_output_contract_detects_invalid_event_grammar(tmp_path: Path) -> None:
    runs_root = tmp_path / "phase2" / "runs"
    run_id = build_run_id("couple_merge", 20260315, "2026-03-15")
    paths = _write_minimal_valid_run_artifacts(runs_root, run_id)

    pd.DataFrame(
        [
            {
                "EventKey": "E2",
                "EventType": "COHABIT",
                "EventDate": "2024-06-10",
                "SubjectPersonKey": "",
                "SubjectHouseholdKey": "",
                "FromAddressKey": "",
                "ToAddressKey": "",
                "PersonKeyA": "1",
                "PersonKeyB": "2",
                "NewHouseholdKey": "H2",
                "CohabitMode": "bad_mode",
                "ChildPersonKey": "",
                "Parent1PersonKey": "",
                "Parent2PersonKey": "",
                "CustodyMode": "",
            }
        ]
    ).to_parquet(paths["truth_events"], index=False)

    result = validate_phase2_run(runs_root=runs_root, run_id=run_id)
    assert result["valid"] is False
    assert "truth_events" in result["schema_errors"]
    assert result["schema_errors"]["truth_events"]["grammar_error_count"] > 0


def test_phase2_output_contract_detects_constraints_violations(tmp_path: Path) -> None:
    runs_root = tmp_path / "phase2" / "runs"
    run_id = build_run_id("edge_case_constraints", 20260316, "2026-03-16")
    paths = _write_minimal_valid_run_artifacts(runs_root, run_id)

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "FormalFirstName": "Alex",
                "MiddleName": "",
                "LastName": "Doe",
                "Suffix": "",
                "FormalFullName": "Alex Doe",
                "Gender": "male",
                "Ethnicity": "White",
                "DOB": "2010-01-01",
                "Age": "16",
                "AgeBin": "age_0_17",
                "SSN": "111-11-1111",
            },
                {
                    "PersonKey": "2",
                    "FormalFirstName": "Jordan",
                    "MiddleName": "",
                    "LastName": "Doe",
                    "Suffix": "",
                    "FormalFullName": "Jordan Doe",
                    "Gender": "female",
                    "Ethnicity": "White",
                    "DOB": "1971-01-01",
                    "Age": "55",
                    "AgeBin": "age_35_64",
                    "SSN": "222-22-2222",
                },
            ]
        ).to_parquet(paths["truth_people"], index=False)

    pd.DataFrame(
        [
            {
                "EventKey": "E99",
                "EventType": "COHABIT",
                "EventDate": "2024-08-01",
                "SubjectPersonKey": "",
                "SubjectHouseholdKey": "",
                "FromAddressKey": "",
                "ToAddressKey": "",
                "PersonKeyA": "1",
                "PersonKeyB": "2",
                "NewHouseholdKey": "H2",
                "CohabitMode": "new_address",
                "ChildPersonKey": "",
                "Parent1PersonKey": "",
                "Parent2PersonKey": "",
                "CustodyMode": "",
            }
        ]
    ).to_parquet(paths["truth_events"], index=False)

    result = validate_phase2_run(runs_root=runs_root, run_id=run_id)
    assert result["valid"] is False
    assert "constraints" in result["metadata_errors"]
    assert result["constraints_validation"] is not None
    assert result["constraints_validation"]["violation_count"] >= 2
