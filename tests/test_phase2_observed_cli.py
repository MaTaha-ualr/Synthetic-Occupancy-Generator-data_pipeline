from __future__ import annotations

import json
import runpy
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.output_contract import parse_run_id, validate_phase2_run


def _write_minimal_truth_run(
    runs_root: Path,
    run_id: str,
    *,
    emission: dict[str, object] | None = None,
) -> Path:
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    run_info = parse_run_id(run_id)
    scenario_id = run_info["scenario_id"]
    seed = run_info["seed"]

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "FormalFirstName": "Ava",
                "MiddleName": "M",
                "LastName": "Stone",
                "Suffix": "",
                "FormalFullName": "Ava M Stone",
                "Gender": "female",
                "Ethnicity": "White",
                "DOB": "1992-01-01",
                "Age": 34,
                "AgeBin": "age_18_34",
                "SSN": "111-11-1111",
                "Phone": "111-222-3333",
            },
            {
                "PersonKey": "2",
                "FormalFirstName": "Liam",
                "MiddleName": "",
                "LastName": "Parker",
                "Suffix": "",
                "FormalFullName": "Liam Parker",
                "Gender": "male",
                "Ethnicity": "White",
                "DOB": "1990-01-01",
                "Age": 36,
                "AgeBin": "age_35_64",
                "SSN": "222-22-2222",
                "Phone": "444-555-6666",
            },
        ]
    ).to_parquet(run_dir / "truth_people.parquet", index=False)

    pd.DataFrame(
        [
            {
                "HouseholdKey": "H1",
                "HouseholdType": "solo_house",
                "HouseholdStartDate": "2026-01-01",
                "HouseholdEndDate": "",
            },
            {
                "HouseholdKey": "H2",
                "HouseholdType": "solo_house",
                "HouseholdStartDate": "2026-01-01",
                "HouseholdEndDate": "",
            },
        ]
    ).to_parquet(run_dir / "truth_households.parquet", index=False)

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "HouseholdKey": "H1",
                "HouseholdRole": "HEAD",
                "MembershipStartDate": "2026-01-01",
                "MembershipEndDate": "",
            },
            {
                "PersonKey": "2",
                "HouseholdKey": "H2",
                "HouseholdRole": "HEAD",
                "MembershipStartDate": "2026-01-01",
                "MembershipEndDate": "",
            },
        ]
    ).to_parquet(run_dir / "truth_household_memberships.parquet", index=False)

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "AddressKey": "A1",
                "ResidenceStartDate": "2026-01-01",
                "ResidenceEndDate": "",
            },
            {
                "PersonKey": "2",
                "AddressKey": "A2",
                "ResidenceStartDate": "2026-01-01",
                "ResidenceEndDate": "",
            },
        ]
    ).to_parquet(run_dir / "truth_residence_history.parquet", index=False)

    pd.DataFrame(
        [
            {
                "EventKey": "E1",
                "EventType": "MOVE",
                "EventDate": "2026-02-01",
                "SubjectPersonKey": "1",
                "SubjectHouseholdKey": "H1",
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
    ).to_parquet(run_dir / "truth_events.parquet", index=False)

    pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "ScenarioId": scenario_id,
                "SelectionSeed": seed,
                "AgeBin": "age_18_34",
                "Gender": "female",
                "Ethnicity": "White",
                "ResidenceType": "HOUSE",
                "RecordsPerEntity": 1,
                "RedundancyProfile": "single_record",
                "MobilityPropensityScore": 0.12,
                "MobilityPropensityBucket": "medium",
                "PartnershipPropensityScore": 0.40,
                "PartnershipPropensityBucket": "medium",
                "FertilityPropensityScore": 0.25,
                "FertilityPropensityBucket": "low",
            },
            {
                "PersonKey": "2",
                "ScenarioId": scenario_id,
                "SelectionSeed": seed,
                "AgeBin": "age_35_64",
                "Gender": "male",
                "Ethnicity": "White",
                "ResidenceType": "HOUSE",
                "RecordsPerEntity": 1,
                "RedundancyProfile": "single_record",
                "MobilityPropensityScore": 0.08,
                "MobilityPropensityBucket": "low",
                "PartnershipPropensityScore": 0.35,
                "PartnershipPropensityBucket": "medium",
                "FertilityPropensityScore": 0.20,
                "FertilityPropensityBucket": "low",
            },
        ]
    ).to_parquet(run_dir / "scenario_population.parquet", index=False)

    emission_payload = emission or {
        "crossfile_match_mode": "single_dataset",
        "datasets": [
            {
                "dataset_id": "registry",
                "filename": "observed_registry.csv",
                "snapshot": "simulation_end",
                "appearance_pct": 100.0,
                "duplication_pct": 25.0,
                "noise": {
                    "name_typo_pct": 0.0,
                    "dob_shift_pct": 0.0,
                    "ssn_mask_pct": 0.0,
                    "phone_mask_pct": 0.0,
                    "address_missing_pct": 0.0,
                    "middle_name_missing_pct": 0.0,
                    "phonetic_error_pct": 0.0,
                    "ocr_error_pct": 0.0,
                    "date_swap_pct": 0.0,
                    "zip_digit_error_pct": 0.0,
                    "nickname_pct": 0.0,
                    "suffix_missing_pct": 0.0,
                },
            }
        ],
    }
    scenario_payload = {
        "scenario_id": scenario_id,
        "seed": seed,
        "phase1": {
            "data_path": "phase1/outputs_phase1/Phase1_people_addresses.csv",
            "manifest_path": "phase1/outputs_phase1/Phase1_people_addresses.manifest.json",
        },
        "selection": {
            "sample": {"mode": "count", "value": 2},
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
            "periods": 12,
        },
        "emission": emission_payload,
        "quality": {"household_size_range": {"min": 1, "max": 8}},
        "constraints": {
            "min_marriage_age": 18,
            "fertility_age_range": {"min": 15, "max": 49},
            "allow_underage_marriage": False,
            "allow_child_lives_alone": False,
            "enforce_non_overlapping_residence_intervals": True,
        },
    }
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump(scenario_payload, sort_keys=False),
        encoding="utf-8",
    )

    selection_log = {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "selection_seed": seed,
        "counts": {"selected_entities": 2},
        "selected_personkey_sha256": "abc123",
    }
    (run_dir / "scenario_selection_log.json").write_text(
        json.dumps(selection_log, indent=2),
        encoding="utf-8",
    )
    return run_dir


def test_generate_phase2_observed_cli_supports_single_dataset_mode(monkeypatch, tmp_path):
    run_id = "2026-04-05_registry_dedup_seed20260405"
    runs_root = tmp_path / "runs"
    run_dir = _write_minimal_truth_run(runs_root, run_id)
    script_path = PROJECT_ROOT / "scripts" / "generate_phase2_observed.py"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script_path),
            "--run-id",
            run_id,
            "--runs-root",
            str(runs_root),
            "--overwrite",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(script_path), run_name="__main__")
    assert exc.value.code == 0

    observed_path = run_dir / "observed_registry.csv"
    entity_record_map_path = run_dir / "entity_record_map.csv"
    crosswalk_path = run_dir / "truth_crosswalk.csv"
    manifest_path = run_dir / "manifest.json"
    quality_path = run_dir / "quality_report.json"

    assert observed_path.exists()
    assert entity_record_map_path.exists()
    assert not crosswalk_path.exists()
    assert manifest_path.exists()
    assert quality_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["observed_outputs"]["datasets"][0]["dataset_id"] == "registry"
    assert manifest["observed_outputs"]["truth_crosswalk"] == ""

    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    topology = quality["phase2_quality"]["er_benchmark_metrics"]["topology"]
    assert topology["dataset_count"] == 1
    assert topology["dataset_ids"] == ["registry"]
    assert topology["relationship_mode"] == "single_dataset"

    observed_df = pd.read_csv(observed_path, dtype=str).fillna("")
    assert {"HouseNumber", "StreetName", "StreetAddress", "City", "State", "ZipCode"} <= set(observed_df.columns)
    assert observed_df.loc[0, "StreetAddress"] != ""

    validation = validate_phase2_run(runs_root, run_id)
    assert validation["valid"] is True


def test_generate_phase2_observed_cli_supports_pairwise_dataset_list_mode(monkeypatch, tmp_path):
    run_id = "2026-04-05_registry_claims_seed20260406"
    runs_root = tmp_path / "runs"
    run_dir = _write_minimal_truth_run(
        runs_root,
        run_id,
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
                    "noise": {
                        "name_typo_pct": 0.0,
                        "dob_shift_pct": 0.0,
                        "ssn_mask_pct": 0.0,
                        "phone_mask_pct": 0.0,
                        "address_missing_pct": 0.0,
                        "middle_name_missing_pct": 0.0,
                        "phonetic_error_pct": 0.0,
                        "ocr_error_pct": 0.0,
                        "date_swap_pct": 0.0,
                        "zip_digit_error_pct": 0.0,
                        "nickname_pct": 0.0,
                        "suffix_missing_pct": 0.0,
                    },
                },
                {
                    "dataset_id": "claims",
                    "filename": "observed_claims.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 50.0,
                    "noise": {
                        "name_typo_pct": 0.0,
                        "dob_shift_pct": 0.0,
                        "ssn_mask_pct": 0.0,
                        "phone_mask_pct": 0.0,
                        "address_missing_pct": 0.0,
                        "middle_name_missing_pct": 0.0,
                        "phonetic_error_pct": 0.0,
                        "ocr_error_pct": 0.0,
                        "date_swap_pct": 0.0,
                        "zip_digit_error_pct": 0.0,
                        "nickname_pct": 0.0,
                        "suffix_missing_pct": 0.0,
                    },
                },
            ],
        },
    )
    script_path = PROJECT_ROOT / "scripts" / "generate_phase2_observed.py"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script_path),
            "--run-id",
            run_id,
            "--runs-root",
            str(runs_root),
            "--overwrite",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(script_path), run_name="__main__")
    assert exc.value.code == 0

    registry_path = run_dir / "observed_registry.csv"
    claims_path = run_dir / "observed_claims.csv"
    crosswalk_path = run_dir / "truth_crosswalk.csv"
    manifest_path = run_dir / "manifest.json"
    quality_path = run_dir / "quality_report.json"

    assert registry_path.exists()
    assert claims_path.exists()
    assert crosswalk_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    dataset_ids = [item["dataset_id"] for item in manifest["observed_outputs"]["datasets"]]
    assert dataset_ids == ["registry", "claims"]
    assert manifest["observed_outputs"]["truth_crosswalk"] == str(crosswalk_path)

    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    topology = quality["phase2_quality"]["er_benchmark_metrics"]["topology"]
    assert topology["dataset_count"] == 2
    assert topology["dataset_ids"] == ["registry", "claims"]
    assert topology["relationship_mode"] == "one_to_many"
    assert quality["observed_quality"]["coverage"]["relationship_mode"] == "one_to_many"
    assert quality["phase2_quality"]["er_benchmark_metrics"]["match_cardinality_achieved"]["one_to_many"] >= 1

    registry_df = pd.read_csv(registry_path, dtype=str).fillna("")
    assert {"HouseNumber", "StreetName", "StreetAddress", "City", "State", "ZipCode"} <= set(registry_df.columns)

    validation = validate_phase2_run(runs_root, run_id)
    assert validation["valid"] is True


def test_generate_phase2_observed_cli_supports_multi_dataset_mode(monkeypatch, tmp_path):
    run_id = "2026-04-05_registry_claims_benefits_seed20260408"
    runs_root = tmp_path / "runs"
    run_dir = _write_minimal_truth_run(
        runs_root,
        run_id,
        emission={
            "crossfile_match_mode": "many_to_many",
            "overlap_entity_pct": 100.0,
            "datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_start",
                    "appearance_pct": 100.0,
                    "duplication_pct": 0.0,
                },
                {
                    "dataset_id": "claims",
                    "filename": "observed_claims.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 50.0,
                },
                {
                    "dataset_id": "benefits",
                    "filename": "observed_benefits.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 25.0,
                },
            ],
        },
    )
    script_path = PROJECT_ROOT / "scripts" / "generate_phase2_observed.py"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script_path),
            "--run-id",
            run_id,
            "--runs-root",
            str(runs_root),
            "--overwrite",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(script_path), run_name="__main__")
    assert exc.value.code == 0

    assert (run_dir / "observed_registry.csv").exists()
    assert (run_dir / "observed_claims.csv").exists()
    assert (run_dir / "observed_benefits.csv").exists()
    assert (run_dir / "entity_record_map.csv").exists()
    assert not (run_dir / "truth_crosswalk.csv").exists()
    assert (run_dir / "truth_crosswalk__registry__claims.csv").exists()
    assert (run_dir / "truth_crosswalk__registry__benefits.csv").exists()
    assert (run_dir / "truth_crosswalk__claims__benefits.csv").exists()

    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    assert [item["dataset_id"] for item in manifest["observed_outputs"]["datasets"]] == ["registry", "claims", "benefits"]
    assert manifest["observed_outputs"]["truth_crosswalk"] == ""
    assert len(manifest["observed_outputs"]["pairwise_crosswalks"]) == 3

    quality = json.loads((run_dir / "quality_report.json").read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["dataset_count"] == 3
    assert er["topology"]["dataset_ids"] == ["registry", "claims", "benefits"]
    assert er["topology"]["relationship_mode"] == "many_to_many"
    assert "multi_dataset_overlap" in er
    assert "pairwise_match_cardinality" in er

    validation = validate_phase2_run(runs_root, run_id)
    assert validation["valid"] is True


def test_validate_phase2_run_requires_crosswalk_for_pairwise_dataset_list(monkeypatch, tmp_path):
    run_id = "2026-04-05_registry_claims_seed20260407"
    runs_root = tmp_path / "runs"
    run_dir = _write_minimal_truth_run(
        runs_root,
        run_id,
        emission={
            "crossfile_match_mode": "one_to_one",
            "overlap_entity_pct": 100.0,
            "datasets": [
                {
                    "dataset_id": "registry",
                    "filename": "observed_registry.csv",
                    "snapshot": "simulation_start",
                    "appearance_pct": 100.0,
                    "duplication_pct": 0.0,
                },
                {
                    "dataset_id": "claims",
                    "filename": "observed_claims.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 0.0,
                },
            ],
        },
    )
    script_path = PROJECT_ROOT / "scripts" / "generate_phase2_observed.py"

    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(script_path),
            "--run-id",
            run_id,
            "--runs-root",
            str(runs_root),
            "--overwrite",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        runpy.run_path(str(script_path), run_name="__main__")
    assert exc.value.code == 0

    crosswalk_path = run_dir / "truth_crosswalk.csv"
    assert crosswalk_path.exists()
    crosswalk_path.unlink()

    validation = validate_phase2_run(runs_root, run_id)
    assert validation["valid"] is False
    assert "truth_crosswalk" in validation["missing_files"]
