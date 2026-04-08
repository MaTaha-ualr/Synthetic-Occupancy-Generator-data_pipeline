from __future__ import annotations

from datetime import date
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.emission import emit_observed_datasets, parse_emission_config


def _truth_people_sample() -> pd.DataFrame:
    return pd.DataFrame(
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
                "DOB": "1990-01-01",
                "Age": 36,
                "AgeBin": "age_35_64",
                "SSN": "111-11-1111",
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
                "DOB": "1991-01-01",
                "Age": 35,
                "AgeBin": "age_35_64",
                "SSN": "222-22-2222",
            },
            {
                "PersonKey": "3",
                "FormalFirstName": "Mia",
                "MiddleName": "",
                "LastName": "Reed",
                "Suffix": "",
                "FormalFullName": "Mia Reed",
                "Gender": "female",
                "Ethnicity": "Black",
                "DOB": "1994-01-01",
                "Age": 32,
                "AgeBin": "age_18_34",
                "SSN": "333-33-3333",
            },
            {
                "PersonKey": "P_CHILD_000001",
                "FormalFirstName": "Child000001",
                "MiddleName": "",
                "LastName": "Stone",
                "Suffix": "",
                "FormalFullName": "Child000001 Stone",
                "Gender": "female",
                "Ethnicity": "White",
                "DOB": "2026-03-01",
                "Age": 0,
                "AgeBin": "age_0_17",
                "SSN": "900-00-1001",
            },
        ]
    )


def _truth_residence_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"PersonKey": "1", "AddressKey": "A1", "ResidenceStartDate": "2026-01-01", "ResidenceEndDate": ""},
            {"PersonKey": "2", "AddressKey": "A2", "ResidenceStartDate": "2026-01-01", "ResidenceEndDate": ""},
            {"PersonKey": "3", "AddressKey": "A3", "ResidenceStartDate": "2026-01-01", "ResidenceEndDate": ""},
            {
                "PersonKey": "P_CHILD_000001",
                "AddressKey": "A1",
                "ResidenceStartDate": "2026-03-01",
                "ResidenceEndDate": "",
            },
        ]
    )


def test_parse_emission_config_defaults() -> None:
    cfg = parse_emission_config({})
    assert cfg.crossfile_match_mode == "one_to_one"
    assert cfg.overlap_entity_pct == 70.0
    assert cfg.appearance_a_pct > 0
    assert cfg.appearance_b_pct > 0
    assert [dataset.dataset_id for dataset in cfg.datasets] == ["A", "B"]


def test_parse_emission_config_supports_single_dataset_schema() -> None:
    cfg = parse_emission_config(
        {
            "crossfile_match_mode": "single_dataset",
            "datasets": [
                {
                    "dataset_id": "registry",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 25.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                }
            ],
        }
    )


def _phase1_address_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "AddressKey": "A1",
                "ResidenceStreetNumber": "101",
                "ResidenceStreetName": "Main St",
                "ResidenceUnitType": "",
                "ResidenceUnitNumber": "",
                "ResidenceCity": "Little Rock",
                "ResidenceState": "AR",
                "ResidencePostalCode": "72201",
            },
            {
                "PersonKey": "2",
                "AddressKey": "A2",
                "ResidenceStreetNumber": "202",
                "ResidenceStreetName": "Oak Ave",
                "ResidenceUnitType": "Apt",
                "ResidenceUnitNumber": "4B",
                "ResidenceCity": "Conway",
                "ResidenceState": "AR",
                "ResidencePostalCode": "72032",
            },
            {
                "PersonKey": "3",
                "AddressKey": "A3",
                "ResidenceStreetNumber": "303",
                "ResidenceStreetName": "Pine Rd",
                "ResidenceUnitType": "",
                "ResidenceUnitNumber": "",
                "ResidenceCity": "Benton",
                "ResidenceState": "AR",
                "ResidencePostalCode": "72015",
            },
        ]
    )
    assert cfg.crossfile_match_mode == "single_dataset"
    assert len(cfg.datasets) == 1
    assert cfg.datasets[0].dataset_id == "registry"
    assert cfg.datasets[0].filename == "observed_registry.csv"


def test_parse_emission_config_rejects_single_dataset_mode_with_two_datasets() -> None:
    with pytest.raises(ValueError, match="exactly one dataset"):
        parse_emission_config(
            {
                "crossfile_match_mode": "single_dataset",
                "datasets": [
                    {"dataset_id": "registry", "snapshot": "simulation_start", "appearance_pct": 100.0, "duplication_pct": 0.0},
                    {"dataset_id": "claims", "snapshot": "simulation_end", "appearance_pct": 100.0, "duplication_pct": 0.0},
                ],
            }
        )


def test_parse_emission_config_rejects_pairwise_mode_with_one_dataset() -> None:
    with pytest.raises(ValueError, match="Single-dataset emission requires crossfile_match_mode=single_dataset"):
        parse_emission_config(
            {
                "crossfile_match_mode": "one_to_one",
                "datasets": [
                    {"dataset_id": "registry", "snapshot": "simulation_end", "appearance_pct": 100.0, "duplication_pct": 0.0}
                ],
            }
        )


def test_parse_emission_config_rejects_duplicate_dataset_ids() -> None:
    with pytest.raises(ValueError, match="Duplicate dataset_id"):
        parse_emission_config(
            {
                "datasets": [
                    {"dataset_id": "registry", "snapshot": "simulation_start", "appearance_pct": 90.0, "duplication_pct": 0.0},
                    {"dataset_id": "registry", "snapshot": "simulation_end", "appearance_pct": 90.0, "duplication_pct": 0.0},
                ],
            }
        )


def test_parse_emission_config_supports_multi_dataset_schema() -> None:
    cfg = parse_emission_config(
        {
            "crossfile_match_mode": "many_to_many",
            "overlap_entity_pct": 50.0,
            "datasets": [
                {"dataset_id": "registry", "snapshot": "simulation_start", "appearance_pct": 100.0, "duplication_pct": 0.0},
                {"dataset_id": "claims", "snapshot": "simulation_end", "appearance_pct": 100.0, "duplication_pct": 20.0},
                {"dataset_id": "benefits", "snapshot": "simulation_end", "appearance_pct": 80.0, "duplication_pct": 10.0},
            ],
        }
    )
    assert cfg.crossfile_match_mode == "many_to_many"
    assert [dataset.dataset_id for dataset in cfg.datasets] == ["registry", "claims", "benefits"]
    assert cfg.datasets[2].filename == "observed_benefits.csv"


def test_parse_emission_config_rejects_invalid_dataset_snapshot() -> None:
    with pytest.raises(ValueError, match="must be one of: simulation_start, simulation_end"):
        parse_emission_config(
            {
                "datasets": [
                    {"dataset_id": "registry", "snapshot": "midstream", "appearance_pct": 100.0, "duplication_pct": 0.0}
                ],
            }
        )


def test_emit_observed_datasets_one_to_many_generates_crosswalk() -> None:
    cfg = parse_emission_config(
        {
            "crossfile_match_mode": "one_to_many",
            "overlap_entity_pct": 100.0,
            "appearance_A_pct": 100.0,
            "appearance_B_pct": 100.0,
            "duplication_in_A_pct": 0.0,
            "duplication_in_B_pct": 40.0,
            "noise": {
                "A": {
                    "name_typo_pct": 0.0,
                    "dob_shift_pct": 0.0,
                    "ssn_mask_pct": 0.0,
                    "address_missing_pct": 0.0,
                    "middle_name_missing_pct": 0.0,
                },
                "B": {
                    "name_typo_pct": 0.0,
                    "dob_shift_pct": 0.0,
                    "ssn_mask_pct": 0.0,
                    "address_missing_pct": 0.0,
                    "middle_name_missing_pct": 0.0,
                },
            },
        }
    )
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people_sample(),
        truth_residence_history_df=_truth_residence_sample(),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260322,
    )

    dataset_a = emitted["datasets"]["A"]
    dataset_b = emitted["datasets"]["B"]
    crosswalk = emitted["truth_crosswalk"]
    entity_record_map = emitted["entity_record_map"]

    assert "RecordKey" in dataset_a.columns
    assert "RecordKey" in dataset_b.columns
    assert "DatasetId" in dataset_a.columns
    assert "DatasetId" in dataset_b.columns
    assert {"PersonKey", "A_RecordKey", "B_RecordKey"} <= set(crosswalk.columns)
    assert {"PersonKey", "DatasetId", "RecordKey"} <= set(entity_record_map.columns)
    assert len(dataset_a) >= 3
    assert len(dataset_b) >= 3
    assert emitted["metrics"]["match_mode"] == "one_to_many"

    linked = crosswalk[(crosswalk["A_RecordKey"] != "") & (crosswalk["B_RecordKey"] != "")]
    if not linked.empty:
        grouped = linked.groupby("PersonKey")["A_RecordKey"].nunique()
        assert grouped.max() <= 1


def test_emit_observed_datasets_single_dataset_mode_omits_pairwise_crosswalk() -> None:
    cfg = parse_emission_config(
        {
            "crossfile_match_mode": "single_dataset",
            "datasets": [
                {
                    "dataset_id": "registry",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 50.0,
                    "noise": {
                        "name_typo_pct": 0.0,
                        "dob_shift_pct": 0.0,
                        "ssn_mask_pct": 0.0,
                        "address_missing_pct": 0.0,
                        "middle_name_missing_pct": 0.0,
                    },
                }
            ],
        }
    )
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people_sample(),
        truth_residence_history_df=_truth_residence_sample(),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260323,
    )

    registry = emitted["datasets"]["registry"]
    entity_record_map = emitted["entity_record_map"]

    assert emitted["truth_crosswalk"] is None
    assert emitted["metrics"]["dataset_count"] == 1
    assert emitted["metrics"]["dataset_ids"] == ["registry"]
    assert "RecordKey" in registry.columns
    assert set(entity_record_map["DatasetId"].astype(str)) == {"registry"}
    assert len(registry) == len(entity_record_map)


def test_emit_observed_datasets_multi_dataset_mode_omits_pairwise_crosswalk() -> None:
    cfg = parse_emission_config(
        {
            "crossfile_match_mode": "many_to_many",
            "overlap_entity_pct": 100.0,
            "datasets": [
                {
                    "dataset_id": "registry",
                    "snapshot": "simulation_start",
                    "appearance_pct": 100.0,
                    "duplication_pct": 0.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
                {
                    "dataset_id": "claims",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 50.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
                {
                    "dataset_id": "benefits",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 25.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
            ],
        }
    )
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people_sample(),
        truth_residence_history_df=_truth_residence_sample(),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260324,
    )

    assert emitted["truth_crosswalk"] is None
    assert emitted["metrics"]["dataset_count"] == 3
    assert emitted["metrics"]["dataset_ids"] == ["registry", "claims", "benefits"]
    assert emitted["metrics"]["coverage"]["all_dataset_overlap_entities"] >= 3
    assert set(emitted["datasets"].keys()) == {"registry", "claims", "benefits"}
    assert set(emitted["entity_record_map"]["DatasetId"].astype(str)) == {"registry", "claims", "benefits"}
    assert "pairwise_overlap" in emitted["metrics"]["coverage"]


def test_emit_observed_datasets_expands_full_address_columns() -> None:
    cfg = parse_emission_config(
        {
            "crossfile_match_mode": "one_to_one",
            "overlap_entity_pct": 100.0,
            "appearance_A_pct": 100.0,
            "appearance_B_pct": 100.0,
            "duplication_in_A_pct": 0.0,
            "duplication_in_B_pct": 0.0,
            "noise": {"A": {"address_missing_pct": 0.0}, "B": {"address_missing_pct": 0.0}},
        }
    )
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people_sample(),
        truth_residence_history_df=_truth_residence_sample(),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260405,
        phase1_df=_phase1_address_sample(),
    )

    row = emitted["datasets"]["A"].iloc[0]
    assert {"HouseNumber", "StreetName", "StreetAddress", "City", "State", "ZipCode"} <= set(emitted["datasets"]["A"].columns)
    assert row["HouseNumber"] == "101"
    assert row["StreetName"] == "Main St"
    assert row["StreetAddress"] == "101 Main St"
    assert row["City"] == "Little Rock"
    assert row["State"] == "AR"
    assert row["ZipCode"] == "72201"


def test_emit_observed_datasets_synthesizes_address_details_for_unknown_keys() -> None:
    cfg = parse_emission_config(
        {
            "crossfile_match_mode": "single_dataset",
            "datasets": [
                {
                    "dataset_id": "registry",
                    "snapshot": "simulation_end",
                    "appearance_pct": 100.0,
                    "duplication_pct": 0.0,
                    "noise": {"address_missing_pct": 0.0},
                }
            ],
        }
    )
    truth_residence = pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "AddressKey": "ADDR_SIM_000001",
                "ResidenceStartDate": "2026-01-01",
                "ResidenceEndDate": "",
            }
        ]
    )
    truth_people = _truth_people_sample().iloc[[0]].copy()

    emitted = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260406,
    )

    row = emitted["datasets"]["registry"].iloc[0]
    assert row["AddressKey"] == "ADDR_SIM_000001"
    assert row["StreetAddress"] != ""
    assert row["City"] != ""
    assert row["State"] != ""
    assert row["ZipCode"] != ""
