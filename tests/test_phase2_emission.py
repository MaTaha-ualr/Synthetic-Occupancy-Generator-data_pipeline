from __future__ import annotations

from datetime import date
import sys
from pathlib import Path

import pandas as pd

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

    dataset_a = emitted["dataset_a"]
    dataset_b = emitted["dataset_b"]
    crosswalk = emitted["truth_crosswalk"]

    assert "A_RecordKey" in dataset_a.columns
    assert "B_RecordKey" in dataset_b.columns
    assert {"PersonKey", "A_RecordKey", "B_RecordKey"} <= set(crosswalk.columns)
    assert len(dataset_a) >= 3
    assert len(dataset_b) >= 3
    assert emitted["metrics"]["match_mode"] == "one_to_many"

    linked = crosswalk[(crosswalk["A_RecordKey"] != "") & (crosswalk["B_RecordKey"] != "")]
    if not linked.empty:
        grouped = linked.groupby("PersonKey")["A_RecordKey"].nunique()
        assert grouped.max() <= 1
