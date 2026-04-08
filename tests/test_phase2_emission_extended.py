"""Extended emission tests — noise injection, field masking, duplication math."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.emission import (
    DatasetNoiseConfig,
    EmissionConfig,
    emit_observed_datasets,
    parse_emission_config,
)


def _truth_people(n: int = 20) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "PersonKey": str(i),
            "FormalFirstName": f"First{i}",
            "MiddleName": "M" if i % 3 == 0 else "",
            "LastName": f"Last{i}",
            "Suffix": "Jr." if i % 7 == 0 else "",
            "FormalFullName": f"First{i} Last{i}",
            "Gender": "female" if i % 2 == 0 else "male",
            "Ethnicity": "White",
            "DOB": f"199{i % 10}-0{(i % 9) + 1}-15",
            "Age": 30 + (i % 10),
            "AgeBin": "age_35_64" if i % 2 == 0 else "age_18_34",
            "SSN": f"{i:03d}-{i:02d}-{i:04d}",
            "Phone": f"555-{i:03d}-{i:04d}",
        })
    return pd.DataFrame(rows)


def _truth_residence(n: int = 20) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "PersonKey": str(i),
            "AddressKey": f"ADDR{i}",
            "ResidenceStartDate": "2026-01-01",
            "ResidenceEndDate": "",
        })
    return pd.DataFrame(rows)


def _zero_noise() -> dict:
    return {
        "name_typo_pct": 0.0, "dob_shift_pct": 0.0, "ssn_mask_pct": 0.0,
        "phone_mask_pct": 0.0, "address_missing_pct": 0.0, "middle_name_missing_pct": 0.0,
        "phonetic_error_pct": 0.0, "ocr_error_pct": 0.0, "date_swap_pct": 0.0,
        "zip_digit_error_pct": 0.0, "nickname_pct": 0.0, "suffix_missing_pct": 0.0,
    }


# ---------------------------------------------------------------------------
# Noise injection validation — high noise rates on larger population
# ---------------------------------------------------------------------------

def test_high_name_typo_rate_produces_some_typos() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 100.0,
        "appearance_A_pct": 100.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 0.0,
        "duplication_in_B_pct": 0.0,
        "noise": {"A": _zero_noise(), "B": {**_zero_noise(), "name_typo_pct": 80.0}},
    })
    people = _truth_people(50)
    residence = _truth_residence(50)
    emitted = emit_observed_datasets(
        truth_people_df=people,
        truth_residence_history_df=residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=42,
    )
    dataset_b = emitted["datasets"]["B"]
    # With 80% typo rate, we expect many names to be different from truth
    assert emitted["metrics"]["datasets"]["B"]["noise_counts"]["name_typo"] > 0


def test_high_middle_name_missing_rate() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 100.0,
        "appearance_A_pct": 100.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 0.0,
        "duplication_in_B_pct": 0.0,
        "noise": {"A": _zero_noise(), "B": {**_zero_noise(), "middle_name_missing_pct": 90.0}},
    })
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people(50),
        truth_residence_history_df=_truth_residence(50),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=43,
    )
    assert emitted["metrics"]["datasets"]["B"]["noise_counts"]["middle_name_missing"] > 0


def test_ssn_mask_produces_partial_masking() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 100.0,
        "appearance_A_pct": 100.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 0.0,
        "duplication_in_B_pct": 0.0,
        "noise": {"A": _zero_noise(), "B": {**_zero_noise(), "ssn_mask_pct": 80.0}},
    })
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people(50),
        truth_residence_history_df=_truth_residence(50),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=44,
    )
    assert emitted["metrics"]["datasets"]["B"]["noise_counts"]["ssn_mask"] > 0


def test_zero_noise_produces_clean_output() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 100.0,
        "appearance_A_pct": 100.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 0.0,
        "duplication_in_B_pct": 0.0,
        "noise": {"A": _zero_noise(), "B": _zero_noise()},
    })
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people(20),
        truth_residence_history_df=_truth_residence(20),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=45,
    )
    for dataset_id in ("A", "B"):
        noise = emitted["metrics"]["datasets"][dataset_id]["noise_counts"]
        total_noise = sum(noise.values())
        assert total_noise == 0, f"Expected zero noise in {dataset_id} but got {noise}"


# ---------------------------------------------------------------------------
# Duplication math
# ---------------------------------------------------------------------------

def test_duplication_increases_record_count() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 100.0,
        "appearance_A_pct": 100.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 0.0,
        "duplication_in_B_pct": 50.0,
        "noise": {"A": _zero_noise(), "B": _zero_noise()},
    })
    people = _truth_people(20)
    emitted = emit_observed_datasets(
        truth_people_df=people,
        truth_residence_history_df=_truth_residence(20),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=46,
    )
    a_rows = len(emitted["datasets"]["A"])
    b_rows = len(emitted["datasets"]["B"])
    assert b_rows > a_rows, "B should have more rows due to 50% duplication"


def test_entity_record_map_covers_all_records() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 100.0,
        "appearance_A_pct": 100.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 10.0,
        "duplication_in_B_pct": 20.0,
        "noise": {"A": _zero_noise(), "B": _zero_noise()},
    })
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people(30),
        truth_residence_history_df=_truth_residence(30),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=47,
    )
    erm = emitted["entity_record_map"]
    total_dataset_rows = sum(len(ds) for ds in emitted["datasets"].values())
    assert len(erm) == total_dataset_rows


# ---------------------------------------------------------------------------
# Overlap and coverage
# ---------------------------------------------------------------------------

def test_low_overlap_produces_fewer_overlap_entities() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 20.0,
        "appearance_A_pct": 100.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 0.0,
        "duplication_in_B_pct": 0.0,
        "noise": {"A": _zero_noise(), "B": _zero_noise()},
    })
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people(30),
        truth_residence_history_df=_truth_residence(30),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=48,
    )
    overlap = emitted["metrics"]["coverage"]["overlap_entities"]
    # With 20% overlap target, expect fewer overlap entities than total
    assert overlap <= 30


def test_appearance_pct_limits_dataset_size() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 100.0,
        "appearance_A_pct": 50.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 0.0,
        "duplication_in_B_pct": 0.0,
        "noise": {"A": _zero_noise(), "B": _zero_noise()},
    })
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people(40),
        truth_residence_history_df=_truth_residence(40),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=49,
    )
    a_rows = len(emitted["datasets"]["A"])
    b_rows = len(emitted["datasets"]["B"])
    # A at 50% appearance should have fewer rows than B at 100%
    assert a_rows < b_rows


# ---------------------------------------------------------------------------
# Record key uniqueness
# ---------------------------------------------------------------------------

def test_record_keys_are_unique_within_dataset() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 100.0,
        "appearance_A_pct": 100.0,
        "appearance_B_pct": 100.0,
        "duplication_in_A_pct": 30.0,
        "duplication_in_B_pct": 30.0,
        "noise": {"A": _zero_noise(), "B": _zero_noise()},
    })
    emitted = emit_observed_datasets(
        truth_people_df=_truth_people(30),
        truth_residence_history_df=_truth_residence(30),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=50,
    )
    for ds_id, ds_df in emitted["datasets"].items():
        assert ds_df["RecordKey"].is_unique, f"RecordKeys not unique in {ds_id}"


# ---------------------------------------------------------------------------
# Emission is deterministic
# ---------------------------------------------------------------------------

def test_emission_is_deterministic() -> None:
    cfg = parse_emission_config({
        "crossfile_match_mode": "one_to_one",
        "overlap_entity_pct": 80.0,
        "appearance_A_pct": 90.0,
        "appearance_B_pct": 90.0,
        "duplication_in_A_pct": 5.0,
        "duplication_in_B_pct": 10.0,
        "noise": {"A": _zero_noise(), "B": {**_zero_noise(), "name_typo_pct": 5.0}},
    })
    kwargs = dict(
        truth_people_df=_truth_people(30),
        truth_residence_history_df=_truth_residence(30),
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=999,
    )
    run1 = emit_observed_datasets(**kwargs)
    run2 = emit_observed_datasets(**kwargs)
    pd.testing.assert_frame_equal(run1["datasets"]["A"], run2["datasets"]["A"])
    pd.testing.assert_frame_equal(run1["datasets"]["B"], run2["datasets"]["B"])
    pd.testing.assert_frame_equal(run1["entity_record_map"], run2["entity_record_map"])


# ---------------------------------------------------------------------------
# Config validation edge cases
# ---------------------------------------------------------------------------

def test_parse_emission_config_rejects_invalid_match_mode() -> None:
    with pytest.raises(ValueError):
        parse_emission_config({"crossfile_match_mode": "random"})


def test_dataset_id_with_special_chars_rejected() -> None:
    with pytest.raises(ValueError, match="letters, digits"):
        parse_emission_config({
            "datasets": [
                {"dataset_id": "reg@istry", "snapshot": "simulation_end", "appearance_pct": 100.0, "duplication_pct": 0.0}
            ],
        })


def test_filename_without_csv_extension_rejected() -> None:
    with pytest.raises(ValueError, match=".csv"):
        parse_emission_config({
            "datasets": [
                {"dataset_id": "registry", "filename": "output.parquet", "snapshot": "simulation_end", "appearance_pct": 100.0, "duplication_pct": 0.0}
            ],
        })
