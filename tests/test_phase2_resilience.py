from __future__ import annotations

from datetime import date
import sys
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import parse_constraints_config
from sog_phase2.emission import emit_observed_datasets, parse_emission_config
from sog_phase2.quality import compute_phase2_quality_report, parse_quality_config


def _build_truth_people(count: int) -> pd.DataFrame:
    rows = []
    for idx in range(1, count + 1):
        rows.append(
            {
                "PersonKey": str(idx),
                "FormalFirstName": f"First{idx}",
                "MiddleName": "M" if idx % 2 == 0 else "",
                "LastName": f"Last{idx}",
                "Suffix": "",
                "FormalFullName": f"First{idx} Last{idx}",
                "Gender": "female" if idx % 2 == 0 else "male",
                "Ethnicity": "White",
                "DOB": f"199{idx % 10}-01-01",
                "Age": 25 + (idx % 30),
                "AgeBin": "age_18_34" if idx % 3 else "age_35_64",
                "SSN": f"{idx:03d}-{idx % 100:02d}-{idx % 10000:04d}",
                "Phone": f"555-01{idx:04d}",
            }
        )
    return pd.DataFrame(rows)


def _build_truth_residence(count: int) -> pd.DataFrame:
    rows = []
    for idx in range(1, count + 1):
        rows.append(
            {
                "PersonKey": str(idx),
                "AddressKey": f"A{idx}",
                "ResidenceStartDate": "2026-01-01",
                "ResidenceEndDate": "",
            }
        )
    return pd.DataFrame(rows)


def _build_truth_households(count: int) -> pd.DataFrame:
    rows = []
    for idx in range(1, count + 1):
        rows.append(
            {
                "HouseholdKey": f"H{idx}",
                "HouseholdType": "solo_house",
                "HouseholdStartDate": "2026-01-01",
                "HouseholdEndDate": "",
            }
        )
    return pd.DataFrame(rows)


def _build_truth_memberships(count: int) -> pd.DataFrame:
    rows = []
    for idx in range(1, count + 1):
        rows.append(
            {
                "PersonKey": str(idx),
                "HouseholdKey": f"H{idx}",
                "HouseholdRole": "HEAD",
                "MembershipStartDate": "2026-01-01",
                "MembershipEndDate": "",
            }
        )
    return pd.DataFrame(rows)


def _build_truth_events() -> pd.DataFrame:
    return pd.DataFrame(
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
    )


def _single_dataset_config(appearance_pct: float = 100.0, duplication_pct: float = 25.0):
    return parse_emission_config(
        {
            "crossfile_match_mode": "single_dataset",
            "datasets": [
                {
                    "dataset_id": "registry",
                    "snapshot": "simulation_end",
                    "appearance_pct": appearance_pct,
                    "duplication_pct": duplication_pct,
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
    )


def _pairwise_dataset_list_config():
    return parse_emission_config(
        {
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
        }
    )


def _multi_dataset_list_config():
    return parse_emission_config(
        {
            "crossfile_match_mode": "many_to_many",
            "overlap_entity_pct": 75.0,
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
                    "duplication_pct": 40.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
                {
                    "dataset_id": "benefits",
                    "filename": "observed_benefits.csv",
                    "snapshot": "simulation_end",
                    "appearance_pct": 80.0,
                    "duplication_pct": 20.0,
                    "noise": {"name_typo_pct": 0.0, "middle_name_missing_pct": 0.0},
                },
            ],
        }
    )


def test_emit_observed_datasets_is_deterministic_for_same_seed() -> None:
    truth_people = _build_truth_people(12)
    truth_residence = _build_truth_residence(12)
    cfg = _single_dataset_config(appearance_pct=80.0, duplication_pct=20.0)

    first = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260405,
    )
    second = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260405,
    )

    assert_frame_equal(first["datasets"]["registry"], second["datasets"]["registry"])
    assert_frame_equal(first["entity_record_map"], second["entity_record_map"])


def test_emit_observed_datasets_changes_when_seed_changes() -> None:
    truth_people = _build_truth_people(20)
    truth_residence = _build_truth_residence(20)
    cfg = _single_dataset_config(appearance_pct=60.0, duplication_pct=10.0)

    first = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=1,
    )
    second = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=2,
    )

    first_people = set(first["entity_record_map"]["PersonKey"].astype(str))
    second_people = set(second["entity_record_map"]["PersonKey"].astype(str))
    assert first_people != second_people


def test_quality_report_supports_single_dataset_topology() -> None:
    truth_people = _build_truth_people(4)
    truth_residence = _build_truth_residence(4)
    cfg = _single_dataset_config(appearance_pct=100.0, duplication_pct=50.0)
    emitted = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260405,
    )

    report = compute_phase2_quality_report(
        truth_people_df=truth_people,
        truth_households_df=_build_truth_households(4),
        truth_household_memberships_df=_build_truth_memberships(4),
        truth_residence_history_df=truth_residence,
        truth_events_df=_build_truth_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({"household_size_range": {"min": 1, "max": 8}}),
        observed_datasets=emitted["datasets"],
        entity_record_map_df=emitted["entity_record_map"],
        truth_crosswalk_df=emitted["truth_crosswalk"],
        observed_relationship_mode=cfg.crossfile_match_mode,
    )

    er = report["er_benchmark_metrics"]
    assert er["topology"]["dataset_count"] == 1
    assert er["topology"]["dataset_ids"] == ["registry"]
    assert er["topology"]["relationship_mode"] == "single_dataset"
    assert "registry" in er["per_dataset"]
    assert er["per_dataset"]["registry"]["duplicate_rows"] == 2
    assert "cross_file_overlap" not in er


def test_quality_report_supports_pairwise_custom_dataset_ids() -> None:
    truth_people = _build_truth_people(4)
    truth_residence = _build_truth_residence(4)
    cfg = _pairwise_dataset_list_config()
    emitted = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260406,
    )

    report = compute_phase2_quality_report(
        truth_people_df=truth_people,
        truth_households_df=_build_truth_households(4),
        truth_household_memberships_df=_build_truth_memberships(4),
        truth_residence_history_df=truth_residence,
        truth_events_df=_build_truth_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({"household_size_range": {"min": 1, "max": 8}}),
        observed_datasets=emitted["datasets"],
        entity_record_map_df=emitted["entity_record_map"],
        truth_crosswalk_df=emitted["truth_crosswalk"],
        observed_relationship_mode=cfg.crossfile_match_mode,
    )

    er = report["er_benchmark_metrics"]
    assert er["topology"]["dataset_ids"] == ["registry", "claims"]
    assert er["topology"]["relationship_mode"] == "one_to_many"
    assert er["cross_file_overlap"]["dataset_ids"] == ["registry", "claims"]
    assert er["match_cardinality_achieved"]["one_to_many"] >= 1
    assert er["within_file_duplicate_rates"]["dataset_b"] == er["within_file_duplicate_rates"]["claims"]


def test_quality_report_supports_multi_dataset_topology() -> None:
    truth_people = _build_truth_people(8)
    truth_residence = _build_truth_residence(8)
    cfg = _multi_dataset_list_config()
    emitted = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260408,
    )

    report = compute_phase2_quality_report(
        truth_people_df=truth_people,
        truth_households_df=_build_truth_households(8),
        truth_household_memberships_df=_build_truth_memberships(8),
        truth_residence_history_df=truth_residence,
        truth_events_df=_build_truth_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({"household_size_range": {"min": 1, "max": 8}}),
        observed_datasets=emitted["datasets"],
        entity_record_map_df=emitted["entity_record_map"],
        truth_crosswalk_df=emitted["truth_crosswalk"],
        observed_relationship_mode=cfg.crossfile_match_mode,
    )

    er = report["er_benchmark_metrics"]
    assert er["topology"]["dataset_count"] == 3
    assert er["topology"]["dataset_ids"] == ["registry", "claims", "benefits"]
    assert er["topology"]["relationship_mode"] == "many_to_many"
    assert "multi_dataset_overlap" in er
    assert "pairwise_match_cardinality" in er
    assert er["multi_dataset_overlap"]["all_dataset_overlap_entities"] >= 1
    assert "registry__claims" in er["multi_dataset_overlap"]["pairwise_overlap"]


def test_emit_observed_datasets_scale_smoke_single_dataset() -> None:
    truth_people = _build_truth_people(1000)
    truth_residence = _build_truth_residence(1000)
    cfg = _single_dataset_config(appearance_pct=100.0, duplication_pct=10.0)
    emitted = emit_observed_datasets(
        truth_people_df=truth_people,
        truth_residence_history_df=truth_residence,
        simulation_start_date=date(2026, 1, 1),
        simulation_end_date=date(2026, 12, 31),
        emission_config=cfg,
        seed=20260407,
    )

    registry = emitted["datasets"]["registry"]
    entity_record_map = emitted["entity_record_map"]

    assert emitted["truth_crosswalk"] is None
    assert len(registry) == 1100
    assert len(entity_record_map) == 1100
    assert registry["RecordKey"].nunique() == 1100
    assert emitted["metrics"]["dataset_count"] == 1
