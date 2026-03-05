from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import parse_constraints_config
from sog_phase2.quality import compute_phase2_quality_report, parse_quality_config


def _truth_people() -> pd.DataFrame:
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
    )


def _truth_households() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"HouseholdKey": "H1", "HouseholdType": "solo_house", "HouseholdStartDate": "2026-01-01", "HouseholdEndDate": ""},
            {"HouseholdKey": "H2", "HouseholdType": "couple", "HouseholdStartDate": "2026-02-01", "HouseholdEndDate": ""},
        ]
    )


def _truth_memberships() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"PersonKey": "1", "HouseholdKey": "H1", "HouseholdRole": "HEAD", "MembershipStartDate": "2026-01-01", "MembershipEndDate": "2026-01-31"},
            {"PersonKey": "1", "HouseholdKey": "H2", "HouseholdRole": "HEAD", "MembershipStartDate": "2026-02-01", "MembershipEndDate": ""},
            {"PersonKey": "2", "HouseholdKey": "H2", "HouseholdRole": "SPOUSE", "MembershipStartDate": "2026-02-01", "MembershipEndDate": ""},
        ]
    )


def _truth_residence() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"PersonKey": "1", "AddressKey": "A1", "ResidenceStartDate": "2026-01-01", "ResidenceEndDate": "2026-01-31"},
            {"PersonKey": "1", "AddressKey": "A2", "ResidenceStartDate": "2026-02-01", "ResidenceEndDate": ""},
            {"PersonKey": "2", "AddressKey": "A3", "ResidenceStartDate": "2026-01-01", "ResidenceEndDate": "2026-01-31"},
            {"PersonKey": "2", "AddressKey": "A2", "ResidenceStartDate": "2026-02-01", "ResidenceEndDate": ""},
        ]
    )


def _truth_events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "EventKey": "E1",
                "EventType": "COHABIT",
                "EventDate": "2026-02-01",
                "SubjectPersonKey": "",
                "SubjectHouseholdKey": "",
                "FromAddressKey": "",
                "ToAddressKey": "",
                "PersonKeyA": "1",
                "PersonKeyB": "2",
                "NewHouseholdKey": "H2",
                "CohabitMode": "move_to_A",
                "ChildPersonKey": "",
                "Parent1PersonKey": "",
                "Parent2PersonKey": "",
                "CustodyMode": "",
            },
            {
                "EventKey": "E2",
                "EventType": "MOVE",
                "EventDate": "2026-03-01",
                "SubjectPersonKey": "",
                "SubjectHouseholdKey": "H2",
                "FromAddressKey": "A2",
                "ToAddressKey": "A4",
                "PersonKeyA": "",
                "PersonKeyB": "",
                "NewHouseholdKey": "",
                "CohabitMode": "",
                "ChildPersonKey": "",
                "Parent1PersonKey": "",
                "Parent2PersonKey": "",
                "CustodyMode": "",
            },
        ]
    )


def test_parse_quality_config_defaults() -> None:
    cfg = parse_quality_config({})
    assert cfg.household_size_min == 1
    assert cfg.household_size_max == 12


def test_phase2_quality_report_includes_er_metrics() -> None:
    dataset_a = pd.DataFrame(
        [
            {
                "A_RecordKey": "A-1",
                "FirstName": "Ava",
                "MiddleName": "M",
                "LastName": "Stone",
                "DOB": "1992-01-01",
                "SSN": "111-11-1111",
                "Phone": "111-222-3333",
                "AddressKey": "A1",
                "SourceSnapshotDate": "2026-01-01",
            },
            {
                "A_RecordKey": "A-2",
                "FirstName": "Liam",
                "MiddleName": "",
                "LastName": "Parker",
                "DOB": "1990-01-01",
                "SSN": "222-22-2222",
                "Phone": "444-555-6666",
                "AddressKey": "A3",
                "SourceSnapshotDate": "2026-01-01",
            },
        ]
    )
    dataset_b = pd.DataFrame(
        [
            {
                "B_RecordKey": "B-1",
                "FirstName": "Ava",
                "MiddleName": "",
                "LastName": "Stone",
                "DOB": "1992-01-01",
                "SSN": "111-11-1111",
                "Phone": "",
                "AddressKey": "A2",
                "SourceSnapshotDate": "2026-02-01",
            },
            {
                "B_RecordKey": "B-2",
                "FirstName": "Liam",
                "MiddleName": "",
                "LastName": "Parker",
                "DOB": "1990-01-01",
                "SSN": "222-22-2222",
                "Phone": "444-555-6666",
                "AddressKey": "A2",
                "SourceSnapshotDate": "2026-02-01",
            },
            {
                "B_RecordKey": "B-3",
                "FirstName": "Liam",
                "MiddleName": "",
                "LastName": "Parker",
                "DOB": "1990-01-01",
                "SSN": "222-22-2222",
                "Phone": "444-555-6666",
                "AddressKey": "A2",
                "SourceSnapshotDate": "2026-02-01",
            },
        ]
    )
    crosswalk = pd.DataFrame(
        [
            {"PersonKey": "1", "A_RecordKey": "A-1", "B_RecordKey": "B-1"},
            {"PersonKey": "2", "A_RecordKey": "A-2", "B_RecordKey": "B-2"},
            {"PersonKey": "2", "A_RecordKey": "A-2", "B_RecordKey": "B-3"},
        ]
    )

    report = compute_phase2_quality_report(
        truth_people_df=_truth_people(),
        truth_households_df=_truth_households(),
        truth_household_memberships_df=_truth_memberships(),
        truth_residence_history_df=_truth_residence(),
        truth_events_df=_truth_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({"household_size_range": {"min": 1, "max": 10}}),
        dataset_a_df=dataset_a,
        dataset_b_df=dataset_b,
        truth_crosswalk_df=crosswalk,
    )

    assert "truth_consistency" in report
    assert "scenario_metrics" in report
    assert "er_benchmark_metrics" in report
    assert report["er_benchmark_metrics"]["available"] is True
    assert report["er_benchmark_metrics"]["match_cardinality_achieved"]["one_to_many"] >= 1
