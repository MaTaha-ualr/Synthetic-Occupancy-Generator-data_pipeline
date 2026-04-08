"""Extended quality module tests — fills the critical gap (was only 2 tests)."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import parse_constraints_config
from sog_phase2.quality import compute_phase2_quality_report, parse_quality_config


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_people(n: int = 3) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "PersonKey": str(i),
            "FormalFirstName": f"First{i}",
            "MiddleName": "M" if i % 2 == 0 else "",
            "LastName": f"Last{i}",
            "Suffix": "",
            "FormalFullName": f"First{i} Last{i}",
            "Gender": "female" if i % 2 == 0 else "male",
            "Ethnicity": "White",
            "DOB": f"199{i}-01-01",
            "Age": 30 + i,
            "AgeBin": "age_18_34" if i <= 2 else "age_35_64",
            "SSN": f"{i}{i}{i}-{i}{i}-{i}{i}{i}{i}",
            "Phone": f"555-{i}{i}{i}-{i}{i}{i}{i}",
        })
    return pd.DataFrame(rows)


def _make_households(n: int = 2) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "HouseholdKey": f"H{i}",
            "HouseholdType": "solo_house",
            "HouseholdStartDate": "2026-01-01",
            "HouseholdEndDate": "",
        })
    return pd.DataFrame(rows)


def _make_memberships(n: int = 2) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "PersonKey": str(i),
            "HouseholdKey": f"H{i}",
            "HouseholdRole": "HEAD",
            "MembershipStartDate": "2026-01-01",
            "MembershipEndDate": "",
        })
    return pd.DataFrame(rows)


def _make_residence(n: int = 2) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "PersonKey": str(i),
            "AddressKey": f"A{i}",
            "ResidenceStartDate": "2026-01-01",
            "ResidenceEndDate": "",
        })
    return pd.DataFrame(rows)


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "EventKey", "EventType", "EventDate", "SubjectPersonKey",
        "SubjectHouseholdKey", "FromAddressKey", "ToAddressKey",
        "PersonKeyA", "PersonKeyB", "NewHouseholdKey", "CohabitMode",
        "ChildPersonKey", "Parent1PersonKey", "Parent2PersonKey", "CustodyMode",
    ])


def _make_move_events(n: int = 1) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "EventKey": f"E{i + 1}", "EventType": "MOVE",
            "EventDate": f"2026-0{min(i + 2, 9)}-01",
            "SubjectPersonKey": "1", "SubjectHouseholdKey": "",
            "FromAddressKey": f"A{i}", "ToAddressKey": f"A{i + 10}",
            "PersonKeyA": "", "PersonKeyB": "", "NewHouseholdKey": "",
            "CohabitMode": "", "ChildPersonKey": "",
            "Parent1PersonKey": "", "Parent2PersonKey": "", "CustodyMode": "",
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Quality config parsing
# ---------------------------------------------------------------------------

def test_parse_quality_config_custom_range() -> None:
    cfg = parse_quality_config({"household_size_range": {"min": 2, "max": 6}})
    assert cfg.household_size_min == 2
    assert cfg.household_size_max == 6


def test_parse_quality_config_rejects_min_zero() -> None:
    with pytest.raises(ValueError, match="min must be >= 1"):
        parse_quality_config({"household_size_range": {"min": 0, "max": 5}})


def test_parse_quality_config_rejects_max_below_min() -> None:
    with pytest.raises(ValueError, match="max must be >= min"):
        parse_quality_config({"household_size_range": {"min": 5, "max": 3}})


def test_parse_quality_config_none_input() -> None:
    cfg = parse_quality_config(None)
    assert cfg.household_size_min == 1
    assert cfg.household_size_max == 12


# ---------------------------------------------------------------------------
# Quality report — truth consistency
# ---------------------------------------------------------------------------

def test_quality_report_empty_events_returns_zero_event_counts() -> None:
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(2),
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=_empty_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
    )
    assert report["scenario_metrics"]["event_counts"]["moves"] == 0
    assert report["scenario_metrics"]["event_counts"]["couples_formed"] == 0
    assert report["scenario_metrics"]["event_counts"]["births"] == 0
    assert report["scenario_metrics"]["event_counts"]["divorces"] == 0


def test_quality_report_move_events_counted() -> None:
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(2),
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=_make_move_events(3),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
    )
    assert report["scenario_metrics"]["event_counts"]["moves"] == 3


def test_quality_report_household_size_violation_detected() -> None:
    """Household with 3 members should violate max=2."""
    memberships = pd.DataFrame([
        {"PersonKey": "1", "HouseholdKey": "H1", "HouseholdRole": "HEAD", "MembershipStartDate": "2026-01-01", "MembershipEndDate": ""},
        {"PersonKey": "2", "HouseholdKey": "H1", "HouseholdRole": "SPOUSE", "MembershipStartDate": "2026-01-01", "MembershipEndDate": ""},
        {"PersonKey": "3", "HouseholdKey": "H1", "HouseholdRole": "CHILD", "MembershipStartDate": "2026-01-01", "MembershipEndDate": ""},
    ])
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(3),
        truth_households_df=_make_households(1),
        truth_household_memberships_df=memberships,
        truth_residence_history_df=_make_residence(3),
        truth_events_df=_empty_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({"household_size_range": {"min": 1, "max": 2}}),
    )
    assert report["truth_consistency"]["household_size_constraints"]["within_config_constraints"] is False
    assert report["truth_consistency"]["household_size_constraints"]["violating_households"] >= 1


def test_quality_report_no_overlap_errors_with_clean_residence() -> None:
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(2),
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=_empty_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
    )
    assert report["truth_consistency"]["time_overlap_errors"]["no_time_overlap_errors"] is True


def test_quality_report_moves_per_person_distribution() -> None:
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(2),
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=_make_move_events(2),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
    )
    dist = report["scenario_metrics"]["moves_per_person_distribution"]
    assert dist["people_evaluated"] >= 2
    assert dist["total_moves"] >= 2
    assert "distribution" in dist


def test_quality_report_household_type_shares() -> None:
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(2),
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=_empty_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
    )
    shares = report["scenario_metrics"]["household_type_shares"]
    assert shares["count"] >= 1
    assert "shares_pct" in shares


# ---------------------------------------------------------------------------
# Quality report — ER benchmark metrics (with observed data)
# ---------------------------------------------------------------------------

def test_quality_report_er_metrics_not_available_without_observed_data() -> None:
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(2),
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=_empty_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
    )
    assert report["er_benchmark_metrics"]["available"] is False


def test_quality_report_er_metrics_with_single_dataset() -> None:
    entity_record_map = pd.DataFrame([
        {"PersonKey": "1", "DatasetId": "registry", "RecordKey": "R1"},
        {"PersonKey": "1", "DatasetId": "registry", "RecordKey": "R2"},
        {"PersonKey": "2", "DatasetId": "registry", "RecordKey": "R3"},
    ])
    observed = {"registry": pd.DataFrame([
        {"RecordKey": "R1", "FirstName": "First1", "MiddleName": "", "LastName": "Last1", "DOB": "1991-01-01", "SSN": "111-11-1111", "Phone": "555-111-1111", "AddressKey": "A1", "SourceSnapshotDate": "2026-01-01"},
        {"RecordKey": "R2", "FirstName": "First1", "MiddleName": "", "LastName": "Last1", "DOB": "1991-01-01", "SSN": "111-11-1111", "Phone": "555-111-1111", "AddressKey": "A1", "SourceSnapshotDate": "2026-01-01"},
        {"RecordKey": "R3", "FirstName": "First2", "MiddleName": "M", "LastName": "Last2", "DOB": "1992-01-01", "SSN": "222-22-2222", "Phone": "555-222-2222", "AddressKey": "A2", "SourceSnapshotDate": "2026-01-01"},
    ])}
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(2),
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=_empty_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
        entity_record_map_df=entity_record_map,
        observed_datasets=observed,
        observed_relationship_mode="single_dataset",
    )
    assert report["er_benchmark_metrics"]["available"] is True
    assert report["er_benchmark_metrics"]["topology"]["relationship_mode"] == "single_dataset"
    dup = report["er_benchmark_metrics"]["within_file_duplicate_rates"]["registry"]
    assert dup["duplicate_rows"] >= 1


def test_quality_report_er_metrics_with_pairwise_crosswalk() -> None:
    dataset_a = pd.DataFrame([
        {"A_RecordKey": "A-1", "FirstName": "First1", "MiddleName": "", "LastName": "Last1", "DOB": "1991-01-01", "SSN": "111-11-1111", "Phone": "555-111-1111", "AddressKey": "A1", "SourceSnapshotDate": "2026-01-01"},
    ])
    dataset_b = pd.DataFrame([
        {"B_RecordKey": "B-1", "FirstName": "First1", "MiddleName": "", "LastName": "Last1", "DOB": "1991-01-01", "SSN": "111-11-1111", "Phone": "555-111-1111", "AddressKey": "A1", "SourceSnapshotDate": "2026-12-31"},
    ])
    crosswalk = pd.DataFrame([{"PersonKey": "1", "A_RecordKey": "A-1", "B_RecordKey": "B-1"}])
    report = compute_phase2_quality_report(
        truth_people_df=_make_people(2),
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=_empty_events(),
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
        dataset_a_df=dataset_a,
        dataset_b_df=dataset_b,
        truth_crosswalk_df=crosswalk,
    )
    assert report["er_benchmark_metrics"]["available"] is True
    assert report["er_benchmark_metrics"]["cross_file_overlap"]["overlap_entities"] == 1
    assert report["er_benchmark_metrics"]["match_cardinality_achieved"]["one_to_one"] >= 1
    assert report["er_benchmark_metrics"]["crosswalk_ambiguity"]["a_record_to_multiple_persons"] == 0


def test_quality_report_event_age_violation_in_report() -> None:
    """Underage cohabit should be flagged in truth_consistency."""
    people = pd.DataFrame([
        {"PersonKey": "1", "FormalFirstName": "Teen", "MiddleName": "", "LastName": "X", "Suffix": "", "FormalFullName": "Teen X", "Gender": "female", "Ethnicity": "White", "DOB": "2010-01-01", "Age": 16, "AgeBin": "age_0_17", "SSN": "000-00-0000", "Phone": "000-000-0000"},
        {"PersonKey": "2", "FormalFirstName": "Adult", "MiddleName": "", "LastName": "Y", "Suffix": "", "FormalFullName": "Adult Y", "Gender": "male", "Ethnicity": "White", "DOB": "1990-01-01", "Age": 36, "AgeBin": "age_35_64", "SSN": "111-11-1111", "Phone": "111-111-1111"},
    ])
    events = pd.DataFrame([{
        "EventKey": "E1", "EventType": "COHABIT", "EventDate": "2026-02-01",
        "SubjectPersonKey": "", "SubjectHouseholdKey": "",
        "FromAddressKey": "", "ToAddressKey": "",
        "PersonKeyA": "1", "PersonKeyB": "2",
        "NewHouseholdKey": "H2", "CohabitMode": "new_address",
        "ChildPersonKey": "", "Parent1PersonKey": "", "Parent2PersonKey": "", "CustodyMode": "",
    }])
    report = compute_phase2_quality_report(
        truth_people_df=people,
        truth_households_df=_make_households(2),
        truth_household_memberships_df=_make_memberships(2),
        truth_residence_history_df=_make_residence(2),
        truth_events_df=events,
        constraints_config=parse_constraints_config({}),
        quality_config=parse_quality_config({}),
    )
    age_validation = report["truth_consistency"]["event_age_validation"]
    assert age_validation["invalid_event_age_count"] >= 1
    assert "min_marriage_age" in age_validation["invalid_event_age_by_rule"]
