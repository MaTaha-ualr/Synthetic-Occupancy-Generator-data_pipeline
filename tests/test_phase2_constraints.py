from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import ConstraintConfig, parse_constraints_config, validate_constraints_against_truth


def _base_truth_people() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"PersonKey": "1", "Age": 16},
            {"PersonKey": "2", "Age": 30},
            {"PersonKey": "3", "Age": 52},
        ]
    )


def _empty_residence() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["PersonKey", "AddressKey", "ResidenceStartDate", "ResidenceEndDate"]
    )


def test_parse_constraints_defaults() -> None:
    cfg = parse_constraints_config({})
    assert cfg.min_marriage_age == 18
    assert cfg.max_partner_age_gap == 25
    assert cfg.fertility_age_min == 15
    assert cfg.fertility_age_max == 49
    assert cfg.allow_underage_marriage is False
    assert cfg.allow_child_lives_alone is False
    assert cfg.enforce_non_overlapping_residence_intervals is True


def test_underage_cohabit_violation_when_disallowed() -> None:
    events = pd.DataFrame(
        [
            {
                "EventKey": "E1",
                "EventType": "COHABIT",
                "EventDate": "2024-01-01",
                "PersonKeyA": "1",
                "PersonKeyB": "2",
                "NewHouseholdKey": "H2",
                "CohabitMode": "new_address",
                "SubjectPersonKey": "",
                "SubjectHouseholdKey": "",
                "FromAddressKey": "",
                "ToAddressKey": "",
                "ChildPersonKey": "",
                "Parent1PersonKey": "",
                "Parent2PersonKey": "",
                "CustodyMode": "",
            }
        ]
    )
    cfg = parse_constraints_config({})
    result = validate_constraints_against_truth(
        truth_people_df=_base_truth_people(),
        truth_events_df=events,
        truth_residence_history_df=_empty_residence(),
        config=cfg,
    )
    assert result["valid"] is False
    assert any(v["rule_id"] == "min_marriage_age" for v in result["violations"])


def test_underage_cohabit_allowed_with_switch() -> None:
    events = pd.DataFrame(
        [
            {
                "EventKey": "E1",
                "EventType": "COHABIT",
                "EventDate": "2024-01-01",
                "PersonKeyA": "1",
                "PersonKeyB": "2",
                "NewHouseholdKey": "H2",
                "CohabitMode": "new_address",
                "SubjectPersonKey": "",
                "SubjectHouseholdKey": "",
                "FromAddressKey": "",
                "ToAddressKey": "",
                "ChildPersonKey": "",
                "Parent1PersonKey": "",
                "Parent2PersonKey": "",
                "CustodyMode": "",
            }
        ]
    )
    cfg = parse_constraints_config({"allow_underage_marriage": True})
    result = validate_constraints_against_truth(
        truth_people_df=_base_truth_people(),
        truth_events_df=events,
        truth_residence_history_df=_empty_residence(),
        config=cfg,
    )
    assert all(v["rule_id"] != "min_marriage_age" for v in result["violations"])


def test_birth_outside_fertility_range_violation() -> None:
    events = pd.DataFrame(
        [
            {
                "EventKey": "E2",
                "EventType": "BIRTH",
                "EventDate": "2024-02-10",
                "Parent1PersonKey": "3",
                "Parent2PersonKey": "",
                "ChildPersonKey": "1",
                "SubjectPersonKey": "",
                "SubjectHouseholdKey": "",
                "FromAddressKey": "",
                "ToAddressKey": "",
                "PersonKeyA": "",
                "PersonKeyB": "",
                "NewHouseholdKey": "",
                "CohabitMode": "",
                "CustodyMode": "",
            }
        ]
    )
    cfg = parse_constraints_config({"fertility_age_range": {"min": 15, "max": 45}})
    result = validate_constraints_against_truth(
        truth_people_df=_base_truth_people(),
        truth_events_df=events,
        truth_residence_history_df=_empty_residence(),
        config=cfg,
    )
    assert result["valid"] is False
    assert any(v["rule_id"] == "fertility_age_range" for v in result["violations"])


def test_overlapping_residence_interval_violation() -> None:
    residence = pd.DataFrame(
        [
            {
                "PersonKey": "2",
                "AddressKey": "A1",
                "ResidenceStartDate": "2024-01-01",
                "ResidenceEndDate": "2024-06-30",
            },
            {
                "PersonKey": "2",
                "AddressKey": "A2",
                "ResidenceStartDate": "2024-06-01",
                "ResidenceEndDate": "2024-12-31",
            },
        ]
    )
    cfg = ConstraintConfig()
    result = validate_constraints_against_truth(
        truth_people_df=_base_truth_people(),
        truth_events_df=pd.DataFrame(columns=["EventType"]),
        truth_residence_history_df=residence,
        config=cfg,
    )
    assert result["valid"] is False
    assert any(v["rule_id"] == "non_overlapping_residence_intervals" for v in result["violations"])
