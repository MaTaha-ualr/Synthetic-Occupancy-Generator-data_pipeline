"""Extended constraint tests — age gap, child lives alone, config validation."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import (
    ConstraintConfig,
    parse_constraints_config,
    validate_constraints_against_truth,
    validate_constraints_config,
)


def _people(*ages: tuple[str, int]) -> pd.DataFrame:
    return pd.DataFrame([{"PersonKey": pk, "Age": age} for pk, age in ages])


def _empty_events() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "EventKey", "EventType", "EventDate", "SubjectPersonKey",
        "SubjectHouseholdKey", "FromAddressKey", "ToAddressKey",
        "PersonKeyA", "PersonKeyB", "NewHouseholdKey", "CohabitMode",
        "ChildPersonKey", "Parent1PersonKey", "Parent2PersonKey", "CustodyMode",
    ])


def _empty_residence() -> pd.DataFrame:
    return pd.DataFrame(columns=["PersonKey", "AddressKey", "ResidenceStartDate", "ResidenceEndDate"])


def _event(event_type: str, **fields: str) -> dict[str, str]:
    base = {
        "EventKey": "E1", "EventType": event_type, "EventDate": "2026-01-01",
        "SubjectPersonKey": "", "SubjectHouseholdKey": "",
        "FromAddressKey": "", "ToAddressKey": "",
        "PersonKeyA": "", "PersonKeyB": "", "NewHouseholdKey": "",
        "CohabitMode": "", "ChildPersonKey": "",
        "Parent1PersonKey": "", "Parent2PersonKey": "", "CustodyMode": "",
    }
    base.update(fields)
    return base


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------

def test_parse_constraints_custom_fertility_range() -> None:
    cfg = parse_constraints_config({"fertility_age_range": {"min": 18, "max": 45}})
    assert cfg.fertility_age_min == 18
    assert cfg.fertility_age_max == 45


def test_parse_constraints_partner_gap_distribution() -> None:
    cfg = parse_constraints_config({
        "partner_age_gap_distribution": {0: 50, 5: 30, 10: 15, 20: 5}
    })
    assert cfg.partner_age_gap_distribution is not None
    assert cfg.partner_age_gap_distribution[0] == 50


def test_parse_constraints_rejects_empty_gap_distribution() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        parse_constraints_config({"partner_age_gap_distribution": {}})


def test_parse_constraints_rejects_zero_weight_gap_distribution() -> None:
    with pytest.raises(ValueError, match="positive weight sum"):
        parse_constraints_config({"partner_age_gap_distribution": {0: 0, 5: 0}})


def test_parse_constraints_rejects_negative_gap_key() -> None:
    with pytest.raises(ValueError, match="keys must be >= 0"):
        parse_constraints_config({"partner_age_gap_distribution": {-1: 10}})


def test_config_rejects_marriage_age_negative() -> None:
    with pytest.raises(ValueError, match="between 0 and 120"):
        validate_constraints_config(ConstraintConfig(min_marriage_age=-1))


def test_config_rejects_marriage_age_above_120() -> None:
    with pytest.raises(ValueError, match="between 0 and 120"):
        validate_constraints_config(ConstraintConfig(min_marriage_age=121))


def test_config_rejects_fertility_min_above_max() -> None:
    with pytest.raises(ValueError, match="min must be <="):
        validate_constraints_config(ConstraintConfig(fertility_age_min=50, fertility_age_max=40))


def test_config_rejects_negative_partner_gap() -> None:
    with pytest.raises(ValueError, match="must be >= 0"):
        validate_constraints_config(ConstraintConfig(max_partner_age_gap=-1))


# ---------------------------------------------------------------------------
# Partner age gap violation
# ---------------------------------------------------------------------------

def test_partner_age_gap_violation() -> None:
    people = _people(("1", 20), ("2", 60))
    events = pd.DataFrame([_event("COHABIT", PersonKeyA="1", PersonKeyB="2",
                                   NewHouseholdKey="H1", CohabitMode="new_address")])
    cfg = parse_constraints_config({"max_partner_age_gap": 10})
    result = validate_constraints_against_truth(
        truth_people_df=people, truth_events_df=events,
        truth_residence_history_df=_empty_residence(), config=cfg,
    )
    assert result["valid"] is False
    assert any(v["rule_id"] == "max_partner_age_gap" for v in result["violations"])


def test_partner_age_gap_within_limit_passes() -> None:
    people = _people(("1", 30), ("2", 35))
    events = pd.DataFrame([_event("COHABIT", PersonKeyA="1", PersonKeyB="2",
                                   NewHouseholdKey="H1", CohabitMode="new_address")])
    cfg = parse_constraints_config({"max_partner_age_gap": 10})
    result = validate_constraints_against_truth(
        truth_people_df=people, truth_events_df=events,
        truth_residence_history_df=_empty_residence(), config=cfg,
    )
    gap_violations = [v for v in result["violations"] if v["rule_id"] == "max_partner_age_gap"]
    assert len(gap_violations) == 0


def test_partner_gap_distribution_uses_max_key() -> None:
    people = _people(("1", 20), ("2", 50))
    events = pd.DataFrame([_event("COHABIT", PersonKeyA="1", PersonKeyB="2",
                                   NewHouseholdKey="H1", CohabitMode="new_address")])
    cfg = parse_constraints_config({"partner_age_gap_distribution": {0: 50, 5: 30, 10: 15}})
    result = validate_constraints_against_truth(
        truth_people_df=people, truth_events_df=events,
        truth_residence_history_df=_empty_residence(), config=cfg,
    )
    assert any(v["rule_id"] == "max_partner_age_gap" for v in result["violations"])


# ---------------------------------------------------------------------------
# Child lives alone
# ---------------------------------------------------------------------------

def test_child_leaves_home_underage_violation() -> None:
    people = _people(("C1", 15))
    events = pd.DataFrame([_event("LEAVE_HOME", ChildPersonKey="C1")])
    cfg = parse_constraints_config({"allow_child_lives_alone": False})
    result = validate_constraints_against_truth(
        truth_people_df=people, truth_events_df=events,
        truth_residence_history_df=_empty_residence(), config=cfg,
    )
    assert result["valid"] is False
    assert any(v["rule_id"] == "allow_child_lives_alone" for v in result["violations"])


def test_child_leaves_home_underage_allowed() -> None:
    people = _people(("C1", 15))
    events = pd.DataFrame([_event("LEAVE_HOME", ChildPersonKey="C1")])
    cfg = parse_constraints_config({"allow_child_lives_alone": True})
    result = validate_constraints_against_truth(
        truth_people_df=people, truth_events_df=events,
        truth_residence_history_df=_empty_residence(), config=cfg,
    )
    alone_violations = [v for v in result["violations"] if v["rule_id"] == "allow_child_lives_alone"]
    assert len(alone_violations) == 0


def test_adult_leaves_home_no_violation() -> None:
    people = _people(("C1", 22))
    events = pd.DataFrame([_event("LEAVE_HOME", ChildPersonKey="C1")])
    cfg = parse_constraints_config({})
    result = validate_constraints_against_truth(
        truth_people_df=people, truth_events_df=events,
        truth_residence_history_df=_empty_residence(), config=cfg,
    )
    alone_violations = [v for v in result["violations"] if v["rule_id"] == "allow_child_lives_alone"]
    assert len(alone_violations) == 0


# ---------------------------------------------------------------------------
# Residence intervals
# ---------------------------------------------------------------------------

def test_residence_end_before_start_violation() -> None:
    residence = pd.DataFrame([{
        "PersonKey": "1", "AddressKey": "A1",
        "ResidenceStartDate": "2024-06-01", "ResidenceEndDate": "2024-01-01",
    }])
    cfg = ConstraintConfig()
    result = validate_constraints_against_truth(
        truth_people_df=_people(("1", 30)),
        truth_events_df=_empty_events(),
        truth_residence_history_df=residence, config=cfg,
    )
    assert result["valid"] is False
    assert any(v["rule_id"] == "residence_interval_order" for v in result["violations"])


def test_non_overlapping_residence_passes() -> None:
    residence = pd.DataFrame([
        {"PersonKey": "1", "AddressKey": "A1", "ResidenceStartDate": "2024-01-01", "ResidenceEndDate": "2024-05-31"},
        {"PersonKey": "1", "AddressKey": "A2", "ResidenceStartDate": "2024-06-01", "ResidenceEndDate": "2024-12-31"},
    ])
    cfg = ConstraintConfig()
    result = validate_constraints_against_truth(
        truth_people_df=_people(("1", 30)),
        truth_events_df=_empty_events(),
        truth_residence_history_df=residence, config=cfg,
    )
    overlap_violations = [v for v in result["violations"] if v["rule_id"] == "non_overlapping_residence_intervals"]
    assert len(overlap_violations) == 0


def test_divorce_also_checks_marriage_age() -> None:
    people = _people(("1", 16), ("2", 30))
    events = pd.DataFrame([_event("DIVORCE", PersonKeyA="1", PersonKeyB="2", CustodyMode="joint")])
    cfg = parse_constraints_config({})
    result = validate_constraints_against_truth(
        truth_people_df=people, truth_events_df=events,
        truth_residence_history_df=_empty_residence(), config=cfg,
    )
    assert any(v["rule_id"] == "min_marriage_age" for v in result["violations"])


def test_empty_events_produces_valid_result() -> None:
    result = validate_constraints_against_truth(
        truth_people_df=_people(("1", 30)),
        truth_events_df=_empty_events(),
        truth_residence_history_df=_empty_residence(),
        config=ConstraintConfig(),
    )
    assert result["valid"] is True
    assert result["violation_count"] == 0
