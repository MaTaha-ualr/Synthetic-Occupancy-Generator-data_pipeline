"""Extended event grammar tests — covers all 5 event types and edge cases."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.event_grammar import (
    ACTIVE_EVENT_TYPES,
    COHABIT_MODES,
    CUSTODY_MODES,
    TRUTH_EVENTS_REQUIRED_COLUMNS,
    validate_truth_events_dataframe,
)


def _blank_row(**overrides: str) -> dict[str, str]:
    row = {col: "" for col in TRUTH_EVENTS_REQUIRED_COLUMNS}
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# COHABIT
# ---------------------------------------------------------------------------

def test_valid_cohabit_event() -> None:
    row = _blank_row(
        EventKey="E1", EventType="COHABIT", EventDate="2026-02-01",
        PersonKeyA="1", PersonKeyB="2", NewHouseholdKey="H1", CohabitMode="move_to_A",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is True


def test_cohabit_missing_person_key_b() -> None:
    row = _blank_row(
        EventKey="E1", EventType="COHABIT", EventDate="2026-02-01",
        PersonKeyA="1", NewHouseholdKey="H1", CohabitMode="new_address",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("PersonKeyB" in err["field"] for err in result["errors"])


def test_cohabit_same_person_a_and_b() -> None:
    row = _blank_row(
        EventKey="E1", EventType="COHABIT", EventDate="2026-02-01",
        PersonKeyA="1", PersonKeyB="1", NewHouseholdKey="H1", CohabitMode="move_to_B",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("differ" in err["error"] for err in result["errors"])


def test_cohabit_invalid_mode() -> None:
    row = _blank_row(
        EventKey="E1", EventType="COHABIT", EventDate="2026-02-01",
        PersonKeyA="1", PersonKeyB="2", NewHouseholdKey="H1", CohabitMode="teleport",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("CohabitMode" in err["field"] for err in result["errors"])


def test_cohabit_all_modes_accepted() -> None:
    for mode in COHABIT_MODES:
        row = _blank_row(
            EventKey="E1", EventType="COHABIT", EventDate="2026-02-01",
            PersonKeyA="1", PersonKeyB="2", NewHouseholdKey="H1", CohabitMode=mode,
        )
        result = validate_truth_events_dataframe(pd.DataFrame([row]))
        assert result["valid"] is True, f"CohabitMode {mode} was rejected"


def test_cohabit_missing_household_key() -> None:
    row = _blank_row(
        EventKey="E1", EventType="COHABIT", EventDate="2026-02-01",
        PersonKeyA="1", PersonKeyB="2", CohabitMode="new_address",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("NewHouseholdKey" in err["field"] for err in result["errors"])


# ---------------------------------------------------------------------------
# BIRTH
# ---------------------------------------------------------------------------

def test_valid_birth_event() -> None:
    row = _blank_row(
        EventKey="E1", EventType="BIRTH", EventDate="2026-03-01",
        Parent1PersonKey="1", ChildPersonKey="C1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is True


def test_birth_with_two_parents() -> None:
    row = _blank_row(
        EventKey="E1", EventType="BIRTH", EventDate="2026-03-01",
        Parent1PersonKey="1", Parent2PersonKey="2", ChildPersonKey="C1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is True


def test_birth_missing_parent() -> None:
    row = _blank_row(
        EventKey="E1", EventType="BIRTH", EventDate="2026-03-01",
        ChildPersonKey="C1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("Parent1PersonKey" in err["field"] for err in result["errors"])


def test_birth_missing_child() -> None:
    row = _blank_row(
        EventKey="E1", EventType="BIRTH", EventDate="2026-03-01",
        Parent1PersonKey="1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("ChildPersonKey" in err["field"] for err in result["errors"])


def test_birth_parent_equals_child() -> None:
    row = _blank_row(
        EventKey="E1", EventType="BIRTH", EventDate="2026-03-01",
        Parent1PersonKey="1", ChildPersonKey="1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("differ" in err["error"] for err in result["errors"])


def test_birth_same_parents() -> None:
    row = _blank_row(
        EventKey="E1", EventType="BIRTH", EventDate="2026-03-01",
        Parent1PersonKey="1", Parent2PersonKey="1", ChildPersonKey="C1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("distinct parents" in err["error"] for err in result["errors"])


# ---------------------------------------------------------------------------
# DIVORCE
# ---------------------------------------------------------------------------

def test_valid_divorce_event() -> None:
    row = _blank_row(
        EventKey="E1", EventType="DIVORCE", EventDate="2026-04-01",
        PersonKeyA="1", PersonKeyB="2", CustodyMode="joint",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is True


def test_divorce_all_custody_modes_accepted() -> None:
    for mode in CUSTODY_MODES:
        row = _blank_row(
            EventKey="E1", EventType="DIVORCE", EventDate="2026-04-01",
            PersonKeyA="1", PersonKeyB="2", CustodyMode=mode,
        )
        result = validate_truth_events_dataframe(pd.DataFrame([row]))
        assert result["valid"] is True, f"CustodyMode {mode} was rejected"


def test_divorce_invalid_custody_mode() -> None:
    row = _blank_row(
        EventKey="E1", EventType="DIVORCE", EventDate="2026-04-01",
        PersonKeyA="1", PersonKeyB="2", CustodyMode="coin_flip",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("CustodyMode" in err["field"] for err in result["errors"])


def test_divorce_same_person() -> None:
    row = _blank_row(
        EventKey="E1", EventType="DIVORCE", EventDate="2026-04-01",
        PersonKeyA="1", PersonKeyB="1", CustodyMode="joint",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False


def test_divorce_missing_custody_mode() -> None:
    row = _blank_row(
        EventKey="E1", EventType="DIVORCE", EventDate="2026-04-01",
        PersonKeyA="1", PersonKeyB="2",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("CustodyMode" in err["field"] for err in result["errors"])


# ---------------------------------------------------------------------------
# LEAVE_HOME
# ---------------------------------------------------------------------------

def test_valid_leave_home_event() -> None:
    row = _blank_row(
        EventKey="E1", EventType="LEAVE_HOME", EventDate="2026-05-01",
        ChildPersonKey="C1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is True


def test_leave_home_missing_child() -> None:
    row = _blank_row(
        EventKey="E1", EventType="LEAVE_HOME", EventDate="2026-05-01",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("ChildPersonKey" in err["field"] for err in result["errors"])


# ---------------------------------------------------------------------------
# MOVE (extended)
# ---------------------------------------------------------------------------

def test_move_same_from_and_to_rejected() -> None:
    row = _blank_row(
        EventKey="E1", EventType="MOVE", EventDate="2026-01-15",
        SubjectPersonKey="1", FromAddressKey="A1", ToAddressKey="A1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("differ" in err["error"] for err in result["errors"])


def test_move_with_household_subject() -> None:
    row = _blank_row(
        EventKey="E1", EventType="MOVE", EventDate="2026-01-15",
        SubjectHouseholdKey="H1", FromAddressKey="A1", ToAddressKey="A2",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is True


def test_move_missing_from_address() -> None:
    row = _blank_row(
        EventKey="E1", EventType="MOVE", EventDate="2026-01-15",
        SubjectPersonKey="1", ToAddressKey="A2",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("FromAddressKey" in err["field"] for err in result["errors"])


def test_move_missing_to_address() -> None:
    row = _blank_row(
        EventKey="E1", EventType="MOVE", EventDate="2026-01-15",
        SubjectPersonKey="1", FromAddressKey="A1",
    )
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("ToAddressKey" in err["field"] for err in result["errors"])


# ---------------------------------------------------------------------------
# General / edge cases
# ---------------------------------------------------------------------------

def test_missing_event_key_flagged() -> None:
    row = _blank_row(EventType="MOVE", EventDate="2026-01-15",
                     SubjectPersonKey="1", FromAddressKey="A1", ToAddressKey="A2")
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("EventKey" in err["field"] for err in result["errors"])


def test_missing_event_date_flagged() -> None:
    row = _blank_row(EventKey="E1", EventType="MOVE",
                     SubjectPersonKey="1", FromAddressKey="A1", ToAddressKey="A2")
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("EventDate" in err["field"] for err in result["errors"])


def test_unsupported_event_type() -> None:
    row = _blank_row(EventKey="E1", EventType="TELEPORT", EventDate="2026-01-01")
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is False
    assert any("Unsupported" in err["error"] for err in result["errors"])


def test_missing_required_columns_returns_missing_list() -> None:
    df = pd.DataFrame([{"EventKey": "E1", "EventType": "MOVE"}])
    result = validate_truth_events_dataframe(df)
    assert result["valid"] is False
    assert len(result["missing_columns"]) > 0


def test_multiple_events_in_one_dataframe() -> None:
    rows = [
        _blank_row(EventKey="E1", EventType="MOVE", EventDate="2026-01-01",
                    SubjectPersonKey="1", FromAddressKey="A1", ToAddressKey="A2"),
        _blank_row(EventKey="E2", EventType="COHABIT", EventDate="2026-02-01",
                    PersonKeyA="1", PersonKeyB="2", NewHouseholdKey="H1", CohabitMode="new_address"),
        _blank_row(EventKey="E3", EventType="BIRTH", EventDate="2026-03-01",
                    Parent1PersonKey="1", ChildPersonKey="C1"),
        _blank_row(EventKey="E4", EventType="DIVORCE", EventDate="2026-04-01",
                    PersonKeyA="1", PersonKeyB="2", CustodyMode="joint"),
        _blank_row(EventKey="E5", EventType="LEAVE_HOME", EventDate="2026-05-01",
                    ChildPersonKey="C1"),
    ]
    result = validate_truth_events_dataframe(pd.DataFrame(rows))
    assert result["valid"] is True
    assert result["error_count"] == 0


def test_case_insensitive_event_type() -> None:
    row = _blank_row(EventKey="E1", EventType="move", EventDate="2026-01-01",
                     SubjectPersonKey="1", FromAddressKey="A1", ToAddressKey="A2")
    result = validate_truth_events_dataframe(pd.DataFrame([row]))
    assert result["valid"] is True


def test_empty_dataframe_is_valid() -> None:
    df = pd.DataFrame(columns=TRUTH_EVENTS_REQUIRED_COLUMNS)
    result = validate_truth_events_dataframe(df)
    assert result["valid"] is True
    assert result["error_count"] == 0
