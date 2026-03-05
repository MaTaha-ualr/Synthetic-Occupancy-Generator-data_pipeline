from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.event_grammar import get_truth_event_grammar, validate_truth_events_dataframe


def _base_event_row() -> dict[str, str]:
    return {
        "EventKey": "E1",
        "EventType": "MOVE",
        "EventDate": "2024-01-01",
        "SubjectPersonKey": "1",
        "SubjectHouseholdKey": "",
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


def test_truth_events_grammar_accepts_valid_move() -> None:
    df = pd.DataFrame([_base_event_row()])
    result = validate_truth_events_dataframe(df)
    assert result["valid"] is True
    assert result["error_count"] == 0


def test_truth_events_grammar_rejects_move_without_subject() -> None:
    row = _base_event_row()
    row["SubjectPersonKey"] = ""
    row["SubjectHouseholdKey"] = ""
    df = pd.DataFrame([row])

    result = validate_truth_events_dataframe(df)
    assert result["valid"] is False
    assert result["error_count"] >= 1


def test_truth_events_grammar_rejects_optional_later_event_for_step4() -> None:
    row = _base_event_row()
    row["EventType"] = "DEATH"
    df = pd.DataFrame([row])

    result = validate_truth_events_dataframe(df)
    assert result["valid"] is False
    assert any("Optional-later event type" in err["error"] for err in result["errors"])


def test_truth_event_grammar_descriptor_includes_minimum_vocabulary() -> None:
    grammar = get_truth_event_grammar()
    assert grammar["active_event_types"] == ["MOVE", "COHABIT", "BIRTH", "DIVORCE", "LEAVE_HOME"]
