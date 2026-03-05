from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

ACTIVE_EVENT_TYPES: tuple[str, ...] = (
    "MOVE",
    "COHABIT",
    "BIRTH",
    "DIVORCE",
    "LEAVE_HOME",
)

OPTIONAL_LATER_EVENT_TYPES: tuple[str, ...] = (
    "DEATH",
    "NAME_CHANGE",
    "ADOPTION",
)

COHABIT_MODES: tuple[str, ...] = (
    "move_to_A",
    "move_to_B",
    "new_address",
)

CUSTODY_MODES: tuple[str, ...] = (
    "joint",
    "parent_a_primary",
    "parent_b_primary",
    "split",
)

TRUTH_EVENTS_REQUIRED_COLUMNS: tuple[str, ...] = (
    "EventKey",
    "EventType",
    "EventDate",
    "SubjectPersonKey",
    "SubjectHouseholdKey",
    "FromAddressKey",
    "ToAddressKey",
    "PersonKeyA",
    "PersonKeyB",
    "NewHouseholdKey",
    "CohabitMode",
    "ChildPersonKey",
    "Parent1PersonKey",
    "Parent2PersonKey",
    "CustodyMode",
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def _non_empty(value: Any) -> bool:
    return bool(_text(value))


def get_truth_event_grammar() -> dict[str, Any]:
    return {
        "active_event_types": list(ACTIVE_EVENT_TYPES),
        "optional_later_event_types": list(OPTIONAL_LATER_EVENT_TYPES),
        "cohabit_modes": list(COHABIT_MODES),
        "custody_modes": list(CUSTODY_MODES),
        "truth_events_required_columns": list(TRUTH_EVENTS_REQUIRED_COLUMNS),
        "event_signatures": {
            "MOVE": {
                "required_any_of": [["SubjectPersonKey", "SubjectHouseholdKey"]],
                "required": ["FromAddressKey", "ToAddressKey"],
            },
            "COHABIT": {
                "required": ["PersonKeyA", "PersonKeyB", "NewHouseholdKey", "CohabitMode"],
                "enum": {"CohabitMode": list(COHABIT_MODES)},
            },
            "BIRTH": {
                "required": ["Parent1PersonKey", "ChildPersonKey"],
                "optional": ["Parent2PersonKey"],
            },
            "DIVORCE": {
                "required": ["PersonKeyA", "PersonKeyB", "CustodyMode"],
                "enum": {"CustodyMode": list(CUSTODY_MODES)},
            },
            "LEAVE_HOME": {
                "required": ["ChildPersonKey"],
            },
        },
    }


def validate_truth_events_dataframe(df: pd.DataFrame) -> dict[str, Any]:
    missing_columns = [col for col in TRUTH_EVENTS_REQUIRED_COLUMNS if col not in df.columns]
    if missing_columns:
        return {
            "valid": False,
            "missing_columns": missing_columns,
            "errors": [],
            "error_count": 0,
        }

    errors: list[dict[str, Any]] = []
    allowed_types = set(ACTIVE_EVENT_TYPES)
    optional_types = set(OPTIONAL_LATER_EVENT_TYPES)
    cohabit_modes = set(COHABIT_MODES)
    custody_modes = set(CUSTODY_MODES)

    for idx, row in df.iterrows():
        event_key = _text(row.get("EventKey"))
        event_type_raw = _text(row.get("EventType"))
        event_type = event_type_raw.upper()
        event_date = _text(row.get("EventDate"))

        if not event_key:
            errors.append({"row_index": int(idx), "field": "EventKey", "error": "EventKey is required"})
        if not event_date:
            errors.append({"row_index": int(idx), "field": "EventDate", "error": "EventDate is required"})
        if not event_type:
            errors.append({"row_index": int(idx), "field": "EventType", "error": "EventType is required"})
            continue
        if event_type not in allowed_types:
            if event_type in optional_types:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "EventType",
                        "error": f"Optional-later event type {event_type} is not enabled in Step-4 minimum grammar",
                    }
                )
            else:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "EventType",
                        "error": f"Unsupported EventType {event_type}",
                    }
                )
            continue

        if event_type == "MOVE":
            has_person = _non_empty(row.get("SubjectPersonKey"))
            has_household = _non_empty(row.get("SubjectHouseholdKey"))
            if not has_person and not has_household:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "SubjectPersonKey/SubjectHouseholdKey",
                        "error": "MOVE requires SubjectPersonKey or SubjectHouseholdKey",
                    }
                )
            from_address = _text(row.get("FromAddressKey"))
            to_address = _text(row.get("ToAddressKey"))
            if not from_address:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "FromAddressKey",
                        "error": "MOVE requires FromAddressKey",
                    }
                )
            if not to_address:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "ToAddressKey",
                        "error": "MOVE requires ToAddressKey",
                    }
                )
            if from_address and to_address and from_address == to_address:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "FromAddressKey/ToAddressKey",
                        "error": "MOVE requires from and to addresses to differ",
                    }
                )

        elif event_type == "COHABIT":
            person_a = _text(row.get("PersonKeyA"))
            person_b = _text(row.get("PersonKeyB"))
            new_household = _text(row.get("NewHouseholdKey"))
            mode = _text(row.get("CohabitMode"))
            if not person_a:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "PersonKeyA",
                        "error": "COHABIT requires PersonKeyA",
                    }
                )
            if not person_b:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "PersonKeyB",
                        "error": "COHABIT requires PersonKeyB",
                    }
                )
            if person_a and person_b and person_a == person_b:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "PersonKeyA/PersonKeyB",
                        "error": "COHABIT requires PersonKeyA and PersonKeyB to differ",
                    }
                )
            if not new_household:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "NewHouseholdKey",
                        "error": "COHABIT requires NewHouseholdKey",
                    }
                )
            if not mode:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "CohabitMode",
                        "error": "COHABIT requires CohabitMode",
                    }
                )
            elif mode not in cohabit_modes:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "CohabitMode",
                        "error": f"CohabitMode must be one of {sorted(cohabit_modes)}",
                    }
                )

        elif event_type == "BIRTH":
            parent1 = _text(row.get("Parent1PersonKey"))
            parent2 = _text(row.get("Parent2PersonKey"))
            child = _text(row.get("ChildPersonKey"))
            if not parent1:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "Parent1PersonKey",
                        "error": "BIRTH requires Parent1PersonKey",
                    }
                )
            if not child:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "ChildPersonKey",
                        "error": "BIRTH requires ChildPersonKey",
                    }
                )
            if parent1 and child and parent1 == child:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "Parent1PersonKey/ChildPersonKey",
                        "error": "BIRTH requires Parent1PersonKey and ChildPersonKey to differ",
                    }
                )
            if parent2 and parent1 and parent2 == parent1:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "Parent1PersonKey/Parent2PersonKey",
                        "error": "BIRTH requires distinct parents when Parent2PersonKey is provided",
                    }
                )

        elif event_type == "DIVORCE":
            person_a = _text(row.get("PersonKeyA"))
            person_b = _text(row.get("PersonKeyB"))
            custody = _text(row.get("CustodyMode"))
            if not person_a:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "PersonKeyA",
                        "error": "DIVORCE requires PersonKeyA",
                    }
                )
            if not person_b:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "PersonKeyB",
                        "error": "DIVORCE requires PersonKeyB",
                    }
                )
            if person_a and person_b and person_a == person_b:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "PersonKeyA/PersonKeyB",
                        "error": "DIVORCE requires PersonKeyA and PersonKeyB to differ",
                    }
                )
            if not custody:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "CustodyMode",
                        "error": "DIVORCE requires CustodyMode",
                    }
                )
            elif custody not in custody_modes:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "CustodyMode",
                        "error": f"CustodyMode must be one of {sorted(custody_modes)}",
                    }
                )

        elif event_type == "LEAVE_HOME":
            child = _text(row.get("ChildPersonKey"))
            if not child:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "ChildPersonKey",
                        "error": "LEAVE_HOME requires ChildPersonKey",
                    }
                )

    return {
        "valid": not errors,
        "missing_columns": [],
        "errors": errors,
        "error_count": len(errors),
    }


def validate_truth_events_parquet(path: Path) -> dict[str, Any]:
    df = pd.read_parquet(path)
    result = validate_truth_events_dataframe(df)
    result["path"] = str(path)
    return result
