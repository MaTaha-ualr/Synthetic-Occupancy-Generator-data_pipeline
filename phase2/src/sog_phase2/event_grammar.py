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
    "DEATH",
    "NAME_CHANGE",
    "ADOPTION",
)

OPTIONAL_LATER_EVENT_TYPES: tuple[str, ...] = ()

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

TRUTH_EVENTS_LIFECYCLE_COLUMNS: tuple[str, ...] = (
    "PreviousFirstName",
    "PreviousMiddleName",
    "PreviousLastName",
    "PreviousFullName",
    "NewFirstName",
    "NewMiddleName",
    "NewLastName",
    "NewFullName",
    "NameChangeReason",
    "PreviousParent1PersonKey",
    "PreviousParent2PersonKey",
    "AdoptiveParent1PersonKey",
    "AdoptiveParent2PersonKey",
)

TRUTH_EVENTS_OUTPUT_COLUMNS: tuple[str, ...] = (
    *TRUTH_EVENTS_REQUIRED_COLUMNS,
    *TRUTH_EVENTS_LIFECYCLE_COLUMNS,
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
        "truth_events_lifecycle_columns": list(TRUTH_EVENTS_LIFECYCLE_COLUMNS),
        "truth_events_output_columns": list(TRUTH_EVENTS_OUTPUT_COLUMNS),
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
            "DEATH": {
                "required": ["SubjectPersonKey"],
                "optional": ["SubjectHouseholdKey", "FromAddressKey"],
            },
            "NAME_CHANGE": {
                "required": [
                    "SubjectPersonKey",
                    "PreviousFullName",
                    "NewFullName",
                    "NameChangeReason",
                ],
                "optional": [
                    "PreviousFirstName",
                    "PreviousMiddleName",
                    "PreviousLastName",
                    "NewFirstName",
                    "NewMiddleName",
                    "NewLastName",
                ],
            },
            "ADOPTION": {
                "required": [
                    "ChildPersonKey",
                    "AdoptiveParent1PersonKey",
                    "NewHouseholdKey",
                ],
                "optional": [
                    "PreviousParent1PersonKey",
                    "PreviousParent2PersonKey",
                    "AdoptiveParent2PersonKey",
                    "FromAddressKey",
                    "ToAddressKey",
                ],
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
                        "error": f"Event type {event_type} is not enabled in the active truth grammar",
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

        elif event_type == "DEATH":
            subject = _text(row.get("SubjectPersonKey"))
            if not subject:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "SubjectPersonKey",
                        "error": "DEATH requires SubjectPersonKey",
                    }
                )

        elif event_type == "NAME_CHANGE":
            subject = _text(row.get("SubjectPersonKey"))
            previous_full = _text(row.get("PreviousFullName"))
            new_full = _text(row.get("NewFullName"))
            reason = _text(row.get("NameChangeReason"))
            if not subject:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "SubjectPersonKey",
                        "error": "NAME_CHANGE requires SubjectPersonKey",
                    }
                )
            if not previous_full:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "PreviousFullName",
                        "error": "NAME_CHANGE requires PreviousFullName",
                    }
                )
            if not new_full:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "NewFullName",
                        "error": "NAME_CHANGE requires NewFullName",
                    }
                )
            if previous_full and new_full and previous_full == new_full:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "PreviousFullName/NewFullName",
                        "error": "NAME_CHANGE requires previous and new full names to differ",
                    }
                )
            if not reason:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "NameChangeReason",
                        "error": "NAME_CHANGE requires NameChangeReason",
                    }
                )

        elif event_type == "ADOPTION":
            child = _text(row.get("ChildPersonKey"))
            parent1 = _text(row.get("AdoptiveParent1PersonKey") or row.get("Parent1PersonKey"))
            parent2 = _text(row.get("AdoptiveParent2PersonKey") or row.get("Parent2PersonKey"))
            household = _text(row.get("NewHouseholdKey"))
            if not child:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "ChildPersonKey",
                        "error": "ADOPTION requires ChildPersonKey",
                    }
                )
            if not parent1:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "AdoptiveParent1PersonKey",
                        "error": "ADOPTION requires AdoptiveParent1PersonKey",
                    }
                )
            if child and parent1 and child == parent1:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "ChildPersonKey/AdoptiveParent1PersonKey",
                        "error": "ADOPTION requires child and adoptive parent to differ",
                    }
                )
            if parent2 and parent1 and parent2 == parent1:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "AdoptiveParent1PersonKey/AdoptiveParent2PersonKey",
                        "error": "ADOPTION requires distinct adoptive parents when AdoptiveParent2PersonKey is provided",
                    }
                )
            if parent2 and child and parent2 == child:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "ChildPersonKey/AdoptiveParent2PersonKey",
                        "error": "ADOPTION requires child and second adoptive parent to differ",
                    }
                )
            if not household:
                errors.append(
                    {
                        "row_index": int(idx),
                        "field": "NewHouseholdKey",
                        "error": "ADOPTION requires NewHouseholdKey",
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
