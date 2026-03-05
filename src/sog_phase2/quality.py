from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from .constraints import ConstraintConfig


@dataclass(frozen=True)
class QualityConfig:
    household_size_min: int = 1
    household_size_max: int = 12


def get_quality_schema() -> dict[str, Any]:
    defaults = {
        "household_size_range": {
            "min": QualityConfig().household_size_min,
            "max": QualityConfig().household_size_max,
        }
    }
    return {
        "defaults": defaults,
        "fields": {
            "household_size_range": {
                "min": "integer >= 1",
                "max": "integer >= min",
            }
        },
    }


def parse_quality_config(raw: dict[str, Any] | None) -> QualityConfig:
    cfg = raw or {}
    if not isinstance(cfg, dict):
        raise ValueError("scenario.quality must be a mapping when provided")
    size_cfg = cfg.get("household_size_range", {})
    if size_cfg is None:
        size_cfg = {}
    if not isinstance(size_cfg, dict):
        raise ValueError("quality.household_size_range must be a mapping")
    quality = QualityConfig(
        household_size_min=int(size_cfg.get("min", 1)),
        household_size_max=int(size_cfg.get("max", 12)),
    )
    if quality.household_size_min < 1:
        raise ValueError("quality.household_size_range.min must be >= 1")
    if quality.household_size_max < quality.household_size_min:
        raise ValueError("quality.household_size_range.max must be >= min")
    return quality


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def _parse_date(value: Any) -> date | None:
    parsed = pd.to_datetime(_text(value), errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _age_map(truth_people_df: pd.DataFrame) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for _, row in truth_people_df.iterrows():
        person = _text(row.get("PersonKey"))
        if not person:
            continue
        try:
            mapping[person] = int(float(row.get("Age", 0)))
        except (TypeError, ValueError):
            continue
    return mapping


def _interval_issue_count(
    df: pd.DataFrame,
    *,
    entity_col: str,
    start_col: str,
    end_col: str,
) -> int:
    if df.empty:
        return 0
    working = df.copy()
    working["_start"] = pd.to_datetime(working[start_col], errors="coerce")
    working["_end"] = pd.to_datetime(working[end_col], errors="coerce")
    working = working.sort_values(by=[entity_col, "_start"], kind="mergesort")

    issues = 0
    for _, group in working.groupby(entity_col, dropna=False):
        prev_end: pd.Timestamp | None = None
        prev_seen = False
        for _, row in group.iterrows():
            start = row["_start"]
            end = row["_end"]
            if pd.isna(start):
                issues += 1
                continue
            if not pd.isna(end) and end < start:
                issues += 1
            if prev_seen:
                effective_prev_end = prev_end if prev_end is not None else pd.Timestamp.max
                if start <= effective_prev_end:
                    issues += 1
            prev_seen = True
            prev_end = None if pd.isna(end) else end
    return issues


def _event_age_violations(
    *,
    truth_events_df: pd.DataFrame,
    age_map: dict[str, int],
    constraints_config: ConstraintConfig,
) -> dict[str, Any]:
    invalid_rows: list[dict[str, Any]] = []
    rule_counts: dict[str, int] = defaultdict(int)
    enabled = {
        "min_marriage_age": not constraints_config.allow_underage_marriage,
        "fertility_age_range": True,
        "leave_home_adult_check": not constraints_config.allow_child_lives_alone,
    }

    for _, row in truth_events_df.iterrows():
        event_type = _text(row.get("EventType")).upper()
        event_key = _text(row.get("EventKey"))
        if event_type in {"COHABIT", "DIVORCE"} and enabled["min_marriage_age"]:
            for field in ("PersonKeyA", "PersonKeyB"):
                person = _text(row.get(field))
                if not person:
                    continue
                age = age_map.get(person)
                if age is None:
                    continue
                if age < constraints_config.min_marriage_age:
                    rule_counts["min_marriage_age"] += 1
                    invalid_rows.append(
                        {
                            "event_key": event_key,
                            "event_type": event_type,
                            "rule": "min_marriage_age",
                            "person_key": person,
                            "age": age,
                        }
                    )

        if event_type == "BIRTH":
            parent1 = _text(row.get("Parent1PersonKey"))
            if parent1:
                age = age_map.get(parent1)
                if age is not None:
                    if age < constraints_config.fertility_age_min or age > constraints_config.fertility_age_max:
                        rule_counts["fertility_age_range"] += 1
                        invalid_rows.append(
                            {
                                "event_key": event_key,
                                "event_type": event_type,
                                "rule": "fertility_age_range",
                                "person_key": parent1,
                                "age": age,
                            }
                        )

        if event_type == "LEAVE_HOME" and enabled["leave_home_adult_check"]:
            child = _text(row.get("ChildPersonKey"))
            if child:
                age = age_map.get(child)
                if age is not None and age < 18:
                    rule_counts["leave_home_adult_check"] += 1
                    invalid_rows.append(
                        {
                            "event_key": event_key,
                            "event_type": event_type,
                            "rule": "leave_home_adult_check",
                            "person_key": child,
                            "age": age,
                        }
                    )

    return {
        "checks_enabled": enabled,
        "invalid_event_age_count": len(invalid_rows),
        "invalid_event_age_by_rule": dict(rule_counts),
        "preview": invalid_rows[:50],
    }


def _household_size_metrics(
    memberships_df: pd.DataFrame,
    quality_config: QualityConfig,
) -> dict[str, Any]:
    if memberships_df.empty:
        return {
            "household_size_range_config": {
                "min": quality_config.household_size_min,
                "max": quality_config.household_size_max,
            },
            "households_evaluated": 0,
            "size_points_evaluated": 0,
            "violating_households": 0,
            "violation_points": 0,
            "peak_size_summary": {"min": 0, "mean": 0.0, "max": 0},
            "within_config_constraints": True,
        }

    timeline_changes: dict[str, dict[date, int]] = defaultdict(lambda: defaultdict(int))
    for _, row in memberships_df.iterrows():
        household = _text(row.get("HouseholdKey"))
        start = _parse_date(row.get("MembershipStartDate"))
        end = _parse_date(row.get("MembershipEndDate"))
        if not household or start is None:
            continue
        timeline_changes[household][start] += 1
        if end is not None:
            timeline_changes[household][end + timedelta(days=1)] -= 1

    violation_households = 0
    violation_points = 0
    size_points = 0
    peaks: list[int] = []

    for household, changes in timeline_changes.items():
        running = 0
        peak = 0
        any_violation = False
        for change_date in sorted(changes.keys()):
            running += int(changes[change_date])
            if running <= 0:
                continue
            size_points += 1
            peak = max(peak, running)
            if running < quality_config.household_size_min or running > quality_config.household_size_max:
                any_violation = True
                violation_points += 1
        peaks.append(peak)
        if any_violation:
            violation_households += 1

    if not peaks:
        peaks = [0]

    return {
        "household_size_range_config": {
            "min": quality_config.household_size_min,
            "max": quality_config.household_size_max,
        },
        "households_evaluated": len(timeline_changes),
        "size_points_evaluated": int(size_points),
        "violating_households": int(violation_households),
        "violation_points": int(violation_points),
        "peak_size_summary": {
            "min": int(min(peaks)),
            "mean": float(np.mean(peaks)),
            "max": int(max(peaks)),
        },
        "within_config_constraints": violation_points == 0,
    }


def _memberships_by_household(memberships_df: pd.DataFrame) -> dict[str, list[tuple[str, date, date | None]]]:
    mapping: dict[str, list[tuple[str, date, date | None]]] = defaultdict(list)
    for _, row in memberships_df.iterrows():
        household = _text(row.get("HouseholdKey"))
        person = _text(row.get("PersonKey"))
        start = _parse_date(row.get("MembershipStartDate"))
        end = _parse_date(row.get("MembershipEndDate"))
        if not household or not person or start is None:
            continue
        mapping[household].append((person, start, end))
    return mapping


def _moves_per_person_metrics(
    *,
    truth_people_df: pd.DataFrame,
    truth_events_df: pd.DataFrame,
    memberships_df: pd.DataFrame,
) -> dict[str, Any]:
    person_keys = truth_people_df["PersonKey"].astype(str).map(_text).tolist()
    move_counts: dict[str, int] = {key: 0 for key in person_keys if key}
    members_by_household = _memberships_by_household(memberships_df)

    move_events = truth_events_df[truth_events_df["EventType"].astype(str).str.upper() == "MOVE"]
    for _, row in move_events.iterrows():
        subject_person = _text(row.get("SubjectPersonKey"))
        if subject_person:
            move_counts[subject_person] = move_counts.get(subject_person, 0) + 1
            continue
        subject_household = _text(row.get("SubjectHouseholdKey"))
        event_date = _parse_date(row.get("EventDate"))
        if not subject_household or event_date is None:
            continue
        for person, start, end in members_by_household.get(subject_household, []):
            if start <= event_date and (end is None or event_date <= end):
                move_counts[person] = move_counts.get(person, 0) + 1

    values = list(move_counts.values())
    histogram: dict[str, int] = defaultdict(int)
    for count in values:
        histogram[str(int(count))] += 1

    if not values:
        values = [0]
    moved_people = sum(1 for count in values if count > 0)

    return {
        "people_evaluated": len(values),
        "total_moves": int(sum(values)),
        "moved_people": int(moved_people),
        "moved_people_pct": float((moved_people / len(values)) * 100.0 if values else 0.0),
        "mean_moves_per_person": float(np.mean(values)),
        "max_moves_for_person": int(max(values)),
        "distribution": dict(sorted(histogram.items(), key=lambda item: int(item[0]))),
    }


def _household_type_shares(truth_households_df: pd.DataFrame) -> dict[str, Any]:
    if truth_households_df.empty or "HouseholdType" not in truth_households_df.columns:
        return {"shares_pct": {}, "count": 0}
    counts = (
        truth_households_df["HouseholdType"]
        .astype(str)
        .str.strip()
        .replace("", "unknown")
        .value_counts()
        .to_dict()
    )
    total = max(1, int(sum(counts.values())))
    shares = {key: float((value / total) * 100.0) for key, value in counts.items()}
    return {"shares_pct": shares, "count": int(total)}


def _record_person_counts(crosswalk_df: pd.DataFrame, record_col: str) -> tuple[dict[str, int], dict[str, str], int]:
    person_counts: dict[str, set[str]] = defaultdict(set)
    record_to_person: dict[str, str] = {}
    ambiguous = 0
    for _, row in crosswalk_df.iterrows():
        person = _text(row.get("PersonKey"))
        record = _text(row.get(record_col))
        if not person or not record:
            continue
        person_counts[person].add(record)
        existing = record_to_person.get(record)
        if existing is None:
            record_to_person[record] = person
        elif existing != person:
            ambiguous += 1
    return ({key: len(value) for key, value in person_counts.items()}, record_to_person, ambiguous)


def _address_map_for_snapshot(residence_df: pd.DataFrame, snapshot: date) -> dict[str, str]:
    working = residence_df.copy()
    if working.empty:
        return {}
    working["PersonKey"] = working["PersonKey"].astype(str).map(_text)
    working["_start"] = pd.to_datetime(working["ResidenceStartDate"], errors="coerce")
    working["_end"] = pd.to_datetime(working["ResidenceEndDate"], errors="coerce")
    snap = pd.Timestamp(snapshot)
    active = working[
        (working["_start"].notna())
        & (working["_start"] <= snap)
        & ((working["_end"].isna()) | (working["_end"] >= snap))
    ].copy()
    active = active.sort_values(by=["PersonKey", "_start"], kind="mergesort")
    if active.empty:
        earlier = working[(working["_start"].notna()) & (working["_start"] <= snap)].copy()
        earlier = earlier.sort_values(by=["PersonKey", "_start"], kind="mergesort")
        return (
            earlier.groupby("PersonKey", as_index=True)["AddressKey"]
            .last()
            .astype(str)
            .map(_text)
            .to_dict()
        )
    return (
        active.groupby("PersonKey", as_index=True)["AddressKey"]
        .last()
        .astype(str)
        .map(_text)
        .to_dict()
    )


def _dataset_drift_metrics(
    *,
    dataset_df: pd.DataFrame,
    record_col: str,
    record_to_person: dict[str, str],
    truth_people_df: pd.DataFrame,
    truth_residence_history_df: pd.DataFrame,
) -> dict[str, Any]:
    if dataset_df.empty:
        return {
            "rows": 0,
            "mapped_rows": 0,
            "unmapped_rows": 0,
            "name_drift_rate_pct": 0.0,
            "address_drift_rate_pct": 0.0,
            "phone_drift_rate_pct": 0.0,
            "phone_available": False,
        }

    truth_by_person = {
        _text(row.get("PersonKey")): row
        for _, row in truth_people_df.iterrows()
        if _text(row.get("PersonKey"))
    }
    phone_available = "Phone" in truth_people_df.columns and "Phone" in dataset_df.columns
    address_cache: dict[str, dict[str, str]] = {}

    mapped_rows = 0
    unmapped_rows = 0
    name_drift = 0
    address_drift = 0
    phone_drift = 0
    phone_denominator = 0

    for _, row in dataset_df.iterrows():
        record_key = _text(row.get(record_col))
        person = record_to_person.get(record_key, "")
        if not person:
            unmapped_rows += 1
            continue
        truth_row = truth_by_person.get(person)
        if truth_row is None:
            unmapped_rows += 1
            continue
        mapped_rows += 1

        first_match = _text(row.get("FirstName")).lower() == _text(truth_row.get("FormalFirstName")).lower()
        middle_match = _text(row.get("MiddleName")).lower() == _text(truth_row.get("MiddleName")).lower()
        last_match = _text(row.get("LastName")).lower() == _text(truth_row.get("LastName")).lower()
        if not (first_match and middle_match and last_match):
            name_drift += 1

        snapshot_text = _text(row.get("SourceSnapshotDate"))
        snapshot_date = _parse_date(snapshot_text)
        expected_address = ""
        if snapshot_date is not None:
            cache_key = snapshot_date.isoformat()
            if cache_key not in address_cache:
                address_cache[cache_key] = _address_map_for_snapshot(truth_residence_history_df, snapshot_date)
            expected_address = _text(address_cache[cache_key].get(person, ""))
        observed_address = _text(row.get("AddressKey"))
        if expected_address != observed_address:
            address_drift += 1

        if phone_available:
            expected_phone = _text(truth_row.get("Phone"))
            observed_phone = _text(row.get("Phone"))
            phone_denominator += 1
            if expected_phone != observed_phone:
                phone_drift += 1

    return {
        "rows": int(len(dataset_df)),
        "mapped_rows": int(mapped_rows),
        "unmapped_rows": int(unmapped_rows),
        "name_drift_count": int(name_drift),
        "name_drift_rate_pct": float((name_drift / mapped_rows) * 100.0 if mapped_rows else 0.0),
        "address_drift_count": int(address_drift),
        "address_drift_rate_pct": float((address_drift / mapped_rows) * 100.0 if mapped_rows else 0.0),
        "phone_available": bool(phone_available),
        "phone_drift_count": int(phone_drift) if phone_available else 0,
        "phone_drift_rate_pct": float((phone_drift / phone_denominator) * 100.0 if phone_denominator else 0.0),
    }


def compute_phase2_quality_report(
    *,
    truth_people_df: pd.DataFrame,
    truth_households_df: pd.DataFrame,
    truth_household_memberships_df: pd.DataFrame,
    truth_residence_history_df: pd.DataFrame,
    truth_events_df: pd.DataFrame,
    constraints_config: ConstraintConfig,
    quality_config: QualityConfig,
    dataset_a_df: pd.DataFrame | None = None,
    dataset_b_df: pd.DataFrame | None = None,
    truth_crosswalk_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    age_map = _age_map(truth_people_df)
    event_age = _event_age_violations(
        truth_events_df=truth_events_df,
        age_map=age_map,
        constraints_config=constraints_config,
    )
    residence_overlap_errors = _interval_issue_count(
        truth_residence_history_df,
        entity_col="PersonKey",
        start_col="ResidenceStartDate",
        end_col="ResidenceEndDate",
    )
    membership_overlap_errors = _interval_issue_count(
        truth_household_memberships_df,
        entity_col="PersonKey",
        start_col="MembershipStartDate",
        end_col="MembershipEndDate",
    )
    household_sizes = _household_size_metrics(
        truth_household_memberships_df,
        quality_config=quality_config,
    )

    truth_consistency = {
        "event_age_validation": event_age,
        "time_overlap_errors": {
            "residence_overlap_or_date_order": int(residence_overlap_errors),
            "household_membership_overlap_or_date_order": int(membership_overlap_errors),
            "no_time_overlap_errors": (residence_overlap_errors + membership_overlap_errors) == 0,
        },
        "household_size_constraints": household_sizes,
    }

    event_counts = (
        truth_events_df["EventType"].astype(str).str.upper().value_counts().to_dict()
        if "EventType" in truth_events_df.columns
        else {}
    )
    scenario_metrics = {
        "event_counts": {
            "couples_formed": int(event_counts.get("COHABIT", 0)),
            "divorces": int(event_counts.get("DIVORCE", 0)),
            "births": int(event_counts.get("BIRTH", 0)),
            "moves": int(event_counts.get("MOVE", 0)),
        },
        "moves_per_person_distribution": _moves_per_person_metrics(
            truth_people_df=truth_people_df,
            truth_events_df=truth_events_df,
            memberships_df=truth_household_memberships_df,
        ),
        "household_type_shares": _household_type_shares(truth_households_df),
    }

    er_metrics: dict[str, Any]
    if dataset_a_df is None or dataset_b_df is None or truth_crosswalk_df is None:
        er_metrics = {"available": False, "reason": "observed datasets not provided"}
    else:
        a_counts, a_record_to_person, a_ambiguous = _record_person_counts(truth_crosswalk_df, "A_RecordKey")
        b_counts, b_record_to_person, b_ambiguous = _record_person_counts(truth_crosswalk_df, "B_RecordKey")
        a_entities = set(a_counts.keys())
        b_entities = set(b_counts.keys())
        overlap_entities = a_entities & b_entities
        union_entities = a_entities | b_entities

        cardinality_counts = {
            "one_to_one": 0,
            "one_to_many": 0,
            "many_to_one": 0,
            "many_to_many": 0,
        }
        for person in overlap_entities:
            a_n = int(a_counts.get(person, 0))
            b_n = int(b_counts.get(person, 0))
            if a_n <= 1 and b_n <= 1:
                cardinality_counts["one_to_one"] += 1
            elif a_n <= 1 and b_n > 1:
                cardinality_counts["one_to_many"] += 1
            elif a_n > 1 and b_n <= 1:
                cardinality_counts["many_to_one"] += 1
            else:
                cardinality_counts["many_to_many"] += 1

        a_dup_rows = sum(max(0, count - 1) for count in a_counts.values())
        b_dup_rows = sum(max(0, count - 1) for count in b_counts.values())
        a_multi_entities = sum(1 for count in a_counts.values() if count > 1)
        b_multi_entities = sum(1 for count in b_counts.values() if count > 1)

        drift_a = _dataset_drift_metrics(
            dataset_df=dataset_a_df,
            record_col="A_RecordKey",
            record_to_person=a_record_to_person,
            truth_people_df=truth_people_df,
            truth_residence_history_df=truth_residence_history_df,
        )
        drift_b = _dataset_drift_metrics(
            dataset_df=dataset_b_df,
            record_col="B_RecordKey",
            record_to_person=b_record_to_person,
            truth_people_df=truth_people_df,
            truth_residence_history_df=truth_residence_history_df,
        )

        er_metrics = {
            "available": True,
            "cross_file_overlap": {
                "overlap_entities": int(len(overlap_entities)),
                "a_entities": int(len(a_entities)),
                "b_entities": int(len(b_entities)),
                "union_entities": int(len(union_entities)),
                "overlap_pct_of_union": float((len(overlap_entities) / len(union_entities)) * 100.0 if union_entities else 0.0),
                "overlap_pct_of_a": float((len(overlap_entities) / len(a_entities)) * 100.0 if a_entities else 0.0),
                "overlap_pct_of_b": float((len(overlap_entities) / len(b_entities)) * 100.0 if b_entities else 0.0),
            },
            "match_cardinality_achieved": {
                **cardinality_counts,
                "evaluated_overlap_entities": int(len(overlap_entities)),
            },
            "within_file_duplicate_rates": {
                "dataset_a": {
                    "row_count": int(len(dataset_a_df)),
                    "entity_count": int(len(a_entities)),
                    "duplicate_rows": int(a_dup_rows),
                    "duplicate_row_rate_pct": float((a_dup_rows / len(dataset_a_df)) * 100.0 if len(dataset_a_df) else 0.0),
                    "multi_record_entity_pct": float((a_multi_entities / len(a_entities)) * 100.0 if a_entities else 0.0),
                },
                "dataset_b": {
                    "row_count": int(len(dataset_b_df)),
                    "entity_count": int(len(b_entities)),
                    "duplicate_rows": int(b_dup_rows),
                    "duplicate_row_rate_pct": float((b_dup_rows / len(dataset_b_df)) * 100.0 if len(dataset_b_df) else 0.0),
                    "multi_record_entity_pct": float((b_multi_entities / len(b_entities)) * 100.0 if b_entities else 0.0),
                },
            },
            "attribute_drift_rates": {
                "dataset_a": drift_a,
                "dataset_b": drift_b,
            },
            "crosswalk_ambiguity": {
                "a_record_to_multiple_persons": int(a_ambiguous),
                "b_record_to_multiple_persons": int(b_ambiguous),
            },
        }

    return {
        "truth_consistency": truth_consistency,
        "scenario_metrics": scenario_metrics,
        "er_benchmark_metrics": er_metrics,
        "quality_config": asdict(quality_config),
    }
