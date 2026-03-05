from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .event_grammar import ACTIVE_EVENT_TYPES


@dataclass(frozen=True)
class ConstraintConfig:
    min_marriage_age: int = 18
    max_partner_age_gap: int | None = 25
    partner_age_gap_distribution: dict[int, float] | None = None
    fertility_age_min: int = 15
    fertility_age_max: int = 49
    allow_underage_marriage: bool = False
    allow_child_lives_alone: bool = False
    enforce_non_overlapping_residence_intervals: bool = True


def get_constraints_schema() -> dict[str, Any]:
    return {
        "defaults": asdict(ConstraintConfig()),
        "fields": {
            "min_marriage_age": "integer",
            "max_partner_age_gap": "integer|null",
            "partner_age_gap_distribution": "mapping[int_gap -> weight]|null",
            "fertility_age_range": {"min": "integer", "max": "integer"},
            "allow_underage_marriage": "boolean",
            "allow_child_lives_alone": "boolean",
            "enforce_non_overlapping_residence_intervals": "boolean",
        },
        "notes": [
            "max_partner_age_gap and partner_age_gap_distribution are alternatives; both may be provided.",
            "When partner_age_gap_distribution is provided, its maximum key is treated as effective max gap.",
            "These switches are novelty levers for controlled unrealistic edge-case stress tests.",
        ],
    }


def _parse_partner_gap_distribution(raw: Any) -> dict[int, float] | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("constraints.partner_age_gap_distribution must be a mapping or null")

    parsed: dict[int, float] = {}
    for key, value in raw.items():
        gap = int(key)
        if gap < 0:
            raise ValueError("constraints.partner_age_gap_distribution keys must be >= 0")
        weight = float(value)
        if weight < 0:
            raise ValueError("constraints.partner_age_gap_distribution values must be >= 0")
        parsed[gap] = weight

    if not parsed:
        raise ValueError("constraints.partner_age_gap_distribution cannot be empty")
    if sum(parsed.values()) <= 0:
        raise ValueError("constraints.partner_age_gap_distribution must have a positive weight sum")
    return parsed


def parse_constraints_config(raw: dict[str, Any] | None) -> ConstraintConfig:
    cfg = raw or {}
    if not isinstance(cfg, dict):
        raise ValueError("scenario.constraints must be a mapping")

    fertility_cfg = cfg.get("fertility_age_range", {})
    if fertility_cfg is None:
        fertility_cfg = {}
    if not isinstance(fertility_cfg, dict):
        raise ValueError("constraints.fertility_age_range must be a mapping")

    gap_distribution = _parse_partner_gap_distribution(cfg.get("partner_age_gap_distribution"))

    max_partner_age_gap_raw = cfg.get("max_partner_age_gap", 25)
    if max_partner_age_gap_raw is None:
        max_partner_age_gap = None
    else:
        max_partner_age_gap = int(max_partner_age_gap_raw)

    result = ConstraintConfig(
        min_marriage_age=int(cfg.get("min_marriage_age", 18)),
        max_partner_age_gap=max_partner_age_gap,
        partner_age_gap_distribution=gap_distribution,
        fertility_age_min=int(fertility_cfg.get("min", 15)),
        fertility_age_max=int(fertility_cfg.get("max", 49)),
        allow_underage_marriage=bool(cfg.get("allow_underage_marriage", False)),
        allow_child_lives_alone=bool(cfg.get("allow_child_lives_alone", False)),
        enforce_non_overlapping_residence_intervals=bool(
            cfg.get("enforce_non_overlapping_residence_intervals", True)
        ),
    )
    validate_constraints_config(result)
    return result


def validate_constraints_config(config: ConstraintConfig) -> None:
    if config.min_marriage_age < 0 or config.min_marriage_age > 120:
        raise ValueError("constraints.min_marriage_age must be between 0 and 120")
    if config.max_partner_age_gap is not None and config.max_partner_age_gap < 0:
        raise ValueError("constraints.max_partner_age_gap must be >= 0 when provided")
    if config.fertility_age_min < 0 or config.fertility_age_min > 120:
        raise ValueError("constraints.fertility_age_range.min must be between 0 and 120")
    if config.fertility_age_max < 0 or config.fertility_age_max > 120:
        raise ValueError("constraints.fertility_age_range.max must be between 0 and 120")
    if config.fertility_age_min > config.fertility_age_max:
        raise ValueError(
            "constraints.fertility_age_range.min must be <= constraints.fertility_age_range.max"
        )


def _to_age_map(truth_people_df: pd.DataFrame) -> dict[str, int]:
    age_map: dict[str, int] = {}
    if "PersonKey" not in truth_people_df.columns or "Age" not in truth_people_df.columns:
        return age_map

    for _, row in truth_people_df.iterrows():
        person_key = str(row.get("PersonKey", "")).strip()
        if not person_key:
            continue
        age_raw = row.get("Age")
        if pd.isna(age_raw):
            continue
        try:
            age_map[person_key] = int(float(age_raw))
        except (TypeError, ValueError):
            continue
    return age_map


def _parse_date(value: Any) -> date | None:
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _effective_max_partner_gap(config: ConstraintConfig) -> int | None:
    if config.partner_age_gap_distribution:
        return max(config.partner_age_gap_distribution.keys())
    return config.max_partner_age_gap


def validate_constraints_against_truth(
    *,
    truth_people_df: pd.DataFrame,
    truth_events_df: pd.DataFrame,
    truth_residence_history_df: pd.DataFrame,
    config: ConstraintConfig,
) -> dict[str, Any]:
    age_map = _to_age_map(truth_people_df)
    violations: list[dict[str, Any]] = []

    active_events = set(ACTIVE_EVENT_TYPES)
    max_gap = _effective_max_partner_gap(config)

    for row_index, row in truth_events_df.iterrows():
        event_type = str(row.get("EventType", "")).strip().upper()
        if event_type not in active_events:
            continue

        event_key = str(row.get("EventKey", "")).strip()
        event_ref = event_key or f"row_{row_index}"

        if event_type in {"COHABIT", "DIVORCE"}:
            person_a = str(row.get("PersonKeyA", "")).strip()
            person_b = str(row.get("PersonKeyB", "")).strip()
            age_a = age_map.get(person_a)
            age_b = age_map.get(person_b)

            if not config.allow_underage_marriage:
                if age_a is not None and age_a < config.min_marriage_age:
                    violations.append(
                        {
                            "rule_id": "min_marriage_age",
                            "event_key": event_ref,
                            "event_type": event_type,
                            "details": f"PersonKeyA age {age_a} < min_marriage_age {config.min_marriage_age}",
                        }
                    )
                if age_b is not None and age_b < config.min_marriage_age:
                    violations.append(
                        {
                            "rule_id": "min_marriage_age",
                            "event_key": event_ref,
                            "event_type": event_type,
                            "details": f"PersonKeyB age {age_b} < min_marriage_age {config.min_marriage_age}",
                        }
                    )

            if max_gap is not None and age_a is not None and age_b is not None:
                gap = abs(age_a - age_b)
                if gap > max_gap:
                    violations.append(
                        {
                            "rule_id": "max_partner_age_gap",
                            "event_key": event_ref,
                            "event_type": event_type,
                            "details": f"Partner age gap {gap} > allowed {max_gap}",
                        }
                    )

        if event_type == "BIRTH":
            parent1 = str(row.get("Parent1PersonKey", "")).strip()
            parent_age = age_map.get(parent1)
            if parent_age is not None:
                if parent_age < config.fertility_age_min or parent_age > config.fertility_age_max:
                    violations.append(
                        {
                            "rule_id": "fertility_age_range",
                            "event_key": event_ref,
                            "event_type": event_type,
                            "details": (
                                f"Parent1PersonKey age {parent_age} outside fertility range "
                                f"[{config.fertility_age_min}, {config.fertility_age_max}]"
                            ),
                        }
                    )

        if event_type == "LEAVE_HOME" and not config.allow_child_lives_alone:
            child_key = str(row.get("ChildPersonKey", "")).strip()
            child_age = age_map.get(child_key)
            if child_age is not None and child_age < 18:
                violations.append(
                    {
                        "rule_id": "allow_child_lives_alone",
                        "event_key": event_ref,
                        "event_type": event_type,
                        "details": f"Child age {child_age} is < 18 but allow_child_lives_alone is false",
                    }
                )

    if config.enforce_non_overlapping_residence_intervals:
        needed = {"PersonKey", "ResidenceStartDate", "ResidenceEndDate"}
        if needed.issubset(truth_residence_history_df.columns):
            sorted_df = truth_residence_history_df.copy()
            sorted_df["_start"] = sorted_df["ResidenceStartDate"].map(_parse_date)
            sorted_df["_end"] = sorted_df["ResidenceEndDate"].map(_parse_date)
            sorted_df = sorted_df.sort_values(by=["PersonKey", "_start"], na_position="last")

            for person_key, group in sorted_df.groupby("PersonKey", dropna=False):
                person = str(person_key).strip()
                prev_end: date | None = None
                prev_start: date | None = None
                for _, row in group.iterrows():
                    start = row["_start"]
                    end = row["_end"]
                    if start is None:
                        violations.append(
                            {
                                "rule_id": "residence_date_parse",
                                "event_key": person or "unknown_person",
                                "event_type": "RESIDENCE_HISTORY",
                                "details": "ResidenceStartDate is missing or invalid",
                            }
                        )
                        continue
                    if end is not None and end < start:
                        violations.append(
                            {
                                "rule_id": "residence_interval_order",
                                "event_key": person or "unknown_person",
                                "event_type": "RESIDENCE_HISTORY",
                                "details": "ResidenceEndDate is before ResidenceStartDate",
                            }
                        )

                    if prev_start is not None:
                        effective_prev_end = prev_end or date.max
                        if start <= effective_prev_end:
                            violations.append(
                                {
                                    "rule_id": "non_overlapping_residence_intervals",
                                    "event_key": person or "unknown_person",
                                    "event_type": "RESIDENCE_HISTORY",
                                    "details": "Residence intervals overlap for the same PersonKey",
                                }
                            )

                    prev_start = start
                    prev_end = end

    return {
        "valid": not violations,
        "violation_count": len(violations),
        "violations": violations,
        "config": asdict(config),
    }


def validate_constraints_for_run(*, run_dir: Path, config: ConstraintConfig) -> dict[str, Any]:
    truth_people = pd.read_parquet(run_dir / "truth_people.parquet")
    truth_events = pd.read_parquet(run_dir / "truth_events.parquet")
    truth_residence_history = pd.read_parquet(run_dir / "truth_residence_history.parquet")
    return validate_constraints_against_truth(
        truth_people_df=truth_people,
        truth_events_df=truth_events,
        truth_residence_history_df=truth_residence_history,
        config=config,
    )
