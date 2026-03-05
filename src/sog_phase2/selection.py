from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SelectionConfig:
    sample_mode: str = "pct"
    sample_value: float = 100.0
    age_bins: tuple[str, ...] = ()
    genders: tuple[str, ...] = ()
    ethnicities: tuple[str, ...] = ()
    residence_types: tuple[str, ...] = ()
    redundancy_profiles: tuple[str, ...] = ()
    mobility_propensity_buckets: tuple[str, ...] = ()
    mobility_low_max: float = 0.09
    mobility_high_min: float = 0.18
    trait_low_max: float = 0.33
    trait_high_min: float = 0.66


def get_selection_schema() -> dict[str, Any]:
    return {
        "defaults": asdict(SelectionConfig()),
        "selection": {
            "sample": {"mode": "all|count|pct", "value": "number"},
            "filters": {
                "age_bins": "list[str]",
                "genders": "list[str]",
                "ethnicities": "list[str]",
                "residence_types": "list[str]",
                "redundancy_profiles": "list[single_record|multi_record]",
                "mobility_propensity_buckets": "list[low|medium|high]",
            },
            "thresholds": {
                "mobility_low_max": "float in [0,1]",
                "mobility_high_min": "float in [0,1]",
                "trait_low_max": "float in [0,1]",
                "trait_high_min": "float in [0,1]",
            },
        },
    }


def _as_tuple(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ValueError("Selection filter values must be lists when provided")
    return tuple(str(item).strip() for item in raw if str(item).strip())


def parse_selection_config(raw: dict[str, Any] | None) -> SelectionConfig:
    cfg = raw or {}
    if not isinstance(cfg, dict):
        raise ValueError("scenario.selection must be a mapping")

    sample = cfg.get("sample", {})
    if sample is None:
        sample = {}
    if not isinstance(sample, dict):
        raise ValueError("selection.sample must be a mapping")

    filters = cfg.get("filters", {})
    if filters is None:
        filters = {}
    if not isinstance(filters, dict):
        raise ValueError("selection.filters must be a mapping")

    thresholds = cfg.get("thresholds", {})
    if thresholds is None:
        thresholds = {}
    if not isinstance(thresholds, dict):
        raise ValueError("selection.thresholds must be a mapping")

    sample_mode = str(sample.get("mode", "pct")).strip().lower()
    if sample_mode not in {"all", "count", "pct"}:
        raise ValueError("selection.sample.mode must be one of: all, count, pct")
    sample_value = float(sample.get("value", 100.0))
    if sample_value < 0:
        raise ValueError("selection.sample.value must be >= 0")

    config = SelectionConfig(
        sample_mode=sample_mode,
        sample_value=sample_value,
        age_bins=_as_tuple(filters.get("age_bins")),
        genders=_as_tuple(filters.get("genders")),
        ethnicities=_as_tuple(filters.get("ethnicities")),
        residence_types=_as_tuple(filters.get("residence_types")),
        redundancy_profiles=_as_tuple(filters.get("redundancy_profiles")),
        mobility_propensity_buckets=_as_tuple(filters.get("mobility_propensity_buckets")),
        mobility_low_max=float(thresholds.get("mobility_low_max", 0.09)),
        mobility_high_min=float(thresholds.get("mobility_high_min", 0.18)),
        trait_low_max=float(thresholds.get("trait_low_max", 0.33)),
        trait_high_min=float(thresholds.get("trait_high_min", 0.66)),
    )
    validate_selection_config(config)
    return config


def validate_selection_config(config: SelectionConfig) -> None:
    if config.sample_mode == "count" and int(config.sample_value) != config.sample_value:
        raise ValueError("selection.sample.value must be an integer when mode=count")
    if config.sample_mode == "pct" and config.sample_value > 100.0:
        raise ValueError("selection.sample.value must be <= 100 when mode=pct")
    if config.mobility_low_max < 0 or config.mobility_low_max > 1:
        raise ValueError("selection.thresholds.mobility_low_max must be in [0,1]")
    if config.mobility_high_min < 0 or config.mobility_high_min > 1:
        raise ValueError("selection.thresholds.mobility_high_min must be in [0,1]")
    if config.mobility_low_max > config.mobility_high_min:
        raise ValueError("selection.thresholds mobility_low_max must be <= mobility_high_min")
    if config.trait_low_max < 0 or config.trait_low_max > 1:
        raise ValueError("selection.thresholds.trait_low_max must be in [0,1]")
    if config.trait_high_min < 0 or config.trait_high_min > 1:
        raise ValueError("selection.thresholds.trait_high_min must be in [0,1]")
    if config.trait_low_max > config.trait_high_min:
        raise ValueError("selection.thresholds trait_low_max must be <= trait_high_min")

    allowed_redundancy = {"single_record", "multi_record"}
    if any(item not in allowed_redundancy for item in config.redundancy_profiles):
        raise ValueError("selection.filters.redundancy_profiles only supports: single_record, multi_record")
    allowed_buckets = {"low", "medium", "high"}
    if any(item.lower() not in allowed_buckets for item in config.mobility_propensity_buckets):
        raise ValueError("selection.filters.mobility_propensity_buckets only supports: low, medium, high")


def _stable_numeric_or_text_key(value: str) -> tuple[int, str]:
    text = str(value).strip()
    if text.isdigit():
        return (0, f"{int(text):020d}")
    return (1, text)


def _deterministic_unit(person_key: str, seed: int, salt: str) -> float:
    payload = f"{seed}|{salt}|{person_key}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    as_int = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return as_int / float(2**64 - 1)


def _score_to_bucket(score: float, low_max: float, high_min: float) -> str:
    if score <= low_max:
        return "low"
    if score >= high_min:
        return "high"
    return "medium"


def build_phase1_entity_view(phase1_df: pd.DataFrame) -> pd.DataFrame:
    required = {
        "PersonKey",
        "EntityRecordIndex",
        "RecordKey",
        "AgeBin",
        "Gender",
        "Ethnicity",
        "ResidenceType",
    }
    missing = [col for col in sorted(required) if col not in phase1_df.columns]
    if missing:
        raise ValueError(f"Phase-1 CSV missing required columns for selection engine: {', '.join(missing)}")

    df = phase1_df.copy()
    df["PersonKey"] = df["PersonKey"].astype(str).str.strip()
    df["EntityRecordIndex_int"] = pd.to_numeric(df["EntityRecordIndex"], errors="coerce").fillna(0).astype(int)
    df["RecordKey_int"] = pd.to_numeric(df["RecordKey"], errors="coerce").fillna(0).astype(int)

    sorted_df = df.sort_values(
        by=["PersonKey", "EntityRecordIndex_int", "RecordKey_int"],
        kind="mergesort",
    )
    representative = sorted_df.groupby("PersonKey", as_index=False).first()
    latest = sorted_df.groupby("PersonKey", as_index=False).last()
    records_per_entity = sorted_df.groupby("PersonKey").size().rename("RecordsPerEntity")

    entity = representative[["PersonKey", "AgeBin", "Gender", "Ethnicity"]].copy()
    entity = entity.merge(
        latest[["PersonKey", "ResidenceType"]],
        on="PersonKey",
        how="left",
    )
    entity = entity.merge(records_per_entity, on="PersonKey", how="left")
    entity["RecordsPerEntity"] = entity["RecordsPerEntity"].fillna(1).astype(int)
    entity["RedundancyProfile"] = np.where(
        entity["RecordsPerEntity"] <= 1,
        "single_record",
        "multi_record",
    )
    entity = entity.sort_values(
        by="PersonKey",
        key=lambda series: series.map(_stable_numeric_or_text_key),
        kind="mergesort",
    ).reset_index(drop=True)
    return entity


def _mobility_agebin_base_map(mobility_params_df: pd.DataFrame) -> dict[str, float]:
    cohort_pct = {
        str(row["age_cohort_id"]): float(row["moved_past_year_pct"]) / 100.0
        for _, row in mobility_params_df.iterrows()
    }
    cohort_pop = {
        str(row["age_cohort_id"]): float(row.get("population", 0.0))
        for _, row in mobility_params_df.iterrows()
    }

    age_18_24 = cohort_pct.get("age_18_24", 0.20)
    age_25_34 = cohort_pct.get("age_25_34", 0.20)
    pop_18_24 = cohort_pop.get("age_18_24", 1.0)
    pop_25_34 = cohort_pop.get("age_25_34", 1.0)
    weighted_18_34 = ((age_18_24 * pop_18_24) + (age_25_34 * pop_25_34)) / max(pop_18_24 + pop_25_34, 1.0)

    return {
        "age_0_17": cohort_pct.get("age_0_17", 0.10),
        "age_18_34": weighted_18_34,
        "age_35_64": cohort_pct.get("age_35_64", 0.09),
        "age_65_plus": cohort_pct.get("age_65_plus", 0.06),
    }


def _age_based_propensity(age_bin: str, mapping: dict[str, float], default: float) -> float:
    return float(mapping.get(str(age_bin), default))


def assign_latent_traits(
    *,
    entity_df: pd.DataFrame,
    selection_config: SelectionConfig,
    mobility_params_df: pd.DataFrame,
    seed: int,
) -> pd.DataFrame:
    df = entity_df.copy()
    mobility_map = _mobility_agebin_base_map(mobility_params_df)

    partnership_map = {
        "age_0_17": 0.08,
        "age_18_34": 0.72,
        "age_35_64": 0.56,
        "age_65_plus": 0.30,
    }
    fertility_map = {
        "age_0_17": 0.10,
        "age_18_34": 0.76,
        "age_35_64": 0.25,
        "age_65_plus": 0.03,
    }

    mobility_scores: list[float] = []
    partnership_scores: list[float] = []
    fertility_scores: list[float] = []
    for _, row in df.iterrows():
        person_key = str(row["PersonKey"]).strip()
        age_bin = str(row.get("AgeBin", "")).strip()

        mobility_base = _age_based_propensity(age_bin, mobility_map, 0.10)
        mobility_jitter = (_deterministic_unit(person_key, seed, "mobility") - 0.5) * 0.08
        mobility_score = float(np.clip(mobility_base + mobility_jitter, 0.0, 1.0))
        mobility_scores.append(mobility_score)

        partnership_base = _age_based_propensity(age_bin, partnership_map, 0.45)
        partnership_jitter = (_deterministic_unit(person_key, seed, "partnership") - 0.5) * 0.30
        partnership_score = float(np.clip(partnership_base + partnership_jitter, 0.0, 1.0))
        partnership_scores.append(partnership_score)

        fertility_base = _age_based_propensity(age_bin, fertility_map, 0.30)
        fertility_jitter = (_deterministic_unit(person_key, seed, "fertility") - 0.5) * 0.30
        fertility_score = float(np.clip(fertility_base + fertility_jitter, 0.0, 1.0))
        fertility_scores.append(fertility_score)

    df["MobilityPropensityScore"] = np.round(mobility_scores, 6)
    df["PartnershipPropensityScore"] = np.round(partnership_scores, 6)
    df["FertilityPropensityScore"] = np.round(fertility_scores, 6)

    df["MobilityPropensityBucket"] = df["MobilityPropensityScore"].map(
        lambda v: _score_to_bucket(
            float(v),
            selection_config.mobility_low_max,
            selection_config.mobility_high_min,
        )
    )
    df["PartnershipPropensityBucket"] = df["PartnershipPropensityScore"].map(
        lambda v: _score_to_bucket(
            float(v),
            selection_config.trait_low_max,
            selection_config.trait_high_min,
        )
    )
    df["FertilityPropensityBucket"] = df["FertilityPropensityScore"].map(
        lambda v: _score_to_bucket(
            float(v),
            selection_config.trait_low_max,
            selection_config.trait_high_min,
        )
    )
    return df


def _apply_filter(df: pd.DataFrame, column: str, values: tuple[str, ...]) -> pd.DataFrame:
    if not values:
        return df
    value_set = {str(v).strip().lower() for v in values}
    mask = df[column].astype(str).str.strip().str.lower().isin(value_set)
    return df[mask].copy()


def filter_scenario_population(
    enriched_entity_df: pd.DataFrame,
    selection_config: SelectionConfig,
) -> tuple[pd.DataFrame, dict[str, int]]:
    counts: dict[str, int] = {"entity_total": int(len(enriched_entity_df))}
    current = enriched_entity_df.copy()
    counts["after_age_bins"] = int(len(current))
    current = _apply_filter(current, "AgeBin", selection_config.age_bins)
    counts["after_age_bins"] = int(len(current))
    current = _apply_filter(current, "Gender", selection_config.genders)
    counts["after_genders"] = int(len(current))
    current = _apply_filter(current, "Ethnicity", selection_config.ethnicities)
    counts["after_ethnicities"] = int(len(current))
    current = _apply_filter(current, "ResidenceType", selection_config.residence_types)
    counts["after_residence_types"] = int(len(current))
    current = _apply_filter(current, "RedundancyProfile", selection_config.redundancy_profiles)
    counts["after_redundancy_profiles"] = int(len(current))
    current = _apply_filter(
        current,
        "MobilityPropensityBucket",
        tuple(item.lower() for item in selection_config.mobility_propensity_buckets),
    )
    counts["after_mobility_propensity_buckets"] = int(len(current))
    return current, counts


def _deterministic_sample(
    candidates_df: pd.DataFrame,
    selection_config: SelectionConfig,
    seed: int,
) -> pd.DataFrame:
    if candidates_df.empty:
        return candidates_df.copy()

    sorted_candidates = candidates_df.sort_values(
        by="PersonKey",
        key=lambda series: series.map(_stable_numeric_or_text_key),
        kind="mergesort",
    ).reset_index(drop=True)
    n = len(sorted_candidates)

    if selection_config.sample_mode == "all":
        selected = sorted_candidates
    elif selection_config.sample_mode == "count":
        requested = int(selection_config.sample_value)
        take = min(max(requested, 0), n)
        if take == 0:
            selected = sorted_candidates.iloc[0:0].copy()
        else:
            rng = np.random.default_rng(seed)
            chosen = np.sort(rng.choice(n, size=take, replace=False))
            selected = sorted_candidates.iloc[chosen].copy()
    else:
        pct = float(selection_config.sample_value)
        take = int(round((pct / 100.0) * n))
        if pct > 0 and take == 0:
            take = 1
        take = min(max(take, 0), n)
        if take == 0:
            selected = sorted_candidates.iloc[0:0].copy()
        else:
            rng = np.random.default_rng(seed)
            chosen = np.sort(rng.choice(n, size=take, replace=False))
            selected = sorted_candidates.iloc[chosen].copy()

    return selected.sort_values(
        by="PersonKey",
        key=lambda series: series.map(_stable_numeric_or_text_key),
        kind="mergesort",
    ).reset_index(drop=True)


def select_scenario_population(
    *,
    phase1_df: pd.DataFrame,
    mobility_params_df: pd.DataFrame,
    selection_config: SelectionConfig,
    seed: int,
    scenario_id: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    entity_df = build_phase1_entity_view(phase1_df)
    enriched = assign_latent_traits(
        entity_df=entity_df,
        selection_config=selection_config,
        mobility_params_df=mobility_params_df,
        seed=seed,
    )

    candidates, filter_counts = filter_scenario_population(enriched, selection_config)
    selected = _deterministic_sample(candidates, selection_config, seed=seed)

    selected = selected.copy()
    selected["ScenarioId"] = str(scenario_id)
    selected["SelectionSeed"] = int(seed)

    output_columns = [
        "PersonKey",
        "ScenarioId",
        "SelectionSeed",
        "AgeBin",
        "Gender",
        "Ethnicity",
        "ResidenceType",
        "RecordsPerEntity",
        "RedundancyProfile",
        "MobilityPropensityScore",
        "MobilityPropensityBucket",
        "PartnershipPropensityScore",
        "PartnershipPropensityBucket",
        "FertilityPropensityScore",
        "FertilityPropensityBucket",
    ]
    selected = selected[output_columns].copy()

    person_keys_joined = "|".join(selected["PersonKey"].astype(str).tolist())
    checksum = hashlib.sha256(person_keys_joined.encode("utf-8")).hexdigest()

    audit_log = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "scenario_id": str(scenario_id),
        "selection_seed": int(seed),
        "selection_config": asdict(selection_config),
        "counts": {
            **filter_counts,
            "candidate_entities": int(len(candidates)),
            "selected_entities": int(len(selected)),
        },
        "selected_personkey_sha256": checksum,
        "selected_personkey_preview": selected["PersonKey"].astype(str).head(20).tolist(),
    }
    return selected, audit_log


def generate_scenario_population_from_files(
    *,
    phase1_csv_path: Path,
    mobility_params_df: pd.DataFrame,
    selection_config: SelectionConfig,
    seed: int,
    scenario_id: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    phase1_df = pd.read_csv(phase1_csv_path, dtype=str)
    return select_scenario_population(
        phase1_df=phase1_df,
        mobility_params_df=mobility_params_df,
        selection_config=selection_config,
        seed=seed,
        scenario_id=scenario_id,
    )
