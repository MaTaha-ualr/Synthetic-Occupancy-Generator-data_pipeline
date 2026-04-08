"""Extended selection tests — sample modes, propensity, config validation."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.selection import (
    assign_latent_traits,
    build_phase1_entity_view,
    filter_scenario_population,
    parse_selection_config,
    select_scenario_population,
    validate_selection_config,
    SelectionConfig,
)


def _phase1_sample(n: int = 10) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "PersonKey": str(i),
            "EntityRecordIndex": 1,
            "RecordKey": str(i),
            "AgeBin": "age_18_34" if i <= n // 2 else "age_35_64",
            "Gender": "female" if i % 2 == 0 else "male",
            "Ethnicity": "White" if i % 3 != 0 else "Black",
            "ResidenceType": "house" if i % 2 == 0 else "apt",
        })
    return pd.DataFrame(rows)


def _phase1_with_redundancy() -> pd.DataFrame:
    return pd.DataFrame([
        {"PersonKey": "1", "EntityRecordIndex": 1, "RecordKey": "R1", "AgeBin": "age_18_34", "Gender": "male", "Ethnicity": "White", "ResidenceType": "house"},
        {"PersonKey": "1", "EntityRecordIndex": 2, "RecordKey": "R2", "AgeBin": "age_18_34", "Gender": "male", "Ethnicity": "White", "ResidenceType": "house"},
        {"PersonKey": "2", "EntityRecordIndex": 1, "RecordKey": "R3", "AgeBin": "age_35_64", "Gender": "female", "Ethnicity": "Black", "ResidenceType": "apt"},
        {"PersonKey": "3", "EntityRecordIndex": 1, "RecordKey": "R4", "AgeBin": "age_18_34", "Gender": "male", "Ethnicity": "White", "ResidenceType": "house"},
    ])


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------

def test_parse_selection_defaults() -> None:
    cfg = parse_selection_config({})
    assert cfg.sample_mode == "pct"
    assert cfg.sample_value == 100.0
    assert cfg.age_bins == ()
    assert cfg.genders == ()


def test_parse_selection_count_mode() -> None:
    cfg = parse_selection_config({"sample": {"mode": "count", "value": 50}})
    assert cfg.sample_mode == "count"
    assert cfg.sample_value == 50


def test_parse_selection_all_mode() -> None:
    cfg = parse_selection_config({"sample": {"mode": "all"}})
    assert cfg.sample_mode == "all"


def test_parse_selection_rejects_invalid_mode() -> None:
    with pytest.raises(ValueError, match="all, count, pct"):
        parse_selection_config({"sample": {"mode": "random"}})


def test_parse_selection_rejects_negative_value() -> None:
    with pytest.raises(ValueError, match=">= 0"):
        parse_selection_config({"sample": {"mode": "pct", "value": -10}})


def test_parse_selection_rejects_pct_above_100() -> None:
    with pytest.raises(ValueError, match="<= 100"):
        parse_selection_config({"sample": {"mode": "pct", "value": 110}})


def test_parse_selection_rejects_invalid_redundancy_profile() -> None:
    with pytest.raises(ValueError, match="redundancy_profiles"):
        parse_selection_config({"filters": {"redundancy_profiles": ["triple_record"]}})


def test_parse_selection_rejects_invalid_mobility_bucket() -> None:
    with pytest.raises(ValueError, match="mobility_propensity_buckets"):
        parse_selection_config({"filters": {"mobility_propensity_buckets": ["ultra"]}})


def test_thresholds_mobility_low_max_above_high_min_rejected() -> None:
    with pytest.raises(ValueError, match="mobility_low_max must be <="):
        parse_selection_config({"thresholds": {"mobility_low_max": 0.5, "mobility_high_min": 0.3}})


def test_thresholds_trait_low_max_above_high_min_rejected() -> None:
    with pytest.raises(ValueError, match="trait_low_max must be <="):
        parse_selection_config({"thresholds": {"trait_low_max": 0.8, "trait_high_min": 0.2}})


# ---------------------------------------------------------------------------
# Entity view
# ---------------------------------------------------------------------------

def test_entity_view_deduplicates_persons() -> None:
    entity = build_phase1_entity_view(_phase1_with_redundancy())
    assert len(entity) == 3
    assert set(entity["PersonKey"]) == {"1", "2", "3"}


def test_entity_view_assigns_redundancy_profile() -> None:
    entity = build_phase1_entity_view(_phase1_with_redundancy())
    person_1 = entity[entity["PersonKey"] == "1"].iloc[0]
    person_2 = entity[entity["PersonKey"] == "2"].iloc[0]
    assert person_1["RedundancyProfile"] == "multi_record"
    assert person_2["RedundancyProfile"] == "single_record"


def test_entity_view_missing_column_raises() -> None:
    df = pd.DataFrame([{"PersonKey": "1", "RecordKey": "R1"}])
    with pytest.raises(ValueError, match="missing required columns"):
        build_phase1_entity_view(df)


# ---------------------------------------------------------------------------
# Latent traits
# ---------------------------------------------------------------------------

def _mobility_params() -> pd.DataFrame:
    return pd.DataFrame([
        {"age_cohort_id": "age_0_17", "moved_past_year_pct": 10.5, "population": 73000000},
        {"age_cohort_id": "age_18_24", "moved_past_year_pct": 25.4, "population": 30000000},
        {"age_cohort_id": "age_25_34", "moved_past_year_pct": 20.7, "population": 45000000},
        {"age_cohort_id": "age_35_64", "moved_past_year_pct": 8.9, "population": 125000000},
        {"age_cohort_id": "age_65_plus", "moved_past_year_pct": 5.5, "population": 56000000},
    ])


def test_assign_latent_traits_adds_columns() -> None:
    entity = build_phase1_entity_view(_phase1_sample())
    result = assign_latent_traits(
        entity_df=entity, mobility_params_df=_mobility_params(),
        seed=42, selection_config=parse_selection_config({}),
    )
    assert "MobilityPropensityScore" in result.columns
    assert "PartnershipPropensityScore" in result.columns
    assert "FertilityPropensityScore" in result.columns
    assert "MobilityPropensityBucket" in result.columns
    assert len(result) == len(entity)


def test_latent_traits_are_deterministic() -> None:
    entity = build_phase1_entity_view(_phase1_sample())
    cfg = parse_selection_config({})
    r1 = assign_latent_traits(entity_df=entity, mobility_params_df=_mobility_params(), seed=42, selection_config=cfg)
    r2 = assign_latent_traits(entity_df=entity, mobility_params_df=_mobility_params(), seed=42, selection_config=cfg)
    pd.testing.assert_frame_equal(r1, r2)


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def _enrich(entity: pd.DataFrame) -> pd.DataFrame:
    """Add latent traits so filter_scenario_population can filter on MobilityPropensityBucket."""
    return assign_latent_traits(
        entity_df=entity, mobility_params_df=_mobility_params(),
        seed=42, selection_config=parse_selection_config({}),
    )


def test_filter_by_gender() -> None:
    entity = _enrich(build_phase1_entity_view(_phase1_sample(10)))
    cfg = parse_selection_config({"filters": {"genders": ["female"]}})
    filtered, _ = filter_scenario_population(entity, cfg)
    assert all(filtered["Gender"] == "female")
    assert len(filtered) > 0


def test_filter_by_age_bin() -> None:
    entity = _enrich(build_phase1_entity_view(_phase1_sample(10)))
    cfg = parse_selection_config({"filters": {"age_bins": ["age_18_34"]}})
    filtered, _ = filter_scenario_population(entity, cfg)
    assert all(filtered["AgeBin"] == "age_18_34")


def test_filter_by_ethnicity() -> None:
    entity = _enrich(build_phase1_entity_view(_phase1_sample(10)))
    cfg = parse_selection_config({"filters": {"ethnicities": ["Black"]}})
    filtered, _ = filter_scenario_population(entity, cfg)
    assert all(filtered["Ethnicity"] == "Black")


def test_filter_by_redundancy_profile() -> None:
    entity = _enrich(build_phase1_entity_view(_phase1_with_redundancy()))
    cfg = parse_selection_config({"filters": {"redundancy_profiles": ["multi_record"]}})
    filtered, _ = filter_scenario_population(entity, cfg)
    assert all(filtered["RedundancyProfile"] == "multi_record")
    assert len(filtered) == 1


def test_filter_no_match_returns_empty() -> None:
    entity = _enrich(build_phase1_entity_view(_phase1_sample(5)))
    cfg = parse_selection_config({"filters": {"ethnicities": ["Martian"]}})
    filtered, _ = filter_scenario_population(entity, cfg)
    assert len(filtered) == 0


# ---------------------------------------------------------------------------
# Full selection pipeline (sample modes)
# ---------------------------------------------------------------------------

def _select(phase1: pd.DataFrame, cfg: SelectionConfig, seed: int = 42):
    return select_scenario_population(
        phase1_df=phase1, mobility_params_df=_mobility_params(),
        selection_config=cfg, seed=seed, scenario_id="test",
    )


def test_select_count_mode() -> None:
    phase1 = _phase1_sample(20)
    cfg = parse_selection_config({"sample": {"mode": "count", "value": 5}})
    result, log = _select(phase1, cfg)
    assert len(result) == 5
    assert log["counts"]["selected_entities"] == 5


def test_select_pct_mode() -> None:
    phase1 = _phase1_sample(20)
    cfg = parse_selection_config({"sample": {"mode": "pct", "value": 50}})
    result, log = _select(phase1, cfg)
    assert len(result) == 10


def test_select_all_mode() -> None:
    phase1 = _phase1_sample(15)
    cfg = parse_selection_config({"sample": {"mode": "all"}})
    result, log = _select(phase1, cfg)
    assert len(result) == 15


def test_select_is_deterministic() -> None:
    phase1 = _phase1_sample(30)
    cfg = parse_selection_config({"sample": {"mode": "count", "value": 10}})
    r1, _ = _select(phase1, cfg, seed=42)
    r2, _ = _select(phase1, cfg, seed=42)
    assert list(r1["PersonKey"]) == list(r2["PersonKey"])


def test_select_different_seeds_produce_different_results() -> None:
    phase1 = _phase1_sample(30)
    cfg = parse_selection_config({"sample": {"mode": "count", "value": 10}})
    r1, _ = _select(phase1, cfg, seed=42)
    r2, _ = _select(phase1, cfg, seed=99)
    # Different seeds should produce different selections (with high probability)
    assert list(r1["PersonKey"]) != list(r2["PersonKey"])
