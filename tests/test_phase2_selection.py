from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.params import load_phase2_params_from_project
from sog_phase2.selection import (
    build_phase1_entity_view,
    parse_selection_config,
    select_scenario_population,
)


def _phase1_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "RecordKey": "1",
                "PersonKey": "1",
                "EntityRecordIndex": "1",
                "AgeBin": "age_18_34",
                "Gender": "female",
                "Ethnicity": "White",
                "ResidenceType": "HOUSE",
            },
            {
                "RecordKey": "2",
                "PersonKey": "1",
                "EntityRecordIndex": "2",
                "AgeBin": "age_18_34",
                "Gender": "female",
                "Ethnicity": "White",
                "ResidenceType": "APARTMENT",
            },
            {
                "RecordKey": "3",
                "PersonKey": "2",
                "EntityRecordIndex": "1",
                "AgeBin": "age_35_64",
                "Gender": "male",
                "Ethnicity": "Black",
                "ResidenceType": "HOUSE",
            },
            {
                "RecordKey": "4",
                "PersonKey": "3",
                "EntityRecordIndex": "1",
                "AgeBin": "age_65_plus",
                "Gender": "female",
                "Ethnicity": "Hispanic",
                "ResidenceType": "APARTMENT",
            },
        ]
    )


def test_build_phase1_entity_view_computes_redundancy_profile() -> None:
    entity_df = build_phase1_entity_view(_phase1_sample())
    profile = dict(zip(entity_df["PersonKey"], entity_df["RedundancyProfile"]))
    residence = dict(zip(entity_df["PersonKey"], entity_df["ResidenceType"]))
    records = dict(zip(entity_df["PersonKey"], entity_df["RecordsPerEntity"]))

    assert profile["1"] == "multi_record"
    assert profile["2"] == "single_record"
    assert records["1"] == 2
    assert records["2"] == 1
    assert residence["1"] == "APARTMENT"


def test_selection_engine_is_deterministic_for_same_seed() -> None:
    params = load_phase2_params_from_project(PROJECT_ROOT)
    selection_cfg = parse_selection_config(
        {
            "sample": {"mode": "count", "value": 2},
            "filters": {"age_bins": ["age_18_34", "age_35_64"]},
        }
    )
    selected_a, log_a = select_scenario_population(
        phase1_df=_phase1_sample(),
        mobility_params_df=params["mobility_by_age_cohort"],
        selection_config=selection_cfg,
        seed=20260318,
        scenario_id="deterministic_case",
    )
    selected_b, log_b = select_scenario_population(
        phase1_df=_phase1_sample(),
        mobility_params_df=params["mobility_by_age_cohort"],
        selection_config=selection_cfg,
        seed=20260318,
        scenario_id="deterministic_case",
    )

    assert selected_a.equals(selected_b)
    assert log_a["selected_personkey_sha256"] == log_b["selected_personkey_sha256"]


def test_selection_filters_support_primitives() -> None:
    params = load_phase2_params_from_project(PROJECT_ROOT)
    selection_cfg = parse_selection_config(
        {
            "sample": {"mode": "all", "value": 100},
            "filters": {
                "age_bins": ["age_18_34"],
                "genders": ["female"],
                "ethnicities": ["White"],
                "residence_types": ["APARTMENT"],
                "redundancy_profiles": ["multi_record"],
            },
        }
    )
    selected, _ = select_scenario_population(
        phase1_df=_phase1_sample(),
        mobility_params_df=params["mobility_by_age_cohort"],
        selection_config=selection_cfg,
        seed=20260319,
        scenario_id="primitive_filters",
    )

    assert len(selected) == 1
    row = selected.iloc[0]
    assert row["PersonKey"] == "1"
    assert row["RedundancyProfile"] == "multi_record"
    assert row["ResidenceType"] == "APARTMENT"
