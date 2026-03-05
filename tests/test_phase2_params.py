from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.params import load_phase2_params_from_project


def test_phase2_params_loads_required_tables() -> None:
    params = load_phase2_params_from_project(PROJECT_ROOT)
    assert "mobility_overall" in params
    assert "mobility_by_age_cohort" in params
    assert "marriage_divorce_rates" in params
    assert "fertility_by_age" in params
    assert "household_type_shares" in params
    assert "priors_snapshot" in params
    assert "sources" in params
    assert "manifest" in params


def test_household_core_shares_sum_to_100_pct() -> None:
    params = load_phase2_params_from_project(PROJECT_ROOT)
    hh = params["household_type_shares"]
    core = hh[
        hh["household_type_id"].isin(
            [
                "married_couple_family",
                "single_parent_male_householder",
                "single_parent_female_householder",
                "nonfamily_household",
            ]
        )
    ]
    total = float(core["share_of_all_households_pct"].sum())
    assert abs(total - 100.0) < 1e-6


def test_mobility_components_match_overall_rate() -> None:
    params = load_phase2_params_from_project(PROJECT_ROOT)
    overall = params["mobility_overall"]
    moved_any = float(overall.loc[overall["metric_id"] == "moved_past_year_pct", "value_pct"].iloc[0])

    components = [
        "moved_within_same_county_pct",
        "moved_from_different_county_same_state_pct",
        "moved_from_different_state_pct",
        "moved_from_abroad_pct",
    ]
    component_total = float(
        overall[overall["metric_id"].isin(components)]["value_pct"].sum()
    )
    assert abs(moved_any - component_total) < 1e-6
