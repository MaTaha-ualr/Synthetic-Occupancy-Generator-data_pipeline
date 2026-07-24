from __future__ import annotations

import pandas as pd
import pytest

from evaluation.external_validity import (
    combine_age_18_34_target,
    household_type_shares,
    relative_deviation_pct,
    relative_verdict,
)


def test_combines_18_34_target_using_population_weights() -> None:
    frame = pd.DataFrame([
        {"age_cohort_id": "age_18_24", "population": 25, "moved_population": 10},
        {"age_cohort_id": "age_25_34", "population": 75, "moved_population": 15},
    ])
    assert combine_age_18_34_target(frame) == pytest.approx(25.0)


def test_household_snapshot_maps_mutually_exclusive_b11001_bins() -> None:
    households = pd.DataFrame({
        "HouseholdKey": ["couple", "male_parent", "alone", "roommates"],
        "HouseholdStartDate": ["2026-01-01"] * 4,
        "HouseholdEndDate": [""] * 4,
    })
    memberships = pd.DataFrame([
        ("p1", "couple", "HEAD"), ("p2", "couple", "SPOUSE"),
        ("p3", "male_parent", "HEAD"), ("p4", "male_parent", "CHILD"),
        ("p5", "alone", "HEAD"),
        ("p6", "roommates", "HEAD"), ("p7", "roommates", "ROOMMATE"),
    ], columns=["PersonKey", "HouseholdKey", "HouseholdRole"])
    memberships["MembershipStartDate"] = "2026-01-01"
    memberships["MembershipEndDate"] = ""
    people = pd.DataFrame({
        "PersonKey": [f"p{i}" for i in range(1, 8)],
        "Gender": ["FEMALE", "MALE", "MALE", "FEMALE", "FEMALE", "MALE", "FEMALE"],
    })
    shares = household_type_shares(households, memberships, people, "2026-01-01")
    assert shares == {
        "married_couple_family": 25.0,
        "single_parent_male_householder": 25.0,
        "single_parent_female_householder": 0.0,
        "nonfamily_living_alone": 25.0,
        "nonfamily_not_alone": 25.0,
    }


def test_relative_deviation_verdict_uses_absolute_magnitude() -> None:
    assert relative_deviation_pct(8, 10) == pytest.approx(-20.0)
    assert relative_verdict(-5.0) == "close"
    assert relative_verdict(-20.0) == "moderate"
    assert relative_verdict(-20.1) == "large deviation"
