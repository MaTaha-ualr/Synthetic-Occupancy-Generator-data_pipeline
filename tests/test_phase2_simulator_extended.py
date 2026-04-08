"""Extended simulator tests — birth, divorce, cohabit events and edge cases."""

from __future__ import annotations

from datetime import date
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import parse_constraints_config
from sog_phase2.simulator import parse_simulation_config, simulate_truth_layer


def _phase1_sample(n: int = 4) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        age = 22 + (i * 3)
        rows.append({
            "RecordKey": str(i), "PersonKey": str(i), "EntityRecordIndex": "1",
            "AddressKey": f"A{i}",
            "FormalFirstName": f"First{i}", "MiddleName": "", "LastName": f"Last{i}",
            "Suffix": "", "FormalFullName": f"First{i} Last{i}",
            "Gender": "female" if i % 2 == 0 else "male",
            "Ethnicity": "White", "DOB": f"{2026 - age}-01-01",
            "Age": str(age), "AgeBin": "age_18_34" if age <= 34 else "age_35_64",
            "SSN": f"{i:03d}-{i:02d}-{i:04d}", "ResidenceType": "HOUSE",
            "ResidenceStartDate": "2020-01-01",
        })
    return pd.DataFrame(rows)


def _scenario_population(n: int = 4) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "PersonKey": str(i),
            "MobilityPropensityScore": 0.85,
            "PartnershipPropensityScore": 0.90,
            "FertilityPropensityScore": 0.80,
        })
    return pd.DataFrame(rows)


def _simulate(params: dict, n: int = 4, periods: int = 4, seed: int = 12345) -> dict:
    sim_cfg = parse_simulation_config(
        {"granularity": "monthly", "start_date": "2026-01-01", "periods": periods}
    )
    base_params = {
        "move_rate_pct": 0.0, "cohabit_rate_pct": 0.0,
        "birth_rate_pct": 0.0, "divorce_rate_pct": 0.0,
        "leave_home_rate_pct": 0.0, "split_rate_pct": 0.0,
    }
    base_params.update(params)
    return simulate_truth_layer(
        phase1_df=_phase1_sample(n),
        scenario_population_df=_scenario_population(n),
        scenario_id="test",
        seed=seed,
        simulation_config=sim_cfg,
        constraints_config=parse_constraints_config({}),
        scenario_parameters=base_params,
        phase2_priors=None,
    )


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------

def test_parse_simulation_config_monthly() -> None:
    cfg = parse_simulation_config({"granularity": "monthly", "periods": 6})
    assert cfg.granularity == "monthly"
    assert cfg.periods == 6


def test_parse_simulation_config_with_start_date() -> None:
    cfg = parse_simulation_config({"start_date": "2025-06-01"})
    assert cfg.start_date == date(2025, 6, 1)


# ---------------------------------------------------------------------------
# Move events
# ---------------------------------------------------------------------------

def test_high_move_rate_produces_moves() -> None:
    result = _simulate({"move_rate_pct": 100.0}, periods=6)
    events = result["truth_events"]
    moves = events[events["EventType"].str.upper() == "MOVE"]
    assert len(moves) > 0


def test_zero_move_rate_produces_no_moves() -> None:
    result = _simulate({"move_rate_pct": 0.0})
    events = result["truth_events"]
    moves = events[events["EventType"].str.upper() == "MOVE"]
    assert len(moves) == 0


# ---------------------------------------------------------------------------
# Cohabit events
# ---------------------------------------------------------------------------

def test_high_cohabit_rate_produces_cohabit_events() -> None:
    result = _simulate({"cohabit_rate_pct": 100.0}, periods=6)
    events = result["truth_events"]
    cohabits = events[events["EventType"].str.upper() == "COHABIT"]
    assert len(cohabits) > 0


def test_cohabit_creates_shared_household() -> None:
    result = _simulate({"cohabit_rate_pct": 100.0}, periods=6)
    events = result["truth_events"]
    cohabits = events[events["EventType"].str.upper() == "COHABIT"]
    if len(cohabits) > 0:
        new_household = cohabits.iloc[0]["NewHouseholdKey"]
        assert str(new_household).strip() != ""
        memberships = result["truth_household_memberships"]
        members = memberships[memberships["HouseholdKey"] == new_household]
        assert len(members) >= 2


# ---------------------------------------------------------------------------
# Birth events
# ---------------------------------------------------------------------------

def test_high_birth_rate_produces_birth_events() -> None:
    # Need couples first, so use cohabit + birth
    result = _simulate({"cohabit_rate_pct": 100.0, "birth_rate_pct": 100.0}, periods=8)
    events = result["truth_events"]
    births = events[events["EventType"].str.upper() == "BIRTH"]
    assert len(births) > 0


def test_birth_creates_new_person() -> None:
    result = _simulate({"cohabit_rate_pct": 100.0, "birth_rate_pct": 100.0}, periods=8)
    events = result["truth_events"]
    births = events[events["EventType"].str.upper() == "BIRTH"]
    if len(births) > 0:
        child_key = births.iloc[0]["ChildPersonKey"]
        assert str(child_key).strip() != ""
        people = result["truth_people"]
        assert str(child_key) in people["PersonKey"].astype(str).values


# ---------------------------------------------------------------------------
# Truth table consistency
# ---------------------------------------------------------------------------

def test_all_truth_tables_present() -> None:
    result = _simulate({"move_rate_pct": 50.0})
    for table in ("truth_people", "truth_households", "truth_household_memberships",
                  "truth_residence_history", "truth_events"):
        assert table in result, f"Missing truth table: {table}"
        assert isinstance(result[table], pd.DataFrame)


def test_every_person_has_residence() -> None:
    result = _simulate({"move_rate_pct": 50.0})
    people_keys = set(result["truth_people"]["PersonKey"].astype(str))
    residence_keys = set(result["truth_residence_history"]["PersonKey"].astype(str))
    assert people_keys.issubset(residence_keys)


def test_every_person_has_membership() -> None:
    result = _simulate({"move_rate_pct": 50.0})
    people_keys = set(result["truth_people"]["PersonKey"].astype(str))
    membership_keys = set(result["truth_household_memberships"]["PersonKey"].astype(str))
    assert people_keys.issubset(membership_keys)


def test_consistency_checks_pass() -> None:
    result = _simulate({"move_rate_pct": 50.0, "cohabit_rate_pct": 50.0}, periods=6)
    checks = result["quality"]["consistency_checks"]
    assert checks["residence_intervals_non_overlapping"] is True
    assert checks["membership_intervals_non_overlapping"] is True


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------

def test_different_seeds_produce_different_results() -> None:
    r1 = _simulate({"move_rate_pct": 80.0}, seed=1)
    r2 = _simulate({"move_rate_pct": 80.0}, seed=2)
    # Different seeds should produce different event counts (with high probability)
    e1 = len(r1["truth_events"])
    e2 = len(r2["truth_events"])
    r1_keys = set(r1["truth_residence_history"]["AddressKey"].astype(str))
    r2_keys = set(r2["truth_residence_history"]["AddressKey"].astype(str))
    assert e1 != e2 or r1_keys != r2_keys


# ---------------------------------------------------------------------------
# Edge case: single person
# ---------------------------------------------------------------------------

def test_single_person_simulation() -> None:
    result = _simulate({"move_rate_pct": 100.0}, n=1, periods=3)
    assert len(result["truth_people"]) == 1
    assert len(result["truth_households"]) >= 1
    assert len(result["truth_household_memberships"]) >= 1
    assert len(result["truth_residence_history"]) >= 1
