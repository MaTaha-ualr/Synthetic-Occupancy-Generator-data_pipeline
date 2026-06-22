"""Extended simulator tests — birth, divorce, cohabit events and edge cases."""

from __future__ import annotations

from datetime import date
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "phase2" / "src"))

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


def _lifecycle_phase1_sample() -> pd.DataFrame:
    rows = [
        {
            "RecordKey": "1", "PersonKey": "1", "EntityRecordIndex": "1",
            "AddressKey": "A1",
            "FormalFirstName": "Evelyn", "MiddleName": "R", "LastName": "Stone",
            "Suffix": "", "FormalFullName": "Evelyn R Stone",
            "Gender": "female", "Ethnicity": "White", "DOB": "1950-01-01",
            "Age": "76", "AgeBin": "age_65_plus",
            "SSN": "111-11-1111", "ResidenceType": "HOUSE",
            "ResidenceStartDate": "2020-01-01",
        },
        {
            "RecordKey": "2", "PersonKey": "2", "EntityRecordIndex": "1",
            "AddressKey": "A2",
            "FormalFirstName": "Ava", "MiddleName": "", "LastName": "Reed",
            "Suffix": "", "FormalFullName": "Ava Reed",
            "Gender": "female", "Ethnicity": "White", "DOB": "1994-01-01",
            "Age": "32", "AgeBin": "age_18_34",
            "SSN": "222-22-2222", "ResidenceType": "APARTMENT",
            "ResidenceStartDate": "2020-01-01",
        },
        {
            "RecordKey": "3", "PersonKey": "3", "EntityRecordIndex": "1",
            "AddressKey": "A3",
            "FormalFirstName": "Leo", "MiddleName": "", "LastName": "Parker",
            "Suffix": "", "FormalFullName": "Leo Parker",
            "Gender": "male", "Ethnicity": "White", "DOB": "2016-01-01",
            "Age": "10", "AgeBin": "age_0_17",
            "SSN": "333-33-3333", "ResidenceType": "HOUSE",
            "ResidenceStartDate": "2020-01-01",
        },
        {
            "RecordKey": "4", "PersonKey": "4", "EntityRecordIndex": "1",
            "AddressKey": "A4",
            "FormalFirstName": "Noah", "MiddleName": "", "LastName": "Cole",
            "Suffix": "", "FormalFullName": "Noah Cole",
            "Gender": "male", "Ethnicity": "Hispanic", "DOB": "1990-01-01",
            "Age": "36", "AgeBin": "age_35_64",
            "SSN": "444-44-4444", "ResidenceType": "HOUSE",
            "ResidenceStartDate": "2020-01-01",
        },
    ]
    return pd.DataFrame(rows)


def _lifecycle_population() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "PersonKey": str(i),
                "MobilityPropensityScore": 0.0,
                "PartnershipPropensityScore": 0.0,
                "FertilityPropensityScore": 0.0,
            }
            for i in range(1, 5)
        ]
    )


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


def _simulate_lifecycle(params: dict, periods: int = 2, seed: int = 20260622) -> dict:
    sim_cfg = parse_simulation_config(
        {"granularity": "monthly", "start_date": "2026-01-01", "periods": periods}
    )
    base_params = {
        "move_rate_pct": 0.0, "cohabit_rate_pct": 0.0,
        "birth_rate_pct": 0.0, "divorce_rate_pct": 0.0,
        "leave_home_rate_pct": 0.0, "split_rate_pct": 0.0,
        "death_rate_pct": 0.0, "name_change_rate_pct": 0.0,
        "adoption_rate_pct": 0.0,
    }
    base_params.update(params)
    return simulate_truth_layer(
        phase1_df=_lifecycle_phase1_sample(),
        scenario_population_df=_lifecycle_population(),
        scenario_id="lifecycle_test",
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
# Lifecycle events
# ---------------------------------------------------------------------------

def test_high_death_rate_produces_death_event_and_closes_person_intervals() -> None:
    result = _simulate_lifecycle({"death_rate_pct": 100.0}, periods=1)
    events = result["truth_events"]
    deaths = events[events["EventType"].str.upper() == "DEATH"]
    assert len(deaths) >= 1

    subject = str(deaths.iloc[0]["SubjectPersonKey"])
    people = result["truth_people"].set_index("PersonKey")
    assert str(people.loc[subject, "IsDeceased"]).lower() == "true"
    assert str(people.loc[subject, "DeathDate"]).strip() == str(deaths.iloc[0]["EventDate"])

    memberships = result["truth_household_memberships"]
    subject_memberships = memberships[memberships["PersonKey"].astype(str) == subject]
    assert subject_memberships["MembershipEndDate"].astype(str).str.strip().ne("").any()
    residence = result["truth_residence_history"]
    subject_residence = residence[residence["PersonKey"].astype(str) == subject]
    assert subject_residence["ResidenceEndDate"].astype(str).str.strip().ne("").any()


def test_high_name_change_rate_produces_name_change_event_with_old_and_new_names() -> None:
    result = _simulate_lifecycle({"name_change_rate_pct": 100.0}, periods=1)
    events = result["truth_events"]
    changes = events[events["EventType"].str.upper() == "NAME_CHANGE"]
    assert len(changes) >= 1

    change = changes.iloc[0]
    assert str(change["SubjectPersonKey"]).strip() != ""
    assert str(change["PreviousFullName"]).strip() != ""
    assert str(change["NewFullName"]).strip() != ""
    assert change["PreviousFullName"] != change["NewFullName"]
    assert str(change["NameChangeReason"]).strip() != ""


def test_high_adoption_rate_moves_minor_to_adoptive_household() -> None:
    result = _simulate_lifecycle({"adoption_rate_pct": 100.0}, periods=1)
    events = result["truth_events"]
    adoptions = events[events["EventType"].str.upper() == "ADOPTION"]
    assert len(adoptions) >= 1

    adoption = adoptions.iloc[0]
    child_key = str(adoption["ChildPersonKey"])
    new_household = str(adoption["NewHouseholdKey"])
    adoptive_parent = str(adoption["AdoptiveParent1PersonKey"])
    assert child_key == "3"
    assert adoptive_parent and adoptive_parent != child_key
    assert new_household
    assert str(adoption["FromAddressKey"]).strip() != str(adoption["ToAddressKey"]).strip()

    memberships = result["truth_household_memberships"]
    child_memberships = memberships[memberships["PersonKey"].astype(str) == child_key]
    assert new_household in set(child_memberships["HouseholdKey"].astype(str))
    active_child_membership = child_memberships[
        child_memberships["MembershipEndDate"].astype(str).str.strip() == ""
    ]
    assert active_child_membership.iloc[0]["HouseholdKey"] == new_household


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
