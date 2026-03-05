from __future__ import annotations

from datetime import date
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import parse_constraints_config
from sog_phase2.simulator import parse_simulation_config, simulate_truth_layer


def _phase1_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "RecordKey": "1",
                "PersonKey": "1",
                "EntityRecordIndex": "1",
                "AddressKey": "A1",
                "FormalFirstName": "Ava",
                "MiddleName": "",
                "LastName": "Stone",
                "Suffix": "",
                "FormalFullName": "Ava Stone",
                "Gender": "female",
                "Ethnicity": "White",
                "DOB": "1998-04-02",
                "Age": "28",
                "AgeBin": "age_18_34",
                "SSN": "111-11-1111",
                "ResidenceType": "HOUSE",
                "ResidenceStartDate": "2020-01-01",
            },
            {
                "RecordKey": "2",
                "PersonKey": "2",
                "EntityRecordIndex": "1",
                "AddressKey": "A2",
                "FormalFirstName": "Liam",
                "MiddleName": "",
                "LastName": "Parker",
                "Suffix": "",
                "FormalFullName": "Liam Parker",
                "Gender": "male",
                "Ethnicity": "White",
                "DOB": "1995-02-11",
                "Age": "31",
                "AgeBin": "age_18_34",
                "SSN": "222-22-2222",
                "ResidenceType": "APARTMENT",
                "ResidenceStartDate": "2021-02-01",
            },
            {
                "RecordKey": "3",
                "PersonKey": "3",
                "EntityRecordIndex": "1",
                "AddressKey": "A3",
                "FormalFirstName": "Mia",
                "MiddleName": "",
                "LastName": "Reed",
                "Suffix": "",
                "FormalFullName": "Mia Reed",
                "Gender": "female",
                "Ethnicity": "Black",
                "DOB": "1997-06-18",
                "Age": "29",
                "AgeBin": "age_18_34",
                "SSN": "333-33-3333",
                "ResidenceType": "HOUSE",
                "ResidenceStartDate": "2022-03-01",
            },
            {
                "RecordKey": "4",
                "PersonKey": "4",
                "EntityRecordIndex": "1",
                "AddressKey": "A4",
                "FormalFirstName": "Noah",
                "MiddleName": "",
                "LastName": "Cole",
                "Suffix": "",
                "FormalFullName": "Noah Cole",
                "Gender": "male",
                "Ethnicity": "Hispanic",
                "DOB": "1994-09-10",
                "Age": "32",
                "AgeBin": "age_18_34",
                "SSN": "444-44-4444",
                "ResidenceType": "APARTMENT",
                "ResidenceStartDate": "2023-04-01",
            },
        ]
    )


def _scenario_population_sample() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "PersonKey": "1",
                "MobilityPropensityScore": 0.90,
                "PartnershipPropensityScore": 0.95,
                "FertilityPropensityScore": 0.88,
            },
            {
                "PersonKey": "2",
                "MobilityPropensityScore": 0.82,
                "PartnershipPropensityScore": 0.86,
                "FertilityPropensityScore": 0.20,
            },
            {
                "PersonKey": "3",
                "MobilityPropensityScore": 0.77,
                "PartnershipPropensityScore": 0.91,
                "FertilityPropensityScore": 0.84,
            },
            {
                "PersonKey": "4",
                "MobilityPropensityScore": 0.71,
                "PartnershipPropensityScore": 0.83,
                "FertilityPropensityScore": 0.18,
            },
        ]
    )


def test_parse_simulation_config_uses_default_start_date() -> None:
    cfg = parse_simulation_config({}, default_start_date=date(2026, 3, 5))
    assert cfg.granularity == "monthly"
    assert cfg.start_date == date(2026, 3, 5)
    assert cfg.periods == 12


def test_truth_simulator_is_deterministic_and_consistent() -> None:
    simulation_cfg = parse_simulation_config(
        {"granularity": "monthly", "start_date": "2026-01-01", "periods": 4}
    )
    constraints_cfg = parse_constraints_config({})
    params = {
        "move_rate_pct": 100.0,
        "cohabit_rate_pct": 100.0,
        "birth_rate_pct": 35.0,
        "divorce_rate_pct": 0.0,
        "leave_home_rate_pct": 0.0,
    }

    result_a = simulate_truth_layer(
        phase1_df=_phase1_sample(),
        scenario_population_df=_scenario_population_sample(),
        scenario_id="sim_test",
        seed=20260320,
        simulation_config=simulation_cfg,
        constraints_config=constraints_cfg,
        scenario_parameters=params,
        phase2_priors=None,
    )
    result_b = simulate_truth_layer(
        phase1_df=_phase1_sample(),
        scenario_population_df=_scenario_population_sample(),
        scenario_id="sim_test",
        seed=20260320,
        simulation_config=simulation_cfg,
        constraints_config=constraints_cfg,
        scenario_parameters=params,
        phase2_priors=None,
    )

    for table_name in (
        "truth_people",
        "truth_households",
        "truth_household_memberships",
        "truth_residence_history",
        "truth_events",
    ):
        pd.testing.assert_frame_equal(result_a[table_name], result_b[table_name])

    checks = result_a["quality"]["consistency_checks"]
    assert checks["residence_intervals_non_overlapping"] is True
    assert checks["membership_intervals_non_overlapping"] is True
    assert checks["coupled_people_colocated"] is True
    assert len(result_a["truth_events"]) > 0


def test_roommates_split_generates_group_households_and_leave_home_event_fields() -> None:
    simulation_cfg = parse_simulation_config(
        {"granularity": "monthly", "start_date": "2026-01-01", "periods": 2}
    )
    constraints_cfg = parse_constraints_config({})
    params = {
        "move_rate_pct": 0.0,
        "cohabit_rate_pct": 0.0,
        "birth_rate_pct": 0.0,
        "divorce_rate_pct": 0.0,
        "split_rate_pct": 100.0,
        "roommate_group_share_pct": 100.0,
        "roommate_household_size_min": 3,
        "roommate_household_size_max": 4,
        "roommate_age_min": 18,
        "roommate_age_max": 40,
    }

    result = simulate_truth_layer(
        phase1_df=_phase1_sample(),
        scenario_population_df=_scenario_population_sample(),
        scenario_id="roommates_split",
        seed=20260321,
        simulation_config=simulation_cfg,
        constraints_config=constraints_cfg,
        scenario_parameters=params,
        phase2_priors=None,
    )

    memberships = result["truth_household_memberships"].copy()
    start_date = "2026-01-01"
    start_memberships = memberships[memberships["MembershipStartDate"] == start_date].copy()
    counts = (
        start_memberships.groupby("HouseholdKey")["PersonKey"]
        .nunique()
        .sort_values(ascending=False)
    )
    assert int(counts.max()) >= 3

    leave_events = result["truth_events"][
        result["truth_events"]["EventType"].astype(str).str.upper() == "LEAVE_HOME"
    ].copy()
    assert not leave_events.empty
    assert leave_events["SubjectPersonKey"].astype(str).str.strip().ne("").all()
    assert leave_events["FromAddressKey"].astype(str).str.strip().ne("").all()
    assert leave_events["ToAddressKey"].astype(str).str.strip().ne("").all()
    assert (
        leave_events["FromAddressKey"].astype(str).str.strip()
        != leave_events["ToAddressKey"].astype(str).str.strip()
    ).all()
