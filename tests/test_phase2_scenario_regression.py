from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.constraints import parse_constraints_config
from sog_phase2.emission import emit_observed_datasets, parse_emission_config
from sog_phase2.params import load_phase2_params_from_project
from sog_phase2.selection import parse_selection_config, select_scenario_population
from sog_phase2.simulator import parse_simulation_config, simulate_truth_layer


SCENARIO_IDS = (
    "single_movers",
    "couple_merge",
    "family_birth",
    "divorce_custody",
    "roommates_split",
)


def _stable_key(value: Any) -> tuple[int, str]:
    text = str(value).strip()
    if text.isdigit():
        return (0, f"{int(text):020d}")
    return (1, text)


def _load_scenario(scenario_id: str) -> dict[str, Any]:
    path = PROJECT_ROOT / "phase2" / "scenarios" / f"{scenario_id}.yaml"
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Scenario YAML must be mapping: {path}")
    return payload


def _add_months(value: date, months: int) -> date:
    month_index = (value.month - 1) + months
    year = value.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(value.day, int(pd.Period(f"{year}-{month:02d}").days_in_month))
    return date(year, month, day)


def _simulation_end_date(start: date, periods: int, granularity: str) -> date:
    if periods <= 0:
        return start
    if granularity == "monthly":
        return _add_months(start, periods)
    return start + timedelta(days=periods)


def _address_on_date(residence_df: pd.DataFrame, person_key: str, event_date: date) -> str:
    person_rows = residence_df[residence_df["PersonKey"].astype(str).str.strip() == str(person_key)].copy()
    if person_rows.empty:
        return ""
    person_rows["_start"] = pd.to_datetime(person_rows["ResidenceStartDate"], errors="coerce")
    person_rows["_end"] = pd.to_datetime(person_rows["ResidenceEndDate"], errors="coerce")
    target = pd.Timestamp(event_date)
    active = person_rows[
        (person_rows["_start"].notna())
        & (person_rows["_start"] <= target)
        & ((person_rows["_end"].isna()) | (person_rows["_end"] >= target))
    ].copy()
    active = active.sort_values(by="_start", kind="mergesort")
    if active.empty:
        return ""
    return str(active.iloc[-1]["AddressKey"]).strip()


def _household_on_date(membership_df: pd.DataFrame, person_key: str, event_date: date) -> str:
    person_rows = membership_df[
        membership_df["PersonKey"].astype(str).str.strip() == str(person_key)
    ].copy()
    if person_rows.empty:
        return ""
    person_rows["_start"] = pd.to_datetime(person_rows["MembershipStartDate"], errors="coerce")
    person_rows["_end"] = pd.to_datetime(person_rows["MembershipEndDate"], errors="coerce")
    target = pd.Timestamp(event_date)
    active = person_rows[
        (person_rows["_start"].notna())
        & (person_rows["_start"] <= target)
        & ((person_rows["_end"].isna()) | (person_rows["_end"] >= target))
    ].copy()
    if active.empty:
        return ""
    active = active.sort_values(by="_start", kind="mergesort")
    return str(active.iloc[-1]["HouseholdKey"]).strip()


def _active_household_size(membership_df: pd.DataFrame, household_key: str, event_date: date) -> int:
    rows = membership_df[
        membership_df["HouseholdKey"].astype(str).str.strip() == str(household_key)
    ].copy()
    if rows.empty:
        return 0
    rows["_start"] = pd.to_datetime(rows["MembershipStartDate"], errors="coerce")
    rows["_end"] = pd.to_datetime(rows["MembershipEndDate"], errors="coerce")
    target = pd.Timestamp(event_date)
    active = rows[
        (rows["_start"].notna())
        & (rows["_start"] <= target)
        & ((rows["_end"].isna()) | (rows["_end"] >= target))
    ]
    return int(active["PersonKey"].astype(str).str.strip().nunique())


def _max_household_size_over_time(membership_df: pd.DataFrame) -> int:
    if membership_df.empty:
        return 0
    rows = membership_df.copy()
    rows["_start"] = pd.to_datetime(rows["MembershipStartDate"], errors="coerce")
    rows["_end"] = pd.to_datetime(rows["MembershipEndDate"], errors="coerce")
    max_size = 0
    for _, group in rows.groupby("HouseholdKey", dropna=False):
        points = sorted(
            {
                ts
                for ts in pd.concat([group["_start"], group["_end"].dropna()], ignore_index=True)
                if pd.notna(ts)
            }
        )
        for point in points:
            active = group[
                (group["_start"].notna())
                & (group["_start"] <= point)
                & ((group["_end"].isna()) | (group["_end"] >= point))
            ]
            size = int(active["PersonKey"].astype(str).str.strip().nunique())
            max_size = max(max_size, size)
    return max_size


@pytest.fixture(scope="module")
def scenario_results() -> dict[str, dict[str, Any]]:
    phase1_path = PROJECT_ROOT / "outputs" / "Phase1_people_addresses.csv"
    phase1_df = pd.read_csv(phase1_path, dtype=str)
    keys = sorted(
        phase1_df["PersonKey"].astype(str).str.strip().unique().tolist(),
        key=_stable_key,
    )
    selected_keys = set(keys[:1200])
    phase1_subset = phase1_df[phase1_df["PersonKey"].astype(str).str.strip().isin(selected_keys)].copy()

    params = load_phase2_params_from_project(PROJECT_ROOT)
    mobility_df = params["mobility_by_age_cohort"]
    results: dict[str, dict[str, Any]] = {}

    for scenario_id in SCENARIO_IDS:
        scenario = _load_scenario(scenario_id)
        selection_cfg = parse_selection_config(scenario.get("selection"))
        selected_df, _ = select_scenario_population(
            phase1_df=phase1_subset,
            mobility_params_df=mobility_df,
            selection_config=selection_cfg,
            seed=int(scenario["seed"]),
            scenario_id=scenario["scenario_id"],
        )
        assert not selected_df.empty

        simulation_cfg = parse_simulation_config(scenario.get("simulation"))
        constraints_cfg = parse_constraints_config(scenario.get("constraints"))
        truth = simulate_truth_layer(
            phase1_df=phase1_subset,
            scenario_population_df=selected_df,
            scenario_id=scenario["scenario_id"],
            seed=int(scenario["seed"]),
            simulation_config=simulation_cfg,
            constraints_config=constraints_cfg,
            scenario_parameters=scenario.get("parameters"),
            phase2_priors=params.get("priors_snapshot"),
        )

        emission_cfg = parse_emission_config(scenario.get("emission"))
        observed = emit_observed_datasets(
            truth_people_df=truth["truth_people"],
            truth_residence_history_df=truth["truth_residence_history"],
            simulation_start_date=simulation_cfg.start_date,
            simulation_end_date=_simulation_end_date(
                simulation_cfg.start_date,
                simulation_cfg.periods,
                simulation_cfg.granularity,
            ),
            emission_config=emission_cfg,
            seed=int(scenario["seed"]),
        )
        results[scenario_id] = {
            "scenario": scenario,
            "truth": truth,
            "observed": observed,
        }
    return results


def test_single_movers_produces_moves(scenario_results: dict[str, dict[str, Any]]) -> None:
    events = scenario_results["single_movers"]["truth"]["truth_events"]
    move_count = int((events["EventType"].astype(str).str.upper() == "MOVE").sum())
    assert move_count > 0


def test_couple_merge_produces_cohabit_and_shared_residence(
    scenario_results: dict[str, dict[str, Any]]
) -> None:
    truth = scenario_results["couple_merge"]["truth"]
    events = truth["truth_events"]
    residence = truth["truth_residence_history"]
    cohabits = events[events["EventType"].astype(str).str.upper() == "COHABIT"].copy()
    assert len(cohabits) > 0

    for _, row in cohabits.head(10).iterrows():
        person_a = str(row["PersonKeyA"]).strip()
        person_b = str(row["PersonKeyB"]).strip()
        event_date = pd.to_datetime(str(row["EventDate"]), errors="coerce").date()
        address_a = _address_on_date(residence, person_a, event_date)
        address_b = _address_on_date(residence, person_b, event_date)
        assert address_a
        assert address_b
        assert address_a == address_b


def test_family_birth_produces_birth_events(scenario_results: dict[str, dict[str, Any]]) -> None:
    truth = scenario_results["family_birth"]["truth"]
    events = truth["truth_events"]
    births = events[events["EventType"].astype(str).str.upper() == "BIRTH"].copy()
    assert len(births) > 0
    child_keys = set(births["ChildPersonKey"].astype(str).str.strip().tolist())
    people_keys = set(truth["truth_people"]["PersonKey"].astype(str).str.strip().tolist())
    assert child_keys.issubset(people_keys)


def test_divorce_custody_produces_divorce_and_split_households(
    scenario_results: dict[str, dict[str, Any]]
) -> None:
    truth = scenario_results["divorce_custody"]["truth"]
    events = truth["truth_events"]
    divorces = events[events["EventType"].astype(str).str.upper() == "DIVORCE"].copy()
    assert len(divorces) > 0
    households = truth["truth_households"]
    assert (households["HouseholdType"].astype(str).str.strip() == "post_divorce").any()


def test_one_to_many_mode_yields_multi_b_records(scenario_results: dict[str, dict[str, Any]]) -> None:
    observed = scenario_results["roommates_split"]["observed"]
    crosswalk = observed["truth_crosswalk"]
    linked = crosswalk[
        (crosswalk["A_RecordKey"].astype(str).str.strip() != "")
        & (crosswalk["B_RecordKey"].astype(str).str.strip() != "")
    ]
    assert not linked.empty
    b_counts = linked.groupby("PersonKey")["B_RecordKey"].nunique()
    assert int(b_counts.max()) >= 2


def test_roommates_split_has_household_with_three_or_more_members(
    scenario_results: dict[str, dict[str, Any]]
) -> None:
    memberships = scenario_results["roommates_split"]["truth"]["truth_household_memberships"]
    assert _max_household_size_over_time(memberships) >= 3


def test_roommates_split_contains_split_household_pattern(
    scenario_results: dict[str, dict[str, Any]]
) -> None:
    truth = scenario_results["roommates_split"]["truth"]
    events = truth["truth_events"]
    memberships = truth["truth_household_memberships"]
    residence = truth["truth_residence_history"]
    leave_events = events[events["EventType"].astype(str).str.upper() == "LEAVE_HOME"].copy()
    assert not leave_events.empty

    found_split = False
    for _, row in leave_events.iterrows():
        person_key = str(row["ChildPersonKey"]).strip()
        parsed = pd.to_datetime(str(row["EventDate"]), errors="coerce")
        if pd.isna(parsed):
            continue
        event_date = parsed.date()
        prev_date = event_date - timedelta(days=1)

        household_before = _household_on_date(memberships, person_key, prev_date)
        household_after = _household_on_date(memberships, person_key, event_date)
        if not household_before or not household_after or household_before == household_after:
            continue

        size_before = _active_household_size(memberships, household_before, prev_date)
        size_after = _active_household_size(memberships, household_before, event_date)
        if size_before < 3 or size_after >= size_before:
            continue

        address_before = _address_on_date(residence, person_key, prev_date)
        address_after = _address_on_date(residence, person_key, event_date)
        if not address_before or not address_after or address_before == address_after:
            continue

        found_split = True
        break

    assert found_split


def test_crosswalk_overlap_matches_claimed_overlap_pct(
    scenario_results: dict[str, dict[str, Any]]
) -> None:
    result = scenario_results["single_movers"]
    scenario = result["scenario"]
    observed = result["observed"]
    coverage = observed["metrics"]["coverage"]
    crosswalk = observed["truth_crosswalk"]

    overlap_people = set(
        crosswalk[
            (crosswalk["A_RecordKey"].astype(str).str.strip() != "")
            & (crosswalk["B_RecordKey"].astype(str).str.strip() != "")
        ]["PersonKey"].astype(str).str.strip().tolist()
    )
    actual_overlap = len(overlap_people)

    n_base = int(coverage["base_entities"])
    overlap_pct = float(scenario["emission"]["overlap_entity_pct"])
    appearance_a_pct = float(scenario["emission"]["appearance_A_pct"])
    appearance_b_pct = float(scenario["emission"]["appearance_B_pct"])
    target_a = min(n_base, max(0, int(round((appearance_a_pct / 100.0) * n_base))))
    target_b = min(n_base, max(0, int(round((appearance_b_pct / 100.0) * n_base))))
    expected_overlap = min(n_base, max(0, int(round((overlap_pct / 100.0) * n_base))))
    expected_overlap = min(expected_overlap, target_a, target_b)
    expected_overlap = max(expected_overlap, max(0, target_a + target_b - n_base))

    assert actual_overlap == int(coverage["overlap_entities"])
    assert actual_overlap == expected_overlap
