from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from .constraints import ConstraintConfig
from .event_grammar import COHABIT_MODES, CUSTODY_MODES, TRUTH_EVENTS_REQUIRED_COLUMNS


@dataclass(frozen=True)
class SimulationConfig:
    granularity: str = "monthly"
    start_date: date = date(2026, 1, 1)
    periods: int = 12


TRUTH_PEOPLE_COLUMNS: tuple[str, ...] = (
    "PersonKey",
    "FormalFirstName",
    "MiddleName",
    "LastName",
    "Suffix",
    "FormalFullName",
    "Gender",
    "Ethnicity",
    "DOB",
    "Age",
    "AgeBin",
    "SSN",
    "Phone",
)

TRUTH_HOUSEHOLDS_COLUMNS: tuple[str, ...] = (
    "HouseholdKey",
    "HouseholdType",
    "HouseholdStartDate",
    "HouseholdEndDate",
)

TRUTH_HOUSEHOLD_MEMBERSHIP_COLUMNS: tuple[str, ...] = (
    "PersonKey",
    "HouseholdKey",
    "HouseholdRole",
    "MembershipStartDate",
    "MembershipEndDate",
)

TRUTH_RESIDENCE_HISTORY_COLUMNS: tuple[str, ...] = (
    "PersonKey",
    "AddressKey",
    "ResidenceStartDate",
    "ResidenceEndDate",
)


def get_simulation_schema() -> dict[str, Any]:
    defaults = asdict(SimulationConfig())
    defaults["start_date"] = SimulationConfig().start_date.isoformat()
    return {
        "defaults": defaults,
        "fields": {
            "granularity": "monthly|daily",
            "start_date": "YYYY-MM-DD",
            "periods": "integer > 0",
        },
        "notes": [
            "monthly is the default and recommended granularity for Phase-2.",
            "events are committed as event-driven stochastic updates with deterministic seeding.",
        ],
    }


def _parse_date(value: Any) -> date:
    parsed = pd.to_datetime(str(value).strip(), errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"Invalid date value: {value}")
    return parsed.date()


def parse_simulation_config(
    raw: dict[str, Any] | None,
    *,
    default_start_date: date | None = None,
) -> SimulationConfig:
    cfg = raw or {}
    if not isinstance(cfg, dict):
        raise ValueError("scenario.simulation must be a mapping when provided")

    granularity = str(cfg.get("granularity", "monthly")).strip().lower()
    if granularity not in {"monthly", "daily"}:
        raise ValueError("simulation.granularity must be one of: monthly, daily")

    if "start_date" in cfg and str(cfg.get("start_date", "")).strip():
        start_date = _parse_date(cfg["start_date"])
    elif default_start_date is not None:
        start_date = default_start_date
    else:
        start_date = SimulationConfig().start_date

    periods = int(cfg.get("periods", 12))
    if periods <= 0:
        raise ValueError("simulation.periods must be > 0")

    return SimulationConfig(
        granularity=granularity,
        start_date=start_date,
        periods=periods,
    )


def _stable_key(value: Any) -> tuple[int, str]:
    text = str(value).strip()
    if text.isdigit():
        return (0, f"{int(text):020d}")
    return (1, text)


def _date_text(value: date) -> str:
    return value.isoformat()


def _previous_day(value: date) -> date:
    return value - timedelta(days=1)


def _add_months(value: date, months: int) -> date:
    month_index = (value.month - 1) + months
    year = value.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(value.day, int(pd.Period(f"{year}-{month:02d}").days_in_month))
    return date(year, month, day)


def _step_dates(config: SimulationConfig) -> list[date]:
    steps: list[date] = []
    for i in range(config.periods):
        if config.granularity == "monthly":
            steps.append(_add_months(config.start_date, i + 1))
        else:
            steps.append(config.start_date + timedelta(days=i + 1))
    return steps


def _non_empty_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    return text


def _to_int(value: Any, default: int) -> int:
    text = _non_empty_text(value)
    if not text:
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def _age_bin_for_age(age: int) -> str:
    if age <= 17:
        return "age_0_17"
    if age <= 34:
        return "age_18_34"
    if age <= 64:
        return "age_35_64"
    return "age_65_plus"


def _safe_score(value: Any, default: float = 0.5) -> float:
    text = _non_empty_text(value)
    if not text:
        return default
    try:
        return float(np.clip(float(text), 0.0, 1.0))
    except ValueError:
        return default


def _pct_to_annual_probability(value_pct: float) -> float:
    return float(np.clip(value_pct / 100.0, 0.0, 1.0))


def _annual_to_step_probability(annual_probability: float, granularity: str) -> float:
    steps_per_year = 12 if granularity == "monthly" else 365
    if annual_probability <= 0:
        return 0.0
    if annual_probability >= 1:
        return 1.0
    return float(np.clip(1.0 - ((1.0 - annual_probability) ** (1.0 / steps_per_year)), 0.0, 1.0))


def _prior_birth_rate_pct(phase2_priors: dict[str, Any] | None) -> float:
    if not isinstance(phase2_priors, dict):
        return 4.0
    fertility = phase2_priors.get("fertility")
    if not isinstance(fertility, dict):
        return 4.0
    rates = fertility.get("birth_rate_per_1000_by_age_group")
    if not isinstance(rates, dict):
        return 4.0
    chosen = []
    for age_group in ("20-24", "25-29", "30-34", "35-39"):
        value = rates.get(age_group)
        if value is None:
            continue
        try:
            chosen.append(float(value) / 10.0)
        except (TypeError, ValueError):
            continue
    if not chosen:
        return 4.0
    return float(np.mean(chosen))


def _resolve_annual_rate_pct(
    *,
    scenario_parameters: dict[str, Any] | None,
    phase2_priors: dict[str, Any] | None,
) -> dict[str, float]:
    raw = scenario_parameters or {}
    if not isinstance(raw, dict):
        raw = {}

    priors = phase2_priors if isinstance(phase2_priors, dict) else {}
    mobility_prior_pct = float(
        ((priors.get("mobility") or {}).get("overall_moved_past_year_pct") or 11.5)
    )
    marriage_per_1000 = float(
        ((priors.get("marriage_divorce") or {}).get("marriage_rate_per_1000") or 0.0)
    )
    divorce_per_1000 = float(
        ((priors.get("marriage_divorce") or {}).get("divorce_rate_per_1000") or 0.0)
    )
    marriage_prior_pct = marriage_per_1000 / 10.0
    divorce_prior_pct = divorce_per_1000 / 10.0
    birth_prior_pct = _prior_birth_rate_pct(priors)
    use_priors_for_unspecified = bool(raw.get("use_priors_for_unspecified_rates", False))

    move_default = mobility_prior_pct if use_priors_for_unspecified else 0.0
    cohabit_default = marriage_prior_pct if use_priors_for_unspecified else 0.0
    birth_default = birth_prior_pct if use_priors_for_unspecified else 0.0
    divorce_default = divorce_prior_pct if use_priors_for_unspecified else 0.0

    return {
        "move_rate_pct": float(raw.get("move_rate_pct", move_default)),
        "cohabit_rate_pct": float(raw.get("cohabit_rate_pct", cohabit_default)),
        "birth_rate_pct": float(raw.get("birth_rate_pct", birth_default)),
        "divorce_rate_pct": float(raw.get("divorce_rate_pct", divorce_default)),
        "leave_home_rate_pct": float(raw.get("leave_home_rate_pct", raw.get("split_rate_pct", 0.0))),
    }


def _resolve_step_rates(
    *,
    scenario_parameters: dict[str, Any] | None,
    phase2_priors: dict[str, Any] | None,
    granularity: str,
) -> dict[str, float]:
    annual_pct = _resolve_annual_rate_pct(
        scenario_parameters=scenario_parameters,
        phase2_priors=phase2_priors,
    )
    return {
        key: _annual_to_step_probability(_pct_to_annual_probability(value), granularity)
        for key, value in annual_pct.items()
    }


def _effective_max_gap(config: ConstraintConfig) -> int | None:
    if config.partner_age_gap_distribution:
        return max(config.partner_age_gap_distribution.keys())
    return config.max_partner_age_gap


def _empty_truth_events_df() -> pd.DataFrame:
    return pd.DataFrame(columns=list(TRUTH_EVENTS_REQUIRED_COLUMNS))


class _SimulationState:
    def __init__(
        self,
        *,
        scenario_id: str,
        seed: int,
        simulation_config: SimulationConfig,
        constraints_config: ConstraintConfig,
        selected_people: pd.DataFrame,
    ) -> None:
        self.scenario_id = str(scenario_id)
        self.seed = int(seed)
        self.simulation_config = simulation_config
        self.constraints_config = constraints_config
        self.rng = np.random.default_rng(self.seed)

        self.people_rows: list[dict[str, Any]] = []
        self.household_rows: list[dict[str, Any]] = []
        self.membership_rows: list[dict[str, Any]] = []
        self.residence_rows: list[dict[str, Any]] = []
        self.event_rows: list[dict[str, Any]] = []

        self.person_age: dict[str, int] = {}
        self.person_gender: dict[str, str] = {}
        self.person_last_name: dict[str, str] = {}
        self.person_ethnicity: dict[str, str] = {}
        self.person_mobility: dict[str, float] = {}
        self.person_partnership: dict[str, float] = {}
        self.person_fertility: dict[str, float] = {}

        self.person_current_household: dict[str, str] = {}
        self.person_current_address: dict[str, str] = {}
        self.household_members: dict[str, set[str]] = {}
        self.household_current_address: dict[str, str] = {}

        self.current_membership_row_index: dict[str, int] = {}
        self.current_residence_row_index: dict[str, int] = {}
        self.household_row_index: dict[str, int] = {}

        self.event_counter = 0
        self.household_counter = 0
        self.address_counter = 0
        self.child_counter = 0

        self.known_person_keys = set()
        self.known_household_keys = set()
        self.known_address_keys = set()
        self.active_couples: set[tuple[str, str]] = set()
        self.selected_people_keys: set[str] = set(
            selected_people["PersonKey"].astype(str).str.strip().tolist()
        )

    def _new_event_key(self) -> str:
        self.event_counter += 1
        return f"EVT_{self.event_counter:07d}"

    def _new_household_key(self) -> str:
        while True:
            self.household_counter += 1
            key = f"HH_SIM_{self.household_counter:07d}"
            if key not in self.known_household_keys:
                self.known_household_keys.add(key)
                return key

    def _new_address_key(self) -> str:
        while True:
            self.address_counter += 1
            key = f"ADDR_SIM_{self.address_counter:07d}"
            if key not in self.known_address_keys:
                self.known_address_keys.add(key)
                return key

    def _new_child_person_key(self) -> str:
        while True:
            self.child_counter += 1
            key = f"P_CHILD_{self.child_counter:07d}"
            if key not in self.known_person_keys:
                self.known_person_keys.add(key)
                return key

    def _close_open_membership(self, person_key: str, end_date: date) -> None:
        idx = self.current_membership_row_index.get(person_key)
        if idx is None:
            return
        row = self.membership_rows[idx]
        row["MembershipEndDate"] = _date_text(end_date)
        del self.current_membership_row_index[person_key]

    def _close_open_residence(self, person_key: str, end_date: date) -> None:
        idx = self.current_residence_row_index.get(person_key)
        if idx is None:
            return
        row = self.residence_rows[idx]
        row["ResidenceEndDate"] = _date_text(end_date)
        del self.current_residence_row_index[person_key]

    def _add_household(self, household_key: str, household_type: str, start_date: date, address_key: str) -> None:
        row = {
            "HouseholdKey": household_key,
            "HouseholdType": str(household_type).strip() or "unspecified",
            "HouseholdStartDate": _date_text(start_date),
            "HouseholdEndDate": "",
        }
        self.household_row_index[household_key] = len(self.household_rows)
        self.household_rows.append(row)
        self.household_members[household_key] = set()
        self.household_current_address[household_key] = address_key
        self.known_household_keys.add(household_key)
        self.known_address_keys.add(address_key)

    def _close_household_if_empty(self, household_key: str, end_date: date) -> None:
        members = self.household_members.get(household_key, set())
        if members:
            return
        idx = self.household_row_index.get(household_key)
        if idx is None:
            return
        self.household_rows[idx]["HouseholdEndDate"] = _date_text(end_date)

    def _add_membership(self, person_key: str, household_key: str, role: str, start_date: date) -> None:
        row = {
            "PersonKey": person_key,
            "HouseholdKey": household_key,
            "HouseholdRole": role,
            "MembershipStartDate": _date_text(start_date),
            "MembershipEndDate": "",
        }
        self.current_membership_row_index[person_key] = len(self.membership_rows)
        self.membership_rows.append(row)
        self.household_members.setdefault(household_key, set()).add(person_key)
        self.person_current_household[person_key] = household_key

    def _add_residence(self, person_key: str, address_key: str, start_date: date) -> None:
        row = {
            "PersonKey": person_key,
            "AddressKey": address_key,
            "ResidenceStartDate": _date_text(start_date),
            "ResidenceEndDate": "",
        }
        self.current_residence_row_index[person_key] = len(self.residence_rows)
        self.residence_rows.append(row)
        self.person_current_address[person_key] = address_key
        self.known_address_keys.add(address_key)

    def _set_person_address(self, person_key: str, to_address_key: str, on_date: date) -> tuple[str, str]:
        from_address = self.person_current_address.get(person_key, "")
        to_address = str(to_address_key).strip()
        if not to_address:
            to_address = self._new_address_key()
        if from_address == to_address:
            return from_address, to_address

        self._close_open_residence(person_key, _previous_day(on_date))
        self._add_residence(person_key, to_address, on_date)
        return from_address, to_address

    def _transfer_person_household(
        self,
        *,
        person_key: str,
        to_household_key: str,
        to_address_key: str,
        on_date: date,
        role: str,
    ) -> tuple[str, str]:
        from_household = self.person_current_household.get(person_key, "")
        if from_household and from_household != to_household_key:
            self._close_open_membership(person_key, _previous_day(on_date))
            self.household_members.setdefault(from_household, set()).discard(person_key)
            self._close_household_if_empty(from_household, _previous_day(on_date))

        if from_household != to_household_key:
            self._add_membership(person_key, to_household_key, role, on_date)
        else:
            idx = self.current_membership_row_index.get(person_key)
            if idx is not None:
                self.membership_rows[idx]["HouseholdRole"] = role

        self._set_person_address(person_key, to_address_key, on_date)
        self.household_current_address[to_household_key] = self.person_current_address[person_key]
        return from_household, to_household_key

    def _move_household(self, household_key: str, to_address_key: str, on_date: date) -> tuple[str, str]:
        members = sorted(self.household_members.get(household_key, set()), key=_stable_key)
        from_address = self.household_current_address.get(household_key, "")
        to_address = str(to_address_key).strip()
        if not to_address:
            to_address = self._new_address_key()
        if from_address == to_address:
            return from_address, to_address

        for person_key in members:
            self._set_person_address(person_key, to_address, on_date)
        self.household_current_address[household_key] = to_address
        return from_address, to_address

    def _append_event(self, event_type: str, event_date: date, **fields: Any) -> None:
        row = {column: "" for column in TRUTH_EVENTS_REQUIRED_COLUMNS}
        row["EventKey"] = self._new_event_key()
        row["EventType"] = str(event_type).strip().upper()
        row["EventDate"] = _date_text(event_date)
        for key, value in fields.items():
            if key in row:
                row[key] = _non_empty_text(value)
        self.event_rows.append(row)

    def _partner_lookup(self) -> dict[str, str]:
        partner_map: dict[str, str] = {}
        for a, b in self.active_couples:
            partner_map[a] = b
            partner_map[b] = a
        return partner_map

    def _age_gap_allowed(self, person_a: str, person_b: str) -> bool:
        max_gap = _effective_max_gap(self.constraints_config)
        if max_gap is None:
            return True
        age_a = self.person_age.get(person_a)
        age_b = self.person_age.get(person_b)
        if age_a is None or age_b is None:
            return True
        return abs(age_a - age_b) <= max_gap

    def _marriage_age_allowed(self, person_key: str) -> bool:
        if self.constraints_config.allow_underage_marriage:
            return True
        age = self.person_age.get(person_key, 0)
        return age >= self.constraints_config.min_marriage_age

    def _simulate_divorces(self, *, step_date: date, step_rates: dict[str, float], locked: set[str]) -> None:
        divorce_base = float(step_rates.get("divorce_rate_pct", 0.0))
        if divorce_base <= 0:
            return
        couples = sorted(self.active_couples, key=lambda pair: (_stable_key(pair[0]), _stable_key(pair[1])))
        for person_a, person_b in couples:
            if person_a in locked or person_b in locked:
                continue
            partnership = 0.5 * (
                self.person_partnership.get(person_a, 0.5) + self.person_partnership.get(person_b, 0.5)
            )
            probability = float(np.clip(divorce_base * (0.7 + (1.0 - partnership)), 0.0, 1.0))
            if self.rng.random() >= probability:
                continue

            custody_mode = str(self.rng.choice(np.array(CUSTODY_MODES)))
            new_household = self._new_household_key()
            new_address = self._new_address_key()
            self._add_household(new_household, "post_divorce", step_date, new_address)
            self._transfer_person_household(
                person_key=person_b,
                to_household_key=new_household,
                to_address_key=new_address,
                on_date=step_date,
                role="HEAD",
            )
            self.active_couples.discard((person_a, person_b))
            self._append_event(
                "DIVORCE",
                step_date,
                PersonKeyA=person_a,
                PersonKeyB=person_b,
                CustodyMode=custody_mode,
            )
            locked.add(person_a)
            locked.add(person_b)

    def _simulate_cohabits(self, *, step_date: date, step_rates: dict[str, float], locked: set[str]) -> None:
        cohabit_base = float(step_rates.get("cohabit_rate_pct", 0.0))
        if cohabit_base <= 0:
            return

        partner_map = self._partner_lookup()
        eligible = [
            key
            for key in sorted(self.person_current_household.keys(), key=_stable_key)
            if key not in locked and key not in partner_map and self._marriage_age_allowed(key)
        ]
        if len(eligible) < 2:
            return

        shuffled = list(np.array(eligible, dtype=object)[self.rng.permutation(len(eligible))])
        while len(shuffled) >= 2:
            person_a = str(shuffled.pop(0))
            person_b = str(shuffled.pop(0))
            if person_a in locked or person_b in locked:
                continue
            if not self._age_gap_allowed(person_a, person_b):
                continue

            partnership = 0.5 * (
                self.person_partnership.get(person_a, 0.5) + self.person_partnership.get(person_b, 0.5)
            )
            probability = float(np.clip(cohabit_base * (0.5 + partnership), 0.0, 1.0))
            if self.rng.random() >= probability:
                continue

            mode = str(self.rng.choice(np.array(COHABIT_MODES)))
            address_a = self.person_current_address.get(person_a, "")
            address_b = self.person_current_address.get(person_b, "")
            if mode == "move_to_A":
                target_address = address_a or self._new_address_key()
            elif mode == "move_to_B":
                target_address = address_b or self._new_address_key()
            else:
                target_address = self._new_address_key()

            new_household = self._new_household_key()
            self._add_household(new_household, "couple", step_date, target_address)
            self._transfer_person_household(
                person_key=person_a,
                to_household_key=new_household,
                to_address_key=target_address,
                on_date=step_date,
                role="HEAD",
            )
            self._transfer_person_household(
                person_key=person_b,
                to_household_key=new_household,
                to_address_key=target_address,
                on_date=step_date,
                role="SPOUSE",
            )
            pair = tuple(sorted((person_a, person_b), key=_stable_key))
            self.active_couples.add(pair)
            self._append_event(
                "COHABIT",
                step_date,
                PersonKeyA=person_a,
                PersonKeyB=person_b,
                NewHouseholdKey=new_household,
                CohabitMode=mode,
            )
            locked.add(person_a)
            locked.add(person_b)

    def _create_child_person(self, *, parent1: str, event_date: date) -> str:
        child_key = self._new_child_person_key()
        child_gender = "female" if self.rng.random() < 0.5 else "male"
        parent_last_name = self.person_last_name.get(parent1, "")
        parent_ethnicity = self.person_ethnicity.get(parent1, "")
        child_first = f"Child{self.child_counter:06d}"
        child_full = f"{child_first} {parent_last_name}".strip()
        ssn_tail = f"{1000 + (self.child_counter % 9000):04d}"
        child_ssn = f"900-00-{ssn_tail}"

        self.people_rows.append(
            {
                "PersonKey": child_key,
                "FormalFirstName": child_first,
                "MiddleName": "",
                "LastName": parent_last_name,
                "Suffix": "",
                "FormalFullName": child_full,
                "Gender": child_gender,
                "Ethnicity": parent_ethnicity,
                "DOB": _date_text(event_date),
                "Age": 0,
                "AgeBin": "age_0_17",
                "SSN": child_ssn,
                "Phone": "",
            }
        )
        self.person_age[child_key] = 0
        self.person_gender[child_key] = child_gender
        self.person_last_name[child_key] = parent_last_name
        self.person_ethnicity[child_key] = parent_ethnicity
        self.person_mobility[child_key] = 0.25
        self.person_partnership[child_key] = 0.05
        self.person_fertility[child_key] = 0.05
        return child_key

    def _simulate_births(self, *, step_date: date, step_rates: dict[str, float], locked: set[str]) -> None:
        birth_base = float(step_rates.get("birth_rate_pct", 0.0))
        if birth_base <= 0:
            return
        partner_map = self._partner_lookup()

        candidates = []
        for person_key in sorted(self.person_current_household.keys(), key=_stable_key):
            if person_key in locked:
                continue
            age = self.person_age.get(person_key, -1)
            if age < self.constraints_config.fertility_age_min or age > self.constraints_config.fertility_age_max:
                continue
            gender = self.person_gender.get(person_key, "").strip().lower()
            if gender and gender not in {"female", "f"}:
                continue
            candidates.append(person_key)

        if not candidates:
            return

        for parent1 in candidates:
            if parent1 in locked:
                continue
            fertility = self.person_fertility.get(parent1, 0.5)
            probability = float(np.clip(birth_base * (0.4 + fertility), 0.0, 1.0))
            if self.rng.random() >= probability:
                continue

            parent2 = partner_map.get(parent1, "")
            child_key = self._create_child_person(parent1=parent1, event_date=step_date)
            household = self.person_current_household[parent1]
            address = self.person_current_address[parent1]
            self._add_membership(child_key, household, "CHILD", step_date)
            self._add_residence(child_key, address, step_date)
            self._append_event(
                "BIRTH",
                step_date,
                Parent1PersonKey=parent1,
                Parent2PersonKey=parent2,
                ChildPersonKey=child_key,
            )
            locked.add(parent1)
            if parent2:
                locked.add(parent2)

    def _simulate_leave_home(self, *, step_date: date, step_rates: dict[str, float], locked: set[str]) -> None:
        leave_base = float(step_rates.get("leave_home_rate_pct", 0.0))
        if leave_base <= 0:
            return

        partner_map = self._partner_lookup()
        for person_key in sorted(self.person_current_household.keys(), key=_stable_key):
            if person_key in locked:
                continue
            if person_key in partner_map:
                continue
            age = self.person_age.get(person_key, 0)
            if age < 18 and not self.constraints_config.allow_child_lives_alone:
                continue
            if age < 18 or age > 30:
                continue
            current_household = self.person_current_household.get(person_key, "")
            if len(self.household_members.get(current_household, set())) <= 1:
                continue

            if self.rng.random() >= leave_base:
                continue
            from_address = self.person_current_address.get(person_key, "")
            new_household = self._new_household_key()
            new_address = self._new_address_key()
            self._add_household(new_household, "solo", step_date, new_address)
            self._transfer_person_household(
                person_key=person_key,
                to_household_key=new_household,
                to_address_key=new_address,
                on_date=step_date,
                role="HEAD",
            )
            to_address = self.person_current_address.get(person_key, "")
            self._append_event(
                "LEAVE_HOME",
                step_date,
                SubjectPersonKey=person_key,
                FromAddressKey=from_address,
                ToAddressKey=to_address,
                ChildPersonKey=person_key,
            )
            locked.add(person_key)

    def _simulate_moves(self, *, step_date: date, step_rates: dict[str, float], locked: set[str]) -> None:
        move_base = float(step_rates.get("move_rate_pct", 0.0))
        if move_base <= 0:
            return

        for person_a, person_b in sorted(
            self.active_couples,
            key=lambda pair: (_stable_key(pair[0]), _stable_key(pair[1])),
        ):
            if person_a in locked or person_b in locked:
                continue
            household_key = self.person_current_household.get(person_a, "")
            if household_key != self.person_current_household.get(person_b, ""):
                continue
            partnership_move = 0.5 * (
                self.person_mobility.get(person_a, 0.5) + self.person_mobility.get(person_b, 0.5)
            )
            probability = float(np.clip(move_base * (0.5 + partnership_move), 0.0, 1.0))
            if self.rng.random() >= probability:
                continue
            to_address = self._new_address_key()
            from_address, moved_to = self._move_household(household_key, to_address, step_date)
            if from_address == moved_to:
                continue
            self._append_event(
                "MOVE",
                step_date,
                SubjectHouseholdKey=household_key,
                FromAddressKey=from_address,
                ToAddressKey=moved_to,
            )
            for member in self.household_members.get(household_key, set()):
                locked.add(member)

        for person_key in sorted(self.person_current_household.keys(), key=_stable_key):
            if person_key in locked:
                continue
            if person_key in self._partner_lookup():
                continue
            household_key = self.person_current_household.get(person_key, "")
            if len(self.household_members.get(household_key, set())) != 1:
                continue
            mobility = self.person_mobility.get(person_key, 0.5)
            probability = float(np.clip(move_base * (0.5 + mobility), 0.0, 1.0))
            if self.rng.random() >= probability:
                continue
            to_address = self._new_address_key()
            from_address, moved_to = self._set_person_address(person_key, to_address, step_date)
            if from_address == moved_to:
                continue
            self.household_current_address[household_key] = moved_to
            self._append_event(
                "MOVE",
                step_date,
                SubjectPersonKey=person_key,
                FromAddressKey=from_address,
                ToAddressKey=moved_to,
            )
            locked.add(person_key)

    def simulate(self, *, step_rates: dict[str, float]) -> None:
        for step_date in _step_dates(self.simulation_config):
            locked: set[str] = set()
            self._simulate_divorces(step_date=step_date, step_rates=step_rates, locked=locked)
            self._simulate_cohabits(step_date=step_date, step_rates=step_rates, locked=locked)
            self._simulate_births(step_date=step_date, step_rates=step_rates, locked=locked)
            self._simulate_leave_home(step_date=step_date, step_rates=step_rates, locked=locked)
            self._simulate_moves(step_date=step_date, step_rates=step_rates, locked=locked)

    def to_dataframes(self) -> dict[str, pd.DataFrame]:
        truth_people = pd.DataFrame(self.people_rows, columns=list(TRUTH_PEOPLE_COLUMNS))
        truth_households = pd.DataFrame(self.household_rows, columns=list(TRUTH_HOUSEHOLDS_COLUMNS))
        truth_memberships = pd.DataFrame(
            self.membership_rows,
            columns=list(TRUTH_HOUSEHOLD_MEMBERSHIP_COLUMNS),
        )
        truth_residence = pd.DataFrame(
            self.residence_rows,
            columns=list(TRUTH_RESIDENCE_HISTORY_COLUMNS),
        )
        truth_events = (
            pd.DataFrame(self.event_rows, columns=list(TRUTH_EVENTS_REQUIRED_COLUMNS))
            if self.event_rows
            else _empty_truth_events_df()
        )

        if not truth_people.empty:
            truth_people = truth_people.sort_values(
                by=["PersonKey"],
                key=lambda s: s.map(_stable_key),
                kind="mergesort",
            ).reset_index(drop=True)
        if not truth_households.empty:
            truth_households = truth_households.sort_values(
                by=["HouseholdKey", "HouseholdStartDate"],
                kind="mergesort",
            ).reset_index(drop=True)
        if not truth_memberships.empty:
            truth_memberships = truth_memberships.sort_values(
                by=["PersonKey", "MembershipStartDate"],
                key=lambda s: s.map(_stable_key) if s.name == "PersonKey" else s,
                kind="mergesort",
            ).reset_index(drop=True)
        if not truth_residence.empty:
            truth_residence = truth_residence.sort_values(
                by=["PersonKey", "ResidenceStartDate"],
                key=lambda s: s.map(_stable_key) if s.name == "PersonKey" else s,
                kind="mergesort",
            ).reset_index(drop=True)
        if not truth_events.empty:
            truth_events = truth_events.sort_values(
                by=["EventDate", "EventKey"],
                kind="mergesort",
            ).reset_index(drop=True)

        return {
            "truth_people": truth_people,
            "truth_households": truth_households,
            "truth_household_memberships": truth_memberships,
            "truth_residence_history": truth_residence,
            "truth_events": truth_events,
        }


def _validate_columns(df: pd.DataFrame, required: set[str], label: str) -> None:
    missing = [column for column in sorted(required) if column not in df.columns]
    if missing:
        raise ValueError(f"{label} missing required columns: {', '.join(missing)}")


def _build_selected_people_baseline(
    *,
    phase1_df: pd.DataFrame,
    scenario_population_df: pd.DataFrame,
    simulation_start_date: date,
) -> pd.DataFrame:
    _validate_columns(
        phase1_df,
        {
            "RecordKey",
            "PersonKey",
            "EntityRecordIndex",
            "AddressKey",
            "FormalFirstName",
            "MiddleName",
            "LastName",
            "Suffix",
            "FormalFullName",
            "Gender",
            "Ethnicity",
            "DOB",
            "Age",
            "AgeBin",
            "SSN",
            "ResidenceType",
        },
        "phase1_df",
    )
    _validate_columns(
        scenario_population_df,
        {
            "PersonKey",
            "MobilityPropensityScore",
            "PartnershipPropensityScore",
            "FertilityPropensityScore",
        },
        "scenario_population_df",
    )

    scenario_people = scenario_population_df.copy()
    scenario_people["PersonKey"] = scenario_people["PersonKey"].astype(str).str.strip()
    selected_keys = set(scenario_people["PersonKey"].tolist())
    if not selected_keys:
        return pd.DataFrame(columns=["PersonKey"])

    phase1 = phase1_df.copy()
    if "Phone" not in phase1.columns:
        phase1["Phone"] = ""
    phase1["PersonKey"] = phase1["PersonKey"].astype(str).str.strip()
    phase1 = phase1[phase1["PersonKey"].isin(selected_keys)].copy()
    if phase1.empty:
        raise ValueError("scenario_population has no PersonKey values present in phase1_df")

    phase1["EntityRecordIndex_int"] = pd.to_numeric(phase1["EntityRecordIndex"], errors="coerce").fillna(0).astype(int)
    phase1["RecordKey_int"] = pd.to_numeric(phase1["RecordKey"], errors="coerce").fillna(0).astype(int)
    phase1 = phase1.sort_values(
        by=["PersonKey", "EntityRecordIndex_int", "RecordKey_int"],
        key=lambda s: s.map(_stable_key) if s.name == "PersonKey" else s,
        kind="mergesort",
    )
    first_rows = phase1.groupby("PersonKey", as_index=False).first()
    last_rows = phase1.groupby("PersonKey", as_index=False).last()

    baseline = first_rows[
        [
            "PersonKey",
            "FormalFirstName",
            "MiddleName",
            "LastName",
            "Suffix",
            "FormalFullName",
            "Gender",
            "Ethnicity",
            "DOB",
            "Age",
            "AgeBin",
            "SSN",
            "Phone",
        ]
    ].merge(
        last_rows[
            [
                "PersonKey",
                "AddressKey",
                "ResidenceType",
                "ResidenceStartDate",
            ]
        ],
        on="PersonKey",
        how="left",
    )

    needed_keys = set(selected_keys)
    found_keys = set(baseline["PersonKey"].astype(str).tolist())
    missing_keys = sorted(needed_keys - found_keys, key=_stable_key)
    if missing_keys:
        raise ValueError(f"Selected PersonKey values missing in Phase-1 baseline: {', '.join(missing_keys[:20])}")

    baseline = baseline.merge(
        scenario_people[
            [
                "PersonKey",
                "MobilityPropensityScore",
                "PartnershipPropensityScore",
                "FertilityPropensityScore",
            ]
        ],
        on="PersonKey",
        how="left",
    )
    baseline["SimulationStartDate"] = _date_text(simulation_start_date)
    return baseline.sort_values(
        by=["PersonKey"],
        key=lambda s: s.map(_stable_key),
        kind="mergesort",
    ).reset_index(drop=True)


def _apply_roommate_baseline_grouping(
    *,
    baseline_df: pd.DataFrame,
    scenario_id: str,
    seed: int,
    scenario_parameters: dict[str, Any] | None,
) -> pd.DataFrame:
    baseline = baseline_df.copy()
    person_key_series = baseline["PersonKey"].astype(str).str.strip()
    baseline["InitHouseholdKey"] = ""
    baseline["InitHouseholdType"] = ""
    baseline["InitHouseholdRole"] = ""
    baseline["InitAddressKey"] = baseline["AddressKey"].map(_non_empty_text)

    if str(scenario_id).strip().lower() != "roommates_split":
        return baseline

    raw = scenario_parameters or {}
    if not isinstance(raw, dict):
        raw = {}
    if not bool(raw.get("enable_roommate_baseline_groups", True)):
        return baseline

    group_share_pct = float(raw.get("roommate_group_share_pct", 40.0))
    min_size = max(2, int(raw.get("roommate_household_size_min", 3)))
    max_size = max(min_size, int(raw.get("roommate_household_size_max", 5)))
    age_min = int(raw.get("roommate_age_min", 18))
    age_max = int(raw.get("roommate_age_max", 30))
    if group_share_pct <= 0:
        return baseline

    ages = pd.to_numeric(baseline["Age"], errors="coerce").fillna(-1)
    eligible_people = person_key_series[(ages >= age_min) & (ages <= age_max)].tolist()
    if not eligible_people:
        return baseline

    target_count = int(round((group_share_pct / 100.0) * len(eligible_people)))
    target_count = min(max(target_count, min_size), len(eligible_people))
    if target_count < min_size:
        return baseline

    rng = np.random.default_rng(int(seed) + 1879)
    shuffled = list(np.array(eligible_people, dtype=object)[rng.permutation(len(eligible_people))])
    selected = [str(value).strip() for value in shuffled[:target_count]]
    selected = [value for value in selected if value]
    if len(selected) < min_size:
        return baseline

    address_by_person = {
        str(row["PersonKey"]).strip(): _non_empty_text(row.get("AddressKey"))
        for _, row in baseline.iterrows()
    }
    household_map: dict[str, str] = {}
    role_map: dict[str, str] = {}
    address_map: dict[str, str] = {}

    remaining = list(selected)
    group_index = 0
    while len(remaining) >= min_size:
        if group_index == 0 and len(remaining) >= 3:
            size = max(min_size, 3)
        else:
            size = int(rng.integers(min_size, max_size + 1))
        size = min(size, len(remaining))
        if len(remaining) - size == 1:
            if size < max_size and len(remaining) >= size + 1:
                size += 1
            elif size > min_size:
                size -= 1
        if size < min_size:
            break

        group_people = remaining[:size]
        remaining = remaining[size:]
        group_index += 1
        household_key = f"HH_ROOM_BASE_{group_index:06d}"
        default_address = f"ADDR_ROOM_BASE_{group_index:06d}"
        group_address = ""
        for person_key in group_people:
            group_address = address_by_person.get(person_key, "")
            if group_address:
                break
        group_address = group_address or default_address

        for idx, person_key in enumerate(group_people):
            household_map[person_key] = household_key
            role_map[person_key] = "HEAD" if idx == 0 else "ROOMMATE"
            address_map[person_key] = group_address

    if not household_map:
        return baseline

    assigned_households = person_key_series.map(household_map)
    assigned_roles = person_key_series.map(role_map)
    assigned_addresses = person_key_series.map(address_map)

    baseline.loc[assigned_households.notna(), "InitHouseholdKey"] = assigned_households.dropna()
    baseline.loc[assigned_households.notna(), "InitHouseholdType"] = "roommates"
    baseline.loc[assigned_roles.notna(), "InitHouseholdRole"] = assigned_roles.dropna()
    baseline.loc[assigned_addresses.notna(), "InitAddressKey"] = assigned_addresses.dropna()
    return baseline


def _check_non_overlapping_intervals(
    df: pd.DataFrame,
    *,
    entity_col: str,
    start_col: str,
    end_col: str,
) -> tuple[bool, int]:
    if df.empty:
        return True, 0
    working = df.copy()
    working["_start"] = pd.to_datetime(working[start_col], errors="coerce")
    working["_end"] = pd.to_datetime(working[end_col], errors="coerce")
    working = working.sort_values(by=[entity_col, "_start"], kind="mergesort")

    violations = 0
    for _, group in working.groupby(entity_col, dropna=False):
        prev_end: pd.Timestamp | None = None
        prev_start: pd.Timestamp | None = None
        for _, row in group.iterrows():
            start = row["_start"]
            end = row["_end"]
            if pd.isna(start):
                violations += 1
                continue
            if not pd.isna(end) and end < start:
                violations += 1
            if prev_start is not None:
                effective_prev_end = prev_end if prev_end is not None else pd.Timestamp.max
                if start <= effective_prev_end:
                    violations += 1
            prev_start = start
            prev_end = None if pd.isna(end) else end
    return violations == 0, violations


def _check_couple_colocation(
    *,
    active_couples: set[tuple[str, str]],
    person_household: dict[str, str],
    person_address: dict[str, str],
) -> tuple[bool, int]:
    violations = 0
    for person_a, person_b in active_couples:
        household_a = person_household.get(person_a, "")
        household_b = person_household.get(person_b, "")
        address_a = person_address.get(person_a, "")
        address_b = person_address.get(person_b, "")
        if household_a != household_b or address_a != address_b:
            violations += 1
    return violations == 0, violations


def simulate_truth_layer(
    *,
    phase1_df: pd.DataFrame,
    scenario_population_df: pd.DataFrame,
    scenario_id: str,
    seed: int,
    simulation_config: SimulationConfig,
    constraints_config: ConstraintConfig,
    scenario_parameters: dict[str, Any] | None = None,
    phase2_priors: dict[str, Any] | None = None,
) -> dict[str, Any]:
    baseline = _build_selected_people_baseline(
        phase1_df=phase1_df,
        scenario_population_df=scenario_population_df,
        simulation_start_date=simulation_config.start_date,
    )
    baseline = _apply_roommate_baseline_grouping(
        baseline_df=baseline,
        scenario_id=scenario_id,
        seed=seed,
        scenario_parameters=scenario_parameters,
    )
    state = _SimulationState(
        scenario_id=scenario_id,
        seed=seed,
        simulation_config=simulation_config,
        constraints_config=constraints_config,
        selected_people=baseline,
    )

    for _, row in baseline.iterrows():
        person_key = _non_empty_text(row.get("PersonKey"))
        if not person_key:
            continue
        state.known_person_keys.add(person_key)
        state.people_rows.append(
            {
                "PersonKey": person_key,
                "FormalFirstName": _non_empty_text(row.get("FormalFirstName")),
                "MiddleName": _non_empty_text(row.get("MiddleName")),
                "LastName": _non_empty_text(row.get("LastName")),
                "Suffix": _non_empty_text(row.get("Suffix")),
                "FormalFullName": _non_empty_text(row.get("FormalFullName")),
                "Gender": _non_empty_text(row.get("Gender")),
                "Ethnicity": _non_empty_text(row.get("Ethnicity")),
                "DOB": _non_empty_text(row.get("DOB")),
                "Age": _to_int(row.get("Age"), 0),
                "AgeBin": _non_empty_text(row.get("AgeBin")) or _age_bin_for_age(_to_int(row.get("Age"), 0)),
                "SSN": _non_empty_text(row.get("SSN")),
                "Phone": _non_empty_text(row.get("Phone")),
            }
        )
        state.person_age[person_key] = _to_int(row.get("Age"), 0)
        state.person_gender[person_key] = _non_empty_text(row.get("Gender"))
        state.person_last_name[person_key] = _non_empty_text(row.get("LastName"))
        state.person_ethnicity[person_key] = _non_empty_text(row.get("Ethnicity"))
        state.person_mobility[person_key] = _safe_score(row.get("MobilityPropensityScore"), default=0.5)
        state.person_partnership[person_key] = _safe_score(row.get("PartnershipPropensityScore"), default=0.5)
        state.person_fertility[person_key] = _safe_score(row.get("FertilityPropensityScore"), default=0.5)

        configured_household = _non_empty_text(row.get("InitHouseholdKey"))
        configured_household_type = _non_empty_text(row.get("InitHouseholdType")).lower()
        configured_role = _non_empty_text(row.get("InitHouseholdRole")).upper()
        address_key = (
            _non_empty_text(row.get("InitAddressKey"))
            or _non_empty_text(row.get("AddressKey"))
            or state._new_address_key()
        )

        if configured_household:
            household_key = configured_household
            household_type = configured_household_type or "roommates"
            if household_key not in state.known_household_keys:
                state._add_household(
                    household_key=household_key,
                    household_type=household_type,
                    start_date=simulation_config.start_date,
                    address_key=address_key,
                )
            else:
                shared_address = state.household_current_address.get(household_key, "")
                if not shared_address:
                    shared_address = address_key or state._new_address_key()
                    state.household_current_address[household_key] = shared_address
                address_key = shared_address
            household_role = configured_role or (
                "HEAD" if not state.household_members.get(household_key) else "ROOMMATE"
            )
        else:
            household_key = f"HH_BASE_{person_key}"
            if household_key in state.known_household_keys:
                household_key = state._new_household_key()
            else:
                state.known_household_keys.add(household_key)
            residence_type = _non_empty_text(row.get("ResidenceType")).lower()
            household_type = f"solo_{residence_type}" if residence_type else "solo"
            state._add_household(
                household_key=household_key,
                household_type=household_type,
                start_date=simulation_config.start_date,
                address_key=address_key,
            )
            household_role = "HEAD"

        state._add_membership(
            person_key=person_key,
            household_key=household_key,
            role=household_role,
            start_date=simulation_config.start_date,
        )
        state._add_residence(
            person_key=person_key,
            address_key=address_key,
            start_date=simulation_config.start_date,
        )

    step_rates = _resolve_step_rates(
        scenario_parameters=scenario_parameters,
        phase2_priors=phase2_priors,
        granularity=simulation_config.granularity,
    )
    state.simulate(step_rates=step_rates)
    outputs = state.to_dataframes()

    residence_ok, residence_violations = _check_non_overlapping_intervals(
        outputs["truth_residence_history"],
        entity_col="PersonKey",
        start_col="ResidenceStartDate",
        end_col="ResidenceEndDate",
    )
    membership_ok, membership_violations = _check_non_overlapping_intervals(
        outputs["truth_household_memberships"],
        entity_col="PersonKey",
        start_col="MembershipStartDate",
        end_col="MembershipEndDate",
    )
    couples_ok, couple_violations = _check_couple_colocation(
        active_couples=state.active_couples,
        person_household=state.person_current_household,
        person_address=state.person_current_address,
    )

    quality = {
        "consistency_checks": {
            "residence_intervals_non_overlapping": residence_ok,
            "membership_intervals_non_overlapping": membership_ok,
            "coupled_people_colocated": couples_ok,
        },
        "violation_counts": {
            "residence_overlap_or_date_order": int(residence_violations),
            "membership_overlap_or_date_order": int(membership_violations),
            "couple_colocation": int(couple_violations),
        },
        "event_counts": outputs["truth_events"]["EventType"].value_counts().to_dict(),
    }

    return {
        **outputs,
        "quality": quality,
        "simulation_meta": {
            "scenario_id": scenario_id,
            "seed": int(seed),
            "config": {
                "granularity": simulation_config.granularity,
                "start_date": simulation_config.start_date.isoformat(),
                "periods": simulation_config.periods,
            },
            "step_event_probabilities": step_rates,
            "annual_rate_pct_inputs": _resolve_annual_rate_pct(
                scenario_parameters=scenario_parameters,
                phase2_priors=phase2_priors,
            ),
        },
    }
