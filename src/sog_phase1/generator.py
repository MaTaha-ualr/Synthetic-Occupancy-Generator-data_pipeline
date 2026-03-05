from __future__ import annotations

import json
import math
import re
import shutil
import string
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import (
    NormalizedDistribution,
    load_phase1_config,
    normalize_distribution,
    resolve_age_bins,
    validate_phase1_core,
)


@dataclass(frozen=True)
class NamePool:
    names: np.ndarray
    probs: np.ndarray


@dataclass(frozen=True)
class PreparedTables:
    first_names: pd.DataFrame
    last_names: pd.DataFrame
    streets: list[str]
    cities: list[str]
    states: list[str]
    demographics: dict[str, Any]


class AddressGenerator:
    def __init__(
        self,
        streets: list[str],
        cities: list[str],
        states: list[str],
        address_cfg: dict[str, Any],
        *,
        seed: int,
    ) -> None:
        self.streets = streets
        self.cities = cities
        self.states = states

        self.n_streets = len(streets)
        self.n_cities = len(cities)
        self.n_states = len(states)

        self.house_min = int(address_cfg["house_number_min"])
        self.house_max = int(address_cfg["house_number_max"])
        self.n_house_numbers = self.house_max - self.house_min + 1
        self.base_space = self.n_house_numbers * self.n_streets * self.n_cities * self.n_states
        self.base_offset = (
            (abs(int(seed)) * 6364136223846793005 + 1442695040888963407) % self.base_space
            if self.base_space > 0
            else 0
        )
        self.base_stride = self._find_coprime_stride(self.base_space, int(seed))

        apartment_cfg = address_cfg.get("apartment", {})
        self.units_per_building = int(apartment_cfg.get("units_per_building", 40))

        self.unit_format_dist = normalize_distribution(
            apartment_cfg.get("unit_format_distribution", {"numeric_3digit": 100}),
            label="address.apartment.unit_format_distribution",
            auto_normalize=True,
        )
        self.unit_type_dist = normalize_distribution(
            apartment_cfg.get("unit_type_distribution", {"APT": 100}),
            label="address.apartment.unit_type_distribution",
            auto_normalize=True,
        )

        self.seed = int(seed)
        mailing_cfg = address_cfg.get("mailing", {})
        self.mailing_style = str(mailing_cfg.get("style", "ohc_po_box")).strip().lower()

        # OHC-like behavior: Mailing is either blank or PO BOX (all related fields populated).
        self.house_po_box_pct = float(mailing_cfg.get("house_po_box_pct", 42.0))
        self.apartment_po_box_pct = float(mailing_cfg.get("apartment_po_box_pct", 44.0))
        self.apartment_shared_po_box_pct = float(mailing_cfg.get("apartment_shared_po_box_pct", 100.0))
        self.po_box_zip_keep_pct = float(mailing_cfg.get("po_box_zip_keep_pct", 17.0))
        self.po_box_zip_shift_min = int(mailing_cfg.get("po_box_zip_shift_min", 1))
        self.po_box_zip_shift_max = int(mailing_cfg.get("po_box_zip_shift_max", 799))

        self.po_box_number_digits_dist = normalize_distribution(
            mailing_cfg.get(
                "po_box_number_digits",
                {"d3": 10.0, "d4": 42.0, "d5": 48.0},
            ),
            label="address.mailing.po_box_number_digits",
            auto_normalize=True,
        )
        self.po_box_route_digits_dist = normalize_distribution(
            mailing_cfg.get(
                "po_box_route_digits",
                {"d3": 15.0, "d4": 35.0, "d5": 25.0, "d6": 25.0},
            ),
            label="address.mailing.po_box_route_digits",
            auto_normalize=True,
        )

    @staticmethod
    def _find_coprime_stride(space: int, seed: int) -> int:
        if space <= 1:
            return 1
        candidate = (
            (abs(seed) * 11400714819323198485 + 7046029254386353131) % space
        )
        if candidate == 0:
            candidate = 1
        if candidate % 2 == 0:
            candidate += 1
        while math.gcd(candidate, space) != 1:
            candidate = (candidate + 2) % space
            if candidate == 0:
                candidate = 1
        return candidate

    def ensure_capacity(self, n_houses: int, n_apartments: int) -> None:
        buildings_needed = math.ceil(n_apartments / self.units_per_building) if n_apartments > 0 else 0
        required_base_slots = n_houses + buildings_needed
        if required_base_slots > self.base_space:
            raise ValueError(
                "Address combination space is too small for requested n_people. "
                f"Required base slots={required_base_slots}, capacity={self.base_space}."
            )

    def _decode_base(self, base_index: int) -> tuple[int, str, str, str]:
        if base_index >= self.base_space:
            raise ValueError(f"Base index {base_index} exceeds address space {self.base_space}")
        mixed_index = (base_index * self.base_stride + self.base_offset) % self.base_space
        q, state_idx = divmod(mixed_index, self.n_states)
        q, city_idx = divmod(q, self.n_cities)
        q, street_idx = divmod(q, self.n_streets)
        house_offset = q
        street_number = self.house_min + house_offset
        return street_number, self.streets[street_idx], self.cities[city_idx], self.states[state_idx]

    def _pick_from_distribution(self, dist: NormalizedDistribution, token: float) -> str:
        cumulative = 0.0
        for key, prob in dist.probabilities.items():
            cumulative += prob
            if token <= cumulative:
                return key
        return next(reversed(dist.probabilities))

    def _unit_number(self, unit_idx: int) -> str:
        # Deterministic pseudo-random format selection for unit number style.
        token = ((unit_idx * 2654435761) & 0xFFFFFFFF) / 4294967296.0
        fmt = self._pick_from_distribution(self.unit_format_dist, token)
        if fmt == "numeric_3digit":
            return f"{101 + unit_idx}"
        if fmt == "floor_letter":
            floor = 1 + (unit_idx // 26)
            letter = string.ascii_uppercase[unit_idx % 26]
            return f"{floor}{letter}"
        if fmt == "wing_numeric":
            wing = string.ascii_uppercase[(unit_idx // 100) % 26]
            return f"{wing}{(unit_idx % 100) + 1:02d}"
        return f"{101 + unit_idx}"

    def _unit_type(self, building_idx: int, unit_idx: int) -> str:
        token = ((building_idx * 1301 + unit_idx * 7919) % 10000) / 10000.0
        return self._pick_from_distribution(self.unit_type_dist, token)

    @staticmethod
    def _extract_digits(key: str, default_digits: int) -> int:
        m = re.search(r"(\d+)", str(key))
        if not m:
            return default_digits
        digits = int(m.group(1))
        return max(1, min(10, digits))

    @staticmethod
    def _number_from_token(digits: int, token: int) -> str:
        if digits <= 1:
            lo, hi = 1, 9
        else:
            lo, hi = 10 ** (digits - 1), (10 ** digits) - 1
        value = lo + (abs(int(token)) % (hi - lo + 1))
        return str(value)

    def _make_po_box_profile(self, *, profile_seed: int, residence_zip: str) -> tuple[str, str, str]:
        token_a = (profile_seed * 1103515245 + self.seed * 12345 + 11) & 0xFFFFFFFF
        token_b = (profile_seed * 214013 + self.seed * 2531011 + 17) & 0xFFFFFFFF
        token_zip = (profile_seed * 1664525 + self.seed * 1013904223 + 29) & 0xFFFFFFFF

        d1_key = self._pick_from_distribution(self.po_box_number_digits_dist, token_a / 4294967296.0)
        d2_key = self._pick_from_distribution(self.po_box_route_digits_dist, token_b / 4294967296.0)
        d1 = self._extract_digits(d1_key, default_digits=4)
        d2 = self._extract_digits(d2_key, default_digits=5)

        address1 = self._number_from_token(d1, token_a)
        address2 = self._number_from_token(d2, token_b)

        if residence_zip.isdigit() and len(residence_zip) == 5:
            base_zip = int(residence_zip)
        else:
            base_zip = int(self._postal_code(abs(profile_seed) % self.base_space))

        keep_token = token_zip / 4294967296.0
        if keep_token < (self.po_box_zip_keep_pct / 100.0):
            mailing_zip = f"{base_zip:05d}"
        else:
            span = max(1, self.po_box_zip_shift_max - self.po_box_zip_shift_min + 1)
            shift = self.po_box_zip_shift_min + (token_zip % span)
            mailing_zip = f"{((base_zip + shift - 1) % 100000) + 1:05d}"

        return address1, address2, mailing_zip

    @staticmethod
    def _postal_code(base_index: int) -> str:
        return f"{10000 + ((base_index * 7919) % 89999):05d}"

    def generate_batch(
        self,
        *,
        start_index: int,
        count: int,
        n_houses_total: int,
        rng: np.random.Generator,
    ) -> dict[str, list[Any]]:
        residence_type: list[str] = []
        street_number: list[int] = []
        street_name: list[str] = []
        unit_type: list[str] = []
        unit_number: list[str] = []
        city: list[str] = []
        state: list[str] = []
        postal_code: list[str] = []
        mailing_mode: list[str] = []
        mailing_street_number: list[str] = []
        mailing_street_name: list[str] = []
        mailing_unit_type: list[str] = []
        mailing_unit_number: list[str] = []
        mailing_city: list[str] = []
        mailing_state: list[str] = []
        mailing_postal_code: list[str] = []

        end_index = start_index + count
        for global_index in range(start_index, end_index):
            if global_index < n_houses_total:
                base_index = global_index
                st_no, st_name, city_name, state_name = self._decode_base(base_index)
                postal = self._postal_code(base_index)
                residence_type.append("HOUSE")
                street_number.append(st_no)
                street_name.append(st_name)
                unit_type.append("")
                unit_number.append("")
                city.append(city_name)
                state.append(state_name)
                postal_code.append(postal)

                if rng.random() < (self.house_po_box_pct / 100.0):
                    mail_a1, mail_a2, mail_zip = self._make_po_box_profile(
                        profile_seed=base_index,
                        residence_zip=postal,
                    )
                    mailing_mode.append("PO BOX")
                    mailing_street_number.append(mail_a1)
                    mailing_street_name.append("PO BOX")
                    mailing_unit_type.append("")
                    mailing_unit_number.append(mail_a2)
                    mailing_city.append(city_name)
                    mailing_state.append(state_name)
                    mailing_postal_code.append(mail_zip)
                else:
                    mailing_mode.append("")
                    mailing_street_number.append("")
                    mailing_street_name.append("")
                    mailing_unit_type.append("")
                    mailing_unit_number.append("")
                    mailing_city.append("")
                    mailing_state.append("")
                    mailing_postal_code.append("")
            else:
                apartment_seq = global_index - n_houses_total
                building_idx = apartment_seq // self.units_per_building
                unit_idx = apartment_seq % self.units_per_building
                base_index = n_houses_total + building_idx
                st_no, st_name, city_name, state_name = self._decode_base(base_index)
                postal = self._postal_code(base_index)
                unit_ty = self._unit_type(building_idx, unit_idx)
                unit_no = self._unit_number(unit_idx)
                residence_type.append("APARTMENT")
                street_number.append(st_no)
                street_name.append(st_name)
                unit_type.append(unit_ty)
                unit_number.append(unit_no)
                city.append(city_name)
                state.append(state_name)
                postal_code.append(postal)

                if rng.random() < (self.apartment_po_box_pct / 100.0):
                    shared = rng.random() < (self.apartment_shared_po_box_pct / 100.0)
                    profile_seed = base_index if shared else global_index
                    mail_a1, mail_a2, mail_zip = self._make_po_box_profile(
                        profile_seed=profile_seed,
                        residence_zip=postal,
                    )
                    mailing_mode.append("PO BOX")
                    mailing_street_number.append(mail_a1)
                    mailing_street_name.append("PO BOX")
                    mailing_unit_type.append("")
                    mailing_unit_number.append(mail_a2)
                    mailing_city.append(city_name)
                    mailing_state.append(state_name)
                    mailing_postal_code.append(mail_zip)
                else:
                    mailing_mode.append("")
                    mailing_street_number.append("")
                    mailing_street_name.append("")
                    mailing_unit_type.append("")
                    mailing_unit_number.append("")
                    mailing_city.append("")
                    mailing_state.append("")
                    mailing_postal_code.append("")

        return {
            "ResidenceType": residence_type,
            "ResidenceStreetNumber": street_number,
            "ResidenceStreetName": street_name,
            "ResidenceUnitType": unit_type,
            "ResidenceUnitNumber": unit_number,
            "ResidenceCity": city,
            "ResidenceState": state,
            "ResidencePostalCode": postal_code,
            "MailingAddressMode": mailing_mode,
            "MailingStreetNumber": mailing_street_number,
            "MailingStreetName": mailing_street_name,
            "MailingUnitType": mailing_unit_type,
            "MailingUnitNumber": mailing_unit_number,
            "MailingCity": mailing_city,
            "MailingState": mailing_state,
            "MailingPostalCode": mailing_postal_code,
        }


def _load_prepared(prepared_dir: Path) -> PreparedTables:
    first_names_path = prepared_dir / "first_names.parquet"
    last_names_path = prepared_dir / "last_names.parquet"
    streets_path = prepared_dir / "streets.parquet"
    cities_path = prepared_dir / "cities.parquet"
    states_path = prepared_dir / "states.parquet"
    demographics_path = prepared_dir / "demographics.json"

    missing = [p for p in [first_names_path, last_names_path, streets_path, cities_path, states_path, demographics_path] if not p.exists()]
    if missing:
        missing_list = ", ".join(str(p) for p in missing)
        raise FileNotFoundError(
            "Prepared cache is incomplete. Run scripts/build_prepared.py first. Missing: " + missing_list
        )

    first_names = pd.read_parquet(first_names_path)
    last_names = pd.read_parquet(last_names_path)
    streets = pd.read_parquet(streets_path)["street_base_name"].astype(str).tolist()
    cities = pd.read_parquet(cities_path)["city_name"].astype(str).tolist()
    states = pd.read_parquet(states_path)["state_name"].astype(str).tolist()
    demographics = json.loads(demographics_path.read_text(encoding="utf-8"))
    return PreparedTables(
        first_names=first_names,
        last_names=last_names,
        streets=streets,
        cities=cities,
        states=states,
        demographics=demographics,
    )


def _build_pool(df: pd.DataFrame, name_col: str, weight_col: str) -> NamePool:
    if df.empty:
        raise ValueError("Cannot create pool from empty dataframe")
    names = df[name_col].astype(str).to_numpy()
    weights = df[weight_col].astype(float).to_numpy()
    total = float(weights.sum())
    if total <= 0:
        raise ValueError("Pool weights must sum to > 0")
    probs = weights / total
    return NamePool(names=names, probs=probs)


def _build_first_name_pools(
    first_names: pd.DataFrame, unisex_multiplier: float
) -> tuple[dict[str, NamePool], NamePool, NamePool]:
    base = first_names.copy()
    base["sex"] = base["sex"].astype(str).str.lower()
    base["weight"] = base["weight"].astype(float)

    female_df = base[base["sex"].isin(["female", "unisex"])].copy()
    female_df.loc[female_df["sex"] == "unisex", "weight"] *= unisex_multiplier

    male_df = base[base["sex"].isin(["male", "unisex"])].copy()
    male_df.loc[male_df["sex"] == "unisex", "weight"] *= unisex_multiplier

    other_df = base[base["sex"] == "unisex"].copy()
    if other_df.empty:
        other_df = base.copy()

    pools = {
        "female": _build_pool(female_df, "name", "weight"),
        "male": _build_pool(male_df, "name", "weight"),
        "other": _build_pool(other_df, "name", "weight"),
    }
    default_pool = _build_pool(base, "name", "weight")
    unisex_df = base[base["sex"] == "unisex"].copy()
    if unisex_df.empty:
        unisex_pool = default_pool
    else:
        unisex_pool = _build_pool(unisex_df, "name", "weight")
    return pools, default_pool, unisex_pool


def _build_last_name_pools(last_names: pd.DataFrame) -> tuple[dict[str, NamePool], NamePool]:
    pools: dict[str, NamePool] = {}
    for ethnicity, subset in last_names.groupby("ethnicity"):
        pools[str(ethnicity)] = _build_pool(subset, "last_name", "weight")
    default_pool = _build_pool(last_names, "last_name", "weight")
    return pools, default_pool


def _resolve_ethnicity_distribution(phase1: dict[str, Any], demographics: dict[str, Any]) -> dict[str, float]:
    configured = phase1.get("distributions", {}).get("ethnicity")
    if configured:
        return {str(k): float(v) for k, v in configured.items()}
    inferred = demographics.get("ethnicity_distribution_pct_for_last_name", {})
    if not inferred:
        raise ValueError(
            "No ethnicity distribution found in config and none available in prepared demographics.json"
        )
    return {str(k): float(v) for k, v in inferred.items()}


def _normalize_housing_mix(address_cfg: dict[str, Any]) -> NormalizedDistribution:
    return normalize_distribution(
        {
            "houses": float(address_cfg.get("houses_pct", 0)),
            "apartments": float(address_cfg.get("apartments_pct", 0)),
        },
        label="address.housing_mix",
        auto_normalize=True,
    )


def _years_ago(reference: date, years: int) -> date:
    try:
        return reference.replace(year=reference.year - years)
    except ValueError:
        # Handle leap day.
        return reference.replace(month=2, day=28, year=reference.year - years)


def _age_on_date(dob: date, reference: date) -> int:
    years = reference.year - dob.year
    if (reference.month, reference.day) < (dob.month, dob.day):
        years -= 1
    return years


def _make_ssn(person_key: int, seed: int) -> str:
    area = ((person_key * 37 + seed) % 899) + 1
    if area == 666:
        area = 665
    group = ((person_key * 73 + seed) % 99) + 1
    serial = ((person_key * 193 + seed) % 9999) + 1
    return f"{area:03d}-{group:02d}-{serial:04d}"


def _make_phone(person_key: int, seed: int) -> str:
    area = 200 + ((person_key * 31 + seed) % 800)
    exchange = 200 + ((person_key * 17 + seed) % 800)
    subscriber = (person_key * 43 + seed) % 10000
    return f"{area:03d}-{exchange:03d}-{subscriber:04d}"


def _distribution_to_choice_inputs(dist: NormalizedDistribution) -> tuple[np.ndarray, np.ndarray]:
    keys = np.array(list(dist.probabilities.keys()), dtype=object)
    probs = np.array([dist.probabilities[k] for k in keys], dtype=float)
    return keys, probs


def _draw_exact_categories(
    keys: np.ndarray, probs: np.ndarray, n: int, rng: np.random.Generator
) -> np.ndarray:
    """Draw categories with exact total count and low drift from target proportions."""
    if n <= 0:
        return np.array([], dtype=object)
    raw = probs * n
    counts = np.floor(raw).astype(int)
    remainder = int(n - counts.sum())
    if remainder > 0:
        frac = raw - counts
        # Randomized tie-break to avoid deterministic bias for equal fractions.
        tie = rng.random(len(keys)) * 1e-12
        order = np.argsort(-(frac + tie))
        counts[order[:remainder]] += 1
    out = np.repeat(keys, counts.astype(int))
    rng.shuffle(out)
    return out


def _distribution_pct(counter: Counter[str], total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {k: (v / total) * 100.0 for k, v in counter.items()}


def _within_tolerance(
    expected_pct: dict[str, float], achieved_pct: dict[str, float], tolerance_pct: float
) -> dict[str, dict[str, Any]]:
    keys = sorted(set(expected_pct) | set(achieved_pct))
    checks: dict[str, dict[str, Any]] = {}
    for key in keys:
        expected = float(expected_pct.get(key, 0.0))
        achieved = float(achieved_pct.get(key, 0.0))
        delta = achieved - expected
        checks[key] = {
            "expected_pct": expected,
            "achieved_pct": achieved,
            "delta_pct": delta,
            "within_tolerance": abs(delta) <= tolerance_pct,
        }
    return checks


def _name_with_optional_parts(first: str, middle: str, last: str, suffix: str) -> str:
    parts = [first]
    if middle:
        parts.append(middle)
    parts.append(last)
    full = " ".join(parts)
    if suffix:
        full = f"{full} {suffix}"
    return full


def _apply_forced_exact_name_duplicates(
    *,
    first_names: np.ndarray,
    middle_names: np.ndarray,
    last_names: np.ndarray,
    suffixes: np.ndarray,
    genders: np.ndarray,
    ethnicities: np.ndarray,
    duplicate_people_pct: float,
    rng: np.random.Generator,
) -> dict[str, int]:
    chunk_count = len(first_names)
    if chunk_count <= 1 or duplicate_people_pct <= 0:
        return {"pairs": 0, "people": 0}

    target_people = int(round(chunk_count * (duplicate_people_pct / 100.0)))
    target_people = max(0, min(chunk_count, target_people))
    if target_people < 2:
        return {"pairs": 0, "people": 0}

    target_pairs = target_people // 2
    if target_pairs == 0:
        return {"pairs": 0, "people": 0}

    selected_pairs: list[tuple[int, int]] = []
    used: set[int] = set()

    bucket_members: dict[tuple[str, str], list[int]] = {}
    for idx in range(chunk_count):
        bucket_key = (str(genders[idx]).lower(), str(ethnicities[idx]))
        bucket_members.setdefault(bucket_key, []).append(idx)

    for indices in bucket_members.values():
        rng.shuffle(indices)
        i = 0
        while i + 1 < len(indices) and len(selected_pairs) < target_pairs:
            a = int(indices[i])
            b = int(indices[i + 1])
            if a not in used and b not in used:
                selected_pairs.append((a, b))
                used.add(a)
                used.add(b)
            i += 2
        if len(selected_pairs) >= target_pairs:
            break

    if len(selected_pairs) < target_pairs:
        remaining = [idx for idx in range(chunk_count) if idx not in used]
        rng.shuffle(remaining)
        i = 0
        while i + 1 < len(remaining) and len(selected_pairs) < target_pairs:
            a = int(remaining[i])
            b = int(remaining[i + 1])
            selected_pairs.append((a, b))
            used.add(a)
            used.add(b)
            i += 2

    for src_idx, dst_idx in selected_pairs:
        first_names[dst_idx] = first_names[src_idx]
        middle_names[dst_idx] = middle_names[src_idx]
        last_names[dst_idx] = last_names[src_idx]
        suffixes[dst_idx] = suffixes[src_idx]

    forced_pairs = len(selected_pairs)
    forced_people = forced_pairs * 2
    return {"pairs": forced_pairs, "people": forced_people}


def generate_phase1_dataset(
    *,
    project_root: Path,
    config_path: Path,
    prepared_dir: Path,
    overwrite: bool = False,
) -> dict[str, Any]:
    phase1 = load_phase1_config(config_path)
    validate_phase1_core(phase1)
    prepared = _load_prepared(prepared_dir)

    gender_norm = normalize_distribution(
        phase1.get("distributions", {}).get("gender", {}),
        label="distributions.gender",
        auto_normalize=True,
    )
    ethnicity_norm = normalize_distribution(
        _resolve_ethnicity_distribution(phase1, prepared.demographics),
        label="distributions.ethnicity",
        auto_normalize=True,
    )
    selected_age_bins, age_norm = resolve_age_bins(phase1.get("age_bins", {}))
    age_bin_lookup = {item["id"]: item for item in selected_age_bins}

    housing_norm = _normalize_housing_mix(phase1.get("address", {}))

    n_people = int(phase1["n_people"])
    seed = int(phase1.get("seed", 0))
    chunk_size = int(phase1["output"]["chunk_size"])
    output_format = str(phase1["output"]["format"]).lower()
    output_path = (project_root / str(phase1["output"]["path"])).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_format == "csv":
        if output_path.exists() and not overwrite:
            raise FileExistsError(f"Output file already exists: {output_path}")
        if output_path.exists() and overwrite:
            output_path.unlink()
    else:
        parts_dir = output_path.parent / f"{output_path.stem}_parts"
        if parts_dir.exists() and not overwrite:
            raise FileExistsError(f"Parquet parts directory already exists: {parts_dir}")
        if parts_dir.exists() and overwrite:
            shutil.rmtree(parts_dir)
        parts_dir.mkdir(parents=True, exist_ok=True)

    first_name_pools, first_name_default_pool, middle_name_pool = _build_first_name_pools(
        prepared.first_names, float(phase1.get("distributions", {}).get("unisex_weight_multiplier", 0.35))
    )
    last_name_pools, last_name_default_pool = _build_last_name_pools(prepared.last_names)

    address_gen = AddressGenerator(
        prepared.streets,
        prepared.cities,
        prepared.states,
        phase1["address"],
        seed=seed,
    )

    n_houses = int(round(n_people * housing_norm.probabilities["houses"]))
    n_houses = max(0, min(n_people, n_houses))
    n_apartments = n_people - n_houses
    address_gen.ensure_capacity(n_houses, n_apartments)

    rng = np.random.default_rng(seed)
    reference_date = date.today()

    gender_keys, gender_probs = _distribution_to_choice_inputs(gender_norm)
    ethnicity_keys, ethnicity_probs = _distribution_to_choice_inputs(ethnicity_norm)
    age_keys = np.array([item["id"] for item in selected_age_bins], dtype=object)
    age_probs = np.array([age_norm.probabilities[key] for key in age_keys], dtype=float)

    fill_rates = phase1.get("fill_rates", {})
    middle_fill_rate = float(fill_rates.get("middle_name", 0.0))
    suffix_fill_rate = float(fill_rates.get("suffix", 0.0))
    phone_fill_rate = float(fill_rates.get("phone", 1.0))
    duplicate_name_pct = float(phase1.get("name_duplication", {}).get("exact_full_name_people_pct", 0.0))

    suffix_dist = normalize_distribution(
        phase1.get("suffix_distribution", {"Jr": 100.0}),
        label="suffix_distribution",
        auto_normalize=True,
    )
    suffix_keys, suffix_probs = _distribution_to_choice_inputs(suffix_dist)

    residence_cfg = phase1.get("residence_dates", {})
    start_year_min = int(residence_cfg.get("start_year_min", reference_date.year - 30))
    start_min_date = date(start_year_min, 1, 1)
    start_min_ordinal = start_min_date.toordinal()
    ref_ordinal = reference_date.toordinal()
    open_ended_rate = float(residence_cfg.get("open_ended_pct", 80.0)) / 100.0
    min_duration_days = int(residence_cfg.get("min_duration_days", 90))

    output_columns = [
        "PersonKey",
        "AddressKey",
        "FirstName",
        "MiddleName",
        "LastName",
        "Suffix",
        "FullName",
        "Gender",
        "Ethnicity",
        "DOB",
        "Age",
        "AgeBin",
        "SSN",
        "Phone",
        "ResidenceType",
        "ResidenceStreetNumber",
        "ResidenceStreetName",
        "ResidenceUnitType",
        "ResidenceUnitNumber",
        "ResidenceCity",
        "ResidenceState",
        "ResidencePostalCode",
        "ResidenceStartDate",
        "ResidenceEndDate",
        "MailingAddressMode",
        "MailingStreetNumber",
        "MailingStreetName",
        "MailingUnitType",
        "MailingUnitNumber",
        "MailingCity",
        "MailingState",
        "MailingPostalCode",
    ]

    missing_counts = {col: 0 for col in output_columns}
    gender_counts: Counter[str] = Counter()
    ethnicity_counts: Counter[str] = Counter()
    age_bin_counts: Counter[str] = Counter()
    forced_duplicate_name_pairs = 0
    forced_duplicate_name_people = 0
    chunk_files: list[str] = []

    for chunk_start in range(0, n_people, chunk_size):
        chunk_count = min(chunk_size, n_people - chunk_start)

        person_keys = np.arange(chunk_start + 1, chunk_start + chunk_count + 1, dtype=np.int64)
        address_keys = person_keys.copy()

        genders = _draw_exact_categories(gender_keys, gender_probs, chunk_count, rng)
        ethnicities = _draw_exact_categories(ethnicity_keys, ethnicity_probs, chunk_count, rng)
        sampled_age_bin_ids = _draw_exact_categories(age_keys, age_probs, chunk_count, rng)

        first_names = np.empty(chunk_count, dtype=object)
        for gender_value in np.unique(genders):
            key = str(gender_value).lower()
            pool = first_name_pools.get(key, first_name_default_pool)
            idx = np.where(genders == gender_value)[0]
            first_names[idx] = rng.choice(pool.names, size=len(idx), p=pool.probs)

        last_names = np.empty(chunk_count, dtype=object)
        for ethnicity_value in np.unique(ethnicities):
            pool = last_name_pools.get(str(ethnicity_value), last_name_default_pool)
            idx = np.where(ethnicities == ethnicity_value)[0]
            last_names[idx] = rng.choice(pool.names, size=len(idx), p=pool.probs)

        middle_fill_mask = rng.random(chunk_count) < middle_fill_rate
        middle_names = np.array([""] * chunk_count, dtype=object)
        sampled_middle_names = rng.choice(
            middle_name_pool.names, size=chunk_count, p=middle_name_pool.probs
        )
        middle_names[middle_fill_mask] = sampled_middle_names[middle_fill_mask]

        suffix_fill_mask = rng.random(chunk_count) < suffix_fill_rate
        suffixes = np.array([""] * chunk_count, dtype=object)
        sampled_suffixes = rng.choice(suffix_keys, size=chunk_count, p=suffix_probs)
        suffixes[suffix_fill_mask] = sampled_suffixes[suffix_fill_mask]

        dup_stats = _apply_forced_exact_name_duplicates(
            first_names=first_names,
            middle_names=middle_names,
            last_names=last_names,
            suffixes=suffixes,
            genders=genders,
            ethnicities=ethnicities,
            duplicate_people_pct=duplicate_name_pct,
            rng=rng,
        )
        forced_duplicate_name_pairs += int(dup_stats["pairs"])
        forced_duplicate_name_people += int(dup_stats["people"])

        dobs = np.empty(chunk_count, dtype=object)
        ages = np.empty(chunk_count, dtype=np.int64)
        for age_bin_id in np.unique(sampled_age_bin_ids):
            idx = np.where(sampled_age_bin_ids == age_bin_id)[0]
            cfg = age_bin_lookup[str(age_bin_id)]
            min_age = int(cfg["min_age"])
            max_age = int(cfg["max_age"])

            max_dob = _years_ago(reference_date, min_age)
            min_dob = _years_ago(reference_date, max_age + 1) + timedelta(days=1)
            min_ordinal = min_dob.toordinal()
            max_ordinal = max_dob.toordinal()
            sampled_ordinals = rng.integers(min_ordinal, max_ordinal + 1, size=len(idx))
            sampled_dates = [date.fromordinal(int(value)) for value in sampled_ordinals]
            dobs[idx] = [d.isoformat() for d in sampled_dates]
            ages[idx] = [_age_on_date(d, reference_date) for d in sampled_dates]

        ssn_values = np.array([_make_ssn(int(k), seed) for k in person_keys], dtype=object)

        phone_fill_mask = rng.random(chunk_count) < phone_fill_rate
        phones = np.array([""] * chunk_count, dtype=object)
        for i, key in enumerate(person_keys):
            if phone_fill_mask[i]:
                phones[i] = _make_phone(int(key), seed)

        residence_start_ordinals = rng.integers(start_min_ordinal, ref_ordinal + 1, size=chunk_count)
        residence_start_dates = np.array(
            [date.fromordinal(int(value)).isoformat() for value in residence_start_ordinals], dtype=object
        )
        residence_end_dates = np.array([""] * chunk_count, dtype=object)
        open_ended_mask = rng.random(chunk_count) < open_ended_rate
        for i in range(chunk_count):
            if open_ended_mask[i]:
                continue
            min_end_ordinal = int(residence_start_ordinals[i]) + min_duration_days
            if min_end_ordinal > ref_ordinal:
                continue
            end_ordinal = int(rng.integers(min_end_ordinal, ref_ordinal + 1))
            residence_end_dates[i] = date.fromordinal(end_ordinal).isoformat()

        full_names = np.array(
            [
                _name_with_optional_parts(
                    str(first_names[i]),
                    str(middle_names[i]),
                    str(last_names[i]),
                    str(suffixes[i]),
                )
                for i in range(chunk_count)
            ],
            dtype=object,
        )

        address_batch = address_gen.generate_batch(
            start_index=chunk_start,
            count=chunk_count,
            n_houses_total=n_houses,
            rng=rng,
        )

        df = pd.DataFrame(
            {
                "PersonKey": person_keys,
                "AddressKey": address_keys,
                "FirstName": first_names,
                "MiddleName": middle_names,
                "LastName": last_names,
                "Suffix": suffixes,
                "FullName": full_names,
                "Gender": genders,
                "Ethnicity": ethnicities,
                "DOB": dobs,
                "Age": ages,
                "AgeBin": sampled_age_bin_ids,
                "SSN": ssn_values,
                "Phone": phones,
                "ResidenceType": address_batch["ResidenceType"],
                "ResidenceStreetNumber": address_batch["ResidenceStreetNumber"],
                "ResidenceStreetName": address_batch["ResidenceStreetName"],
                "ResidenceUnitType": address_batch["ResidenceUnitType"],
                "ResidenceUnitNumber": address_batch["ResidenceUnitNumber"],
                "ResidenceCity": address_batch["ResidenceCity"],
                "ResidenceState": address_batch["ResidenceState"],
                "ResidencePostalCode": address_batch["ResidencePostalCode"],
                "ResidenceStartDate": residence_start_dates,
                "ResidenceEndDate": residence_end_dates,
                "MailingAddressMode": address_batch["MailingAddressMode"],
                "MailingStreetNumber": address_batch["MailingStreetNumber"],
                "MailingStreetName": address_batch["MailingStreetName"],
                "MailingUnitType": address_batch["MailingUnitType"],
                "MailingUnitNumber": address_batch["MailingUnitNumber"],
                "MailingCity": address_batch["MailingCity"],
                "MailingState": address_batch["MailingState"],
                "MailingPostalCode": address_batch["MailingPostalCode"],
            },
            columns=output_columns,
        )

        if output_format == "csv":
            df.to_csv(output_path, index=False, mode="a", header=(chunk_start == 0))
        else:
            part_path = parts_dir / f"part_{(chunk_start // chunk_size) + 1:05d}.parquet"
            df.to_parquet(part_path, index=False)
            chunk_files.append(str(part_path))

        gender_counts.update(df["Gender"].astype(str).tolist())
        ethnicity_counts.update(df["Ethnicity"].astype(str).tolist())
        age_bin_counts.update(df["AgeBin"].astype(str).tolist())

        for col in output_columns:
            series = df[col]
            if pd.api.types.is_numeric_dtype(series):
                missing_counts[col] += int(series.isna().sum())
            else:
                missing_counts[col] += int(series.fillna("").astype(str).str.strip().eq("").sum())

    achieved_gender_pct = _distribution_pct(gender_counts, n_people)
    achieved_ethnicity_pct = _distribution_pct(ethnicity_counts, n_people)
    achieved_age_pct = _distribution_pct(age_bin_counts, n_people)

    tolerance_pct = float(phase1.get("quality", {}).get("distribution_tolerance_pct", 1.5))
    gender_checks = _within_tolerance(gender_norm.normalized_percentages, achieved_gender_pct, tolerance_pct)
    ethnicity_checks = _within_tolerance(ethnicity_norm.normalized_percentages, achieved_ethnicity_pct, tolerance_pct)
    age_checks = _within_tolerance(age_norm.normalized_percentages, achieved_age_pct, tolerance_pct)

    missingness_pct = {col: (count / n_people) * 100.0 for col, count in missing_counts.items()}
    name_duplication_metrics: dict[str, Any] = {
        "target_exact_full_name_people_pct": duplicate_name_pct,
        "forced_duplicate_name_pairs": forced_duplicate_name_pairs,
        "forced_duplicate_name_people": forced_duplicate_name_people,
        "forced_duplicate_name_people_pct": (forced_duplicate_name_people / n_people) * 100.0,
        "actual_duplicate_name_people_pct": None,
    }

    exact_uniqueness_check_max = int(phase1.get("quality", {}).get("exact_uniqueness_check_max_rows", 250000))
    uniqueness_checks: dict[str, Any] = {
        "person_key_unique": True,
        "address_key_unique": True,
        "full_address_unique": True,
        "method": "deterministic_by_construction",
        "checked_exactly": False,
        "duplicate_counts": {
            "PersonKey": 0,
            "AddressKey": 0,
            "full_address": 0,
        },
    }

    if n_people <= exact_uniqueness_check_max:
        if output_format == "csv":
            check_df = pd.read_csv(output_path, dtype=str)
        else:
            frames = [pd.read_parquet(path) for path in chunk_files]
            check_df = pd.concat(frames, ignore_index=True).astype(str)
        full_address_cols = [
            "ResidenceStreetNumber",
            "ResidenceStreetName",
            "ResidenceUnitType",
            "ResidenceUnitNumber",
            "ResidenceCity",
            "ResidenceState",
            "ResidencePostalCode",
        ]
        person_dups = int(check_df.duplicated(["PersonKey"]).sum())
        address_dups = int(check_df.duplicated(["AddressKey"]).sum())
        full_dups = int(check_df.duplicated(full_address_cols).sum())
        full_name_counts = check_df["FullName"].value_counts()
        dup_name_people = int(full_name_counts[full_name_counts > 1].sum())
        name_duplication_metrics["actual_duplicate_name_people_pct"] = (dup_name_people / n_people) * 100.0
        uniqueness_checks = {
            "person_key_unique": person_dups == 0,
            "address_key_unique": address_dups == 0,
            "full_address_unique": full_dups == 0,
            "method": "exact",
            "checked_exactly": True,
            "duplicate_counts": {
                "PersonKey": person_dups,
                "AddressKey": address_dups,
                "full_address": full_dups,
            },
        }
    if name_duplication_metrics["actual_duplicate_name_people_pct"] is None:
        name_duplication_metrics["actual_duplicate_name_people_pct"] = name_duplication_metrics[
            "forced_duplicate_name_people_pct"
        ]

    stem = output_path.stem
    manifest_path = output_path.parent / f"{stem}.manifest.json"
    quality_path = output_path.parent / f"{stem}.quality_report.json"

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": f"{stem}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "seed": seed,
        "records_requested": n_people,
        "records_written": n_people,
        "output_format": output_format,
        "output_path": str(output_path),
        "output_parts": chunk_files,
        "chunk_size": chunk_size,
        "housing_counts": {
            "houses": n_houses,
            "apartments": n_apartments,
        },
        "name_duplication": name_duplication_metrics,
        "normalization": {
            "gender": {
                "raw_percentages": gender_norm.raw_percentages,
                "raw_sum": gender_norm.raw_sum,
                "normalized": gender_norm.normalized,
                "normalized_percentages": gender_norm.normalized_percentages,
            },
            "ethnicity": {
                "raw_percentages": ethnicity_norm.raw_percentages,
                "raw_sum": ethnicity_norm.raw_sum,
                "normalized": ethnicity_norm.normalized,
                "normalized_percentages": ethnicity_norm.normalized_percentages,
            },
            "age_bins": {
                "raw_percentages": age_norm.raw_percentages,
                "raw_sum": age_norm.raw_sum,
                "normalized": age_norm.normalized,
                "normalized_percentages": age_norm.normalized_percentages,
                "selected_bins": selected_age_bins,
                "enabled_bins_only": bool(phase1.get("age_bins", {}).get("enabled_bins_only", True)),
                "pct_interpretation": str(phase1.get("age_bins", {}).get("pct_interpretation", "absolute")),
            },
            "housing_mix": {
                "raw_percentages": housing_norm.raw_percentages,
                "raw_sum": housing_norm.raw_sum,
                "normalized": housing_norm.normalized,
                "normalized_percentages": housing_norm.normalized_percentages,
            },
        },
        "config_snapshot": phase1,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    quality_report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "row_count": n_people,
        "tolerance_pct": tolerance_pct,
        "expected_distributions_pct": {
            "gender": gender_norm.normalized_percentages,
            "ethnicity": ethnicity_norm.normalized_percentages,
            "age_bins": age_norm.normalized_percentages,
        },
        "achieved_distributions_pct": {
            "gender": achieved_gender_pct,
            "ethnicity": achieved_ethnicity_pct,
            "age_bins": achieved_age_pct,
        },
        "distribution_checks": {
            "gender": gender_checks,
            "ethnicity": ethnicity_checks,
            "age_bins": age_checks,
        },
        "name_duplication": name_duplication_metrics,
        "uniqueness_checks": uniqueness_checks,
        "missingness_pct": missingness_pct,
    }
    quality_path.write_text(json.dumps(quality_report, indent=2), encoding="utf-8")

    return {
        "output_path": str(output_path),
        "output_parts": chunk_files,
        "manifest_path": str(manifest_path),
        "quality_report_path": str(quality_path),
        "n_people": n_people,
    }
