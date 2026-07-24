"""Reusable achieved-versus-target measurements for paper experiment E7."""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.spatial.distance import jensenshannon
from scipy.stats import linregress


HOUSEHOLD_BINS = (
    "married_couple_family",
    "single_parent_male_householder",
    "single_parent_female_householder",
    "nonfamily_living_alone",
    "nonfamily_not_alone",
)


def normalize_token(value: object) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value).upper())


def sample_sd(values: pd.Series) -> float:
    return float(values.std(ddof=1)) if len(values) > 1 else 0.0


def relative_deviation_pct(achieved: float, target: float) -> float:
    if not np.isfinite(target) or target == 0:
        return float("nan")
    return 100.0 * (float(achieved) - float(target)) / float(target)


def relative_verdict(deviation_pct: float) -> str:
    if not np.isfinite(deviation_pct):
        return "not applicable"
    magnitude = abs(float(deviation_pct))
    if magnitude <= 5.0:
        return "close"
    if magnitude <= 20.0:
        return "moderate"
    return "large deviation"


def distance_verdict(distance: float) -> str:
    if not np.isfinite(distance):
        return "not estimable"
    if distance <= 0.05:
        return "close"
    if distance <= 0.15:
        return "moderate"
    return "large deviation"


def js_distance(left: Mapping[str, float], right: Mapping[str, float]) -> float:
    keys = sorted(set(left) | set(right))
    p = np.asarray([float(left.get(key, 0.0)) for key in keys], dtype=float)
    q = np.asarray([float(right.get(key, 0.0)) for key in keys], dtype=float)
    if p.sum() <= 0 or q.sum() <= 0:
        return float("nan")
    return float(jensenshannon(p / p.sum(), q / q.sum(), base=2))


def combine_age_18_34_target(mobility: pd.DataFrame) -> float:
    indexed = mobility.set_index("age_cohort_id")
    rows = indexed.loc[["age_18_24", "age_25_34"]]
    return 100.0 * float(rows["moved_population"].sum()) / float(rows["population"].sum())


def rank_frequency_slope(distribution: pd.Series, lower: int = 10, upper: int = 1000) -> float:
    ordered = distribution[distribution > 0].sort_values(ascending=False)
    maximum = min(int(upper), len(ordered))
    if maximum < lower:
        return float("nan")
    ranks = np.arange(lower, maximum + 1, dtype=float)
    probabilities = ordered.iloc[lower - 1 : maximum].to_numpy(dtype=float)
    return float(linregress(np.log(ranks), np.log(probabilities)).slope)


def surname_source_distribution(source: pd.DataFrame, population: pd.DataFrame) -> pd.Series:
    work = source[["last_name", "ethnicity", "weight"]].copy()
    work["ethnicity_key"] = work["ethnicity"].map(normalize_token)
    population_shares = population["Ethnicity"].map(normalize_token).value_counts(normalize=True)
    conditional_total = work.groupby("ethnicity_key")["weight"].transform("sum")
    work["probability"] = (
        work["weight"].astype(float)
        / conditional_total.astype(float)
        * work["ethnicity_key"].map(population_shares).fillna(0.0)
    )
    result = work.groupby(work["last_name"].map(normalize_token))["probability"].sum()
    return result / result.sum()


def surname_achieved_distribution(population: pd.DataFrame) -> pd.Series:
    result = population["LastName"].map(normalize_token).value_counts(normalize=True)
    return result[result.index != ""].astype(float)


def surname_profile(distribution: pd.Series) -> dict[str, float]:
    ordered = distribution.sort_values(ascending=False)
    return {
        "top_10_share_pct": 100.0 * float(ordered.head(10).sum()),
        "top_50_share_pct": 100.0 * float(ordered.head(50).sum()),
        "top_100_share_pct": 100.0 * float(ordered.head(100).sum()),
        "rank_slope_10_1000": rank_frequency_slope(ordered),
    }


def coarsen_distribution(distribution: pd.Series, retained_keys: set[str]) -> pd.Series:
    """Retain named categories and pool the sampling-sensitive tail."""
    kept = distribution[distribution.index.isin(retained_keys)].copy()
    kept.loc["__OTHER__"] = float(distribution[~distribution.index.isin(retained_keys)].sum())
    return kept / kept.sum()


def _active_on(frame: pd.DataFrame, start_col: str, end_col: str, at: pd.Timestamp) -> pd.DataFrame:
    starts = pd.to_datetime(frame[start_col], errors="coerce")
    ends = pd.to_datetime(frame[end_col].replace("", pd.NA), errors="coerce")
    return frame[(starts <= at) & (ends.isna() | (ends > at))].copy()


def household_type_shares(
    households: pd.DataFrame,
    memberships: pd.DataFrame,
    people: pd.DataFrame,
    at: str | pd.Timestamp,
) -> dict[str, float]:
    snapshot = pd.Timestamp(at)
    active_households = _active_on(households, "HouseholdStartDate", "HouseholdEndDate", snapshot)
    active_memberships = _active_on(
        memberships, "MembershipStartDate", "MembershipEndDate", snapshot
    )
    gender = people.set_index(people["PersonKey"].astype(str))["Gender"].to_dict()
    grouped = {
        str(key): group for key, group in active_memberships.groupby(active_memberships["HouseholdKey"].astype(str))
    }
    counts = {key: 0 for key in HOUSEHOLD_BINS}
    for household_key in active_households["HouseholdKey"].astype(str):
        group = grouped.get(household_key)
        if group is None or group.empty:
            continue
        roles = group["HouseholdRole"].astype(str).str.upper()
        if (roles == "SPOUSE").any():
            category = "married_couple_family"
        elif len(group) == 1:
            category = "nonfamily_living_alone"
        elif (roles == "CHILD").any():
            heads = group.loc[roles == "HEAD", "PersonKey"].astype(str)
            head_gender = normalize_token(gender.get(heads.iloc[0], "")) if len(heads) else ""
            category = (
                "single_parent_male_householder"
                if head_gender in {"MALE", "M"}
                else "single_parent_female_householder"
            )
        else:
            category = "nonfamily_not_alone"
        counts[category] += 1
    total = sum(counts.values())
    return {key: (100.0 * value / total if total else float("nan")) for key, value in counts.items()}


def moved_people(residence: pd.DataFrame, baseline: str | pd.Timestamp) -> set[str]:
    after = pd.to_datetime(residence["ResidenceStartDate"], errors="coerce") > pd.Timestamp(baseline)
    return set(residence.loc[after, "PersonKey"].astype(str))


def mobility_rates(people: pd.DataFrame, residence: pd.DataFrame, baseline: str) -> dict[str, float]:
    # Births created during the simulation are not members of the baseline ACS
    # population-at-risk and must not enter the annual mover denominator.
    dob = pd.to_datetime(people.get("DOB", pd.Series(index=people.index, dtype=object)), errors="coerce")
    people = people[dob <= pd.Timestamp(baseline)].copy()
    moved = moved_people(residence, baseline)
    keys = people["PersonKey"].astype(str)
    answer = {"mobility_overall_pct": 100.0 * float(keys.isin(moved).mean())}
    age_bins = people["AgeBin"].map(normalize_token)
    for source, label in (
        ("AGE017", "mobility_age_0_17_pct"),
        ("AGE1834", "mobility_age_18_34_pct"),
        ("AGE3564", "mobility_age_35_64_pct"),
        ("AGE65PLUS", "mobility_age_65_plus_pct"),
    ):
        selected = age_bins == source
        answer[label] = 100.0 * float(keys[selected].isin(moved).mean()) if selected.any() else float("nan")
    return answer


def parse_age_band(label: str) -> tuple[int, int]:
    lower, upper = label.split("-", 1)
    # The NCHS 45-54 row uses the 45-49 female population as its denominator.
    return int(lower), 49 if label == "45-54" else int(upper)


def fertility_rates(people: pd.DataFrame, events: pd.DataFrame, bands: list[str]) -> dict[str, float]:
    births = events[events["EventType"].astype(str).str.upper() == "BIRTH"]
    parent_counts = births["Parent1PersonKey"].astype(str).value_counts()
    population = people.copy()
    population["person_key"] = population["PersonKey"].astype(str)
    female = population["Gender"].map(normalize_token).isin({"FEMALE", "F"})
    result: dict[str, float] = {}
    for band in bands:
        lower, upper = parse_age_band(band)
        eligible = population[female & population["Age"].astype(float).between(lower, upper)]
        births_in_band = int(eligible["person_key"].map(parent_counts).fillna(0).sum())
        result[band] = 1000.0 * births_in_band / len(eligible) if len(eligible) else float("nan")
    return result


def file_sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
