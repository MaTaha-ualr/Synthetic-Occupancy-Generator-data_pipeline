from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class NormalizedDistribution:
    probabilities: dict[str, float]
    raw_percentages: dict[str, float]
    normalized_percentages: dict[str, float]
    raw_sum: float
    normalized: bool


def load_phase1_config(config_path: Path) -> dict[str, Any]:
    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if "phase1" not in data:
        raise ValueError(f"Expected top-level key 'phase1' in {config_path}")
    phase1 = data["phase1"]
    if not isinstance(phase1, dict):
        raise ValueError("The 'phase1' config section must be a mapping")
    return phase1


def _clean_dist_map(dist: dict[str, Any], label: str) -> dict[str, float]:
    cleaned: dict[str, float] = {}
    for key, value in dist.items():
        if value is None:
            continue
        val = float(value)
        if val < 0:
            raise ValueError(f"{label}: negative percentage/weight for '{key}' is not allowed")
        cleaned[str(key)] = val
    if not cleaned:
        raise ValueError(f"{label}: no usable values were found")
    return cleaned


def normalize_distribution(
    dist: dict[str, Any],
    *,
    label: str,
    auto_normalize: bool = True,
) -> NormalizedDistribution:
    cleaned = _clean_dist_map(dist, label)
    raw_sum = float(sum(cleaned.values()))
    if raw_sum <= 0:
        raise ValueError(f"{label}: sum of values must be > 0")

    if abs(raw_sum - 100.0) < 1e-9:
        normalized = False
        normalized_pct = {k: float(v) for k, v in cleaned.items()}
    elif auto_normalize:
        normalized = True
        normalized_pct = {k: (v / raw_sum) * 100.0 for k, v in cleaned.items()}
    else:
        raise ValueError(
            f"{label}: percentages sum to {raw_sum:.6f}, but auto_normalize is disabled "
            "(expected exactly 100)"
        )

    probabilities = {k: v / 100.0 for k, v in normalized_pct.items()}
    return NormalizedDistribution(
        probabilities=probabilities,
        raw_percentages=cleaned,
        normalized_percentages=normalized_pct,
        raw_sum=raw_sum,
        normalized=normalized,
    )


def resolve_age_bins(age_cfg: dict[str, Any]) -> tuple[list[dict[str, Any]], NormalizedDistribution]:
    bins = age_cfg.get("bins", [])
    if not bins:
        raise ValueError("age_bins.bins is required and cannot be empty")

    enabled_bins_only = bool(age_cfg.get("enabled_bins_only", True))
    auto_normalize = bool(age_cfg.get("auto_normalize", True))

    selected: list[dict[str, Any]] = []
    for item in bins:
        if enabled_bins_only and not bool(item.get("enabled", False)):
            continue
        min_age = int(item["min_age"])
        max_age = int(item["max_age"])
        if min_age < 0 or max_age < min_age:
            raise ValueError(f"Invalid age range in bin '{item.get('id', 'unknown')}'")
        selected.append(
            {
                "id": str(item["id"]),
                "label": str(item.get("label", item["id"])),
                "min_age": min_age,
                "max_age": max_age,
                "pct": float(item["pct"]),
            }
        )

    if not selected:
        raise ValueError("No active age bins after applying enabled_bins_only rule")

    dist = {item["id"]: item["pct"] for item in selected}
    normalized = normalize_distribution(
        dist,
        label="age_bins",
        auto_normalize=auto_normalize,
    )
    return selected, normalized


def validate_phase1_core(phase1: dict[str, Any]) -> None:
    n_people = int(phase1.get("n_people", 0))
    if n_people <= 0:
        raise ValueError("phase1.n_people must be > 0")

    seed = int(phase1.get("seed", 0))
    max_seed = (2**63) - 1
    if seed < 0 or seed > max_seed:
        raise ValueError(f"phase1.seed must be between 0 and {max_seed}")

    output_cfg = phase1.get("output", {})
    output_format = str(output_cfg.get("format", "csv")).lower()
    if output_format not in {"csv", "parquet"}:
        raise ValueError("phase1.output.format must be one of: csv, parquet")

    chunk_size = int(output_cfg.get("chunk_size", 0))
    if chunk_size <= 0:
        raise ValueError("phase1.output.chunk_size must be > 0")

    address_cfg = phase1.get("address", {})
    houses_pct = float(address_cfg.get("houses_pct", 0))
    apartments_pct = float(address_cfg.get("apartments_pct", 0))
    if houses_pct < 0 or apartments_pct < 0:
        raise ValueError("address houses/apartments percentages cannot be negative")
    if houses_pct + apartments_pct <= 0:
        raise ValueError("address houses_pct + apartments_pct must be > 0")

    house_min = int(address_cfg.get("house_number_min", 1))
    house_max = int(address_cfg.get("house_number_max", 1))
    if house_min <= 0 or house_max < house_min:
        raise ValueError("address house number range is invalid")

    units_per_building = int(address_cfg.get("apartment", {}).get("units_per_building", 0))
    if units_per_building <= 0:
        raise ValueError("address.apartment.units_per_building must be > 0")

    name_dup_cfg = phase1.get("name_duplication", {})
    exact_name_pct = float(name_dup_cfg.get("exact_full_name_people_pct", 0.0))
    if exact_name_pct < 0 or exact_name_pct > 100:
        raise ValueError("name_duplication.exact_full_name_people_pct must be between 0 and 100")

    fill_rates = phase1.get("fill_rates", {})
    for key in ("middle_name", "suffix", "phone"):
        val = float(fill_rates.get(key, 0.0))
        if val < 0 or val > 1:
            raise ValueError(f"fill_rates.{key} must be between 0 and 1")

    mailing_cfg = address_cfg.get("mailing", {})
    style = str(mailing_cfg.get("style", "ohc_po_box")).strip().lower()
    if style == "ohc_po_box" or "house_po_box_pct" in mailing_cfg or "apartment_po_box_pct" in mailing_cfg:
        for key in (
            "house_po_box_pct",
            "apartment_po_box_pct",
            "apartment_shared_po_box_pct",
            "po_box_zip_keep_pct",
        ):
            val = float(mailing_cfg.get(key, 0.0))
            if val < 0 or val > 100:
                raise ValueError(f"address.mailing.{key} must be between 0 and 100")
        zip_shift_min = int(mailing_cfg.get("po_box_zip_shift_min", 1))
        zip_shift_max = int(mailing_cfg.get("po_box_zip_shift_max", 799))
        if zip_shift_min < 0 or zip_shift_max < zip_shift_min:
            raise ValueError("address.mailing po_box_zip_shift_min/max are invalid")
        if float(mailing_cfg.get("house_po_box_pct", 0.0)) < 0:
            raise ValueError("address.mailing.house_po_box_pct must be >= 0")
        if float(mailing_cfg.get("apartment_po_box_pct", 0.0)) < 0:
            raise ValueError("address.mailing.apartment_po_box_pct must be >= 0")
    else:
        # Backward compatibility for legacy same-as-residence model.
        for key in (
            "house_same_as_residence_pct",
            "house_blank_pct",
            "apartment_shared_complex_pct",
            "apartment_same_as_residence_pct",
            "apartment_blank_pct",
        ):
            val = float(mailing_cfg.get(key, 0.0))
            if val < 0 or val > 100:
                raise ValueError(f"address.mailing.{key} must be between 0 and 100")
        if float(mailing_cfg.get("house_same_as_residence_pct", 0.0)) + float(
            mailing_cfg.get("house_blank_pct", 0.0)
        ) <= 0:
            raise ValueError("address.mailing house distribution total must be > 0")
        if (
            float(mailing_cfg.get("apartment_shared_complex_pct", 0.0))
            + float(mailing_cfg.get("apartment_same_as_residence_pct", 0.0))
            + float(mailing_cfg.get("apartment_blank_pct", 0.0))
            <= 0
        ):
            raise ValueError("address.mailing apartment distribution total must be > 0")

    residence_cfg = phase1.get("residence_dates", {})
    open_ended_pct = float(residence_cfg.get("open_ended_pct", 0.0))
    if open_ended_pct < 0 or open_ended_pct > 100:
        raise ValueError("residence_dates.open_ended_pct must be between 0 and 100")
    start_year_min = int(residence_cfg.get("start_year_min", 1900))
    if start_year_min < 1900 or start_year_min > 2100:
        raise ValueError("residence_dates.start_year_min is out of supported range")
    min_duration_days = int(residence_cfg.get("min_duration_days", 0))
    if min_duration_days < 0:
        raise ValueError("residence_dates.min_duration_days must be >= 0")
