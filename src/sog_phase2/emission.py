from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date
from functools import lru_cache
import hashlib
from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


MATCH_MODES: tuple[str, ...] = (
    "single_dataset",
    "one_to_one",
    "one_to_many",
    "many_to_one",
    "many_to_many",
)

ADDRESS_DETAIL_COLUMNS: tuple[str, ...] = (
    "HouseNumber",
    "StreetName",
    "UnitType",
    "UnitNumber",
    "StreetAddress",
    "City",
    "State",
    "ZipCode",
)

# ---------------------------------------------------------------------------
# Nickname map — loaded lazily from phase1/prepared/nicknames.json
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_nickname_map() -> dict[str, list[str]]:
    """Build formal→[nickname, ...] map from the prepared nicknames file."""
    candidates = [
        Path(__file__).resolve().parents[2] / "phase1" / "prepared" / "nicknames.json",
        Path(__file__).resolve().parents[3] / "phase1" / "prepared" / "nicknames.json",
    ]
    for path in candidates:
        if path.exists():
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                result: dict[str, list[str]] = {}
                for _cat, entries in raw.get("categories", {}).items():
                    if not isinstance(entries, dict):
                        continue
                    for formal, payload in entries.items():
                        formal_key = str(formal).strip().title()
                        nicks = list(zip(
                            payload.get("nicknames", []),
                            payload.get("weights", []),
                        ))
                        nicks_sorted = sorted(nicks, key=lambda x: -float(x[1]))
                        top = [str(n).strip().title() for n, w in nicks_sorted
                               if float(w) > 5000 and str(n).strip().title() != formal_key][:2]
                        if top:
                            result[formal_key] = top
                return result
            except Exception:
                pass
    return {}


# ---------------------------------------------------------------------------
# OCR confusion table — character pairs commonly confused in scanned text
# ---------------------------------------------------------------------------

_OCR_CONFUSIONS: dict[str, list[str]] = {
    "O": ["0"], "0": ["O"],
    "l": ["1", "I"], "1": ["l", "I"], "I": ["l", "1"],
    "B": ["8"], "8": ["B"],
    "S": ["5"], "5": ["S"],
    "G": ["6"], "6": ["G"],
    "Z": ["2"], "2": ["Z"],
    "rn": ["m"], "m": ["rn"],
    "cl": ["d"], "vv": ["w"],
    "li": ["h"],
}

# Phonetic substitution clusters (consonant groups that sound similar)
_PHONETIC_SUBS: list[tuple[str, str]] = [
    ("ph", "f"), ("f", "ph"),
    ("ck", "k"), ("k", "ck"),
    ("ie", "y"), ("y", "ie"),
    ("th", "d"), ("d", "th"),
    ("v", "w"), ("w", "v"),
    ("sm", "zm"), ("sch", "sh"), ("sh", "sch"),
    ("ey", "ay"), ("ay", "ey"),
    ("yn", "in"), ("in", "yn"),
    ("ow", "o"), ("o", "ow"),
    ("er", "ar"), ("ar", "er"),
]


@dataclass(frozen=True)
class DatasetNoiseConfig:
    # Original noise types
    name_typo_pct: float = 1.0
    dob_shift_pct: float = 0.5
    ssn_mask_pct: float = 2.0
    phone_mask_pct: float = 1.0
    address_missing_pct: float = 1.0
    middle_name_missing_pct: float = 20.0
    # Enhanced noise types (default 0.0 — backward-compatible)
    phonetic_error_pct: float = 0.0
    ocr_error_pct: float = 0.0
    date_swap_pct: float = 0.0
    zip_digit_error_pct: float = 0.0
    nickname_pct: float = 0.0
    suffix_missing_pct: float = 0.0


@dataclass(frozen=True)
class ObservedDatasetConfig:
    dataset_id: str
    filename: str
    snapshot: str
    appearance_pct: float
    duplication_pct: float
    noise: DatasetNoiseConfig


@dataclass(frozen=True)
class EmissionConfig:
    crossfile_match_mode: str = "one_to_one"
    overlap_entity_pct: float = 70.0
    appearance_a_pct: float = 85.0
    appearance_b_pct: float = 90.0
    duplication_in_a_pct: float = 5.0
    duplication_in_b_pct: float = 8.0
    dataset_a_noise: DatasetNoiseConfig = DatasetNoiseConfig(
        name_typo_pct=1.0,
        dob_shift_pct=0.4,
        ssn_mask_pct=1.5,
        phone_mask_pct=0.8,
        address_missing_pct=0.8,
        middle_name_missing_pct=20.0,
    )
    dataset_b_noise: DatasetNoiseConfig = DatasetNoiseConfig(
        name_typo_pct=2.5,
        dob_shift_pct=1.2,
        ssn_mask_pct=6.0,
        phone_mask_pct=3.0,
        address_missing_pct=2.2,
        middle_name_missing_pct=30.0,
    )
    datasets: tuple[ObservedDatasetConfig, ...] = (
        ObservedDatasetConfig(
            dataset_id="A",
            filename="DatasetA.csv",
            snapshot="simulation_start",
            appearance_pct=85.0,
            duplication_pct=5.0,
            noise=DatasetNoiseConfig(
                name_typo_pct=1.0,
                dob_shift_pct=0.4,
                ssn_mask_pct=1.5,
                phone_mask_pct=0.8,
                address_missing_pct=0.8,
                middle_name_missing_pct=20.0,
            ),
        ),
        ObservedDatasetConfig(
            dataset_id="B",
            filename="DatasetB.csv",
            snapshot="simulation_end",
            appearance_pct=90.0,
            duplication_pct=8.0,
            noise=DatasetNoiseConfig(
                name_typo_pct=2.5,
                dob_shift_pct=1.2,
                ssn_mask_pct=6.0,
                phone_mask_pct=3.0,
                address_missing_pct=2.2,
                middle_name_missing_pct=30.0,
            ),
        ),
    )
    is_legacy_pairwise: bool = True


def get_emission_schema() -> dict[str, Any]:
    defaults = asdict(EmissionConfig())
    defaults.pop("is_legacy_pairwise", None)
    defaults["appearance_A_pct"] = defaults.pop("appearance_a_pct")
    defaults["appearance_B_pct"] = defaults.pop("appearance_b_pct")
    defaults["duplication_in_A_pct"] = defaults.pop("duplication_in_a_pct")
    defaults["duplication_in_B_pct"] = defaults.pop("duplication_in_b_pct")
    defaults["noise"] = {
        "A": defaults.pop("dataset_a_noise"),
        "B": defaults.pop("dataset_b_noise"),
    }
    return {
        "defaults": defaults,
        "fields": {
            "crossfile_match_mode": "single_dataset|one_to_one|one_to_many|many_to_one|many_to_many",
            "overlap_entity_pct": "float in [0,100]",
            "appearance_A_pct": "float in [0,100]",
            "appearance_B_pct": "float in [0,100]",
            "duplication_in_A_pct": "float in [0,100]",
            "duplication_in_B_pct": "float in [0,100]",
            "datasets": [
                {
                    "dataset_id": "string identifier, unique within the run",
                    "filename": "optional CSV filename; default is observed_<dataset_id>.csv",
                    "snapshot": "simulation_start|simulation_end",
                    "appearance_pct": "float in [0,100]",
                    "duplication_pct": "float in [0,100]",
                    "noise": "same schema as emission.noise.A/B",
                }
            ],
            "noise": {
                "A": {
                    "name_typo_pct": "float in [0,100]",
                    "dob_shift_pct": "float in [0,100]",
                    "ssn_mask_pct": "float in [0,100]",
                    "phone_mask_pct": "float in [0,100]",
                    "address_missing_pct": "float in [0,100]",
                    "middle_name_missing_pct": "float in [0,100]",
                    "phonetic_error_pct": "float in [0,100] — phonetic name variants",
                    "ocr_error_pct": "float in [0,100] — OCR character confusion (O/0, l/1, etc.)",
                    "date_swap_pct": "float in [0,100] — MM/DD transposition in DOB",
                    "zip_digit_error_pct": "float in [0,100] — single digit error in ZIP",
                    "nickname_pct": "float in [0,100] — replace formal name with nickname",
                    "suffix_missing_pct": "float in [0,100] — remove Jr./Sr./III suffix",
                },
                "B": {
                    "name_typo_pct": "float in [0,100]",
                    "dob_shift_pct": "float in [0,100]",
                    "ssn_mask_pct": "float in [0,100]",
                    "phone_mask_pct": "float in [0,100]",
                    "address_missing_pct": "float in [0,100]",
                    "middle_name_missing_pct": "float in [0,100]",
                    "phonetic_error_pct": "float in [0,100] — phonetic name variants",
                    "ocr_error_pct": "float in [0,100] — OCR character confusion (O/0, l/1, etc.)",
                    "date_swap_pct": "float in [0,100] — MM/DD transposition in DOB",
                    "zip_digit_error_pct": "float in [0,100] — single digit error in ZIP",
                    "nickname_pct": "float in [0,100] — replace formal name with nickname",
                    "suffix_missing_pct": "float in [0,100] — remove Jr./Sr./III suffix",
                },
            },
        },
    }


def _stable_key(value: Any) -> tuple[int, str]:
    text = str(value).strip()
    if text.isdigit():
        return (0, f"{int(text):020d}")
    return (1, text)


def _text(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def _compose_street_address(
    house_number: str,
    street_name: str,
    unit_type: str,
    unit_number: str,
) -> str:
    line = " ".join(part for part in [house_number, street_name] if part).strip()
    unit = " ".join(part for part in [unit_type, unit_number] if part).strip()
    return " ".join(part for part in [line, unit] if part).strip()


def _default_address_details() -> dict[str, str]:
    return {column: "" for column in ADDRESS_DETAIL_COLUMNS}


def _normalize_address_details(raw: dict[str, Any]) -> dict[str, str]:
    house_number = _text(raw.get("HouseNumber"))
    street_name = _text(raw.get("StreetName"))
    unit_type = _text(raw.get("UnitType"))
    unit_number = _text(raw.get("UnitNumber"))
    street_address = _text(raw.get("StreetAddress")) or _compose_street_address(
        house_number,
        street_name,
        unit_type,
        unit_number,
    )
    return {
        "HouseNumber": house_number,
        "StreetName": street_name,
        "UnitType": unit_type,
        "UnitNumber": unit_number,
        "StreetAddress": street_address,
        "City": _text(raw.get("City")),
        "State": _text(raw.get("State")),
        "ZipCode": _text(raw.get("ZipCode")),
    }


def _select_first_present(row: pd.Series, candidates: tuple[str, ...]) -> str:
    for candidate in candidates:
        if candidate in row.index:
            value = _text(row.get(candidate))
            if value:
                return value
    return ""


def _build_known_address_book(phase1_df: pd.DataFrame | None) -> dict[str, dict[str, str]]:
    if phase1_df is None or phase1_df.empty or "AddressKey" not in phase1_df.columns:
        return {}

    working = phase1_df.copy()
    working["AddressKey"] = working["AddressKey"].map(_text)
    working = working[working["AddressKey"] != ""]
    if working.empty:
        return {}

    book: dict[str, dict[str, str]] = {}
    for address_key, group in working.groupby("AddressKey", sort=False):
        details = {
            "HouseNumber": "",
            "StreetName": "",
            "UnitType": "",
            "UnitNumber": "",
            "City": "",
            "State": "",
            "ZipCode": "",
        }
        for _, row in group.iterrows():
            if not details["HouseNumber"]:
                details["HouseNumber"] = _select_first_present(row, ("ResidenceStreetNumber",))
            if not details["StreetName"]:
                details["StreetName"] = _select_first_present(row, ("ResidenceStreetName",))
            if not details["UnitType"]:
                details["UnitType"] = _select_first_present(row, ("ResidenceUnitType",))
            if not details["UnitNumber"]:
                details["UnitNumber"] = _select_first_present(row, ("ResidenceUnitNumber",))
            if not details["City"]:
                details["City"] = _select_first_present(row, ("ResidenceCity",))
            if not details["State"]:
                details["State"] = _select_first_present(row, ("ResidenceState",))
            if not details["ZipCode"]:
                details["ZipCode"] = _select_first_present(row, ("ResidencePostalCode",))
            if all(details.values()):
                break
        book[str(address_key)] = _normalize_address_details(details)
    return book


def _build_address_pools(known_address_book: dict[str, dict[str, str]]) -> dict[str, list[Any]]:
    street_names = sorted(
        {
            details["StreetName"]
            for details in known_address_book.values()
            if details.get("StreetName")
        }
    )
    if not street_names:
        street_names = ["Main St", "Oak Ave", "Maple Dr", "Cedar Ln", "Pine Rd"]

    locations = sorted(
        {
            (
                details["City"],
                details["State"],
                details["ZipCode"],
            )
            for details in known_address_book.values()
            if details.get("City") and details.get("State") and details.get("ZipCode")
        }
    )
    if not locations:
        locations = [
            ("Little Rock", "AR", "72201"),
            ("North Little Rock", "AR", "72114"),
            ("Conway", "AR", "72032"),
            ("Benton", "AR", "72015"),
        ]

    unit_types = sorted(
        {
            details["UnitType"]
            for details in known_address_book.values()
            if details.get("UnitType")
        }
    )
    if not unit_types:
        unit_types = ["Apt", "Unit", "Ste"]

    house_numbers = sorted(
        {
            details["HouseNumber"]
            for details in known_address_book.values()
            if details.get("HouseNumber")
        }
    )
    return {
        "street_names": street_names,
        "locations": locations,
        "unit_types": unit_types,
        "house_numbers": house_numbers,
    }


def _stable_int(seed: int, value: str, salt: str) -> int:
    digest = hashlib.sha256(f"{seed}|{salt}|{value}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)


def _synthesize_address_details(
    address_key: str,
    *,
    seed: int,
    pools: dict[str, list[Any]],
) -> dict[str, str]:
    if not address_key:
        return _default_address_details()

    street_names = list(pools.get("street_names", [])) or ["Main St"]
    locations = list(pools.get("locations", [])) or [("Little Rock", "AR", "72201")]
    unit_types = list(pools.get("unit_types", [])) or ["Apt", "Unit", "Ste"]
    house_numbers = list(pools.get("house_numbers", []))

    base = _stable_int(seed, address_key, "base")
    if house_numbers:
        house_number = str(house_numbers[base % len(house_numbers)]).strip()
    else:
        house_number = str(100 + (base % 9800))
    street_name = str(street_names[_stable_int(seed, address_key, "street") % len(street_names)]).strip()
    city, state, zip_code = locations[_stable_int(seed, address_key, "location") % len(locations)]

    include_unit = (_stable_int(seed, address_key, "unit-flag") % 5) == 0
    if include_unit:
        unit_type = str(unit_types[_stable_int(seed, address_key, "unit-type") % len(unit_types)]).strip()
        unit_number = str(1 + (_stable_int(seed, address_key, "unit-num") % 999))
    else:
        unit_type = ""
        unit_number = ""

    return _normalize_address_details(
        {
            "HouseNumber": house_number,
            "StreetName": street_name,
            "UnitType": unit_type,
            "UnitNumber": unit_number,
            "City": str(city).strip(),
            "State": str(state).strip(),
            "ZipCode": str(zip_code).strip(),
        }
    )


def _build_address_book(
    *,
    truth_residence_history_df: pd.DataFrame,
    phase1_df: pd.DataFrame | None,
    seed: int,
) -> dict[str, dict[str, str]]:
    book = _build_known_address_book(phase1_df)
    pools = _build_address_pools(book)
    address_keys = sorted(
        {
            _text(value)
            for value in truth_residence_history_df.get("AddressKey", pd.Series(dtype=object)).tolist()
            if _text(value)
        },
        key=_stable_key,
    )
    for address_key in address_keys:
        if address_key not in book:
            book[address_key] = _synthesize_address_details(address_key, seed=seed, pools=pools)
    return book


def _normalize_snapshot(value: Any, field_name: str) -> str:
    text = str(value).strip().lower()
    aliases = {
        "start": "simulation_start",
        "simulation_start": "simulation_start",
        "end": "simulation_end",
        "simulation_end": "simulation_end",
    }
    normalized = aliases.get(text)
    if not normalized:
        raise ValueError(f"{field_name} must be one of: simulation_start, simulation_end")
    return normalized


def _validate_dataset_id(value: Any, field_name: str) -> str:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    if any(ch for ch in text if not (ch.isalnum() or ch in {"_", "-"})):
        raise ValueError(f"{field_name} must contain only letters, digits, underscores, or dashes")
    return text


def _default_dataset_filename(dataset_id: str) -> str:
    if dataset_id == "A":
        return "DatasetA.csv"
    if dataset_id == "B":
        return "DatasetB.csv"
    return f"observed_{dataset_id}.csv"


def _parse_datasets(raw: Any, defaults: EmissionConfig) -> tuple[ObservedDatasetConfig, ...]:
    if raw is None:
        raise ValueError("emission.datasets must be a non-empty list when provided")
    if not isinstance(raw, list) or not raw:
        raise ValueError("emission.datasets must be a non-empty list")

    datasets: list[ObservedDatasetConfig] = []
    seen_ids: set[str] = set()
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"emission.datasets[{idx}] must be a mapping")
        dataset_id = _validate_dataset_id(item.get("dataset_id", item.get("id", "")), f"datasets[{idx}].dataset_id")
        if dataset_id in seen_ids:
            raise ValueError(f"Duplicate dataset_id in emission.datasets: {dataset_id}")
        seen_ids.add(dataset_id)
        filename = str(item.get("filename", "")).strip() or _default_dataset_filename(dataset_id)
        if not filename.lower().endswith(".csv"):
            raise ValueError(f"datasets[{idx}].filename must end with .csv")
        snapshot = _normalize_snapshot(item.get("snapshot", "simulation_end"), f"datasets[{idx}].snapshot")
        default_noise = defaults.dataset_a_noise if idx == 0 else defaults.dataset_b_noise
        datasets.append(
            ObservedDatasetConfig(
                dataset_id=dataset_id,
                filename=filename,
                snapshot=snapshot,
                appearance_pct=_parse_pct(item.get("appearance_pct", 100.0), f"datasets[{idx}].appearance_pct"),
                duplication_pct=_parse_pct(item.get("duplication_pct", 0.0), f"datasets[{idx}].duplication_pct"),
                noise=_parse_noise(item.get("noise"), default_noise, f"datasets[{idx}]"),
            )
        )

    return tuple(datasets)


def _parse_pct(value: Any, field_name: str) -> float:
    parsed = float(value)
    if parsed < 0.0 or parsed > 100.0:
        raise ValueError(f"{field_name} must be in [0,100]")
    return parsed


def _parse_noise(raw: dict[str, Any] | None, defaults: DatasetNoiseConfig, label: str) -> DatasetNoiseConfig:
    cfg = raw or {}
    if not isinstance(cfg, dict):
        raise ValueError(f"emission.noise.{label} must be a mapping")
    return DatasetNoiseConfig(
        name_typo_pct=_parse_pct(cfg.get("name_typo_pct", defaults.name_typo_pct), f"noise.{label}.name_typo_pct"),
        dob_shift_pct=_parse_pct(cfg.get("dob_shift_pct", defaults.dob_shift_pct), f"noise.{label}.dob_shift_pct"),
        ssn_mask_pct=_parse_pct(cfg.get("ssn_mask_pct", defaults.ssn_mask_pct), f"noise.{label}.ssn_mask_pct"),
        phone_mask_pct=_parse_pct(cfg.get("phone_mask_pct", defaults.phone_mask_pct), f"noise.{label}.phone_mask_pct"),
        address_missing_pct=_parse_pct(
            cfg.get("address_missing_pct", defaults.address_missing_pct),
            f"noise.{label}.address_missing_pct",
        ),
        middle_name_missing_pct=_parse_pct(
            cfg.get("middle_name_missing_pct", defaults.middle_name_missing_pct),
            f"noise.{label}.middle_name_missing_pct",
        ),
        phonetic_error_pct=_parse_pct(
            cfg.get("phonetic_error_pct", defaults.phonetic_error_pct),
            f"noise.{label}.phonetic_error_pct",
        ),
        ocr_error_pct=_parse_pct(
            cfg.get("ocr_error_pct", defaults.ocr_error_pct),
            f"noise.{label}.ocr_error_pct",
        ),
        date_swap_pct=_parse_pct(
            cfg.get("date_swap_pct", defaults.date_swap_pct),
            f"noise.{label}.date_swap_pct",
        ),
        zip_digit_error_pct=_parse_pct(
            cfg.get("zip_digit_error_pct", defaults.zip_digit_error_pct),
            f"noise.{label}.zip_digit_error_pct",
        ),
        nickname_pct=_parse_pct(
            cfg.get("nickname_pct", defaults.nickname_pct),
            f"noise.{label}.nickname_pct",
        ),
        suffix_missing_pct=_parse_pct(
            cfg.get("suffix_missing_pct", defaults.suffix_missing_pct),
            f"noise.{label}.suffix_missing_pct",
        ),
    )


def parse_emission_config(raw: dict[str, Any] | None) -> EmissionConfig:
    cfg = raw or {}
    if not isinstance(cfg, dict):
        raise ValueError("scenario.emission must be a mapping")

    defaults = EmissionConfig()
    mode = str(cfg.get("crossfile_match_mode", "")).strip().lower()
    if mode and mode not in MATCH_MODES:
        raise ValueError(
            "emission.crossfile_match_mode must be one of: "
            + ", ".join(MATCH_MODES)
        )

    datasets_raw = cfg.get("datasets")
    if datasets_raw is not None:
        datasets = _parse_datasets(datasets_raw, defaults)
        inferred_mode = mode or ("single_dataset" if len(datasets) == 1 else "one_to_one")
        if len(datasets) == 1 and inferred_mode != "single_dataset":
            raise ValueError("Single-dataset emission requires crossfile_match_mode=single_dataset")
        if len(datasets) == 2 and inferred_mode == "single_dataset":
            raise ValueError("crossfile_match_mode=single_dataset requires exactly one dataset")
        return EmissionConfig(
            crossfile_match_mode=inferred_mode,
            overlap_entity_pct=(
                0.0
                if inferred_mode == "single_dataset"
                else _parse_pct(cfg.get("overlap_entity_pct", defaults.overlap_entity_pct), "overlap_entity_pct")
            ),
            appearance_a_pct=datasets[0].appearance_pct,
            appearance_b_pct=datasets[1].appearance_pct if len(datasets) > 1 else 0.0,
            duplication_in_a_pct=datasets[0].duplication_pct,
            duplication_in_b_pct=datasets[1].duplication_pct if len(datasets) > 1 else 0.0,
            dataset_a_noise=datasets[0].noise,
            dataset_b_noise=datasets[1].noise if len(datasets) > 1 else defaults.dataset_b_noise,
            datasets=datasets,
            is_legacy_pairwise=False,
        )

    noise_cfg = cfg.get("noise", {})
    if noise_cfg is None:
        noise_cfg = {}
    if not isinstance(noise_cfg, dict):
        raise ValueError("emission.noise must be a mapping")

    parsed_mode = mode or "one_to_one"
    if parsed_mode == "single_dataset":
        dataset_a_noise = _parse_noise(noise_cfg.get("A"), defaults.dataset_a_noise, "A")
        dataset_a = ObservedDatasetConfig(
            dataset_id="A",
            filename="DatasetA.csv",
            snapshot="simulation_end",
            appearance_pct=_parse_pct(cfg.get("appearance_A_pct", 100.0), "appearance_A_pct"),
            duplication_pct=_parse_pct(cfg.get("duplication_in_A_pct", defaults.duplication_in_a_pct), "duplication_in_A_pct"),
            noise=dataset_a_noise,
        )
        return EmissionConfig(
            crossfile_match_mode="single_dataset",
            overlap_entity_pct=0.0,
            appearance_a_pct=dataset_a.appearance_pct,
            appearance_b_pct=0.0,
            duplication_in_a_pct=dataset_a.duplication_pct,
            duplication_in_b_pct=0.0,
            dataset_a_noise=dataset_a_noise,
            dataset_b_noise=defaults.dataset_b_noise,
            datasets=(dataset_a,),
            is_legacy_pairwise=False,
        )

    dataset_a_noise = _parse_noise(noise_cfg.get("A"), defaults.dataset_a_noise, "A")
    dataset_b_noise = _parse_noise(noise_cfg.get("B"), defaults.dataset_b_noise, "B")
    parsed = EmissionConfig(
        crossfile_match_mode=parsed_mode,
        overlap_entity_pct=_parse_pct(cfg.get("overlap_entity_pct", defaults.overlap_entity_pct), "overlap_entity_pct"),
        appearance_a_pct=_parse_pct(cfg.get("appearance_A_pct", defaults.appearance_a_pct), "appearance_A_pct"),
        appearance_b_pct=_parse_pct(cfg.get("appearance_B_pct", defaults.appearance_b_pct), "appearance_B_pct"),
        duplication_in_a_pct=_parse_pct(
            cfg.get("duplication_in_A_pct", defaults.duplication_in_a_pct),
            "duplication_in_A_pct",
        ),
        duplication_in_b_pct=_parse_pct(
            cfg.get("duplication_in_B_pct", defaults.duplication_in_b_pct),
            "duplication_in_B_pct",
        ),
        dataset_a_noise=dataset_a_noise,
        dataset_b_noise=dataset_b_noise,
        datasets=(
            ObservedDatasetConfig(
                dataset_id="A",
                filename="DatasetA.csv",
                snapshot="simulation_start",
                appearance_pct=_parse_pct(cfg.get("appearance_A_pct", defaults.appearance_a_pct), "appearance_A_pct"),
                duplication_pct=_parse_pct(
                    cfg.get("duplication_in_A_pct", defaults.duplication_in_a_pct),
                    "duplication_in_A_pct",
                ),
                noise=dataset_a_noise,
            ),
            ObservedDatasetConfig(
                dataset_id="B",
                filename="DatasetB.csv",
                snapshot="simulation_end",
                appearance_pct=_parse_pct(cfg.get("appearance_B_pct", defaults.appearance_b_pct), "appearance_B_pct"),
                duplication_pct=_parse_pct(
                    cfg.get("duplication_in_B_pct", defaults.duplication_in_b_pct),
                    "duplication_in_B_pct",
                ),
                noise=dataset_b_noise,
            ),
        ),
        is_legacy_pairwise=True,
    )
    return parsed


def _age_on_snapshot(dob_text: str, snapshot: date, fallback_age: int) -> int:
    parsed = pd.to_datetime(str(dob_text).strip(), errors="coerce")
    if pd.isna(parsed):
        return int(fallback_age)
    dob = parsed.date()
    years = snapshot.year - dob.year
    if (snapshot.month, snapshot.day) < (dob.month, dob.day):
        years -= 1
    return int(max(years, 0))


def _pick_address_for_snapshot(history_df: pd.DataFrame, snapshot_date: date) -> pd.Series:
    if history_df.empty:
        return pd.Series([], dtype=object)
    working = history_df.copy()
    working["PersonKey"] = working["PersonKey"].astype(str).str.strip()
    working["_start"] = pd.to_datetime(working["ResidenceStartDate"], errors="coerce")
    working["_end"] = pd.to_datetime(working["ResidenceEndDate"], errors="coerce")
    snap = pd.Timestamp(snapshot_date)
    active = working[
        (working["_start"].notna())
        & (working["_start"] <= snap)
        & ((working["_end"].isna()) | (working["_end"] >= snap))
    ].copy()
    active = active.sort_values(by=["PersonKey", "_start"], kind="mergesort")
    if not active.empty:
        return active.groupby("PersonKey", as_index=True)["AddressKey"].last()

    earlier = working[(working["_start"].notna()) & (working["_start"] <= snap)].copy()
    earlier = earlier.sort_values(by=["PersonKey", "_start"], kind="mergesort")
    return earlier.groupby("PersonKey", as_index=True)["AddressKey"].last()


def _build_snapshot(
    *,
    truth_people_df: pd.DataFrame,
    truth_residence_history_df: pd.DataFrame,
    snapshot_date: date,
    address_book: dict[str, dict[str, str]],
) -> pd.DataFrame:
    people = truth_people_df.copy()
    people["PersonKey"] = people["PersonKey"].astype(str).str.strip()
    people["DOB"] = people["DOB"].astype(str)
    people["Age"] = pd.to_numeric(people["Age"], errors="coerce").fillna(0).astype(int)
    people = people.sort_values(by="PersonKey", key=lambda s: s.map(_stable_key), kind="mergesort")
    address_map = _pick_address_for_snapshot(truth_residence_history_df, snapshot_date)
    people["AddressKey"] = people["PersonKey"].map(address_map).fillna("").astype(str)
    address_rows = [
        address_book.get(address_key, _default_address_details())
        for address_key in people["AddressKey"].tolist()
    ]
    address_details_df = pd.DataFrame(address_rows, columns=list(ADDRESS_DETAIL_COLUMNS))
    people = pd.concat(
        [people.reset_index(drop=True), address_details_df.reset_index(drop=True)],
        axis=1,
    )
    people["AgeSnapshot"] = people.apply(
        lambda row: _age_on_snapshot(str(row.get("DOB", "")), snapshot_date, int(row.get("Age", 0))),
        axis=1,
    )
    people["SnapshotDate"] = snapshot_date.isoformat()
    return people.reset_index(drop=True)


def _candidate_keys_for_snapshot(snapshot_df: pd.DataFrame, snapshot_date: date) -> list[str]:
    if snapshot_df.empty:
        return []
    working = snapshot_df.copy()
    working["_dob"] = pd.to_datetime(working["DOB"], errors="coerce").dt.date
    eligible = working[working["_dob"].isna() | (working["_dob"] <= snapshot_date)]
    return sorted(eligible["PersonKey"].astype(str).tolist(), key=_stable_key)


def _deterministic_sample(keys: list[str], n: int, rng: np.random.Generator) -> list[str]:
    if n <= 0 or not keys:
        return []
    take = min(max(int(n), 0), len(keys))
    if take <= 0:
        return []
    positions = np.sort(rng.choice(len(keys), size=take, replace=False))
    return [keys[int(i)] for i in positions]


def _choose_entity_appearance(
    *,
    base_keys: list[str],
    late_only_keys: list[str],
    config: EmissionConfig,
    rng: np.random.Generator,
) -> tuple[set[str], set[str], dict[str, int]]:
    n = len(base_keys)
    target_a = min(n, max(0, int(round((config.appearance_a_pct / 100.0) * n))))
    target_b_base = min(n, max(0, int(round((config.appearance_b_pct / 100.0) * n))))
    target_overlap = min(n, max(0, int(round((config.overlap_entity_pct / 100.0) * n))))
    target_overlap = min(target_overlap, target_a, target_b_base)
    min_overlap_needed = max(0, target_a + target_b_base - n)
    if target_overlap < min_overlap_needed:
        target_overlap = min_overlap_needed

    sorted_base = sorted(base_keys, key=_stable_key)
    overlap = set(_deterministic_sample(sorted_base, target_overlap, rng))
    remaining_after_overlap = [key for key in sorted_base if key not in overlap]

    target_a_only = max(0, target_a - target_overlap)
    a_only = set(_deterministic_sample(remaining_after_overlap, target_a_only, rng))
    remaining_for_b = [key for key in remaining_after_overlap if key not in a_only]
    target_b_only = max(0, target_b_base - target_overlap)
    b_only = set(_deterministic_sample(remaining_for_b, target_b_only, rng))

    a_entities = overlap | a_only
    b_entities = overlap | b_only

    sorted_late = sorted(late_only_keys, key=_stable_key)
    late_for_b_n = int(round((config.appearance_b_pct / 100.0) * len(sorted_late)))
    late_for_b = set(_deterministic_sample(sorted_late, late_for_b_n, rng))
    b_entities |= late_for_b

    counts = {
        "base_entities": n,
        "late_only_entities": len(sorted_late),
        "a_entities_base": len(a_entities & set(base_keys)),
        "b_entities_base": len(b_entities & set(base_keys)),
        "overlap_entities": len(overlap),
        "a_only_entities": len(a_only),
        "b_only_entities": len(b_only),
        "late_only_in_b": len(late_for_b),
        "relationship_mode": config.crossfile_match_mode,
    }
    return a_entities, b_entities, counts


def _allocate_record_counts(
    *,
    entity_keys: set[str],
    overlap_keys: set[str],
    side: str,
    config: EmissionConfig,
    rng: np.random.Generator,
) -> dict[str, int]:
    keys = sorted(entity_keys, key=_stable_key)
    counts = {key: 1 for key in keys}

    if side == "A" and config.crossfile_match_mode in {"many_to_one", "many_to_many"}:
        for key in overlap_keys:
            if key in counts:
                counts[key] = max(counts[key], 2)
    if side == "B" and config.crossfile_match_mode in {"one_to_many", "many_to_many"}:
        for key in overlap_keys:
            if key in counts:
                counts[key] = max(counts[key], 2)

    target_dup_pct = config.duplication_in_a_pct if side == "A" else config.duplication_in_b_pct
    target_extra = int(round((target_dup_pct / 100.0) * len(keys)))
    existing_extra = sum(max(0, value - 1) for value in counts.values())
    remaining_extra = max(0, target_extra - existing_extra)

    if remaining_extra > 0 and keys:
        for _ in range(remaining_extra):
            idx = int(rng.integers(0, len(keys)))
            counts[keys[idx]] += 1
    return counts


def _choose_entities_for_single_dataset(
    *,
    candidate_keys: list[str],
    dataset: ObservedDatasetConfig,
    rng: np.random.Generator,
) -> tuple[set[str], dict[str, Any]]:
    target = min(len(candidate_keys), max(0, int(round((dataset.appearance_pct / 100.0) * len(candidate_keys)))))
    selected = set(_deterministic_sample(sorted(candidate_keys, key=_stable_key), target, rng))
    coverage = {
        "dataset_count": 1,
        "dataset_ids": [dataset.dataset_id],
        "candidate_entities": {dataset.dataset_id: len(candidate_keys)},
        "dataset_entities": {dataset.dataset_id: len(selected)},
        "relationship_mode": "single_dataset",
    }
    return selected, coverage


def _choose_entities_for_multiple_datasets(
    *,
    candidate_keys_by_dataset: dict[str, list[str]],
    datasets: list[ObservedDatasetConfig],
    overlap_entity_pct: float,
    relationship_mode: str,
    rng: np.random.Generator,
) -> tuple[dict[str, set[str]], dict[str, Any]]:
    dataset_ids = [dataset.dataset_id for dataset in datasets]
    pools = {
        dataset.dataset_id: set(candidate_keys_by_dataset.get(dataset.dataset_id, []))
        for dataset in datasets
    }
    target_counts = {
        dataset.dataset_id: min(
            len(candidate_keys_by_dataset.get(dataset.dataset_id, [])),
            max(
                0,
                int(
                    round(
                        (dataset.appearance_pct / 100.0)
                        * len(candidate_keys_by_dataset.get(dataset.dataset_id, []))
                    )
                ),
            ),
        )
        for dataset in datasets
    }

    shared_all = set.intersection(*(pool for pool in pools.values())) if pools else set()
    target_overlap = min(
        len(shared_all),
        min(target_counts.values(), default=0),
        max(0, int(round((overlap_entity_pct / 100.0) * len(shared_all)))),
    )
    overlap = set(_deterministic_sample(sorted(shared_all, key=_stable_key), target_overlap, rng))

    selected_sets: dict[str, set[str]] = {
        dataset_id: set(overlap)
        for dataset_id in dataset_ids
    }

    shared_remainder = sorted(shared_all - overlap, key=_stable_key)
    shared_assignment_counts = {key: 0 for key in shared_remainder}

    for dataset in datasets:
        dataset_id = dataset.dataset_id
        need = max(0, target_counts[dataset_id] - len(selected_sets[dataset_id]))
        unique_pool = sorted(pools[dataset_id] - shared_all, key=_stable_key)
        unique_pick = _deterministic_sample(unique_pool, min(need, len(unique_pool)), rng)
        selected_sets[dataset_id].update(unique_pick)
        need -= len(unique_pick)
        if need <= 0:
            continue

        remaining_shared = [key for key in shared_remainder if key not in selected_sets[dataset_id]]
        if not remaining_shared:
            continue
        randomized = list(remaining_shared)
        if len(randomized) > 1:
            order = rng.permutation(len(randomized))
            randomized = [randomized[int(i)] for i in order]
        randomized.sort(key=lambda key: (shared_assignment_counts.get(key, 0), _stable_key(key)))
        for key in randomized[:need]:
            selected_sets[dataset_id].add(key)
            shared_assignment_counts[key] = shared_assignment_counts.get(key, 0) + 1

    selected_union = set().union(*selected_sets.values()) if selected_sets else set()
    selected_all_overlap = (
        set.intersection(*(selected_sets[dataset_id] for dataset_id in dataset_ids))
        if dataset_ids
        else set()
    )
    pairwise_overlap: dict[str, Any] = {}
    for idx, first_id in enumerate(dataset_ids):
        for second_id in dataset_ids[idx + 1:]:
            pair_overlap = selected_sets[first_id] & selected_sets[second_id]
            pair_union = selected_sets[first_id] | selected_sets[second_id]
            pairwise_overlap[f"{first_id}__{second_id}"] = {
                "dataset_ids": [first_id, second_id],
                "overlap_entities": int(len(pair_overlap)),
                "union_entities": int(len(pair_union)),
                "overlap_pct_of_union": float((len(pair_overlap) / len(pair_union)) * 100.0 if pair_union else 0.0),
            }

    coverage = {
        "dataset_count": len(dataset_ids),
        "dataset_ids": dataset_ids,
        "candidate_entities": {
            dataset_id: len(candidate_keys_by_dataset.get(dataset_id, []))
            for dataset_id in dataset_ids
        },
        "dataset_entities": {
            dataset_id: len(selected_sets[dataset_id])
            for dataset_id in dataset_ids
        },
        "all_dataset_overlap_candidate_entities": int(len(shared_all)),
        "all_dataset_overlap_entities": int(len(selected_all_overlap)),
        "union_entities": int(len(selected_union)),
        "relationship_mode": relationship_mode,
        "pairwise_overlap": pairwise_overlap,
    }
    return selected_sets, coverage


def _allocate_generic_record_counts(
    *,
    entity_keys: set[str],
    overlap_keys: set[str],
    dataset_index: int,
    dataset: ObservedDatasetConfig,
    mode: str,
    rng: np.random.Generator,
) -> dict[str, int]:
    keys = sorted(entity_keys, key=_stable_key)
    counts = {key: 1 for key in keys}

    if mode == "many_to_one" and dataset_index == 0:
        for key in overlap_keys:
            if key in counts:
                counts[key] = max(counts[key], 2)
    elif mode == "one_to_many" and dataset_index > 0:
        for key in overlap_keys:
            if key in counts:
                counts[key] = max(counts[key], 2)
    elif mode == "many_to_many":
        for key in overlap_keys:
            if key in counts:
                counts[key] = max(counts[key], 2)

    target_extra = int(round((dataset.duplication_pct / 100.0) * len(keys)))
    existing_extra = sum(max(0, value - 1) for value in counts.values())
    remaining_extra = max(0, target_extra - existing_extra)
    if remaining_extra > 0 and keys:
        for _ in range(remaining_extra):
            idx = int(rng.integers(0, len(keys)))
            counts[keys[idx]] += 1
    return counts


def _apply_phonetic_error(name: str, rng: np.random.Generator) -> str:
    """Replace a phonetic cluster in a name with a similar-sounding alternative."""
    if len(name) < 3:
        return name
    lower = name.lower()
    # Collect all valid (pattern, replacement) substitutions for this name
    candidates: list[tuple[int, str, str]] = []
    for pattern, replacement in _PHONETIC_SUBS:
        idx = lower.find(pattern)
        if idx >= 0:
            candidates.append((idx, pattern, replacement))
    if not candidates:
        return name
    idx, pattern, replacement = candidates[int(rng.integers(0, len(candidates)))]
    mutated = name[:idx] + replacement + name[idx + len(pattern):]
    # Preserve original title-case
    if name[0].isupper():
        mutated = mutated[0].upper() + mutated[1:]
    return mutated


def _apply_ocr_error(text: str, rng: np.random.Generator) -> str:
    """Substitute one character (or digraph) in text using the OCR confusion table."""
    if not text:
        return text
    # Try multi-char confusions first (digraphs)
    digraph_positions: list[tuple[int, str]] = []
    for pattern in _OCR_CONFUSIONS:
        if len(pattern) > 1:
            idx = text.find(pattern)
            if idx >= 0:
                digraph_positions.append((idx, pattern))
    if digraph_positions:
        idx, pattern = digraph_positions[int(rng.integers(0, len(digraph_positions)))]
        replacement = _OCR_CONFUSIONS[pattern][0]
        return text[:idx] + replacement + text[idx + len(pattern):]
    # Fall back to single-char confusion
    char_positions = [(i, ch) for i, ch in enumerate(text) if ch in _OCR_CONFUSIONS]
    if not char_positions:
        return text
    pos, ch = char_positions[int(rng.integers(0, len(char_positions)))]
    options = _OCR_CONFUSIONS[ch]
    replacement = options[int(rng.integers(0, len(options)))]
    return text[:pos] + replacement + text[pos + 1:]


def _apply_date_swap(dob_str: str, rng: np.random.Generator) -> str:  # noqa: ARG001
    """Swap month and day in a DOB string if both are valid as day/month."""
    parsed = pd.to_datetime(str(dob_str).strip(), errors="coerce")
    if pd.isna(parsed):
        return dob_str
    month = parsed.month
    day = parsed.day
    # Only swap when the swapped values are still valid (day ≤ 12 means it could be a month)
    if day <= 12 and month != day:
        try:
            swapped = parsed.replace(month=day, day=month)
            return swapped.date().isoformat()
        except ValueError:
            pass
    return dob_str


def _apply_zip_error(zip_str: str, rng: np.random.Generator) -> str:
    """Replace one digit in a ZIP code with an adjacent digit (±1)."""
    digits = [ch for ch in zip_str if ch.isdigit()]
    if not digits:
        return zip_str
    # Pick a random digit position
    positions = [i for i, ch in enumerate(zip_str) if ch.isdigit()]
    pos = positions[int(rng.integers(0, len(positions)))]
    original_digit = int(zip_str[pos])
    # Adjacent digit: wrap within 0-9
    delta = 1 if rng.random() < 0.5 else -1
    new_digit = (original_digit + delta) % 10
    return zip_str[:pos] + str(new_digit) + zip_str[pos + 1:]


def _apply_nickname(
    name: str,
    rng: np.random.Generator,
    nickname_map: dict[str, list[str]],
) -> str:
    """Replace a formal name with one of its nickname variants."""
    key = name.strip().title()
    variants = nickname_map.get(key)
    if not variants:
        return name
    return variants[int(rng.integers(0, len(variants)))]


def _inject_typo(text: str, rng: np.random.Generator) -> str:
    value = str(text)
    if not value:
        return value
    letters = list(value)
    index = int(rng.integers(0, len(letters)))
    replacement = chr(ord("A") + int(rng.integers(0, 26)))
    letters[index] = replacement
    return "".join(letters)


def _mask_ssn(value: str) -> str:
    text = str(value).strip()
    if len(text) < 4:
        return ""
    return "***-**-" + text[-4:]


def _shift_dob(value: str, rng: np.random.Generator) -> str:
    parsed = pd.to_datetime(str(value).strip(), errors="coerce")
    if pd.isna(parsed):
        return str(value).strip()
    shift = int(rng.integers(-3, 4))
    shifted = (parsed + pd.Timedelta(days=shift)).date()
    return shifted.isoformat()


def _build_dataset_rows(
    *,
    snapshot_df: pd.DataFrame,
    record_counts: dict[str, int],
    dataset_id: str,
    noise: DatasetNoiseConfig,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, dict[str, list[str]], dict[str, Any]]:
    nickname_map = _load_nickname_map()
    rows: list[dict[str, Any]] = []
    person_to_records: dict[str, list[str]] = {}
    record_counter = 0
    noise_counts = {
        "name_typo": 0,
        "dob_shift": 0,
        "ssn_mask": 0,
        "phone_mask": 0,
        "address_missing": 0,
        "middle_name_missing": 0,
        "phonetic_error": 0,
        "ocr_error": 0,
        "date_swap": 0,
        "zip_digit_error": 0,
        "nickname": 0,
        "suffix_missing": 0,
    }

    for person_key in sorted(record_counts.keys(), key=_stable_key):
        row_df = snapshot_df[snapshot_df["PersonKey"].astype(str) == str(person_key)]
        if row_df.empty:
            continue
        base = row_df.iloc[0]
        for dup_idx in range(record_counts[person_key]):
            record_counter += 1
            record_key = f"{dataset_id}-{record_counter:09d}"
            first_name = str(base.get("FormalFirstName", "")).strip()
            middle_name = str(base.get("MiddleName", "")).strip()
            last_name = str(base.get("LastName", "")).strip()
            suffix = str(base.get("Suffix", "")).strip()
            dob = str(base.get("DOB", "")).strip()
            ssn = str(base.get("SSN", "")).strip()
            phone = str(base.get("Phone", "")).strip()
            address_key = str(base.get("AddressKey", "")).strip()
            house_number = _text(base.get("HouseNumber"))
            street_name = _text(base.get("StreetName"))
            unit_type = _text(base.get("UnitType"))
            unit_number = _text(base.get("UnitNumber"))
            city = _text(base.get("City"))
            state = _text(base.get("State"))
            zip_code = _text(base.get("ZipCode"))

            # --- Original noise types ---
            if rng.random() < (noise.name_typo_pct / 100.0):
                if rng.random() < 0.5:
                    first_name = _inject_typo(first_name, rng)
                else:
                    last_name = _inject_typo(last_name, rng)
                noise_counts["name_typo"] += 1
            if rng.random() < (noise.middle_name_missing_pct / 100.0):
                middle_name = ""
                noise_counts["middle_name_missing"] += 1
            if rng.random() < (noise.dob_shift_pct / 100.0):
                dob = _shift_dob(dob, rng)
                noise_counts["dob_shift"] += 1
            if rng.random() < (noise.ssn_mask_pct / 100.0):
                ssn = _mask_ssn(ssn)
                noise_counts["ssn_mask"] += 1
            if rng.random() < (noise.phone_mask_pct / 100.0):
                phone = ""
                noise_counts["phone_mask"] += 1
            if rng.random() < (noise.address_missing_pct / 100.0):
                address_key = ""
                house_number = ""
                street_name = ""
                unit_type = ""
                unit_number = ""
                city = ""
                state = ""
                zip_code = ""
                noise_counts["address_missing"] += 1

            # --- Enhanced noise types ---
            if noise.ocr_error_pct > 0.0 and rng.random() < (noise.ocr_error_pct / 100.0):
                if rng.random() < 0.5:
                    first_name = _apply_ocr_error(first_name, rng)
                else:
                    last_name = _apply_ocr_error(last_name, rng)
                noise_counts["ocr_error"] += 1

            if noise.phonetic_error_pct > 0.0 and rng.random() < (noise.phonetic_error_pct / 100.0):
                if rng.random() < 0.6:
                    first_name = _apply_phonetic_error(first_name, rng)
                else:
                    last_name = _apply_phonetic_error(last_name, rng)
                noise_counts["phonetic_error"] += 1

            if noise.nickname_pct > 0.0 and rng.random() < (noise.nickname_pct / 100.0):
                first_name = _apply_nickname(first_name, rng, nickname_map)
                noise_counts["nickname"] += 1

            if noise.date_swap_pct > 0.0 and rng.random() < (noise.date_swap_pct / 100.0):
                dob = _apply_date_swap(dob, rng)
                noise_counts["date_swap"] += 1

            if noise.zip_digit_error_pct > 0.0 and rng.random() < (noise.zip_digit_error_pct / 100.0):
                zip_code = _apply_zip_error(zip_code, rng)
                noise_counts["zip_digit_error"] += 1

            if noise.suffix_missing_pct > 0.0 and rng.random() < (noise.suffix_missing_pct / 100.0):
                suffix = ""
                noise_counts["suffix_missing"] += 1

            full_name = " ".join(part for part in [first_name, middle_name, last_name] if part).strip()
            street_address = _compose_street_address(
                house_number,
                street_name,
                unit_type,
                unit_number,
            )
            payload = {
                "RecordKey": record_key,
                "DatasetId": dataset_id,
                "FirstName": first_name,
                "MiddleName": middle_name,
                "LastName": last_name,
                "Suffix": suffix,
                "FullName": full_name,
                "Gender": str(base.get("Gender", "")).strip(),
                "Ethnicity": str(base.get("Ethnicity", "")).strip(),
                "DOB": dob,
                "Age": int(base.get("AgeSnapshot", 0)),
                "SSN": ssn,
                "Phone": phone,
                "AddressKey": address_key,
                "HouseNumber": house_number,
                "StreetName": street_name,
                "UnitType": unit_type,
                "UnitNumber": unit_number,
                "StreetAddress": street_address,
                "City": city,
                "State": state,
                "ZipCode": zip_code,
                "SourceSnapshotDate": str(base.get("SnapshotDate", "")).strip(),
                "SourceSystem": dataset_id,
            }
            rows.append(payload)
            person_to_records.setdefault(str(person_key), []).append(record_key)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(by=["RecordKey"], kind="mergesort").reset_index(drop=True)
    stats = {
        "rows": int(len(df)),
        "entities": int(len(record_counts)),
        "duplicates": int(max(0, len(df) - len(record_counts))),
        "noise_counts": noise_counts,
    }
    return df, person_to_records, stats


def _build_legacy_dataset_view(dataset_df: pd.DataFrame, side: str) -> pd.DataFrame:
    key_col = f"{side}_RecordKey"
    if dataset_df.empty:
        columns = [key_col] + [col for col in dataset_df.columns if col != "RecordKey"]
        return pd.DataFrame(columns=columns)
    renamed = dataset_df.rename(columns={"RecordKey": key_col}).copy()
    ordered = [key_col] + [col for col in renamed.columns if col != key_col]
    return renamed[ordered]


def _build_entity_record_map(
    records_by_dataset: dict[str, dict[str, list[str]]]
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for dataset_id, records_by_person in records_by_dataset.items():
        for person_key in sorted(records_by_person.keys(), key=_stable_key):
            for record_key in records_by_person.get(person_key, []):
                rows.append(
                    {
                        "PersonKey": person_key,
                        "DatasetId": dataset_id,
                        "RecordKey": record_key,
                    }
                )
    entity_record_map = pd.DataFrame(rows, columns=["PersonKey", "DatasetId", "RecordKey"])
    if not entity_record_map.empty:
        entity_record_map = entity_record_map.sort_values(
            by=["PersonKey", "DatasetId", "RecordKey"],
            key=lambda s: s.map(_stable_key) if s.name == "PersonKey" else s,
            kind="mergesort",
        ).reset_index(drop=True)
    return entity_record_map


def _build_crosswalk_rows(
    *,
    overlap_keys: set[str],
    a_only_keys: set[str],
    b_only_keys: set[str],
    a_records_by_person: dict[str, list[str]],
    b_records_by_person: dict[str, list[str]],
    mode: str,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for person_key in sorted(overlap_keys, key=_stable_key):
        a_keys = list(a_records_by_person.get(person_key, []))
        b_keys = list(b_records_by_person.get(person_key, []))
        if mode == "one_to_one":
            pairs = min(len(a_keys), len(b_keys))
            for idx in range(pairs):
                rows.append({"PersonKey": person_key, "A_RecordKey": a_keys[idx], "B_RecordKey": b_keys[idx]})
            for idx in range(pairs, len(a_keys)):
                rows.append({"PersonKey": person_key, "A_RecordKey": a_keys[idx], "B_RecordKey": ""})
            for idx in range(pairs, len(b_keys)):
                rows.append({"PersonKey": person_key, "A_RecordKey": "", "B_RecordKey": b_keys[idx]})
        elif mode == "one_to_many":
            primary_a = a_keys[0] if a_keys else ""
            for key in b_keys:
                rows.append({"PersonKey": person_key, "A_RecordKey": primary_a, "B_RecordKey": key})
            for key in a_keys[1:]:
                rows.append({"PersonKey": person_key, "A_RecordKey": key, "B_RecordKey": ""})
        elif mode == "many_to_one":
            primary_b = b_keys[0] if b_keys else ""
            for key in a_keys:
                rows.append({"PersonKey": person_key, "A_RecordKey": key, "B_RecordKey": primary_b})
            for key in b_keys[1:]:
                rows.append({"PersonKey": person_key, "A_RecordKey": "", "B_RecordKey": key})
        else:
            if a_keys and b_keys:
                for a_key in a_keys:
                    for b_key in b_keys:
                        rows.append({"PersonKey": person_key, "A_RecordKey": a_key, "B_RecordKey": b_key})
            elif a_keys:
                for a_key in a_keys:
                    rows.append({"PersonKey": person_key, "A_RecordKey": a_key, "B_RecordKey": ""})
            else:
                for b_key in b_keys:
                    rows.append({"PersonKey": person_key, "A_RecordKey": "", "B_RecordKey": b_key})

    for person_key in sorted(a_only_keys, key=_stable_key):
        for a_key in a_records_by_person.get(person_key, []):
            rows.append({"PersonKey": person_key, "A_RecordKey": a_key, "B_RecordKey": ""})

    for person_key in sorted(b_only_keys, key=_stable_key):
        for b_key in b_records_by_person.get(person_key, []):
            rows.append({"PersonKey": person_key, "A_RecordKey": "", "B_RecordKey": b_key})

    crosswalk = pd.DataFrame(rows, columns=["PersonKey", "A_RecordKey", "B_RecordKey"])
    if not crosswalk.empty:
        crosswalk = crosswalk.sort_values(
            by=["PersonKey", "A_RecordKey", "B_RecordKey"],
            key=lambda s: s.map(_stable_key) if s.name == "PersonKey" else s,
            kind="mergesort",
        ).reset_index(drop=True)
    return crosswalk


def _pairwise_crosswalk_key(first_dataset_id: str, second_dataset_id: str) -> str:
    return f"{first_dataset_id}__{second_dataset_id}"


def emit_observed_datasets(
    *,
    truth_people_df: pd.DataFrame,
    truth_residence_history_df: pd.DataFrame,
    simulation_start_date: date,
    simulation_end_date: date,
    emission_config: EmissionConfig,
    seed: int,
    phase1_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    rng = np.random.default_rng(int(seed))
    address_book = _build_address_book(
        truth_residence_history_df=truth_residence_history_df,
        phase1_df=phase1_df,
        seed=int(seed),
    )

    snapshot_a = _build_snapshot(
        truth_people_df=truth_people_df,
        truth_residence_history_df=truth_residence_history_df,
        snapshot_date=simulation_start_date,
        address_book=address_book,
    )
    snapshot_b = _build_snapshot(
        truth_people_df=truth_people_df,
        truth_residence_history_df=truth_residence_history_df,
        snapshot_date=simulation_end_date,
        address_book=address_book,
    )

    snapshots = {
        "simulation_start": snapshot_a,
        "simulation_end": snapshot_b,
    }

    datasets = list(emission_config.datasets)
    snapshot_dates = {
        "simulation_start": simulation_start_date,
        "simulation_end": simulation_end_date,
    }
    candidate_keys_by_snapshot = {
        snapshot_name: _candidate_keys_for_snapshot(snapshot_df, snapshot_dates[snapshot_name])
        for snapshot_name, snapshot_df in snapshots.items()
    }
    base_keys = candidate_keys_by_snapshot["simulation_start"]
    late_only_keys = sorted(
        set(candidate_keys_by_snapshot["simulation_end"]) - set(base_keys),
        key=_stable_key,
    )
    dataset_frames: dict[str, pd.DataFrame] = {}
    records_by_dataset: dict[str, dict[str, list[str]]] = {}
    dataset_stats: dict[str, dict[str, Any]] = {}
    truth_crosswalk: pd.DataFrame | None = None
    pairwise_crosswalks: dict[str, pd.DataFrame] = {}
    coverage_counts: dict[str, Any]
    overlap_keys: set[str] = set()
    first_only_keys: set[str] = set()
    second_only_keys: set[str] = set()

    if emission_config.is_legacy_pairwise:
        a_entities, b_entities, coverage_counts = _choose_entity_appearance(
            base_keys=base_keys,
            late_only_keys=late_only_keys,
            config=emission_config,
            rng=rng,
        )
        overlap_keys = a_entities & b_entities
        first_only_keys = a_entities - overlap_keys
        second_only_keys = b_entities - overlap_keys
        selected_sets = {"A": a_entities, "B": b_entities}
        record_counts_by_dataset = {
            "A": _allocate_record_counts(
                entity_keys=a_entities,
                overlap_keys=overlap_keys,
                side="A",
                config=emission_config,
                rng=rng,
            ),
            "B": _allocate_record_counts(
                entity_keys=b_entities,
                overlap_keys=overlap_keys,
                side="B",
                config=emission_config,
                rng=rng,
            ),
        }
    elif len(datasets) == 1:
        dataset = datasets[0]
        candidate_keys = candidate_keys_by_snapshot[dataset.snapshot]
        selected_keys, coverage_counts = _choose_entities_for_single_dataset(
            candidate_keys=candidate_keys,
            dataset=dataset,
            rng=rng,
        )
        selected_sets = {dataset.dataset_id: selected_keys}
        record_counts_by_dataset = {
            dataset.dataset_id: _allocate_generic_record_counts(
                entity_keys=selected_keys,
                overlap_keys=set(),
                dataset_index=0,
                dataset=dataset,
                mode="single_dataset",
                rng=rng,
            )
        }
    else:
        selected_sets, coverage_counts = _choose_entities_for_multiple_datasets(
            candidate_keys_by_dataset={
                dataset.dataset_id: candidate_keys_by_snapshot[dataset.snapshot]
                for dataset in datasets
            },
            datasets=datasets,
            overlap_entity_pct=emission_config.overlap_entity_pct,
            relationship_mode=emission_config.crossfile_match_mode,
            rng=rng,
        )
        shared_selected_keys = set.intersection(*(selected_sets[item.dataset_id] for item in datasets))
        if len(datasets) == 2:
            first_dataset, second_dataset = datasets[0], datasets[1]
            overlap_keys = selected_sets[first_dataset.dataset_id] & selected_sets[second_dataset.dataset_id]
            first_only_keys = selected_sets[first_dataset.dataset_id] - overlap_keys
            second_only_keys = selected_sets[second_dataset.dataset_id] - overlap_keys
        record_counts_by_dataset = {
            dataset.dataset_id: _allocate_generic_record_counts(
                entity_keys=selected_sets[dataset.dataset_id],
                overlap_keys=shared_selected_keys,
                dataset_index=index,
                dataset=dataset,
                mode=emission_config.crossfile_match_mode,
                rng=rng,
            )
            for index, dataset in enumerate(datasets)
        }

    for dataset in datasets:
        dataset_df, records_by_person, stats = _build_dataset_rows(
            snapshot_df=snapshots[dataset.snapshot],
            record_counts=record_counts_by_dataset[dataset.dataset_id],
            dataset_id=dataset.dataset_id,
            noise=dataset.noise,
            rng=rng,
        )
        dataset_frames[dataset.dataset_id] = dataset_df
        records_by_dataset[dataset.dataset_id] = records_by_person
        dataset_stats[dataset.dataset_id] = stats

    entity_record_map = _build_entity_record_map(records_by_dataset)

    if len(datasets) >= 2:
        for first_dataset, second_dataset in combinations(datasets, 2):
            pair_overlap = selected_sets[first_dataset.dataset_id] & selected_sets[second_dataset.dataset_id]
            pair_first_only = selected_sets[first_dataset.dataset_id] - pair_overlap
            pair_second_only = selected_sets[second_dataset.dataset_id] - pair_overlap
            pair_key = _pairwise_crosswalk_key(first_dataset.dataset_id, second_dataset.dataset_id)
            pairwise_crosswalks[pair_key] = _build_crosswalk_rows(
                overlap_keys=pair_overlap,
                a_only_keys=pair_first_only,
                b_only_keys=pair_second_only,
                a_records_by_person=records_by_dataset[first_dataset.dataset_id],
                b_records_by_person=records_by_dataset[second_dataset.dataset_id],
                mode=emission_config.crossfile_match_mode,
            )
        if len(datasets) == 2:
            first_dataset, second_dataset = datasets[0], datasets[1]
            truth_crosswalk = pairwise_crosswalks[_pairwise_crosswalk_key(first_dataset.dataset_id, second_dataset.dataset_id)]

    metrics = {
        "coverage": coverage_counts,
        "datasets": dataset_stats,
        "entity_record_map_rows": int(len(entity_record_map)),
        "match_mode": emission_config.crossfile_match_mode,
        "dataset_ids": [dataset.dataset_id for dataset in datasets],
        "dataset_count": len(datasets),
        "pairwise_crosswalk_rows": {
            pair_key: int(len(crosswalk_df))
            for pair_key, crosswalk_df in pairwise_crosswalks.items()
        },
    }

    result = {
        "datasets": dataset_frames,
        "entity_record_map": entity_record_map,
        "truth_crosswalk": truth_crosswalk,
        "pairwise_crosswalks": pairwise_crosswalks,
        "metrics": metrics,
    }
    if "A" in dataset_frames:
        result["dataset_a"] = _build_legacy_dataset_view(dataset_frames["A"], "A")
        metrics["dataset_a"] = dataset_stats["A"]
    if "B" in dataset_frames:
        result["dataset_b"] = _build_legacy_dataset_view(dataset_frames["B"], "B")
        metrics["dataset_b"] = dataset_stats["B"]
    if len(datasets) == 2:
        metrics["crosswalk_rows"] = int(len(truth_crosswalk) if truth_crosswalk is not None else 0)
        metrics["overlap_entities"] = int(len(overlap_keys))
        metrics["a_only_entities"] = int(len(first_only_keys))
        metrics["b_only_entities"] = int(len(second_only_keys))
    return result
