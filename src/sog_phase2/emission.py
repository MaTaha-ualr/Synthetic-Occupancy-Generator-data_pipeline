from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from typing import Any

import numpy as np
import pandas as pd


MATCH_MODES: tuple[str, ...] = (
    "one_to_one",
    "one_to_many",
    "many_to_one",
    "many_to_many",
)


@dataclass(frozen=True)
class DatasetNoiseConfig:
    name_typo_pct: float = 1.0
    dob_shift_pct: float = 0.5
    ssn_mask_pct: float = 2.0
    phone_mask_pct: float = 1.0
    address_missing_pct: float = 1.0
    middle_name_missing_pct: float = 20.0


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


def get_emission_schema() -> dict[str, Any]:
    defaults = asdict(EmissionConfig())
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
            "crossfile_match_mode": "one_to_one|one_to_many|many_to_one|many_to_many",
            "overlap_entity_pct": "float in [0,100]",
            "appearance_A_pct": "float in [0,100]",
            "appearance_B_pct": "float in [0,100]",
            "duplication_in_A_pct": "float in [0,100]",
            "duplication_in_B_pct": "float in [0,100]",
            "noise": {
                "A": {
                    "name_typo_pct": "float in [0,100]",
                    "dob_shift_pct": "float in [0,100]",
                    "ssn_mask_pct": "float in [0,100]",
                    "phone_mask_pct": "float in [0,100]",
                    "address_missing_pct": "float in [0,100]",
                    "middle_name_missing_pct": "float in [0,100]",
                },
                "B": {
                    "name_typo_pct": "float in [0,100]",
                    "dob_shift_pct": "float in [0,100]",
                    "ssn_mask_pct": "float in [0,100]",
                    "phone_mask_pct": "float in [0,100]",
                    "address_missing_pct": "float in [0,100]",
                    "middle_name_missing_pct": "float in [0,100]",
                },
            },
        },
    }


def _stable_key(value: Any) -> tuple[int, str]:
    text = str(value).strip()
    if text.isdigit():
        return (0, f"{int(text):020d}")
    return (1, text)


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
    )


def parse_emission_config(raw: dict[str, Any] | None) -> EmissionConfig:
    cfg = raw or {}
    if not isinstance(cfg, dict):
        raise ValueError("scenario.emission must be a mapping")

    mode = str(cfg.get("crossfile_match_mode", "one_to_one")).strip().lower()
    if mode not in MATCH_MODES:
        raise ValueError(
            "emission.crossfile_match_mode must be one of: "
            + ", ".join(MATCH_MODES)
        )

    noise_cfg = cfg.get("noise", {})
    if noise_cfg is None:
        noise_cfg = {}
    if not isinstance(noise_cfg, dict):
        raise ValueError("emission.noise must be a mapping")

    defaults = EmissionConfig()
    parsed = EmissionConfig(
        crossfile_match_mode=mode,
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
        dataset_a_noise=_parse_noise(noise_cfg.get("A"), defaults.dataset_a_noise, "A"),
        dataset_b_noise=_parse_noise(noise_cfg.get("B"), defaults.dataset_b_noise, "B"),
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
) -> pd.DataFrame:
    people = truth_people_df.copy()
    people["PersonKey"] = people["PersonKey"].astype(str).str.strip()
    people["DOB"] = people["DOB"].astype(str)
    people["Age"] = pd.to_numeric(people["Age"], errors="coerce").fillna(0).astype(int)
    people = people.sort_values(by="PersonKey", key=lambda s: s.map(_stable_key), kind="mergesort")
    address_map = _pick_address_for_snapshot(truth_residence_history_df, snapshot_date)
    people["AddressKey"] = people["PersonKey"].map(address_map).fillna("").astype(str)
    people["AgeSnapshot"] = people.apply(
        lambda row: _age_on_snapshot(str(row.get("DOB", "")), snapshot_date, int(row.get("Age", 0))),
        axis=1,
    )
    people["SnapshotDate"] = snapshot_date.isoformat()
    return people.reset_index(drop=True)


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
    side: str,
    noise: DatasetNoiseConfig,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, dict[str, list[str]], dict[str, Any]]:
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
    }

    for person_key in sorted(record_counts.keys(), key=_stable_key):
        row_df = snapshot_df[snapshot_df["PersonKey"].astype(str) == str(person_key)]
        if row_df.empty:
            continue
        base = row_df.iloc[0]
        for dup_idx in range(record_counts[person_key]):
            record_counter += 1
            record_key = f"{side}-{record_counter:09d}"
            first_name = str(base.get("FormalFirstName", "")).strip()
            middle_name = str(base.get("MiddleName", "")).strip()
            last_name = str(base.get("LastName", "")).strip()
            dob = str(base.get("DOB", "")).strip()
            ssn = str(base.get("SSN", "")).strip()
            phone = str(base.get("Phone", "")).strip()
            address_key = str(base.get("AddressKey", "")).strip()

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
                noise_counts["address_missing"] += 1

            full_name = " ".join(part for part in [first_name, middle_name, last_name] if part).strip()
            payload = {
                f"{side}_RecordKey": record_key,
                "FirstName": first_name,
                "MiddleName": middle_name,
                "LastName": last_name,
                "Suffix": str(base.get("Suffix", "")).strip(),
                "FullName": full_name,
                "Gender": str(base.get("Gender", "")).strip(),
                "Ethnicity": str(base.get("Ethnicity", "")).strip(),
                "DOB": dob,
                "Age": int(base.get("AgeSnapshot", 0)),
                "SSN": ssn,
                "Phone": phone,
                "AddressKey": address_key,
                "SourceSnapshotDate": str(base.get("SnapshotDate", "")).strip(),
                "SourceSystem": f"Dataset{side}",
            }
            rows.append(payload)
            person_to_records.setdefault(str(person_key), []).append(record_key)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(by=[f"{side}_RecordKey"], kind="mergesort").reset_index(drop=True)
    stats = {
        "rows": int(len(df)),
        "entities": int(len(record_counts)),
        "duplicates": int(max(0, len(df) - len(record_counts))),
        "noise_counts": noise_counts,
    }
    return df, person_to_records, stats


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


def emit_observed_datasets(
    *,
    truth_people_df: pd.DataFrame,
    truth_residence_history_df: pd.DataFrame,
    simulation_start_date: date,
    simulation_end_date: date,
    emission_config: EmissionConfig,
    seed: int,
) -> dict[str, Any]:
    rng = np.random.default_rng(int(seed))

    snapshot_a = _build_snapshot(
        truth_people_df=truth_people_df,
        truth_residence_history_df=truth_residence_history_df,
        snapshot_date=simulation_start_date,
    )
    snapshot_b = _build_snapshot(
        truth_people_df=truth_people_df,
        truth_residence_history_df=truth_residence_history_df,
        snapshot_date=simulation_end_date,
    )

    snapshot_a["_dob"] = pd.to_datetime(snapshot_a["DOB"], errors="coerce").dt.date
    snapshot_b["_dob"] = pd.to_datetime(snapshot_b["DOB"], errors="coerce").dt.date
    base_keys = sorted(
        snapshot_a[snapshot_a["_dob"].isna() | (snapshot_a["_dob"] <= simulation_start_date)]["PersonKey"]
        .astype(str)
        .tolist(),
        key=_stable_key,
    )
    late_only_keys = sorted(
        set(snapshot_b["PersonKey"].astype(str).tolist()) - set(base_keys),
        key=_stable_key,
    )
    snapshot_a = snapshot_a.drop(columns=["_dob"])
    snapshot_b = snapshot_b.drop(columns=["_dob"])

    a_entities, b_entities, coverage_counts = _choose_entity_appearance(
        base_keys=base_keys,
        late_only_keys=late_only_keys,
        config=emission_config,
        rng=rng,
    )

    overlap_keys = a_entities & b_entities
    a_only_keys = a_entities - overlap_keys
    b_only_keys = b_entities - overlap_keys

    a_counts = _allocate_record_counts(
        entity_keys=a_entities,
        overlap_keys=overlap_keys,
        side="A",
        config=emission_config,
        rng=rng,
    )
    b_counts = _allocate_record_counts(
        entity_keys=b_entities,
        overlap_keys=overlap_keys,
        side="B",
        config=emission_config,
        rng=rng,
    )

    dataset_a, a_records_by_person, a_stats = _build_dataset_rows(
        snapshot_df=snapshot_a,
        record_counts=a_counts,
        side="A",
        noise=emission_config.dataset_a_noise,
        rng=rng,
    )
    dataset_b, b_records_by_person, b_stats = _build_dataset_rows(
        snapshot_df=snapshot_b,
        record_counts=b_counts,
        side="B",
        noise=emission_config.dataset_b_noise,
        rng=rng,
    )
    crosswalk = _build_crosswalk_rows(
        overlap_keys=overlap_keys,
        a_only_keys=a_only_keys,
        b_only_keys=b_only_keys,
        a_records_by_person=a_records_by_person,
        b_records_by_person=b_records_by_person,
        mode=emission_config.crossfile_match_mode,
    )

    return {
        "dataset_a": dataset_a,
        "dataset_b": dataset_b,
        "truth_crosswalk": crosswalk,
        "metrics": {
            "coverage": coverage_counts,
            "dataset_a": a_stats,
            "dataset_b": b_stats,
            "crosswalk_rows": int(len(crosswalk)),
            "overlap_entities": int(len(overlap_keys)),
            "a_only_entities": int(len(a_only_keys)),
            "b_only_entities": int(len(b_only_keys)),
            "match_mode": emission_config.crossfile_match_mode,
        },
    }
