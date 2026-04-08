from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase1.generator import generate_phase1_dataset
from sog_phase1.preprocess import build_prepared_cache


def _base_config() -> dict:
    config_path = PROJECT_ROOT / "configs" / "phase1.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _run_generation(cfg: dict, tmp_path: Path, out_name: str) -> tuple[dict, pd.DataFrame]:
    phase1 = cfg["phase1"]
    phase1["output"]["format"] = "csv"
    phase1["output"]["chunk_size"] = 2000
    phase1["output"]["path"] = str(tmp_path / out_name)
    phase1["quality"]["distribution_tolerance_pct"] = 4.0
    phase1["quality"]["exact_uniqueness_check_max_rows"] = 100000

    run_config = tmp_path / f"{out_name}.yaml"
    run_config.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    prepared_dir = tmp_path / "prepared"
    build_prepared_cache(PROJECT_ROOT, prepared_dir)

    result = generate_phase1_dataset(
        project_root=PROJECT_ROOT,
        config_path=run_config,
        prepared_dir=prepared_dir,
        overwrite=True,
    )
    df = pd.read_csv(Path(result["output_path"]), dtype=str)
    return result, df


def _full_address_cols() -> list[str]:
    return [
        "ResidenceStreetNumber",
        "ResidenceStreetName",
        "ResidenceUnitType",
        "ResidenceUnitNumber",
        "ResidenceCity",
        "ResidenceState",
        "ResidencePostalCode",
    ]


def _assert_uppercase_text_output(df: pd.DataFrame) -> None:
    non_uppercase_columns: list[str] = []
    for column in df.columns:
        values = df[column].fillna("").astype(str)
        if not values.equals(values.str.upper()):
            non_uppercase_columns.append(column)
    assert non_uppercase_columns == []


def test_phase1_redundancy_with_per_record_nicknames(tmp_path: Path) -> None:
    cfg = _base_config()
    phase1 = cfg["phase1"]

    phase1["n_people"] = 3000
    phase1["n_records"] = 4500
    phase1["seed"] = 20260305

    phase1["redundancy"]["enabled"] = True
    phase1["redundancy"]["min_records_per_entity"] = 1
    phase1["redundancy"]["max_records_per_entity"] = 3
    phase1["redundancy"]["shape"] = "balanced"

    phase1["nicknames"]["enabled"] = True
    phase1["nicknames"]["mode"] = "per_record"
    phase1["nicknames"]["usage_pct"] = 55.0

    phase1["name_duplication"]["exact_full_name_people_pct"] = 18.0

    result, df = _run_generation(cfg, tmp_path, "redundancy_per_record.csv")

    assert len(df) == 4500
    assert int(result["n_people"]) == 3000
    assert int(result["n_records"]) == 4500
    _assert_uppercase_text_output(df)

    assert df["RecordKey"].nunique() == len(df)
    assert df["AddressKey"].nunique() == len(df)
    assert df.duplicated(_full_address_cols()).sum() == 0

    assert df["PersonKey"].nunique() == 3000
    assert df.duplicated(["PersonKey"]).sum() > 0

    per_entity_counts = df["PersonKey"].value_counts()
    assert per_entity_counts.min() >= 1
    assert per_entity_counts.max() <= 3

    same_person_same_address = df.duplicated(["PersonKey"] + _full_address_cols()).sum()
    assert same_person_same_address == 0

    assert set(df["FirstNameType"].fillna("").unique().tolist()).issubset({"FORMAL", "NICKNAME"})
    assert (df["FirstNameType"] == "NICKNAME").any()

    multi_record_people = per_entity_counts[per_entity_counts > 1].index
    varied_people = (
        df[df["PersonKey"].isin(multi_record_people)]
        .groupby("PersonKey")["FirstName"]
        .nunique()
    )
    assert (varied_people > 1).any()

    quality_report = json.loads(Path(result["quality_report_path"]).read_text(encoding="utf-8"))
    for section in ("gender", "ethnicity", "age_bins"):
        assert all(key == key.upper() for key in quality_report["expected_distributions_pct"][section])
        assert all(key == key.upper() for key in quality_report["achieved_distributions_pct"][section])
        assert all(key == key.upper() for key in quality_report["distribution_checks"][section])


def test_phase1_redundancy_with_per_person_nicknames(tmp_path: Path) -> None:
    cfg = _base_config()
    phase1 = cfg["phase1"]

    phase1["n_people"] = 2200
    phase1["n_records"] = 3400
    phase1["seed"] = 20260305

    phase1["redundancy"]["enabled"] = True
    phase1["redundancy"]["min_records_per_entity"] = 1
    phase1["redundancy"]["max_records_per_entity"] = 4
    phase1["redundancy"]["shape"] = "heavy_tail"
    phase1["redundancy"]["heavy_tail_alpha"] = 1.4

    phase1["nicknames"]["enabled"] = True
    phase1["nicknames"]["mode"] = "per_person"
    phase1["nicknames"]["usage_pct"] = 70.0

    phase1["name_duplication"]["exact_full_name_people_pct"] = 22.0

    _, df = _run_generation(cfg, tmp_path, "redundancy_per_person.csv")

    assert len(df) == 3400
    _assert_uppercase_text_output(df)
    assert df["PersonKey"].nunique() == 2200
    assert df["AddressKey"].nunique() == len(df)
    assert df.duplicated(_full_address_cols()).sum() == 0

    per_entity_counts = df["PersonKey"].value_counts()
    assert per_entity_counts.min() >= 1
    assert per_entity_counts.max() <= 4

    display_name_uniques = df.groupby("PersonKey")["FirstName"].nunique()
    assert (display_name_uniques <= 1).all()

    type_uniques = df.groupby("PersonKey")["FirstNameType"].nunique()
    assert (type_uniques <= 1).all()

    assert (df["FirstNameType"] == "NICKNAME").any()
