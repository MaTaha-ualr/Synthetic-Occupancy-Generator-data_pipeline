from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase1.generator import generate_phase1_dataset
from sog_phase1.preprocess import build_prepared_cache


def _base_config() -> dict:
    config_path = PROJECT_ROOT / "configs" / "phase1.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _read_generated_file(path: Path, output_format: str) -> pd.DataFrame:
    if output_format == "txt":
        return pd.read_csv(path, dtype=str, sep="\t")
    if output_format in {"excel", "xlsx"}:
        return pd.read_excel(path, dtype=str, sheet_name="Phase1")
    return pd.read_csv(path, dtype=str)


def _run_generation(
    cfg: dict,
    tmp_path: Path,
    out_name: str,
    output_format: str = "csv",
) -> tuple[dict, pd.DataFrame]:
    phase1 = cfg["phase1"]
    phase1["output"]["format"] = output_format
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
    df = _read_generated_file(Path(result["output_path"]), output_format)
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


def _nickname_people(df: pd.DataFrame) -> pd.Index:
    return df.loc[df["FirstNameType"] == "NICKNAME", "PersonKey"].drop_duplicates()


def _assert_nickname_people_have_formal_rows(df: pd.DataFrame) -> None:
    nickname_people = _nickname_people(df)
    assert len(nickname_people) > 0
    formal_people = df.loc[df["FirstNameType"] == "FORMAL", "PersonKey"].drop_duplicates()
    missing_formal = nickname_people[~nickname_people.isin(formal_people)]
    assert missing_formal.tolist() == []


def _assert_generation_counts(result: dict, df: pd.DataFrame, records_requested: int) -> None:
    assert int(result["n_records"]) == records_requested
    assert int(result["records_requested"]) == records_requested
    assert int(result["records_written"]) == len(df)
    assert int(result["formal_copy_records_added"]) == len(df) - records_requested
    assert len(df) >= records_requested


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

    phase1["name_duplication"]["full_name_people_pct"] = 18.0

    result, df = _run_generation(cfg, tmp_path, "redundancy_per_record.csv")

    assert int(result["n_people"]) == 3000
    _assert_generation_counts(result, df, 4500)
    assert int(result["formal_copy_records_added"]) > 0
    _assert_uppercase_text_output(df)

    assert df["RecordKey"].nunique() == len(df)
    assert df["AddressKey"].nunique() == len(df)
    assert df.duplicated(_full_address_cols()).sum() == 0

    assert df["PersonKey"].nunique() == 3000
    assert df.duplicated(["PersonKey"]).sum() > 0

    per_entity_counts = df["PersonKey"].value_counts()
    assert per_entity_counts.min() >= 1
    assert per_entity_counts.max() <= 4

    same_person_same_address = df.duplicated(["PersonKey"] + _full_address_cols()).sum()
    assert same_person_same_address == 0

    assert set(df["FirstNameType"].fillna("").unique().tolist()).issubset({"FORMAL", "NICKNAME"})
    assert (df["FirstNameType"] == "NICKNAME").any()
    _assert_nickname_people_have_formal_rows(df)

    appended_rows = df[df["RecordKey"].astype(int) > int(result["records_requested"])]
    assert len(appended_rows) == int(result["formal_copy_records_added"])
    assert appended_rows["AddressKey"].nunique() == len(appended_rows)
    assert appended_rows.duplicated(_full_address_cols()).sum() == 0
    assert (appended_rows["FirstNameType"] == "FORMAL").all()
    assert (appended_rows["FirstName"] == appended_rows["FormalFirstName"]).all()
    assert (appended_rows["FullName"] == appended_rows["FormalFullName"]).all()

    multi_record_people = per_entity_counts[per_entity_counts > 1].index
    varied_people = (
        df[df["PersonKey"].isin(multi_record_people)]
        .groupby("PersonKey")["FirstName"]
        .nunique()
    )
    assert (varied_people > 1).any()

    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    quality_report = json.loads(Path(result["quality_report_path"]).read_text(encoding="utf-8"))
    for payload in (manifest, quality_report):
        assert payload["records_requested"] == 4500
        assert payload["records_written"] == len(df)
        assert payload["formal_copy_records_added"] == int(result["formal_copy_records_added"])
        assert payload["nickname_formal_backup"]["formal_copy_records_added"] == int(
            result["formal_copy_records_added"]
        )
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

    phase1["name_duplication"]["full_name_people_pct"] = 22.0

    result, df = _run_generation(cfg, tmp_path, "redundancy_per_person.csv")

    _assert_generation_counts(result, df, 3400)
    assert int(result["formal_copy_records_added"]) > 0
    _assert_uppercase_text_output(df)
    assert df["PersonKey"].nunique() == 2200
    assert df["AddressKey"].nunique() == len(df)
    assert df.duplicated(_full_address_cols()).sum() == 0

    per_entity_counts = df["PersonKey"].value_counts()
    assert per_entity_counts.min() >= 1
    assert per_entity_counts.max() <= 5

    nickname_rows = df[df["FirstNameType"] == "NICKNAME"]
    assert not nickname_rows.empty
    nickname_display_name_uniques = nickname_rows.groupby("PersonKey")["FirstName"].nunique()
    assert (nickname_display_name_uniques <= 1).all()
    _assert_nickname_people_have_formal_rows(df)

    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))
    quality_report = json.loads(Path(result["quality_report_path"]).read_text(encoding="utf-8"))
    assert manifest["records_requested"] == 3400
    assert manifest["records_written"] == len(df)
    assert quality_report["records_requested"] == 3400
    assert quality_report["records_written"] == len(df)
    assert quality_report["nickname_metrics"]["per_person_consistency_check"] is True


@pytest.mark.parametrize(
    ("output_format", "expected_suffix", "expected_manifest_format"),
    [
        ("txt", ".txt", "txt"),
        ("excel", ".xlsx", "xlsx"),
    ],
)
def test_phase1_writes_selectable_text_and_excel_outputs(
    tmp_path: Path,
    output_format: str,
    expected_suffix: str,
    expected_manifest_format: str,
) -> None:
    cfg = _base_config()
    phase1 = cfg["phase1"]

    phase1["n_people"] = 120
    phase1["n_records"] = 180
    phase1["seed"] = 20260618
    phase1["redundancy"]["enabled"] = True
    phase1["redundancy"]["min_records_per_entity"] = 1
    phase1["redundancy"]["max_records_per_entity"] = 3
    phase1["name_duplication"]["full_name_people_pct"] = 10.0

    result, df = _run_generation(cfg, tmp_path, "selectable_output.csv", output_format=output_format)
    output_path = Path(result["output_path"])
    manifest = json.loads(Path(result["manifest_path"]).read_text(encoding="utf-8"))

    assert output_path.exists()
    assert output_path.suffix == expected_suffix
    assert result["output_format"] == expected_manifest_format
    assert result["output_parts"] == []
    assert manifest["output_format"] == expected_manifest_format
    assert Path(manifest["output_path"]) == output_path
    _assert_generation_counts(result, df, 180)
    assert manifest["records_requested"] == 180
    assert manifest["records_written"] == len(df)
    assert df["RecordKey"].nunique() == len(df)
    _assert_uppercase_text_output(df)


@pytest.mark.parametrize(
    ("config_key", "metric_surface"),
    [
        ("first_name_people_pct", "first_name"),
        ("last_name_people_pct", "last_name"),
    ],
)
def test_phase1_name_duplication_can_target_name_surfaces(
    tmp_path: Path,
    config_key: str,
    metric_surface: str,
) -> None:
    cfg = _base_config()
    phase1 = cfg["phase1"]

    phase1["n_people"] = 240
    phase1["n_records"] = 360
    phase1["seed"] = 20260619
    phase1["redundancy"]["enabled"] = True
    phase1["redundancy"]["min_records_per_entity"] = 1
    phase1["redundancy"]["max_records_per_entity"] = 3
    phase1["nicknames"]["enabled"] = False

    name_duplication = phase1["name_duplication"]
    name_duplication["first_name_people_pct"] = 0.0
    name_duplication["last_name_people_pct"] = 0.0
    name_duplication["full_name_people_pct"] = 0.0
    name_duplication[config_key] = 35.0
    name_duplication["collision_group_min_size"] = 2
    name_duplication["collision_group_max_size"] = 4

    result, df = _run_generation(cfg, tmp_path, f"{metric_surface}_duplication.csv")
    quality_report = json.loads(Path(result["quality_report_path"]).read_text(encoding="utf-8"))
    metrics = quality_report["name_duplication"]

    assert len(df) == 360
    assert metrics[f"target_{metric_surface}_people_pct"] == 35.0
    assert metrics[f"forced_{metric_surface}"]["people"] > 0
    assert metrics[f"forced_{metric_surface}"]["collision_group_min_size_observed"] >= 2
    assert metrics[f"forced_{metric_surface}"]["collision_group_max_size_observed"] <= 4
    assert metrics["forced_full_name"]["people"] == 0
