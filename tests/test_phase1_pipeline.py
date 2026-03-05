from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase1.config import normalize_distribution, resolve_age_bins
from sog_phase1.generator import generate_phase1_dataset
from sog_phase1.preprocess import build_prepared_cache


def test_phase1_generation_smoke(tmp_path: Path) -> None:
    config_path = PROJECT_ROOT / "configs" / "phase1.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)

    phase1 = cfg["phase1"]
    phase1["n_people"] = 12000
    phase1["seed"] = 12345
    phase1["output"]["format"] = "csv"
    phase1["output"]["chunk_size"] = 3000
    phase1["output"]["path"] = str(tmp_path / "Phase1_people_addresses.csv")
    phase1["quality"]["distribution_tolerance_pct"] = 3.0
    phase1["quality"]["exact_uniqueness_check_max_rows"] = 50000
    phase1["name_duplication"]["exact_full_name_people_pct"] = 24.0

    run_config = tmp_path / "phase1_test.yaml"
    run_config.write_text(yaml.safe_dump(cfg, sort_keys=False), encoding="utf-8")

    prepared_dir = tmp_path / "prepared"
    build_prepared_cache(PROJECT_ROOT, prepared_dir)

    result = generate_phase1_dataset(
        project_root=PROJECT_ROOT,
        config_path=run_config,
        prepared_dir=prepared_dir,
        overwrite=True,
    )

    output_path = Path(result["output_path"])
    assert output_path.exists()
    df = pd.read_csv(output_path, dtype=str)
    assert len(df) == phase1["n_people"]

    assert df["PersonKey"].nunique() == len(df)
    assert df["AddressKey"].nunique() == len(df)

    full_addr_cols = [
        "ResidenceStreetNumber",
        "ResidenceStreetName",
        "ResidenceUnitType",
        "ResidenceUnitNumber",
        "ResidenceCity",
        "ResidenceState",
        "ResidencePostalCode",
    ]
    assert df.duplicated(full_addr_cols).sum() == 0
    assert df["ResidenceStreetName"].nunique() > 10
    assert df["ResidenceStreetNumber"].nunique() > 10

    full_name_counts = df["FullName"].value_counts()
    dup_name_people_pct = (full_name_counts[full_name_counts > 1].sum() / len(df)) * 100.0
    assert 20.0 <= dup_name_people_pct <= 30.0

    gender_norm = normalize_distribution(
        phase1["distributions"]["gender"],
        label="gender",
        auto_normalize=True,
    )
    achieved_gender_pct = (df["Gender"].value_counts(normalize=True) * 100).to_dict()
    for key, expected in gender_norm.normalized_percentages.items():
        achieved = float(achieved_gender_pct.get(key, 0.0))
        assert abs(achieved - expected) <= 3.0

    _, age_norm = resolve_age_bins(phase1["age_bins"])
    achieved_age_pct = (df["AgeBin"].value_counts(normalize=True) * 100).to_dict()
    for key, expected in age_norm.normalized_percentages.items():
        achieved = float(achieved_age_pct.get(key, 0.0))
        assert abs(achieved - expected) <= 3.0

    apartments = df[df["ResidenceType"] == "APARTMENT"].copy()
    assert len(apartments) > 0
    complex_cols = [
        "ResidenceStreetNumber",
        "ResidenceStreetName",
        "ResidenceCity",
        "ResidenceState",
        "ResidencePostalCode",
    ]
    mailing_cols = [
        "MailingStreetNumber",
        "MailingStreetName",
        "MailingUnitType",
        "MailingUnitNumber",
        "MailingCity",
        "MailingState",
        "MailingPostalCode",
    ]
    assert set(apartments["MailingAddressMode"].fillna("").unique().tolist()).issubset({"", "PO BOX"})
    apartments_po = apartments[apartments["MailingAddressMode"] == "PO BOX"].copy()
    assert len(apartments_po) > 0
    mailing_per_complex = apartments_po.groupby(complex_cols)[mailing_cols].nunique(dropna=False)
    assert (mailing_per_complex.max(axis=1) == 1).all()
    # OHC-like rule: non-blank mailing city/state should align with residence city/state.
    assert (apartments_po["MailingCity"] == apartments_po["ResidenceCity"]).all()
    assert (apartments_po["MailingState"] == apartments_po["ResidenceState"]).all()

    middle_name_populated_pct = (df["MiddleName"].fillna("").str.strip().ne("").sum() / len(df)) * 100.0
    assert middle_name_populated_pct >= 50.0

    residence_zip = df["ResidencePostalCode"].fillna("").str.strip()
    mailing_zip = df["MailingPostalCode"].fillna("").str.strip()
    assert residence_zip[residence_zip != ""].str.fullmatch(r"\d{5}").all()
    assert mailing_zip[mailing_zip != ""].str.fullmatch(r"\d{5}").all()

    assert Path(result["manifest_path"]).exists()
    assert Path(result["quality_report_path"]).exists()
