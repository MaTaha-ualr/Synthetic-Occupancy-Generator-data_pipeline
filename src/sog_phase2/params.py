from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_PARAM_FILES: dict[str, str] = {
    "mobility_overall": "mobility_overall_acs_2024.csv",
    "mobility_by_age_cohort": "mobility_by_age_cohort_acs_2024.csv",
    "marriage_divorce_rates": "marriage_divorce_rates_cdc_2023.csv",
    "fertility_by_age": "fertility_by_age_nchs_2024.csv",
    "household_type_shares": "household_type_shares_acs_2024.csv",
    "priors_snapshot": "phase2_priors_snapshot.json",
    "sources": "sources.json",
    "manifest": "manifest.json",
}


REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "mobility_overall": ("metric_id", "value_pct", "source_id"),
    "mobility_by_age_cohort": ("age_cohort_id", "moved_past_year_pct", "source_id"),
    "marriage_divorce_rates": ("metric_id", "value", "source_id"),
    "fertility_by_age": ("age_group", "birth_rate_per_1000_women", "source_id"),
    "household_type_shares": ("household_type_id", "share_of_all_households_pct", "source_id"),
}


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def load_phase2_params(params_dir: Path) -> dict[str, Any]:
    root = params_dir.resolve()
    missing = [
        filename
        for filename in REQUIRED_PARAM_FILES.values()
        if not (root / filename).exists()
    ]
    if missing:
        raise FileNotFoundError(
            f"Missing required Phase-2 parameter files in {root}: {', '.join(sorted(missing))}"
        )

    tables: dict[str, Any] = {}
    for logical_name, filename in REQUIRED_PARAM_FILES.items():
        path = root / filename
        if filename.endswith(".csv"):
            tables[logical_name] = pd.read_csv(path)
        else:
            tables[logical_name] = _load_json(path)

    for table_name, required_columns in REQUIRED_COLUMNS.items():
        df = tables[table_name]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"{table_name} is missing required columns: {', '.join(missing_cols)}"
            )

    return tables


def default_phase2_params_dir(project_root: Path) -> Path:
    return project_root.resolve() / "Data" / "phase2_params"


def load_phase2_params_from_project(project_root: Path) -> dict[str, Any]:
    return load_phase2_params(default_phase2_params_dir(project_root))
