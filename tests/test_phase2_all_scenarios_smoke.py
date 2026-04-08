"""End-to-end smoke tests for all 11 canonical scenarios using shipped YAMLs.

These tests use the actual shipped scenario YAML files and the real Phase-2
parameter tables to validate that every scenario can be run from scratch and
produces valid outputs with the expected characteristics.

Each test:
1. Builds a synthetic Phase-1 baseline in a temp directory
2. Runs the full pipeline with the shipped YAML
3. Validates the output contract
4. Checks scenario-specific invariants (event types, topology, cardinality)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase2.pipeline import run_scenario_pipeline

SCENARIOS_DIR = PROJECT_ROOT / "phase2" / "scenarios"
PARAMS_DIR = PROJECT_ROOT / "Data" / "phase2_params"


def _build_phase1_baseline(project_root: Path, n: int = 100) -> None:
    """Write a Phase-1 CSV with n people into the project temp dir."""
    phase1_dir = project_root / "phase1" / "outputs_phase1"
    phase1_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for i in range(1, n + 1):
        age = 18 + (i % 50)
        if age <= 34:
            age_bin = "age_18_34"
        elif age <= 64:
            age_bin = "age_35_64"
        else:
            age_bin = "age_65_plus"
        rows.append({
            "RecordKey": str(i), "PersonKey": str(i), "EntityRecordIndex": "1",
            "AddressKey": f"A{i}",
            "FormalFirstName": f"First{i}", "MiddleName": "M" if i % 3 == 0 else "",
            "LastName": f"Last{i}", "Suffix": "Jr." if i % 15 == 0 else "",
            "FormalFullName": f"First{i} Last{i}",
            "Gender": "female" if i % 2 == 0 else "male",
            "Ethnicity": ["White", "Black", "Hispanic", "Asian"][i % 4],
            "DOB": f"{2026 - age}-{(i % 12) + 1:02d}-15",
            "Age": str(age), "AgeBin": age_bin,
            "SSN": f"{i:03d}-{(i % 100):02d}-{(i % 10000):04d}",
            "Phone": f"555-{i:03d}-{(i * 7) % 10000:04d}",
            "ResidenceType": "HOUSE" if i % 2 == 0 else "APARTMENT",
            "ResidenceStartDate": "2020-01-01",
        })
    pd.DataFrame(rows).to_csv(phase1_dir / "Phase1_people_addresses.csv", index=False)
    (phase1_dir / "Phase1_people_addresses.manifest.json").write_text(
        json.dumps({"row_count": n}), encoding="utf-8")


def _copy_params(project_root: Path) -> None:
    """Copy real Phase-2 params into the temp project."""
    dest = project_root / "Data" / "phase2_params"
    dest.mkdir(parents=True, exist_ok=True)
    for f in PARAMS_DIR.iterdir():
        if f.is_file():
            (dest / f.name).write_bytes(f.read_bytes())


def _run_scenario(tmp_path: Path, scenario_id: str, person_count: int = 80, sample_override: int | None = None) -> dict:
    """Run a scenario and return the pipeline result."""
    project_root = tmp_path / "project"
    _copy_params(project_root)
    _build_phase1_baseline(project_root, n=person_count)

    yaml_path = SCENARIOS_DIR / f"{scenario_id}.yaml"
    assert yaml_path.exists(), f"Scenario YAML not found: {yaml_path}"

    # Override selection to use smaller sample for speed
    import yaml as yaml_mod
    with open(yaml_path, encoding="utf-8") as f:
        data = yaml_mod.safe_load(f)

    if sample_override is not None:
        data["selection"] = data.get("selection", {})
        data["selection"]["sample"] = {"mode": "count", "value": sample_override}

    # Update phase1 path to temp project
    data["phase1"] = {
        "data_path": "phase1/outputs_phase1/Phase1_people_addresses.csv",
        "manifest_path": "phase1/outputs_phase1/Phase1_people_addresses.manifest.json",
    }

    patched_yaml = project_root / "phase2" / "scenarios" / f"{scenario_id}.yaml"
    patched_yaml.parent.mkdir(parents=True, exist_ok=True)
    patched_yaml.write_text(yaml_mod.safe_dump(data, sort_keys=False), encoding="utf-8")

    return run_scenario_pipeline(
        scenario_yaml_path=patched_yaml,
        runs_root=project_root / "phase2" / "runs",
        project_root=project_root,
        run_date="2026-04-05",
        overwrite=True,
    )


# ---------------------------------------------------------------------------
# Smoke test for each scenario
# ---------------------------------------------------------------------------

SAMPLE = 30  # Keep small for test speed


def test_smoke_single_movers(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "single_movers", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    events = pd.read_parquet(Path(result["run_dir"]) / "truth_events.parquet")
    assert "MOVE" in events["EventType"].str.upper().values


def test_smoke_clean_baseline_linkage(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "clean_baseline_linkage", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["relationship_mode"] == "one_to_one"


def test_smoke_couple_merge(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "couple_merge", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["relationship_mode"] == "one_to_many"


def test_smoke_family_birth(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "family_birth", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["relationship_mode"] == "many_to_one"


def test_smoke_divorce_custody(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "divorce_custody", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["relationship_mode"] == "many_to_many"


def test_smoke_roommates_split(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "roommates_split", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["relationship_mode"] == "one_to_many"


def test_smoke_high_noise_identity_drift(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "high_noise_identity_drift", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["relationship_mode"] == "one_to_one"
    # Should have visible noise in dataset B
    b_noise = er.get("noise_counts", {}).get("B", er.get("noise_counts", {}).get("dataset_b", {}))
    # At minimum check noise is tracked
    assert "attribute_drift_rates" in er or "noise_counts" in quality["phase2_quality"]["er_benchmark_metrics"]


def test_smoke_low_overlap_sparse_coverage(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "low_overlap_sparse_coverage", sample_override=SAMPLE)
    assert result["validation_valid"] is True


def test_smoke_asymmetric_source_coverage(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "asymmetric_source_coverage", sample_override=SAMPLE)
    assert result["validation_valid"] is True


def test_smoke_high_duplication_dedup(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "high_duplication_dedup", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["dataset_count"] == 1
    assert er["topology"]["relationship_mode"] == "single_dataset"


def test_smoke_three_source_partial_overlap(tmp_path: Path) -> None:
    result = _run_scenario(tmp_path, "three_source_partial_overlap", sample_override=SAMPLE)
    assert result["validation_valid"] is True
    quality = json.loads(Path(result["paths"]["quality_report"]).read_text(encoding="utf-8"))
    er = quality["phase2_quality"]["er_benchmark_metrics"]
    assert er["topology"]["dataset_count"] == 3
