from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

import frontend.agents.export_agent as export_agent


def _seed_run(run_root: Path, run_id: str) -> Path:
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "DatasetA.csv").write_text(
        "A_RecordKey,FirstName,MiddleName,LastName,Suffix,FullName,DOB,Age,SSN,Phone,AddressKey,Gender,Ethnicity,SourceSnapshotDate,SourceSystem\n"
        "A-1,Alice,,Jones,,Alice Jones,1980-01-01,44,111-11-1111,555-0001,ADDR-1,female,GroupA,2026-01-01,DatasetA\n",
        encoding="utf-8",
    )
    (run_dir / "DatasetB.csv").write_text(
        "B_RecordKey,FirstName,MiddleName,LastName,Suffix,FullName,DOB,Age,SSN,Phone,AddressKey,Gender,Ethnicity,SourceSnapshotDate,SourceSystem\n"
        "B-1,Alicia,,Jones,,Alicia Jones,1980-01-01,44,111-11-1111,555-0002,ADDR-1,female,GroupA,2026-01-01,DatasetB\n",
        encoding="utf-8",
    )
    (run_dir / "truth_crosswalk.csv").write_text(
        "A_RecordKey,B_RecordKey,PersonKey\nA-1,B-1,p1\n",
        encoding="utf-8",
    )
    return run_dir


def _seed_multi_dataset_run(run_root: Path, run_id: str) -> Path:
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "observed_registry.csv").write_text(
        "RecordKey,FirstName,MiddleName,LastName,Suffix,FullName,DOB,Age,SSN,Phone,AddressKey,Gender,Ethnicity,SourceSnapshotDate,SourceSystem\n"
        "R-1,Alice,,Jones,,Alice Jones,1980-01-01,44,111-11-1111,555-0001,ADDR-1,female,GroupA,2026-01-01,registry\n",
        encoding="utf-8",
    )
    (run_dir / "observed_claims.csv").write_text(
        "RecordKey,FirstName,MiddleName,LastName,Suffix,FullName,DOB,Age,SSN,Phone,AddressKey,Gender,Ethnicity,SourceSnapshotDate,SourceSystem\n"
        "C-1,Alicia,,Jones,,Alicia Jones,1980-01-01,44,111-11-1111,555-0002,ADDR-1,female,GroupA,2026-01-01,claims\n",
        encoding="utf-8",
    )
    (run_dir / "observed_benefits.csv").write_text(
        "RecordKey,FirstName,MiddleName,LastName,Suffix,FullName,DOB,Age,SSN,Phone,AddressKey,Gender,Ethnicity,SourceSnapshotDate,SourceSystem\n"
        "B-1,Alice,,Jones,,Alice Jones,1980-01-01,44,111-11-1111,555-0003,ADDR-1,female,GroupA,2026-01-01,benefits\n",
        encoding="utf-8",
    )
    (run_dir / "truth_crosswalk__registry__claims.csv").write_text(
        "A_RecordKey,B_RecordKey,PersonKey\nR-1,C-1,p1\n",
        encoding="utf-8",
    )
    (run_dir / "truth_crosswalk__registry__benefits.csv").write_text(
        "A_RecordKey,B_RecordKey,PersonKey\nR-1,B-1,p1\n",
        encoding="utf-8",
    )
    (run_dir / "truth_crosswalk__claims__benefits.csv").write_text(
        "A_RecordKey,B_RecordKey,PersonKey\nC-1,B-1,p1\n",
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "observed_outputs": {
                    "datasets": [
                        {"dataset_id": "registry", "path": str(run_dir / "observed_registry.csv")},
                        {"dataset_id": "claims", "path": str(run_dir / "observed_claims.csv")},
                        {"dataset_id": "benefits", "path": str(run_dir / "observed_benefits.csv")},
                    ],
                    "pairwise_crosswalks": [
                        {"dataset_ids": ["registry", "claims"], "path": str(run_dir / "truth_crosswalk__registry__claims.csv")},
                        {"dataset_ids": ["registry", "benefits"], "path": str(run_dir / "truth_crosswalk__registry__benefits.csv")},
                        {"dataset_ids": ["claims", "benefits"], "path": str(run_dir / "truth_crosswalk__claims__benefits.csv")},
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    return run_dir


def test_export_for_splink_normalizes_schema(monkeypatch, tmp_path):
    run_id = "2026-03-10_single_movers_seed20260310"
    run_root = tmp_path / "runs"
    exports_root = tmp_path / "exports"
    _seed_run(run_root, run_id)

    monkeypatch.setattr(export_agent, "RUNS_ROOT", run_root)
    monkeypatch.setattr(export_agent, "EXPORTS_ROOT", exports_root)

    result = export_agent.export_for_splink(run_id)
    assert result["success"] is True

    dataset_a = pd.read_parquet(Path(result["output_dir"]) / "DatasetA_splink.parquet")
    assert "unique_id" in dataset_a.columns
    assert "source_dataset" in dataset_a.columns
    assert "A_RecordKey" not in dataset_a.columns
    assert dataset_a.loc[0, "unique_id"] == "A-1"
    assert dataset_a.loc[0, "source_dataset"] == "DatasetA"


def test_export_for_zingg_writes_matching_field_config(monkeypatch, tmp_path):
    run_id = "2026-03-10_single_movers_seed20260310"
    run_root = tmp_path / "runs"
    exports_root = tmp_path / "exports"
    _seed_run(run_root, run_id)

    monkeypatch.setattr(export_agent, "RUNS_ROOT", run_root)
    monkeypatch.setattr(export_agent, "EXPORTS_ROOT", exports_root)

    result = export_agent.export_for_zingg(run_id)
    assert result["success"] is True

    dataset_a = pd.read_csv(Path(result["output_dir"]) / "DatasetA.csv", dtype=str)
    assert "RecordKey" in dataset_a.columns
    assert "SourceDataset" in dataset_a.columns
    assert "A_RecordKey" not in dataset_a.columns

    config = json.loads((Path(result["output_dir"]) / "zingg_field_config.json").read_text(encoding="utf-8"))
    field_names = {field["fieldName"] for field in config["fieldDefinition"]}
    assert "AddressKey" in field_names
    assert "SourceDataset" in field_names
    assert "StreetAddress" not in field_names


def test_export_for_splink_copies_pairwise_crosswalks_for_multi_dataset_run(monkeypatch, tmp_path):
    run_id = "2026-04-05_multi_dataset_seed20260405"
    run_root = tmp_path / "runs"
    exports_root = tmp_path / "exports"
    _seed_multi_dataset_run(run_root, run_id)

    monkeypatch.setattr(export_agent, "RUNS_ROOT", run_root)
    monkeypatch.setattr(export_agent, "EXPORTS_ROOT", exports_root)

    result = export_agent.export_for_splink(run_id)
    assert result["success"] is True

    output_dir = Path(result["output_dir"])
    assert (output_dir / "truth_crosswalk__registry__claims.csv").exists()
    assert (output_dir / "truth_crosswalk__registry__benefits.csv").exists()
    assert (output_dir / "truth_crosswalk__claims__benefits.csv").exists()
