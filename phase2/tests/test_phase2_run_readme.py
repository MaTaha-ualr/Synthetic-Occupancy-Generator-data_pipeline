from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "phase2" / "src"))

from sog_phase2.run_readme import build_run_readme_text, write_run_readme


def _make_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "2026-03-10_single_movers_seed20260310"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": "2026-03-10_single_movers_seed20260310",
                "scenario_id": "single_movers",
                "seed": 20260310,
                "generated_at_utc": "2026-03-10T00:00:00Z",
                "emission_meta": {
                    "crossfile_match_mode": "one_to_one",
                    "simulation_start_date": "2026-01-01",
                    "simulation_end_date": "2027-01-01",
                    "dataset_ids": ["A", "B"],
                },
                "observed_outputs": {
                    "datasets": [
                        {"dataset_id": "A", "filename": "DatasetA.csv"},
                        {"dataset_id": "B", "filename": "DatasetB.csv"},
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "quality_report.json").write_text(
        json.dumps(
            {
                "status": "ok",
                "truth_counts": {"truth_people": 10, "truth_events": 3},
                "simulation_quality": {"event_counts": {"MOVE": 3}},
                "phase2_quality": {
                    "er_benchmark_metrics": {
                        "cross_file_overlap": {"overlap_entities": 7, "overlap_pct_of_union": 70.0}
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump({"scenario_id": "single_movers", "seed": 20260310}), encoding="utf-8"
    )
    (run_dir / "DatasetA.csv").write_text("RecordKey\nA-1\n", encoding="utf-8")
    (run_dir / "entity_record_map.csv").write_text("PersonKey,DatasetId,RecordKey\n1,A,A-1\n", encoding="utf-8")
    return run_dir


def test_build_run_readme_text_includes_core_facts(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path)
    text = build_run_readme_text(run_dir)
    assert "2026-03-10_single_movers_seed20260310" in text
    assert "single_movers" in text
    assert "one_to_one" in text
    assert "entity_record_map.csv" in text
    # Answer-key guidance must be present so consumers use the canonical key.
    assert "canonical" in text.lower()
    # Event counts surfaced.
    assert "MOVE" in text


def test_write_run_readme_creates_file(tmp_path: Path) -> None:
    run_dir = _make_run_dir(tmp_path)
    path = write_run_readme(run_dir)
    assert path == run_dir / "README.md"
    assert path.exists()
    assert path.read_text(encoding="utf-8").startswith("# Run:")


def test_build_run_readme_text_is_robust_to_missing_metadata(tmp_path: Path) -> None:
    # Only a truth file present, no manifest/quality — should not raise.
    run_dir = tmp_path / "2026-03-10_x_seed1"
    run_dir.mkdir()
    (run_dir / "truth_people.parquet").write_text("", encoding="utf-8")
    text = build_run_readme_text(run_dir)
    assert "2026-03-10_x_seed1" in text
    assert "truth_people.parquet" in text
