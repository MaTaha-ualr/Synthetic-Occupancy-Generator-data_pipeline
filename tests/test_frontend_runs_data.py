from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

import matplotlib.pyplot as plt

import frontend.sog_tools as sog_tools
import frontend.visualizations.core as viz_core
from frontend.visualizations.charts.quality import generate_overlap_venn


def _seed_run(run_root: Path, run_id: str) -> Path:
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    (run_dir / "scenario.yaml").write_text(
        dedent(
            """
            scenario_id: single_movers
            seed: 20260310
            emission:
              crossfile_match_mode: one_to_one
              overlap_entity_pct: 70.0
              duplication_in_A_pct: 4.0
              duplication_in_B_pct: 6.0
              noise:
                A:
                  name_typo_pct: 1.0
                  phonetic_error_pct: 0.0
                  ocr_error_pct: 0.0
                  nickname_pct: 0.0
                B:
                  name_typo_pct: 2.5
                  phonetic_error_pct: 0.0
                  ocr_error_pct: 0.0
                  nickname_pct: 0.0
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "quality_report.json").write_text(
        json.dumps(
            {
                "phase2_quality": {
                    "er_benchmark_metrics": {
                        "cross_file_overlap": {
                            "overlap_entities": 75,
                            "a_entities": 100,
                            "b_entities": 110,
                            "union_entities": 135,
                        }
                    }
                },
                "observed_quality": {"coverage": {}},
                "simulation_quality": {"event_counts": {"MOVE": 10}},
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "DatasetA.csv").write_text(
        "A_RecordKey,FirstName,MiddleName,LastName,Suffix,DOB,SSN,Phone\n"
        "A-1,Alice,,Jones,,1980-01-01,111-11-1111,555-0001\n",
        encoding="utf-8",
    )
    (run_dir / "DatasetB.csv").write_text(
        "B_RecordKey,FirstName,MiddleName,LastName,Suffix,DOB,SSN,Phone\n"
        "B-1,Alicia,,Jones,,1980-01-01,111-11-1111,\n",
        encoding="utf-8",
    )
    return run_dir


def test_generate_chart_uses_html_for_noise_radar(monkeypatch, tmp_path):
    run_id = "2026-03-10_single_movers_seed20260310"
    run_root = tmp_path / "runs"
    _seed_run(run_root, run_id)

    monkeypatch.setattr(viz_core, "RUNS_ROOT", run_root)
    monkeypatch.setattr(viz_core, "CHARTS_DIR", tmp_path / ".charts")

    result = sog_tools.generate_chart(run_id, "noise_radar")

    chart_path = Path(result["chart_path"])
    assert chart_path.suffix == ".html"
    assert chart_path.read_text(encoding="utf-8").lstrip().startswith("<html>")


def test_generate_dashboard_keeps_noise_radar_as_html(monkeypatch, tmp_path):
    run_id = "2026-03-10_single_movers_seed20260310"
    run_root = tmp_path / "runs"
    _seed_run(run_root, run_id)

    monkeypatch.setattr(viz_core, "RUNS_ROOT", run_root)
    monkeypatch.setattr(viz_core, "CHARTS_DIR", tmp_path / ".charts")

    result = sog_tools.generate_dashboard(run_id)
    chart_paths = {item["chart_type"]: Path(item["chart_path"]).suffix for item in result["charts"]}

    assert chart_paths["noise_radar"] == ".html"
    assert chart_paths["difficulty_scorecard"] == ".png"
    assert chart_paths["overlap_venn"] == ".png"
    assert chart_paths["missing_matrix"] == ".png"


def test_generate_overlap_venn_reads_current_quality_report_keys(tmp_path):
    run_dir = _seed_run(tmp_path, "2026-03-10_single_movers_seed20260310")

    fig, data = generate_overlap_venn(run_dir)
    plt.close(fig)

    assert data["entities_in_both"] == 75
    assert data["entities_only_in_a"] == 25
    assert data["entities_only_in_b"] == 35
    assert round(data["overlap_pct"], 2) == round(75 / 135 * 100, 2)


def test_generate_overlap_venn_supports_multi_dataset_summary(tmp_path):
    run_dir = tmp_path / "2026-04-05_multi_dataset_seed20260405"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "quality_report.json").write_text(
        json.dumps(
            {
                "phase2_quality": {
                    "er_benchmark_metrics": {
                        "topology": {
                            "dataset_count": 3,
                            "dataset_ids": ["registry", "claims", "benefits"],
                            "relationship_mode": "many_to_many",
                        },
                        "multi_dataset_overlap": {
                            "dataset_ids": ["registry", "claims", "benefits"],
                            "all_dataset_overlap_entities": 42,
                            "union_entities": 120,
                            "all_dataset_overlap_pct_of_union": 35.0,
                            "pairwise_overlap": {
                                "registry__claims": {
                                    "dataset_ids": ["registry", "claims"],
                                    "overlap_entities": 70,
                                    "union_entities": 120,
                                    "overlap_pct_of_union": 58.3,
                                },
                                "registry__benefits": {
                                    "dataset_ids": ["registry", "benefits"],
                                    "overlap_entities": 63,
                                    "union_entities": 118,
                                    "overlap_pct_of_union": 53.4,
                                },
                            },
                        },
                    }
                },
                "observed_quality": {"coverage": {}},
            }
        ),
        encoding="utf-8",
    )

    fig, data = generate_overlap_venn(run_dir)
    plt.close(fig)

    assert data["mode"] == "multi_dataset"
    assert data["dataset_count"] == 3
    assert data["all_dataset_overlap_entities"] == 42
    assert data["pairwise_overlap_pairs"][0]["pair"] == "registry vs claims"


def test_get_run_results_reads_single_dataset_manifest_outputs(monkeypatch, tmp_path):
    run_id = "2026-04-05_registry_dedup_seed20260405"
    run_root = tmp_path / "runs"
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    observed = run_dir / "observed_registry.csv"
    entity_record_map = run_dir / "entity_record_map.csv"
    observed.write_text(
        "RecordKey,DatasetId,FirstName,LastName,DOB,AddressKey,SourceSnapshotDate\n"
        "R-1,registry,Ava,Stone,1992-01-01,A1,2026-12-31\n",
        encoding="utf-8",
    )
    entity_record_map.write_text(
        "PersonKey,DatasetId,RecordKey\n1,registry,R-1\n",
        encoding="utf-8",
    )
    (run_dir / "quality_report.json").write_text(
        json.dumps({"status": "ok", "truth_counts": {}, "simulation_quality": {}, "observed_quality": {}}),
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "scenario_id": "registry_dedup",
                "seed": 20260405,
                "observed_outputs": {
                    "datasets": [
                        {
                            "dataset_id": "registry",
                            "filename": "observed_registry.csv",
                            "path": str(observed),
                        }
                    ],
                    "entity_record_map": str(entity_record_map),
                    "truth_crosswalk": "",
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(sog_tools, "RUNS_ROOT", run_root)
    result = sog_tools.get_run_results(run_id)

    assert result["scenario_id"] == "registry_dedup"
    assert result["download_paths"]["registry"] == str(observed)
    assert result["download_paths"]["entity_record_map"] == str(entity_record_map)
    assert "truth_crosswalk" not in result["download_paths"]


def test_get_run_results_reads_pairwise_crosswalks_for_multi_dataset_manifest(monkeypatch, tmp_path):
    run_id = "2026-04-05_multi_dataset_seed20260405"
    run_root = tmp_path / "runs"
    run_dir = run_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    registry = run_dir / "observed_registry.csv"
    claims = run_dir / "observed_claims.csv"
    crosswalk = run_dir / "truth_crosswalk__registry__claims.csv"
    entity_record_map = run_dir / "entity_record_map.csv"
    registry.write_text(
        "RecordKey,DatasetId,FirstName,LastName,DOB,AddressKey,SourceSnapshotDate\n"
        "R-1,registry,Ava,Stone,1992-01-01,A1,2026-12-31\n",
        encoding="utf-8",
    )
    claims.write_text(
        "RecordKey,DatasetId,FirstName,LastName,DOB,AddressKey,SourceSnapshotDate\n"
        "C-1,claims,Ava,Stone,1992-01-01,A1,2026-12-31\n",
        encoding="utf-8",
    )
    crosswalk.write_text(
        "PersonKey,A_RecordKey,B_RecordKey\n1,R-1,C-1\n",
        encoding="utf-8",
    )
    entity_record_map.write_text(
        "PersonKey,DatasetId,RecordKey\n1,registry,R-1\n1,claims,C-1\n",
        encoding="utf-8",
    )
    (run_dir / "quality_report.json").write_text(
        json.dumps({"status": "ok", "truth_counts": {}, "simulation_quality": {}, "observed_quality": {}}),
        encoding="utf-8",
    )
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "scenario_id": "multi_dataset",
                "seed": 20260405,
                "observed_outputs": {
                    "datasets": [
                        {"dataset_id": "registry", "filename": "observed_registry.csv", "path": str(registry)},
                        {"dataset_id": "claims", "filename": "observed_claims.csv", "path": str(claims)},
                    ],
                    "entity_record_map": str(entity_record_map),
                    "truth_crosswalk": "",
                    "pairwise_crosswalks": [
                        {
                            "dataset_ids": ["registry", "claims"],
                            "filename": "truth_crosswalk__registry__claims.csv",
                            "path": str(crosswalk),
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(sog_tools, "RUNS_ROOT", run_root)
    result = sog_tools.get_run_results(run_id)

    assert result["download_paths"]["registry"] == str(registry)
    assert result["download_paths"]["claims"] == str(claims)
    assert result["download_paths"]["truth_crosswalk__registry__claims"] == str(crosswalk)
