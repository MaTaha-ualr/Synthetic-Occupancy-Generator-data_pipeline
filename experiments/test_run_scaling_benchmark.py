from __future__ import annotations

import copy
from pathlib import Path

from experiments.run_scaling_benchmark import (
    aggregate_file_checksum,
    build_reproducibility_rows,
    build_summary_rows,
    repository_relative_path,
    retry_path_io,
    scaled_phase1_config,
    scaled_scenario_config,
    weighted_distribution_stats,
)


def test_repository_relative_path_uses_portable_separators(tmp_path: Path) -> None:
    artifact = tmp_path / "experiments" / "scaling_artifacts" / "trial"

    assert repository_relative_path(artifact, tmp_path) == (
        "experiments/scaling_artifacts/trial"
    )


def test_retry_path_io_recovers_from_transient_os_errors(monkeypatch) -> None:
    attempts = 0

    def flaky_operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise OSError("temporarily unavailable")
        return "written"

    monkeypatch.setattr(
        "experiments.run_scaling_benchmark.PATH_IO_RETRY_INTERVAL_SECONDS", 0.0
    )
    assert retry_path_io(flaky_operation, description="test write") == "written"
    assert attempts == 3


def test_scaled_phase1_config_preserves_canonical_and_uses_1_4_ratio(tmp_path: Path) -> None:
    canonical = {
        "phase1": {
            "n_people": 10,
            "n_records": 14,
            "seed": 1,
            "output": {"format": "csv", "path": "old.csv", "chunk_size": 5000},
            "redundancy": {"enabled": True, "shape": "heavy_tail"},
        }
    }
    before = copy.deepcopy(canonical)
    result = scaled_phase1_config(
        canonical,
        population=50_000,
        seed=20260829,
        output_path=tmp_path / "phase1.csv",
    )

    assert canonical == before
    assert result["phase1"]["n_people"] == 50_000
    assert result["phase1"]["n_records"] == 70_000
    assert result["phase1"]["seed"] == 20260829
    assert result["phase1"]["redundancy"] == canonical["phase1"]["redundancy"]
    assert result["phase1"]["output"]["chunk_size"] == 5000


def test_scaled_scenario_changes_only_seed_and_phase1_paths(tmp_path: Path) -> None:
    canonical = {
        "scenario_id": "clean_baseline_linkage",
        "seed": 7,
        "phase1": {"data_path": "old.csv", "manifest_path": "old.json"},
        "emission": {"appearance_A_pct": 96.0},
    }
    before = copy.deepcopy(canonical)
    result = scaled_scenario_config(
        canonical,
        seed=20260830,
        phase1_csv_path=tmp_path / "input.csv",
        phase1_manifest_path=tmp_path / "input.manifest.json",
    )

    assert canonical == before
    assert result["seed"] == 20260830
    assert result["phase1"]["data_path"].endswith("input.csv")
    assert result["phase1"]["manifest_path"].endswith("input.manifest.json")
    assert result["emission"] == canonical["emission"]


def test_weighted_distribution_stats_uses_population_standard_deviation() -> None:
    mean, median, sd = weighted_distribution_stats({"1": 2, "2": 2})
    assert mean == 1.5
    assert median == 1.5
    assert sd == 0.5


def test_aggregate_file_checksum_is_order_independent_and_content_sensitive(tmp_path: Path) -> None:
    first = tmp_path / "a.txt"
    second = tmp_path / "b.txt"
    first.write_text("alpha", encoding="utf-8")
    second.write_text("beta", encoding="utf-8")
    expected = aggregate_file_checksum([first, second], base_dir=tmp_path)
    assert aggregate_file_checksum([second, first], base_dir=tmp_path) == expected
    second.write_text("changed", encoding="utf-8")
    assert aggregate_file_checksum([first, second], base_dir=tmp_path) != expected


def test_summary_uses_only_main_runs() -> None:
    rows = [
        {
            "run_kind": "main",
            "requested_population": "10000",
            "status": "success",
            "all_output_contract_checks_passed": "True",
            "generation_wall_seconds": "10",
            "records_generated_per_second": "100",
            "peak_process_rss_mb": "50",
            "total_observed_records": "1000",
            "people_sharing_identical_full_name": "200",
            "records_per_person_median": "2",
            "records_per_person_sd": "0.5",
        },
        {
            "run_kind": "repro",
            "requested_population": "10000",
            "status": "success",
            "all_output_contract_checks_passed": "True",
            "generation_wall_seconds": "1000",
            "records_generated_per_second": "1",
            "peak_process_rss_mb": "500",
            "total_observed_records": "1000",
        },
    ]
    summary = build_summary_rows(rows, [10_000], planned_seed_count=3)[0]
    assert summary["successful_seed_count"] == 1
    assert summary["mean_generation_wall_seconds"] == 10.0
    assert summary["mean_peak_process_rss_mb"] == 50.0
    assert summary["mean_people_sharing_identical_full_name"] == 200.0
    assert summary["mean_records_per_person_median"] == 2.0
    assert summary["mean_records_per_person_sd"] == 0.5


def test_reproducibility_requires_nonempty_matching_truth_and_output_hashes() -> None:
    rows = [
        {
            "trial_id": "n10000_seed20260829_main",
            "status": "success",
            "selection_personkey_sha256": "selection",
            "phase1_output_sha256": "phase1",
            "truth_checksum_sha256": "truth",
            "output_checksum_sha256": "output",
        },
        {
            "trial_id": "n10000_seed20260829_repro",
            "status": "success",
            "selection_personkey_sha256": "selection",
            "phase1_output_sha256": "phase1",
            "truth_checksum_sha256": "truth",
            "output_checksum_sha256": "output",
        },
    ]
    repro = build_reproducibility_rows(rows, [10_000], 20260829)[0]
    assert repro["selection_checksum_agrees"] is True
    assert repro["phase1_checksum_agrees"] is True
    assert repro["deterministic_truth_output_checksums_agree"] is True


def test_reproducibility_marks_unfinished_repeat_as_unavailable() -> None:
    rows = [
        {
            "trial_id": "n10000_seed20260829_main",
            "status": "success",
            "phase1_output_sha256": "phase1",
            "truth_checksum_sha256": "truth",
            "output_checksum_sha256": "output",
        }
    ]
    repro = build_reproducibility_rows(rows, [10_000], 20260829)[0]
    assert repro["reference_status"] == "success"
    assert repro["repeat_status"] == "not_run"
    assert repro["phase1_checksum_agrees"] == ""
    assert repro["deterministic_truth_output_checksums_agree"] == ""
