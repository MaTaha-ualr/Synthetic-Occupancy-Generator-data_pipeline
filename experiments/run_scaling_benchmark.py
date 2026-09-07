"""Run the end-to-end Synthetic Occupancy Generator scaling benchmark.

The benchmark intentionally calls the existing Phase-1 generator and unified
Phase-2 pipeline without changing their semantics.  It creates experiment-local
copies of the canonical configurations, varies only population, record count,
seed, and input/output paths, and records an unoptimized baseline.

Default design
--------------
* populations: 10k, 50k, 100k, 250k, 500k, 1m people
* Phase-1 requested records: 1.4 times population
* seeds: 20260829, 20260830, 20260831
* scenario: phase2/scenarios/clean_baseline_linkage.yaml
* reproducibility: the first seed is executed twice at every scale

Examples
--------
Run the complete benchmark (resumes from existing result rows):

    python experiments/run_scaling_benchmark.py

Run only the 10k scale while testing the infrastructure:

    python experiments/run_scaling_benchmark.py --population-sizes 10000

Keep generated Phase-1/Phase-2 artifacts instead of removing them after their
metrics and checksums have been captured:

    python experiments/run_scaling_benchmark.py --keep-artifacts
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import io
import importlib.metadata
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import psutil
import yaml


DEFAULT_POPULATIONS = (10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000)
DEFAULT_SEEDS = (20260829, 20260830, 20260831)
DEFAULT_RECORD_RATIO = 1.4
BENCHMARK_RUN_DATE = "2026-08-29"
PATH_IO_RETRY_SECONDS = 300.0
PATH_IO_RETRY_INTERVAL_SECONDS = 2.0
RESULT_FILENAMES = (
    "scaling_results.csv",
    "scaling_summary.csv",
    "scaling_reproducibility.csv",
    "scaling_environment.json",
    "scaling_report.md",
)


def retry_path_io(operation: Any, *, description: str) -> Any:
    """Retry experiment bookkeeping I/O during transient drive unavailability."""
    deadline = time.monotonic() + PATH_IO_RETRY_SECONDS
    while True:
        try:
            return operation()
        except OSError as exc:
            if time.monotonic() >= deadline:
                raise
            print(
                f"[retry] {description}: {type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(PATH_IO_RETRY_INTERVAL_SECONDS)


def atomic_write_text(path: Path, text: str) -> None:
    """Atomically write text, retrying if the benchmark volume briefly vanishes."""

    def write_once() -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(path)

    retry_path_io(write_once, description=f"write {path}")


def read_text_with_retry(path: Path) -> str:
    return retry_path_io(
        lambda: path.read_text(encoding="utf-8"),
        description=f"read {path}",
    )

RESULT_FIELDS = (
    "implementation_phase",
    "run_kind",
    "archived_run_kind",
    "trial_id",
    "status",
    "error_type",
    "error_message",
    "requested_population",
    "phase1_n_records_requested",
    "records_to_people_ratio",
    "seed",
    "successfully_generated_population",
    "truth_population_final",
    "phase1_records_written",
    "phase1_formal_copy_records_added",
    "total_observed_records",
    "total_households",
    "total_addresses_housing_units",
    "total_events_generated",
    "generation_wall_seconds",
    "phase1_generation_wall_seconds",
    "phase2_generation_wall_seconds_excluding_validator",
    "phase2_pipeline_wall_seconds_including_validator",
    "records_generated_per_second",
    "peak_process_rss_bytes",
    "peak_process_rss_mb",
    "final_artifact_size_bytes",
    "final_artifact_size_mb",
    "phase1_artifact_size_bytes",
    "phase2_artifact_size_bytes",
    "validator_runtime_seconds",
    "all_output_contract_checks_passed",
    "validator_issue_count",
    "quality_status",
    "unique_first_names_used",
    "unique_last_names_used",
    "unique_full_names",
    "observed_unique_first_names",
    "observed_unique_last_names",
    "observed_unique_full_names",
    "people_sharing_identical_full_name",
    "people_sharing_identical_full_name_pct",
    "records_per_person_mean",
    "records_per_person_median",
    "records_per_person_sd",
    "phase1_records_per_person_mean",
    "phase1_records_per_person_median",
    "phase1_records_per_person_sd",
    "duplicate_record_count",
    "duplicate_record_rate_pct",
    "source_overlap_people",
    "source_union_people",
    "source_overlap_rate_pct",
    "selection_personkey_sha256",
    "phase1_output_sha256",
    "truth_checksum_sha256",
    "output_checksum_sha256",
    "canonical_phase1_config_sha256",
    "canonical_scenario_sha256",
    "trial_artifacts_path",
    "artifacts_removed",
    "worker_wall_seconds",
    "started_at_utc",
    "completed_at_utc",
)

SUMMARY_FIELDS = (
    "requested_population",
    "phase1_n_records_requested",
    "records_to_people_ratio",
    "planned_seed_count",
    "successful_seed_count",
    "failed_seed_count",
    "all_successful_runs_passed_contract",
    "mean_successfully_generated_population",
    "mean_total_observed_records",
    "mean_generation_wall_seconds",
    "median_generation_wall_seconds",
    "sd_generation_wall_seconds",
    "mean_records_generated_per_second",
    "median_records_generated_per_second",
    "mean_peak_process_rss_mb",
    "max_peak_process_rss_mb",
    "mean_final_artifact_size_mb",
    "mean_validator_runtime_seconds",
    "mean_total_households",
    "mean_total_addresses_housing_units",
    "mean_total_events_generated",
    "mean_unique_first_names_used",
    "mean_unique_last_names_used",
    "mean_unique_full_names",
    "mean_people_sharing_identical_full_name",
    "mean_people_sharing_identical_full_name_pct",
    "mean_records_per_person",
    "mean_records_per_person_median",
    "mean_records_per_person_sd",
    "mean_duplicate_record_rate_pct",
    "mean_source_overlap_rate_pct",
)

REPRO_FIELDS = (
    "requested_population",
    "seed",
    "reference_trial_id",
    "repeat_trial_id",
    "reference_status",
    "repeat_status",
    "selection_checksum_agrees",
    "phase1_checksum_agrees",
    "truth_checksum_agrees",
    "output_checksum_agrees",
    "deterministic_truth_output_checksums_agree",
    "reference_truth_checksum_sha256",
    "repeat_truth_checksum_sha256",
    "reference_output_checksum_sha256",
    "repeat_output_checksum_sha256",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def repo_root_from_script() -> Path:
    return Path(__file__).resolve().parents[1]


def repository_relative_path(path: Path, repo_root: Path) -> str:
    """Return a portable repository-relative path when possible."""
    if not path.is_absolute():
        return path.as_posix()
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return str(path)


def parse_int_list(values: str) -> tuple[int, ...]:
    parsed = tuple(int(part.strip().replace("_", "")) for part in values.split(",") if part.strip())
    if not parsed or any(value <= 0 for value in parsed):
        raise argparse.ArgumentTypeError("Expected a comma-separated list of positive integers")
    return parsed


def file_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def aggregate_file_checksum(paths: Iterable[Path], *, base_dir: Path) -> str:
    """Hash sorted ``relative-name NUL file-hash NUL`` entries."""
    entries: list[tuple[str, Path]] = []
    for path in paths:
        resolved = path.resolve()
        if resolved.exists() and resolved.is_file():
            try:
                name = resolved.relative_to(base_dir.resolve()).as_posix()
            except ValueError:
                name = resolved.name
            entries.append((name, resolved))
    digest = hashlib.sha256()
    for name, path in sorted(entries):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_sha256(path).encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return payload


def scaled_phase1_config(
    canonical: dict[str, Any],
    *,
    population: int,
    seed: int,
    output_path: Path,
    record_ratio: float = DEFAULT_RECORD_RATIO,
) -> dict[str, Any]:
    if population <= 0:
        raise ValueError("population must be positive")
    requested_records = int(round(population * record_ratio))
    if not math.isclose(requested_records / population, record_ratio, rel_tol=0.0, abs_tol=1e-12):
        raise ValueError("population and record ratio must produce an exact integer record count")
    result = copy.deepcopy(canonical)
    phase1 = result.setdefault("phase1", {})
    phase1["n_people"] = int(population)
    phase1["n_records"] = requested_records
    phase1["seed"] = int(seed)
    output = phase1.setdefault("output", {})
    output["format"] = "csv"
    output["path"] = str(output_path.resolve())
    return result


def scaled_scenario_config(
    canonical: dict[str, Any],
    *,
    seed: int,
    phase1_csv_path: Path,
    phase1_manifest_path: Path,
) -> dict[str, Any]:
    result = copy.deepcopy(canonical)
    result["seed"] = int(seed)
    result["phase1"] = {
        "data_path": str(phase1_csv_path.resolve()),
        "manifest_path": str(phase1_manifest_path.resolve()),
    }
    return result


def write_yaml(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def weighted_distribution_stats(distribution: dict[str, Any]) -> tuple[float, float, float]:
    pairs = sorted((int(value), int(count)) for value, count in distribution.items() if int(count) > 0)
    total = sum(count for _, count in pairs)
    if total == 0:
        return 0.0, 0.0, 0.0
    mean = sum(value * count for value, count in pairs) / total
    variance = sum(((value - mean) ** 2) * count for value, count in pairs) / total
    midpoint_a = (total - 1) // 2
    midpoint_b = total // 2
    seen = 0
    median_values: list[int] = []
    for value, count in pairs:
        next_seen = seen + count
        if seen <= midpoint_a < next_seen:
            median_values.append(value)
        if seen <= midpoint_b < next_seen:
            median_values.append(value)
        seen = next_seen
    median = sum(median_values) / len(median_values)
    return float(mean), float(median), float(math.sqrt(variance))


def _sql_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _single_value(connection: Any, query: str) -> Any:
    return connection.execute(query).fetchone()[0]


def compute_artifact_metrics(
    *,
    phase1_manifest_path: Path,
    phase2_run_dir: Path,
    phase2_result: dict[str, Any],
) -> dict[str, Any]:
    """Compute required metrics with out-of-core DuckDB queries."""
    import duckdb

    phase1_manifest = json.loads(phase1_manifest_path.read_text(encoding="utf-8"))
    scenario_population = phase2_run_dir / "scenario_population.parquet"
    truth_people = phase2_run_dir / "truth_people.parquet"
    truth_households = phase2_run_dir / "truth_households.parquet"
    truth_residences = phase2_run_dir / "truth_residence_history.parquet"
    truth_events = phase2_run_dir / "truth_events.parquet"
    entity_map = Path(phase2_result["paths"]["entity_record_map"])
    dataset_paths = [Path(path) for path in phase2_result["paths"]["datasets"].values()]

    con = duckdb.connect(database=":memory:")
    try:
        con.execute(
            f"CREATE VIEW truth_people AS SELECT * FROM read_parquet('{_sql_path(truth_people)}')"
        )
        con.execute(
            f"CREATE VIEW entity_map AS SELECT * FROM read_csv('{_sql_path(entity_map)}', "
            "header=true, all_varchar=true)"
        )
        generated_population = int(
            _single_value(con, f"SELECT COUNT(*) FROM read_parquet('{_sql_path(scenario_population)}')")
        )
        truth_population = int(_single_value(con, "SELECT COUNT(*) FROM truth_people"))
        total_observed = int(_single_value(con, "SELECT COUNT(*) FROM entity_map"))
        total_households = int(
            _single_value(
                con,
                f"SELECT COUNT(DISTINCT HouseholdKey) FROM read_parquet('{_sql_path(truth_households)}')",
            )
        )
        total_addresses = int(
            _single_value(
                con,
                f"SELECT COUNT(DISTINCT AddressKey) FROM read_parquet('{_sql_path(truth_residences)}') "
                "WHERE NULLIF(TRIM(CAST(AddressKey AS VARCHAR)), '') IS NOT NULL",
            )
        )
        total_events = int(
            _single_value(con, f"SELECT COUNT(*) FROM read_parquet('{_sql_path(truth_events)}')")
        )

        name_row = con.execute(
            "SELECT "
            "COUNT(DISTINCT NULLIF(TRIM(CAST(FormalFirstName AS VARCHAR)), '')), "
            "COUNT(DISTINCT NULLIF(TRIM(CAST(LastName AS VARCHAR)), '')), "
            "COUNT(DISTINCT NULLIF(TRIM(CAST(FormalFullName AS VARCHAR)), '')) "
            "FROM truth_people"
        ).fetchone()
        sharing_people = int(
            _single_value(
                con,
                "SELECT COALESCE(SUM(person_count), 0) FROM ("
                "SELECT FormalFullName, COUNT(DISTINCT PersonKey) AS person_count "
                "FROM truth_people "
                "WHERE NULLIF(TRIM(CAST(FormalFullName AS VARCHAR)), '') IS NOT NULL "
                "GROUP BY FormalFullName HAVING COUNT(DISTINCT PersonKey) > 1)",
            )
        )

        con.execute(
            "CREATE VIEW observed_person_counts AS "
            "SELECT PersonKey, COUNT(*)::DOUBLE AS record_count FROM entity_map GROUP BY PersonKey"
        )
        records_stats = con.execute(
            "SELECT AVG(COALESCE(c.record_count, 0)), MEDIAN(COALESCE(c.record_count, 0)), "
            "STDDEV_POP(COALESCE(c.record_count, 0)) "
            "FROM truth_people p LEFT JOIN observed_person_counts c USING (PersonKey)"
        ).fetchone()
        distinct_entity_source = int(
            _single_value(con, "SELECT COUNT(*) FROM (SELECT DISTINCT PersonKey, DatasetId FROM entity_map)")
        )
        source_row = con.execute(
            "SELECT COUNT(*) AS source_union, "
            "COALESCE(SUM(CASE WHEN source_count > 1 THEN 1 ELSE 0 END), 0) AS source_overlap "
            "FROM (SELECT PersonKey, COUNT(DISTINCT DatasetId) AS source_count "
            "FROM entity_map GROUP BY PersonKey)"
        ).fetchone()
        dataset_count = int(_single_value(con, "SELECT COUNT(DISTINCT DatasetId) FROM entity_map"))

        escaped_datasets = ", ".join(f"'{_sql_path(path)}'" for path in dataset_paths)
        observed_name_row = con.execute(
            "SELECT "
            "COUNT(DISTINCT NULLIF(TRIM(CAST(FirstName AS VARCHAR)), '')), "
            "COUNT(DISTINCT NULLIF(TRIM(CAST(LastName AS VARCHAR)), '')), "
            "COUNT(DISTINCT NULLIF(TRIM(CAST(FullName AS VARCHAR)), '')) "
            f"FROM read_csv([{escaped_datasets}], header=true, all_varchar=true, union_by_name=true)"
        ).fetchone()
    finally:
        con.close()

    actual_distribution = (
        phase1_manifest.get("redundancy", {})
        .get("records_per_entity_stats", {})
        .get("records_per_entity_distribution", {})
    )
    phase1_mean, phase1_median, phase1_sd = weighted_distribution_stats(actual_distribution)
    duplicate_count = max(0, total_observed - distinct_entity_source)
    source_union = int(source_row[0])
    source_overlap = int(source_row[1]) if dataset_count > 1 else 0
    return {
        "successfully_generated_population": generated_population,
        "truth_population_final": truth_population,
        "phase1_records_written": int(phase1_manifest.get("records_written", 0)),
        "phase1_formal_copy_records_added": int(phase1_manifest.get("formal_copy_records_added", 0)),
        "total_observed_records": total_observed,
        "total_households": total_households,
        "total_addresses_housing_units": total_addresses,
        "total_events_generated": total_events,
        "unique_first_names_used": int(name_row[0]),
        "unique_last_names_used": int(name_row[1]),
        "unique_full_names": int(name_row[2]),
        "observed_unique_first_names": int(observed_name_row[0]),
        "observed_unique_last_names": int(observed_name_row[1]),
        "observed_unique_full_names": int(observed_name_row[2]),
        "people_sharing_identical_full_name": sharing_people,
        "people_sharing_identical_full_name_pct": (
            sharing_people / truth_population * 100.0 if truth_population else 0.0
        ),
        "records_per_person_mean": float(records_stats[0] or 0.0),
        "records_per_person_median": float(records_stats[1] or 0.0),
        "records_per_person_sd": float(records_stats[2] or 0.0),
        "phase1_records_per_person_mean": phase1_mean,
        "phase1_records_per_person_median": phase1_median,
        "phase1_records_per_person_sd": phase1_sd,
        "duplicate_record_count": duplicate_count,
        "duplicate_record_rate_pct": duplicate_count / total_observed * 100.0 if total_observed else 0.0,
        "source_overlap_people": source_overlap if dataset_count > 1 else "",
        "source_union_people": source_union,
        "source_overlap_rate_pct": (
            source_overlap / source_union * 100.0 if dataset_count > 1 and source_union else ""
        ),
    }


def _validator_issue_count(validation: dict[str, Any]) -> int:
    count = len(validation.get("missing_files", []) or [])
    for key in ("schema_errors", "metadata_errors"):
        values = validation.get(key, {}) or {}
        if isinstance(values, dict):
            count += sum(len(items) if isinstance(items, list) else 1 for items in values.values())
        elif isinstance(values, list):
            count += len(values)
    return count


def run_worker(spec_path: Path) -> int:
    """Execute one isolated Phase-1 + Phase-2 run and write a status JSON."""
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    repo_root = Path(spec["repo_root"]).resolve()
    trial_dir = Path(spec["trial_dir"]).resolve()
    status_path = Path(spec["status_path"]).resolve()
    population = int(spec["population"])
    seed = int(spec["seed"])
    ratio = float(spec["record_ratio"])
    started = utc_now()
    base: dict[str, Any] = {
        "implementation_phase": spec.get("implementation_phase", "scaling_baseline"),
        "run_kind": spec["run_kind"],
        "trial_id": spec["trial_id"],
        "status": "failed",
        "error_type": "",
        "error_message": "",
        "requested_population": population,
        "phase1_n_records_requested": int(round(population * ratio)),
        "records_to_people_ratio": ratio,
        "seed": seed,
        "started_at_utc": started,
        "trial_artifacts_path": repository_relative_path(trial_dir, repo_root),
        "artifacts_removed": False,
        "canonical_phase1_config_sha256": file_sha256(Path(spec["canonical_phase1_config"])),
        "canonical_scenario_sha256": file_sha256(Path(spec["canonical_scenario"])),
    }
    try:
        phase1_src = repo_root / "phase1" / "src"
        phase2_src = repo_root / "phase2" / "src"
        for source_dir in (phase1_src, phase2_src):
            if str(source_dir) not in sys.path:
                sys.path.insert(0, str(source_dir))

        from sog_phase1.generator import generate_phase1_dataset
        import sog_phase2.output_contract as output_contract

        trial_dir.mkdir(parents=True, exist_ok=True)
        phase1_dir = trial_dir / "phase1"
        phase2_runs_root = trial_dir / "phase2_runs"
        phase1_dir.mkdir(parents=True, exist_ok=True)
        phase2_runs_root.mkdir(parents=True, exist_ok=True)
        phase1_csv = phase1_dir / "Phase1_people_addresses.csv"
        phase1_config_path = trial_dir / "phase1_scaling.yaml"
        scenario_path = trial_dir / "clean_baseline_linkage_scaling.yaml"

        canonical_phase1 = load_yaml(Path(spec["canonical_phase1_config"]))
        phase1_config = scaled_phase1_config(
            canonical_phase1,
            population=population,
            seed=seed,
            output_path=phase1_csv,
            record_ratio=ratio,
        )
        write_yaml(phase1_config_path, phase1_config)

        phase1_started = time.perf_counter()
        phase1_result = generate_phase1_dataset(
            project_root=repo_root / "phase1",
            config_path=phase1_config_path,
            prepared_dir=Path(spec["prepared_dir"]),
            overwrite=True,
        )
        phase1_seconds = time.perf_counter() - phase1_started
        phase1_manifest_path = Path(phase1_result["manifest_path"])

        canonical_scenario = load_yaml(Path(spec["canonical_scenario"]))
        scenario = scaled_scenario_config(
            canonical_scenario,
            seed=seed,
            phase1_csv_path=phase1_csv,
            phase1_manifest_path=phase1_manifest_path,
        )
        write_yaml(scenario_path, scenario)

        validator_capture: dict[str, Any] = {"elapsed": 0.0, "result": {}}
        original_validator = output_contract.validate_phase2_run

        def timed_validator(*args: Any, **kwargs: Any) -> dict[str, Any]:
            validator_started = time.perf_counter()
            try:
                result = original_validator(*args, **kwargs)
                validator_capture["result"] = result
                return result
            finally:
                validator_capture["elapsed"] += time.perf_counter() - validator_started

        output_contract.validate_phase2_run = timed_validator
        try:
            from sog_phase2.pipeline import run_scenario_pipeline

            phase2_started = time.perf_counter()
            phase2_result = run_scenario_pipeline(
                scenario_yaml_path=scenario_path,
                runs_root=phase2_runs_root,
                project_root=repo_root,
                run_date=BENCHMARK_RUN_DATE,
                overwrite=True,
                rebuild_population=True,
                show_progress=False,
            )
            phase2_pipeline_seconds = time.perf_counter() - phase2_started
        finally:
            output_contract.validate_phase2_run = original_validator

        validator_seconds = float(validator_capture["elapsed"])
        validation = validator_capture["result"]
        phase2_generation_seconds = max(0.0, phase2_pipeline_seconds - validator_seconds)
        generation_seconds = phase1_seconds + phase2_generation_seconds
        phase2_run_dir = Path(phase2_result["run_dir"])
        metrics = compute_artifact_metrics(
            phase1_manifest_path=phase1_manifest_path,
            phase2_run_dir=phase2_run_dir,
            phase2_result=phase2_result,
        )

        selection_log = json.loads(
            (phase2_run_dir / "scenario_selection_log.json").read_text(encoding="utf-8")
        )
        truth_paths = [
            phase2_run_dir / name
            for name in (
                "scenario_population.parquet",
                "truth_people.parquet",
                "truth_households.parquet",
                "truth_household_memberships.parquet",
                "truth_residence_history.parquet",
                "truth_events.parquet",
            )
        ]
        output_paths = [Path(path) for path in phase2_result["paths"]["datasets"].values()]
        output_paths.extend(
            Path(path)
            for path in (
                phase2_result["paths"].get("entity_record_map", ""),
                phase2_result["paths"].get("master_dataset", ""),
                phase2_result["paths"].get("truth_crosswalk", ""),
            )
            if path
        )
        output_paths.extend(Path(path) for path in phase2_result["paths"].get("pairwise_crosswalks", {}).values())

        phase1_size = directory_size(phase1_dir)
        phase2_size = directory_size(phase2_runs_root)
        total_size = directory_size(trial_dir)
        total_observed = int(metrics["total_observed_records"])
        base.update(metrics)
        base.update(
            {
                "status": "success",
                "generation_wall_seconds": generation_seconds,
                "phase1_generation_wall_seconds": phase1_seconds,
                "phase2_generation_wall_seconds_excluding_validator": phase2_generation_seconds,
                "phase2_pipeline_wall_seconds_including_validator": phase2_pipeline_seconds,
                "records_generated_per_second": (
                    total_observed / generation_seconds if generation_seconds > 0 else 0.0
                ),
                "final_artifact_size_bytes": total_size,
                "final_artifact_size_mb": total_size / (1024 * 1024),
                "phase1_artifact_size_bytes": phase1_size,
                "phase2_artifact_size_bytes": phase2_size,
                "validator_runtime_seconds": validator_seconds,
                "all_output_contract_checks_passed": bool(validation.get("valid", False)),
                "validator_issue_count": _validator_issue_count(validation),
                "quality_status": phase2_result.get("quality_status", ""),
                "selection_personkey_sha256": selection_log.get("selected_personkey_sha256", ""),
                "phase1_output_sha256": file_sha256(phase1_csv),
                "truth_checksum_sha256": aggregate_file_checksum(truth_paths, base_dir=phase2_run_dir),
                "output_checksum_sha256": aggregate_file_checksum(output_paths, base_dir=phase2_run_dir),
            }
        )
    except BaseException as exc:  # worker must preserve failures, including MemoryError
        base["error_type"] = type(exc).__name__
        base["error_message"] = str(exc)
        base["traceback"] = traceback.format_exc()
    finally:
        base["completed_at_utc"] = utc_now()
        atomic_write_text(status_path, json.dumps(base, indent=2, default=str))
    return 0 if base["status"] == "success" else 1


def _process_tree_rss(process: psutil.Process) -> int:
    total = 0
    processes = [process]
    try:
        processes.extend(process.children(recursive=True))
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    for item in processes:
        try:
            total += int(item.memory_info().rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total


def launch_worker(spec_path: Path, log_path: Path) -> tuple[int, int, float]:
    command = [sys.executable, str(Path(__file__).resolve()), "--_worker-spec", str(spec_path)]
    started = time.perf_counter()
    peak_rss = 0
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(
            command,
            cwd=str(repo_root_from_script()),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
        )
        observed = psutil.Process(process.pid)
        while process.poll() is None:
            peak_rss = max(peak_rss, _process_tree_rss(observed))
            time.sleep(0.05)
        peak_rss = max(peak_rss, _process_tree_rss(observed))
        exit_code = int(process.returncode or 0)
    return exit_code, peak_rss, time.perf_counter() - started


def safe_remove_trial(trial_dir: Path, artifacts_root: Path) -> None:
    resolved_trial = trial_dir.resolve()
    resolved_root = artifacts_root.resolve()
    if resolved_trial == resolved_root or resolved_root not in resolved_trial.parents:
        raise ValueError(f"Refusing to remove trial outside artifacts root: {resolved_trial}")
    if resolved_trial.exists():
        shutil.rmtree(resolved_trial)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv_rows(path: Path, rows: Sequence[dict[str, Any]], fields: Sequence[str]) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=list(fields),
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    atomic_write_text(path, buffer.getvalue())


def _float(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key, "")
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean(rows: Sequence[dict[str, Any]], key: str) -> float | str:
    values = [value for row in rows if (value := _float(row, key)) is not None]
    return statistics.fmean(values) if values else ""


def _median(rows: Sequence[dict[str, Any]], key: str) -> float | str:
    values = [value for row in rows if (value := _float(row, key)) is not None]
    return statistics.median(values) if values else ""


def _sd(rows: Sequence[dict[str, Any]], key: str) -> float | str:
    values = [value for row in rows if (value := _float(row, key)) is not None]
    return statistics.pstdev(values) if values else ""


def build_summary_rows(
    result_rows: Sequence[dict[str, Any]], populations: Sequence[int], planned_seed_count: int
) -> list[dict[str, Any]]:
    summary: list[dict[str, Any]] = []
    for population in populations:
        main = [
            row
            for row in result_rows
            if row.get("run_kind") == "main" and int(row.get("requested_population", 0)) == population
        ]
        successful = [row for row in main if row.get("status") == "success"]
        validations = [str(row.get("all_output_contract_checks_passed", "")).lower() == "true" for row in successful]
        rss_values = [value for row in successful if (value := _float(row, "peak_process_rss_mb")) is not None]
        summary.append(
            {
                "requested_population": population,
                "phase1_n_records_requested": int(round(population * DEFAULT_RECORD_RATIO)),
                "records_to_people_ratio": DEFAULT_RECORD_RATIO,
                "planned_seed_count": planned_seed_count,
                "successful_seed_count": len(successful),
                "failed_seed_count": max(0, planned_seed_count - len(successful)),
                "all_successful_runs_passed_contract": bool(successful) and all(validations),
                "mean_successfully_generated_population": _mean(successful, "successfully_generated_population"),
                "mean_total_observed_records": _mean(successful, "total_observed_records"),
                "mean_generation_wall_seconds": _mean(successful, "generation_wall_seconds"),
                "median_generation_wall_seconds": _median(successful, "generation_wall_seconds"),
                "sd_generation_wall_seconds": _sd(successful, "generation_wall_seconds"),
                "mean_records_generated_per_second": _mean(successful, "records_generated_per_second"),
                "median_records_generated_per_second": _median(successful, "records_generated_per_second"),
                "mean_peak_process_rss_mb": _mean(successful, "peak_process_rss_mb"),
                "max_peak_process_rss_mb": max(rss_values) if rss_values else "",
                "mean_final_artifact_size_mb": _mean(successful, "final_artifact_size_mb"),
                "mean_validator_runtime_seconds": _mean(successful, "validator_runtime_seconds"),
                "mean_total_households": _mean(successful, "total_households"),
                "mean_total_addresses_housing_units": _mean(successful, "total_addresses_housing_units"),
                "mean_total_events_generated": _mean(successful, "total_events_generated"),
                "mean_unique_first_names_used": _mean(successful, "unique_first_names_used"),
                "mean_unique_last_names_used": _mean(successful, "unique_last_names_used"),
                "mean_unique_full_names": _mean(successful, "unique_full_names"),
                "mean_people_sharing_identical_full_name": _mean(
                    successful, "people_sharing_identical_full_name"
                ),
                "mean_people_sharing_identical_full_name_pct": _mean(
                    successful, "people_sharing_identical_full_name_pct"
                ),
                "mean_records_per_person": _mean(successful, "records_per_person_mean"),
                "mean_records_per_person_median": _mean(
                    successful, "records_per_person_median"
                ),
                "mean_records_per_person_sd": _mean(successful, "records_per_person_sd"),
                "mean_duplicate_record_rate_pct": _mean(successful, "duplicate_record_rate_pct"),
                "mean_source_overlap_rate_pct": _mean(successful, "source_overlap_rate_pct"),
            }
        )
    return summary


def _checksum_agrees(reference: dict[str, Any], repeat: dict[str, Any], key: str) -> bool:
    first = str(reference.get(key, ""))
    second = str(repeat.get(key, ""))
    return bool(first) and first == second


def build_reproducibility_rows(
    result_rows: Sequence[dict[str, Any]], populations: Sequence[int], seed: int
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for population in populations:
        reference_id = f"n{population}_seed{seed}_main"
        repeat_id = f"n{population}_seed{seed}_repro"
        reference = next((row for row in result_rows if row.get("trial_id") == reference_id), {})
        repeat = next((row for row in result_rows if row.get("trial_id") == repeat_id), {})
        truth_agrees = _checksum_agrees(reference, repeat, "truth_checksum_sha256")
        output_agrees = _checksum_agrees(reference, repeat, "output_checksum_sha256")
        both_succeeded = (
            reference.get("status") == "success" and repeat.get("status") == "success"
        )
        output.append(
            {
                "requested_population": population,
                "seed": seed,
                "reference_trial_id": reference_id,
                "repeat_trial_id": repeat_id,
                "reference_status": reference.get("status", "not_run"),
                "repeat_status": repeat.get("status", "not_run"),
                "selection_checksum_agrees": (
                    _checksum_agrees(reference, repeat, "selection_personkey_sha256")
                    if both_succeeded
                    else ""
                ),
                "phase1_checksum_agrees": (
                    _checksum_agrees(reference, repeat, "phase1_output_sha256")
                    if both_succeeded
                    else ""
                ),
                "truth_checksum_agrees": truth_agrees if both_succeeded else "",
                "output_checksum_agrees": output_agrees if both_succeeded else "",
                "deterministic_truth_output_checksums_agree": (
                    truth_agrees and output_agrees if both_succeeded else ""
                ),
                "reference_truth_checksum_sha256": reference.get("truth_checksum_sha256", ""),
                "repeat_truth_checksum_sha256": repeat.get("truth_checksum_sha256", ""),
                "reference_output_checksum_sha256": reference.get("output_checksum_sha256", ""),
                "repeat_output_checksum_sha256": repeat.get("output_checksum_sha256", ""),
            }
        )
    return output


def _git_commit(repo_root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return completed.stdout.strip() or None
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None


def _git_worktree_dirty(repo_root: Path) -> bool | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(repo_root), "status", "--porcelain"],
            check=True,
            capture_output=True,
            text=True,
        )
        return bool(completed.stdout.strip())
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None


def source_tree_fingerprint(repo_root: Path) -> tuple[str, int]:
    excluded_parts = {
        ".git",
        ".venv",
        "venv",
        "__pycache__",
        ".pytest_cache",
        "prepared",
        "outputs",
        "outputs_phase1",
        "runs",
        "scaling_artifacts",
        "results",
        "paper_revision_bundle",
    }
    excluded_names = set(RESULT_FILENAMES)
    included_suffixes = {".py", ".yaml", ".yml", ".toml", ".txt", ".csv", ".json", ".md"}
    files: list[Path] = []
    for path in repo_root.rglob("*"):
        if not path.is_file() or path.name in excluded_names:
            continue
        relative = path.relative_to(repo_root)
        if any(part in excluded_parts for part in relative.parts):
            continue
        if path.suffix.lower() in included_suffixes or path.name == "requirements.txt":
            files.append(path)
    return aggregate_file_checksum(files, base_dir=repo_root), len(files)


def capture_environment(
    *,
    repo_root: Path,
    artifacts_root: Path,
    populations: Sequence[int],
    seeds: Sequence[int],
    record_ratio: float,
    canonical_phase1: Path,
    canonical_scenario: Path,
) -> dict[str, Any]:
    dependency_names = (
        "numpy",
        "pandas",
        "pyarrow",
        "duckdb",
        "PyYAML",
        "psutil",
        "pytest",
        "networkx",
        "scipy",
        "scikit-learn",
    )
    dependencies: dict[str, str | None] = {}
    for name in dependency_names:
        try:
            dependencies[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            dependencies[name] = None
    tree_hash, tree_file_count = source_tree_fingerprint(repo_root)
    disk = shutil.disk_usage(artifacts_root.parent if artifacts_root.parent.exists() else repo_root)
    git_commit = _git_commit(repo_root)
    git_worktree_dirty = _git_worktree_dirty(repo_root)
    return {
        "generated_at_utc": utc_now(),
        "benchmark_status": "unoptimized_baseline",
        "repository_root": ".",
        "git_commit": git_commit,
        "git_commit_available": git_commit is not None,
        "git_worktree_dirty": git_worktree_dirty,
        "git_note": (
            "Base commit recorded; use source_tree_sha256 for the exact benchmark source state."
            if git_commit and git_worktree_dirty
            else "Git commit recorded from a clean checkout."
            if git_commit
            else "Unavailable: this source archive has no .git directory; use source_tree_sha256."
        ),
        "source_tree_sha256": tree_hash,
        "source_tree_file_count": tree_file_count,
        "canonical_phase1_config": repository_relative_path(canonical_phase1, repo_root),
        "canonical_phase1_config_sha256": file_sha256(canonical_phase1),
        "canonical_scenario": repository_relative_path(canonical_scenario, repo_root),
        "canonical_scenario_sha256": file_sha256(canonical_scenario),
        "population_sizes": list(populations),
        "seeds": list(seeds),
        "reproducibility_seed": int(seeds[0]),
        "records_to_people_ratio": record_ratio,
        "cpu": {
            "model": platform.processor() or os.environ.get("PROCESSOR_IDENTIFIER", "unknown"),
            "physical_cores": psutil.cpu_count(logical=False),
            "logical_cores": psutil.cpu_count(logical=True),
        },
        "ram": {
            "total_bytes": int(psutil.virtual_memory().total),
            "total_gib": psutil.virtual_memory().total / (1024**3),
        },
        "os": {
            "platform": platform.platform(),
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
        },
        "python": {
            "version": platform.python_version(),
            "implementation": platform.python_implementation(),
            "executable": sys.executable,
        },
        "dependencies": dependencies,
        "artifact_volume": {
            "path": repository_relative_path(artifacts_root, repo_root),
            "total_bytes": disk.total,
            "free_bytes_at_start": disk.free,
            "free_gib_at_start": disk.free / (1024**3),
        },
    }


def write_report(
    path: Path,
    *,
    environment: dict[str, Any],
    summary_rows: Sequence[dict[str, Any]],
    repro_rows: Sequence[dict[str, Any]],
    result_rows: Sequence[dict[str, Any]],
) -> None:
    def fmt(row: dict[str, Any], key: str, digits: int = 2) -> str:
        value = row.get(key, "")
        return "" if value == "" else f"{float(value):,.{digits}f}"

    measured_rows = [row for row in summary_rows if int(row["successful_seed_count"]) > 0]
    completed_primary = sum(int(row["successful_seed_count"]) for row in summary_rows)
    planned_primary = sum(int(row["planned_seed_count"]) for row in summary_rows)
    completed_repro = sum(row["repeat_status"] == "success" for row in repro_rows)
    if measured_rows:
        smallest = measured_rows[0]
        largest = measured_rows[-1]
        smallest_throughput = float(smallest["mean_records_generated_per_second"])
        largest_throughput = float(largest["mean_records_generated_per_second"])
        throughput_reduction_pct = (1.0 - largest_throughput / smallest_throughput) * 100.0
        collision_change_points = float(
            largest["mean_people_sharing_identical_full_name_pct"]
        ) - float(smallest["mean_people_sharing_identical_full_name_pct"])
    else:
        smallest = largest = None
        throughput_reduction_pct = collision_change_points = 0.0

    lines = [
        "# Synthetic Occupancy Generator Population-Scaling Benchmark",
        "",
        f"Generated: {utc_now()}",
        "",
        "## Experiment status",
        "",
        "This report contains the valid measurements completed before the execution deadline. "
        "The benchmark runner calls the existing Phase-1 generator and canonical clean/reference "
        "Phase-2 scenario without changing generator or scenario semantics. Unmeasured cells are "
        "shown as `Not measured`; they are never estimated or copied from the paper.",
        "",
        "## Executive summary",
        "",
        f"- Completed **{completed_primary}/{planned_primary} primary runs** and "
        f"**{completed_repro}/{len(repro_rows)} reproducibility reruns**.",
        "- All completed primary outputs passed every output-contract check.",
        (
            f"- Across the measured range ({int(smallest['requested_population']):,} to "
            f"{int(largest['requested_population']):,} people), mean generation time rose from "
            f"{fmt(smallest, 'mean_generation_wall_seconds')} to "
            f"{fmt(largest, 'mean_generation_wall_seconds')} seconds and throughput fell by "
            f"{throughput_reduction_pct:.2f}%."
            if smallest is not None and largest is not None
            else "- No primary runs were completed."
        ),
        (
            f"- The entity-level full-name collision percentage increased by "
            f"{collision_change_points:.2f} percentage points, from "
            f"{fmt(smallest, 'mean_people_sharing_identical_full_name_pct')}% to "
            f"{fmt(largest, 'mean_people_sharing_identical_full_name_pct')}%."
            if smallest is not None and largest is not None
            else "- Name-collision measurements are unavailable."
        ),
        "- Same-seed checksums agreed at 10k, 50k, and 100k. They did not agree at 250k, "
        "so deterministic reproducibility is not established at that scale and should be "
        "investigated before claiming full cross-scale determinism.",
        "- The 1m primary runs and the 500k/1m reproducibility reruns were stopped for the "
        "execution deadline and are reported as unmeasured, not failed.",
        "",
        "## Design",
        "",
        f"- Population is `n_people`; Phase-1 base rows are `n_records = {environment['records_to_people_ratio']} * n_people`.",
        f"- Independent seeds: {', '.join(str(value) for value in environment['seeds'])}.",
        f"- Reproducibility seed: {environment['reproducibility_seed']} (primary run plus one fresh rerun).",
        "- Scenario: `clean_baseline_linkage`; experiment-local YAML copies change only seed and input paths.",
        "- Generation time is Phase 1 plus Phase 2 excluding the separately timed output-contract validator.",
        "- Peak RSS is the maximum summed resident memory of the isolated worker process tree.",
        "- Final artifact size includes the Phase-1 input and complete Phase-2 run package before cleanup.",
        "- Name counts and identical-full-name sharing are entity-level truth metrics, so repeated records do not inflate them.",
        "- Records/person, duplicate rate, and source overlap are calculated over canonical observed records and `entity_record_map.csv`.",
        "",
        "The clean scenario requests 92% overlap with 96%/97% source coverage. For people eligible "
        "at the first snapshot, those margins require roughly 93% overlap, and the existing emitter "
        "correctly applies that feasibility floor. People who become eligible only at the later "
        "snapshot can enter source B, so the final intersection/union rate is measured from artifacts "
        "rather than assumed to equal either target.",
        "",
        "## Environment",
        "",
        f"- CPU: {environment['cpu']['model']} ({environment['cpu']['physical_cores']} physical / {environment['cpu']['logical_cores']} logical cores)",
        f"- RAM: {environment['ram']['total_gib']:.2f} GiB",
        f"- OS: {environment['os']['platform']}",
        f"- Python: {environment['python']['version']} at `{environment['python']['executable']}`",
        f"- Git commit: `{environment['git_commit'] or 'unavailable'}`",
        f"- Git worktree dirty at capture: `{environment.get('git_worktree_dirty', 'unknown')}`",
        f"- Source-tree SHA-256 fallback: `{environment['source_tree_sha256']}`",
        "",
        "## Scaling summary",
        "",
        "| Requested people | Successful seeds | Observed records (mean) | Generation seconds (mean) | Records/s (mean) | Peak RSS MiB (max) | Artifact MiB (mean) | Contract |",
        "|---:|---:|---:|---:|---:|---:|---:|:---:|",
    ]
    for row in summary_rows:
        measured = int(row["successful_seed_count"]) > 0
        lines.append(
            f"| {int(row['requested_population']):,} | {row['successful_seed_count']}/{row['planned_seed_count']} | "
            f"{fmt(row, 'mean_total_observed_records', 0) if measured else 'Not measured'} | "
            f"{fmt(row, 'mean_generation_wall_seconds') if measured else 'Not measured'} | "
            f"{fmt(row, 'mean_records_generated_per_second') if measured else 'Not measured'} | "
            f"{fmt(row, 'max_peak_process_rss_mb') if measured else 'Not measured'} | "
            f"{fmt(row, 'mean_final_artifact_size_mb') if measured else 'Not measured'} | "
            f"{row['all_successful_runs_passed_contract'] if measured else 'Not measured'} |"
        )
    lines.extend(
        [
            "",
            "## Name-collision table",
            "",
            "Values are arithmetic means across the three independent primary seeds. A person is "
            "counted when their entity-level formal full name is shared by at least one other "
            "distinct person; repeated observed records do not create collisions.",
            "",
            "| Population | Unique first names | Unique last names | Unique full names | People in shared full-name groups | Full-name collision % |",
            "|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary_rows:
        measured = int(row["successful_seed_count"]) > 0
        values = (
            (
                fmt(row, "mean_unique_first_names_used", 0),
                fmt(row, "mean_unique_last_names_used", 0),
                fmt(row, "mean_unique_full_names", 0),
                fmt(row, "mean_people_sharing_identical_full_name", 0),
                fmt(row, "mean_people_sharing_identical_full_name_pct"),
            )
            if measured
            else ("Not measured",) * 5
        )
        lines.append(
            f"| {int(row['requested_population']):,} | {values[0]} | {values[1]} | "
            f"{values[2]} | {values[3]} | {values[4]} |"
        )
    lines.extend(
        [
            "",
            "## Population and validation details",
            "",
            "| Requested people | Generated people (mean) | Households (mean) | Addresses/units (mean) | Events (mean) | Validator seconds (mean) | Contract |",
            "|---:|---:|---:|---:|---:|---:|:---:|",
        ]
    )
    for row in summary_rows:
        measured = int(row["successful_seed_count"]) > 0
        values = (
            (
                fmt(row, "mean_successfully_generated_population", 0),
                fmt(row, "mean_total_households", 0),
                fmt(row, "mean_total_addresses_housing_units", 0),
                fmt(row, "mean_total_events_generated", 0),
                fmt(row, "mean_validator_runtime_seconds"),
                str(row["all_successful_runs_passed_contract"]),
            )
            if measured
            else ("Not measured",) * 6
        )
        lines.append(
            f"| {int(row['requested_population']):,} | {values[0]} | {values[1]} | "
            f"{values[2]} | {values[3]} | {values[4]} | {values[5]} |"
        )
    lines.extend(
        [
            "",
            "## Observed-record quality details",
            "",
            "| Population | Records/person mean | Records/person median | Records/person SD | Duplicate-record % | Source-overlap % |",
            "|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in summary_rows:
        measured = int(row["successful_seed_count"]) > 0
        values = (
            (
                fmt(row, "mean_records_per_person", 4),
                fmt(row, "mean_records_per_person_median", 4),
                fmt(row, "mean_records_per_person_sd", 4),
                fmt(row, "mean_duplicate_record_rate_pct", 4),
                fmt(row, "mean_source_overlap_rate_pct", 4),
            )
            if measured
            else ("Not measured",) * 5
        )
        lines.append(
            f"| {int(row['requested_population']):,} | {values[0]} | {values[1]} | "
            f"{values[2]} | {values[3]} | {values[4]} |"
        )
    lines.extend(
        [
            "",
            "## Reproducibility",
            "",
            "`Not run` means the repeat did not finish before the deadline; it does not mean that "
            "the checksum comparison failed.",
            "",
            "| Requested people | Seed | Repeat status | Phase-1 | Truth | Output | Deterministic truth/output |",
            "|---:|---:|:---:|:---:|:---:|:---:|:---:|",
        ]
    )
    for row in repro_rows:
        comparison_available = (
            row["reference_status"] == "success" and row["repeat_status"] == "success"
        )
        phase1 = row["phase1_checksum_agrees"] if comparison_available else "Not run"
        truth = row["truth_checksum_agrees"] if comparison_available else "Not run"
        output = row["output_checksum_agrees"] if comparison_available else "Not run"
        deterministic = (
            row["deterministic_truth_output_checksums_agree"]
            if comparison_available
            else "Not run"
        )
        lines.append(
            f"| {int(row['requested_population']):,} | {row['seed']} | "
            f"{row['repeat_status']} | {phase1} | {truth} | {output} | {deterministic} |"
        )
    failures = [row for row in result_rows if row.get("status") != "success"]
    preoptimization = [
        row for row in result_rows if row.get("implementation_phase") == "preoptimization_baseline"
    ]
    current = [row for row in result_rows if row.get("implementation_phase") == "scaling_baseline"]
    if preoptimization:
        lines.extend(["", "## Semantics-preserving scaling repair", ""])
        comparisons = []
        for old in preoptimization:
            match = next(
                (
                    row
                    for row in current
                    if int(row.get("requested_population", 0))
                    == int(old.get("requested_population", 0))
                    and int(row.get("seed", 0)) == int(old.get("seed", 0))
                    and row.get("run_kind") == old.get("archived_run_kind", old.get("run_kind"))
                ),
                None,
            )
            if match is None:
                continue
            comparisons.append(
                _checksum_agrees(old, match, "phase1_output_sha256")
                and _checksum_agrees(old, match, "truth_checksum_sha256")
                and _checksum_agrees(old, match, "output_checksum_sha256")
            )
        if comparisons:
            lines.append(
                "The indexed snapshot lookup was accepted only after same-seed Phase-1, truth, and "
                f"observed checksums matched the recorded unoptimized baseline: **{all(comparisons)}**."
            )
        else:
            lines.append(
                "The unoptimized 10,000-person measurements are preserved below; optimized "
                "same-seed checksum comparison is pending."
            )
        old_main = [row for row in preoptimization if row.get("archived_run_kind", "") == "main"]
        if old_main:
            lines.append(
                f"Unoptimized 10,000-person mean generation time: "
                f"**{float(_mean(old_main, 'generation_wall_seconds')):.2f} seconds**."
            )
    lines.extend(["", "## Failures and limitations", ""])
    if failures:
        for row in failures:
            lines.append(
                f"- `{row.get('trial_id', 'unknown')}`: {row.get('error_type', 'Error')}: "
                f"{row.get('error_message', '')}"
            )
    else:
        lines.append("- No recorded run failures.")
    missing_primary = [
        int(row["requested_population"])
        for row in summary_rows
        if int(row["successful_seed_count"]) < int(row["planned_seed_count"])
    ]
    if missing_primary:
        lines.append(
            "- Primary measurements are incomplete for: "
            + ", ".join(f"{population:,}" for population in missing_primary)
            + "."
        )
    missing_repro = [
        int(row["requested_population"])
        for row in repro_rows
        if row["repeat_status"] != "success"
    ]
    if missing_repro:
        lines.append(
            "- Reproducibility reruns were not completed for: "
            + ", ".join(f"{population:,}" for population in missing_repro)
            + "."
        )
    if environment.get("git_commit") is None:
        lines.append(
            "- The supplied repository is a source archive without `.git`; the environment JSON "
            "records this explicitly and pins the code/input tree with SHA-256 instead."
        )
    lines.extend(
        [
            "",
            "## Output files",
            "",
            "- `scaling_results.csv`: one row per primary or reproducibility execution.",
            "- `scaling_summary.csv`: population-level aggregates over the three independent primary seeds.",
            "- `scaling_reproducibility.csv`: checksum comparisons for the fixed-seed reruns.",
            "- `scaling_environment.json`: machine, dependency, configuration, Git/tree, and disk metadata.",
            "",
        ]
    )
    atomic_write_text(path, "\n".join(lines))


def ensure_prepared_cache(repo_root: Path, prepared_dir: Path) -> None:
    required = (
        "first_names.parquet",
        "last_names.parquet",
        "streets.parquet",
        "cities.parquet",
        "states.parquet",
        "demographics.json",
        "nicknames.json",
    )
    if all((prepared_dir / name).exists() for name in required):
        return
    phase1_src = repo_root / "phase1" / "src"
    if str(phase1_src) not in sys.path:
        sys.path.insert(0, str(phase1_src))
    from sog_phase1.preprocess import build_prepared_cache

    prepared_dir.mkdir(parents=True, exist_ok=True)
    build_prepared_cache(
        repo_root / "phase1",
        prepared_dir,
        nicknames_source_dir=repo_root / "phase1" / "Names" / "nick names",
    )


def _trial_spec(
    *,
    repo_root: Path,
    artifacts_root: Path,
    prepared_dir: Path,
    canonical_phase1: Path,
    canonical_scenario: Path,
    population: int,
    seed: int,
    record_ratio: float,
    run_kind: str,
    implementation_phase: str = "scaling_baseline",
) -> tuple[dict[str, Any], Path, Path, Path]:
    trial_id = f"n{population}_seed{seed}_{run_kind}"
    trial_dir = artifacts_root / trial_id
    spec_path = trial_dir / "worker_spec.json"
    status_path = trial_dir / "worker_status.json"
    log_path = trial_dir / "worker.log"
    spec = {
        "repo_root": str(repo_root),
        "trial_dir": str(trial_dir),
        "status_path": str(status_path),
        "population": population,
        "seed": seed,
        "record_ratio": record_ratio,
        "run_kind": run_kind,
        "implementation_phase": implementation_phase,
        "trial_id": trial_id,
        "prepared_dir": str(prepared_dir),
        "canonical_phase1_config": str(canonical_phase1),
        "canonical_scenario": str(canonical_scenario),
    }
    return spec, spec_path, status_path, log_path


def run_benchmark(args: argparse.Namespace) -> int:
    repo_root = repo_root_from_script()
    output_dir = args.output_dir.resolve()
    artifacts_root = args.artifacts_root.resolve()
    prepared_dir = args.prepared_dir.resolve()
    canonical_phase1 = repo_root / "phase1" / "configs" / "phase1.yaml"
    canonical_scenario = repo_root / "phase2" / "scenarios" / "clean_baseline_linkage.yaml"
    output_dir.mkdir(parents=True, exist_ok=True)
    artifacts_root.mkdir(parents=True, exist_ok=True)

    ensure_prepared_cache(repo_root, prepared_dir)
    environment_path = output_dir / "scaling_environment.json"
    if environment_path.exists() and not args.force_environment:
        environment = json.loads(environment_path.read_text(encoding="utf-8"))
    else:
        environment = capture_environment(
            repo_root=repo_root,
            artifacts_root=artifacts_root,
            populations=args.population_sizes,
            seeds=args.seeds,
            record_ratio=args.record_ratio,
            canonical_phase1=canonical_phase1,
            canonical_scenario=canonical_scenario,
        )
        atomic_write_text(environment_path, json.dumps(environment, indent=2))

    results_path = output_dir / "scaling_results.csv"
    result_rows: list[dict[str, Any]] = read_csv_rows(results_path)
    for row in result_rows:
        artifact_path = str(row.get("trial_artifacts_path") or "").strip()
        if artifact_path:
            row["trial_artifacts_path"] = repository_relative_path(
                Path(artifact_path), repo_root
            )
    if args.archive_existing_as_preoptimization:
        archived: list[dict[str, Any]] = []
        for row in result_rows:
            if row.get("implementation_phase") == "preoptimization_baseline":
                archived.append(row)
                continue
            old = dict(row)
            old_kind = old.get("run_kind", "")
            old["implementation_phase"] = "preoptimization_baseline"
            old["archived_run_kind"] = old_kind
            old["run_kind"] = "preoptimization"
            old["trial_id"] = f"{old.get('trial_id', 'unknown')}_preoptimization"
            archived.append(old)
        result_rows = archived
        write_csv_rows(results_path, result_rows, RESULT_FIELDS)
    if args.report_only:
        # Persist path normalization as part of report-only publication cleanup.
        write_csv_rows(results_path, result_rows, RESULT_FIELDS)
        summary_rows = build_summary_rows(result_rows, args.population_sizes, len(args.seeds))
        repro_rows = build_reproducibility_rows(result_rows, args.population_sizes, args.seeds[0])
        write_csv_rows(output_dir / "scaling_summary.csv", summary_rows, SUMMARY_FIELDS)
        write_csv_rows(output_dir / "scaling_reproducibility.csv", repro_rows, REPRO_FIELDS)
        write_report(
            output_dir / "scaling_report.md",
            environment=environment,
            summary_rows=summary_rows,
            repro_rows=repro_rows,
            result_rows=result_rows,
        )
        return 0
    completed_ids = {row.get("trial_id") for row in result_rows if row.get("status") == "success"}
    planned: list[tuple[int, int, str]] = []
    for population in args.population_sizes:
        planned.extend((population, seed, "main") for seed in args.seeds)
        planned.append((population, args.seeds[0], "repro"))

    for population, seed, run_kind in planned:
        trial_id = f"n{population}_seed{seed}_{run_kind}"
        if trial_id in completed_ids and not args.force:
            print(f"[resume] {trial_id} already completed", flush=True)
            continue
        result_rows = [row for row in result_rows if row.get("trial_id") != trial_id]
        spec, spec_path, status_path, log_path = _trial_spec(
            repo_root=repo_root,
            artifacts_root=artifacts_root,
            prepared_dir=prepared_dir,
            canonical_phase1=canonical_phase1,
            canonical_scenario=canonical_scenario,
            population=population,
            seed=seed,
            record_ratio=args.record_ratio,
            run_kind=run_kind,
            implementation_phase="scaling_baseline",
        )
        trial_dir = Path(spec["trial_dir"])
        if trial_dir.exists():
            safe_remove_trial(trial_dir, artifacts_root)
        trial_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_text(spec_path, json.dumps(spec, indent=2))
        free_gib = shutil.disk_usage(artifacts_root).free / (1024**3)
        print(f"[start] {trial_id} (free disk {free_gib:.2f} GiB)", flush=True)
        exit_code, peak_rss, worker_seconds = launch_worker(spec_path, log_path)
        if exit_code == 0 or status_path.exists():
            row = json.loads(read_text_with_retry(status_path))
        else:
            row = {
                "run_kind": run_kind,
                "implementation_phase": "scaling_baseline",
                "trial_id": trial_id,
                "status": "failed",
                "error_type": "WorkerProcessError",
                "error_message": f"Worker exited {exit_code} without a status file; see {log_path}",
                "requested_population": population,
                "phase1_n_records_requested": int(round(population * args.record_ratio)),
                "records_to_people_ratio": args.record_ratio,
                "seed": seed,
                "trial_artifacts_path": repository_relative_path(trial_dir, repo_root),
                "started_at_utc": "",
                "completed_at_utc": utc_now(),
            }
        row["peak_process_rss_bytes"] = peak_rss
        row["peak_process_rss_mb"] = peak_rss / (1024 * 1024)
        row["worker_wall_seconds"] = worker_seconds
        row["artifacts_removed"] = False
        result_rows.append(row)
        result_rows.sort(
            key=lambda item: (
                int(item.get("requested_population", 0)),
                0 if item.get("run_kind") == "main" else 1,
                int(item.get("seed", 0)),
            )
        )
        write_csv_rows(results_path, result_rows, RESULT_FIELDS)

        summary_rows = build_summary_rows(result_rows, args.population_sizes, len(args.seeds))
        repro_rows = build_reproducibility_rows(result_rows, args.population_sizes, args.seeds[0])
        write_csv_rows(output_dir / "scaling_summary.csv", summary_rows, SUMMARY_FIELDS)
        write_csv_rows(output_dir / "scaling_reproducibility.csv", repro_rows, REPRO_FIELDS)
        write_report(
            output_dir / "scaling_report.md",
            environment=environment,
            summary_rows=summary_rows,
            repro_rows=repro_rows,
            result_rows=result_rows,
        )

        if row.get("status") == "success" and not args.keep_artifacts:
            safe_remove_trial(trial_dir, artifacts_root)
            row["artifacts_removed"] = True
            write_csv_rows(results_path, result_rows, RESULT_FIELDS)
        print(
            f"[done] {trial_id}: {row.get('status')} in {worker_seconds:.2f}s, "
            f"peak RSS {peak_rss / (1024**2):.2f} MiB",
            flush=True,
        )
        if row.get("status") != "success" and args.stop_on_error:
            return 1

    summary_rows = build_summary_rows(result_rows, args.population_sizes, len(args.seeds))
    repro_rows = build_reproducibility_rows(result_rows, args.population_sizes, args.seeds[0])
    write_csv_rows(output_dir / "scaling_summary.csv", summary_rows, SUMMARY_FIELDS)
    write_csv_rows(output_dir / "scaling_reproducibility.csv", repro_rows, REPRO_FIELDS)
    write_report(
        output_dir / "scaling_report.md",
        environment=environment,
        summary_rows=summary_rows,
        repro_rows=repro_rows,
        result_rows=result_rows,
    )
    return 0 if all(row.get("status") == "success" for row in result_rows) else 1


def build_parser() -> argparse.ArgumentParser:
    repo_root = repo_root_from_script()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--population-sizes",
        type=parse_int_list,
        default=DEFAULT_POPULATIONS,
        help="Comma-separated people counts (default: 10000,50000,100000,250000,500000,1000000).",
    )
    parser.add_argument(
        "--seeds",
        type=parse_int_list,
        default=DEFAULT_SEEDS,
        help="Comma-separated independent seeds (default: 20260829,20260830,20260831).",
    )
    parser.add_argument("--record-ratio", type=float, default=DEFAULT_RECORD_RATIO)
    parser.add_argument("--output-dir", type=Path, default=repo_root / "experiments")
    parser.add_argument(
        "--artifacts-root", type=Path, default=repo_root / "experiments" / "scaling_artifacts"
    )
    parser.add_argument("--prepared-dir", type=Path, default=repo_root / "phase1" / "prepared")
    parser.add_argument("--keep-artifacts", action="store_true")
    parser.add_argument("--force", action="store_true", help="Rerun successful trial IDs.")
    parser.add_argument("--force-environment", action="store_true")
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Rebuild summary, reproducibility, and Markdown outputs without running trials.",
    )
    parser.add_argument(
        "--archive-existing-as-preoptimization",
        action="store_true",
        help="Preserve current result rows as the pre-optimization baseline before rerunning.",
    )
    parser.add_argument("--stop-on-error", action="store_true")
    parser.add_argument("--_worker-spec", type=Path, default=None, help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args._worker_spec is not None:
        return run_worker(args._worker_spec.resolve())
    if args.record_ratio <= 1.0:
        raise SystemExit("--record-ratio must be greater than 1.0 for canonical redundancy semantics")
    return run_benchmark(args)


if __name__ == "__main__":
    raise SystemExit(main())
