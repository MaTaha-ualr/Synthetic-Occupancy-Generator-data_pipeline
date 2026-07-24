"""Run E8 repeated timing, hardware, and raw-workload measurements."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from evaluation.baseline_matcher import build_true_pairs, load_scenario_inputs, score_candidates

SEED = 20260720
CONDITIONS = ("clean", "high_noise", "low_overlap", "one_to_many")
RUN_DATE = "2026-07-22"
REPETITIONS = 5
OUTPUT = REPO_ROOT / "paper_experiments" / "results" / "E7_E11" / "E8"


def run_dir(condition: str) -> Path:
    exact = REPO_ROOT / "phase2" / "runs" / f"{RUN_DATE}_e1_{condition}_s{SEED}_seed{SEED}"
    if exact.exists():
        return exact
    choices = sorted((REPO_ROOT / "phase2" / "runs").glob(f"*_e1_{condition}_s{SEED}_seed{SEED}"))
    if not choices:
        raise FileNotFoundError(f"No E1 {condition} run for seed {SEED}")
    return choices[-1]


def cpu_model() -> str:
    if sys.platform == "win32":
        try:
            import winreg

            key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"HARDWARE\DESCRIPTION\System\CentralProcessor\0",
            )
            return str(winreg.QueryValueEx(key, "ProcessorNameString")[0]).strip()
        except OSError:
            pass
    return platform.processor() or "unavailable"


def windows_inventory() -> dict[str, object]:
    if sys.platform != "win32":
        return {}
    command = (
        "$cpu=Get-CimInstance Win32_Processor;"
        "$system=Get-CimInstance Win32_ComputerSystem;"
        "$os=Get-CimInstance Win32_OperatingSystem;"
        "[pscustomobject]@{"
        "PhysicalCores=($cpu|Measure-Object NumberOfCores -Sum).Sum;"
        "LogicalProcessors=($cpu|Measure-Object NumberOfLogicalProcessors -Sum).Sum;"
        "TotalPhysicalMemory=$system.TotalPhysicalMemory;"
        "OSCaption=$os.Caption;OSVersion=$os.Version;OSBuild=$os.BuildNumber"
        "}|ConvertTo-Json -Compress"
    )
    try:
        completed = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", command],
            text=True, capture_output=True, check=True, timeout=20,
        )
        return json.loads(completed.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return {}


def hardware() -> dict[str, object]:
    inventory = windows_inventory()
    try:
        import psutil

        physical = psutil.cpu_count(logical=False)
        logical = psutil.cpu_count(logical=True)
        ram_bytes = int(psutil.virtual_memory().total)
    except ImportError:
        physical = inventory.get("PhysicalCores")
        logical = inventory.get("LogicalProcessors", os.cpu_count())
        ram_bytes = inventory.get("TotalPhysicalMemory")
    os_description = platform.platform()
    if inventory.get("OSCaption"):
        os_description = f"{inventory['OSCaption']} {inventory.get('OSVersion', '')} build {inventory.get('OSBuild', '')}".strip()
    executable = Path(sys.executable).resolve()
    try:
        executable_label = executable.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        executable_label = str(executable)
    return {
        "cpu_model": cpu_model(),
        "physical_cores": physical,
        "logical_cores": logical,
        "ram_bytes": ram_bytes,
        "ram_gib": (ram_bytes / (1024**3)) if ram_bytes is not None else None,
        "os": os_description,
        "python": platform.python_version(),
        "python_executable": executable_label,
        "python_implementation": platform.python_implementation(),
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "timer": "time.perf_counter inside score_candidates",
        "timed_scope": "preparation + blocking + feature computation + scoring; excludes CSV load and threshold metrics",
        "warmups_per_condition": 1,
        "reported_repetitions_per_condition": REPETITIONS,
    }


def score_digest(scores: np.ndarray, labels: np.ndarray) -> str:
    digest = hashlib.sha256()
    digest.update(np.ascontiguousarray(scores).tobytes())
    digest.update(np.ascontiguousarray(labels).tobytes())
    return digest.hexdigest()


def timing_measurements() -> pd.DataFrame:
    cached: dict[str, tuple[pd.DataFrame, pd.DataFrame, set[tuple[str, str]]]] = {}
    for condition in CONDITIONS:
        a, b, entity_map, _ = load_scenario_inputs(run_dir(condition))
        true_pairs, _, _ = build_true_pairs(entity_map)
        cached[condition] = (a, b, true_pairs)

    for condition in CONDITIONS:
        a, b, true_pairs = cached[condition]
        score_candidates(a, b, true_pairs)

    rows: list[dict[str, object]] = []
    for repetition in range(1, REPETITIONS + 1):
        offset = (repetition - 1) % len(CONDITIONS)
        order = CONDITIONS[offset:] + CONDITIONS[:offset]
        for order_index, condition in enumerate(order):
            a, b, true_pairs = cached[condition]
            scores, labels, elapsed = score_candidates(a, b, true_pairs)
            rows.append({
                "seed": SEED,
                "condition": condition,
                "repetition": repetition,
                "round_order": order_index + 1,
                "runtime_s": elapsed,
                "candidate_pairs": len(scores),
                "candidate_true_pairs": int(labels.sum()),
                "score_label_sha256": score_digest(scores, labels),
            })
    return pd.DataFrame(rows).sort_values(["condition", "repetition"]).reset_index(drop=True)


def workload_summary() -> tuple[pd.DataFrame, pd.DataFrame]:
    source = pd.read_csv(REPO_ROOT / "paper_experiments" / "results" / "E1_multiseed" / "e1_per_run.csv")
    per_seed = source[source.threshold_variant.eq("fixed")].copy()
    per_seed["cartesian_space"] = per_seed["records_a"].astype(np.int64) * per_seed["records_b"].astype(np.int64)
    per_seed["candidate_reduction_ratio"] = 1.0 - per_seed["candidate_pairs"] / per_seed["cartesian_space"]
    keep = [
        "seed", "condition", "run_id", "records_a", "records_b", "true_links",
        "candidate_pairs", "cartesian_space", "candidate_recall", "candidate_reduction_ratio",
    ]
    per_seed = per_seed[keep].sort_values(["condition", "seed"])
    rows: list[dict[str, object]] = []
    for condition in CONDITIONS:
        group = per_seed[per_seed.condition.eq(condition)]
        row: dict[str, object] = {"condition": condition, "n_seeds": len(group)}
        for metric in ("records_a", "records_b", "true_links", "candidate_pairs", "cartesian_space", "candidate_recall", "candidate_reduction_ratio"):
            row[f"{metric}_mean"] = float(group[metric].mean())
            row[f"{metric}_sd"] = float(group[metric].std(ddof=1))
        rows.append(row)
    return per_seed, pd.DataFrame(rows)


def fmt(mean: float, sd: float, count: bool = False) -> str:
    return f"{mean:,.1f} ± {sd:,.1f}" if count else f"{mean:.4f} ± {sd:.4f}"


def write_report(timings: pd.DataFrame, workloads: pd.DataFrame, machine: dict[str, object]) -> None:
    runtime = timings.groupby("condition", sort=False).runtime_s.agg(["count", "mean", "std", "min", "max"]).reset_index()
    lines = [
        "# E8: measurement rigor", "", "## Hardware", "",
        "| Item | Value |", "| --- | --- |",
        f"| CPU | {machine['cpu_model']} |",
        f"| Cores | {machine['physical_cores']} physical / {machine['logical_cores']} logical |",
        f"| Installed RAM | {machine['ram_gib']:.2f} GiB |" if machine["ram_gib"] is not None else "| Installed RAM | unavailable |",
        f"| OS | {machine['os']} |",
        f"| Python | {machine['python']} ({machine['python_implementation']}) |",
        f"| pandas / NumPy | {machine['pandas']} / {machine['numpy']} |",
        "", "## Repeated matcher runtime", "",
        "One unreported warm-up and five timed repetitions were run for each condition on seed 20260720. CSV loading and threshold calculation are outside the timed region.", "",
        "| Condition | Repetitions | Runtime, s (mean ± SD) | Min | Max |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in runtime.itertuples(index=False):
        lines.append(f"| {row.condition.replace('_', ' ').title()} | {row.count} | {row.mean:.4f} ± {row.std:.4f} | {row.min:.4f} | {row.max:.4f} |")
    lines += [
        "", "## Raw workload over ten seeds", "",
        "| Condition | Candidate pairs | Cartesian space | Candidate recall | Reduction ratio |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for row in workloads.itertuples(index=False):
        lines.append(
            f"| {row.condition.replace('_', ' ').title()} | "
            f"{fmt(row.candidate_pairs_mean, row.candidate_pairs_sd, True)} | "
            f"{fmt(row.cartesian_space_mean, row.cartesian_space_sd, True)} | "
            f"{fmt(row.candidate_recall_mean, row.candidate_recall_sd)} | "
            f"{fmt(row.candidate_reduction_ratio_mean, row.candidate_reduction_ratio_sd)} |"
        )
    lines += [
        "", "The raw counts replace the previously rounded, visually identical reduction ratios. Exact per-seed counts and every repeated runtime are retained in CSV files.",
    ]
    (OUTPUT / "E8_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reuse-timings", action="store_true", help="Refresh provenance/report without repeating timed measurements")
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    machine = hardware()
    timings_path = OUTPUT / "e8_runtime_repetitions.csv"
    if args.reuse_timings:
        if not timings_path.exists():
            raise FileNotFoundError("--reuse-timings requested but e8_runtime_repetitions.csv does not exist")
        timings = pd.read_csv(timings_path)
    else:
        timings = timing_measurements()
    per_seed, workloads = workload_summary()
    timings.to_csv(timings_path, index=False)
    per_seed.to_csv(OUTPUT / "e8_workload_per_seed.csv", index=False)
    workloads.to_csv(OUTPUT / "e8_workload_summary.csv", index=False)
    runtime_summary = timings.groupby("condition", sort=False).runtime_s.agg(["count", "mean", "std", "min", "max"]).reset_index()
    runtime_summary.to_csv(OUTPUT / "e8_runtime_summary.csv", index=False)
    (OUTPUT / "e8_hardware.json").write_text(json.dumps(machine, indent=2) + "\n", encoding="utf-8")
    expected = per_seed[per_seed.seed.eq(SEED)].set_index("condition")["candidate_pairs"]
    observed = timings.groupby("condition")["candidate_pairs"].first()
    validation = {
        "conditions": list(CONDITIONS),
        "repetitions_per_condition": timings.groupby("condition").size().astype(int).to_dict(),
        "candidate_count_stable_within_condition": bool((timings.groupby("condition").candidate_pairs.nunique() == 1).all()),
        "score_label_digest_stable_within_condition": bool((timings.groupby("condition").score_label_sha256.nunique() == 1).all()),
        "fresh_candidate_counts_match_e1": bool(observed.astype(int).equals(expected.reindex(observed.index).astype(int))),
        "all_cartesian_spaces_exact": bool((per_seed.cartesian_space == per_seed.records_a * per_seed.records_b).all()),
        "runtime_rows": len(timings),
        "workload_rows": len(per_seed),
    }
    (OUTPUT / "e8_validation.json").write_text(json.dumps(validation, indent=2) + "\n", encoding="utf-8")
    write_report(timings, workloads, machine)
    print(json.dumps(validation, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
