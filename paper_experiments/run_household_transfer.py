"""Run the paper-revision household calibration-transfer analyses.

This script never modifies the checked-in E1-E11 result directories.  New
calibration runs, models, score audits, and resumable per-run results are
written below ``paper_revision_bundle/_provenance/household_transfer``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import yaml
from scipy import stats

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from evaluation.matchers import score_learned, score_splink, select_threshold, train_splink
from evaluation.metrics import build_candidates, load_run, oracle_from_curve, pair_metrics, threshold_curve

SEEDS = tuple(range(20260720, 20260730))
CALIBRATION_SEED = 20260719
EVALUATION_RUN_DATE = "2026-07-24"
SUPPLEMENT_RUN_DATE = "2026-07-23"
TRANSFER_SCENARIOS = ("couple_merge", "family_birth", "adoption_blended_family")
HOUSEHOLD_SCENARIOS = (
    "clean_baseline_linkage",
    "couple_merge",
    "family_birth",
    "roommates_split",
    "divorce_custody",
    "adoption_blended_family",
)
MATCHERS = ("baseline", "splink", "learned")
E2 = REPO / "paper_experiments" / "results" / "E2_E6" / "E2"
BUNDLE = REPO / "paper_revision_bundle"
PROVENANCE = BUNDLE / "_provenance" / "household_transfer"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        return None if not math.isfinite(float(value)) else float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(json_ready(value), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def phase1_paths(seed: int) -> tuple[Path, Path]:
    root = REPO / "paper_experiments" / "results" / "E1_multiseed" / "phase1_populations"
    return root / f"phase1_seed_{seed}.csv", root / f"phase1_seed_{seed}.manifest.json"


def calibration_scenario_id(scenario: str) -> str:
    return f"paper_revision_household_{scenario}_calibration_s{CALIBRATION_SEED}"


def calibration_config_path(scenario: str) -> Path:
    return PROVENANCE / "configs" / f"{calibration_scenario_id(scenario)}.yaml"


def calibration_run_path(scenario: str) -> Path:
    return PROVENANCE / "runs" / (
        f"{SUPPLEMENT_RUN_DATE}_{calibration_scenario_id(scenario)}_seed{CALIBRATION_SEED}"
    )


def evaluation_run_path(scenario: str, seed: int) -> Path:
    return REPO / "phase2" / "runs" / (
        f"{EVALUATION_RUN_DATE}_e2_{scenario}_s{seed}_seed{seed}"
    )


def write_calibration_configs() -> None:
    for scenario in TRANSFER_SCENARIOS:
        source = REPO / "phase2" / "scenarios" / f"{scenario}.yaml"
        data = yaml.safe_load(source.read_text(encoding="utf-8"))
        data["scenario_id"] = calibration_scenario_id(scenario)
        data["seed"] = CALIBRATION_SEED
        csv_path, manifest_path = phase1_paths(CALIBRATION_SEED)
        if not csv_path.exists() or not manifest_path.exists():
            raise FileNotFoundError(
                f"Missing independent Phase 1 population for seed {CALIBRATION_SEED}"
            )
        data["phase1"]["data_path"] = csv_path.relative_to(REPO).as_posix()
        data["phase1"]["manifest_path"] = manifest_path.relative_to(REPO).as_posix()
        data.setdefault("parameters", {})["initialize_households_from_public_targets"] = True
        path = calibration_config_path(scenario)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def _generate_calibration_run(scenario: str) -> dict[str, Any]:
    destination = calibration_run_path(scenario)
    config = calibration_config_path(scenario)
    command = [
        sys.executable,
        str(REPO / "phase2" / "scripts" / "run_phase2_pipeline.py"),
        "--scenario-yaml",
        str(config),
        "--runs-root",
        str(PROVENANCE / "runs"),
        "--run-date",
        SUPPLEMENT_RUN_DATE,
        "--rebuild-population",
        "--no-progress",
    ]
    started = time.perf_counter()
    def complete_and_valid() -> bool:
        manifest_path = destination / "manifest.json"
        quality_path = destination / "quality_report.json"
        if not manifest_path.exists() or not quality_path.exists():
            return False
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        quality = json.loads(quality_path.read_text(encoding="utf-8"))
        return bool(
            manifest.get("observed_outputs", {}).get("datasets")
            and quality.get("validation", {}).get("valid", quality.get("valid", True))
        )

    if complete_and_valid():
        return {
            "kind": "calibration_generation",
            "scenario": scenario,
            "command": subprocess.list2cmdline(command),
            "status": "reused_validated",
            "return_code": 0,
            "seconds": 0.0,
            "run_path": destination.relative_to(REPO).as_posix(),
        }
    proc = subprocess.run(command, cwd=REPO, text=True, capture_output=True)
    log_path = PROVENANCE / "logs" / f"generate_{scenario}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(
        proc.stdout + "\n--- STDERR ---\n" + proc.stderr,
        encoding="utf-8",
    )
    complete = complete_and_valid()
    result = {
        "kind": "calibration_generation",
        "scenario": scenario,
        "command": subprocess.list2cmdline(command),
        "status": (
            "passed"
            if proc.returncode == 0
            else "completed_validated_with_nonzero_exit"
            if complete
            else "failed"
        ),
        "return_code": int(proc.returncode),
        "seconds": time.perf_counter() - started,
        "run_path": destination.relative_to(REPO).as_posix(),
        "log_path": log_path.relative_to(REPO).as_posix(),
        "complete_validated_artifacts": complete,
    }
    if not complete:
        write_json(PROVENANCE / f"generation_failure_{scenario}.json", result)
        raise RuntimeError(f"Calibration generation failed for {scenario}; see {log_path}")
    return result


def generate_calibration_runs(workers: int) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, min(workers, 2))) as executor:
        futures = {
            executor.submit(_generate_calibration_run, scenario): scenario
            for scenario in TRANSFER_SCENARIOS
        }
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            print(
                f"calibration generation {len(results)}/{len(TRANSFER_SCENARIOS)} "
                f"{result['scenario']} {result['status']} {result['seconds']:.1f}s",
                flush=True,
            )
    return sorted(results, key=lambda row: str(row["scenario"]))


def score_digest(scores: np.ndarray, labels: np.ndarray) -> str:
    digest = hashlib.sha256()
    digest.update(np.asarray(scores, dtype="<f8").tobytes())
    digest.update(np.asarray(labels, dtype=np.uint8).tobytes())
    return digest.hexdigest()


def train_scenario_models() -> dict[str, dict[str, Any]]:
    audits: dict[str, dict[str, Any]] = {}
    for scenario in TRANSFER_SCENARIOS:
        run_path = calibration_run_path(scenario)
        config_path = calibration_config_path(scenario)
        model_path = PROVENANCE / "models" / f"splink_{scenario}.json"
        scores_path = PROVENANCE / "calibration_scores" / f"{scenario}.parquet"
        audit_path = PROVENANCE / "calibration_audits" / f"{scenario}.json"
        config_hash = sha256_file(config_path)
        manifest_hash = sha256_file(run_path / "manifest.json")
        if audit_path.exists() and model_path.exists() and scores_path.exists():
            audit = json.loads(audit_path.read_text(encoding="utf-8"))
            if (
                audit.get("config_sha256") == config_hash
                and audit.get("run_manifest_sha256") == manifest_hash
                and audit.get("model_sha256") == sha256_file(model_path)
            ):
                audits[scenario] = audit
                print(f"scenario calibration {scenario}: reused", flush=True)
                continue

        started = time.perf_counter()
        run = load_run(run_path)
        candidates, candidate_seconds = build_candidates(run)
        train_splink(run, model_path)
        scores = score_splink(run, candidates, model_path)
        labels = candidates["is_match"].to_numpy(dtype=bool)
        threshold = select_threshold(scores, labels, len(run.true_pairs))
        fixed = pair_metrics(scores, labels, threshold, len(run.true_pairs))
        oracle = oracle_from_curve(threshold_curve(scores, labels, len(run.true_pairs)))
        scored = candidates[
            ["node_l", "node_r", "is_match", "co_resident_nonmatch"]
        ].copy()
        scored["splink_score"] = scores
        scores_path.parent.mkdir(parents=True, exist_ok=True)
        scored.to_parquet(scores_path, index=False, compression="zstd")
        audit = {
            "scenario": scenario,
            "calibration_seed": CALIBRATION_SEED,
            "evaluation_seeds": list(SEEDS),
            "calibration_disjoint_from_evaluation": CALIBRATION_SEED not in SEEDS,
            "config_path": config_path.relative_to(REPO).as_posix(),
            "config_sha256": config_hash,
            "run_path": run_path.relative_to(REPO).as_posix(),
            "run_manifest_sha256": manifest_hash,
            "model_path": model_path.relative_to(REPO).as_posix(),
            "model_sha256": sha256_file(model_path),
            "calibration_scores_path": scores_path.relative_to(REPO).as_posix(),
            "calibration_scores_sha256": sha256_file(scores_path),
            "candidate_pairs": len(candidates),
            "positive_pairs": int(labels.sum()),
            "negative_pairs": int((~labels).sum()),
            "co_resident_distinct_person_negative_pairs": int(
                candidates["co_resident_nonmatch"].sum()
            ),
            "true_pairs": len(run.true_pairs),
            "candidate_recall": float(labels.sum() / len(run.true_pairs)),
            "candidate_seconds": candidate_seconds,
            "training_seed": CALIBRATION_SEED,
            "threshold_selection": (
                "Maximum F1 over every finite distinct observed Splink score; ties "
                "break by precision then higher threshold."
            ),
            "threshold": threshold,
            "calibration_metrics": fixed,
            "oracle_metrics": oracle,
            "score_label_sha256": score_digest(scores, labels),
            "seconds": time.perf_counter() - started,
        }
        write_json(audit_path, audit)
        audits[scenario] = audit
        print(
            f"scenario calibration {scenario}: trained threshold={threshold:.17g} "
            f"pairs={len(candidates)} {audit['seconds']:.1f}s",
            flush=True,
        )
    return audits


def diagnostic_metrics(
    candidates: pd.DataFrame,
    scores: np.ndarray,
    *,
    threshold: float,
    seed: int,
) -> dict[str, Any]:
    labels = candidates["is_match"].to_numpy(dtype=bool)
    nonmatch_mask = ~labels
    co_mask = candidates["co_resident_nonmatch"].to_numpy(dtype=bool) & nonmatch_mask
    pool_indices = np.flatnonzero(nonmatch_mask & ~co_mask)
    co_indices = np.flatnonzero(co_mask)
    sample_n = min(len(co_indices) if len(co_indices) else 1_000, len(pool_indices))
    if sample_n:
        rng = np.random.default_rng(seed)
        random_indices = rng.choice(pool_indices, size=sample_n, replace=False)
    else:
        random_indices = np.asarray([], dtype=int)
    predicted_nonmatch = nonmatch_mask & (scores >= threshold)
    co_false_positives = int((predicted_nonmatch & co_mask).sum())
    false_positives = int(predicted_nonmatch.sum())
    return {
        "false_positive_count": false_positives,
        "co_resident_false_positive_count": co_false_positives,
        "co_resident_false_positive_share": (
            co_false_positives / false_positives if false_positives else 0.0
        ),
        "co_resident_nonmatch_count": int(co_mask.sum()),
        "median_true_match_score": (
            float(np.median(scores[labels])) if labels.any() else None
        ),
        "median_co_resident_nonmatch_score": (
            float(np.median(scores[co_mask])) if co_mask.any() else None
        ),
        "median_random_nonmatch_score": (
            float(np.median(scores[random_indices])) if len(random_indices) else None
        ),
        "random_nonmatch_sample_count": int(len(random_indices)),
    }


def _score_evaluation_run(target: tuple[str, int]) -> dict[str, Any]:
    scenario, seed = target
    cache_path = PROVENANCE / "per_run" / scenario / f"seed_{seed}.json"
    run_path = evaluation_run_path(scenario, seed)
    frozen_model = E2 / "models" / "splink.json"
    learned_model_path = E2 / "models" / "learned.joblib"
    scenario_model = PROVENANCE / "models" / f"splink_{scenario}.json"
    source_hashes = {
        "run_manifest_sha256": sha256_file(run_path / "manifest.json"),
        "frozen_splink_model_sha256": sha256_file(frozen_model),
        "learned_model_sha256": sha256_file(learned_model_path),
        "scenario_splink_model_sha256": (
            sha256_file(scenario_model) if scenario in TRANSFER_SCENARIOS else None
        ),
    }
    if cache_path.exists():
        cached = json.loads(cache_path.read_text(encoding="utf-8"))
        if cached.get("source_hashes") == source_hashes:
            cached["cache_status"] = "reused"
            return cached

    started = time.perf_counter()
    run = load_run(run_path)
    candidates, candidate_seconds = build_candidates(run)
    labels = candidates["is_match"].to_numpy(dtype=bool)
    true_count = len(run.true_pairs)
    candidate_recall = float(labels.sum() / true_count) if true_count else 0.0
    thresholds = json.loads(
        (E2 / "models" / "thresholds.json").read_text(encoding="utf-8")
    )["thresholds"]
    learned = joblib.load(learned_model_path)
    score_map = {
        "baseline": candidates["baseline_score"].to_numpy(dtype=float),
        "splink": score_splink(run, candidates, frozen_model),
        "learned": score_learned(learned, candidates),
    }
    matcher_results: dict[str, Any] = {}
    for matcher, scores in score_map.items():
        threshold = float(thresholds[matcher])
        matcher_results[matcher] = {
            "threshold": threshold,
            **pair_metrics(scores, labels, threshold, true_count),
            **oracle_from_curve(threshold_curve(scores, labels, true_count)),
            **diagnostic_metrics(
                candidates,
                scores,
                threshold=threshold,
                seed=seed,
            ),
            "score_label_sha256": score_digest(scores, labels),
        }

    scenario_specific = None
    if scenario in TRANSFER_SCENARIOS:
        audit = json.loads(
            (
                PROVENANCE / "calibration_audits" / f"{scenario}.json"
            ).read_text(encoding="utf-8")
        )
        threshold = float(audit["threshold"])
        scores = score_splink(run, candidates, scenario_model)
        scenario_specific = {
            "threshold": threshold,
            **pair_metrics(scores, labels, threshold, true_count),
            **oracle_from_curve(threshold_curve(scores, labels, true_count)),
            **diagnostic_metrics(
                candidates,
                scores,
                threshold=threshold,
                seed=seed,
            ),
            "score_label_sha256": score_digest(scores, labels),
        }

    result = {
        "scenario": scenario,
        "seed": seed,
        "run_path": run_path.relative_to(REPO).as_posix(),
        "records": len(run.nodes),
        "true_pairs": true_count,
        "candidate_pairs": len(candidates),
        "candidate_recall": candidate_recall,
        "candidate_seconds": candidate_seconds,
        "matchers": matcher_results,
        "scenario_specific_splink": scenario_specific,
        "source_hashes": source_hashes,
        "seconds": time.perf_counter() - started,
        "cache_status": "computed",
    }
    write_json(cache_path, result)
    return result


def score_evaluation_runs(workers: int) -> list[dict[str, Any]]:
    targets = [
        (scenario, seed)
        for scenario in HOUSEHOLD_SCENARIOS
        for seed in SEEDS
    ]
    rows: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(_score_evaluation_run, target): target for target in targets
        }
        for future in as_completed(futures):
            result = future.result()
            rows.append(result)
            scenario, seed = futures[future]
            print(
                f"household scoring {len(rows)}/{len(targets)} "
                f"{scenario} seed={seed} {result['cache_status']} "
                f"{result['seconds']:.1f}s",
                flush=True,
            )
    return sorted(rows, key=lambda row: (str(row["scenario"]), int(row["seed"])))


def mean_ci(values: pd.Series, confidence: float = 0.95) -> tuple[float, float, float, float]:
    data = values.dropna().astype(float).to_numpy()
    mean = float(np.mean(data))
    sd = float(np.std(data, ddof=1)) if len(data) > 1 else 0.0
    if len(data) <= 1:
        return mean, sd, mean, mean
    half = float(stats.t.ppf((1.0 + confidence) / 2.0, len(data) - 1) * sd / np.sqrt(len(data)))
    return mean, sd, mean - half, mean + half


def sign_test(values: pd.Series) -> tuple[int, int, float]:
    data = values.dropna().astype(float).to_numpy()
    nonzero = data[data != 0]
    positive = int((nonzero > 0).sum())
    pvalue = float(stats.binomtest(positive, len(nonzero), 0.5).pvalue) if len(nonzero) else 1.0
    return positive, int(len(nonzero)), pvalue


def paired_tests(values: pd.Series) -> dict[str, Any]:
    data = values.dropna().astype(float).to_numpy()
    t_result = stats.ttest_1samp(data, 0.0)
    if np.all(data == 0):
        wilcoxon_statistic, wilcoxon_p = 0.0, 1.0
    else:
        wilcoxon = stats.wilcoxon(data, alternative="two-sided", method="auto")
        wilcoxon_statistic, wilcoxon_p = float(wilcoxon.statistic), float(wilcoxon.pvalue)
    positive, nonzero, sign_p = sign_test(pd.Series(data))
    return {
        "paired_t_statistic": float(t_result.statistic),
        "paired_t_p_value": float(t_result.pvalue),
        "wilcoxon_statistic": wilcoxon_statistic,
        "wilcoxon_p_value": wilcoxon_p,
        "sign_positive_count": positive,
        "sign_nonzero_count": nonzero,
        "exact_sign_p_value": sign_p,
    }


def result_row(
    result: dict[str, Any],
    *,
    matcher: str,
    calibration_mode: str,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "row_type": "per_seed",
        "scenario": result["scenario"],
        "seed": result["seed"],
        "matcher": matcher,
        "calibration_mode": calibration_mode,
        "calibration_seed": CALIBRATION_SEED,
        "evaluation_seed_disjoint": int(result["seed"]) != CALIBRATION_SEED,
        "records": result["records"],
        "true_pairs": result["true_pairs"],
        "candidate_pairs": result["candidate_pairs"],
        "candidate_recall": result["candidate_recall"],
        "precision": metrics["precision"],
        "recall": metrics["recall"],
        "f1": metrics["f1"],
        "threshold": metrics["threshold"],
        "false_positive_count": metrics["false_positive_count"],
        "co_resident_false_positive_count": metrics[
            "co_resident_false_positive_count"
        ],
        "co_resident_false_positive_share": metrics[
            "co_resident_false_positive_share"
        ],
        "co_resident_nonmatch_count": metrics["co_resident_nonmatch_count"],
        "median_true_match_score": metrics["median_true_match_score"],
        "median_co_resident_nonmatch_score": metrics[
            "median_co_resident_nonmatch_score"
        ],
        "median_random_nonmatch_score": metrics[
            "median_random_nonmatch_score"
        ],
        "random_nonmatch_sample_count": metrics["random_nonmatch_sample_count"],
        "oracle_f1": metrics["oracle_f1"],
        "oracle_threshold": metrics["oracle_threshold"],
        "score_label_sha256": metrics["score_label_sha256"],
        "source_run_path": result["run_path"],
        "source_run_manifest_sha256": result["source_hashes"][
            "run_manifest_sha256"
        ],
    }


def add_arm_summaries(frame: pd.DataFrame) -> pd.DataFrame:
    metrics = (
        "precision",
        "recall",
        "f1",
        "threshold",
        "candidate_recall",
        "false_positive_count",
        "co_resident_false_positive_count",
        "co_resident_false_positive_share",
        "median_true_match_score",
        "median_co_resident_nonmatch_score",
        "median_random_nonmatch_score",
        "oracle_f1",
    )
    summaries: list[dict[str, Any]] = []
    for keys, group in frame.groupby(
        ["scenario", "matcher", "calibration_mode"], sort=False
    ):
        scenario, matcher, calibration_mode = keys
        row: dict[str, Any] = {
            "row_type": "arm_summary",
            "scenario": scenario,
            "seed": "",
            "matcher": matcher,
            "calibration_mode": calibration_mode,
            "calibration_seed": CALIBRATION_SEED,
            "n_seeds": len(group),
        }
        for metric in metrics:
            mean, sd, low, high = mean_ci(group[metric])
            row[f"{metric}_mean"] = mean
            row[f"{metric}_sd"] = sd
            row[f"{metric}_ci95_low"] = low
            row[f"{metric}_ci95_high"] = high
        summaries.append(row)
    return pd.concat([frame, pd.DataFrame(summaries)], ignore_index=True, sort=False)


def build_outputs(results: list[dict[str, Any]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    full_rows: list[dict[str, Any]] = []
    transfer_rows: list[dict[str, Any]] = []
    for result in results:
        for matcher in MATCHERS:
            full_rows.append(
                result_row(
                    result,
                    matcher=matcher,
                    calibration_mode="clean_frozen",
                    metrics=result["matchers"][matcher],
                )
            )
        if result["scenario"] in TRANSFER_SCENARIOS:
            transfer_rows.append(
                result_row(
                    result,
                    matcher="splink",
                    calibration_mode="clean_frozen",
                    metrics=result["matchers"]["splink"],
                )
            )
            transfer_rows.append(
                result_row(
                    result,
                    matcher="splink",
                    calibration_mode="scenario_specific",
                    metrics=result["scenario_specific_splink"],
                )
            )

    full = add_arm_summaries(pd.DataFrame(full_rows))
    transfer_base = pd.DataFrame(transfer_rows)
    transfer = add_arm_summaries(transfer_base)

    paired_rows: list[dict[str, Any]] = []
    paired_metrics = (
        "precision",
        "recall",
        "f1",
        "false_positive_count",
        "co_resident_false_positive_count",
        "co_resident_false_positive_share",
        "median_true_match_score",
        "median_co_resident_nonmatch_score",
        "median_random_nonmatch_score",
        "oracle_f1",
    )
    for scenario in TRANSFER_SCENARIOS:
        current = transfer_base[transfer_base["scenario"] == scenario]
        row: dict[str, Any] = {
            "row_type": "paired_delta_summary",
            "scenario": scenario,
            "seed": "",
            "matcher": "splink",
            "calibration_mode": "scenario_specific_minus_clean_frozen",
            "calibration_seed": CALIBRATION_SEED,
            "n_seeds": len(SEEDS),
        }
        for metric in paired_metrics:
            pivot = current.pivot(
                index="seed", columns="calibration_mode", values=metric
            )
            delta = pivot["scenario_specific"] - pivot["clean_frozen"]
            mean, sd, low, high = mean_ci(delta)
            row[f"{metric}_mean_difference"] = mean
            row[f"{metric}_difference_sd"] = sd
            row[f"{metric}_difference_ci95_low"] = low
            row[f"{metric}_difference_ci95_high"] = high
            if metric == "f1":
                row.update(paired_tests(delta))
        paired_rows.append(row)
    transfer = pd.concat(
        [transfer, pd.DataFrame(paired_rows)], ignore_index=True, sort=False
    )

    full_path = BUNDLE / "household_full_matrix.csv"
    transfer_path = BUNDLE / "household_transfer_experiment.csv"
    BUNDLE.mkdir(parents=True, exist_ok=True)
    full.to_csv(full_path, index=False)
    transfer.to_csv(transfer_path, index=False)
    return transfer, full


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    BUNDLE.mkdir(parents=True, exist_ok=True)
    write_calibration_configs()
    generation = generate_calibration_runs(args.workers)
    audits = train_scenario_models()
    results = score_evaluation_runs(args.workers)
    transfer, full = build_outputs(results)
    run_audit = {
        "script": Path(__file__).relative_to(REPO).as_posix(),
        "command": subprocess.list2cmdline(sys.argv),
        "status": "passed",
        "calibration_seed": CALIBRATION_SEED,
        "evaluation_seeds": list(SEEDS),
        "calibration_disjoint_from_evaluation": CALIBRATION_SEED not in SEEDS,
        "generation_commands": generation,
        "scenario_calibrations": audits,
        "evaluation_run_count": len(results),
        "transfer_csv_rows": len(transfer),
        "full_matrix_csv_rows": len(full),
        "prior_result_files_overwritten": False,
    }
    write_json(PROVENANCE / "household_transfer_run.json", run_audit)
    print(
        f"wrote {BUNDLE / 'household_transfer_experiment.csv'} ({len(transfer)} rows)",
        flush=True,
    )
    print(
        f"wrote {BUNDLE / 'household_full_matrix.csv'} ({len(full)} rows)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
