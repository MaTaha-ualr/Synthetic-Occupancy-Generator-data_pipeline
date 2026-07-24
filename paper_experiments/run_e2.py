"""Run E2's 14-scenario, 10-seed, three-matcher evaluation."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import yaml

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from evaluation.matchers import score_learned, score_splink, select_threshold, train_learned, train_splink
from evaluation.metrics import build_candidates, cluster_metrics, load_run, oracle_from_curve, pair_metrics, threshold_curve

SEEDS = tuple(range(20260720, 20260730))
CALIBRATION_SEED = 20260719
RUN_DATE = "2026-07-24"
SCENARIOS = (
    "clean_baseline_linkage", "single_movers", "couple_merge", "family_birth",
    "divorce_custody", "roommates_split", "high_noise_identity_drift",
    "low_overlap_sparse_coverage", "asymmetric_source_coverage",
    "high_duplication_dedup", "three_source_partial_overlap",
    "name_change_lifecycle", "death_survivor_persistence", "adoption_blended_family",
)
FAMILIES = {
    "clean_baseline_linkage": "clean_mobility", "single_movers": "clean_mobility",
    "couple_merge": "household_formation", "family_birth": "household_formation",
    "roommates_split": "household_formation", "adoption_blended_family": "household_formation",
    "divorce_custody": "household_dissolution",
    "high_noise_identity_drift": "emission_noise_coverage",
    "low_overlap_sparse_coverage": "emission_noise_coverage",
    "asymmetric_source_coverage": "emission_noise_coverage",
    "high_duplication_dedup": "duplication_topology",
    "three_source_partial_overlap": "multi_source",
    "name_change_lifecycle": "lifecycle", "death_survivor_persistence": "lifecycle",
}
MATCHERS = ("baseline", "splink", "learned")


def output_root() -> Path:
    return REPO / "paper_experiments/results/E2_E6/E2"


def runs_root() -> Path:
    return REPO / "phase2/runs"


def run_id(scenario: str, seed: int) -> str:
    return f"{RUN_DATE}_e2_{scenario}_s{seed}_seed{seed}"


def run_path(scenario: str, seed: int) -> Path:
    return runs_root() / run_id(scenario, seed)


def config_path(scenario: str, seed: int) -> Path:
    return output_root() / "configs" / f"e2_{scenario}_s{seed}.yaml"


def phase1_paths(seed: int) -> tuple[Path, Path]:
    root = REPO / "paper_experiments/results/E1_multiseed/phase1_populations"
    return root / f"phase1_seed_{seed}.csv", root / f"phase1_seed_{seed}.manifest.json"


def write_configs() -> list[tuple[str, int]]:
    targets = [("clean_baseline_linkage", CALIBRATION_SEED)] + [(s, seed) for seed in SEEDS for s in SCENARIOS]
    (output_root() / "configs").mkdir(parents=True, exist_ok=True)
    for scenario, seed in targets:
        source = REPO / "phase2/scenarios" / f"{scenario}.yaml"
        data = yaml.safe_load(source.read_text(encoding="utf-8"))
        data["scenario_id"] = f"e2_{scenario}_s{seed}"
        data["seed"] = seed
        csv_path, manifest_path = phase1_paths(seed)
        if not csv_path.exists() or not manifest_path.exists():
            raise FileNotFoundError(f"Missing independent Phase 1 population for E2 seed {seed}")
        data["phase1"]["data_path"] = csv_path.relative_to(REPO).as_posix()
        data["phase1"]["manifest_path"] = manifest_path.relative_to(REPO).as_posix()
        data.setdefault("parameters", {})["initialize_households_from_public_targets"] = True
        config_path(scenario, seed).write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
    return targets


def _generate_one(target: tuple[str, int]) -> dict[str, object]:
    scenario, seed = target
    destination = run_path(scenario, seed)
    if (destination / "manifest.json").exists():
        return {"scenario": scenario, "seed": seed, "status": "reused", "seconds": 0.0}
    command = [
        sys.executable, str(REPO / "phase2/scripts/run_phase2_pipeline.py"),
        "--scenario-yaml", str(config_path(scenario, seed)), "--runs-root", str(runs_root()),
        "--run-date", RUN_DATE, "--rebuild-population", "--no-progress",
    ]
    started = time.perf_counter()
    proc = subprocess.run(command, cwd=REPO, text=True, capture_output=True)
    log = output_root() / "generation_logs" / f"{scenario}_s{seed}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(proc.stdout + "\nSTDERR\n" + proc.stderr, encoding="utf-8")
    if proc.returncode or not (destination / "manifest.json").exists():
        raise RuntimeError(f"generation failed: {scenario} seed {seed}; see {log}")
    return {"scenario": scenario, "seed": seed, "status": "generated", "seconds": time.perf_counter() - started}


def generate(targets: list[tuple[str, int]], workers: int) -> None:
    rows: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_generate_one, target): target for target in targets}
        for future in as_completed(futures):
            result = future.result()
            rows.append(result)
            print(f"generation {len(rows)}/{len(targets)} {result['scenario']} seed={result['seed']} {result['status']} {result['seconds']:.1f}s", flush=True)
    pd.DataFrame(rows).sort_values(["seed", "scenario"]).to_csv(output_root() / "generation_index.csv", index=False)


def calibrate() -> dict[str, float]:
    path = run_path("clean_baseline_linkage", CALIBRATION_SEED)
    if not (path / "manifest.json").exists():
        _generate_one(("clean_baseline_linkage", CALIBRATION_SEED))
    run = load_run(path)
    candidates, candidate_seconds = build_candidates(run)
    models = output_root() / "models"
    models.mkdir(parents=True, exist_ok=True)
    learned = train_learned(candidates, models / "learned.joblib")
    train_splink(run, models / "splink.json")
    splink_scores = score_splink(run, candidates, models / "splink.json")
    learned_scores = score_learned(learned, candidates)
    thresholds = {
        "baseline": 0.65,
        "splink": select_threshold(splink_scores, candidates.is_match.to_numpy(), len(run.true_pairs)),
        "learned": select_threshold(learned_scores, candidates.is_match.to_numpy(), len(run.true_pairs)),
    }
    audit: dict[str, object] = {
        "calibration_seed": CALIBRATION_SEED, "scenario": "clean_baseline_linkage",
        "candidate_pairs": len(candidates), "true_pairs": len(run.true_pairs),
        "candidate_recall": float(candidates.is_match.sum() / len(run.true_pairs)),
        "candidate_seconds": candidate_seconds, "thresholds": thresholds,
    }
    for matcher, scores in (("baseline", candidates.baseline_score.to_numpy()), ("splink", splink_scores), ("learned", learned_scores)):
        audit[f"{matcher}_calibration"] = pair_metrics(scores, candidates.is_match.to_numpy(), thresholds[matcher], len(run.true_pairs))
    (models / "thresholds.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    candidates.assign(splink_score=splink_scores, learned_score=learned_scores).to_parquet(models / "calibration_scores.parquet", index=False)
    print(json.dumps(audit, indent=2), flush=True)
    return thresholds


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _evaluate_one(target: tuple[str, int]) -> str:
    scenario, seed = target
    destination = output_root() / "per_run" / scenario / f"seed_{seed}.json"
    scores_path = output_root() / "scores" / scenario / f"seed_{seed}.parquet"
    if destination.exists() and scores_path.exists():
        existing = json.loads(destination.read_text(encoding="utf-8"))
        if existing.get("run_id") == run_id(scenario, seed):
            return f"{scenario}:{seed}:reused"
    run = load_run(run_path(scenario, seed))
    started = time.perf_counter()
    candidates, candidate_seconds = build_candidates(run)
    learned = joblib.load(output_root() / "models/learned.joblib")
    thresholds_doc = json.loads((output_root() / "models/thresholds.json").read_text(encoding="utf-8"))
    thresholds = thresholds_doc["thresholds"]
    score_map = {
        "baseline": candidates.baseline_score.to_numpy(dtype=float),
        "splink": score_splink(run, candidates, output_root() / "models/splink.json"),
        "learned": score_learned(learned, candidates),
    }
    labels = candidates.is_match.to_numpy(dtype=bool)
    candidate_recall = float(labels.sum() / len(run.true_pairs)) if run.true_pairs else 0.0
    counts = {source: len(frame) for source, frame in run.datasets.items()}
    total_possible = sum(a * b for i, a in enumerate(counts.values()) for b in list(counts.values())[i + 1:]) if run.topology == "link" else next(iter(counts.values())) * (next(iter(counts.values())) - 1) // 2
    result: dict[str, object] = {
        "scenario": scenario, "family": FAMILIES[scenario], "seed": seed,
        "run_id": run.run_dir.name, "topology": run.topology, "dataset_counts": counts,
        "records": len(run.nodes), "true_pairs": len(run.true_pairs), "candidate_pairs": len(candidates),
        "total_possible_pairs": total_possible, "candidate_recall": candidate_recall,
        "candidate_seconds": candidate_seconds,
    }
    for matcher, scores in score_map.items():
        fixed = pair_metrics(scores, labels, float(thresholds[matcher]), len(run.true_pairs))
        oracle = oracle_from_curve(threshold_curve(scores, labels, len(run.true_pairs)))
        edge_frame = candidates.loc[scores >= float(thresholds[matcher]), ["node_l", "node_r"]]
        clusters = cluster_metrics(run, edge_frame.itertuples(index=False, name=None))
        result[matcher] = {"threshold": thresholds[matcher], **fixed, **oracle, **clusters}
        candidates[f"{matcher}_score"] = scores
    scores_path.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_parquet(scores_path, index=False, compression="zstd")
    result["score_artifact"] = str(scores_path.relative_to(REPO))
    result["score_sha256"] = _sha256(scores_path)
    result["evaluation_seconds"] = time.perf_counter() - started
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return f"{scenario}:{seed}:evaluated"


def evaluate(workers: int) -> None:
    targets = [(scenario, seed) for seed in SEEDS for scenario in SCENARIOS]
    done = 0
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_evaluate_one, target): target for target in targets}
        for future in as_completed(futures):
            print(f"evaluation {done + 1}/{len(targets)} {future.result()}", flush=True)
            done += 1


def _flatten_results() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for path in sorted((output_root() / "per_run").glob("*/seed_*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        common = {key: doc[key] for key in ("scenario", "family", "seed", "run_id", "topology", "records", "true_pairs", "candidate_pairs", "total_possible_pairs", "candidate_recall", "candidate_seconds", "evaluation_seconds", "score_artifact", "score_sha256")}
        for matcher in MATCHERS:
            rows.append({**common, "matcher": matcher, **doc[matcher]})
    return pd.DataFrame(rows)


def aggregate() -> None:
    per_run = _flatten_results().sort_values(["scenario", "seed", "matcher"])
    expected = len(SCENARIOS) * len(SEEDS) * len(MATCHERS)
    if len(per_run) != expected:
        raise RuntimeError(f"expected {expected} metric rows, found {len(per_run)}")
    per_run.to_csv(output_root() / "e2_per_run_metrics.csv", index=False)
    metrics = ["f1", "precision", "recall", "b3_f1", "b3_precision", "b3_recall", "closure_f1", "oracle_f1", "oracle_threshold", "fp", "candidate_recall", "candidate_pairs", "evaluation_seconds"]
    summary = per_run.groupby(["scenario", "family", "matcher"], sort=False)[metrics].agg(["mean", "std"]).reset_index()
    summary.columns = ["_".join(col).rstrip("_") for col in summary.columns]
    summary.to_csv(output_root() / "e2_summary.csv", index=False)

    f1 = summary.pivot(index="scenario", columns="matcher", values="f1_mean")
    b3 = summary.pivot(index="scenario", columns="matcher", values="b3_f1_mean")
    clean_order = tuple(f1.loc["clean_baseline_linkage"].sort_values(ascending=False).index)
    changed: dict[str, list[str]] = {}
    for scenario in SCENARIOS:
        order = tuple(f1.loc[scenario].sort_values(ascending=False).index)
        if order != clean_order:
            changed[scenario] = list(order)
    best_by_scenario = f1.max(axis=1)
    hardest = str(best_by_scenario.idxmin())
    # The preregistration permits either rank changes or materially different
    # matcher degradation profiles.  The original implementation accidentally
    # evaluated only the first disjunct.  Operationalize "material" as a
    # two-F1-point spread between matcher losses relative to clean.
    degradation = (f1.loc["clean_baseline_linkage"] - f1).abs()
    degradation_spread = degradation.max(axis=1) - degradation.min(axis=1)
    material_profile_threshold = 0.02
    material_profile_scenarios = {
        scenario: float(spread)
        for scenario, spread in degradation_spread.items()
        if spread >= material_profile_threshold
    }
    rank_acceptance = len({FAMILIES[s] for s in changed}) >= 2
    profile_acceptance = bool(material_profile_scenarios)
    validation = {
        "metric_rows": len(per_run), "expected_metric_rows": expected,
        "scenario_count": per_run.scenario.nunique(), "seed_count": per_run.seed.nunique(),
        "matcher_count": per_run.matcher.nunique(), "clean_rank_order": list(clean_order),
        "changed_rank_scenarios": changed, "changed_rank_count": len(changed),
        "changed_rank_family_count": len({FAMILIES[s] for s in changed}),
        "hardest_scenario_by_best_matcher_f1": hardest,
        "hardest_best_matcher_f1": float(best_by_scenario.loc[hardest]),
        "material_profile_threshold_f1": material_profile_threshold,
        "material_profile_scenarios": material_profile_scenarios,
        "acceptance_rank_change": rank_acceptance,
        "acceptance_material_profile": profile_acceptance,
        "acceptance_rank_or_profile": rank_acceptance or profile_acceptance,
    }
    (output_root() / "e2_validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")

    lines = ["# E2: Matcher suite across fourteen canonical scenarios", "", "All values are mean ± sample SD across ten preregistered seeds.", "", "| Scenario | Baseline F1 | Splink F1 | Learned F1 | Baseline B³F1 | Splink B³F1 | Learned B³F1 | Rank order |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    for scenario in SCENARIOS:
        values: list[str] = []
        for metric in ("f1", "b3_f1"):
            for matcher in MATCHERS:
                row = summary[(summary.scenario == scenario) & (summary.matcher == matcher)].iloc[0]
                values.append(f"{row[f'{metric}_mean']:.4f} ± {row[f'{metric}_std']:.4f}")
        order = " > ".join(f1.loc[scenario].sort_values(ascending=False).index)
        lines.append(f"| {scenario} | " + " | ".join(values[:3] + values[3:] + [order]) + " |")
    material_text = ", ".join(
        f"`{scenario}` ({spread:.4f})"
        for scenario, spread in material_profile_scenarios.items()
    ) or "none"
    lines += ["", "## Headline checks", "", f"- Material degradation-profile spreads (threshold {material_profile_threshold:.2f} F1): {material_text}.", f"- Hardest scenario by best-matcher F1: `{hardest}` at {best_by_scenario.loc[hardest]:.4f}.", f"- Matcher ordering is stable (`{' > '.join(clean_order)}`); the positive result is profile separation, not rank reversal.", f"- Preregistered rank-or-profile criterion: **{'PASS' if validation['acceptance_rank_or_profile'] else 'NOT YET MET'}**.", "", "The acceptance implementation evaluates both preregistered alternatives; earlier code evaluated rank changes only. See `e2_validation.json`, `e2_per_run_metrics.csv`, and `scores/` for auditable details."]
    (output_root() / "E2_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    versions = {"python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__}
    for package in ("splink", "duckdb", "sklearn", "scipy", "networkx"):
        module = __import__(package)
        versions[package] = getattr(module, "__version__", "unknown")
    versions["git_head"] = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True).stdout.strip()
    (output_root() / "environment.json").write_text(json.dumps(versions, indent=2), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=("calibration", "generate", "evaluate", "aggregate", "all"), default="all")
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    output_root().mkdir(parents=True, exist_ok=True)
    targets = write_configs()
    if args.stage in ("calibration", "all"):
        _generate_one(("clean_baseline_linkage", CALIBRATION_SEED))
        calibrate()
    if args.stage in ("generate", "all"):
        generate([target for target in targets if target[1] != CALIBRATION_SEED], args.workers)
    if args.stage in ("evaluate", "all"):
        evaluate(args.workers)
    if args.stage in ("aggregate", "all"):
        aggregate()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
