"""Derive E4 threshold, E5 clustering, and E6 false-pressure experiments."""

from __future__ import annotations

import json
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from evaluation.matchers import score_learned, score_splink
from evaluation.metrics import build_candidates, cluster_metrics, load_run, oracle_from_curve, pair_metrics, threshold_curve

SEEDS = tuple(range(20260720, 20260730))
CONDITIONS = ("clean", "high_noise", "low_overlap", "one_to_many")
MATCHERS = ("baseline", "splink", "learned")
E2 = REPO / "paper_experiments/results/E2_E6/E2"
OUT = REPO / "paper_experiments/results/E2_E6"


def e1_run(condition: str, seed: int) -> Path:
    return REPO / "phase2/runs" / f"2026-07-22_e1_{condition}_s{seed}_seed{seed}"


def _score_e1_one(target: tuple[str, int]) -> str:
    condition, seed = target
    destination = OUT / "E4/e1_per_run" / condition / f"seed_{seed}.json"
    scores_path = OUT / "E4/e1_scores" / condition / f"seed_{seed}.parquet"
    if destination.exists() and scores_path.exists():
        existing = json.loads(destination.read_text(encoding="utf-8"))
        if existing.get("run_id") == e1_run(condition, seed).name:
            return f"{condition}:{seed}:reused"
    run = load_run(e1_run(condition, seed))
    candidates, candidate_seconds = build_candidates(run)
    thresholds = json.loads((E2 / "models/thresholds.json").read_text(encoding="utf-8"))["thresholds"]
    learned = joblib.load(E2 / "models/learned.joblib")
    scores_by_matcher = {
        "baseline": candidates.baseline_score.to_numpy(dtype=float),
        "splink": score_splink(run, candidates, E2 / "models/splink.json"),
        "learned": score_learned(learned, candidates),
    }
    labels = candidates.is_match.to_numpy(dtype=bool)
    result: dict[str, object] = {
        "condition": condition, "seed": seed, "run_id": run.run_dir.name,
        "records": len(run.nodes), "true_pairs": len(run.true_pairs),
        "candidate_pairs": len(candidates),
        "candidate_recall": float(labels.sum() / len(run.true_pairs)),
        "candidate_seconds": candidate_seconds,
    }
    for matcher, scores in scores_by_matcher.items():
        fixed = pair_metrics(scores, labels, float(thresholds[matcher]), len(run.true_pairs))
        oracle = oracle_from_curve(threshold_curve(scores, labels, len(run.true_pairs)))
        edges = candidates.loc[scores >= float(thresholds[matcher]), ["node_l", "node_r"]]
        result[matcher] = {"threshold": thresholds[matcher], **fixed, **oracle, **cluster_metrics(run, edges.itertuples(index=False, name=None))}
        candidates[f"{matcher}_score"] = scores
    scores_path.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_parquet(scores_path, compression="zstd", index=False)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return f"{condition}:{seed}:scored"


def score_e1(workers: int = 3) -> None:
    targets = [(condition, seed) for seed in SEEDS for condition in CONDITIONS]
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_score_e1_one, target) for target in targets]
        for i, future in enumerate(as_completed(futures), 1):
            print(f"E1 scoring {i}/{len(targets)} {future.result()}", flush=True)


def flatten_e1() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for path in (OUT / "E4/e1_per_run").glob("*/seed_*.json"):
        doc = json.loads(path.read_text(encoding="utf-8"))
        common = {k: doc[k] for k in ("condition", "seed", "run_id", "records", "true_pairs", "candidate_pairs", "candidate_recall", "candidate_seconds")}
        for matcher in MATCHERS:
            rows.append({**common, "matcher": matcher, **doc[matcher]})
    frame = pd.DataFrame(rows).sort_values(["condition", "seed", "matcher"])
    if len(frame) != 120:
        raise RuntimeError(f"E4 expected 120 E1 matcher rows, found {len(frame)}")
    frame.to_csv(OUT / "E4/e1_e4_e5_per_run.csv", index=False)
    return frame


def derive_e4(e1: pd.DataFrame) -> None:
    e4 = OUT / "E4"
    e4.mkdir(parents=True, exist_ok=True)
    metrics = ["f1", "oracle_f1", "oracle_threshold", "precision", "recall", "candidate_recall"]
    summary = e1.groupby(["condition", "matcher"])[metrics].agg(["mean", "std"]).reset_index()
    summary.columns = ["_".join(x).rstrip("_") for x in summary.columns]
    summary["fixed_minus_oracle_mean"] = summary.f1_mean - summary.oracle_f1_mean
    summary.to_csv(e4 / "e4_e1_threshold_summary.csv", index=False)

    e2 = pd.read_csv(E2 / "e2_per_run_metrics.csv")
    e2_summary = e2.groupby(["scenario", "matcher"])[metrics].agg(["mean", "std"]).reset_index()
    e2_summary.columns = ["_".join(x).rstrip("_") for x in e2_summary.columns]
    e2_summary["fixed_minus_oracle_mean"] = e2_summary.f1_mean - e2_summary.oracle_f1_mean
    e2_summary.to_csv(e4 / "e4_e2_threshold_summary.csv", index=False)

    baseline = summary[summary.matcher == "baseline"].set_index("condition")
    clean_fixed = float(baseline.loc["clean", "f1_mean"])
    high_fixed = float(baseline.loc["high_noise", "f1_mean"])
    high_oracle = float(baseline.loc["high_noise", "oracle_f1_mean"])
    fixed_drop = clean_fixed - high_fixed
    recovered = high_oracle - high_fixed
    recovery_fraction = recovered / fixed_drop if fixed_drop else 0.0
    lines = [
        "# E4: Threshold and oracle analysis", "",
        "Oracle values are in-sample descriptive upper bounds, not deployable performance estimates.", "",
        "| Condition | Matcher | Fixed F1 | Oracle F1 | Oracle threshold | Fixed - oracle |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(f"| {row.condition} | {row.matcher} | {row.f1_mean:.4f} ± {row.f1_std:.4f} | {row.oracle_f1_mean:.4f} ± {row.oracle_f1_std:.4f} | {row.oracle_threshold_mean:.4f} | {row.fixed_minus_oracle_mean:.4f} |")
    lines += ["", f"For the baseline matcher, clean fixed F1 was {clean_fixed:.4f}; high-noise fixed F1 was {high_fixed:.4f}; and high-noise oracle F1 was {high_oracle:.4f}. Oracle calibration recovered {recovered:.4f}, or {100*recovery_fraction:.1f}% of the {fixed_drop:.4f} fixed-threshold drop.", "", "Full fourteen-scenario threshold results are in `e4_e2_threshold_summary.csv`."]
    (e4 / "E4_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    fig, axes = plt.subplots(1, 2, figsize=(12.2, 5.0))
    for condition in CONDITIONS:
        scores: list[np.ndarray] = []
        labels: list[np.ndarray] = []
        true_total = 0
        for seed in SEEDS:
            frame = pd.read_parquet(e4 / "e1_scores" / condition / f"seed_{seed}.parquet", columns=["baseline_score", "is_match"])
            scores.append(frame.baseline_score.to_numpy())
            labels.append(frame.is_match.to_numpy(dtype=bool))
            true_total += int(e1[(e1.condition == condition) & (e1.seed == seed)].true_pairs.iloc[0])
        curve = threshold_curve(np.concatenate(scores), np.concatenate(labels), true_total)
        curve.to_csv(e4 / f"pr_curve_baseline_{condition}.csv", index=False)
        for axis in axes:
            axis.plot(curve.recall, curve.precision, label=condition.replace("_", " "))
    axes[0].set(title="Full curves", xlim=(0, 1.01), ylim=(0, 1.01))
    axes[1].set(title="High-performance region", xlim=(0.90, 1.001), ylim=(0.90, 1.001))
    for axis in axes:
        axis.set_xlabel("Recall"); axis.set_ylabel("Precision"); axis.grid(alpha=.25)
    axes[0].legend(loc="lower left")
    fig.suptitle("Baseline matcher precision-recall curves"); fig.tight_layout()
    fig.savefig(e4 / "baseline_pr_curves.png", dpi=180); plt.close(fig)


def derive_e5(e1: pd.DataFrame) -> None:
    e5 = OUT / "E5"
    e5.mkdir(parents=True, exist_ok=True)
    metrics = ["f1", "closure_f1", "b3_precision", "b3_recall", "b3_f1"]
    summary = e1.groupby(["condition", "matcher"])[metrics].agg(["mean", "std"]).reset_index()
    summary.columns = ["_".join(x).rstrip("_") for x in summary.columns]
    summary.to_csv(e5 / "e5_e1_cluster_summary.csv", index=False)
    e2 = pd.read_csv(E2 / "e2_per_run_metrics.csv")
    e2_summary = e2.groupby(["scenario", "matcher"])[metrics].agg(["mean", "std"]).reset_index()
    e2_summary.columns = ["_".join(x).rstrip("_") for x in e2_summary.columns]
    e2_summary.to_csv(e5 / "e5_e2_cluster_summary.csv", index=False)
    baseline = summary[summary.matcher == "baseline"]
    lines = ["# E5: Cluster-level evaluation", "", "Truth clusters are `PersonKey` groups over all emitted records. Predicted clusters are connected components of frozen-threshold eligible edges; B³ includes singleton nodes.", "", "| Condition | Pairwise F1 | Closure F1 | B³ precision | B³ recall | B³ F1 |", "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for row in baseline.itertuples(index=False):
        lines.append(f"| {row.condition} | {row.f1_mean:.4f} | {row.closure_f1_mean:.4f} | {row.b3_precision_mean:.4f} | {row.b3_recall_mean:.4f} | {row.b3_f1_mean:.4f} |")
    focus = e2_summary[(e2_summary.matcher == "baseline") & e2_summary.scenario.isin(["high_duplication_dedup", "divorce_custody", "couple_merge", "family_birth"])]
    lines += ["", "## Canonical cluster-pressure scenarios", "", "| Scenario | Pairwise F1 | Closure F1 | B³ F1 |", "| --- | ---: | ---: | ---: |"]
    for row in focus.itertuples(index=False):
        lines.append(f"| {row.scenario} | {row.f1_mean:.4f} | {row.closure_f1_mean:.4f} | {row.b3_f1_mean:.4f} |")
    baseline_by_condition = baseline.set_index("condition")
    pair_delta = float(
        baseline_by_condition.loc["one_to_many", "f1_mean"]
        - baseline_by_condition.loc["clean", "f1_mean"]
    )
    b3_delta = float(
        baseline_by_condition.loc["one_to_many", "b3_f1_mean"]
        - baseline_by_condition.loc["clean", "b3_f1_mean"]
    )
    direction = "upward" if b3_delta > 0 else "downward" if b3_delta < 0 else "not at all"
    lines += [
        "",
        "## Acceptance interpretation",
        "",
        f"The one-to-many absolute B³ change ({abs(b3_delta):.6f}) is "
        f"{'larger' if abs(b3_delta) >= abs(pair_delta) else 'smaller'} than its pairwise "
        f"change ({abs(pair_delta):.6f}), and B³ moves {direction}. "
        "The metric implementation is valid, but the preregistered substantive "
        "cluster-pressure claim is **not supported** unless the regenerated effect is "
        "both directionally adverse and practically meaningful.",
    ]
    (e5 / "E5_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def derive_e6() -> None:
    e6 = OUT / "E6"; e6.mkdir(parents=True, exist_ok=True)
    scenarios = ("clean_baseline_linkage", "couple_merge", "roommates_split", "family_birth")
    thresholds = json.loads((E2 / "models/thresholds.json").read_text(encoding="utf-8"))["thresholds"]
    rows: list[dict[str, object]] = []
    for scenario in scenarios:
        for seed in SEEDS:
            frame = pd.read_parquet(E2 / "scores" / scenario / f"seed_{seed}.parquet")
            nonmatch = frame[~frame.is_match]
            co = nonmatch[nonmatch.co_resident_nonmatch]
            pool = nonmatch[~nonmatch.co_resident_nonmatch]
            # Keep the matched-size control where co-resident pairs exist; use
            # a 1,000-pair reference sample when the clean condition has none.
            sample_n = min(len(co) if len(co) else 1_000, len(pool))
            random = pool.sample(n=sample_n, random_state=seed, replace=False) if sample_n else pool.iloc[:0]
            truth = frame[frame.is_match]
            for matcher in MATCHERS:
                column = f"{matcher}_score"
                predicted_nonmatch = nonmatch[nonmatch[column] >= thresholds[matcher]]
                fp_co = int(predicted_nonmatch.co_resident_nonmatch.sum())
                rows.append({
                    "scenario": scenario, "seed": seed, "matcher": matcher,
                    "co_resident_nonmatch_count": len(co),
                    "co_resident_score_median": float(co[column].median()) if len(co) else np.nan,
                    "random_nonmatch_score_median": float(random[column].median()) if len(random) else np.nan,
                    "true_match_score_median": float(truth[column].median()) if len(truth) else np.nan,
                    "false_positives": len(predicted_nonmatch),
                    "false_positive_rate": len(predicted_nonmatch) / len(nonmatch) if len(nonmatch) else 0.0,
                    "co_resident_false_positives": fp_co,
                    "co_resident_fp_share": fp_co / len(predicted_nonmatch) if len(predicted_nonmatch) else 0.0,
                })
    per_run = pd.DataFrame(rows)
    per_run.to_csv(e6 / "e6_per_run.csv", index=False)
    metrics = ["co_resident_nonmatch_count", "co_resident_score_median", "random_nonmatch_score_median", "true_match_score_median", "false_positives", "false_positive_rate", "co_resident_false_positives", "co_resident_fp_share"]
    summary = per_run.groupby(["scenario", "matcher"])[metrics].agg(["mean", "std"]).reset_index()
    summary.columns = ["_".join(x).rstrip("_") for x in summary.columns]
    summary.to_csv(e6 / "e6_summary.csv", index=False)
    lines = ["# E6: Household false-match pressure", "", "**Calibration disclosure.** Splink and the learned matcher were calibrated once on the independent SOG clean calibration seed; their models and thresholds were then frozen for every scenario. No household-scenario retuning was performed. Splink's co-residence failure mode is interpreted under that fixed deployment choice, not as a universally calibrated comparison.", "", "Co-resident non-matches are distinct truth entities with the same nonempty normalized observed street/city/state/postal tuple.", "", "| Scenario | Matcher | Co-resident pairs | Median co-resident score | Median random score | Median true score | False positives | Co-resident FP share |", "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for row in summary.itertuples(index=False):
        lines.append(f"| {row.scenario} | {row.matcher} | {row.co_resident_nonmatch_count_mean:.1f} | {row.co_resident_score_median_mean:.6g} | {row.random_nonmatch_score_median_mean:.6g} | {row.true_match_score_median_mean:.6g} | {row.false_positives_mean:.1f} | {100*row.co_resident_fp_share_mean:.1f}% |")
    lines += ["", "## Acceptance interpretation", "", "The score-distribution and false-positive shift is clear for the deterministic baseline and present for Splink, especially in `couple_merge` and `family_birth`; the learned model largely rejects co-residence-only evidence. E6 therefore supports household false-match pressure as a **matcher-dependent** effect, not a universal error guarantee."]
    (e6 / "E6_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    score_e1()
    e1 = flatten_e1()
    derive_e4(e1); derive_e5(e1); derive_e6()
    print("E4-E6 results written", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
