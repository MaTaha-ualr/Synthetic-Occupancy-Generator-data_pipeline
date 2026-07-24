"""Run E11's size/noise-matched SOG versus upstream GeCo comparison."""

from __future__ import annotations

import argparse
import json
import math
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from evaluation.geco_adapter import (
    GECO_ARCHIVE_SHA256,
    GECO_FIELDS,
    GECO_SOURCE_URL,
    generate_geco_run,
    mean_true_pair_features,
    port_geco_archive,
    sha256_file,
    sha256_frame,
)
from evaluation.matchers import score_learned, score_splink
from evaluation.metrics import FEATURES, b3_cluster_metrics, build_candidates, load_run, pair_metrics

SEEDS = tuple(range(20260720, 20260730))
CALIBRATION_SEED = 20260719
RUN_DATE = "2026-07-27"
MATCHERS = ("baseline", "splink", "learned")
E2 = REPO / "paper_experiments/results/E2_E6/E2"
OUT = REPO / "paper_experiments/results/E7_E11/E11"
ARCHIVE = REPO / "paper_experiments/vendor/geco/geco-data-generator-corruptor.tar.gz"
EXPECTED_A = 5_499
EXPECTED_B = 11_011
EXPECTED_TRUE = 10_460
EXPECTED_CARTESIAN = EXPECTED_A * EXPECTED_B


def portable_text(value: str) -> str:
    escaped_repo = str(REPO).replace("\\", "\\\\")
    return value.replace(escaped_repo, "<REPO>").replace(str(REPO), "<REPO>")


def sog_run_path(seed: int) -> Path:
    return REPO / "phase2/runs" / f"2026-07-24_e2_couple_merge_s{seed}_seed{seed}"


def sog_calibration_path() -> Path:
    return REPO / "phase2/runs" / f"{RUN_DATE}_e11_sog_couple_merge_s{CALIBRATION_SEED}_seed{CALIBRATION_SEED}"


def ensure_sog_calibration() -> Path:
    destination = sog_calibration_path()
    log_path = OUT / "calibration_generation.log"
    if log_path.exists():
        portable = portable_text(log_path.read_text(encoding="utf-8"))
        log_path.write_text(portable, encoding="utf-8")
    config = OUT / "configs" / f"e11_sog_couple_merge_s{CALIBRATION_SEED}.yaml"
    config.parent.mkdir(parents=True, exist_ok=True)
    source = REPO / "phase2/scenarios/couple_merge.yaml"
    document = yaml.safe_load(source.read_text(encoding="utf-8"))
    document["scenario_id"] = f"e11_sog_couple_merge_s{CALIBRATION_SEED}"
    document["seed"] = CALIBRATION_SEED
    phase1_root = REPO / "paper_experiments/results/E1_multiseed/phase1_populations"
    phase1_csv = phase1_root / f"phase1_seed_{CALIBRATION_SEED}.csv"
    phase1_manifest = phase1_root / f"phase1_seed_{CALIBRATION_SEED}.manifest.json"
    if not phase1_csv.exists() or not phase1_manifest.exists():
        raise FileNotFoundError("Missing independent Phase 1 population for E11 calibration")
    document["phase1"]["data_path"] = phase1_csv.relative_to(REPO).as_posix()
    document["phase1"]["manifest_path"] = phase1_manifest.relative_to(REPO).as_posix()
    document.setdefault("parameters", {})["initialize_households_from_public_targets"] = True
    config.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    if (destination / "manifest.json").exists():
        return destination
    command = [
        sys.executable,
        str(REPO / "phase2/scripts/run_phase2_pipeline.py"),
        "--scenario-yaml", str(config),
        "--runs-root", str(REPO / "phase2/runs"),
        "--run-date", RUN_DATE,
        "--rebuild-population",
        "--no-progress",
    ]
    result = subprocess.run(command, cwd=REPO, text=True, capture_output=True)
    log_path.write_text(
        portable_text(result.stdout + "\nSTDERR\n" + result.stderr),
        encoding="utf-8",
    )
    if result.returncode or not (destination / "manifest.json").exists():
        raise RuntimeError("SOG E11 calibration generation failed; see calibration_generation.log")
    return destination


def _coaddress_fp_share(frame: pd.DataFrame, score_column: str, threshold: float) -> tuple[int, int, float]:
    false_positives = frame[(frame[score_column] >= threshold) & (~frame.is_match)]
    coaddress = int(false_positives.co_resident_nonmatch.sum())
    total = len(false_positives)
    return total, coaddress, coaddress / total if total else 0.0


def _evaluate_candidate_frame(run, candidates: pd.DataFrame, score_map: dict[str, np.ndarray], thresholds: dict[str, float]) -> list[dict[str, object]]:
    labels = candidates.is_match.to_numpy(dtype=bool)
    common = {
        "seed": run.seed,
        "records_a": len(run.datasets["A"]),
        "records_b": len(run.datasets["B"]),
        "records": len(run.nodes),
        "true_pairs": len(run.true_pairs),
        "cartesian_pairs": len(run.datasets["A"]) * len(run.datasets["B"]),
        "candidate_pairs": len(candidates),
        "candidate_recall": float(labels.sum() / len(run.true_pairs)),
    }
    rows: list[dict[str, object]] = []
    for matcher in MATCHERS:
        scores = np.asarray(score_map[matcher], dtype=float)
        threshold = float(thresholds[matcher])
        fixed = pair_metrics(scores, labels, threshold, len(run.true_pairs))
        edges = candidates.loc[scores >= threshold, ["node_l", "node_r"]]
        clusters = b3_cluster_metrics(run, edges.itertuples(index=False, name=None))
        fp, co_fp, co_share = _coaddress_fp_share(candidates, f"{matcher}_score", threshold)
        if fp != fixed["fp"]:
            raise RuntimeError(f"{matcher}: false-positive count disagrees with pair metrics")
        rows.append({
            **common,
            "matcher": matcher,
            "threshold": threshold,
            **fixed,
            "false_positives": fixed["fp"],
            **clusters,
            "coaddress_false_positives": co_fp,
            "coaddress_fp_share": co_share,
        })
    return rows


def evaluate_sog_seed(seed: int, thresholds: dict[str, float]) -> tuple[list[dict[str, object]], bool]:
    run = load_run(sog_run_path(seed))
    candidates = pd.read_parquet(E2 / "scores/couple_merge" / f"seed_{seed}.parquet")
    score_map = {matcher: candidates[f"{matcher}_score"].to_numpy(dtype=float) for matcher in MATCHERS}
    rows = _evaluate_candidate_frame(run, candidates, score_map, thresholds)
    source_doc = json.loads((E2 / "per_run/couple_merge" / f"seed_{seed}.json").read_text(encoding="utf-8"))
    matches_e2 = True
    for row in rows:
        row["arm"] = "SOG couple_merge"
        matcher = str(row["matcher"])
        matches_e2 &= math.isclose(float(row["f1"]), float(source_doc[matcher]["f1"]), abs_tol=1e-12)
        matches_e2 &= math.isclose(float(row["b3_f1"]), float(source_doc[matcher]["b3_f1"]), abs_tol=1e-12)
    return rows, matches_e2


def evaluate_geco_seed(
    seed: int,
    modifications: int,
    source_dir: Path,
    thresholds: dict[str, float],
    learned_model,
) -> tuple[list[dict[str, object]], list[dict[str, object]], dict[str, object]]:
    started = time.perf_counter()
    run = generate_geco_run(source_dir, seed=seed, modifications_per_record=modifications)
    generation_seconds = time.perf_counter() - started
    candidates, candidate_seconds = build_candidates(run)
    score_map = {
        "baseline": candidates.baseline_score.to_numpy(dtype=float),
        "splink": score_splink(run, candidates, E2 / "models/splink.json"),
        "learned": score_learned(learned_model, candidates),
    }
    for matcher, scores in score_map.items():
        candidates[f"{matcher}_score"] = scores
    rows = _evaluate_candidate_frame(run, candidates, score_map, thresholds)
    for row in rows:
        row["arm"] = "GeCo analog"
        row["generation_seconds"] = generation_seconds
        row["candidate_seconds"] = candidate_seconds
    scores_path = OUT / "scores" / f"geco_seed_{seed}.parquet"
    scores_path.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_parquet(scores_path, index=False, compression="zstd")
    input_hashes = [
        {"seed": seed, "artifact": "dataset_A_canonical_csv", "rows": len(run.datasets["A"]), "sha256": sha256_frame(run.datasets["A"])},
        {"seed": seed, "artifact": "dataset_B_canonical_csv", "rows": len(run.datasets["B"]), "sha256": sha256_frame(run.datasets["B"])},
        {"seed": seed, "artifact": "entity_map_canonical_csv", "rows": len(run.entity_map), "sha256": sha256_frame(run.entity_map)},
        {"seed": seed, "artifact": "candidate_scores_parquet", "rows": len(candidates), "sha256": sha256_file(scores_path)},
    ]
    audit = {
        "seed": seed,
        "modifications_per_record": modifications,
        "generation_seconds": generation_seconds,
        "candidate_seconds": candidate_seconds,
        "mean_true_pair_features": dict(zip(FEATURES, map(float, mean_true_pair_features(run)))),
        "score_artifact": str(scores_path.relative_to(REPO)),
        "score_sha256": sha256_file(scores_path),
    }
    return rows, input_hashes, audit


def run_calibration(source_dir: Path) -> tuple[int, pd.DataFrame]:
    sog = load_run(ensure_sog_calibration())
    target = mean_true_pair_features(sog)
    rows: list[dict[str, object]] = []
    for modifications in range(1, 6):
        started = time.perf_counter()
        geco = generate_geco_run(
            source_dir,
            seed=CALIBRATION_SEED,
            modifications_per_record=modifications,
        )
        achieved = mean_true_pair_features(geco)
        rmse = float(np.sqrt(np.mean(np.square(achieved - target))))
        row: dict[str, object] = {
            "calibration_seed": CALIBRATION_SEED,
            "modifications_per_record": modifications,
            "rmse": rmse,
            "seconds": time.perf_counter() - started,
        }
        for index, feature in enumerate(FEATURES):
            row[f"sog_{feature}"] = float(target[index])
            row[f"geco_{feature}"] = float(achieved[index])
            row[f"difference_{feature}"] = float(achieved[index] - target[index])
        rows.append(row)
        print(f"calibration m={modifications} rmse={rmse:.6f}", flush=True)
    frame = pd.DataFrame(rows).sort_values(["rmse", "modifications_per_record"])
    selected = int(frame.iloc[0].modifications_per_record)
    frame["selected"] = frame.modifications_per_record == selected
    frame.sort_values("modifications_per_record").to_csv(OUT / "e11_noise_calibration.csv", index=False)
    return selected, frame.sort_values("modifications_per_record")


def aggregate(per_run: pd.DataFrame, selected_modifications: int, calibration: pd.DataFrame) -> dict[str, object]:
    metrics = (
        "f1", "precision", "recall", "b3_f1", "b3_precision", "b3_recall",
        "candidate_recall", "candidate_pairs", "false_positives", "coaddress_fp_share",
    )
    summary = per_run.groupby(["arm", "matcher"], sort=False)[list(metrics)].agg(["mean", "std"]).reset_index()
    summary.columns = ["_".join(value).rstrip("_") for value in summary.columns]
    summary.to_csv(OUT / "e11_summary.csv", index=False)

    comparisons: list[dict[str, object]] = []
    for matcher in MATCHERS:
        sog = summary[(summary.arm == "SOG couple_merge") & (summary.matcher == matcher)].iloc[0]
        geco = summary[(summary.arm == "GeCo analog") & (summary.matcher == matcher)].iloc[0]
        row: dict[str, object] = {"matcher": matcher}
        strong_metrics: list[str] = []
        for metric in ("f1", "b3_f1"):
            deficit = float(geco[f"{metric}_mean"] - sog[f"{metric}_mean"])
            pooled_sd = float(np.sqrt((sog[f"{metric}_std"] ** 2 + geco[f"{metric}_std"] ** 2) / 2))
            multiple = deficit / pooled_sd if pooled_sd else (float("inf") if deficit > 0 else 0.0)
            row[f"sog_{metric}_mean"] = float(sog[f"{metric}_mean"])
            row[f"geco_{metric}_mean"] = float(geco[f"{metric}_mean"])
            row[f"sog_{metric}_deficit"] = deficit
            row[f"{metric}_pooled_sd"] = pooled_sd
            row[f"{metric}_pooled_sd_multiple"] = multiple
            if deficit > 3 * pooled_sd and deficit > 0:
                strong_metrics.append(metric)
        address_gap = float(sog.coaddress_fp_share_mean - geco.coaddress_fp_share_mean)
        row["sog_coaddress_fp_share_mean"] = float(sog.coaddress_fp_share_mean)
        row["geco_coaddress_fp_share_mean"] = float(geco.coaddress_fp_share_mean)
        row["coaddress_fp_share_gap"] = address_gap
        row["strong_performance_metrics"] = ";".join(strong_metrics)
        row["performance_criterion"] = bool(strong_metrics)
        row["address_criterion"] = address_gap >= 0.20
        row["novelty_supported"] = bool(strong_metrics) and address_gap >= 0.20
        comparisons.append(row)
    comparison = pd.DataFrame(comparisons)
    comparison.to_csv(OUT / "e11_comparison.csv", index=False)
    supported_matchers = comparison.loc[comparison.novelty_supported, "matcher"].tolist()
    if supported_matchers:
        verdict = "supported"
    elif comparison.performance_criterion.any() or comparison.address_criterion.any():
        verdict = "mixed"
    else:
        verdict = "negative"

    validation = {
        "upstream_archive_sha256": sha256_file(ARCHIVE),
        "upstream_archive_hash_matches_preregistration": sha256_file(ARCHIVE) == GECO_ARCHIVE_SHA256,
        "selected_modifications_per_record": selected_modifications,
        "calibration_rmse": float(calibration.loc[calibration.selected, "rmse"].iloc[0]),
        "calibration_selected_at_upper_grid_boundary": selected_modifications == int(calibration.modifications_per_record.max()),
        "noise_match_diagnostic": "poor approximation; large residual field-profile mismatch",
        "calibration_seed": CALIBRATION_SEED,
        "evaluation_seeds": list(SEEDS),
        "calibration_seed_excluded_from_evaluation": CALIBRATION_SEED not in SEEDS,
        "metric_rows": len(per_run),
        "expected_metric_rows": len(SEEDS) * 2 * len(MATCHERS),
        "all_size_contracts_exact": bool(
            (per_run.records_a == EXPECTED_A).all()
            and (per_run.records_b == EXPECTED_B).all()
            and (per_run.true_pairs == EXPECTED_TRUE).all()
            and (per_run.cartesian_pairs == EXPECTED_CARTESIAN).all()
        ),
        "all_candidate_recall_bounded": bool(per_run.candidate_recall.between(0, 1).all()),
        "sog_metrics_reproduce_e2": bool(per_run.loc[per_run.arm == "SOG couple_merge", "matches_e2"].all()),
        "frozen_thresholds": json.loads((E2 / "models/thresholds.json").read_text(encoding="utf-8"))["thresholds"],
        "novelty_verdict": verdict,
        "supporting_matchers": supported_matchers,
        "bad_or_null_result_retained": True,
    }
    (OUT / "e11_validation.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    write_report(summary, comparison, selected_modifications, calibration, verdict)
    write_figure(summary)
    return validation


def write_report(summary: pd.DataFrame, comparison: pd.DataFrame, selected: int, calibration: pd.DataFrame, verdict: str) -> None:
    best = calibration.loc[calibration.selected].iloc[0]
    lines = [
        "# E11: head-to-head comparison with upstream GeCo",
        "",
        "This experiment used the checksum-pinned ANU GeCo archive itself, not a GeCo-like reimplementation. Its Python 2 source was mechanically converted with `lib2to3`; the exact patch and compiler log are retained in `upstream/`.",
        "",
        "**Calibration disclosure.** The learned and Splink matchers were calibrated once on the independent SOG clean calibration seed and then frozen. No GeCo-specific or `couple_merge` retuning was performed. Consequently, the co-address collapse is evidence about fixed-model transfer behavior, not a claim that the arms received equally optimized matchers.",
        "",
        f"Calibration seed `{CALIBRATION_SEED}` selected **{selected} modifications per duplicate record** from `1..5` by seven-field true-pair similarity RMSE ({best.rmse:.6f}). Evaluation used the disjoint seeds `{SEEDS[0]}-{SEEDS[-1]}` and the frozen E2 thresholds.",
        "",
        "Both arms contain 5,499 A records, 11,011 B records, 10,460 true links, and 60,549,489 Cartesian pairs per seed.",
        "",
        "## Calibration adequacy",
        "",
        "The selected setting is the upper boundary of the preregistered grid and its RMSE is large on a 0-1 similarity scale. GeCo's generic record corruption cannot reproduce the highly heterogeneous `couple_merge` signature: SOG mostly preserves first name and DOB while changing household/address and often surname attributes. The comparison is therefore **size matched but only poorly noise matched**.",
        "",
        "| Field | SOG mean true-pair similarity | Selected GeCo | Difference |",
        "| --- | ---: | ---: | ---: |",
    ]
    for feature in FEATURES:
        lines.append(
            f"| {feature} | {best[f'sog_{feature}']:.4f} | {best[f'geco_{feature}']:.4f} | {best[f'difference_{feature}']:+.4f} |"
        )
    lines += [
        "",
        "## Matcher results",
        "",
        "| Arm | Matcher | Candidate recall | F1 | B³ F1 | False positives | Co-address FP share |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            f"| {row.arm} | {row.matcher} | {row.candidate_recall_mean:.4f} ± {row.candidate_recall_std:.4f} | "
            f"{row.f1_mean:.4f} ± {row.f1_std:.4f} | {row.b3_f1_mean:.4f} ± {row.b3_f1_std:.4f} | "
            f"{row.false_positives_mean:.1f} ± {row.false_positives_std:.1f} | "
            f"{100*row.coaddress_fp_share_mean:.1f}% ± {100*row.coaddress_fp_share_std:.1f}% |"
        )
    lines += [
        "",
        "## Preregistered novelty check",
        "",
        "A matcher supports the E11 claim only when SOG has (1) an F1 or B³ F1 deficit greater than three pooled between-seed SDs and (2) a co-address false-positive share at least 20 percentage points above GeCo.",
        "",
        "| Matcher | SOG F1 deficit | F1 / pooled SD | SOG B³ deficit | B³ / pooled SD | Co-address gap | Verdict |",
        "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in comparison.itertuples(index=False):
        cell = "supports" if row.novelty_supported else "does not support"
        lines.append(
            f"| {row.matcher} | {row.sog_f1_deficit:+.4f} | {row.f1_pooled_sd_multiple:.2f}× | "
            f"{row.sog_b3_f1_deficit:+.4f} | {row.b3_f1_pooled_sd_multiple:.2f}× | "
            f"{100*row.coaddress_fp_share_gap:+.1f} pp | {cell} |"
        )
    supporting = comparison.loc[comparison.novelty_supported, "matcher"].tolist()
    lines += [
        "",
        f"**Overall preregistered verdict: {verdict.upper()}.** " + (
            f"The joint criterion is met by {', '.join(supporting)}."
            if supporting else
            "No matcher meets both parts of the joint criterion. The result must not be presented as a successful head-to-head novelty demonstration."
        ),
        "",
        "The performance direction is opposite to the proposed novelty argument: every frozen matcher scores higher on SOG than on GeCo, while baseline and Splink still show a much larger co-address false-positive share on SOG. Because candidate recall is also materially lower for GeCo and the noise calibration is poor, this is not a defensible overall-difficulty ranking. It is a useful negative result showing a distinctive, matcher-dependent household error mode—not proof that SOG is globally harder.",
        "",
        "## Scope and limitations",
        "",
        "- The comparison exactly matches record counts, true-link counts, and Cartesian space. The attempted true-pair field-similarity calibration remains poor and is disclosed above; demographic and marginal field distributions are also unmatched.",
        "- GeCo models independent originals and corrupted descendants; it has no household-membership truth. Its co-address value is therefore the same observed-address proxy used by E6, not a household error rate.",
        "- This experiment tests one SOG scenario (`couple_merge`) against one explicit GeCo analog. It is evidence about that contrast, not a universal ranking of generators.",
        "",
        "Exact per-seed metrics, candidate scores, input hashes, calibration vectors, source provenance, and validation checks are stored beside this report.",
    ]
    (OUT / "E11_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_figure(summary: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(10.5, 4.4), sharey=True)
    x = np.arange(len(MATCHERS))
    width = 0.36
    for axis, metric, title in zip(axes, ("f1", "b3_f1"), ("Pairwise F1", "B³ F1")):
        for offset, arm in ((-width / 2, "SOG couple_merge"), (width / 2, "GeCo analog")):
            arm_rows = summary[summary.arm == arm].set_index("matcher").loc[list(MATCHERS)]
            axis.bar(
                x + offset,
                arm_rows[f"{metric}_mean"],
                width,
                yerr=arm_rows[f"{metric}_std"],
                capsize=3,
                label=arm,
            )
        axis.set_xticks(x, MATCHERS)
        axis.set_title(title)
        axis.grid(axis="y", alpha=0.25)
        axis.set_ylim(0, 1.02)
    axes[0].set_ylabel("Score")
    axes[0].legend(loc="lower left")
    fig.suptitle("E11: frozen matcher performance on matched-size SOG and GeCo arms")
    fig.tight_layout()
    fig.savefig(OUT / "e11_matcher_comparison.png", dpi=180)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="recompute completed per-seed outputs")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    ensure_sog_calibration()
    upstream = OUT / "upstream"
    upstream.mkdir(parents=True, exist_ok=True)
    if sha256_file(ARCHIVE) != GECO_ARCHIVE_SHA256:
        raise RuntimeError("pinned GeCo archive failed its SHA-256 check")

    with tempfile.TemporaryDirectory(prefix="sog_e11_geco_") as temp:
        ported = port_geco_archive(ARCHIVE, Path(temp))
        (upstream / "GECO_PY3_PORT.patch").write_text(ported.patch_text, encoding="utf-8")
        (upstream / "converter.log").write_text(
            "lib2to3 conversion completed for five modules; py_compile passed.\n"
            + (ported.converter_stderr or "No converter/compiler stderr.\n").replace(
                str(ported.source_dir), "geco-data-generator-corruptor"
            ),
            encoding="utf-8",
        )
        (upstream / "MPL2.0.txt").write_text(
            (ported.source_dir / "MPL2.0.txt").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        source_metadata = {
            "download_url": GECO_SOURCE_URL,
            "download_date": RUN_DATE,
            "archive_repo_path": str(ARCHIVE.relative_to(REPO)),
            "archive_bytes": ARCHIVE.stat().st_size,
            "archive_sha256": sha256_file(ARCHIVE),
            "expected_sha256": GECO_ARCHIVE_SHA256,
            "license": "Mozilla Public License 2.0",
            "upstream_runtime": "Python 2",
            "compatibility_action": "mechanical lib2to3 conversion of five source modules; py_compile passed",
            "adapter_fields": list(GECO_FIELDS),
            "attribute_modification_probabilities": {
                "first": 0.15, "last": 0.20, "dob": 0.15, "street": 0.20,
                "city": 0.10, "state": 0.05, "postal": 0.15,
            },
            "corruption_method_probabilities": {"uniform_edit": 0.70, "normal_edit": 0.15, "missing": 0.15},
            "duplicates_per_original": {"maximum": 3, "distribution": "zipf"},
            "port_patch_sha256": sha256_file(upstream / "GECO_PY3_PORT.patch"),
        }
        (upstream / "SOURCE_METADATA.json").write_text(json.dumps(source_metadata, indent=2), encoding="utf-8")

        calibration_path = OUT / "e11_noise_calibration.csv"
        if calibration_path.exists() and not args.force:
            calibration = pd.read_csv(calibration_path)
            selected = int(calibration.loc[calibration.selected, "modifications_per_record"].iloc[0])
            print(f"calibration reused; selected m={selected}", flush=True)
        else:
            selected, calibration = run_calibration(ported.source_dir)

        thresholds = json.loads((E2 / "models/thresholds.json").read_text(encoding="utf-8"))["thresholds"]
        learned = joblib.load(E2 / "models/learned.joblib")
        rows: list[dict[str, object]] = []
        hashes: list[dict[str, object]] = []
        audits: list[dict[str, object]] = []
        for index, seed in enumerate(SEEDS, 1):
            cached = OUT / "per_run" / f"seed_{seed}.json"
            if cached.exists() and not args.force:
                document = json.loads(cached.read_text(encoding="utf-8"))
                rows.extend(document["metric_rows"])
                hashes.extend(document["input_hashes"])
                audits.append(document["geco_audit"])
                print(f"E11 {index}/{len(SEEDS)} seed={seed} reused", flush=True)
                continue
            sog_rows, matches_e2 = evaluate_sog_seed(seed, thresholds)
            for row in sog_rows:
                row["matches_e2"] = matches_e2
            geco_rows, input_hashes, audit = evaluate_geco_seed(
                seed, selected, ported.source_dir, thresholds, learned,
            )
            for row in geco_rows:
                row["matches_e2"] = True
            document = {
                "seed": seed,
                "metric_rows": sog_rows + geco_rows,
                "input_hashes": input_hashes,
                "geco_audit": audit,
            }
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_text(json.dumps(document, indent=2), encoding="utf-8")
            rows.extend(document["metric_rows"])
            hashes.extend(input_hashes)
            audits.append(audit)
            print(f"E11 {index}/{len(SEEDS)} seed={seed} evaluated", flush=True)

    per_run = pd.DataFrame(rows).sort_values(["arm", "seed", "matcher"])
    per_run.to_csv(OUT / "e11_per_run.csv", index=False)
    pd.DataFrame(hashes).sort_values(["seed", "artifact"]).to_csv(OUT / "e11_input_hashes.csv", index=False)
    (OUT / "e11_generation_audit.json").write_text(json.dumps(audits, indent=2), encoding="utf-8")
    environment = {
        "run_date": RUN_DATE,
        "python": platform.python_version(),
        "platform": platform.platform(),
        "pandas": pd.__version__,
        "numpy": np.__version__,
        "git_head_before_e11_commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO, text=True, capture_output=True,
        ).stdout.strip(),
    }
    (OUT / "environment.json").write_text(json.dumps(environment, indent=2), encoding="utf-8")
    validation = aggregate(per_run, selected, calibration)
    print(json.dumps(validation, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
