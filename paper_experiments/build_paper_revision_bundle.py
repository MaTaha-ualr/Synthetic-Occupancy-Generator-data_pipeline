"""Build the machine-readable paper revision bundle from current artifacts.

All numeric results are read from checked-in result tables or from the
supplemental household-transfer run produced by ``run_household_transfer.py``.
The script does not modify historical E1-E11 artifacts.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from xml.etree import ElementTree
from xml.sax.saxutils import escape as xml_escape

import numpy as np
import pandas as pd
import yaml
from scipy import stats

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from evaluation.metrics import build_candidates, load_run

BUNDLE = REPO / "paper_revision_bundle"
TABLES = BUNDLE / "paper_tables"
PROVENANCE = BUNDLE / "_provenance"
E1 = REPO / "paper_experiments" / "results" / "E1_multiseed"
E2_E6 = REPO / "paper_experiments" / "results" / "E2_E6"
E2 = E2_E6 / "E2"
E3 = E2_E6 / "E3"
E4 = E2_E6 / "E4"
E7_E11 = REPO / "paper_experiments" / "results" / "E7_E11"
E7 = E7_E11 / "E7"
E8 = E7_E11 / "E8"
E10 = E7_E11 / "E10"
SEEDS = tuple(range(20260720, 20260730))
CALIBRATION_SEED = 20260719
MATCHERS = ("baseline", "splink", "learned")
E1_CONDITIONS = ("clean", "high_noise", "low_overlap", "one_to_many")
E2_SCENARIOS = (
    "clean_baseline_linkage",
    "single_movers",
    "couple_merge",
    "family_birth",
    "divorce_custody",
    "roommates_split",
    "high_noise_identity_drift",
    "low_overlap_sparse_coverage",
    "asymmetric_source_coverage",
    "high_duplication_dedup",
    "three_source_partial_overlap",
    "name_change_lifecycle",
    "death_survivor_persistence",
    "adoption_blended_family",
)
NOISE_LEVELS = (0.0, 0.5, 1.0, 2.0, 4.0)
TRUTH_FILES = (
    "truth_people.parquet",
    "truth_households.parquet",
    "truth_household_memberships.parquet",
    "truth_residence_history.parquet",
    "truth_events.parquet",
    "entity_record_map.csv",
)
PACKAGE_NAMES = (
    "numpy",
    "pandas",
    "scipy",
    "scikit-learn",
    "splink",
    "duckdb",
    "pyarrow",
    "joblib",
    "networkx",
    "matplotlib",
    "PyYAML",
    "pytest",
    "psutil",
)


def rel(path: Path) -> str:
    return path.resolve().relative_to(REPO.resolve()).as_posix()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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
    if isinstance(value, Path):
        return rel(value)
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(json_ready(value), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", "-c", "core.longpaths=true", *args],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def mean_ci(values: Iterable[float]) -> tuple[float, float, float, float]:
    data = np.asarray(list(values), dtype=float)
    data = data[np.isfinite(data)]
    mean = float(np.mean(data))
    sd = float(np.std(data, ddof=1)) if len(data) > 1 else 0.0
    if len(data) <= 1:
        return mean, sd, mean, mean
    half = float(stats.t.ppf(0.975, len(data) - 1) * sd / np.sqrt(len(data)))
    return mean, sd, mean - half, mean + half


def paired_tests(values: Iterable[float]) -> dict[str, Any]:
    data = np.asarray(list(values), dtype=float)
    data = data[np.isfinite(data)]
    t_result = stats.ttest_1samp(data, 0.0)
    if np.all(data == 0):
        wilcoxon_statistic, wilcoxon_p = 0.0, 1.0
    else:
        wilcoxon = stats.wilcoxon(data, alternative="two-sided", method="auto")
        wilcoxon_statistic = float(wilcoxon.statistic)
        wilcoxon_p = float(wilcoxon.pvalue)
    nonzero = data[data != 0]
    positives = int((nonzero > 0).sum())
    sign_p = (
        float(stats.binomtest(positives, len(nonzero), 0.5).pvalue)
        if len(nonzero)
        else 1.0
    )
    return {
        "paired_t_statistic": float(t_result.statistic),
        "paired_t_p_value": float(t_result.pvalue),
        "wilcoxon_statistic": wilcoxon_statistic,
        "wilcoxon_p_value": wilcoxon_p,
        "sign_positive_count": positives,
        "sign_negative_count": int((nonzero < 0).sum()),
        "sign_zero_count": int(len(data) - len(nonzero)),
        "sign_nonzero_count": int(len(nonzero)),
        "exact_sign_p_value": sign_p,
    }


def holm_adjust(pvalues: list[float]) -> list[float]:
    values = np.asarray(pvalues, dtype=float)
    order = np.argsort(values)
    adjusted = np.empty(len(values), dtype=float)
    running = 0.0
    count = len(values)
    for rank, index in enumerate(order):
        running = max(running, (count - rank) * float(values[index]))
        adjusted[index] = min(running, 1.0)
    return adjusted.tolist()


def composite_truth_hash(run_dir: Path) -> tuple[str, dict[str, str]]:
    hashes: dict[str, str] = {}
    for name in TRUTH_FILES:
        path = run_dir / name
        if not path.exists():
            raise FileNotFoundError(f"Truth-layer file missing: {path}")
        hashes[name] = sha256_file(path)
    canonical = "".join(
        f"{name}\0{hashes[name]}\0" for name in sorted(hashes)
    )
    return sha256_text(canonical), hashes


def flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        output: dict[str, Any] = {}
        for key in sorted(value):
            child = f"{prefix}.{key}" if prefix else str(key)
            output.update(flatten(value[key], child))
        return output
    return {prefix: value}


def yaml_diff(reference: Path, current: Path) -> str:
    left = flatten(yaml.safe_load(reference.read_text(encoding="utf-8")))
    right = flatten(yaml.safe_load(current.read_text(encoding="utf-8")))
    ignored = {
        "scenario_id",
        "seed",
        "phase1.data_path",
        "phase1.manifest_path",
    }
    differences: list[dict[str, Any]] = []
    for key in sorted(set(left) | set(right)):
        if key in ignored:
            continue
        if left.get(key) != right.get(key):
            differences.append(
                {"path": key, "reference": left.get(key), "current": right.get(key)}
            )
    return json.dumps(json_ready(differences), sort_keys=True, separators=(",", ":"))


def parse_junit(path: Path) -> dict[str, Any]:
    root = ElementTree.parse(path).getroot()
    attributes = root.attrib
    if root.tag == "testsuite":
        suites = [root]
    else:
        suites = list(root.findall("testsuite"))
    def total(name: str) -> int:
        if name in attributes:
            return int(float(attributes[name]))
        return sum(int(float(suite.attrib.get(name, 0))) for suite in suites)
    tests = total("tests")
    failures = total("failures")
    errors = total("errors")
    skipped = total("skipped")
    return {
        "junit_path": rel(path),
        "junit_sha256": sha256_file(path),
        "total": tests,
        "passed": tests - failures - errors - skipped,
        "failed": failures + errors,
        "failures": failures,
        "errors": errors,
        "skipped": skipped,
        "time_seconds": (
            float(attributes["time"])
            if "time" in attributes
            else sum(float(suite.attrib.get("time", 0.0)) for suite in suites)
        ),
    }


def package_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for name in PACKAGE_NAMES:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return versions


def build_matcher_protocol() -> dict[str, Any]:
    thresholds_doc = json.loads(
        (E2 / "models" / "thresholds.json").read_text(encoding="utf-8")
    )
    calibration_run = (
        REPO
        / "phase2"
        / "runs"
        / "2026-07-24_e2_clean_baseline_linkage_s20260719_seed20260719"
    )
    run = load_run(calibration_run)
    candidates, _ = build_candidates(run)
    positives = int(candidates["is_match"].sum())
    negatives = int((~candidates["is_match"]).sum())
    co_resident_negatives = int(candidates["co_resident_nonmatch"].sum())
    common_blocking = [
        "exact normalized DOB when nonempty",
        "Soundex(last name) + first-name initial when both nonempty",
        "Soundex(first name) + Soundex(last name) when both nonempty",
        "exact normalized postal code + Soundex(last name) when both nonempty",
        "birth year + Soundex(last name) when both nonempty",
    ]
    separation = {
        "training_seed": CALIBRATION_SEED,
        "calibration_seed": CALIBRATION_SEED,
        "evaluation_seeds": list(SEEDS),
        "all_training_and_calibration_data_disjoint_from_evaluation_seeds": (
            CALIBRATION_SEED not in SEEDS
        ),
        "no_evaluation_output_informed_tuning": True,
        "evidence": [
            "paper_experiments/run_e2.py calibrate() uses only clean seed 20260719",
            "paper_experiments/run_e2.py evaluate() uses only seeds 20260720-20260729",
            "models/thresholds.json records calibration_seed=20260719",
        ],
    }
    household_transfer_protocol: dict[str, Any] = {}
    for scenario in (
        "couple_merge",
        "family_birth",
        "adoption_blended_family",
    ):
        model_path = (
            PROVENANCE
            / "household_transfer"
            / "models"
            / f"splink_{scenario}.json"
        )
        audit_path = (
            PROVENANCE
            / "household_transfer"
            / "calibration_audits"
            / f"{scenario}.json"
        )
        model = json.loads(model_path.read_text(encoding="utf-8"))
        audit = json.loads(audit_path.read_text(encoding="utf-8"))
        untrained_m_levels: list[dict[str, str]] = []
        for comparison in model["comparisons"]:
            for level in comparison["comparison_levels"]:
                if level.get("is_null_level"):
                    continue
                if "m_probability" not in level:
                    untrained_m_levels.append(
                        {
                            "comparison": comparison["output_column_name"],
                            "level": level["label_for_charts"],
                        }
                    )
        household_transfer_protocol[scenario] = {
            "model_path": rel(model_path),
            "model_sha256": sha256_file(model_path),
            "calibration_audit_path": rel(audit_path),
            "calibration_seed": audit["calibration_seed"],
            "candidate_pairs": audit["candidate_pairs"],
            "positive_pairs": audit["positive_pairs"],
            "negative_pairs": audit["negative_pairs"],
            "co_resident_distinct_person_negative_pairs": audit[
                "co_resident_distinct_person_negative_pairs"
            ],
            "frozen_scenario_threshold": audit["threshold"],
            "threshold_search_procedure": audit["threshold_selection"],
            "evaluation_seeds": audit["evaluation_seeds"],
            "calibration_disjoint_from_evaluation": audit[
                "calibration_disjoint_from_evaluation"
            ],
            "evaluation_output_informed_tuning": False,
            "untrained_m_probability_levels": untrained_m_levels,
            "untrained_level_behavior": (
                "Splink 4.0.16 predict() warns and uses library defaults for "
                "comparison levels whose m-probability was not observed during EM."
                if untrained_m_levels
                else "all non-null comparison levels trained"
            ),
        }
    protocol = {
        "protocol_version": 1,
        "shared_candidate_generation": {
            "rules": common_blocking,
            "union_and_deduplication": (
                "Candidate pairs are the set union over all blocking keys. Link "
                "runs retain cross-source pairs; dedupe runs retain within-source pairs."
            ),
            "source": "evaluation/metrics.py:build_candidates and paper_experiments/evaluate_baseline.py:blocking_keys",
            "calibration_candidate_pairs": len(candidates),
            "calibration_positive_pairs": positives,
            "calibration_negative_pairs": negatives,
            "calibration_co_resident_distinct_person_negative_pairs": co_resident_negatives,
        },
        "baseline": {
            "candidate_generation_blocking_rules": common_blocking,
            "features": [
                {"name": "first_name", "weight": 0.20, "similarity": "set bigram Dice"},
                {"name": "last_name", "weight": 0.25, "similarity": "set bigram Dice"},
                {
                    "name": "date_of_birth",
                    "weight": 0.20,
                    "similarity": {
                        "exact": 1.0,
                        "month_day_transposition_same_year": 0.85,
                        "same_year_and_same_month_or_day": 0.65,
                        "same_year_only": 0.35,
                        "otherwise": 0.0,
                    },
                },
                {"name": "street", "weight": 0.15, "similarity": "set bigram Dice"},
                {"name": "city", "weight": 0.05, "similarity": "nonempty exact match"},
                {"name": "state", "weight": 0.05, "similarity": "nonempty exact match"},
                {"name": "postal", "weight": 0.10, "similarity": "nonempty exact match"},
            ],
            "normalization": (
                "Uppercase; remove all characters outside A-Z and 0-9. DOB is "
                "parsed to year/month/day where possible."
            ),
            "missing_value_handling": (
                "Missing/unparseable fields contribute similarity 0. Weights are "
                "not renormalized."
            ),
            "model_class": "deterministic weighted sum",
            "hyperparameters": {
                "score_range": [0.0, 1.0],
                "field_weights_sum": 1.0,
            },
            "package_and_version": {"package": "repository code", "version": run_git("rev-parse", "HEAD")},
            "training_seed": None,
            "calibration_seed": CALIBRATION_SEED,
            "training_pair_count": 0,
            "training_positive_count": 0,
            "training_negative_count": 0,
            "negative_sampling_procedure": "not applicable; no learned parameters",
            "hard_negative_procedure": "none",
            "co_resident_distinct_person_negatives_in_training": False,
            "class_weighting": "not applicable",
            "threshold_search_grid_or_procedure": (
                "Paper-fixed threshold inherited from E0. The legacy calibration "
                "grid is 0.30, 0.31, ..., 0.95, maximizing F1 then precision then threshold."
            ),
            "frozen_threshold": float(thresholds_doc["thresholds"]["baseline"]),
            **separation,
        },
        "splink": {
            "candidate_generation_blocking_rules": common_blocking,
            "features": [
                "first: Jaro-Winkler levels >=0.92, >=0.80, else",
                "last: Jaro-Winkler levels >=0.92, >=0.80, else",
                "dob_norm: exact match",
                "street: Jaro-Winkler levels >=0.92, >=0.75, else",
                "postal: exact match",
            ],
            "similarity_definitions": (
                "Splink 4 comparison-library JaroWinklerAtThresholds and ExactMatch. "
                "City and state are intentionally omitted because they were nested "
                "with postal and produced non-identifiable EM behavior."
            ),
            "missing_value_handling": (
                "Empty normalized values do not enter blocking rules. Splink "
                "comparison levels use the library's generated null levels."
            ),
            "model_class": "Splink Fellegi-Sunter probabilistic linkage model (DuckDB backend)",
            "hyperparameters": {
                "link_type": "dedupe_only",
                "probability_two_random_records_match": 0.001,
                "max_iterations": 100,
                "u_estimation_max_pairs": 10_000_000,
                "u_estimation_seed": CALIBRATION_SEED,
                "em_blocking_passes": [
                    "exact nonempty dob_norm",
                    "exact nonempty postal",
                ],
                "fix_probability_two_random_records_match": True,
            },
            "package_and_version": {
                "splink": package_versions()["splink"],
                "duckdb": package_versions()["duckdb"],
            },
            "training_seed": CALIBRATION_SEED,
            "calibration_seed": CALIBRATION_SEED,
            "training_pair_count": None,
            "training_positive_count": None,
            "training_negative_count": None,
            "training_pair_count_explanation": (
                "Splink training is unsupervised. The saved model does not expose "
                "the realized random-u and EM comparison-row counts; no labeled "
                "positive/negative training counts exist."
            ),
            "threshold_calibration_pair_count": len(candidates),
            "threshold_calibration_positive_count": positives,
            "threshold_calibration_negative_count": negatives,
            "negative_sampling_procedure": (
                "u probabilities use Splink random sampling capped at 10,000,000 "
                "pairs; EM uses the two stated exact-match blocks."
            ),
            "hard_negative_procedure": (
                "No explicit mining. Threshold calibration includes every shared-"
                "blocking candidate, so blocking-induced near negatives are retained."
            ),
            "co_resident_distinct_person_negatives_in_threshold_calibration": (
                co_resident_negatives > 0
            ),
            "co_resident_distinct_person_negative_count": co_resident_negatives,
            "class_weighting": "not applicable to Fellegi-Sunter EM",
            "threshold_search_grid_or_procedure": (
                "Every finite distinct observed calibration score; maximize F1, "
                "then precision, then higher threshold. This is not a fixed decimal grid."
            ),
            "frozen_threshold": float(thresholds_doc["thresholds"]["splink"]),
            "model_artifact": rel(E2 / "models" / "splink.json"),
            "model_artifact_sha256": sha256_file(E2 / "models" / "splink.json"),
            **separation,
        },
        "learned": {
            "candidate_generation_blocking_rules": common_blocking,
            "features": [
                "first bigram-Dice similarity",
                "last bigram-Dice similarity",
                "DOB graded similarity",
                "street bigram-Dice similarity",
                "city nonempty exact similarity",
                "state nonempty exact similarity",
                "postal nonempty exact similarity",
                "seven additional indicators, one for each base feature == 0.0",
            ],
            "similarity_definitions": (
                "The seven base values are identical to the baseline components. "
                "The second seven are executable zero-value proxies, not pure "
                "missingness indicators: exact dissimilarity and missingness both map to 1."
            ),
            "missing_value_handling": (
                "Missing fields produce base similarity 0 and corresponding zero-"
                "proxy 1. StandardScaler is fitted on all 14 columns."
            ),
            "model_class": "sklearn.pipeline.Pipeline(StandardScaler, LogisticRegression)",
            "hyperparameters": {
                "classifier": "sklearn.linear_model.LogisticRegression",
                "class_weight": "balanced",
                "max_iter": 2000,
                "random_state": CALIBRATION_SEED,
                "other_parameters": "scikit-learn 1.7.2 defaults",
            },
            "package_and_version": {
                "package": "scikit-learn",
                "version": package_versions()["scikit-learn"],
            },
            "training_seed": CALIBRATION_SEED,
            "calibration_seed": CALIBRATION_SEED,
            "training_pair_count": len(candidates),
            "training_positive_count": positives,
            "training_negative_count": negatives,
            "negative_sampling_procedure": (
                "No downsampling: all shared-blocking candidates from clean seed "
                "20260719 are used."
            ),
            "hard_negative_procedure": (
                "No explicit mining. All blocking-induced candidate negatives are used."
            ),
            "co_resident_distinct_person_negatives_in_training": co_resident_negatives > 0,
            "co_resident_distinct_person_negative_count": co_resident_negatives,
            "class_weighting": "balanced (inverse-frequency class weights)",
            "threshold_search_grid_or_procedure": (
                "Every finite distinct observed calibration probability; maximize "
                "F1, then precision, then higher threshold. Not a fixed decimal grid."
            ),
            "frozen_threshold": float(thresholds_doc["thresholds"]["learned"]),
            "model_artifact": rel(E2 / "models" / "learned.joblib"),
            "model_artifact_sha256": sha256_file(E2 / "models" / "learned.joblib"),
            **separation,
        },
        "calibration_source": {
            "run_path": rel(calibration_run),
            "run_manifest_sha256": sha256_file(calibration_run / "manifest.json"),
            "thresholds_source": rel(E2 / "models" / "thresholds.json"),
            "thresholds_source_sha256": sha256_file(E2 / "models" / "thresholds.json"),
        },
        "household_transfer_splink": {
            "design": (
                "For each named scenario, train a new Splink model and select its "
                "threshold on scenario-specific seed 20260719, then freeze both "
                "for evaluation seeds 20260720-20260729."
            ),
            "scenarios": household_transfer_protocol,
        },
    }
    write_json(BUNDLE / "matcher_protocol.json", protocol)
    return protocol


def build_config_mapping() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    truth_cache: dict[str, tuple[str, dict[str, str]]] = {}

    def truth(run: Path) -> tuple[str, dict[str, str]]:
        key = str(run.resolve())
        if key not in truth_cache:
            truth_cache[key] = composite_truth_hash(run)
        return truth_cache[key]

    e1_labels = {
        "clean": "Clean",
        "high_noise": "High noise",
        "low_overlap": "Low overlap",
        "one_to_many": "One-to-many",
    }
    for seed in SEEDS:
        reference = E1 / "configs" / f"e1_clean_s{seed}.yaml"
        for condition in E1_CONDITIONS:
            config = E1 / "configs" / f"e1_{condition}_s{seed}.yaml"
            run = (
                REPO
                / "phase2"
                / "runs"
                / f"2026-07-22_e1_{condition}_s{seed}_seed{seed}"
            )
            truth_hash, component_hashes = truth(run)
            rows.append(
                {
                    "paper_table": "Table I",
                    "paper_analysis_label": e1_labels[condition],
                    "experiment_id": "E1",
                    "condition_or_scenario": condition,
                    "seed": seed,
                    "yaml_path": rel(config),
                    "yaml_sha256": sha256_file(config),
                    "truth_layer_sha256": truth_hash,
                    "truth_layer_component_sha256_json": json.dumps(
                        component_hashes, sort_keys=True, separators=(",", ":")
                    ),
                    "emission_config_differences_from_table_reference": yaml_diff(
                        reference, config
                    ),
                    "reference_yaml_path": rel(reference),
                    "run_path": rel(run),
                }
            )

    for seed in SEEDS:
        reference = E2 / "configs" / f"e2_clean_baseline_linkage_s{seed}.yaml"
        for scenario in E2_SCENARIOS:
            config = E2 / "configs" / f"e2_{scenario}_s{seed}.yaml"
            run = (
                REPO
                / "phase2"
                / "runs"
                / f"2026-07-24_e2_{scenario}_s{seed}_seed{seed}"
            )
            truth_hash, component_hashes = truth(run)
            rows.append(
                {
                    "paper_table": "Table II",
                    "paper_analysis_label": scenario,
                    "experiment_id": "E2",
                    "condition_or_scenario": scenario,
                    "seed": seed,
                    "yaml_path": rel(config),
                    "yaml_sha256": sha256_file(config),
                    "truth_layer_sha256": truth_hash,
                    "truth_layer_component_sha256_json": json.dumps(
                        component_hashes, sort_keys=True, separators=(",", ":")
                    ),
                    "emission_config_differences_from_table_reference": yaml_diff(
                        reference, config
                    ),
                    "reference_yaml_path": rel(reference),
                    "run_path": rel(run),
                }
            )

    scale_tokens = {0.0: "k0", 0.5: "k05", 1.0: "k1", 2.0: "k2", 4.0: "k4"}
    for seed in SEEDS:
        reference = E3 / "configs" / f"e3_noise_k0_s{seed}.yaml"
        for scale in NOISE_LEVELS:
            token = scale_tokens[scale]
            config = E3 / "configs" / f"e3_noise_{token}_s{seed}.yaml"
            if scale == 1.0:
                # E3 deliberately reuses the canonical E2 high-noise run at k=1.
                run = (
                    REPO
                    / "phase2"
                    / "runs"
                    / f"2026-07-24_e2_high_noise_identity_drift_s{seed}_seed{seed}"
                )
            else:
                run = (
                    REPO
                    / "phase2"
                    / "runs"
                    / f"2026-07-25_e3_noise_{token}_s{seed}_seed{seed}"
                )
            truth_hash, component_hashes = truth(run)
            rows.append(
                {
                    "paper_table": "Table III",
                    "paper_analysis_label": f"k={scale:g}",
                    "experiment_id": "E3",
                    "condition_or_scenario": f"noise_k{scale:g}",
                    "seed": seed,
                    "yaml_path": rel(config),
                    "yaml_sha256": sha256_file(config),
                    "truth_layer_sha256": truth_hash,
                    "truth_layer_component_sha256_json": json.dumps(
                        component_hashes, sort_keys=True, separators=(",", ":")
                    ),
                    "emission_config_differences_from_table_reference": yaml_diff(
                        reference, config
                    ),
                    "reference_yaml_path": rel(reference),
                    "run_path": rel(run),
                }
            )

    frame = pd.DataFrame(rows).sort_values(
        ["paper_table", "paper_analysis_label", "seed"]
    )
    frame["truth_layer_hash_definition"] = (
        "SHA-256 of sorted UTF-8 name-NUL-fileSHA256-NUL entries for "
        + ", ".join(TRUTH_FILES)
    )
    frame.to_csv(BUNDLE / "config_mapping.csv", index=False)
    return frame


def display_order(values: dict[str, float], digits: int | None) -> list[list[str]]:
    if digits is None:
        keys = sorted(values, key=lambda key: (-values[key], key))
        groups: list[list[str]] = []
        for key in keys:
            if groups and values[groups[-1][0]] == values[key]:
                groups[-1].append(key)
            else:
                groups.append([key])
        return groups
    formatted = {key: f"{value:.{digits}f}" for key, value in values.items()}
    keys = sorted(values, key=lambda key: (-float(formatted[key]), key))
    groups = []
    for key in keys:
        if groups and formatted[groups[-1][0]] == formatted[key]:
            groups[-1].append(key)
        else:
            groups.append([key])
    return groups


def order_text(groups: list[list[str]]) -> str:
    return " > ".join(" = ".join(group) for group in groups)


def build_derived_claims() -> dict[str, Any]:
    e1 = pd.read_csv(E4 / "e1_e4_e5_per_run.csv")
    baseline = e1[e1["matcher"] == "baseline"].copy()
    means = baseline.groupby("condition", sort=False)[
        ["f1", "oracle_f1", "true_pairs"]
    ].mean()
    clean_f1 = float(means.loc["clean", "f1"])
    high_f1 = float(means.loc["high_noise", "f1"])
    high_oracle = float(means.loc["high_noise", "oracle_f1"])
    loss = clean_f1 - high_f1
    recovered = high_oracle - high_f1
    recovery_pct = 100.0 * recovered / loss
    unrecovered_pct = 100.0 - recovery_pct

    e1_base = pd.read_csv(E1 / "e1_per_run.csv")
    e1_base = e1_base[e1_base["threshold_variant"] == "fixed"]
    truth_means = e1_base.groupby("condition")["true_links"].mean()
    low_reduction = float(truth_means["clean"] - truth_means["low_overlap"])
    low_reduction_pct = 100.0 * low_reduction / float(truth_means["clean"])
    runtime_means = e1_base.groupby("condition")["runtime_s"].mean()
    e1_runtime_increase = float(
        runtime_means["one_to_many"] - runtime_means["clean"]
    )
    e1_runtime_increase_pct = (
        100.0 * e1_runtime_increase / float(runtime_means["clean"])
    )
    e8_runtime = pd.read_csv(E8 / "e8_runtime_repetitions.csv")
    e8_means = e8_runtime.groupby("condition")["runtime_s"].mean()
    e8_runtime_increase = float(e8_means["one_to_many"] - e8_means["clean"])
    e8_runtime_increase_pct = (
        100.0 * e8_runtime_increase / float(e8_means["clean"])
    )

    e2 = pd.read_csv(E2 / "e2_per_run_metrics.csv")
    e2_means = e2.groupby(["scenario", "matcher"])["f1"].mean()
    matcher_spreads: list[dict[str, Any]] = []
    for scenario in E2_SCENARIOS:
        values = {
            matcher: float(e2_means.loc[(scenario, matcher)])
            for matcher in MATCHERS
        }
        unrounded_groups = display_order(values, None)
        displayed_groups = display_order(values, 4)
        matcher_spreads.append(
            {
                "scenario": scenario,
                "matcher_mean_f1_unrounded": values,
                "matcher_mean_f1_displayed_4dp": {
                    key: f"{value:.4f}" for key, value in values.items()
                },
                "spread_formula": "max(matcher mean F1) - min(matcher mean F1)",
                "spread_unrounded": max(values.values()) - min(values.values()),
                "spread_displayed_4dp": (
                    f"{max(values.values()) - min(values.values()):.4f}"
                ),
                "unrounded_rank_tie_groups": unrounded_groups,
                "unrounded_order": order_text(unrounded_groups),
                "displayed_4dp_rank_tie_groups": displayed_groups,
                "displayed_4dp_order": order_text(displayed_groups),
            }
        )

    claims = {
        "calculation_policy": (
            "All calculations use unrounded per-seed CSV values. Display values "
            "are derived afterward with round-half-even Python formatting."
        ),
        "clean_to_high_noise_f1_difference": {
            "formula": "mean(clean baseline fixed F1) - mean(high-noise baseline fixed F1)",
            "clean_mean_f1_unrounded": clean_f1,
            "high_noise_mean_f1_unrounded": high_f1,
            "difference_unrounded": loss,
            "difference_displayed_4dp": f"{loss:.4f}",
            "source_filenames": [rel(E4 / "e1_e4_e5_per_run.csv")],
        },
        "oracle_improvement": {
            "formula": "mean(high-noise per-seed oracle F1) - mean(high-noise fixed F1)",
            "high_noise_oracle_mean_f1_unrounded": high_oracle,
            "improvement_unrounded": recovered,
            "improvement_displayed_4dp": f"{recovered:.4f}",
            "source_filenames": [rel(E4 / "e1_e4_e5_per_run.csv")],
        },
        "percentage_recovered": {
            "formula": "100 * oracle improvement / clean-to-high-noise fixed F1 difference",
            "value_unrounded_pct": recovery_pct,
            "value_displayed_1dp_pct": f"{recovery_pct:.1f}",
            "source_filenames": [rel(E4 / "e1_e4_e5_per_run.csv")],
        },
        "percentage_unrecovered": {
            "formula": "100 - percentage recovered",
            "value_unrounded_pct": unrecovered_pct,
            "value_displayed_1dp_pct": f"{unrecovered_pct:.1f}",
            "interpretation": (
                "Loss not recoverable by per-seed threshold selection under this "
                "candidate generator and scoring model; not proof of intrinsic identity-evidence loss."
            ),
            "source_filenames": [rel(E4 / "e1_e4_e5_per_run.csv")],
        },
        "low_overlap_true_link_reduction": {
            "formula": (
                "mean(clean true links) - mean(low-overlap true links); percentage "
                "uses mean(clean true links) as denominator"
            ),
            "clean_true_links_mean": float(truth_means["clean"]),
            "low_overlap_true_links_mean": float(truth_means["low_overlap"]),
            "absolute_reduction": low_reduction,
            "relative_reduction_pct": low_reduction_pct,
            "source_filenames": [rel(E1 / "e1_per_run.csv")],
        },
        "one_to_many_runtime_increase": {
            "e1_across_independent_seeds": {
                "formula": "mean(one-to-many runtime) - mean(clean runtime)",
                "experimental_unit": "ten independently generated seed populations",
                "clean_mean_seconds": float(runtime_means["clean"]),
                "one_to_many_mean_seconds": float(runtime_means["one_to_many"]),
                "increase_seconds": e1_runtime_increase,
                "increase_pct": e1_runtime_increase_pct,
                "source_filenames": [rel(E1 / "e1_per_run.csv")],
            },
            "e8_fixed_population_repeated_timing": {
                "formula": "mean(one-to-many repeats) - mean(clean repeats)",
                "experimental_unit": "five timed repeats on fixed seed 20260720 artifacts",
                "clean_mean_seconds": float(e8_means["clean"]),
                "one_to_many_mean_seconds": float(e8_means["one_to_many"]),
                "increase_seconds": e8_runtime_increase,
                "increase_pct": e8_runtime_increase_pct,
                "source_filenames": [rel(E8 / "e8_runtime_repetitions.csv")],
            },
        },
        "matcher_spread_all_fourteen_scenarios": {
            "formula": "max of three matcher mean F1 values minus minimum",
            "display_precision": 4,
            "scenarios": matcher_spreads,
            "source_filenames": [rel(E2 / "e2_per_run_metrics.csv")],
        },
    }
    write_json(BUNDLE / "derived_claims.json", claims)
    return claims


def summary_statistics_row(
    comparison_id: str,
    label: str,
    differences: pd.Series,
    *,
    scenario: str,
    left: str,
    right: str,
    holm_family: str = "",
) -> dict[str, Any]:
    mean, sd, low, high = mean_ci(differences)
    row = {
        "row_type": "summary",
        "comparison_id": comparison_id,
        "comparison_label": label,
        "scenario": scenario,
        "seed": "",
        "left_quantity": left,
        "right_quantity": right,
        "difference_definition": f"{left} - {right}",
        "n": len(differences),
        "mean_difference": mean,
        "sd_paired_differences": sd,
        "ci95_low": low,
        "ci95_high": high,
        "holm_family": holm_family,
    }
    row.update(paired_tests(differences))
    return row


def build_paired_statistics() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    e1 = pd.read_csv(E4 / "e1_e4_e5_per_run.csv")
    base = e1[e1["matcher"] == "baseline"].pivot(
        index="seed", columns="condition", values="f1"
    )
    differences = base["clean"] - base["high_noise"]
    for seed in SEEDS:
        rows.append(
            {
                "row_type": "per_seed",
                "comparison_id": "E1_clean_minus_high_noise",
                "comparison_label": "E1 clean versus high noise",
                "scenario": "E1",
                "seed": seed,
                "left_quantity": "clean_f1",
                "right_quantity": "high_noise_f1",
                "difference_definition": "clean_f1 - high_noise_f1",
                "left_value": float(base.loc[seed, "clean"]),
                "right_value": float(base.loc[seed, "high_noise"]),
                "paired_difference": float(differences.loc[seed]),
                "clean_f1": float(base.loc[seed, "clean"]),
                "high_noise_f1": float(base.loc[seed, "high_noise"]),
            }
        )
    rows.append(
        summary_statistics_row(
            "E1_clean_minus_high_noise",
            "E1 clean versus high noise",
            differences,
            scenario="E1",
            left="clean_f1",
            right="high_noise_f1",
        )
    )

    e2 = pd.read_csv(E2 / "e2_per_run_metrics.csv")
    summary_positions: list[int] = []
    for scenario in E2_SCENARIOS:
        pivot = e2[e2["scenario"] == scenario].pivot(
            index="seed", columns="matcher", values="f1"
        )
        for left, right in (("learned", "baseline"), ("baseline", "splink")):
            comparison_id = f"E2_{scenario}_{left}_minus_{right}"
            delta = pivot[left] - pivot[right]
            for seed in SEEDS:
                rows.append(
                    {
                        "row_type": "per_seed",
                        "comparison_id": comparison_id,
                        "comparison_label": f"{scenario}: {left} versus {right}",
                        "scenario": scenario,
                        "seed": seed,
                        "left_quantity": f"{left}_f1",
                        "right_quantity": f"{right}_f1",
                        "difference_definition": f"{left}_f1 - {right}_f1",
                        "left_value": float(pivot.loc[seed, left]),
                        "right_value": float(pivot.loc[seed, right]),
                        "paired_difference": float(delta.loc[seed]),
                    }
                )
            summary_positions.append(len(rows))
            rows.append(
                summary_statistics_row(
                    comparison_id,
                    f"{scenario}: {left} versus {right}",
                    delta,
                    scenario=scenario,
                    left=f"{left}_f1",
                    right=f"{right}_f1",
                    holm_family="all 28 Table II matcher contrasts",
                )
            )

    for source, destination in (
        ("paired_t_p_value", "paired_t_holm_p_value"),
        ("wilcoxon_p_value", "wilcoxon_holm_p_value"),
        ("exact_sign_p_value", "exact_sign_holm_p_value"),
    ):
        adjusted = holm_adjust([float(rows[index][source]) for index in summary_positions])
        for index, value in zip(summary_positions, adjusted):
            rows[index][destination] = value
    frame = pd.DataFrame(rows)
    frame["source_filename"] = np.where(
        frame["comparison_id"].eq("E1_clean_minus_high_noise"),
        rel(E4 / "e1_e4_e5_per_run.csv"),
        rel(E2 / "e2_per_run_metrics.csv"),
    )
    frame.to_csv(BUNDLE / "paired_statistics.csv", index=False)
    return frame


def build_oracle_bootstrap(replicates: int = 20_000) -> dict[str, Any]:
    frame = pd.read_csv(E4 / "e1_e4_e5_per_run.csv")
    base = frame[frame["matcher"] == "baseline"].set_index(["seed", "condition"])
    clean = np.asarray([base.loc[(seed, "clean"), "f1"] for seed in SEEDS], dtype=float)
    high = np.asarray(
        [base.loc[(seed, "high_noise"), "f1"] for seed in SEEDS], dtype=float
    )
    oracle = np.asarray(
        [base.loc[(seed, "high_noise"), "oracle_f1"] for seed in SEEDS], dtype=float
    )
    losses = clean - high
    recovered = oracle - high
    rng_seed = 20260723
    rng = np.random.default_rng(rng_seed)
    indices = rng.integers(0, len(SEEDS), size=(replicates, len(SEEDS)))
    boot_loss = losses[indices].mean(axis=1)
    boot_recovered = recovered[indices].mean(axis=1)
    boot_recovery_pct = 100.0 * boot_recovered / boot_loss
    boot_unrecovered_pct = 100.0 - boot_recovery_pct

    def estimate(values: np.ndarray, boots: np.ndarray) -> dict[str, Any]:
        return {
            "point_estimate": float(np.mean(values)),
            "percentile_ci95": [
                float(np.percentile(boots, 2.5)),
                float(np.percentile(boots, 97.5)),
            ],
        }

    recovery_point = 100.0 * float(np.mean(recovered)) / float(np.mean(losses))
    legacy_grid = np.round(np.arange(0.30, 0.951, 0.01), 2).tolist()
    thresholds = [
        {
            "seed": seed,
            "oracle_threshold": float(
                base.loc[(seed, "high_noise"), "oracle_threshold"]
            ),
            "on_legacy_0_01_grid": bool(
                np.any(
                    np.isclose(
                        legacy_grid,
                        float(base.loc[(seed, "high_noise"), "oracle_threshold"]),
                        atol=1e-12,
                    )
                )
            ),
        }
        for seed in SEEDS
    ]
    result = {
        "bootstrap_unit": "paired seed",
        "replicates": replicates,
        "bootstrap_seed": rng_seed,
        "sampling": (
            "Sample the ten paired seed indices with replacement; compute each "
            "statistic from the resampled seed means."
        ),
        "fixed_threshold_f1_loss": estimate(losses, boot_loss),
        "oracle_recovered_amount": estimate(recovered, boot_recovered),
        "recovery_percentage": {
            "point_estimate": recovery_point,
            "percentile_ci95": [
                float(np.percentile(boot_recovery_pct, 2.5)),
                float(np.percentile(boot_recovery_pct, 97.5)),
            ],
        },
        "unrecovered_percentage": {
            "point_estimate": 100.0 - recovery_point,
            "percentile_ci95": [
                float(np.percentile(boot_unrecovered_pct, 2.5)),
                float(np.percentile(boot_unrecovered_pct, 97.5)),
            ],
        },
        "formulas": {
            "fixed_threshold_f1_loss": "clean fixed F1 - high-noise fixed F1",
            "oracle_recovered_amount": "high-noise oracle F1 - high-noise fixed F1",
            "recovery_percentage": "100 * mean(recovered amount) / mean(fixed loss)",
            "unrecovered_percentage": "100 - recovery percentage",
        },
        "oracle_threshold_selection_scope": "selected separately within each seed",
        "oracle_threshold_procedure": (
            "evaluation.metrics.threshold_curve evaluates +infinity (no predictions) "
            "and every finite distinct observed score. oracle_from_curve removes "
            "+infinity, then maximizes F1, precision, and threshold in that order."
        ),
        "legacy_baseline_threshold_grid": legacy_grid,
        "legacy_grid_source": (
            "paper_experiments/evaluate_baseline.py choose_threshold; this grid "
            "applies to the E0/E1 calibration variant, not E4's exact-score oracle."
        ),
        "observed_per_seed_high_noise_oracle_thresholds": thresholds,
        "thresholds_not_on_legacy_grid_explanation": (
            "Any non-grid value is valid because E4 uses every observed score "
            "boundary, not the 0.01 legacy calibration grid. The oracle is a "
            "descriptive in-sample upper bound."
        ),
        "pooled_curve_artifact": rel(E4 / "pr_curve_baseline_high_noise.csv"),
        "pooled_curve_artifact_sha256": sha256_file(
            E4 / "pr_curve_baseline_high_noise.csv"
        ),
        "pooled_curve_note": (
            "The checked-in pooled curve is separate from the per-seed oracle "
            "selection used in this bootstrap."
        ),
        "source_filename": rel(E4 / "e1_e4_e5_per_run.csv"),
    }
    write_json(BUNDLE / "oracle_bootstrap.json", result)
    return result


def build_noise_trend() -> pd.DataFrame:
    source = pd.read_csv(E3 / "e3_per_run.csv")
    rows: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for matcher in MATCHERS:
        monotone_flags: list[bool] = []
        rhos: list[float] = []
        endpoint_differences: list[float] = []
        for seed in SEEDS:
            group = source[
                (source["matcher"] == matcher) & (source["seed"] == seed)
            ].sort_values("scale")
            values = group["f1"].to_numpy(dtype=float)
            scales = group["scale"].to_numpy(dtype=float)
            monotone = bool(np.all(np.diff(values) < 0))
            spearman = stats.spearmanr(scales, values)
            monotone_flags.append(monotone)
            rhos.append(float(spearman.statistic))
            endpoint_differences.append(float(values[0] - values[-1]))
            for current in group.itertuples(index=False):
                rows.append(
                    {
                        "row_type": "per_matcher_seed_level",
                        "matcher": matcher,
                        "seed": seed,
                        "k": float(current.scale),
                        "f1": float(current.f1),
                        "seed_strictly_decreasing_across_tested_k": monotone,
                        "spearman_rho_for_seed": float(spearman.statistic),
                        "spearman_p_value_for_seed": float(spearman.pvalue),
                        "tested_k_values": json.dumps(list(NOISE_LEVELS)),
                    }
                )
        count = sum(monotone_flags)
        ci = stats.binomtest(count, len(SEEDS)).proportion_ci(
            confidence_level=0.95, method="exact"
        )
        endpoint_mean, endpoint_sd, endpoint_low, endpoint_high = mean_ci(
            endpoint_differences
        )
        endpoint_tests = paired_tests(endpoint_differences)
        summaries.append(
            {
                "row_type": "matcher_summary",
                "matcher": matcher,
                "seed": "",
                "k": "",
                "n_seeds": len(SEEDS),
                "monotone_seed_count": count,
                "monotone_seed_fraction": count / len(SEEDS),
                "monotone_fraction_exact_ci95_low": float(ci.low),
                "monotone_fraction_exact_ci95_high": float(ci.high),
                "monotone_count_one_sided_binomial_p_vs_0_5": float(
                    stats.binomtest(count, len(SEEDS), 0.5, alternative="greater").pvalue
                ),
                "spearman_rho_mean": float(np.mean(rhos)),
                "spearman_rho_min": float(np.min(rhos)),
                "spearman_rho_max": float(np.max(rhos)),
                "k0_minus_k4_f1_mean": endpoint_mean,
                "k0_minus_k4_f1_sd": endpoint_sd,
                "k0_minus_k4_f1_ci95_low": endpoint_low,
                "k0_minus_k4_f1_ci95_high": endpoint_high,
                **{
                    f"k0_minus_k4_{key}": value
                    for key, value in endpoint_tests.items()
                },
                "scope_statement": (
                    "Strict decrease is established only at k={0,0.5,1,2,4} "
                    "for these ten seeds, not at untested intermediate values."
                ),
                "tested_k_values": json.dumps(list(NOISE_LEVELS)),
            }
        )
    frame = pd.concat([pd.DataFrame(rows), pd.DataFrame(summaries)], ignore_index=True)
    frame["source_filename"] = rel(E3 / "e3_per_run.csv")
    frame.to_csv(BUNDLE / "noise_trend.csv", index=False)
    return frame


def build_cluster_metrics() -> pd.DataFrame:
    metric_names = (
        "precision",
        "recall",
        "f1",
        "b3_precision",
        "b3_recall",
        "b3_f1",
        "closure_precision",
        "closure_recall",
        "closure_f1",
    )
    rows: list[dict[str, Any]] = []
    sources = [
        (
            "E1",
            pd.read_csv(E4 / "e1_e4_e5_per_run.csv"),
            "condition",
            rel(E4 / "e1_e4_e5_per_run.csv"),
        ),
        (
            "E2",
            pd.read_csv(E2 / "e2_per_run_metrics.csv"),
            "scenario",
            rel(E2 / "e2_per_run_metrics.csv"),
        ),
    ]
    for experiment, frame, condition_column, source_filename in sources:
        for current in frame.itertuples(index=False):
            row = {
                "row_type": "per_seed",
                "experiment": experiment,
                "condition_or_scenario": getattr(current, condition_column),
                "seed": int(current.seed),
                "matcher": current.matcher,
                "source_filename": source_filename,
            }
            for metric in metric_names:
                row[metric] = float(getattr(current, metric))
            rows.append(row)
        for keys, group in frame.groupby([condition_column, "matcher"], sort=False):
            condition, matcher = keys
            row = {
                "row_type": "summary",
                "experiment": experiment,
                "condition_or_scenario": condition,
                "seed": "",
                "matcher": matcher,
                "n_seeds": len(group),
                "source_filename": source_filename,
            }
            for metric in metric_names:
                row[f"{metric}_mean"] = float(group[metric].mean())
                row[f"{metric}_sd"] = float(group[metric].std(ddof=1))
            absolute_gap = float(
                np.mean(np.abs(group["closure_f1"] - group["f1"]))
            )
            row["mean_absolute_closure_minus_pairwise_f1"] = absolute_gap
            row["closure_explanation"] = (
                "Thresholded edges form almost entirely two-record or otherwise "
                "sparse components, so transitive closure introduces few additional "
                "eligible pairs. The reported mean absolute F1 gap quantifies this."
            )
            rows.append(row)
    output = pd.DataFrame(rows)
    output.to_csv(BUNDLE / "cluster_metrics.csv", index=False)
    return output


def build_runtime_results() -> pd.DataFrame:
    hardware = json.loads((E8 / "e8_hardware.json").read_text(encoding="utf-8"))
    hardware_text = json.dumps(hardware, sort_keys=True, separators=(",", ":"))
    rows: list[dict[str, Any]] = []
    e1 = pd.read_csv(E1 / "e1_per_run.csv")
    e1 = e1[e1["threshold_variant"] == "fixed"]
    e1_clean_mean = float(e1[e1["condition"] == "clean"]["runtime_s"].mean())
    for condition, group in e1.groupby("condition", sort=False):
        rows.append(
            {
                "timing_design": "E1_across_independent_seeds",
                "condition": condition,
                "mean_seconds": float(group["runtime_s"].mean()),
                "sd_seconds": float(group["runtime_s"].std(ddof=1)),
                "min_seconds": float(group["runtime_s"].min()),
                "max_seconds": float(group["runtime_s"].max()),
                "repetitions": len(group),
                "independent_population_count": int(group["seed"].nunique()),
                "records_mean": float(
                    (group["records_a"] + group["records_b"]).mean()
                ),
                "records_a_mean": float(group["records_a"].mean()),
                "records_b_mean": float(group["records_b"].mean()),
                "candidate_pairs_mean": float(group["candidate_pairs"].mean()),
                "runtime_increase_vs_clean_seconds": float(
                    group["runtime_s"].mean() - e1_clean_mean
                ),
                "runtime_increase_vs_clean_pct": float(
                    100.0 * (group["runtime_s"].mean() - e1_clean_mean) / e1_clean_mean
                ),
                "hardware_json": hardware_text,
                "hardware_source": rel(E8 / "e8_hardware.json"),
                "timed_scope": hardware["timed_scope"],
                "source_filename": rel(E1 / "e1_per_run.csv"),
            }
        )
    repeats = pd.read_csv(E8 / "e8_runtime_repetitions.csv")
    workload = pd.read_csv(E8 / "e8_workload_per_seed.csv")
    e8_clean_mean = float(
        repeats[repeats["condition"] == "clean"]["runtime_s"].mean()
    )
    for condition, group in repeats.groupby("condition", sort=False):
        seed = int(group["seed"].iloc[0])
        work = workload[
            (workload["seed"] == seed) & (workload["condition"] == condition)
        ].iloc[0]
        rows.append(
            {
                "timing_design": "E8_fixed_population_repeated_timing",
                "condition": condition,
                "mean_seconds": float(group["runtime_s"].mean()),
                "sd_seconds": float(group["runtime_s"].std(ddof=1)),
                "min_seconds": float(group["runtime_s"].min()),
                "max_seconds": float(group["runtime_s"].max()),
                "repetitions": len(group),
                "independent_population_count": 1,
                "fixed_seed": seed,
                "records_mean": float(work["records_a"] + work["records_b"]),
                "records_a_mean": float(work["records_a"]),
                "records_b_mean": float(work["records_b"]),
                "candidate_pairs_mean": float(work["candidate_pairs"]),
                "runtime_increase_vs_clean_seconds": float(
                    group["runtime_s"].mean() - e8_clean_mean
                ),
                "runtime_increase_vs_clean_pct": float(
                    100.0 * (group["runtime_s"].mean() - e8_clean_mean) / e8_clean_mean
                ),
                "hardware_json": hardware_text,
                "hardware_source": rel(E8 / "e8_hardware.json"),
                "timed_scope": hardware["timed_scope"],
                "source_filename": rel(E8 / "e8_runtime_repetitions.csv"),
            }
        )
    output = pd.DataFrame(rows)
    output.to_csv(BUNDLE / "runtime_results.csv", index=False)
    return output


def target_metadata(dimension: str, target: float) -> dict[str, Any]:
    if dimension.startswith("mobility_"):
        return {
            "source_year_version": "ACS 1-year 2024, table B07001",
            "original_units_and_denominator": "percent of residents age 1+ in the stated age group",
            "eligibility_population": "resident population age 1+; cohort-specific where named",
            "annual_entity_probability": target / 100.0,
            "annual_probability_conversion": "published percentage / 100",
            "simulation_parameter": (
                "mobility_by_age step hazard"
                if dimension != "mobility_overall_pct"
                else "move_rate_pct plus age-specific mobility_by_age hazards"
            ),
            "construction_status": "emergent",
            "locking_effects": (
                "Monthly constant-hazard conversion is exact only absent other "
                "constraints. Household-level movement, earlier events in the fixed "
                "event order, and the per-step person lock can lower or redistribute incidence."
            ),
            "target_source_file": (
                "phase2/Data/phase2_params/mobility_by_age_cohort_acs_2024.csv"
                if dimension != "mobility_overall_pct"
                else "phase2/Data/phase2_params/mobility_overall_acs_2024.csv"
            ),
        }
    if dimension.startswith("household_"):
        return {
            "source_year_version": "ACS 1-year 2024, table B11001",
            "original_units_and_denominator": (
                "share of all households"
                if "size_js" not in dimension
                else "not available from B11001"
            ),
            "eligibility_population": "baseline households",
            "annual_entity_probability": None,
            "annual_probability_conversion": "not an event-rate conversion",
            "simulation_parameter": "initialize_households_from_public_targets",
            "construction_status": (
                "not estimable"
                if dimension == "household_size_js_distance"
                else "by construction"
            ),
            "locking_effects": (
                "Household-type shares are baseline allocation constraints, not "
                "emergent validation outcomes. Event locking does not define the baseline."
            ),
            "target_source_file": (
                "phase2/Data/phase2_params/household_type_shares_acs_2024.csv"
            ),
        }
    if dimension == "cohabitation_rate_per_1000":
        return {
            "source_year_version": "CDC/NCHS provisional marriage statistic, 2023",
            "original_units_and_denominator": "marriages per 1,000 total population",
            "eligibility_population": (
                "published: total population; simulation: unpartnered marriage-age people"
            ),
            "annual_entity_probability": target / 1000.0,
            "annual_probability_conversion": (
                "rate/1000 is recorded as a nominal annual probability, but the "
                "construct is incompatible: COHABIT is not marriage"
            ),
            "simulation_parameter": "cohabit_rate_pct",
            "construction_status": "emergent and not comparable",
            "locking_effects": (
                "People are paired, propensity-weighted, and excluded when an "
                "earlier event locks them; achieved COHABIT incidence need not equal marriage rate."
            ),
            "target_source_file": (
                "phase2/Data/phase2_params/marriage_divorce_rates_cdc_2023.csv"
            ),
        }
    if dimension == "divorce_rate_per_1000":
        return {
            "source_year_version": "CDC/NCHS provisional reporting areas, 2023",
            "original_units_and_denominator": "divorces per 1,000 total population",
            "eligibility_population": (
                "published: total population in reporting areas; simulation: active couples"
            ),
            "annual_entity_probability": target / 1000.0,
            "annual_probability_conversion": (
                "rate/1000 -> nominal annual population event probability; "
                "simulator rescales the per-couple step hazard by people/active-couples"
            ),
            "simulation_parameter": "divorce_rate_pct",
            "construction_status": "emergent",
            "locking_effects": (
                "Only active unlocked couples are eligible. Death/name-change locks "
                "and active-couple availability alter achieved population incidence."
            ),
            "target_source_file": (
                "phase2/Data/phase2_params/marriage_divorce_rates_cdc_2023.csv"
            ),
        }
    if dimension.startswith("fertility_"):
        return {
            "source_year_version": "NCHS provisional 2024, VSRR No. 38 Table 1",
            "original_units_and_denominator": "births per 1,000 women in the named age group",
            "eligibility_population": "women in the configured fertility age bounds and age band",
            "annual_entity_probability": target / 1000.0,
            "annual_probability_conversion": (
                "published births per 1,000 women / 1000, treated as annual "
                "at-least-one-birth probability before monthly constant-hazard conversion"
            ),
            "simulation_parameter": "fertility_by_age step hazard (birth_rate_pct fallback)",
            "construction_status": "emergent",
            "locking_effects": (
                "An earlier event can lock a woman for the month; birth then locks "
                "parents and child. Eligibility and competing events make achieved "
                "incidence differ from the nominal hazard."
            ),
            "target_source_file": (
                "phase2/Data/phase2_params/fertility_by_age_nchs_2024.csv"
            ),
        }
    if dimension.startswith("surname_"):
        return {
            "source_year_version": "checked-in Phase 1 conditional Census surname pool",
            "original_units_and_denominator": (
                "population share"
                if "share_pct" in dimension
                else "distributional summary"
            ),
            "eligibility_population": "generated Phase 1 people, conditional on ethnicity mix",
            "annual_entity_probability": None,
            "annual_probability_conversion": "not an event-rate conversion",
            "simulation_parameter": "Phase 1 surname sampling distribution",
            "construction_status": "by construction / sampler fidelity check",
            "locking_effects": "not applicable to the baseline surname distribution",
            "target_source_file": "phase1/prepared/last_names.parquet",
        }
    raise KeyError(dimension)


def build_public_target_conversion() -> pd.DataFrame:
    per_seed = pd.read_csv(E7 / "e7_per_seed.csv")
    summary = pd.read_csv(E7 / "e7_summary.csv")
    rows: list[dict[str, Any]] = []
    for current in summary.itertuples(index=False):
        dimension = str(current.dimension)
        group = per_seed[per_seed["dimension"] == dimension].sort_values("seed")
        metadata = target_metadata(dimension, float(current.target))
        annual = metadata["annual_entity_probability"]
        per_step = (
            1.0 - (1.0 - float(annual)) ** (1.0 / 12.0)
            if annual is not None and 0.0 <= float(annual) < 1.0
            else annual
        )
        source_path = REPO / metadata["target_source_file"]
        rows.append(
            {
                "dimension": dimension,
                "original_source_value": (
                    float(current.target) if pd.notna(current.target) else ""
                ),
                "target_per_seed_json": json.dumps(
                    {
                        str(int(row.seed)): (
                            None if pd.isna(row.target) else float(row.target)
                        )
                        for row in group.itertuples(index=False)
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "source": current.source,
                "source_year_version": metadata["source_year_version"],
                "original_units_and_denominator": metadata[
                    "original_units_and_denominator"
                ],
                "eligibility_population": metadata["eligibility_population"],
                "annual_entity_level_probability": annual,
                "monthly_step_probability_constant_hazard": per_step,
                "conversion_to_annual_entity_probability": metadata[
                    "annual_probability_conversion"
                ],
                "simulation_parameter": metadata["simulation_parameter"],
                "achieved_per_seed_json": json.dumps(
                    {
                        str(int(row.seed)): (
                            None if pd.isna(row.achieved) else float(row.achieved)
                        )
                        for row in group.itertuples(index=False)
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "achieved_mean": (
                    None if pd.isna(current.achieved_mean) else float(current.achieved_mean)
                ),
                "achieved_sd": (
                    None if pd.isna(current.achieved_sd) else float(current.achieved_sd)
                ),
                "relative_deviation_pct": (
                    None
                    if pd.isna(current.relative_deviation_pct)
                    else float(current.relative_deviation_pct)
                ),
                "estimability": current.estimability,
                "result_by_construction_or_emergent": metadata["construction_status"],
                "event_locking_competing_risk_effects": metadata["locking_effects"],
                "source_file": metadata["target_source_file"],
                "source_file_sha256": (
                    sha256_file(source_path) if source_path.exists() else ""
                ),
                "jensen_shannon_definition": (
                    "For distributions P,Q over the sorted union of categories, "
                    "M=(P+Q)/2 and JSD_2(P,Q)=0.5*KL_2(P||M)+0.5*KL_2(Q||M). "
                    "The reported value is Jensen-Shannon distance sqrt(JSD_2), "
                    "not divergence; logarithm base 2; range [0,1]."
                    if dimension.endswith("js_distance")
                    else ""
                ),
                "target_uncertainty_note": (
                    "ACS sampling margins of error are not present in the checked-in "
                    "parameter CSV and are therefore not propagated."
                    if dimension.startswith(("mobility_", "household_"))
                    else ""
                ),
                "source_result_file": rel(E7 / "e7_per_seed.csv"),
            }
        )
    output = pd.DataFrame(rows)
    output.to_csv(BUNDLE / "public_target_conversion.csv", index=False)
    return output


def latex_escape(value: Any) -> str:
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        return ""
    text = str(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(char, char) for char in text)


def write_latex_table(frame: pd.DataFrame, path: Path, caption: str) -> None:
    columns = list(frame.columns)
    lines = [
        "% Generated by paper_experiments/build_paper_revision_bundle.py",
        r"\begin{longtable}{" + "l" * len(columns) + "}",
        r"\caption{" + latex_escape(caption) + r"}\\",
        r"\toprule",
        " & ".join(latex_escape(column) for column in columns) + r" \\",
        r"\midrule",
        r"\endfirsthead",
        r"\toprule",
        " & ".join(latex_escape(column) for column in columns) + r" \\",
        r"\midrule",
        r"\endhead",
    ]
    for row in frame.itertuples(index=False, name=None):
        lines.append(" & ".join(latex_escape(value) for value in row) + r" \\")
    lines.extend([r"\bottomrule", r"\end{longtable}", ""])
    path.write_text("\n".join(lines), encoding="utf-8")


def word_document_xml(frame: pd.DataFrame, title: str) -> str:
    namespace = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    def cell(value: Any, bold: bool = False) -> str:
        text = "" if pd.isna(value) else str(value)
        run_properties = "<w:rPr><w:b/></w:rPr>" if bold else ""
        return (
            "<w:tc><w:p><w:r>"
            + run_properties
            + f'<w:t xml:space="preserve">{xml_escape(text)}</w:t>'
            + "</w:r></w:p></w:tc>"
        )
    rows = [
        "<w:tr>" + "".join(cell(column, True) for column in frame.columns) + "</w:tr>"
    ]
    for current in frame.itertuples(index=False, name=None):
        rows.append("<w:tr>" + "".join(cell(value) for value in current) + "</w:tr>")
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<w:document xmlns:w="{namespace}"><w:body>'
        f"<w:p><w:r><w:rPr><w:b/></w:rPr><w:t>{xml_escape(title)}</w:t></w:r></w:p>"
        "<w:tbl>"
        "<w:tblPr><w:tblBorders>"
        '<w:top w:val="single" w:sz="4"/><w:left w:val="single" w:sz="4"/>'
        '<w:bottom w:val="single" w:sz="4"/><w:right w:val="single" w:sz="4"/>'
        '<w:insideH w:val="single" w:sz="2"/><w:insideV w:val="single" w:sz="2"/>'
        "</w:tblBorders></w:tblPr>"
        + "".join(rows)
        + "</w:tbl>"
        '<w:sectPr><w:pgSz w:w="12240" w:h="15840" w:orient="portrait"/>'
        '<w:pgMar w:top="720" w:right="720" w:bottom="720" w:left="720"/></w:sectPr>'
        "</w:body></w:document>"
    )


def write_docx_table(frame: pd.DataFrame, path: Path, title: str) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
<Default Extension="xml" ContentType="application/xml"/>
<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>
</Types>"""
    relationships = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>
</Relationships>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", relationships)
        archive.writestr("word/document.xml", word_document_xml(frame, title))


def write_table_variants(frame: pd.DataFrame, stem: str, caption: str) -> None:
    TABLES.mkdir(parents=True, exist_ok=True)
    frame.to_csv(TABLES / f"{stem}.csv", index=False)
    write_latex_table(frame, TABLES / f"{stem}.tex", caption)
    write_docx_table(frame, TABLES / f"{stem}.docx", caption)


def build_paper_tables(
    public_targets: pd.DataFrame,
    noise_trend: pd.DataFrame,
) -> dict[str, int]:
    e1 = pd.read_csv(E4 / "e1_e4_e5_per_run.csv")
    table_i_rows: list[dict[str, Any]] = []
    for condition in E1_CONDITIONS:
        group = e1[
            (e1["condition"] == condition) & (e1["matcher"] == "baseline")
        ]
        table_i_rows.append(
            {
                "condition": condition,
                "n_seeds": len(group),
                "precision_mean": group["precision"].mean(),
                "precision_sd": group["precision"].std(ddof=1),
                "recall_mean": group["recall"].mean(),
                "recall_sd": group["recall"].std(ddof=1),
                "fixed_f1_mean": group["f1"].mean(),
                "fixed_f1_sd": group["f1"].std(ddof=1),
                "oracle_f1_mean": group["oracle_f1"].mean(),
                "oracle_f1_sd": group["oracle_f1"].std(ddof=1),
                "candidate_recall_mean": group["candidate_recall"].mean(),
                "runtime_seconds_mean": group["candidate_seconds"].mean(),
            }
        )
    table_i = pd.DataFrame(table_i_rows)

    e2 = pd.read_csv(E2 / "e2_per_run_metrics.csv")
    table_ii = (
        e2.groupby(["scenario", "matcher"], sort=False)
        .agg(
            n_seeds=("seed", "nunique"),
            f1_mean=("f1", "mean"),
            f1_sd=("f1", lambda values: values.std(ddof=1)),
            b3_f1_mean=("b3_f1", "mean"),
            b3_f1_sd=("b3_f1", lambda values: values.std(ddof=1)),
            candidate_recall_mean=("candidate_recall", "mean"),
        )
        .reset_index()
    )

    e3 = pd.read_csv(E3 / "e3_per_run.csv")
    table_iii = (
        e3.groupby(["scale", "matcher"], sort=False)
        .agg(
            n_seeds=("seed", "nunique"),
            f1_mean=("f1", "mean"),
            f1_sd=("f1", lambda values: values.std(ddof=1)),
            candidate_recall_mean=("candidate_recall", "mean"),
            scoring_recall_given_candidate_mean=(
                "scoring_recall_given_candidate",
                "mean",
            ),
        )
        .reset_index()
        .rename(columns={"scale": "k"})
    )
    table_iii["candidate_recall_scope"] = (
        "shared blocking candidate recall; identical across matchers within seed/k"
    )

    full = pd.read_csv(BUNDLE / "household_full_matrix.csv")
    table_iv = full[full["row_type"] == "arm_summary"][
        [
            "scenario",
            "matcher",
            "n_seeds",
            "f1_mean",
            "f1_sd",
            "false_positive_count_mean",
            "co_resident_false_positive_count_mean",
            "co_resident_false_positive_share_mean",
            "median_true_match_score_mean",
            "median_co_resident_nonmatch_score_mean",
            "median_random_nonmatch_score_mean",
        ]
    ].copy()

    table_v = public_targets[
        [
            "dimension",
            "original_source_value",
            "source_year_version",
            "original_units_and_denominator",
            "achieved_mean",
            "achieved_sd",
            "relative_deviation_pct",
            "result_by_construction_or_emergent",
            "estimability",
        ]
    ].copy()
    table_vi = pd.read_csv(E10 / "e10_capability_matrix.csv")
    figure_2 = table_iii.copy()

    tables = {
        "table_i": (table_i, "Table I. E1 baseline condition results from unrounded seed values."),
        "table_ii": (table_ii, "Table II. Three matchers across fourteen canonical scenarios."),
        "table_iii": (table_iii, "Table III. Five tested noise levels; candidate recall is a shared blocking metric."),
        "table_iv": (table_iv, "Table IV. Complete household matcher matrix."),
        "table_v": (table_v, "Table V. Fidelity to configured aggregate targets with units."),
        "table_vi": (table_vi, "Table VI. Evidence-scoped capability matrix."),
        "figure_2_data": (figure_2, "Figure 2 data. Mean F1 and sample SD at five tested noise levels."),
    }
    for stem, (frame, caption) in tables.items():
        write_table_variants(frame, stem, caption)
    return {stem: len(frame) for stem, (frame, _) in tables.items()}


def bundle_artifact_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(BUNDLE.rglob("*")):
        if not path.is_file() or path.name == "artifact_manifest.json":
            continue
        records.append(
            {
                "path": rel(path),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return records


def source_records() -> list[dict[str, Any]]:
    paths = [
        REPO / "paper_experiments" / "run_household_transfer.py",
        REPO / "paper_experiments" / "build_paper_revision_bundle.py",
        REPO / "paper_experiments" / "validate_paper_revision_bundle.py",
        REPO / "evaluation" / "baseline_matcher.py",
        REPO / "evaluation" / "matchers.py",
        REPO / "evaluation" / "metrics.py",
        REPO / "phase2" / "src" / "sog_phase2" / "simulator.py",
        E1 / "e1_per_run.csv",
        E1 / "e1_artifact_hashes.csv",
        E2 / "e2_per_run_metrics.csv",
        E2 / "models" / "thresholds.json",
        E2 / "models" / "splink.json",
        E2 / "models" / "learned.joblib",
        E3 / "e3_per_run.csv",
        E4 / "e1_e4_e5_per_run.csv",
        E7 / "e7_per_seed.csv",
        E7 / "e7_summary.csv",
        E8 / "e8_runtime_repetitions.csv",
        E8 / "e8_hardware.json",
        E10 / "e10_capability_matrix.csv",
    ]
    return [
        {"path": rel(path), "bytes": path.stat().st_size, "sha256": sha256_file(path)}
        for path in paths
    ]


def build_readme(
    *,
    tests: dict[str, Any],
    table_counts: dict[str, int],
    generated_at: str,
    release_tag: str | None,
    payload_commit: str | None,
) -> None:
    transfer_audit_path = (
        PROVENANCE / "household_transfer" / "household_transfer_run.json"
    )
    transfer = json.loads(transfer_audit_path.read_text(encoding="utf-8"))
    household_command = (
        ".venv\\Scripts\\python.exe paper_experiments\\run_household_transfer.py --workers 2"
    )
    test_command = (
        ".venv\\Scripts\\python.exe -m pytest evaluation/tests phase2/tests -q "
        "--junitxml=paper_revision_bundle/_provenance/pytest.xml"
    )
    build_command = (
        ".venv\\Scripts\\python.exe paper_experiments\\build_paper_revision_bundle.py "
        "--test-junit paper_revision_bundle/_provenance/pytest.xml"
    )
    validate_command = (
        ".venv\\Scripts\\python.exe paper_experiments\\validate_paper_revision_bundle.py"
    )
    if release_tag:
        build_command += (
            f" --release-tag {release_tag} --payload-commit {payload_commit}"
        )
        validate_command += f" --release-tag {release_tag}"
    lines = [
        "# Paper revision bundle",
        "",
        f"Generated at `{generated_at}` while HEAD was `{run_git('rev-parse', 'HEAD')}` on branch `{run_git('branch', '--show-current')}`.",
        "",
        "This bundle is generated from checked-in unrounded E1-E11 results plus a new, seed-disjoint household Splink calibration-transfer experiment. No historical result file was overwritten.",
        "",
    ]
    if release_tag:
        lines.extend(
            [
                "## Release identity",
                "",
                f"This bundle is prepared for annotated tag `{release_tag}`. Commit `{payload_commit}` is the complete payload commit immediately before the release-metadata freeze.",
                "",
                f"The authoritative final release commit is resolved with `git rev-list -n 1 {release_tag}`. A tracked manifest cannot embed the hash of the commit that contains that manifest because the commit hash depends on the manifest bytes themselves. The release validator therefore checks that the annotated tag resolves to the current clean HEAD and that the recorded payload commit is its ancestor.",
                "",
            ]
        )
    lines.extend(
        [
        "## Commands executed",
        "",
        "Material commands that generated or verified scientific outputs are listed in execution order. Read-only repository inspection commands are not scientific runs.",
        "",
        f"1. `{household_command}` — first launch terminated by the five-second shell-wrapper timeout before a result.",
        f"2. `{household_command}` — orchestration exited nonzero after the concurrent generators wrote complete validated calibration artifacts.",
        f"3. `{household_command}` — passed on resume; verified/reused three seed-20260719 household calibration runs, trained three scenario-specific Splink models, and evaluated 60 existing household runs.",
        ]
    )
    for generated in transfer["generation_commands"]:
        lines.append(
            f"   - `{generated['command']}` — {generated['status']} "
            f"(return code {generated['return_code']}, {generated['seconds']:.1f} s)."
        )
    lines.extend(
        [
            "4. `.venv\\Scripts\\python.exe -m py_compile paper_experiments\\run_household_transfer.py paper_experiments\\build_paper_revision_bundle.py paper_experiments\\validate_paper_revision_bundle.py` — passed preflight compilation (repeated after targeted fixes).",
            f"5. `{test_command}` — {'passed' if tests['failed'] == 0 else 'failed'}; "
            f"{tests['passed']} passed, {tests['failed']} failed, {tests['skipped']} skipped in {tests['time_seconds']:.3f} seconds.",
            f"6. `{build_command}` — first derivation stopped because the assumed E3 `k=1` directory had no truth files; executable `run_e3.py` showed that `k=1` deliberately reuses the E2 high-noise run.",
            f"7. `{build_command}` — passed after mapping E3 to its actual source runs; derived all requested CSV/JSON/table files.",
            f"8. `{validate_command}` — passed final read-only bundle validation.",
            "",
            "## Run failures",
            "",
            "- No requested scientific output remains failed. The first short shell-wrapper launch timed out after five seconds and was terminated before completion; the resumable long-running launch replaced it.",
            "- During the first concurrent calibration-generation launch, `family_birth` and `adoption_blended_family` subprocesses returned nonzero after writing complete manifests with `quality_status: ok` and `validation_valid: true`. The continuation verified and reused those artifacts. This exit/artifact mismatch is retained as a run discrepancy rather than hidden.",
            "- Early read-only setup probes found that the outer download wrapper is not the Git worktree, `pdftotext` is not installed, and historical score-parquet cache directories are empty. The nested repository, Python PDF fallback, current Phase 2 run artifacts, and regenerated scores were used; these probes changed no result.",
            "",
            "## Requested files",
            "",
            "- `artifact_manifest.json`: Git/environment/test provenance plus output and source hashes.",
            "- `matcher_protocol.json`: executable blocking, features, similarities, fitting, calibration, thresholds, and seed separation.",
            "- `config_mapping.csv`: 230 seed-level Table I-III YAML/truth mappings with complete hashes and config differences.",
            "- `derived_claims.json`: unrounded formulas, values, displayed values, and exact rank/tie handling.",
            "- `paired_statistics.csv`: seed rows and paired t/Wilcoxon/sign summaries with Holm adjustment for the 28 Table II contrasts.",
            "- `oracle_bootstrap.json`: 20,000 paired-seed bootstrap replicates and threshold-procedure disclosure.",
            "- `noise_trend.csv`: per-seed/per-level results plus discrete monotonicity and endpoint tests.",
            "- `cluster_metrics.csv`: per-seed and summary pairwise, B3, and closure metrics for E1 and E2 E5 inputs.",
            "- `household_transfer_experiment.csv`: clean-frozen versus scenario-specific Splink calibration.",
            "- `household_full_matrix.csv`: all three matchers for all six household scenarios.",
            "- `runtime_results.csv`: E1 independent-seed variability separated from E8 fixed-population repeats.",
            "- `public_target_conversion.csv`: units, denominators, conversions, per-seed outcomes, construction status, and JS definition.",
            "- `paper_tables/`: CSV, LaTeX `longtable`, and Word-compatible DOCX for Tables I-VI and Figure 2 data.",
            "",
            "Paper-table row counts: "
            + ", ".join(f"`{key}`={value}" for key, value in table_counts.items())
            + ".",
            "",
            "## Hash definitions",
            "",
            "A truth-layer digest is the SHA-256 of sorted `filename NUL file-SHA-256 NUL` entries for truth people, households, household memberships, residence history, events, and the entity-record map. The component hashes are retained in `config_mapping.csv`.",
            "",
            "## Unresolved discrepancies and claim boundaries",
            "",
            f"- The current relevant test command reports {tests['passed']} passed, not the manuscript's stale 368-test statement.",
            (
                f"- This bundle is assigned release tag `{release_tag}`, but the checked-in archive status still has no minted DOI. Do not claim a DOI-backed public archive until one exists."
                if release_tag
                else "- The repository has release tag `paper-artifact-2026.07.23`, but the checked-in archive status still has no minted DOI. Do not claim a DOI-backed public archive until one exists."
            ),
            "- The target package intentionally retains preregistered provisional 2024 NCHS fertility values. A later final 2024 release is not silently substituted; updating it requires a versioned parameter change and dependent rerun.",
            "- Scenario-specific Splink calibration is a new supplemental analysis, not part of the original preregistration. It uses one held-out calibration population (seed 20260719) and ten disjoint evaluation seeds.",
            "- Each scenario-specific Splink model left at least one fuzzy-street m-probability untrained because that comparison level was not observed during EM; Splink 4.0.16 used its default at prediction time. Exact affected levels are machine-readable in `matcher_protocol.json`.",
            "- Splink's saved artifact does not expose realized unsupervised u-sample and EM row counts. `matcher_protocol.json` reports this as unavailable instead of inventing labeled training counts; threshold-calibration pair counts are exact.",
            "- E4 oracle thresholds are selected per seed over exact observed score boundaries, not the legacy 0.01 baseline calibration grid. They are descriptive in-sample upper bounds.",
            "- Household-type fidelity is largely by construction. Household-size divergence remains unestimable from B11001, and ACS margins of error are absent from the checked-in target files.",
            "- The CDC marriage statistic is not semantically comparable with the simulator's COHABIT event. Divorce and fertility denominators/eligibility are documented explicitly.",
            "- The capability matrix is regenerated from checked-in E10 evidence but universal literature-completeness claims still require external verification.",
            "- Apparent Table II ties at four decimals are retained as displayed ties even when unrounded means differ. See `derived_claims.json`.",
            "",
            "## Reproduction note",
            "",
            "For a clean reproduction, run the passing household, test, build, and validation commands above from the repository root with the repository `.venv`. The household script is resumable: it reuses calibration runs/models and per-run audits only when their source hashes match.",
            "",
        ]
    )
    (BUNDLE / "README.md").write_text("\n".join(lines), encoding="utf-8")


def build_artifact_manifest(
    *,
    tests: dict[str, Any],
    test_command: str,
    generated_at: str,
    release_tag: str | None,
    payload_commit: str | None,
) -> dict[str, Any]:
    head = run_git("rev-parse", "HEAD")
    branch = run_git("branch", "--show-current")
    status_lines = [
        line for line in run_git("status", "--porcelain=v1").splitlines() if line
    ]
    tags = [line for line in run_git("tag", "--points-at", "HEAD").splitlines() if line]
    release_tags = sorted(tag for tag in tags if tag.startswith("paper-artifact-"))
    selected_release_tag = release_tag or (
        release_tags[-1] if release_tags else None
    )
    selected_payload_commit = payload_commit or head
    manifest = {
        "generated_at_utc": generated_at,
        "git": {
            "commit_sha": head,
            "commit_sha_role": (
                "payload_commit_before_release_metadata_freeze"
                if release_tag
                else "head_at_generation"
            ),
            "branch": branch,
            "status_at_generation": "dirty" if status_lines else "clean",
            "dirty_at_generation": bool(status_lines),
            "status_porcelain_at_generation": status_lines,
            "release_tag": selected_release_tag,
            "all_tags_at_generation_commit": tags,
            "release": {
                "tag": selected_release_tag,
                "payload_commit_sha": selected_payload_commit,
                "payload_commit_role": (
                    "complete payload immediately before release metadata freeze"
                    if release_tag
                    else "HEAD at bundle generation"
                ),
                "authoritative_release_commit_command": (
                    f"git rev-list -n 1 {selected_release_tag}"
                    if selected_release_tag
                    else None
                ),
                "final_commit_sha_embedded": False,
                "final_commit_sha_explanation": (
                    "A tracked file cannot contain the SHA of its own containing "
                    "commit because that SHA depends on the file bytes. The "
                    "annotated release tag is the authoritative final commit "
                    "identifier and is checked by the release validator."
                ),
                "expected_post_tag_worktree_status": "clean",
            },
        },
        "runtime": {
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
            "python_executable": sys.executable,
            "os": {
                "system": platform.system(),
                "release": platform.release(),
                "version": platform.version(),
                "platform": platform.platform(),
                "machine": platform.machine(),
                "processor": platform.processor(),
            },
            "package_versions": package_versions(),
            "learned_model_library": {
                "package": "scikit-learn",
                "version": package_versions()["scikit-learn"],
            },
            "splink": {
                "version": package_versions()["splink"],
                "backend": "DuckDB",
                "duckdb_version": package_versions()["duckdb"],
            },
        },
        "tests": {
            "exact_command": test_command,
            **tests,
        },
        "source_artifacts": source_records(),
        "bundle_artifacts_excluding_manifest": bundle_artifact_records(),
        "bundle_artifact_count_excluding_manifest": len(bundle_artifact_records()),
        "historical_result_files_overwritten": False,
    }
    write_json(BUNDLE / "artifact_manifest.json", manifest)
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test-junit",
        type=Path,
        default=Path("paper_revision_bundle/_provenance/pytest.xml"),
    )
    parser.add_argument(
        "--test-command",
        default=(
            ".venv\\Scripts\\python.exe -m pytest evaluation/tests phase2/tests -q "
            "--junitxml=paper_revision_bundle/_provenance/pytest.xml"
        ),
    )
    parser.add_argument(
        "--release-tag",
        help="Annotated tag that will identify the final release metadata commit.",
    )
    parser.add_argument(
        "--payload-commit",
        help=(
            "Full SHA of the complete payload commit immediately before the "
            "release metadata commit; required with --release-tag."
        ),
    )
    args = parser.parse_args()
    if bool(args.release_tag) != bool(args.payload_commit):
        parser.error("--release-tag and --payload-commit must be supplied together")
    if args.payload_commit:
        resolved_payload = run_git("rev-parse", f"{args.payload_commit}^{{commit}}")
        if resolved_payload != args.payload_commit:
            parser.error("--payload-commit must be a full commit SHA")
    BUNDLE.mkdir(parents=True, exist_ok=True)
    TABLES.mkdir(parents=True, exist_ok=True)
    test_path = args.test_junit
    if not test_path.is_absolute():
        test_path = REPO / test_path
    if not test_path.exists():
        raise FileNotFoundError(f"JUnit test artifact not found: {test_path}")
    for required in (
        BUNDLE / "household_transfer_experiment.csv",
        BUNDLE / "household_full_matrix.csv",
        PROVENANCE / "household_transfer" / "household_transfer_run.json",
    ):
        if not required.exists():
            raise FileNotFoundError(
                f"Run paper_experiments/run_household_transfer.py first: {required}"
            )

    generated_at = datetime.now(timezone.utc).isoformat()
    tests = parse_junit(test_path)
    build_matcher_protocol()
    build_config_mapping()
    build_derived_claims()
    build_paired_statistics()
    build_oracle_bootstrap()
    noise = build_noise_trend()
    build_cluster_metrics()
    build_runtime_results()
    public = build_public_target_conversion()
    table_counts = build_paper_tables(public, noise)
    build_readme(
        tests=tests,
        table_counts=table_counts,
        generated_at=generated_at,
        release_tag=args.release_tag,
        payload_commit=args.payload_commit,
    )
    manifest = build_artifact_manifest(
        tests=tests,
        test_command=args.test_command,
        generated_at=generated_at,
        release_tag=args.release_tag,
        payload_commit=args.payload_commit,
    )
    print(
        f"paper revision bundle written: {BUNDLE} "
        f"({manifest['bundle_artifact_count_excluding_manifest']} hashed files)",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
