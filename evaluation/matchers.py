"""Three frozen matcher families used by E2-E6."""

from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from evaluation.baseline_matcher import normalize
from evaluation.metrics import FEATURES, RunData, threshold_curve, oracle_from_curve

MATCHERS = ("baseline", "splink", "learned")


def select_threshold(scores: np.ndarray, labels: np.ndarray, true_count: int) -> float:
    return oracle_from_curve(threshold_curve(scores, labels, true_count))["oracle_threshold"]


def learned_matrix(candidates: pd.DataFrame) -> np.ndarray:
    base = candidates.loc[:, FEATURES].to_numpy(dtype=float)
    missing_proxy = (base == 0.0).astype(float)
    return np.column_stack([base, missing_proxy])


def train_learned(candidates: pd.DataFrame, output_path: Path) -> Pipeline:
    model = Pipeline([
        ("scale", StandardScaler()),
        ("classifier", LogisticRegression(class_weight="balanced", max_iter=2000, random_state=20260719)),
    ])
    model.fit(learned_matrix(candidates), candidates.is_match.astype(int).to_numpy())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, output_path)
    return model


def score_learned(model: Pipeline, candidates: pd.DataFrame) -> np.ndarray:
    return model.predict_proba(learned_matrix(candidates))[:, 1]


def splink_input(run: RunData) -> pd.DataFrame:
    from evaluation.baseline_matcher import prepare

    rows: list[pd.DataFrame] = []
    for source, frame in run.datasets.items():
        values = prepare(frame, source)
        rows.append(pd.DataFrame({
            "unique_id": [f"{source}::{key}" for key in values["keys"]],
            "source_dataset": source,
            "first": values["first"], "last": values["last"],
            "dob_norm": values["dob_norm"], "street": values["street"],
            "city": values["city"], "state": values["state"], "postal": values["zip"],
            "sx_first": values["sx_first"], "sx_last": values["sx_last"],
            "first_initial": [value[:1] for value in values["first"]],
            "birth_year": values["birth_year"],
        }))
    return pd.concat(rows, ignore_index=True)


def _splink_settings():
    from splink import SettingsCreator
    import splink.comparison_library as cl
    from splink.blocking_rule_library import CustomRule

    rules = [
        CustomRule("l.dob_norm = r.dob_norm AND l.dob_norm <> ''"),
        CustomRule("l.sx_last = r.sx_last AND l.first_initial = r.first_initial AND l.sx_last <> '' AND l.first_initial <> ''"),
        CustomRule("l.sx_first = r.sx_first AND l.sx_last = r.sx_last AND l.sx_first <> '' AND l.sx_last <> ''"),
        CustomRule("l.postal = r.postal AND l.sx_last = r.sx_last AND l.postal <> '' AND l.sx_last <> ''"),
        CustomRule("l.birth_year = r.birth_year AND l.sx_last = r.sx_last AND l.birth_year <> '' AND l.sx_last <> ''"),
    ]
    return SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
        source_dataset_column_name="source_dataset",
        comparisons=[
            cl.JaroWinklerAtThresholds("first", [0.92, 0.80]),
            cl.JaroWinklerAtThresholds("last", [0.92, 0.80]),
            # The clean calibration condition contains essentially no near-date
            # examples. An exact DOB comparison avoids silently retaining
            # Splink defaults for unobserved fuzzy DOB levels.
            cl.ExactMatch("dob_norm"),
            cl.JaroWinklerAtThresholds("street", [0.92, 0.75]),
            # City/state are strongly nested with postal in this synthetic
            # population and caused non-identifiable EM oscillation. Postal
            # retains the location signal without three redundant exact terms.
            cl.ExactMatch("postal"),
        ],
        blocking_rules_to_generate_predictions=rules,
        retain_matching_columns=False,
        retain_intermediate_calculation_columns=False,
        probability_two_random_records_match=0.001,
        max_iterations=100,
    )


def train_splink(run: RunData, model_path: Path) -> None:
    from splink import DuckDBAPI, Linker
    from splink.blocking_rule_library import CustomRule

    linker = Linker(splink_input(run), _splink_settings(), db_api=DuckDBAPI())
    linker.training.estimate_u_using_random_sampling(max_pairs=10_000_000, seed=20260719)
    linker.training.estimate_parameters_using_expectation_maximisation(
        CustomRule("l.dob_norm = r.dob_norm AND l.dob_norm <> ''"),
        fix_probability_two_random_records_match=True,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        CustomRule("l.postal = r.postal AND l.postal <> ''"),
        fix_probability_two_random_records_match=True,
    )
    model_path.parent.mkdir(parents=True, exist_ok=True)
    linker.misc.save_model_to_json(str(model_path), overwrite=True)


def score_splink(run: RunData, candidates: pd.DataFrame, model_path: Path) -> np.ndarray:
    from splink import DuckDBAPI, Linker

    linker = Linker(splink_input(run), str(model_path), db_api=DuckDBAPI())
    predicted = linker.inference.predict().as_pandas_dataframe()
    left = predicted["unique_id_l"].astype(str)
    right = predicted["unique_id_r"].astype(str)
    predicted["node_l"] = np.where(left < right, left, right)
    predicted["node_r"] = np.where(left < right, right, left)
    lookup = predicted.set_index(["node_l", "node_r"])["match_probability"]
    keys = pd.MultiIndex.from_frame(candidates[["node_l", "node_r"]])
    missing = keys.difference(lookup.index)
    if len(missing):
        raise RuntimeError(f"Splink omitted {len(missing)} shared-blocking candidates")
    return lookup.reindex(keys).to_numpy(dtype=float)
