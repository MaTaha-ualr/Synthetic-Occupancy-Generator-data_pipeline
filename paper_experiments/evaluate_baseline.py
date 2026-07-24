from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


SCENARIOS = (
    "paper_clean",
    "paper_high_noise",
    "paper_low_overlap",
    "paper_one_to_many",
)
SEED = 20260720
RUN_DATE = "2026-07-20"


@dataclass
class ScenarioMetrics:
    scenario: str
    run_id: str
    rows_a: int
    rows_b: int
    entities_a: int
    entities_b: int
    true_links: int
    candidate_pairs: int
    total_possible_pairs: int
    candidate_recall: float
    candidate_reduction_ratio: float
    threshold: float
    predicted_links: int
    true_positives: int
    false_positives: int
    false_negatives: int
    precision: float
    recall: float
    f1: float
    evaluation_seconds: float
    dataset_a_sha256: str
    dataset_b_sha256: str
    entity_map_sha256: str
    truth_signature_sha256: str


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def normalize(value: object) -> str:
    return re.sub(r"[^A-Z0-9]", "", _text(value).upper())


def soundex(value: object) -> str:
    text = re.sub(r"[^A-Z]", "", _text(value).upper())
    if not text:
        return ""
    first = text[0]
    groups = {
        **{c: "1" for c in "BFPV"},
        **{c: "2" for c in "CGJKQSXZ"},
        **{c: "3" for c in "DT"},
        "L": "4",
        **{c: "5" for c in "MN"},
        "R": "6",
    }
    encoded: list[str] = []
    previous = groups.get(first, "")
    for char in text[1:]:
        code = groups.get(char, "")
        if code and code != previous:
            encoded.append(code)
        previous = code
    return (first + "".join(encoded) + "000")[:4]


def bigrams(value: str) -> frozenset[str]:
    if not value:
        return frozenset()
    if len(value) == 1:
        return frozenset({value})
    return frozenset(value[i : i + 2] for i in range(len(value) - 1))


def dice(left: frozenset[str], right: frozenset[str]) -> float:
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    return (2.0 * len(left & right)) / (len(left) + len(right))


def normalize_date(value: object) -> str:
    text = _text(value)
    if not text:
        return ""
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        digits = re.sub(r"\D", "", text)
        return digits
    return parsed.strftime("%Y%m%d")


def date_parts(value: object) -> tuple[int, int, int] | None:
    text = _text(value)
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return int(parsed.year), int(parsed.month), int(parsed.day)


def dob_similarity(left: tuple[int, int, int] | None, right: tuple[int, int, int] | None) -> float:
    if left is None or right is None:
        return 0.0
    if left == right:
        return 1.0
    ly, lm, ld = left
    ry, rm, rd = right
    if ly == ry and lm == rd and ld == rm:
        return 0.85
    if ly == ry and (lm == rm or ld == rd):
        return 0.65
    if ly == ry:
        return 0.35
    return 0.0


def find_column(frame: pd.DataFrame, candidates: Iterable[str], *, required: bool = False) -> str | None:
    lowered = {str(column).lower(): str(column) for column in frame.columns}
    for candidate in candidates:
        if candidate in frame.columns:
            return candidate
        match = lowered.get(candidate.lower())
        if match:
            return match
    if required:
        raise ValueError(f"Missing required column. Tried: {', '.join(candidates)}")
    return None


def record_key_column(frame: pd.DataFrame, side: str) -> str:
    return find_column(
        frame,
        ("RecordKey", f"{side}_RecordKey"),
        required=True,
    )  # type: ignore[return-value]


def series_or_blank(frame: pd.DataFrame, candidates: Iterable[str]) -> pd.Series:
    column = find_column(frame, candidates)
    if column is None:
        return pd.Series([""] * len(frame), index=frame.index, dtype="object")
    return frame[column].fillna("").astype(str)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def prepare(frame: pd.DataFrame, side: str) -> dict[str, list]:
    key_col = record_key_column(frame, side)
    first = series_or_blank(frame, ("FirstName", "FormalFirstName")).map(normalize).tolist()
    last = series_or_blank(frame, ("LastName",)).map(normalize).tolist()
    dob_raw = series_or_blank(frame, ("DOB", "DateOfBirth"))
    dob_norm = dob_raw.map(normalize_date).tolist()
    dob_parts_values = dob_raw.map(date_parts).tolist()
    street = series_or_blank(frame, ("StreetAddress", "Address")).map(normalize).tolist()
    city = series_or_blank(frame, ("City",)).map(normalize).tolist()
    state = series_or_blank(frame, ("State",)).map(normalize).tolist()
    zip_code = series_or_blank(frame, ("ZipCode", "ZIP", "PostalCode")).map(normalize).tolist()

    return {
        "keys": frame[key_col].fillna("").astype(str).str.strip().tolist(),
        "first": first,
        "last": last,
        "dob_norm": dob_norm,
        "dob_parts": dob_parts_values,
        "street": street,
        "city": city,
        "state": state,
        "zip": zip_code,
        "first_bigrams": [bigrams(value) for value in first],
        "last_bigrams": [bigrams(value) for value in last],
        "street_bigrams": [bigrams(value) for value in street],
        "sx_first": [soundex(value) for value in first],
        "sx_last": [soundex(value) for value in last],
        "birth_year": [value[:4] if len(value) >= 4 else "" for value in dob_norm],
    }


def blocking_keys(data: dict[str, list], idx: int) -> tuple[str, ...]:
    first = data["first"][idx]
    last = data["last"][idx]
    dob = data["dob_norm"][idx]
    year = data["birth_year"][idx]
    zip_code = data["zip"][idx]
    sx_first = data["sx_first"][idx]
    sx_last = data["sx_last"][idx]
    first_initial = first[:1]

    keys: list[str] = []
    if dob:
        keys.append(f"D:{dob}")
    if sx_last and first_initial:
        keys.append(f"N:{sx_last}:{first_initial}")
    if sx_first and sx_last:
        keys.append(f"P:{sx_first}:{sx_last}")
    if zip_code and sx_last:
        keys.append(f"Z:{zip_code}:{sx_last}")
    if year and sx_last:
        keys.append(f"Y:{year}:{sx_last}")
    return tuple(keys)


def build_true_pairs(entity_map: pd.DataFrame) -> tuple[set[tuple[str, str]], int, int]:
    person_col = find_column(entity_map, ("PersonKey", "EntityKey"), required=True)
    dataset_col = find_column(entity_map, ("DatasetId",), required=True)
    record_col = find_column(entity_map, ("RecordKey",), required=True)

    work = entity_map[[person_col, dataset_col, record_col]].fillna("").astype(str)
    work.columns = ["person", "dataset", "record"]
    work["person"] = work["person"].str.strip()
    work["dataset"] = work["dataset"].str.strip()
    work["record"] = work["record"].str.strip()
    work = work[(work["person"] != "") & (work["record"] != "")]

    true_pairs: set[tuple[str, str]] = set()
    entities_a: set[str] = set()
    entities_b: set[str] = set()

    for person, group in work.groupby("person", sort=False):
        a_records = sorted(set(group.loc[group["dataset"] == "A", "record"]))
        b_records = sorted(set(group.loc[group["dataset"] == "B", "record"]))
        if a_records:
            entities_a.add(person)
        if b_records:
            entities_b.add(person)
        for a_key in a_records:
            for b_key in b_records:
                true_pairs.add((a_key, b_key))

    return true_pairs, len(entities_a), len(entities_b)


def pair_score(a: dict[str, list], ai: int, b: dict[str, list], bi: int) -> float:
    first = dice(a["first_bigrams"][ai], b["first_bigrams"][bi])
    last = dice(a["last_bigrams"][ai], b["last_bigrams"][bi])
    dob = dob_similarity(a["dob_parts"][ai], b["dob_parts"][bi])
    street = dice(a["street_bigrams"][ai], b["street_bigrams"][bi])
    city = 1.0 if a["city"][ai] and a["city"][ai] == b["city"][bi] else 0.0
    state = 1.0 if a["state"][ai] and a["state"][ai] == b["state"][bi] else 0.0
    zip_score = 1.0 if a["zip"][ai] and a["zip"][ai] == b["zip"][bi] else 0.0

    return (
        0.20 * first
        + 0.25 * last
        + 0.20 * dob
        + 0.15 * street
        + 0.05 * city
        + 0.05 * state
        + 0.10 * zip_score
    )


def score_candidates(
    dataset_a: pd.DataFrame,
    dataset_b: pd.DataFrame,
    true_pairs: set[tuple[str, str]],
) -> tuple[np.ndarray, np.ndarray, float]:
    started = time.perf_counter()
    a = prepare(dataset_a, "A")
    b = prepare(dataset_b, "B")

    b_index: dict[str, list[int]] = defaultdict(list)
    for bi in range(len(dataset_b)):
        for key in blocking_keys(b, bi):
            b_index[key].append(bi)

    scores: list[float] = []
    labels: list[bool] = []

    for ai in range(len(dataset_a)):
        candidates: set[int] = set()
        for key in blocking_keys(a, ai):
            candidates.update(b_index.get(key, ()))
        a_key = a["keys"][ai]
        for bi in candidates:
            b_key = b["keys"][bi]
            scores.append(pair_score(a, ai, b, bi))
            labels.append((a_key, b_key) in true_pairs)

    elapsed = time.perf_counter() - started
    return np.asarray(scores, dtype=np.float32), np.asarray(labels, dtype=bool), elapsed


def metrics_at_threshold(
    scores: np.ndarray,
    labels: np.ndarray,
    threshold: float,
    true_link_count: int,
) -> dict[str, float | int]:
    predicted = scores >= threshold
    tp = int(np.logical_and(predicted, labels).sum())
    fp = int(np.logical_and(predicted, ~labels).sum())
    fn = int(true_link_count - tp)
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / true_link_count if true_link_count else 0.0
    f1 = (2.0 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {
        "predicted_links": int(predicted.sum()),
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def choose_threshold(scores: np.ndarray, labels: np.ndarray, true_link_count: int) -> float:
    candidates = np.round(np.arange(0.30, 0.951, 0.01), 2)
    evaluated = []
    for threshold in candidates:
        metrics = metrics_at_threshold(scores, labels, float(threshold), true_link_count)
        evaluated.append((metrics["f1"], metrics["precision"], float(threshold)))
    return max(evaluated)[2]


def run_dir_for(runs_root: Path, scenario: str) -> Path:
    exact = runs_root / f"{RUN_DATE}_{scenario}_seed{SEED}"
    if exact.exists():
        return exact
    matches = sorted(runs_root.glob(f"*_{scenario}_seed{SEED}"))
    if not matches:
        raise FileNotFoundError(
            f"No run directory found for {scenario}. Expected {exact}. "
            "Run paper_experiments/run_experiments.ps1 first."
        )
    return matches[-1]


def load_scenario_inputs(run_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Path]]:
    paths = {
        "a": run_dir / "DatasetA.csv",
        "b": run_dir / "DatasetB.csv",
        "map": run_dir / "entity_record_map.csv",
        "truth_people": run_dir / "truth_people.parquet",
        "truth_households": run_dir / "truth_households.parquet",
        "truth_memberships": run_dir / "truth_household_memberships.parquet",
        "truth_residence": run_dir / "truth_residence_history.parquet",
        "truth_events": run_dir / "truth_events.parquet",
    }
    for path in paths.values():
        if not path.exists():
            raise FileNotFoundError(f"Missing required experiment artifact: {path}")
    dataset_a = pd.read_csv(paths["a"], dtype=str, keep_default_na=False)
    dataset_b = pd.read_csv(paths["b"], dtype=str, keep_default_na=False)
    entity_map = pd.read_csv(paths["map"], dtype=str, keep_default_na=False)
    return dataset_a, dataset_b, entity_map, paths


def markdown_table(frame: pd.DataFrame) -> str:
    columns = [
        "scenario",
        "rows_a",
        "rows_b",
        "true_links",
        "candidate_recall",
        "candidate_reduction_ratio",
        "precision",
        "recall",
        "f1",
        "evaluation_seconds",
    ]
    display = frame[columns].copy()
    for column in (
        "candidate_recall",
        "candidate_reduction_ratio",
        "precision",
        "recall",
        "f1",
        "evaluation_seconds",
    ):
        display[column] = display[column].map(lambda value: f"{float(value):.4f}")
    headers = list(display.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in display.itertuples(index=False, name=None):
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate a fixed transparent ER baseline across controlled SOG paper scenarios."
    )
    parser.add_argument("--runs-root", type=Path, default=Path("phase2/runs"))
    parser.add_argument("--output-dir", type=Path, default=Path("paper_experiments/results"))
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)

    cached: dict[str, dict] = {}
    for scenario in SCENARIOS:
        run_dir = run_dir_for(args.runs_root, scenario)
        dataset_a, dataset_b, entity_map, paths = load_scenario_inputs(run_dir)
        true_pairs, entities_a, entities_b = build_true_pairs(entity_map)
        scores, labels, elapsed = score_candidates(dataset_a, dataset_b, true_pairs)
        cached[scenario] = {
            "run_dir": run_dir,
            "dataset_a": dataset_a,
            "dataset_b": dataset_b,
            "entities_a": entities_a,
            "entities_b": entities_b,
            "true_pairs": true_pairs,
            "scores": scores,
            "labels": labels,
            "elapsed": elapsed,
            "paths": paths,
        }
        print(
            f"{scenario}: rows=({len(dataset_a)}, {len(dataset_b)}), "
            f"true_links={len(true_pairs)}, candidates={len(scores)}"
        )

    clean = cached["paper_clean"]
    threshold = choose_threshold(
        clean["scores"],
        clean["labels"],
        len(clean["true_pairs"]),
    )
    print(f"Threshold selected once on paper_clean: {threshold:.2f}")

    results: list[ScenarioMetrics] = []
    hash_rows: list[dict[str, str]] = []

    for scenario in SCENARIOS:
        item = cached[scenario]
        dataset_a = item["dataset_a"]
        dataset_b = item["dataset_b"]
        true_pairs = item["true_pairs"]
        scores = item["scores"]
        labels = item["labels"]
        threshold_metrics = metrics_at_threshold(scores, labels, threshold, len(true_pairs))

        candidate_true_links = int(labels.sum())
        candidate_recall = candidate_true_links / len(true_pairs) if true_pairs else 0.0
        total_possible = int(len(dataset_a) * len(dataset_b))
        reduction = 1.0 - (len(scores) / total_possible) if total_possible else 0.0

        paths = item["paths"]
        truth_file_hashes = [
            sha256_file(paths[name])
            for name in (
                "truth_people",
                "truth_households",
                "truth_memberships",
                "truth_residence",
                "truth_events",
            )
        ]
        truth_signature = hashlib.sha256("|".join(truth_file_hashes).encode("utf-8")).hexdigest()
        hashes = {
            "dataset_a_sha256": sha256_file(paths["a"]),
            "dataset_b_sha256": sha256_file(paths["b"]),
            "entity_map_sha256": sha256_file(paths["map"]),
            "truth_signature_sha256": truth_signature,
        }
        hash_rows.append({"scenario": scenario, **hashes})

        results.append(
            ScenarioMetrics(
                scenario=scenario,
                run_id=item["run_dir"].name,
                rows_a=len(dataset_a),
                rows_b=len(dataset_b),
                entities_a=item["entities_a"],
                entities_b=item["entities_b"],
                true_links=len(true_pairs),
                candidate_pairs=len(scores),
                total_possible_pairs=total_possible,
                candidate_recall=candidate_recall,
                candidate_reduction_ratio=reduction,
                threshold=threshold,
                predicted_links=int(threshold_metrics["predicted_links"]),
                true_positives=int(threshold_metrics["true_positives"]),
                false_positives=int(threshold_metrics["false_positives"]),
                false_negatives=int(threshold_metrics["false_negatives"]),
                precision=float(threshold_metrics["precision"]),
                recall=float(threshold_metrics["recall"]),
                f1=float(threshold_metrics["f1"]),
                evaluation_seconds=float(item["elapsed"]),
                **hashes,
            )
        )

    truth_signatures = {result.truth_signature_sha256 for result in results}
    if len(truth_signatures) != 1:
        raise RuntimeError(
            "Controlled experiment truth layers are not identical across scenarios. "
            "Do not use the results until the scenario configurations are corrected."
        )
    print("Verified: all four conditions share the same truth-layer signature.")

    result_frame = pd.DataFrame([asdict(result) for result in results])
    result_frame.to_csv(args.output_dir / "paper_experiment_results.csv", index=False)
    (args.output_dir / "paper_experiment_results.json").write_text(
        json.dumps([asdict(result) for result in results], indent=2),
        encoding="utf-8",
    )
    (args.output_dir / "paper_experiment_results.md").write_text(
        "# Controlled Entity Resolution Baseline Results\n\n"
        f"Decision threshold selected on `paper_clean` and fixed for all scenarios: **{threshold:.2f}**\n\n"
        + markdown_table(result_frame),
        encoding="utf-8",
    )
    pd.DataFrame(hash_rows).to_csv(args.output_dir / "paper_experiment_hashes.csv", index=False)

    print()
    print(result_frame[
        ["scenario", "precision", "recall", "f1", "candidate_recall", "candidate_reduction_ratio"]
    ].to_string(index=False))
    print(f"\nResults written to: {args.output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
