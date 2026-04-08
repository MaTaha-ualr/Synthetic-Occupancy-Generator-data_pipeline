# SOG Worked Example: DataLink ER Benchmark

Last updated: April 4, 2026

This document is a single worked example for a user who wants to benchmark an entity-resolution system end to end.

If you want the full canonical scenario catalog and the guidance for choosing between the shipped linkage and dedup templates, use [`../SCENARIO_USE_CASES_AND_TESTING.md`](../SCENARIO_USE_CASES_AND_TESTING.md).

---

## 1) Example Goal

**Team:** DataLink Solutions

**Objective:** evaluate a record-linkage model on synthetic customer-style data without exposing real PII.

**Need:** two messy datasets, a known ground truth, and realistic household/address change so precision and recall are meaningful.

This worked example is intentionally pairwise because `couple_merge` is one of the built-in two-dataset scenarios. Custom single-dataset dedup runs use `entity_record_map.csv` instead of `truth_crosswalk.csv`.

For this example, use `couple_merge` because it creates shared residence after cohabitation and introduces one-to-many cross-file ambiguity in Dataset B.

Canonical scenario source:

- `phase2/scenarios/couple_merge.yaml`

---

## 2) Run the Scenario

From repo root:

```bash
python scripts/run_phase2_pipeline.py --scenario couple_merge
```

Or, if you want the explicit three-step flow:

```bash
python scripts/generate_phase2_truth.py --run-id 2026-03-11_couple_merge_seed20260311
python scripts/generate_phase2_observed.py --run-id 2026-03-11_couple_merge_seed20260311
python scripts/validate_phase2_outputs.py --run-id 2026-03-11_couple_merge_seed20260311
```

Outputs are written under:

```text
phase2/runs/2026-03-11_couple_merge_seed20260311/
```

---

## 3) Focus on the Core Artifacts

For ER evaluation, you only need a few files first:

- `DatasetA.csv`
- `DatasetB.csv`
- `entity_record_map.csv`
- `truth_crosswalk.csv`
- `truth_events.parquet`
- `quality_report.json`

What each one is for:

- `DatasetA.csv` and `DatasetB.csv`: the two observed datasets your ER system must link.
- `entity_record_map.csv`: the canonical mapping from `PersonKey` to observed records.
- `truth_crosswalk.csv`: the answer key for scoring predicted matches.
- `truth_events.parquet`: explains why records may have changed over time.
- `quality_report.json`: tells you whether overlap, duplication, and drift match the intended benchmark.

---

## 4) Evaluate an ER Model

```python
from pathlib import Path
import pandas as pd

run_dir = Path("phase2/runs/2026-03-11_couple_merge_seed20260311")

dataset_a = pd.read_csv(run_dir / "DatasetA.csv")
dataset_b = pd.read_csv(run_dir / "DatasetB.csv")
truth_crosswalk = pd.read_csv(run_dir / "truth_crosswalk.csv")

# Replace this with your actual ER pipeline.
predicted_matches = my_entity_resolution_algorithm(dataset_a, dataset_b)

predicted_set = set(zip(predicted_matches["A_RecordKey"], predicted_matches["B_RecordKey"]))
truth_matches = truth_crosswalk[
    truth_crosswalk["A_RecordKey"].astype(str).str.strip().ne("")
    & truth_crosswalk["B_RecordKey"].astype(str).str.strip().ne("")
]
truth_set = set(zip(truth_matches["A_RecordKey"], truth_matches["B_RecordKey"]))

tp = len(predicted_set & truth_set)
fp = len(predicted_set - truth_set)
fn = len(truth_set - predicted_set)

precision = tp / (tp + fp) if (tp + fp) else 0.0
recall = tp / (tp + fn) if (tp + fn) else 0.0
f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

print({"precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn})
```

---

## 5) Diagnose Errors

If recall is low:

- inspect `truth_events.parquet` for `COHABIT` and address changes,
- inspect `quality_report.json` under `phase2_quality.er_benchmark_metrics.attribute_drift_rates`,
- check whether your model is under-weighting changed addresses or missing middle names.

If precision is low:

- inspect `truth_crosswalk.csv` for one-to-many cases,
- inspect `quality_report.json` under `phase2_quality.er_benchmark_metrics.match_cardinality_achieved.one_to_many`,
- check whether your model over-links people who now share an address.

Useful truth inspection snippet:

```python
truth_events = pd.read_parquet(run_dir / "truth_events.parquet")
cohabits = truth_events[truth_events["EventType"].astype(str).str.upper() == "COHABIT"]
print(cohabits[["EventDate", "PersonKeyA", "PersonKeyB", "CohabitMode"]].head())
```

---

## 6) What "Good" Looks Like for This Example

For `couple_merge`, you should expect:

- at least one `COHABIT` event,
- shared household or residence after the cohabitation event,
- one-to-many cross-file behavior visible in `truth_crosswalk.csv`,
- stronger drift and messiness on Dataset B than on Dataset A.

If those conditions are not visible, inspect the canonical scenario YAML and `quality_report.json` before trusting the benchmark run.

---

## 7) Next Step

After you complete this worked example, use [`../SCENARIO_USE_CASES_AND_TESTING.md`](../SCENARIO_USE_CASES_AND_TESTING.md) to choose the next scenario based on the ER behavior you want to stress.
