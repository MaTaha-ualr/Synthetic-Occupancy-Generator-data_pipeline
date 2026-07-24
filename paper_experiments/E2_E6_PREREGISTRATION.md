# E2-E6 preregistered evaluation protocol

This protocol is frozen before inspecting E2-E6 outcome metrics. It extends the
committed E0/E1 evaluation without changing the canonical scenario YAMLs.

## Seeds and calibration split

- Evaluation seeds: `20260720` through `20260729` (the E1 seed list).
- Dedicated development/calibration seed: `20260719`; it is never included in
  reported means or standard deviations.
- Scikit-learn training labels come only from the calibration seed's
  `clean_baseline_linkage` candidates.
- Matcher settings and decision thresholds are selected using only
  `clean_baseline_linkage` on the calibration seed, then frozen for every
  evaluation seed and scenario.
- Oracle thresholds in E4 are descriptive upper bounds evaluated per run and
  are never substituted for the primary frozen-threshold results.

## Topology-aware evaluation universe

The 14 canonical YAML scenarios define three task topologies:

- pairwise linkage (12 scenarios): compare records only across Dataset A and B;
- single-source deduplication (`high_duplication_dedup`): compare distinct
  records within the registry dataset once, using canonical key ordering; and
- N-way linkage (`three_source_partial_overlap`): compare records across all
  unordered source pairs (registry-claims, registry-benefits, claims-benefits).

Truth-positive pairs are every eligible pair of records sharing `PersonKey` in
`entity_record_map.csv`. Pairwise truth therefore includes cross-source
duplicate combinations for linkage, within-source duplicate pairs for dedup,
and excludes ineligible same-source pairs from linkage tasks.

## Candidate generation

All three matchers receive the exact same candidate-pair set from the E0 union
of five blocking keys. This isolates scoring/model differences. Candidate
recall is reported separately, so a shared blocking ceiling cannot be mistaken
for a matcher-scoring failure. E3 additionally reports whether candidate recall
or conditional scoring recall degrades first.

## Matcher families

1. **Deterministic E0 baseline:** the committed seven-field weighted score.
2. **Splink 4 / Fellegi-Sunter:** DuckDB backend, unsupervised `u` estimation
   and EM `m` estimation on the calibration clean dataset. Learned parameters,
   comparisons, and blocking settings are serialized and frozen.
3. **Learned fallback:** scikit-learn logistic regression over the same seven
   field similarities plus zero-similarity indicators (which flag either
   missingness or complete disagreement). Class imbalance is handled
   with `class_weight="balanced"`; preprocessing/model parameters are saved.

The learned feature vector is ordered as the seven baseline similarities
(`first`, `last`, `dob`, `street`, `city`, `state`, `postal`) followed by seven
binary indicators equal to one when the corresponding similarity is zero.
Every candidate pair from the held-out calibration run is used; there is no
pair subsampling or imputation. `StandardScaler` is fitted on that calibration
matrix, followed by scikit-learn `LogisticRegression(max_iter=2000,
class_weight="balanced", random_state=20260719)`. The positive-class
probability is the score. Exact package versions, serialized models, and the
three frozen thresholds are retained under
`results/E2_E6/E2/environment.json` and `results/E2_E6/E2/models/`.

The primary learned model is logistic regression because it is deterministic,
auditable, inexpensive, and genuinely supervised. A deep matcher is outside
the primary protocol and may be added only as a separately labeled appendix.

## Primary and cluster metrics

- Candidate recall.
- Pairwise precision, recall, and F1 at the frozen threshold.
- Oracle-threshold precision-recall curve, best F1, and threshold (E4).
- Pairwise precision/recall/F1 after transitive closure of predicted edges.
- B-cubed precision, recall, and F1 over all emitted records, including
  singleton predicted and truth clusters (E5).

Predicted clusters are connected components of frozen-threshold edges. Truth
clusters are records grouped by authoritative `PersonKey`. B-cubed is computed
per record, then macro-averaged across records.

## E2 acceptance tests

- Exactly 14 canonical scenarios, 10 evaluation seeds, and 3 matcher families.
- Every run has a finalized manifest and a unique artifact-hash record.
- Truth signatures are stable when the same run is re-evaluated.
- At least two scenario families change matcher ordering versus clean, or
  degradation profiles materially differ by matcher family.
- If every family saturates, report this as evidence that canonical difficulty
  is insufficient; do not tune scenarios after inspecting E2.

Scenario families used for the degradation analysis are: clean/mobility,
household formation, household dissolution, emission noise/coverage,
duplication/topology, multi-source, and lifecycle.

## E3 noise gradient

For `k in {0, 0.5, 1, 2, 4}`, multiply every Dataset-B high-noise percentage by
`k` and cap each field at 100%. All unscaled settings remain identical. Report
mean and sample SD over the ten evaluation seeds. Monotonicity is assessed on
mean F1, with changes smaller than the larger adjacent SD treated as
near-monotone rather than a reversal. Thresholds remain frozen from clean.

## E4 threshold analysis

Retain one row per scored candidate pair with run, matcher, endpoint keys,
truth label, and score. PR points use distinct observed score thresholds plus
explicit no-link/all-link endpoints. Oracle F1 ties resolve by higher precision
and then higher threshold, matching E0. Report `fixed F1 - oracle F1`; the
oracle is an in-sample diagnostic, not a deployable estimate.

## E5 cluster analysis

Transitive closure is computed only over eligible predicted edges, but all
emitted records enter clustering as nodes. This makes fragmentation and
over-merging visible and avoids dropping singletons from B-cubed.

## E6 co-resident false-match pressure

For clean, couple merge, roommates split, and family birth, a co-resident
non-match is an eligible candidate pair with different `PersonKey` values and
the same nonempty normalized observed street/city/state/postal tuple. Random
non-matches are sampled without replacement from remaining non-matches, matched
to the co-resident count per run using the run seed. Report score medians,
realized false positives, false-positive rate, and the share of false positives
that are co-resident. This operational definition measures evidence visible to
the matcher; truth-residence co-occupancy is a separate possible appendix.

## Preservation and reporting

All compact outputs live under `paper_experiments/results/E2_E6/`: exact YAMLs,
model files, score tables (Parquet), per-run metrics, summaries, figures,
validation JSON, environment versions, Git revisions, hashes, and a paper-ready
index. Bulky Phase-2 run folders remain under ignored `phase2/runs` and are
linked by run ID and SHA-256 hashes.
