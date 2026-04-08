# SOG Scenario Use Cases and Testing Guide

Last updated: April 5, 2026

This is the main guide for choosing a Phase-2 scenario, running it, and checking whether the output is useful for entity-resolution benchmarking.

Use the scenario YAMLs in `phase2/scenarios/` as the source of truth for rates, overlap, duplication, and match-cardinality settings. This guide explains what each built-in scenario is meant to stress, what SOG generates, and what to inspect after a run.

If you need the broader runnable-vs-planned scenario roadmap, use:

- `docs/SCENARIO_SUPPORT_MATRIX.md`

---

## 1) What SOG Testing Is For

SOG Phase-2 is built to answer a practical benchmarking question:

"If my entity-resolution system is exposed to realistic household change, messy records, and partial coverage, how well does it recover the true entities?"

All runs give you:

- a truth-layer event history,
- one or more observed dataset CSVs,
- a canonical entity-to-record map in `entity_record_map.csv`,
- and a quality report in `quality_report.json`.

The current built-in catalog in this guide includes both pairwise linkage and single-dataset dedup templates.

The pairwise scenarios in this guide also emit:

- `DatasetA.csv`,
- `DatasetB.csv`,
- `truth_crosswalk.csv`.

You use those artifacts to:

1. pick a realistic behavioral pattern,
2. generate benchmark data,
3. run your ER pipeline,
4. score predicted links against the truth artifacts,
5. diagnose where overlap, duplication, or attribute drift are making the task harder.

---

## 2) How To Choose a Scenario

Pick the scenario based on the benchmark question you want to answer.

| Scenario | Best For | Primary Truth Event | Observed Match Mode | Main ER Stress |
|---|---|---|---|---|
| `clean_baseline_linkage` | Easy baseline sanity checks | `MOVE` | `one_to_one` | near-ideal low-noise linkage |
| `single_movers` | Address change benchmarking | `MOVE` | `one_to_one` | same person, changed residence |
| `couple_merge` | Household formation and co-residence | `COHABIT` | `one_to_many` | one person linked to multiple B records |
| `family_birth` | Household growth and new child entities | `BIRTH` | `many_to_one` | multiple A records converging to one B record |
| `divorce_custody` | Household split and ambiguous relationships | `DIVORCE` | `many_to_many` | ambiguity on both sides of the crosswalk |
| `roommates_split` | Young-adult churn and noisy splits | `LEAVE_HOME` plus `MOVE` | `one_to_many` | high duplication and unstable household context |
| `high_noise_identity_drift` | Severe field-level corruption | `MOVE` | `one_to_one` | OCR, nickname, phonetic, and date drift |
| `low_overlap_sparse_coverage` | Sparse shared coverage | `MOVE` | `one_to_one` | large unmatched populations and non-match pressure |
| `asymmetric_source_coverage` | One broad source linked to one sparse source | `MOVE` | `one_to_one` | dominant A-side coverage and many unmatched A-only entities |
| `high_duplication_dedup` | Single-file dedup benchmarking | optional `MOVE` background activity | `single_dataset` | clustering many duplicate records in one file |
| `three_source_partial_overlap` | Three-source linkage | `MOVE` | `one_to_one` | partial shared evidence across three systems |

Use this quick decision rule:

- Need the easiest baseline before anything harder: run `clean_baseline_linkage`.
- Need the cleanest address-change benchmark with a bit more realism: run `single_movers`.
- Need shared-address ambiguity after household formation: run `couple_merge`.
- Need new-child and household-expansion behavior: run `family_birth`.
- Need the hardest family-structure ambiguity: run `divorce_custody`.
- Need high churn, roommate grouping, and noisy duplication pressure: run `roommates_split`.
- Need heavy identity corruption without much structural ambiguity: run `high_noise_identity_drift`.
- Need weak shared coverage and lots of true non-matches: run `low_overlap_sparse_coverage`.
- Need one system to dominate coverage while the other remains sparse: run `asymmetric_source_coverage`.
- Need one-file deduplication instead of cross-file linkage: run `high_duplication_dedup`.
- Need to benchmark three-source linkage instead of A/B only: run `three_source_partial_overlap`.

---

## 3) Built-In Scenarios

### 3.1 `single_movers`

**Real-world case**

People relocate between the start and end snapshots, but the scenario remains structurally simpler than the others.

**Use it when**

- you want to benchmark address-change handling,
- you want a baseline ER run before moving to harder household dynamics,
- you want overlap close to a mostly clean one-to-one setup.

**Do not use it when**

- you need household formation or split behavior,
- you want strong one-to-many or many-to-many ambiguity,
- you specifically want child creation or custody dynamics.

**Truth behavior that should appear**

- `MOVE` events in `truth_events.parquet`
- address changes in `truth_residence_history.parquet`

**Observed behavior that should appear**

- pairwise matching centered on `one_to_one`
- overlap near the scenario target
- lower within-file duplication than the harder scenarios

**ER difficulty stressed**

- matching the same person when address is no longer stable
- avoiding false negatives caused by residence changes

**Inspect after a run**

- `truth_events.parquet`: confirm `MOVE` events exist
- `entity_record_map.csv`: confirm each linked person stays structurally simple
- `truth_crosswalk.csv`: inspect A/B record pairs
- `quality_report.json`:
  - `phase2_quality.scenario_metrics.event_counts.moves`
  - `phase2_quality.er_benchmark_metrics.cross_file_overlap`
  - `phase2_quality.er_benchmark_metrics.match_cardinality_achieved`

### 3.2 `couple_merge`

**Real-world case**

Two people form a household and co-reside after a cohabitation event.

**Use it when**

- you want to test household formation,
- you want record linkage to handle shared post-event addresses,
- you want one-to-many ambiguity concentrated in Dataset B.

**Do not use it when**

- you need children to be created,
- you want post-divorce household separation,
- you need the heaviest duplication levels in the catalog.

**Truth behavior that should appear**

- `COHABIT` events in `truth_events.parquet`
- shared residence or shared household after the event

**Observed behavior that should appear**

- `one_to_many` pairwise behavior
- Dataset B should be the messier side
- linked people may have multiple B records

**ER difficulty stressed**

- distinguishing true shared-address couples from coincidental co-residence
- handling cross-file duplication concentrated on one side

**Inspect after a run**

- `truth_events.parquet`: confirm `COHABIT`
- `truth_household_memberships.parquet`: confirm shared households after the event
- `entity_record_map.csv`: confirm repeated B-side records per person
- `truth_crosswalk.csv`: look for persons with multiple B-side records
- `quality_report.json`:
  - `phase2_quality.scenario_metrics.event_counts.couples_formed`
  - `phase2_quality.er_benchmark_metrics.match_cardinality_achieved.one_to_many`
  - `phase2_quality.er_benchmark_metrics.attribute_drift_rates.dataset_b`

### 3.3 `family_birth`

**Real-world case**

Existing households grow because children are added during the simulation window.

**Use it when**

- you want to test child-creation and household-expansion behavior,
- you want many-to-one matching pressure,
- you want to see how ER behaves when a family's composition changes over time.

**Do not use it when**

- you need divorce or custody ambiguity,
- you need roommate-style mobility stress,
- you want the simplest baseline scenario.

**Truth behavior that should appear**

- `BIRTH` events in `truth_events.parquet`
- new child `PersonKey` values in `truth_people.parquet`
- updated household membership over time

**Observed behavior that should appear**

- `many_to_one` pairwise behavior
- higher A-side duplication than B-side duplication

**ER difficulty stressed**

- linking household records when the household has expanded
- dealing with multiple A-side records that collapse to one B-side record

**Inspect after a run**

- `truth_events.parquet`: confirm `BIRTH`
- `truth_people.parquet`: verify child keys from events exist
- `entity_record_map.csv`: confirm repeated A-side records per person
- `truth_crosswalk.csv`: inspect persons with multiple A-side records and one B-side record
- `quality_report.json`:
  - `phase2_quality.scenario_metrics.event_counts.births`
  - `phase2_quality.er_benchmark_metrics.match_cardinality_achieved.many_to_one`
  - `phase2_quality.er_benchmark_metrics.within_file_duplicate_rates.dataset_a`

### 3.4 `divorce_custody`

**Real-world case**

Households form and later split, producing the most structurally ambiguous family scenario in the built-in catalog.

**Use it when**

- you want to stress-test ambiguity,
- you need household separation after prior co-residence,
- you want many-to-many match behavior across the crosswalk.

**Do not use it when**

- you only need a clean address-change test,
- you want low duplication,
- you want a single-event benchmark.

**Truth behavior that should appear**

- `DIVORCE` events in `truth_events.parquet`
- post-divorce households in `truth_households.parquet`
- changed household membership after the event

**Observed behavior that should appear**

- `many_to_many` pairwise ambiguity
- elevated duplication on both A and B

**ER difficulty stressed**

- separating prior household ties from current household structure
- avoiding false positives caused by shared history and duplicate records on both sides

**Inspect after a run**

- `truth_events.parquet`: confirm `DIVORCE`
- `truth_households.parquet`: inspect `post_divorce` household types
- `entity_record_map.csv`: confirm repeated records on both sides for some persons
- `truth_crosswalk.csv`: verify some persons have multiple A and multiple B records
- `quality_report.json`:
  - `phase2_quality.scenario_metrics.event_counts.divorces`
  - `phase2_quality.er_benchmark_metrics.match_cardinality_achieved.many_to_many`
  - `phase2_quality.er_benchmark_metrics.within_file_duplicate_rates`

### 3.5 `roommates_split`

**Real-world case**

Young-adult households begin with roommate groupings and then split as some members leave home or move.

**Use it when**

- you want high mobility and high within-file duplication,
- you want shared-address noise that is not family-based,
- you want a harder non-family churn benchmark.

**Do not use it when**

- you need births,
- you need divorce and custody dynamics,
- you want the cleanest interpretability.

**Truth behavior that should appear**

- `LEAVE_HOME` events and often `MOVE` events
- households with three or more active members at some point
- a leaving roommate should end up in a new household or address

**Observed behavior that should appear**

- `one_to_many` pairwise behavior
- high B-side duplication
- more instability in residence and household context than `single_movers`

**ER difficulty stressed**

- separating roommates from true entity duplication
- handling noisy household-level context when co-residence is temporary

**Inspect after a run**

- `truth_events.parquet`: confirm `LEAVE_HOME`
- `truth_household_memberships.parquet`: inspect larger shared households
- `truth_residence_history.parquet`: inspect post-split address changes
- `quality_report.json`:
  - `phase2_quality.scenario_metrics.event_counts.moves`
  - `phase2_quality.er_benchmark_metrics.match_cardinality_achieved.one_to_many`
  - `phase2_quality.er_benchmark_metrics.within_file_duplicate_rates.dataset_b`

### 3.6 `clean_baseline_linkage`

**Real-world case**

Two operational systems describe mostly the same people with very little drift, duplication, or structural ambiguity.

**Use it when**

- you want a first-pass sanity check before harder benchmarks,
- you want a low-noise, high-overlap pairwise run,
- you want to isolate basic linkage quality without heavy household complexity.

**Do not use it when**

- you want severe field corruption,
- you want one-to-many or many-to-many ambiguity,
- you want a deduplication-only benchmark.

**Truth behavior that should appear**

- `MOVE` events in `truth_events.parquet`
- otherwise simple residence histories with minimal household complexity

**Observed behavior that should appear**

- `one_to_one` pairwise behavior
- overlap close to the high configured target
- very low duplicate rates in both observed datasets
- very low noise counts compared with the harder templates

**ER difficulty stressed**

- basic cross-file identity recovery without much confounding structure
- regression sanity checks when you are comparing algorithm or parameter changes

**Inspect after a run**

- `truth_events.parquet`: confirm `MOVE`
- `truth_crosswalk.csv`: verify simple one-to-one pairs
- `entity_record_map.csv`: verify most persons have one record per dataset
- `quality_report.json`:
  - `phase2_quality.er_benchmark_metrics.cross_file_overlap`
  - `phase2_quality.er_benchmark_metrics.match_cardinality_achieved.one_to_one`
  - `phase2_quality.er_benchmark_metrics.within_file_duplicate_rates`

### 3.7 `high_noise_identity_drift`

**Real-world case**

Two systems still refer to the same people, but one of them has heavy identity corruption from OCR, nicknames, phonetic variation, suffix loss, and date formatting issues.

**Use it when**

- you want to stress blocking and scoring under heavy field drift,
- you want a hard identity-matching benchmark without heavy household ambiguity,
- you want to test whether the linker is overly dependent on exact name and date agreement.

**Do not use it when**

- you want multi-record structural ambiguity to be the dominant stress,
- you want a one-file dedup run,
- you want the easiest baseline.

**Truth behavior that should appear**

- `MOVE` events in `truth_events.parquet`
- no special family-formation or custody semantics beyond the background movement

**Observed behavior that should appear**

- `one_to_one` pairwise behavior
- Dataset B should realize much higher noise than Dataset A
- visible counts for OCR, phonetic, nickname, date-swap, ZIP-digit, and suffix-loss noise

**ER difficulty stressed**

- recovering matches under realistic identity-field corruption
- avoiding false negatives caused by approximate rather than structural differences

**Inspect after a run**

- the observed dataset CSVs in `manifest.json`
- `truth_crosswalk.csv`: confirm the pairwise task is still fundamentally one-to-one
- `quality_report.json`:
  - `phase2_quality.er_benchmark_metrics.attribute_drift_rates.dataset_a`
  - `phase2_quality.er_benchmark_metrics.attribute_drift_rates.dataset_b`
  - `phase2_quality.er_benchmark_metrics.within_file_duplicate_rates`

### 3.8 `high_duplication_dedup`

**Real-world case**

One operational system contains many duplicate records for the same underlying people, and the task is to deduplicate within that one file.

**Use it when**

- you want a single-file dedup benchmark,
- you want to stress clustering or consolidation quality,
- you want high duplicate pressure without requiring a pairwise crosswalk.

**Do not use it when**

- you need A/B linkage scoring,
- you want explicit one-to-many or many-to-many cross-file behavior,
- you need the pairwise artifacts for an external linkage pipeline.

**Truth behavior that should appear**

- optional background `MOVE` activity may still appear
- no cross-file event semantics are required for the benchmark intent

**Observed behavior that should appear**

- exactly one observed dataset, typically `observed_registry.csv`
- no `truth_crosswalk.csv`
- high within-file duplication in `entity_record_map.csv`
- dense duplicate groups for the same `PersonKey`

**ER difficulty stressed**

- deduplicating one messy registry rather than linking two sources
- clustering duplicate records under one entity assignment

**Inspect after a run**

- `entity_record_map.csv`: verify repeated `RecordKey` values per `PersonKey` within one dataset id
- `manifest.json`: confirm there is only one observed dataset and no pairwise crosswalk
- `quality_report.json`:
  - `phase2_quality.er_benchmark_metrics.topology`
  - `phase2_quality.er_benchmark_metrics.within_file_duplicate_rates`
  - `phase2_quality.er_benchmark_metrics.attribute_drift_rates`

### 3.9 `low_overlap_sparse_coverage`

**Real-world case**

Two systems each cover only part of the same population, and the amount of shared population is intentionally weak.

**Use it when**

- you want to measure precision and recall under strong non-match pressure,
- you want a pairwise benchmark where overlap is intentionally low,
- you want many A-only and B-only entities without heavy duplication.

**Do not use it when**

- you want a mostly matched baseline,
- you want structural ambiguity to dominate the difficulty,
- you want a deduplication-only benchmark.

**Truth behavior that should appear**

- `MOVE` events in `truth_events.parquet`
- otherwise simple truth semantics with no special family event stress

**Observed behavior that should appear**

- `one_to_one` pairwise behavior
- low overlap relative to each dataset's population
- large A-only and B-only populations
- low duplication compared with household-ambiguity scenarios

**ER difficulty stressed**

- maintaining precision when true matches are rare relative to candidate pairs
- calibrating thresholds and blocking for low-overlap linkage

**Inspect after a run**

- `truth_crosswalk.csv`: verify relatively few linked entities
- `entity_record_map.csv`: verify many entities appear in only one dataset
- `quality_report.json`:
  - `phase2_quality.er_benchmark_metrics.cross_file_overlap`
  - `phase2_quality.er_benchmark_metrics.within_file_duplicate_rates`
  - `phase2_quality.er_benchmark_metrics.match_cardinality_achieved.one_to_one`

### 3.10 `asymmetric_source_coverage`

**Real-world case**

One source is operationally broad and one source is narrow, so most entities appear in the broad source but many do not appear in the sparse source.

**Use it when**

- you want to benchmark linkage under unequal source coverage,
- you want a pairwise benchmark where one dataset dominates the population,
- you want to understand recall ceilings imposed by sparse source availability.

**Do not use it when**

- you want balanced source populations,
- you want N-way linkage,
- you want duplication to be the main challenge.

**Truth behavior that should appear**

- `MOVE` events in `truth_events.parquet`
- otherwise straightforward truth histories

**Observed behavior that should appear**

- `one_to_one` pairwise behavior
- Dataset A should contain far more entities than Dataset B
- many unmatched A-only entities, but relatively few B-only entities

**ER difficulty stressed**

- interpreting recall and coverage when the source systems are operationally imbalanced
- avoiding over-penalizing the linker for entities missing from the sparse source

**Inspect after a run**

- `truth_crosswalk.csv`: verify the matched subset is much smaller than Dataset A
- `entity_record_map.csv`: compare per-dataset entity counts
- `quality_report.json`:
  - `phase2_quality.er_benchmark_metrics.cross_file_overlap`
  - `phase2_quality.er_benchmark_metrics.topology`
  - `phase2_quality.er_benchmark_metrics.within_file_duplicate_rates`

### 3.11 `three_source_partial_overlap`

**Real-world case**

Three systems each cover overlapping but not identical parts of the population, so some entities appear in all three, some only in two, and some only in one.

**Use it when**

- you want to benchmark N-way linkage instead of pairwise linkage,
- you want partial shared evidence rather than full three-way overlap,
- you want canonical per-pair truth crosswalks plus the global entity-record map.

**Do not use it when**

- you need a simple A/B benchmark,
- you want the legacy `truth_crosswalk.csv` only,
- you want household ambiguity to be the dominant stressor.

**Truth behavior that should appear**

- `MOVE` events in `truth_events.parquet`
- no extra truth-event semantics beyond background movement

**Observed behavior that should appear**

- three observed dataset CSVs
- no top-level `truth_crosswalk.csv`
- per-pair crosswalk files such as `truth_crosswalk__registry__claims.csv`
- partial overlap across all three datasets with different pairwise overlap levels

**ER difficulty stressed**

- reconciling identities across more than two systems
- making use of partial pairwise evidence when full three-way overlap is limited

**Inspect after a run**

- `entity_record_map.csv`: verify records span three dataset ids
- `manifest.json`: verify the per-pair crosswalk inventory
- `quality_report.json`:
  - `phase2_quality.er_benchmark_metrics.topology`
  - `phase2_quality.er_benchmark_metrics.multi_dataset_overlap`
  - `phase2_quality.er_benchmark_metrics.pairwise_match_cardinality`

---

## 4) User-Facing Testing Workflow

Use this workflow from repo root.

### 4.1 Run a scenario

If you want one command:

```bash
python scripts/run_phase2_pipeline.py --scenario single_movers
```

If you want explicit control:

```bash
python scripts/generate_phase2_truth.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/generate_phase2_observed.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
```

### 4.2 Inspect the core artifacts

Review these after every run:

- `truth_events.parquet`
- `entity_record_map.csv`
- `quality_report.json`

For the pairwise scenarios, also review:

- `truth_crosswalk.csv`

For N-way scenarios, review:

- `truth_crosswalk__<left>__<right>.csv` per pair listed in `manifest.json`

Use them in this order:

1. `truth_events.parquet`: did the scenario's primary behavior actually occur?
2. `entity_record_map.csv`: does the observed record topology match what the scenario intends?
3. `truth_crosswalk.csv` for pairwise runs or `truth_crosswalk__<left>__<right>.csv` for N-way runs: does the linking pattern match the intended cardinality and source coverage?
4. `quality_report.json`: did overlap, duplication, and drift metrics land in the expected range?

### 4.3 Evaluate your ER system

For pairwise runs, run your ER algorithm on `DatasetA.csv` and `DatasetB.csv`, then compare predicted links with `truth_crosswalk.csv`.

For custom single-dataset runs, run your deduplication pipeline on the single observed CSV and use `entity_record_map.csv` as the truth source for record-to-person grouping.

For N-way runs, compare predictions against the per-pair truth crosswalks listed in `manifest.json`, and use `entity_record_map.csv` as the canonical truth mapping across all systems.

At minimum, track:

- precision,
- recall,
- F1,
- false positives,
- false negatives.

If recall is low, start by checking overlap and attribute drift. If precision is low, start by checking duplicate rates and observed cardinality.

---

## 5) Failure Diagnosis

If a run does not behave as expected, use this checklist.

### 5.1 No primary event for the scenario

Examples:

- no `MOVE` in `single_movers`
- no `COHABIT` in `couple_merge`
- no `BIRTH` in `family_birth`
- no `DIVORCE` in `divorce_custody`
- no `LEAVE_HOME` in `roommates_split`

Check:

- `truth_events.parquet`
- `quality_report.json` under `phase2_quality.scenario_metrics.event_counts`
- the canonical YAML in `phase2/scenarios/<scenario_id>.yaml`

### 5.2 Overlap is lower or higher than expected

Check:

- `entity_record_map.csv`
- `truth_crosswalk.csv` for pairwise runs
- `quality_report.json` under `phase2_quality.er_benchmark_metrics.cross_file_overlap`
- the scenario emission settings:
  - `overlap_entity_pct` for pairwise mode
  - `appearance_A_pct` / `appearance_B_pct` for legacy A/B mode
  - `emission.datasets[*].appearance_pct` for canonical dataset-list mode

### 5.3 Cardinality is not what you expected

Check:

- `entity_record_map.csv`
- `truth_crosswalk.csv` for pairwise runs
- `quality_report.json` under `phase2_quality.er_benchmark_metrics.match_cardinality_achieved`
- `crossfile_match_mode` in the scenario YAML

### 5.4 Noise or drift is not visible enough

Check:

- the observed dataset CSVs listed in `manifest.json`
- `quality_report.json` under `phase2_quality.er_benchmark_metrics.attribute_drift_rates`
- the scenario YAML noise blocks under either:
  - `emission.noise.A` / `emission.noise.B`
  - `emission.datasets[*].noise`

### 5.5 Household behavior looks wrong

Check:

- `truth_households.parquet`
- `truth_household_memberships.parquet`
- `truth_residence_history.parquet`
- `quality_report.json` under `phase2_quality.truth_consistency`

---

## 6) Mapping to Automated Regression Coverage

The user-facing scenarios are backed by regression tests so the documented behavior stays aligned with the engine.

| Scenario | Canonical YAML | Core regression target | What the test protects |
|---|---|---|---|
| `single_movers` | `phase2/scenarios/single_movers.yaml` | `tests/test_phase2_scenario_regression.py` | `MOVE` events and overlap math |
| `clean_baseline_linkage` | `phase2/scenarios/clean_baseline_linkage.yaml` | `tests/test_phase2_scenario_regression.py` | low-noise one-to-one baseline behavior |
| `couple_merge` | `phase2/scenarios/couple_merge.yaml` | `tests/test_phase2_scenario_regression.py` | `COHABIT`, shared residence, one-to-many behavior |
| `family_birth` | `phase2/scenarios/family_birth.yaml` | `tests/test_phase2_scenario_regression.py` | `BIRTH`, child creation, many-to-one behavior |
| `divorce_custody` | `phase2/scenarios/divorce_custody.yaml` | `tests/test_phase2_scenario_regression.py` | `DIVORCE`, post-divorce households, many-to-many behavior |
| `roommates_split` | `phase2/scenarios/roommates_split.yaml` | `tests/test_phase2_scenario_regression.py` | roommate grouping, `LEAVE_HOME`, one-to-many behavior |
| `high_noise_identity_drift` | `phase2/scenarios/high_noise_identity_drift.yaml` | `tests/test_phase2_scenario_regression.py` | heavy B-side identity drift with low structural ambiguity |
| `low_overlap_sparse_coverage` | `phase2/scenarios/low_overlap_sparse_coverage.yaml` | `tests/test_phase2_scenario_regression.py` | low overlap and large unmatched populations |
| `asymmetric_source_coverage` | `phase2/scenarios/asymmetric_source_coverage.yaml` | `tests/test_phase2_scenario_regression.py` | strong A-side coverage imbalance |
| `high_duplication_dedup` | `phase2/scenarios/high_duplication_dedup.yaml` | `tests/test_phase2_scenario_regression.py` | single-dataset topology and high duplicate pressure |
| `three_source_partial_overlap` | `phase2/scenarios/three_source_partial_overlap.yaml` | `tests/test_phase2_scenario_regression.py` | N-way topology, pairwise crosswalk inventory, and partial overlap |

Quality-report completeness is covered by `tests/test_phase2_quality.py`.

Single-dataset emission behavior is covered separately by:

- `tests/test_phase2_emission.py`
- `tests/test_phase2_output_contract.py`

---

## 7) Related Documents

- `phase2/scenarios/README.md`: YAML schema and scenario-field reference
- `docs/reference/USE_CASE_GUIDE.md`: one worked DataLink example
- `docs/SOG_COMPLETE_USER_GUIDE.md`: full project guide

Note:

- This guide is centered on the current canonical scenario templates shipped in `phase2/scenarios/`.
- Pairwise scenarios use `truth_crosswalk.csv` for scoring, while single-dataset dedup scenarios rely on `entity_record_map.csv`.
