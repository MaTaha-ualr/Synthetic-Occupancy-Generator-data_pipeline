# Phase-1 and Phase-2 Tuning Playbook

## Purpose

This guide is the practical companion to `reference/PHASE1_PHASE2_PARAMETER_REFERENCE.md`.

The reference explains what each parameter means.

This playbook explains how to combine those parameters on purpose.

Use this when the question is not:

- "What does this key do?"

Use this when the question is:

- "What should I change to get the behavior I want?"

## First Principle

Separate the problem into three layers before changing anything.

### Layer 1: Phase-1 baseline shape

Phase-1 decides:

- how many people exist
- how many rows exist
- how much row redundancy exists
- how much identity ambiguity already exists

### Layer 2: Phase-2 truth behavior

Phase-2 `parameters`, `selection`, `simulation`, and `constraints` decide:

- who enters the scenario
- what happens to them over time
- how realistic the truth history must remain

### Layer 3: Phase-2 observed difficulty

Phase-2 `emission` decides:

- which systems see which entities
- how much duplication each system has
- how much field-level corruption each system has

If you mix those layers mentally, tuning becomes confusing fast.

## Quick Decision Rule

When you want to make the benchmark harder, ask what kind of hard you mean:

| If you want harder because of... | Change primarily... | Avoid overusing... |
|---|---|---|
| more common names | Phase-1 `name_duplication` | emission noise |
| more repeated administrative rows | Phase-1 `redundancy` | truth event rates |
| more life changes over time | Phase-2 `parameters` and `simulation` | Phase-1 nickname noise |
| more unmatched entities across systems | Phase-2 `appearance_*` or `datasets[*].appearance_pct`, plus `overlap_entity_pct` | duplication |
| more within-file clutter | Phase-2 `duplication_*` or `datasets[*].duplication_pct` | lowering overlap too early |
| dirtier fields | Phase-2 `emission.noise.*` | truth event rates |
| more household ambiguity | Phase-2 `cohabit`, `divorce`, `birth`, `roommate_*` | raw OCR noise |
| more difficult dedup | single-dataset emission plus high duplication and moderate noise | pairwise-only settings |

## Baseline Tuning Strategy

Start from the easiest faithful version first, then add one stress type at a time.

Recommended progression:

1. clean baseline
2. one structural stress
3. one observed-data stress
4. both together

That order makes it easier to explain why a benchmark became hard.

## Phase-1 Recipes

### Recipe A: Clean canonical baseline

Use this when:

- you want the easiest Phase-1 input for Phase-2
- you want most difficulty to come from Phase-2, not the seed layer

Recommended direction:

- keep `n_records` close to `n_people`
- reduce `name_duplication.exact_full_name_people_pct`
- disable or minimize `nicknames`
- keep `fill_rates.phone` high
- keep `fill_rates.middle_name` and `fill_rates.suffix` stable rather than sparse

Example direction:

```yaml
phase1:
  n_people: 10000
  n_records: 10000
  name_duplication:
    exact_full_name_people_pct: 5.0
  redundancy:
    enabled: false
  nicknames:
    enabled: false
```

What this gives you:

- one row per person
- fewer common-name collisions
- cleaner entity surfaces before Phase-2 starts

### Recipe B: Administrative-repeat baseline

Use this when:

- you want repeated rows per person
- you want record-management complexity before observed emission starts

Recommended direction:

- set `n_records > n_people`
- enable `redundancy`
- use `balanced` first
- use `heavy_tail` only when you specifically want concentration

Example direction:

```yaml
phase1:
  n_people: 10000
  n_records: 16000
  redundancy:
    enabled: true
    min_records_per_entity: 1
    max_records_per_entity: 4
    shape: balanced
```

What this gives you:

- repeated persons in the flat file
- moderate row clutter
- easier interpretation than a heavy-tail run

### Recipe C: Heavy-tail record pressure

Use this when:

- you want a few people to generate many more rows than others
- you want operational-system skew

Recommended direction:

- use `shape: heavy_tail`
- lower `heavy_tail_alpha` if you want stronger concentration
- increase `max_records_per_entity`

Example direction:

```yaml
phase1:
  n_people: 10000
  n_records: 18000
  redundancy:
    enabled: true
    min_records_per_entity: 1
    max_records_per_entity: 8
    shape: heavy_tail
    heavy_tail_alpha: 1.2
```

What this gives you:

- realistic skew where a minority of people dominate row count
- more pressure on downstream dedup and entity consolidation

### Recipe D: Common-name pressure

Use this when:

- you want true ambiguity without corrupting fields
- you want same-name collisions to exist even in otherwise clean data

Recommended direction:

- raise `name_duplication.exact_full_name_people_pct`
- keep noise low at first so the effect is measurable

Example direction:

```yaml
phase1:
  name_duplication:
    exact_full_name_people_pct: 40.0
    collision_group_min_size: 2
    collision_group_max_size: 5
```

What this gives you:

- harder linkage because distinct people can look identical by name
- cleaner interpretation than heavy OCR or nickname drift

### Recipe E: Preferred-name variability

Use this when:

- you want display-name instability
- you want the same formal person to appear under nickname variants

Recommended direction:

- enable nicknames
- use `per_person` when you want stable preferred-name behavior
- use `per_record` when you want row-level display inconsistency

Example direction:

```yaml
phase1:
  nicknames:
    enabled: true
    mode: per_record
    usage_pct: 45.0
```

What this gives you:

- softer identity drift than OCR noise
- realistic display-name variance before Phase-2 emission even starts

## Phase-2 Recipes by Use Case

### 1. Clean linkage sanity check

Goal:

- easy pairwise linkage
- high overlap
- low duplication
- almost no corruption

Best starting template:

- `clean_baseline_linkage`

Tune like this:

- keep `crossfile_match_mode: one_to_one`
- keep `overlap_entity_pct` high
- keep `appearance_A_pct` and `appearance_B_pct` both very high
- keep duplication near zero
- keep all noise fields near zero
- keep `move_rate_pct` modest but non-zero if you still want temporal realism

Example direction:

```yaml
parameters:
  move_rate_pct: 6.0
emission:
  crossfile_match_mode: one_to_one
  overlap_entity_pct: 94.0
  appearance_A_pct: 97.0
  appearance_B_pct: 97.0
  duplication_in_A_pct: 1.0
  duplication_in_B_pct: 1.0
```

What to avoid:

- high `name_duplication` in Phase-1
- OCR or phonetic noise
- many-to-many match modes

### 2. Address-change linkage

Goal:

- same person across systems
- changed address is the main challenge
- limited household ambiguity

Best starting template:

- `single_movers`

Tune like this:

- keep `crossfile_match_mode: one_to_one`
- use moderate `move_rate_pct`
- keep overlap moderately high
- keep duplication low to moderate
- keep noise low enough that address change is the main stress

Good direction:

```yaml
parameters:
  move_rate_pct: 10.0
emission:
  crossfile_match_mode: one_to_one
  overlap_entity_pct: 70.0
  appearance_A_pct: 85.0
  appearance_B_pct: 90.0
```

What to avoid:

- high cohabit or divorce rates
- heavy nickname or OCR drift if the real focus is address continuity

### 3. High-noise identity drift

Goal:

- same underlying entities
- minimal structural ambiguity
- heavy field corruption

Best starting template:

- `high_noise_identity_drift`

Tune like this:

- keep `crossfile_match_mode: one_to_one`
- keep duplication low
- raise `phonetic_error_pct`, `ocr_error_pct`, `date_swap_pct`, `nickname_pct`, `suffix_missing_pct`
- leave truth rates modest so the challenge comes from observed fields

Good direction:

```yaml
parameters:
  move_rate_pct: 8.0
emission:
  crossfile_match_mode: one_to_one
  duplication_in_A_pct: 0.0
  duplication_in_B_pct: 1.0
  noise:
    B:
      name_typo_pct: 9.0
      phonetic_error_pct: 8.0
      ocr_error_pct: 8.0
      date_swap_pct: 6.0
      nickname_pct: 12.0
```

What to avoid:

- simultaneously making overlap very low
- simultaneously making duplication very high

If you change too many difficulty axes at once, you stop learning what actually broke the ER system.

### 4. Sparse-overlap linkage

Goal:

- many true non-matches
- weak shared coverage
- precision and recall stress

Best starting templates:

- `low_overlap_sparse_coverage`
- `asymmetric_source_coverage`

Tune like this:

- lower `overlap_entity_pct`
- lower one or both appearance rates
- keep duplication modest at first
- use only moderate noise so the benchmark mainly tests coverage sparsity

Good direction:

```yaml
emission:
  crossfile_match_mode: one_to_one
  overlap_entity_pct: 35.0
  appearance_A_pct: 60.0
  appearance_B_pct: 50.0
  duplication_in_A_pct: 1.0
  duplication_in_B_pct: 2.0
```

For asymmetry:

```yaml
emission:
  appearance_A_pct: 96.0
  appearance_B_pct: 44.0
```

What this gives you:

- broad unmatched populations
- realistic non-match pressure
- stronger importance of blocking and candidate generation

### 5. Shared-address ambiguity from household formation

Goal:

- people begin to share addresses
- household membership changes are part of the challenge

Best starting templates:

- `couple_merge`
- `family_birth`
- `roommates_split`

Tune like this:

- increase `cohabit_rate_pct` for couple formation
- increase `birth_rate_pct` for household growth
- use roommate baseline grouping when you want non-family shared households
- keep some noise, but not so much that the structural ambiguity gets buried

Examples:

```yaml
parameters:
  cohabit_rate_pct: 6.0
```

```yaml
parameters:
  birth_rate_pct: 3.5
```

```yaml
parameters:
  roommate_group_share_pct: 45.0
  roommate_household_size_min: 3
  roommate_household_size_max: 5
```

What to avoid:

- using only noise knobs when the actual goal is household ambiguity

If the benchmark question is about shared households, the truth layer must create shared households.

### 6. Hard family-structure ambiguity

Goal:

- household merge, split, or custody-like ambiguity
- both observed sides can be messy

Best starting template:

- `divorce_custody`

Tune like this:

- use non-zero `cohabit_rate_pct` and `divorce_rate_pct`
- use `many_to_many`
- raise duplication on both sides
- add only moderate field noise at first

Good direction:

```yaml
parameters:
  cohabit_rate_pct: 8.0
  divorce_rate_pct: 18.0
emission:
  crossfile_match_mode: many_to_many
  duplication_in_A_pct: 14.0
  duplication_in_B_pct: 14.0
```

Why this works:

- structural ambiguity already makes the scenario hard
- symmetric duplication on both sides creates dense link ambiguity

### 7. Single-dataset dedup benchmark

Goal:

- one observed file
- no cross-file linkage
- within-file duplicate clustering problem

Best starting template:

- `high_duplication_dedup`

Tune like this:

- set `crossfile_match_mode: single_dataset`
- define exactly one dataset in `emission.datasets`
- raise `duplication_pct`
- use moderate noise

Good direction:

```yaml
emission:
  crossfile_match_mode: single_dataset
  datasets:
    - dataset_id: registry
      filename: observed_registry.csv
      snapshot: simulation_end
      appearance_pct: 100.0
      duplication_pct: 68.0
```

What to avoid:

- legacy `appearance_A_pct` and `appearance_B_pct` when you really want one dataset only

### 8. Three-source or N-way linkage

Goal:

- more than two observed systems
- partial pairwise overlap
- non-uniform evidence across sources

Best starting template:

- `three_source_partial_overlap`

Tune like this:

- use `emission.datasets`
- keep unique `dataset_id` values
- vary `appearance_pct` by dataset
- keep noise different per dataset to mimic real system differences

Good direction:

```yaml
emission:
  crossfile_match_mode: one_to_one
  overlap_entity_pct: 42.0
  datasets:
    - dataset_id: registry
      snapshot: simulation_start
      appearance_pct: 96.0
    - dataset_id: claims
      snapshot: simulation_end
      appearance_pct: 74.0
    - dataset_id: benefits
      snapshot: simulation_end
      appearance_pct: 52.0
```

Why this works:

- different source coverages create different overlap graphs
- pairwise evidence is no longer uniform
- N-way linkage strategy becomes measurable

## If You Want X, Change Y

### If you want easier linkage

- lower Phase-1 `name_duplication.exact_full_name_people_pct`
- lower Phase-1 redundancy
- disable Phase-1 nicknames
- keep `crossfile_match_mode: one_to_one`
- raise overlap
- raise appearance
- lower duplication
- lower emission noise

### If you want harder linkage without making records dirty

- raise Phase-1 common-name duplication
- raise Phase-1 redundancy
- use household-changing truth scenarios
- lower overlap slightly
- increase duplication moderately

This gives ambiguity from structure rather than corruption.

### If you want harder linkage because records are dirty

- increase `name_typo_pct`
- increase `phonetic_error_pct`
- increase `ocr_error_pct`
- increase `date_swap_pct`
- increase `nickname_pct`
- increase `suffix_missing_pct`

Keep truth simpler while you do this, or you will combine too many difficulty types.

### If you want more non-matches

- lower `overlap_entity_pct`
- lower one or more `appearance_pct` values

Do not use duplication to simulate non-matches. Duplication increases repeated positives inside a dataset; it does not create true cross-system absence.

### If you want more duplicates within one file

- increase `duplication_in_A_pct`
- increase `duplication_in_B_pct`
- or increase `datasets[*].duplication_pct`

Do not lower overlap for this. Overlap and duplication are different difficulty axes.

### If you want more family complexity

- increase `cohabit_rate_pct`
- increase `birth_rate_pct`
- increase `divorce_rate_pct`
- use `many_to_one`, `one_to_many`, or `many_to_many` depending on the benchmark goal

### If you want more roommate instability

- use `roommates_split`
- raise `roommate_group_share_pct`
- widen the eligible roommate age band
- raise `split_rate_pct`
- raise B-side duplication

## What Not To Do

### Do not tune five difficulty axes at once

Bad pattern:

- low overlap
- high duplication
- high OCR noise
- high phonetic noise
- many-to-many
- high household churn

That will make the scenario hard, but not interpretable.

### Do not use field noise to simulate household behavior

If you want co-residence ambiguity, use:

- `cohabit_rate_pct`
- `birth_rate_pct`
- `divorce_rate_pct`
- roommate parameters

Do not try to fake household difficulty by only masking addresses.

### Do not use household events when your real question is OCR robustness

If the benchmark question is "can my system survive OCR drift?", keep:

- `one_to_one`
- low duplication
- moderate overlap
- simple truth events

Then increase only the noise fields.

### Do not forget the Phase-1 baseline

If Phase-1 already has:

- high common-name pressure
- high row redundancy
- nickname variation

then even a supposedly "clean" Phase-2 run will not be truly easy.

## Recommended Starting Points

| Benchmark goal | Best template | First knobs to change |
|---|---|---|
| easiest sanity check | `clean_baseline_linkage` | overlap, duplication, tiny noise changes |
| address continuity | `single_movers` | `move_rate_pct`, overlap, low noise |
| severe field corruption | `high_noise_identity_drift` | OCR, phonetic, nickname, date swap |
| sparse source coverage | `low_overlap_sparse_coverage` | overlap and appearance |
| one broad and one sparse source | `asymmetric_source_coverage` | `appearance_A_pct`, `appearance_B_pct` |
| shared-address couple ambiguity | `couple_merge` | `cohabit_rate_pct`, B-side duplication |
| household growth | `family_birth` | `birth_rate_pct`, A-side duplication |
| hard family split ambiguity | `divorce_custody` | `divorce_rate_pct`, both-side duplication |
| roommate churn | `roommates_split` | roommate-group and split settings |
| single-file dedup | `high_duplication_dedup` | one dataset plus high duplication |
| multi-source linkage | `three_source_partial_overlap` | `datasets[*].appearance_pct` and per-dataset noise |

## Safe Tuning Workflow

1. Pick the closest shipped scenario.
2. Change only one of these categories first:
   truth dynamics, overlap and coverage, duplication, or noise.
3. Run it and inspect the manifest and quality report.
4. Only then change a second category.

That workflow makes it much easier to explain benchmark difficulty later.

## Relationship to the Reference Guide

Use this playbook together with:

- `reference/PHASE1_PHASE2_PARAMETER_REFERENCE.md`

Use the reference when you need exact field meaning.

Use this playbook when you need a design decision:

- which template to start from
- which parameter family to change
- which settings should move together
- which settings should not be mixed too early
