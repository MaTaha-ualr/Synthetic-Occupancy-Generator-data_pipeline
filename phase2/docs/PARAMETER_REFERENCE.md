# Phase-2 Parameter Reference

Every knob you can set in a scenario YAML, what it means, its default, and its effect on
the output. This is the input-side companion to
[`phase2/runs/README.md`](../runs/README.md) (which documents the *outputs*).

For a copyable, heavily commented starter YAML, use
[`phase2/scenarios/_working_scenario_template.yaml`](../scenarios/_working_scenario_template.yaml).

A scenario YAML has these top-level sections:

```yaml
scenario_id: single_movers          # required
seed: 20260310                       # required (also seeds the run date if it looks like YYYYMMDD)
phase1: { data_path: ..., manifest_path: ... }   # required: where the Phase-1 baseline lives
parameters: { ... }                  # event rates for the simulation
selection: { ... }                   # who participates
constraints: { ... }                 # realism rules / novelty levers
simulation: { ... }                  # the calendar model
emission: { ... }                    # how observed datasets are produced
quality: { ... }                     # quality-report thresholds
```

Only `scenario_id`, `seed`, and `phase1` are strictly required; every other section
falls back to documented defaults.

---

## `parameters` — simulation event rates

These are **annual** percentages. The engine converts each to a per-step probability via
`1 − (1 − p)^(1/steps_per_year)` and then modulates it per person by their latent
propensity score. If a rate is omitted it defaults to `0.0` (that event won't fire) unless
`use_priors_for_unspecified_rates: true`, in which case the ACS/CDC/NCHS prior is used.
The built-in priors currently cover move/cohabit/birth/divorce only; lifecycle rates
are explicit scenario knobs.

Here `p` is an annual probability in `[0, 1]`, not a published count rate.
The conversion assumes an equal, constant discrete hazard in each simulation
step. Public rates are converted before this formula is applied: ACS mobility
percentages become age-specific person/household probabilities; NCHS births
per 1,000 women become probabilities for eligible people in the corresponding
age band; and the CDC divorce count per 1,000 total population is rescaled to
the active-couple population at risk. A person can participate in at most one
locked event per step, and eligibility changes and competing events can make
achieved annual incidence differ from the input probability. Birth generation
allows at most one birth per eligible parent per step.

| Key | Default | Meaning |
| --- | --- | --- |
| `move_rate_pct` | `0.0` | Annual chance a solo person or couple-household changes address (`MOVE`). |
| `cohabit_rate_pct` | `0.0` | Annual chance two eligible singles form a household (`COHABIT`). |
| `birth_rate_pct` | `0.0` | Annual chance an eligible person has a child (`BIRTH`). |
| `divorce_rate_pct` | `0.0` | Annual chance an active couple splits (`DIVORCE`). |
| `leave_home_rate_pct` (alias `split_rate_pct`) | `0.0` | Annual chance an 18–30 member of a multi-person household leaves to live solo (`LEAVE_HOME`). |
| `death_rate_pct` | `0.0` | Annual chance an alive person dies (`DEATH`), age-weighted so older people are more likely to be selected. Death closes active household and residence intervals and records `IsDeceased` / `DeathDate` in `truth_people.parquet`. |
| `name_change_rate_pct` | `0.0` | Annual chance an age-16+ person has a legal, marriage, or divorce name change (`NAME_CHANGE`). Later observed snapshots and timeline rows replay the new name. |
| `adoption_rate_pct` | `0.0` | Annual chance an under-18 person moves into an eligible adult household as an adopted child (`ADOPTION`). The event records previous/adoptive parent keys and can update the child's surname. |
| `use_priors_for_unspecified_rates` | `false` | When true, omitted move/cohabit/birth/divorce rates use real-world priors instead of `0.0`. Lifecycle rates currently remain explicit-only. |
| `calibrate_to_public_targets` | `false` | Use age-specific ACS mobility and NCHS fertility hazards and convert the CDC total-population divorce rate to an eligible-couple hazard. |
| `initialize_households_from_public_targets` | `false` | Build the initial Phase-2 family graph from checked-in ACS household-type shares instead of creating one singleton household per person. |
| `mobility_age_0_17_pct`, `mobility_age_18_34_pct`, `mobility_age_35_64_pct`, `mobility_age_65_plus_pct` | public priors | Optional household-hazard calibration inputs. E7 measures the resulting person-level mover rates. |

Lifecycle rates are part of the active event engine. They are not currently backed by
external priors: set `death_rate_pct`, `name_change_rate_pct`, or `adoption_rate_pct`
directly when you want those events.

**Roommate baseline grouping** (only read when `scenario_id == roommates_split`):

| Key | Default | Meaning |
| --- | --- | --- |
| `enable_roommate_baseline_groups` | `true` | Cluster young adults into shared starting households so there's something to split. |
| `roommate_group_share_pct` | `40.0` | Share of eligible people placed into roommate groups. |
| `roommate_household_size_min` / `_max` | `3` / `5` | Size range of a roommate household. |
| `roommate_age_min` / `_max` | `18` / `30` | Age window eligible for roommate grouping. |

> Propensity modulation, in words: movers with a higher `MobilityPropensityScore` move
> more often; higher `PartnershipPropensityScore` cohabit more and divorce less; higher
> `FertilityPropensityScore` give birth more. This is why two runs with the same rate but
> different seeds differ in *who* moves, not just how many.

---

## `selection` — who participates

```yaml
selection:
  sample: { mode: pct, value: 100.0 }
  filters: { age_bins: [], genders: [], ethnicities: [], residence_types: [],
             redundancy_profiles: [], mobility_propensity_buckets: [] }
  thresholds: { mobility_low_max: 0.09, mobility_high_min: 0.18,
                trait_low_max: 0.33, trait_high_min: 0.66 }
```

| Key | Default | Meaning |
| --- | --- | --- |
| `sample.mode` | `pct` | `all` (everyone after filters), `count` (an integer N), or `pct` (a percentage). |
| `sample.value` | `100.0` | The N or percentage (must be ≤ 100 for `pct`; integer for `count`). |
| `filters.age_bins` | `[]` | Keep only these age bins (`age_0_17`, `age_18_34`, `age_35_64`, `age_65_plus`). Empty = no filter. |
| `filters.genders` / `ethnicities` / `residence_types` | `[]` | Keep only matching values (case-insensitive). |
| `filters.redundancy_profiles` | `[]` | `single_record` or `multi_record` (how many Phase-1 records the person had). |
| `filters.mobility_propensity_buckets` | `[]` | `low` / `medium` / `high`. |
| `thresholds.mobility_low_max` / `mobility_high_min` | `0.09` / `0.18` | Cutoffs that turn a mobility score into a bucket. |
| `thresholds.trait_low_max` / `trait_high_min` | `0.33` / `0.66` | Cutoffs for the partnership/fertility buckets. |

Sampling is deterministic for a given seed: candidates are sorted by `PersonKey` and a
seeded RNG picks the subset, so the selected set is reproducible. To control exactly
how many people enter a simulation, use `sample.mode: count` and set `sample.value`
to the desired number. If the filters leave fewer candidates than that number, the
engine uses all available candidates.

---

## `constraints` — realism rules and novelty levers

```yaml
constraints:
  min_marriage_age: 18
  max_partner_age_gap: 25          # or set to null and use partner_age_gap_distribution
  fertility_age_range: { min: 15, max: 49 }
  allow_underage_marriage: false
  allow_child_lives_alone: false
  enforce_non_overlapping_residence_intervals: true
```

| Key | Default | Meaning |
| --- | --- | --- |
| `min_marriage_age` | `18` | Minimum age to be in a `COHABIT`/`DIVORCE` event. |
| `max_partner_age_gap` | `25` | Max allowed age gap between partners. `null` disables the gap check. |
| `partner_age_gap_distribution` | `null` | Optional `{gap: weight}` map; when present its **largest key** is the effective max gap. |
| `fertility_age_range.min` / `.max` | `15` / `49` | Age window in which `BIRTH` can occur. |
| `allow_underage_marriage` | `false` | Novelty lever: permit sub-`min_marriage_age` partners (unrealistic stress test). |
| `allow_child_lives_alone` | `false` | Novelty lever: drop the 18+ floor on `LEAVE_HOME`, so minors in a multi-person household can also move out solo. |
| `enforce_non_overlapping_residence_intervals` | `true` | Validate that each person's residence intervals don't overlap. |

These are enforced **during** simulation (to gate which events can fire) and re-checked
**after** simulation in `quality_report.json` → `constraints_validation`.

---

## `simulation` — the calendar model

```yaml
simulation:
  granularity: monthly       # or daily
  start_date: 2026-01-01
  periods: 12
```

| Key | Default | Meaning |
| --- | --- | --- |
| `granularity` | `monthly` | `monthly` (recommended) or `daily`. Sets `steps_per_year` = 12 or 365 for rate conversion. |
| `start_date` | run date | First snapshot. Dataset A is taken here; the start of the simulation window. |
| `periods` | `12` | Number of steps to simulate. The end snapshot (Dataset B) is `start_date + periods`. |

So a default monthly run spans `2026-01-01 → 2027-01-01` (12 months).

---

## `emission` — producing the observed datasets

Two schemas are accepted. They are equivalent; pick one.

### Canonical schema (any number of datasets)
```yaml
emission:
  crossfile_match_mode: one_to_one
  overlap_entity_pct: 70.0
  datasets:
    - { dataset_id: A, filename: DatasetA.csv, snapshot: simulation_start,
        appearance_pct: 85.0, duplication_pct: 4.0, record_count: 8784,
        noise: { ... } }
    - { dataset_id: B, filename: DatasetB.csv, snapshot: simulation_end,
        appearance_pct: 90.0, duplication_pct: 6.0, record_count: 9540,
        noise: { ... } }
```

### Legacy A/B schema (exactly two datasets)
```yaml
emission:
  crossfile_match_mode: one_to_one
  overlap_entity_pct: 70.0
  appearance_A_pct: 85.0
  appearance_B_pct: 90.0
  duplication_in_A_pct: 4.0
  duplication_in_B_pct: 6.0
  record_count_A: 8784
  record_count_B: 9540
  noise: { A: { ... }, B: { ... } }
```

| Key | Default | Meaning |
| --- | --- | --- |
| `crossfile_match_mode` | `one_to_one` | `single_dataset` (dedup, 1 file), `one_to_one`, `one_to_many`, `many_to_one`, `many_to_many`. Controls which side gets *forced* duplicates for overlap entities. |
| `overlap_entity_pct` | `70.0` | Target % of entities that appear in **both** datasets (0 for `single_dataset`). |
| `datasets[*].dataset_id` | — | Unique id (`A`, `B`, `registry`, …). |
| `datasets[*].filename` | `observed_<id>.csv` | Output CSV name (`A`→`DatasetA.csv`, `B`→`DatasetB.csv` by convention). |
| `datasets[*].snapshot` | `simulation_end` | `simulation_start` or `simulation_end` — which point in time the records reflect. |
| `datasets[*].appearance_pct` | `100.0` | % of candidate entities that appear in this dataset. |
| `datasets[*].duplication_pct` | `0.0` | Extra within-file duplicate records, as a % of entities. |
| `datasets[*].record_count` | `null` | Optional exact final row count for this dataset. When set, it overrides the final row count that appearance/duplication would otherwise produce. |
| `datasets[*].noise.*` | see below | The 12 noise dials (percent chance per record). |
| `record_count_A` / `record_count_B` | `null` | Legacy A/B aliases for exact final rows in `DatasetA.csv` and `DatasetB.csv`. |

**Match mode effect.** `crossfile_match_mode` decides which overlap entities get a forced
second record so the *cross-file* relationship has the named shape:
`one_to_many` duplicates the second dataset, `many_to_one` the first, `many_to_many` both,
`one_to_one` neither. (`duplication_pct` adds *additional* random within-file duplicates
on top, independent of the mode — which is why a `one_to_one` run can still show a few
many-to-one pairs.)

If `record_count` is set, the engine still uses the same entity selection and match-mode
logic, then trims or adds duplicate allocations deterministically so the final CSV has
the requested number of rows.

**Observed snapshots plus master timeline.** `DatasetA.csv` and `DatasetB.csv` are
snapshot files. In the legacy A/B schema, A is emitted at `simulation_start` and B is
emitted at `simulation_end`. `masterDataset.csv` now has two row types:
`RecordType=source_snapshot` stacks the observed source files, and
`RecordType=residence_timeline` adds `DatasetId=TIMELINE` rows for every interval in
`truth_residence_history.parquet`. Use the source row type for ER input parity and the
timeline row type when you need to inspect intermediate moves.

### The 12 noise dials (`noise.<dataset>.*` or `datasets[*].noise.*`)
All are a percent chance applied independently per record. First six default on, last six
default to `0.0`.

| Dial | Default (A / B presets) | Effect |
| --- | --- | --- |
| `name_typo_pct` | 1.0 / 2.5 | Replace one character in first or last name. |
| `dob_shift_pct` | 0.4 / 1.2 | Shift DOB by −3…+3 days. |
| `ssn_mask_pct` | 1.5 / 6.0 | Mask SSN to `***-**-1234`. |
| `phone_mask_pct` | 0.8 / 3.0 | Blank the phone. |
| `address_missing_pct` | 0.8 / 2.2 | Drop the whole address. |
| `middle_name_missing_pct` | 20.0 / 30.0 | Drop the middle name. |
| `phonetic_error_pct` | 0.0 | Swap a phonetic cluster (`ph`↔`f`, `ck`↔`k`, …). |
| `ocr_error_pct` | 0.0 | OCR confusion (`O`↔`0`, `l`↔`1`↔`I`, `rn`↔`m`, …). |
| `date_swap_pct` | 0.0 | Transpose DOB month/day when valid. |
| `zip_digit_error_pct` | 0.0 | Nudge one ZIP digit by ±1. |
| `nickname_pct` | 0.0 | Replace a formal first name with a nickname. |
| `suffix_missing_pct` | 0.0 | Drop the Jr./Sr./III suffix. |

---

## `quality` — quality-report thresholds

```yaml
quality:
  household_size_range: { min: 1, max: 12 }
```

| Key | Default | Meaning |
| --- | --- | --- |
| `household_size_range.min` | `1` | Household sizes below this are flagged in the quality report. |
| `household_size_range.max` | `12` | Household sizes above this are flagged (and can flip `status` to `quality_issues_detected`). |

---

## How a run is graded (`quality_report.json` → `status`)

`status` is `ok` only when **all** of these hold; otherwise `quality_issues_detected`:

1. `constraints_validation.valid` — no constraint violations in the truth layer.
2. No invalid event ages (e.g. an underage marriage when not allowed).
3. No overlapping residence/membership intervals.
4. All household sizes within `quality.household_size_range`.

---

## Determinism

Given the same Phase-1 input, scenario YAML, and seed, a run is **byte-for-byte
reproducible** — selection (SHA-256 + seeded RNG), simulation, and emission all draw from
seeded generators. Change the seed to get a different but equally-valid instance of the
same scenario.
