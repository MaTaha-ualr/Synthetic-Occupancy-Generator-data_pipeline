# Phase-2 Scenario Definitions

The machine-readable scenario support plan lives in:
- `phase2/scenarios/catalog.yaml`

The user-facing support matrix is:
- `docs/SCENARIO_SUPPORT_MATRIX.md`

Each scenario YAML should minimally define:
- `scenario_id`
- `seed`
- `phase1.data_path`
- `phase1.manifest_path`
- `selection.*` (deterministic participant selection rules)
- `constraints.*` (eligibility + novelty switches)
- `simulation.*` (calendar model for truth-layer simulation)
- `emission.*` (observed dataset coverage, cardinality, and duplication rules)
- `quality.*` (quality-report constraints for household size checks)

Selection keys:
- `sample.mode` in `all | count | pct`
- `sample.value`
- `filters.age_bins`, `filters.genders`, `filters.ethnicities`
- `filters.residence_types`
- `filters.redundancy_profiles` (`single_record`, `multi_record`)
- `filters.mobility_propensity_buckets` (`low`, `medium`, `high`)
- `thresholds.mobility_low_max`, `thresholds.mobility_high_min`
- `thresholds.trait_low_max`, `thresholds.trait_high_min`

Constraint keys:
- `min_marriage_age`
- `max_partner_age_gap` or `partner_age_gap_distribution`
- `fertility_age_range.min` / `fertility_age_range.max`
- `allow_underage_marriage` (default false)
- `allow_child_lives_alone` (default false)
- `enforce_non_overlapping_residence_intervals` (default true)

Simulation keys:
- `granularity` in `monthly | daily` (default `monthly`)
- `start_date` (`YYYY-MM-DD`)
- `periods` (`> 0`)

Emission keys:
- `crossfile_match_mode` in `single_dataset | one_to_one | one_to_many | many_to_one | many_to_many`
- Canonical schema:
  - `datasets[*].dataset_id`
  - `datasets[*].filename`
  - `datasets[*].snapshot` in `simulation_start | simulation_end`
  - `datasets[*].appearance_pct`
  - `datasets[*].duplication_pct`
  - optional `datasets[*].noise.*` controls
- Legacy A/B schema is still accepted for backward compatibility:
  - `overlap_entity_pct`
  - `appearance_A_pct`, `appearance_B_pct`
  - `duplication_in_A_pct`, `duplication_in_B_pct`
  - optional `noise.A.*` and `noise.B.*` controls

Observed output topology:
- all runs emit one or more observed dataset CSVs plus `entity_record_map.csv`
- pairwise two-dataset runs also emit `truth_crosswalk.csv`
- `single_dataset` runs emit exactly one observed CSV and no crosswalk

Quality keys:
- `household_size_range.min`
- `household_size_range.max`

Runs are materialized under `phase2/runs/`. That directory is generated output and is intentionally not versioned.

Validate a run with:
`python scripts/validate_phase2_outputs.py --run-id <run_id>`

Scenario population generation command:
`python scripts/build_phase2_scenario_population.py --run-id <run_id> --overwrite`

Truth-layer simulation command:
`python scripts/generate_phase2_truth.py --run-id <run_id> --overwrite`

Observed-layer emission command:
`python scripts/generate_phase2_observed.py --run-id <run_id> --overwrite`

Scenario regression test command:
`python -m pytest -q tests/test_phase2_scenario_regression.py`
