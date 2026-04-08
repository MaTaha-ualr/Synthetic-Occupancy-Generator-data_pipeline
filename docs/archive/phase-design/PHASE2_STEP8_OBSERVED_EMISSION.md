# Phase-2 Step-8 Observed Emission (As of March 5, 2026)

## Goal
Convert truth simulation outputs into two observed datasets for ER experiments:
- `DatasetA.csv` (baseline-like system view)
- `DatasetB.csv` (later system view)
- `truth_crosswalk.csv` (PersonKey to A/B record mapping)

## Configuration
Scenario YAML supports:
- `emission.crossfile_match_mode`
  - `one_to_one`
  - `one_to_many`
  - `many_to_one`
  - `many_to_many`
- `emission.overlap_entity_pct`
- `emission.appearance_A_pct`
- `emission.appearance_B_pct`
- `emission.duplication_in_A_pct`
- `emission.duplication_in_B_pct`
- Optional dataset noise blocks:
  - `emission.noise.A.*`
  - `emission.noise.B.*`

## Dataset Views
- Dataset A uses baseline snapshot date (`simulation.start_date`).
- Dataset B uses later snapshot date (simulation end).
- Births after baseline naturally become B-only candidates.

## Cardinality + Coverage Behavior
- Coverage percentages determine who appears in A, B, and overlap.
- Match mode controls how overlap records pair in crosswalk.
- Duplication percentages create within-file duplicates per dataset.

## Enforced In Code
- Emission engine:
  - `src/sog_phase2/emission.py`
- Emission CLI:
  - `scripts/generate_phase2_observed.py`

## Run Command
```bash
python scripts/generate_phase2_observed.py --run-id <run_id> --overwrite
```

Example:
```bash
python scripts/generate_phase2_observed.py --run-id 2026-03-10_single_movers_seed20260310 --overwrite
```
