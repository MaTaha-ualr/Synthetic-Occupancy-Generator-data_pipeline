# Phase-2 Step-9 Quality Report (As of March 5, 2026)

## Goal
Extend Phase-1-style quality checks to full Phase-2 outputs (truth + observed ER layer).

## Included Sections
- `truth_consistency`
  - event age validation (respecting constraint toggles)
  - time-overlap checks for residence and household membership intervals
  - household-size checks against scenario `quality.household_size_range`
- `scenario_metrics`
  - event counts (`COHABIT`, `DIVORCE`, `BIRTH`, `MOVE`)
  - moves-per-person distribution
  - household-type shares
- `er_benchmark_metrics`
  - cross-file overlap achieved
  - match cardinality achieved (`one_to_one`, `one_to_many`, `many_to_one`, `many_to_many`)
  - within-file duplicate rates
  - attribute drift rates (name, address, phone) for Dataset A and Dataset B

## Enforced In Code
- Quality engine:
  - `src/sog_phase2/quality.py`
- Truth generation (truth-only quality section):
  - `scripts/generate_phase2_truth.py`
- Observed generation (full quality section, including ER metrics):
  - `scripts/generate_phase2_observed.py`

## Output Location
- `phase2/runs/<run_id>/quality_report.json`

`phase2_quality` now contains Step-9 metrics.
