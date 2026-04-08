# Phase 2 Runbook

This is the practical command guide for Phase 2.

Phase 2 takes the canonical Phase-1 baseline, selects participants for a scenario, simulates truth events, emits observed datasets, and validates the run package.

## Required Inputs

- `phase1/outputs_phase1/Phase1_people_addresses.csv`
- `phase1/outputs_phase1/Phase1_people_addresses.manifest.json`
- `phase2/scenarios/<scenario>.yaml`
- `Data/phase2_params/*`

## One-Time Setup

If the Phase-1 canonical baseline does not exist yet:

```powershell
python phase1/scripts/build_prepared.py
python phase1/scripts/generate_phase1.py --overwrite
Copy-Item phase1/outputs/Phase1_people_addresses.csv phase1/outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item phase1/outputs/Phase1_people_addresses.manifest.json phase1/outputs_phase1/Phase1_people_addresses.manifest.json -Force
Copy-Item phase1/outputs/Phase1_people_addresses.quality_report.json phase1/outputs_phase1/Phase1_people_addresses.quality_report.json -Force
```

Build or refresh the Phase-2 priors package:

```powershell
python scripts/build_phase2_params.py
```

## Run One Scenario

Use a run id of the form `YYYY-MM-DD_<scenario_id>_seed<seed>`.

Example:

```powershell
python scripts/generate_phase2_truth.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/generate_phase2_observed.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
```

Add `--overwrite` to the truth or observed command if you want to replace an existing run folder.

## What Gets Written

Each run lands under `phase2/runs/<run_id>/` and may include:

- `truth_people.parquet`
- `truth_households.parquet`
- `truth_household_memberships.parquet`
- `truth_residence_history.parquet`
- `truth_events.parquet`
- one or more observed dataset CSVs
- `entity_record_map.csv`
- `truth_crosswalk.csv` for pairwise two-dataset runs
- `scenario.yaml`
- `scenario_selection_log.json`
- `manifest.json`
- `quality_report.json`

`phase2/runs/` is generated output and should not be committed.

## Commands by Stage

Build deterministic scenario population only:

```powershell
python scripts/build_phase2_scenario_population.py --run-id <run_id> --overwrite
```

Run truth simulation:

```powershell
python scripts/generate_phase2_truth.py --run-id <run_id> --overwrite
```

Emit observed datasets:

```powershell
python scripts/generate_phase2_observed.py --run-id <run_id> --overwrite
```

Validate the run package:

```powershell
python scripts/validate_phase2_outputs.py --run-id <run_id>
```

## Tests

```powershell
python -m pytest -q tests/test_phase2_scenario_regression.py
```

See `phase2/scenarios/README.md` for the scenario YAML schema and `docs/SCENARIO_USE_CASES_AND_TESTING.md` for the benchmark-oriented scenario guide.
