# Phase-2 Step-2 Scenario Packaging Model (As of March 5, 2026)

## Decision
Use one baseline Phase-1 master dataset plus many scenario definitions and run outputs.

Do not store multiple giant master copies.

## Canonical Layout
```text
SOG/
|-- outputs_phase1/
|   |-- Phase1_people_addresses.csv
|   `-- Phase1_people_addresses.manifest.json
`-- phase2/
    |-- scenarios/
    |   |-- single_movers.yaml
    |   |-- couple_merge.yaml
    |   |-- family_birth.yaml
    |   |-- divorce_custody.yaml
    |   `-- roommates_split.yaml
    `-- runs/
        `-- YYYY-MM-DD_<scenario_id>_seed<seed>/
            |-- scenario.yaml
            |-- scenario_selection_log.json
            |-- manifest.json
            |-- quality_report.json
            |-- truth_people.parquet
            |-- truth_households.parquet
            |-- truth_household_memberships.parquet
            |-- truth_residence_history.parquet
            |-- truth_events.parquet
            |-- scenario_population.parquet
            |-- DatasetA.csv
            |-- DatasetB.csv
            `-- truth_crosswalk.csv
```

## Reproducibility Rule
A run folder must be reproducible using only:
- Phase-1 CSV
- Phase-1 manifest
- resolved `scenario.yaml`
- seed

Enforcement:
- `scenario.yaml` must include `scenario_id`, `seed`, and `phase1.data_path` + `phase1.manifest_path`.
- `manifest.json` must include `run_id`, `scenario_id`, `seed`, `phase1_input_csv`, and `phase1_input_manifest`.
- `run_id` must match `YYYY-MM-DD_<scenario_id>_seed<seed>`.
- scenario and manifest values must agree.

Validator:
```bash
python scripts/validate_phase2_outputs.py --run-id <run_id>
```

## Next Step
Step-3 parameter layer is in:
- `docs/PHASE2_STEP3_PARAMETER_LAYER.md`
