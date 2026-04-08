# Phase 1 Runbook

Phase 1 builds the baseline synthetic person-and-address population that Phase 2 consumes.

## What Lives Here

- `Addresses/`, `Names/`, `Data/`: reference input files
- `configs/phase1.yaml`: main generation settings
- `scripts/build_prepared.py`: raw inputs to prepared cache
- `scripts/generate_phase1.py`: prepared cache to output dataset
- `src/sog_phase1/`: implementation
- `tests/`: Phase-1 tests
- `prepared/`: generated cache, gitignored
- `outputs/`: generated run output, gitignored
- `outputs_phase1/`: canonical baseline copy for Phase-2, data gitignored

## Commands

From repository root:

```powershell
python phase1/scripts/build_prepared.py
python phase1/scripts/generate_phase1.py --overwrite
```

Copy the latest Phase-1 output into the canonical Phase-2 baseline location:

```powershell
Copy-Item phase1/outputs/Phase1_people_addresses.csv phase1/outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item phase1/outputs/Phase1_people_addresses.manifest.json phase1/outputs_phase1/Phase1_people_addresses.manifest.json -Force
Copy-Item phase1/outputs/Phase1_people_addresses.quality_report.json phase1/outputs_phase1/Phase1_people_addresses.quality_report.json -Force
```

## Configuration

Edit `phase1/configs/phase1.yaml` to change:

- population size
- record redundancy
- nickname behavior
- gender and age distributions
- address and mailing behavior
- output format and path

The default output path in the config is `outputs/Phase1_people_addresses.csv`, which resolves inside the `phase1/` directory when you run the Phase-1 script.

## Testing

```powershell
python -m pytest -q phase1/tests/test_phase1_pipeline.py
```
