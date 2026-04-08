# SOG Synthetic Data Pipeline

SOG is a reproducible synthetic data pipeline for entity-resolution benchmarking.

It has two phases:

1. Phase 1 generates a baseline person-and-address population.
2. Phase 2 simulates household and life events, then emits one or more observed datasets plus truth mappings.

## Start Here

- `docs/HANDOFF.md`: final ownership-transfer summary and operational starting points
- `phase1/README.md`: practical Phase-1 setup and baseline generation
- `phase2/README.md`: practical Phase-2 run sequence
- `docs/README.md`: documentation index
- `docs/SOG_COMPLETE_USER_GUIDE.md`: end-to-end guide
- `docs/SCENARIO_USE_CASES_AND_TESTING.md`: scenario selection and benchmark workflow
- `docs/FRONTEND_RUNBOOK.md`: local Streamlit frontend guide

## Repository Layout

```text
SOG/
|-- phase1/                     # Phase-1 raw inputs, config, scripts, source, tests
|   |-- Addresses/             # reference address CSVs
|   |-- Names/                 # reference name and nickname files
|   |-- Data/                  # reference demographic data
|   |-- configs/phase1.yaml    # main Phase-1 control file
|   |-- scripts/               # Phase-1 CLI entrypoints
|   |-- src/sog_phase1/        # Phase-1 implementation
|   |-- prepared/              # generated cache, gitignored
|   |-- outputs/               # generated Phase-1 outputs, gitignored
|   `-- outputs_phase1/        # canonical baseline location for Phase-2, data gitignored
|-- Data/phase2_params/         # source-backed Phase-2 parameter tables
|-- scripts/                    # Phase-2 CLI entrypoints
|-- src/sog_phase2/             # Phase-2 implementation
|-- phase2/
|   |-- scenarios/             # canonical scenario YAML files
|   |-- runs/                  # generated Phase-2 run folders, gitignored except .gitkeep
|   `-- .sog_*/                # local frontend/runtime state, gitignored
|-- frontend/                   # Streamlit app, agents, charts, and helpers
|-- tests/                      # Phase-2 and frontend tests
|-- examples/                   # small usage examples
|-- docs/                       # active docs, reference docs, and archive material
|-- requirements.txt
|-- requirements-dev.txt
`-- run_frontend.ps1
```

## Quick Start

### 1. Install dependencies

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 2. Build the Phase-1 baseline

```powershell
python phase1/scripts/build_prepared.py
python phase1/scripts/generate_phase1.py --overwrite
Copy-Item phase1/outputs/Phase1_people_addresses.csv phase1/outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item phase1/outputs/Phase1_people_addresses.manifest.json phase1/outputs_phase1/Phase1_people_addresses.manifest.json -Force
Copy-Item phase1/outputs/Phase1_people_addresses.quality_report.json phase1/outputs_phase1/Phase1_people_addresses.quality_report.json -Force
```

### 3. Build Phase-2 parameter tables

```powershell
python scripts/build_phase2_params.py
```

### 4. Run one Phase-2 scenario

```powershell
python scripts/generate_phase2_truth.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/generate_phase2_observed.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
```

## Frontend

Launch the local Streamlit frontend from the repository root:

```powershell
.\run_frontend.ps1
```

Manual entrypoint:

```powershell
python -u -m streamlit run frontend/chatbot_production.py --server.headless true
```

## Tests

Run the full suite:

```powershell
python -m pytest -q
```

Run the Phase-2 scenario regression suite only:

```powershell
python -m pytest -q tests/test_phase2_scenario_regression.py
```

## Notes Before Pushing

- `phase1/prepared/`, `phase1/outputs/`, `phase1/outputs_phase1/*.csv`, `phase2/runs/`, and `phase2/.sog_*/` are generated artifacts and should not be committed.
- `phase1/outputs_phase1/README.md` and `phase2/runs/.gitkeep` are intentionally kept so the directory layout remains understandable after clone.
- The active frontend entrypoint is `frontend/chatbot_production.py`, which dispatches into `frontend/chatbot.py`.

## License

MIT. See `LICENSE`.
