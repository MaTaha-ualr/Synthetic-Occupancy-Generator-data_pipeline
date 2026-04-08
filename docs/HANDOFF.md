# SOG Handoff Note

## Purpose

This file is the shortest practical summary for the next owner of the repository.

Use it when you need to understand:

- what the project ships
- where the active entrypoints are
- what is intentionally versioned versus generated locally
- how the repository was validated at handoff

## What Ships

SOG is a deterministic synthetic data pipeline for entity-resolution benchmarking.

- `phase1/` builds the baseline synthetic person-and-address population.
- `phase2/` selects scenario participants, simulates truth events, emits observed datasets, and validates run artifacts.
- `frontend/` provides a local Streamlit control surface for scenario drafting, execution, charting, and export.

## Primary Entry Points

- Phase 1 setup and generation: `phase1/README.md`
- Phase 2 execution: `phase2/README.md`
- Frontend runtime: `frontend/chatbot_production.py`
- Frontend launcher: `run_frontend.ps1`
- Scenario YAML schema: `phase2/scenarios/README.md`
- Scenario benchmark guidance: `docs/SCENARIO_USE_CASES_AND_TESTING.md`
- Technical deep dive: `docs/SOG_TECHNICAL_WALKTHROUGH.md`

## Validation Snapshot At Handoff

- Date: April 8, 2026
- Branch: `main`
- Final handoff commit series anchored by `ddb4911`
- Test result before handoff push: `322 passed`
- Command used: `python -m pytest -q`

## Important Repository Rules

- `phase1/prepared/`, `phase1/outputs/`, `phase1/outputs_phase1/*.csv`, `phase2/runs/`, and `phase2/.sog_*/` are generated artifacts and should not be committed.
- `phase1/outputs_phase1/README.md` is intentionally versioned so the canonical baseline location remains visible after clone.
- `phase2/runs/.gitkeep` is intentionally versioned so the generated run root remains visible after clone.
- Archived design and ops notes were moved under `docs/archive/`.

## How To Recreate A Clean Local Working State

From repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
python phase1/scripts/build_prepared.py
python phase1/scripts/generate_phase1.py --overwrite
Copy-Item phase1/outputs/Phase1_people_addresses.csv phase1/outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item phase1/outputs/Phase1_people_addresses.manifest.json phase1/outputs_phase1/Phase1_people_addresses.manifest.json -Force
Copy-Item phase1/outputs/Phase1_people_addresses.quality_report.json phase1/outputs_phase1/Phase1_people_addresses.quality_report.json -Force
python scripts/build_phase2_params.py
python -m pytest -q
```

## If You Need To Operate The Project Quickly

1. Read `README.md`.
2. Read `phase1/README.md` and `phase2/README.md`.
3. Read `docs/FRONTEND_RUNBOOK.md` if you plan to use the UI.
4. Use `docs/README.md` as the map for deeper documentation.

## If You Need To Change Behavior Safely

1. Update the relevant YAML or module.
2. Run targeted tests first.
3. Run `python -m pytest -q` before publishing.
4. Keep generated run data out of Git unless there is a deliberate reason to version it.
