# SOG Professor Walkthrough - Run Everything, See Everything

This is a top-to-bottom classroom/demo walkthrough for the Synthetic Occupancy
Generator (SOG). It is written for PowerShell and assumes every command is run
from the repository root:

```powershell
cd C:\path\to\Synthetic-Occupancy-Generator-data_pipeline-main
```

The goal is not only to run the pipeline, but to show what each command changes
and where to look for evidence afterward.

## 0. Confirm The Starting Point

Command:

```powershell
Get-Location
python --version
Get-ChildItem
```

What it does:

- Confirms you are in the repo root.
- Confirms Python is available.
- Shows the top-level folders: `phase1`, `phase2`, `frontend`, `docs`, and
  `tests`.

What to expect:

- `Get-Location` should end at the repository folder.
- Python should be 3.10 or newer.
- You should see `requirements.txt`, `README.md`, `phase1`, and `phase2`.

## 1. Create And Activate A Local Environment

Command:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

What it does:

- Creates an isolated Python environment under `.venv`.
- Installs all runtime, frontend, and test dependencies from
  `requirements.txt`.

What to expect:

- Your prompt usually shows `(.venv)` after activation.
- `pip install` should finish without dependency errors.
- You only need to repeat this when dependencies change or you create a fresh
  checkout.

If PowerShell blocks activation, run this once in the same terminal:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
.\.venv\Scripts\Activate.ps1
```

## 2. Build The Phase 1 Prepared Cache

Command:

```powershell
python phase1/scripts/build_prepared.py
```

What it does:

- Reads the raw Phase 1 source files under `phase1/Addresses`,
  `phase1/Names`, and `phase1/Data`.
- Normalizes those sources into a generated cache under `phase1/prepared`.
- Writes `phase1/prepared/prepared_manifest.json`.

What to expect:

- The command prints a JSON manifest.
- `phase1/prepared` contains parquet/json files used by Phase 1 generation.
- This command is required on a fresh clone and should be rerun when raw source
  CSVs change.

See the result:

```powershell
Get-ChildItem phase1/prepared
Get-Content phase1/prepared/prepared_manifest.json
```

The important files are:

- `first_names.parquet`
- `last_names.parquet`
- `streets.parquet`
- `cities.parquet`
- `states.parquet`
- `demographics.json`
- `nicknames.json`
- `prepared_manifest.json`

## 3. Generate The Phase 1 Baseline Population

Command:

```powershell
python phase1/scripts/generate_phase1.py --overwrite
```

What it does:

- Reads `phase1/configs/phase1.yaml`.
- Uses the prepared cache from Step 2.
- Generates the baseline synthetic people-and-address records.
- Writes output files under `phase1/outputs`.

What to expect:

- The command prints a JSON result with output paths and counts.
- The default config creates 10,000 people and requests 14,000 base records.
- Actual rows can be higher than the requested base count because nickname
  formal-backup rows can be appended.

Primary output files:

- `phase1/outputs/Phase1_people_addresses.csv`
- `phase1/outputs/Phase1_people_addresses.manifest.json`
- `phase1/outputs/Phase1_people_addresses.quality_report.json`

See the result:

```powershell
Get-ChildItem phase1/outputs

Import-Csv phase1/outputs/Phase1_people_addresses.csv |
  Select-Object -First 5 |
  Format-Table -AutoSize

$phase1Rows = Import-Csv phase1/outputs/Phase1_people_addresses.csv
$phase1Rows.Count
($phase1Rows | Select-Object -ExpandProperty PersonKey -Unique).Count

Get-Content phase1/outputs/Phase1_people_addresses.manifest.json
Get-Content phase1/outputs/Phase1_people_addresses.quality_report.json
```

What to look for:

- `RecordKey` is unique per output row.
- `PersonKey` identifies the real synthetic person and can repeat.
- `FirstName` may differ from `FormalFirstName` when nickname behavior is used.
- `records_requested`, `records_written`, and
  `formal_copy_records_added` explain why final row count can exceed the
  configured request.

## 4. Copy Phase 1 Into The Canonical Phase 2 Input Folder

Command:

```powershell
Copy-Item phase1/outputs/Phase1_people_addresses.csv phase1/outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item phase1/outputs/Phase1_people_addresses.manifest.json phase1/outputs_phase1/Phase1_people_addresses.manifest.json -Force
Copy-Item phase1/outputs/Phase1_people_addresses.quality_report.json phase1/outputs_phase1/Phase1_people_addresses.quality_report.json -Force
```

What it does:

- Promotes the latest Phase 1 output to the canonical input location consumed by
  Phase 2 scenarios.

What to expect:

- `phase1/outputs_phase1` contains the same three current baseline files.
- Scenario YAML files under `phase2/scenarios` point to this location.

See the result:

```powershell
Get-ChildItem phase1/outputs_phase1
```

## 5. Build Or Refresh Phase 2 Parameter Tables

Command:

```powershell
python phase2/scripts/build_phase2_params.py
```

What it does:

- Builds the Phase 2 priors package under `phase2/Data/phase2_params`.
- These tables drive mobility, household, marriage/divorce, fertility, and
  source-prior behavior in Phase 2.

What to expect:

- The command completes without errors.
- `phase2/Data/phase2_params/manifest.json` is refreshed.
- Existing CSV/JSON parameter tables remain in that folder.

See the result:

```powershell
Get-ChildItem phase2/Data/phase2_params
Get-Content phase2/Data/phase2_params/manifest.json
```

## 6. See The Scenario Menu

Command:

```powershell
Get-Content phase2/scenarios/catalog.yaml
Get-ChildItem phase2/scenarios -Filter *.yaml
```

What it does:

- Shows every built-in Phase 2 scenario and its purpose.
- Shows the runnable scenario YAML files.

What to expect:

- Supported canonical scenarios include:
  - `single_movers`
  - `couple_merge`
  - `family_birth`
  - `divorce_custody`
  - `roommates_split`
  - `clean_baseline_linkage`
  - `high_noise_identity_drift`
  - `high_duplication_dedup`
  - `low_overlap_sparse_coverage`
  - `asymmetric_source_coverage`
  - `three_source_partial_overlap`
  - `name_change_lifecycle`
  - `death_survivor_persistence`
  - `adoption_blended_family`

Optional teaching view:

```powershell
Get-Content phase2/scenarios/_working_scenario_template.yaml
```

This commented template is the safest starting point when creating a new local
scenario. Files named `_working_*.yaml` are intentionally treated as working
templates instead of shipped catalog scenarios.

## 7. Run One Complete Phase 2 Scenario

Command:

```powershell
python phase2/scripts/run_phase2_pipeline.py --scenario single_movers --overwrite --rebuild-population
```

What it does:

- Runs the complete Phase 2 pipeline in one command:
  1. Loads `phase2/scenarios/single_movers.yaml`.
  2. Selects scenario participants from the Phase 1 baseline.
  3. Simulates the truth layer.
  4. Emits observed datasets.
  5. Computes quality and manifest metadata.
  6. Validates the output contract.
  7. Writes a per-run `README.md`.

What to expect:

- The command prints a JSON result.
- `validation_valid` should be `true`.
- `quality_status` should usually be `ok`.
- The default `single_movers` run folder is:

```text
phase2/runs/2026-03-10_single_movers_seed20260310
```

Why `--rebuild-population` is included:

- `--overwrite` replaces generated outputs.
- `--rebuild-population` also refreshes `scenario_population.parquet`.
- Use both when demonstrating from a changed scenario YAML.

## 8. Inspect The Phase 2 Run Folder

Command:

```powershell
$runId = '2026-03-10_single_movers_seed20260310'
$runDir = "phase2/runs/$runId"
Get-ChildItem $runDir
```

What it does:

- Lists everything created by the Phase 2 run.

What to expect:

- Truth-layer parquet files.
- Observed dataset CSVs.
- Answer-key CSVs.
- Metadata JSON files.
- A human-readable run `README.md`.

Important files:

- `truth_people.parquet`
- `truth_households.parquet`
- `truth_household_memberships.parquet`
- `truth_residence_history.parquet`
- `truth_events.parquet`
- `DatasetA.csv`
- `DatasetB.csv`
- `entity_record_map.csv`
- `truth_crosswalk.csv`
- `masterDataset.csv`
- `scenario.yaml`
- `scenario_selection_log.json`
- `manifest.json`
- `quality_report.json`
- `README.md`

Read the auto-generated run summary:

```powershell
Get-Content "$runDir/README.md"
```

Inspect observed source files:

```powershell
Import-Csv "$runDir/DatasetA.csv" |
  Select-Object -First 5 |
  Format-Table -AutoSize

Import-Csv "$runDir/DatasetB.csv" |
  Select-Object -First 5 |
  Format-Table -AutoSize
```

Inspect the canonical answer key:

```powershell
Import-Csv "$runDir/entity_record_map.csv" |
  Select-Object -First 10 |
  Format-Table -AutoSize
```

How to explain it:

- `entity_record_map.csv` maps every observed `RecordKey` back to the true
  `PersonKey`.
- Two observed records refer to the same real person if they share the same
  `PersonKey`.
- This is the safest answer key for clustering and entity-resolution evaluation.

Inspect the pairwise crosswalk:

```powershell
Import-Csv "$runDir/truth_crosswalk.csv" |
  Select-Object -First 10 |
  Format-Table -AutoSize
```

How to explain it:

- `truth_crosswalk.csv` is the backward-compatible two-file A/B view.
- When duplicates exist, prefer `entity_record_map.csv` for complete cluster
  truth.

Inspect the combined master dataset:

```powershell
Import-Csv "$runDir/masterDataset.csv" |
  Select-Object -First 15 |
  Format-Table -AutoSize
```

How to explain it:

- `masterDataset.csv` stacks observed source rows and residence-timeline rows.
- It includes `PersonKey`, so it is useful for teaching how observed records
  connect back to truth.

Inspect truth-layer parquet counts:

```powershell
@'
from pathlib import Path
import pandas as pd

run_dir = Path("phase2/runs/2026-03-10_single_movers_seed20260310")
for name in [
    "truth_people",
    "truth_households",
    "truth_household_memberships",
    "truth_residence_history",
    "truth_events",
]:
    df = pd.read_parquet(run_dir / f"{name}.parquet")
    print(f"{name}: {len(df):,} rows")
    print(df.head(3).to_string(index=False))
    print()
'@ | python -
```

What to expect:

- `truth_people.parquet` represents clean real people.
- `truth_events.parquet` shows simulated life/household events.
- Observed CSVs are messy views derived from this truth.

## 9. Validate The Run Explicitly

Command:

```powershell
python phase2/scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
```

What it does:

- Checks that required run artifacts exist.
- Checks the output contract for the selected run.

What to expect:

- The command prints JSON.
- `"valid": true` means the run package is structurally valid.
- Missing files or contract issues appear in the JSON details.

Also inspect quality and manifest:

```powershell
Get-Content "$runDir/manifest.json"
Get-Content "$runDir/quality_report.json"
Get-Content "$runDir/scenario_selection_log.json"
```

What to look for:

- `manifest.json` tells you which files were generated and which inputs were
  used.
- `quality_report.json` reports truth consistency, observed coverage, overlap,
  duplicate rates, and status.
- `scenario_selection_log.json` shows selected participants, filter survivor
  counts, seed, and checksum.

## 10. Run The Same Scenario Stage By Stage

Use this section when teaching the pipeline internals. It produces the same kind
of run package, but exposes the main stages separately.

Command:

```powershell
python phase2/scripts/generate_phase2_truth.py --run-id 2026-03-10_single_movers_seed20260310 --overwrite --rebuild-population
python phase2/scripts/generate_phase2_observed.py --run-id 2026-03-10_single_movers_seed20260310 --overwrite
python phase2/scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
```

What it does:

- `generate_phase2_truth.py` selects participants and writes the truth layer.
- `generate_phase2_observed.py` emits noisy observed datasets and answer keys.
- `validate_phase2_outputs.py` validates the finished run folder.

What to expect:

- Truth parquet files exist after the first command.
- Observed CSVs and answer keys exist after the second command.
- Validation JSON reports `"valid": true` after the third command.

## 11. Run Every Built-In Scenario

Command:

```powershell
$scenarios = @(
  'single_movers',
  'couple_merge',
  'family_birth',
  'divorce_custody',
  'roommates_split',
  'clean_baseline_linkage',
  'high_noise_identity_drift',
  'high_duplication_dedup',
  'low_overlap_sparse_coverage',
  'asymmetric_source_coverage',
  'three_source_partial_overlap',
  'name_change_lifecycle',
  'death_survivor_persistence',
  'adoption_blended_family'
)

foreach ($scenario in $scenarios) {
  Write-Host "Running $scenario"
  python phase2/scripts/run_phase2_pipeline.py --scenario $scenario --overwrite --rebuild-population --no-progress
}
```

What it does:

- Runs the full Phase 2 pipeline for each canonical shipped scenario YAML.

What to expect:

- Each scenario prints a JSON result.
- Each result should include `"validation_valid": true`.
- Run folders are created or refreshed under `phase2/runs`.

See the generated run folders:

```powershell
Get-ChildItem phase2/runs -Directory |
  Select-Object Name, LastWriteTime |
  Sort-Object Name
```

Refresh human-readable run summaries:

```powershell
python phase2/scripts/write_run_readmes.py
```

## 12. Compare Scenario Outputs

Command:

```powershell
@'
from pathlib import Path
import json

runs_root = Path("phase2/runs")
for manifest_path in sorted(runs_root.glob("*/manifest.json")):
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    quality_path = manifest_path.parent / "quality_report.json"
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    datasets = manifest.get("observed_outputs", {}).get("datasets", [])
    dataset_ids = ",".join(d.get("dataset_id", "") for d in datasets)
    truth_counts = quality.get("truth_counts", {})
    print(
        f"{manifest['run_id']} | datasets={dataset_ids} | "
        f"people={truth_counts.get('truth_people')} | "
        f"events={truth_counts.get('truth_events')} | "
        f"status={quality.get('status')}"
    )
'@ | python -
```

What it does:

- Reads every run's manifest and quality report.
- Prints a compact comparison line for each scenario.

What to expect:

- Pairwise scenarios show datasets such as `A,B`.
- The multi-source scenario shows three datasets.
- The dedup scenario shows one dataset.
- `status` should usually be `ok`.

## 13. Run Tests For Confidence

Fast Phase 1 check:

```powershell
python -m pytest -q phase1/tests
```

Fast Phase 2 check:

```powershell
python -m pytest -q phase2/tests/test_phase2_scenario_regression.py
```

Full Phase 2 suite:

```powershell
python -m pytest -q phase2/tests
```

Frontend tests:

```powershell
python -m pytest -q tests
```

Full repository suite:

```powershell
python -m pytest -q
```

What it does:

- Confirms the pipeline logic, scenario contracts, output contracts, and
  frontend helper behavior still pass.

What to expect:

- Pytest should report passing tests.
- The full suite takes longer than the focused checks.

If Windows numerical libraries fail with a thread-start error, retry with:

```powershell
$env:OMP_NUM_THREADS='1'
$env:OPENBLAS_NUM_THREADS='1'
$env:MKL_NUM_THREADS='1'
$env:NUMEXPR_NUM_THREADS='1'
python -m pytest -q
```

## 14. Optional: Launch The Frontend

Command:

```powershell
.\run_frontend.ps1
```

Manual equivalent:

```powershell
python -u -m streamlit run frontend/chatbot_production.py --server.headless true
```

What it does:

- Starts the local Streamlit frontend.
- Uses the production frontend entrypoint.

What to expect:

- Streamlit prints a local URL.
- Open the URL in a browser to inspect and drive runs from the UI.
- Press `Ctrl+C` in the terminal to stop the server.

## 15. What Not To Commit

Generated artifacts are useful for demos but normally should not be committed:

- `phase1/prepared`
- `phase1/outputs`
- `phase1/outputs_phase1/*.csv`
- `phase1/outputs_phase1/*.json`
- `phase2/runs/*`
- `phase2/.sog_*`

The committed source of truth is the code, configs, scenario YAMLs, docs, and
tests. Generated files can be reproduced by rerunning the commands above.

## Troubleshooting Quick Reference

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `ModuleNotFoundError` | Dependencies are not installed in the active environment. | Activate `.venv`, then run `python -m pip install -r requirements.txt`. |
| Phase 1 says prepared cache is missing | Step 2 was skipped. | Run `python phase1/scripts/build_prepared.py`. |
| Phase 1 output already exists | The output path is already populated. | Add `--overwrite` or change `phase1.output.path`. |
| Phase 2 cannot find Phase 1 input | `phase1/outputs_phase1` is missing the canonical baseline. | Repeat Step 4. |
| Scenario edits seem ignored | Existing `scenario_population.parquet` was reused. | Rerun Phase 2 with `--rebuild-population`. |
| Validation reports missing observed files | Truth ran but observed emission did not finish. | Run `python phase2/scripts/generate_phase2_observed.py --run-id <run_id> --overwrite`. |
| Full tests fail with thread-start errors on Windows | Numerical libraries tried to open too many threads. | Set the four thread environment variables in Step 13 and retry. |

## One-Screen Demo Script

Use this when you want the shortest complete demonstration:

```powershell
python -m pip install -r requirements.txt
python phase1/scripts/build_prepared.py
python phase1/scripts/generate_phase1.py --overwrite
Copy-Item phase1/outputs/Phase1_people_addresses.csv phase1/outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item phase1/outputs/Phase1_people_addresses.manifest.json phase1/outputs_phase1/Phase1_people_addresses.manifest.json -Force
Copy-Item phase1/outputs/Phase1_people_addresses.quality_report.json phase1/outputs_phase1/Phase1_people_addresses.quality_report.json -Force
python phase2/scripts/build_phase2_params.py
python phase2/scripts/run_phase2_pipeline.py --scenario single_movers --overwrite --rebuild-population
python phase2/scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
Get-Content phase2/runs/2026-03-10_single_movers_seed20260310/README.md
```

Expected end state:

- Phase 1 baseline files exist under `phase1/outputs` and
  `phase1/outputs_phase1`.
- Phase 2 parameter files exist under `phase2/Data/phase2_params`.
- The `single_movers` run exists under `phase2/runs`.
- Validation reports `"valid": true`.
- The run `README.md` explains the generated artifacts and counts.
