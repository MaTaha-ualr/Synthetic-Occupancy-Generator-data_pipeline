# Phase-2 Runbook (Practical Guide)

This README is the fast, practical guide for running Phase-2 without digging through all design docs.

If you are feeling that there is a gap between steps, use this file as your "do this next" checklist.

---

## 1) Mental Model (One Minute)

Phase-2 is a 4-step pipeline:

1. **Baseline input**: Phase-1 canonical data (`outputs_phase1/Phase1_people_addresses.csv` + manifest).
2. **Truth simulation**: event-driven timeline (MOVE, COHABIT, BIRTH, DIVORCE, LEAVE_HOME).
3. **Observed emission**: create `DatasetA.csv`, `DatasetB.csv`, and `truth_crosswalk.csv`.
4. **Validation/quality**: ensure contract and consistency checks pass.

Keep this rule in mind:

- One Phase-1 baseline can be reused for many Phase-2 scenarios.

---

## 2) Folder Structure You Actually Need

```text
phase2/
  scenarios/
    single_movers.yaml
    couple_merge.yaml
    family_birth.yaml
    divorce_custody.yaml
    roommates_split.yaml
  runs/
    <run_id>/
      truth_*.parquet
      DatasetA.csv
      DatasetB.csv
      truth_crosswalk.csv
      scenario.yaml
      scenario_selection_log.json
      manifest.json
      quality_report.json
```

---

## 3) Prerequisites

From repo root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## 4) One-Time Baseline Setup (Phase-1)

If baseline files already exist in `outputs_phase1/`, you can skip this section.

```powershell
python scripts/build_prepared.py --raw-root . --prepared-dir prepared
python scripts/generate_phase1.py --config configs/phase1.yaml --prepared-dir prepared --overwrite
```

Copy Phase-1 output to canonical Phase-2 input location:

```powershell
Copy-Item outputs/Phase1_people_addresses.csv outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item outputs/Phase1_people_addresses.manifest.json outputs_phase1/Phase1_people_addresses.manifest.json -Force
Copy-Item outputs/Phase1_people_addresses.quality_report.json outputs_phase1/Phase1_people_addresses.quality_report.json -Force
```

Build/rebuild Phase-2 parameter tables:

```powershell
python scripts/build_phase2_params.py
```

---

## 5) Run One Scenario (Recommended Pattern)

Use run-id format:

`YYYY-MM-DD_<scenario_id>_seed<seed>`

Example (`single_movers`):

```powershell
python scripts/generate_phase2_truth.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/generate_phase2_observed.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
```

If files already exist, add `--overwrite` to truth/observed commands.

---

## 6) Run All Existing Scenarios

Current scenario run-ids:

- `2026-03-10_single_movers_seed20260310`
- `2026-03-11_couple_merge_seed20260311`
- `2026-03-12_family_birth_seed20260312`
- `2026-03-13_divorce_custody_seed20260313`
- `2026-03-14_roommates_split_seed20260314`

Run each with:

```powershell
python scripts/generate_phase2_truth.py --run-id <run_id>
python scripts/generate_phase2_observed.py --run-id <run_id>
python scripts/validate_phase2_outputs.py --run-id <run_id>
```

---

## 7) What Each Script Does

### `scripts/build_phase2_params.py`

- Builds `Data/phase2_params/*` from public-source priors.

### `scripts/build_phase2_scenario_population.py`

- Deterministic participant selection only.
- Writes `scenario_population.parquet` + `scenario_selection_log.json`.

### `scripts/generate_phase2_truth.py`

- Runs selection (if needed) + truth simulation.
- Writes `truth_people.parquet`, `truth_households.parquet`, `truth_household_memberships.parquet`,
  `truth_residence_history.parquet`, `truth_events.parquet`, plus metadata files.

### `scripts/generate_phase2_observed.py`

- Emits `DatasetA.csv`, `DatasetB.csv`, `truth_crosswalk.csv`.
- Recomputes quality report with ER metrics.

### `scripts/validate_phase2_outputs.py`

- Contract + schema + metadata + constraint validation for a run folder.

---

## 8) Core Inputs and Outputs

### Inputs

- `outputs_phase1/Phase1_people_addresses.csv`
- `outputs_phase1/Phase1_people_addresses.manifest.json`
- `phase2/scenarios/<scenario>.yaml`
- `Data/phase2_params/*`

### Outputs

- `phase2/runs/<run_id>/` containing truth, observed, and metadata artifacts.

---

## 9) Common Confusions (and Fixes)

### "I changed scenario YAML seed; run-id seed mismatch?"

- System uses run-id as execution seed.
- Run-local `scenario.yaml` is resolved and written with the run seed.

### "Why is there no `outputs_phase1` data?"

- Phase-2 expects canonical baseline there.
- Copy from `outputs/` after Phase-1 generation (commands in Section 4).

### "Command says outputs already exist"

- Add `--overwrite` for truth/observed generation scripts.

### "Validator fails"

Check:

1. You ran truth before observed.
2. Run-id matches folder naming.
3. Scenario and manifest metadata are aligned.

---

## 10) Verification Commands

Run all tests:

```powershell
python -m pytest -q
```

Run scenario regression tests only:

```powershell
python -m pytest -q tests/test_phase2_scenario_regression.py
```

---

## 11) Minimal "From Scratch" Sequence

If you want the shortest complete sequence:

```powershell
python scripts/build_prepared.py --raw-root . --prepared-dir prepared
python scripts/generate_phase1.py --config configs/phase1.yaml --prepared-dir prepared --overwrite
Copy-Item outputs/Phase1_people_addresses.csv outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item outputs/Phase1_people_addresses.manifest.json outputs_phase1/Phase1_people_addresses.manifest.json -Force
python scripts/build_phase2_params.py
python scripts/generate_phase2_truth.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/generate_phase2_observed.py --run-id 2026-03-10_single_movers_seed20260310
python scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
```

---

## 12) Where to Go Next

- Full detailed guide: `docs/SOG_COMPLETE_USER_GUIDE.md`
- Scenario schema notes: `phase2/scenarios/README.md`
- Output contract: `docs/PHASE2_STEP1_OUTPUT_CONTRACT.md`

