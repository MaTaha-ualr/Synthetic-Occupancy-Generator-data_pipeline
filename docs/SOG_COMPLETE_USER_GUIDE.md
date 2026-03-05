# SOG Complete User Guide

Last updated: March 5, 2026

This document is the single, end-to-end guide for the SOG project in this repository. It explains:

- What the system is and why it is designed this way.
- How Phase-1 and Phase-2 fit together.
- How to run, validate, and test the pipeline.
- How to read outputs and quality reports.
- How to extend scenarios and avoid common mistakes.

If you are new to the project, start at Section 1 and follow in order. If you are already running scenarios, jump to Sections 7, 8, 9, and 13.

---

## 1) What SOG Is

SOG (Synthetic Occupancy Generator) is a synthetic data pipeline for:

1. Generating a baseline person-address dataset (Phase-1).
2. Simulating temporal life/household events on top of that baseline (Phase-2 truth layer).
3. Emitting two observed datasets (Dataset A, Dataset B) plus a truth crosswalk for entity-resolution benchmarking.

The project is designed for reproducibility and benchmarking. Every scenario run is deterministic given:

- the Phase-1 baseline file and manifest,
- the scenario definition YAML,
- and the seed.

---

## 2) Core Data Model and Terminology

Understanding these terms is critical.

### 2.1 Identity keys

- `PersonKey`: canonical truth entity identity.
- `RecordKey`: record-instance identity in datasets.
- `AddressKey`: address identity for a residence snapshot or interval.

### 2.2 Redundancy

Phase-1 can represent one `PersonKey` with multiple records (`EntityRecordIndex` progression).  
This is intentional and used to stress entity-resolution systems.

### 2.3 Truth vs observed

- **Truth layer**: normalized, latent simulation state and event history.
- **Observed layer**: what two systems would "see" after sampling, duplication, and noise.

Phase-2 simulates on truth entities first, then emits observed records.

---

## 3) End-to-End Architecture

SOG is split into major layers:

1. **Phase-1 baseline generation** (`outputs/Phase1_people_addresses.csv` + metadata).
2. **Phase-2 parameter layer** (public-source priors in `Data/phase2_params/`).
3. **Scenario selection layer** (deterministic participant selection and latent traits).
4. **Truth simulation layer** (event-driven timeline: MOVE, COHABIT, BIRTH, DIVORCE, LEAVE_HOME).
5. **Observed emission layer** (Dataset A/B + crosswalk under configurable overlap/cardinality/noise).
6. **Quality layer** (truth consistency + scenario metrics + ER benchmark metrics).
7. **Validation and regression tests** (contract validation and scenario behavior tests).

---

## 4) Repository Map by Responsibility

Key directories and files:

- `configs/phase1.yaml`: Phase-1 generation config.
- `outputs/`: Phase-1 generated artifacts (baseline + manifest + quality report).
- `Data/phase2_params/`: source-citable parameter tables loaded by Phase-2.
- `phase2/scenarios/*.yaml`: scenario definitions.
- `phase2/runs/<run_id>/`: per-run outputs and metadata.
- `src/sog_phase2/`:
  - `selection.py`: deterministic population selection + latent trait assignment.
  - `constraints.py`: eligibility and realism/novelty constraint checks.
  - `event_grammar.py`: truth event schema and validators.
  - `simulator.py`: truth event-driven simulator.
  - `emission.py`: observed A/B emission engine.
  - `quality.py`: Phase-2 quality report metrics.
  - `output_contract.py`: required artifact schemas + run validator.
- `scripts/`:
  - `build_phase2_params.py`
  - `build_phase2_scenario_population.py`
  - `generate_phase2_truth.py`
  - `generate_phase2_observed.py`
  - `validate_phase2_outputs.py`
- `tests/test_phase2_*.py`: unit + regression tests for all layers.

---

## 5) Environment Setup

### 5.1 Prerequisites

- Python 3.10+
- Git
- Terminal (PowerShell/CMD/bash)

### 5.2 Install

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
# or .\.venv\Scripts\Activate.ps1 on Windows PowerShell

pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## 6) Phase-1 Baseline (Canonical Input to Phase-2)

Phase-2 assumes the Phase-1 contract is frozen.

Canonical files:

- `outputs/Phase1_people_addresses.csv`
- `outputs/Phase1_people_addresses.manifest.json`
- `outputs/Phase1_people_addresses.quality_report.json`

Generate Phase-1 if needed:

```bash
python scripts/build_prepared.py --raw-root . --prepared-dir prepared
python scripts/generate_phase1.py --config configs/phase1.yaml --prepared-dir prepared --overwrite
```

Important design decision:

- Phase-2 consumes an **entity-centric view** (`PersonKey` truth people), not raw Phase-1 rows directly.

---

## 7) Phase-2 Quick Start

There are two CLI styles:

- **Preferred explicit style**: `--run-id`.
- **Legacy-compatible style**: `--scenario --seed --run-date` (truth script) and `--run` (observed/validate).

### 7.1 Run-id format

`YYYY-MM-DD_<scenario_id>_seed<seed>`

Example:

`2026-03-14_roommates_split_seed20260314`

### 7.2 Truth generation

Preferred:

```bash
python scripts/generate_phase2_truth.py --run-id 2026-03-14_roommates_split_seed20260314
```

Legacy-compatible:

```bash
python scripts/generate_phase2_truth.py \
  --scenario roommates_split \
  --seed 20260314 \
  --run-date 2026-03-14 \
  --phase1 outputs/Phase1_people_addresses.csv
```

### 7.3 Observed emission

Preferred:

```bash
python scripts/generate_phase2_observed.py --run-id 2026-03-14_roommates_split_seed20260314
```

Legacy-compatible:

```bash
python scripts/generate_phase2_observed.py --run phase2/runs/2026-03-14_roommates_split_seed20260314
```

### 7.4 Validation

Preferred:

```bash
python scripts/validate_phase2_outputs.py --run-id 2026-03-14_roommates_split_seed20260314
```

Legacy-compatible:

```bash
python scripts/validate_phase2_outputs.py --run phase2/runs/2026-03-14_roommates_split_seed20260314
```

### 7.5 Overwrite behavior

- Truth and observed scripts fail if outputs already exist.
- Add `--overwrite` to replace existing files.

---
Use this exact sequence from repo root h:\AAA_Taha\SOG_DATASETS\SOG.

1. Setup env + deps

..1..|python -m venv .venv
..2..|.\.venv\Scripts\Activate.ps1
..3..|pip install -r requirements.txt
..4..|pip install -r requirements-dev.txt

2. Build Phase-1 baseline (only needed if not already generated)
..1..|python scripts/build_prepared.py --raw-root . --prepared-dir prepared
..2..|python scripts/generate_phase1.py --config configs/phase1.yaml --prepared-dir prepared --overwrite
3. Run one Phase-2 scenario (example: roommates_split)
..1..|python scripts/generate_phase2_truth.py --run-id 2026-03-14_roommates_split_seed20260314
..2..|python scripts/generate_phase2_observed.py --run-id 2026-03-14_roommates_split_seed20260314
..3..|python scripts/validate_phase2_outputs.py --run-id 2026-03-14_roommates_split_seed20260314

4. Run tests
..1..|python -m pytest -q

## 8) Scenario Configuration (YAML) Deep Guide

Each `phase2/scenarios/<scenario_id>.yaml` contains:

- `scenario_id`
- `seed`
- `phase1`
- `parameters`
- `simulation`
- `emission`
- `quality`
- `selection`
- `constraints`

### 8.1 `phase1`

- `data_path`
- `manifest_path`

The scripts include fallback logic from `outputs_phase1/...` to `outputs/...` for compatibility.

### 8.2 `parameters` (event rates and scenario behavior)

Rates are annual percentages converted internally to step probabilities.

Common keys:

- `move_rate_pct`
- `cohabit_rate_pct`
- `birth_rate_pct`
- `divorce_rate_pct`
- `split_rate_pct` (maps to leave-home behavior)

Special keys in `roommates_split`:

- `roommate_group_share_pct`
- `roommate_household_size_min`
- `roommate_household_size_max`
- `roommate_age_min`
- `roommate_age_max`

### 8.3 `simulation`

- `granularity`: `monthly` or `daily` (monthly is default/recommended)
- `start_date`: `YYYY-MM-DD`
- `periods`: positive integer number of steps

### 8.4 `emission`

- `crossfile_match_mode`: `one_to_one | one_to_many | many_to_one | many_to_many`
- `overlap_entity_pct`
- `appearance_A_pct`
- `appearance_B_pct`
- `duplication_in_A_pct`
- `duplication_in_B_pct`
- Optional noise:
  - `noise.A.*`
  - `noise.B.*`

### 8.5 `selection`

- `sample.mode`: `all | count | pct`
- `sample.value`
- filters for demographics, residence type, redundancy profile, mobility bucket
- threshold controls for bucketing:
  - `mobility_low_max`, `mobility_high_min`
  - `trait_low_max`, `trait_high_min`

### 8.6 `constraints`

- `min_marriage_age`
- `max_partner_age_gap` or `partner_age_gap_distribution`
- `fertility_age_range.min/max`
- `allow_underage_marriage`
- `allow_child_lives_alone`
- `enforce_non_overlapping_residence_intervals`

### 8.7 `quality`

- `household_size_range.min`
- `household_size_range.max`

---

## 9) Truth Layer: What Gets Generated

Required truth artifacts in each run folder:

- `truth_people.parquet`
- `truth_households.parquet`
- `truth_household_memberships.parquet`
- `truth_residence_history.parquet`
- `truth_events.parquet`
- `scenario_population.parquet`

### 9.1 Event vocabulary

Active events:

- `MOVE`
- `COHABIT`
- `BIRTH`
- `DIVORCE`
- `LEAVE_HOME`

### 9.2 Consistency invariants

Simulator and quality checks enforce:

- non-overlapping residence intervals per person
- non-overlapping household membership intervals per person
- coupled partners colocated while active couple

---

## 10) Observed Layer: What ER Systems Consume

Each run emits:

- `DatasetA.csv`
- `DatasetB.csv`
- `truth_crosswalk.csv`

### 10.1 A vs B snapshot model

- Dataset A: baseline/system-A style snapshot at simulation start.
- Dataset B: later snapshot at simulation end.

### 10.2 Match cardinality modes

Controlled by `crossfile_match_mode`:

- `one_to_one`
- `one_to_many`
- `many_to_one`
- `many_to_many`

### 10.3 Crosswalk meaning

`truth_crosswalk.csv` links:

- `PersonKey`
- `A_RecordKey`
- `B_RecordKey`

This is the ground-truth mapping for ER scoring.

---

## 11) Output Contract and Validator

Contract is defined in `src/sog_phase2/output_contract.py`.

Validator checks:

1. Required files exist.
2. Required columns exist.
3. Truth event grammar validity.
4. Metadata consistency (`scenario.yaml`, `manifest.json`, `scenario_selection_log.json`).
5. Reproducibility alignment (run-id segments vs metadata).
6. Constraint validation status.

Run validator:

```bash
python scripts/validate_phase2_outputs.py --run-id <run_id>
```

---

## 12) Quality Report: How to Read It

`phase2/runs/<run_id>/quality_report.json` includes:

- top-level:
  - `truth_counts`
  - `simulation_quality`
  - `phase2_quality`
  - `constraints_validation`
  - `observed_quality`
  - `status`

### 12.1 `phase2_quality.truth_consistency`

- event age validity by rule
- time overlap errors
- household size constraints

### 12.2 `phase2_quality.scenario_metrics`

- event counts:
  - couples formed
  - divorces
  - births
  - moves
- moves-per-person distribution
- household type shares

### 12.3 `phase2_quality.er_benchmark_metrics`

- cross-file overlap achieved
- achieved cardinality counts
- within-file duplicate rates
- drift rates (name/address/phone) for A and B
- crosswalk ambiguity checks

---

## 13) Current Scenario Catalog and What Each Stresses

As of March 5, 2026:

1. `single_movers`
2. `couple_merge`
3. `family_birth`
4. `divorce_custody`
5. `roommates_split`

### 13.1 Emission profile summary

| Scenario | Match Mode | Overlap % | A Appearance % | B Appearance % | Dup A % | Dup B % |
|---|---|---:|---:|---:|---:|---:|
| single_movers | one_to_one | 70.0 | 85.0 | 90.0 | 4.0 | 6.0 |
| couple_merge | one_to_many | 65.0 | 80.0 | 90.0 | 3.0 | 12.0 |
| family_birth | many_to_one | 72.0 | 88.0 | 86.0 | 10.0 | 4.0 |
| divorce_custody | many_to_many | 60.0 | 82.0 | 88.0 | 14.0 | 14.0 |
| roommates_split | one_to_many | 58.0 | 80.0 | 88.0 | 12.0 | 24.0 |

### 13.2 Behavior profile summary

- `single_movers`: mobility-focused.
- `couple_merge`: cohabitation-focused.
- `family_birth`: birth-focused.
- `divorce_custody`: cohabit + divorce dynamics.
- `roommates_split`: high mobility + roommate baseline grouping + split stress for false-positive household signals.

---

## 14) Parameter Layer (Public-Source Priors)

`Data/phase2_params/` includes:

- `mobility_overall_acs_2024.csv`
- `mobility_by_age_cohort_acs_2024.csv`
- `marriage_divorce_rates_cdc_2023.csv`
- `fertility_by_age_nchs_2024.csv`
- `household_type_shares_acs_2024.csv`
- `phase2_priors_snapshot.json`
- `sources.json`
- `manifest.json`

Rebuild parameters:

```bash
python scripts/build_phase2_params.py
```

Notes:

- Pulls ACS values via Census API.
- Uses CDC/NCHS published rates for marriage/divorce/fertility priors.
- Stores source metadata for citation and reproducibility.

---

## 15) Selection Layer Details

Selection process:

1. Build entity view from Phase-1 rows.
2. Assign deterministic latent trait scores:
   - mobility
   - partnership
   - fertility
3. Bucket traits (`low`, `medium`, `high`).
4. Apply configured filters.
5. Deterministically sample (all/count/pct) with seed.

Outputs:

- `scenario_population.parquet`
- `scenario_selection_log.json` (counts + checksum + audit metadata)

---

## 16) Reproducibility Contract

A run is considered reproducible from:

- Phase-1 CSV
- Phase-1 manifest
- scenario YAML
- seed

Run packaging under:

`phase2/runs/<run_id>/`

Expected files include truth outputs, observed outputs, and metadata:

- `scenario.yaml`
- `scenario_selection_log.json`
- `manifest.json`
- `quality_report.json`

---

## 17) Testing Strategy and Commands

### 17.1 Full test suite

```bash
python -m pytest -q
```

### 17.2 Scenario regression suite

```bash
python -m pytest -q tests/test_phase2_scenario_regression.py
```

### 17.3 What regression tests protect

- scenario-specific event expectations
- co-residence logic checks
- crosswalk overlap math
- cardinality mode behavior
- roommates split dynamics and multi-member household patterns

---

## 18) Typical Workflows

### Workflow A: Generate one scenario run (preferred)

```bash
python scripts/generate_phase2_truth.py --run-id 2026-03-14_roommates_split_seed20260314
python scripts/generate_phase2_observed.py --run-id 2026-03-14_roommates_split_seed20260314
python scripts/validate_phase2_outputs.py --run-id 2026-03-14_roommates_split_seed20260314
```

### Workflow B: Generate with legacy-compatible flags

```bash
python scripts/generate_phase2_truth.py \
  --scenario roommates_split \
  --seed 20260314 \
  --run-date 2026-03-14 \
  --phase1 outputs/Phase1_people_addresses.csv

python scripts/generate_phase2_observed.py --run phase2/runs/2026-03-14_roommates_split_seed20260314
python scripts/validate_phase2_outputs.py --run phase2/runs/2026-03-14_roommates_split_seed20260314
```

---

## 19) Troubleshooting

### "Output file exists"

Use `--overwrite` on truth/observed scripts.

### "run_id must match pattern"

Use: `YYYY-MM-DD_<scenario_id>_seed<seed>`.

### "Scenario YAML scenario_id mismatch"

The scenario id in YAML must match the run id scenario segment.

### Missing Phase-1 files under `outputs_phase1`

The scripts include compatibility fallback to `outputs/Phase1_people_addresses*.json/csv`.  
You can also explicitly pass `--phase1` and `--phase1-manifest`.

### Validation fails on metadata consistency

Check these are aligned for the run:

- `scenario.yaml`
- `manifest.json`
- `scenario_selection_log.json`

---

## 20) How to Add a New Scenario

1. Copy an existing file in `phase2/scenarios/`.
2. Set a unique `scenario_id` and default `seed`.
3. Configure:
   - `parameters`
   - `selection`
   - `constraints`
   - `simulation`
   - `emission`
   - `quality`
4. Run truth + observed generation.
5. Validate with `validate_phase2_outputs.py`.
6. Add scenario regression test coverage in `tests/test_phase2_scenario_regression.py`.

Recommendation:

- Start with one deterministic seed.
- Ensure at least one non-trivial event occurs for the scenario.
- Confirm crosswalk metrics match intended overlap/cardinality behavior.

---

## 21) Known Design Choices and Limits

- Simulator is stochastic but deterministic under seed.
- `monthly` simulation is default for cost/performance.
- Event grammar currently includes minimum active set (MOVE/COHABIT/BIRTH/DIVORCE/LEAVE_HOME).
- Optional-later events (DEATH/NAME_CHANGE/ADOPTION) are defined but not enabled in minimum grammar checks.
- Observed emission is snapshot-based (A at start, B at end), not full longitudinal export.

---

## 22) Glossary

- **Entity view**: one row per `PersonKey` truth person.
- **Record view**: potentially many rows per `PersonKey`.
- **Crosswalk**: ground-truth mapping from person/entity to record ids across datasets.
- **Overlap**: entities appearing in both A and B.
- **Cardinality**: how many records per entity across files (1-1, 1-many, many-1, many-many).
- **Drift**: attribute differences between observed records and truth.

---

## 23) Final Notes

This repository now contains a full Phase-1 baseline + Phase-2 simulation and ER emission framework with:

- contract-enforced outputs,
- source-backed parameter priors,
- deterministic scenario selection,
- event-driven truth simulation,
- dual-observed dataset emission with cardinality controls,
- comprehensive quality metrics,
- and regression tests.

For operational use, standardize on:

1. run-id naming convention,
2. scenario YAML review process,
3. validator pass as a release gate,
4. regression tests as a change gate.

