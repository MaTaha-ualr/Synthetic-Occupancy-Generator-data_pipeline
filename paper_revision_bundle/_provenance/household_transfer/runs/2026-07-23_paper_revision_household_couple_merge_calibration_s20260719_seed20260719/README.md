# Run: `2026-07-23_paper_revision_household_couple_merge_calibration_s20260719_seed20260719`

Auto-generated summary of this Phase-2 benchmark run. For the full column-by-column reference of every file below, see [`phase2/runs/README.md`](../README.md).

## At a glance

| Field | Value |
| --- | --- |
| Scenario | `paper_revision_household_couple_merge_calibration_s20260719` |
| Seed | `20260719` |
| Generated (UTC) | 2026-07-24T04:26:33Z |
| Cross-file match mode | `one_to_many` |
| Simulation window | 2026-01-01 → 2027-07-01 |
| Observed datasets | A, B |
| Quality status | **ok** |

## Truth layer (ground truth)

| Table | Rows |
| --- | --- |
| `truth_people` | 6050 |
| `truth_households` | 4896 |
| `truth_household_memberships` | 8772 |
| `truth_residence_history` | 14188 |
| `truth_events` | 4711 |

### Simulated events

`COHABIT`=1361, `MOVE`=3350

## Observed layer (what an ER system receives)

| Dataset | File |
| --- | --- |
| `A` | `DatasetA.csv` |
| `B` | `DatasetB.csv` |

Cross-file overlap: **5499** shared entities (90.9% of the union).

## Files in this folder

| File | What it is |
| --- | --- |
| `DatasetA.csv` | An observed (noisy) dataset emitted from a truth snapshot. |
| `DatasetB.csv` | An observed (noisy) dataset emitted from a truth snapshot. |
| `entity_record_map.csv` | CANONICAL answer key: maps every observed RecordKey (in any dataset) to its true PersonKey. |
| `manifest.json` | Machine-readable run manifest (inputs, outputs, simulation metadata). |
| `masterDataset.csv` | Deduplicated convenience stack of observed source rows, with PersonKey attached for deterministic entity-based ordering. |
| `quality_report.json` | Truth-consistency checks plus ER-benchmark metrics for this run. |
| `scenario.yaml` | The fully resolved scenario configuration this run was generated from. |
| `scenario_population.parquet` | The deterministically selected participants plus their latent propensity scores. |
| `scenario_selection_log.json` | Audit log of participant selection (filter counts, seed, PersonKey checksum). |
| `truth_crosswalk.csv` | Pairwise (A/B) answer key for two-dataset runs — a positional view; see README note on duplicates. |
| `truth_events.parquet` | The simulated life events (MOVE/COHABIT/BIRTH/DIVORCE/LEAVE_HOME/DEATH/NAME_CHANGE/ADOPTION) that drove every change. |
| `truth_household_memberships.parquet` | Who belonged to which household over which date interval, with role. |
| `truth_households.parquet` | Ground-truth households with their lifespan (start/end dates). |
| `truth_people.parquet` | Ground-truth person registry (one row per real person, including children born during the simulation). |
| `truth_residence_history.parquet` | Each person's address timeline (non-overlapping intervals). |

## How to use the answer keys

- Use **`entity_record_map.csv`** as the canonical ground truth: every record that shares a `PersonKey` is the same real person (transitively complete, across all datasets).
- **`masterDataset.csv`** is the convenience observed table: it stacks all observed datasets, adds `PersonKey`, collapses exact same-source duplicate payloads, and also adds `DatasetId=TIMELINE` rows from `truth_residence_history.parquet`. Filter `RecordType=source_snapshot` for only A/B source rows, or `RecordType=residence_timeline` to see every simulated residence interval.
- **`truth_crosswalk.csv`** is a backward-compatible *pairwise* view for two-dataset runs. It pairs records positionally, so when an entity has within-file duplicates some of its records appear with the opposite side left blank. For clustering/transitive scoring prefer `entity_record_map.csv`.

_Regenerate this file with_ `python phase2/scripts/write_run_readmes.py`.
