# Synthetic Occupancy Generator Population-Scaling Benchmark

Generated: 2026-09-07T04:26:02+00:00

## Experiment status

This report contains the valid measurements completed before the execution deadline. The benchmark runner calls the existing Phase-1 generator and canonical clean/reference Phase-2 scenario without changing generator or scenario semantics. Unmeasured cells are shown as `Not measured`; they are never estimated or copied from the paper.

## Executive summary

- Completed **15/18 primary runs** and **4/6 reproducibility reruns**.
- All completed primary outputs passed every output-contract check.
- Across the measured range (10,000 to 500,000 people), mean generation time rose from 36.59 to 4,413.64 seconds and throughput fell by 58.54%.
- The entity-level full-name collision percentage increased by 1.33 percentage points, from 58.02% to 59.36%.
- Same-seed checksums agreed at 10k, 50k, and 100k. They did not agree at 250k, so deterministic reproducibility is not established at that scale and should be investigated before claiming full cross-scale determinism.
- The 1m primary runs and the 500k/1m reproducibility reruns were stopped for the execution deadline and are reported as unmeasured, not failed.

## Design

- Population is `n_people`; Phase-1 base rows are `n_records = 1.4 * n_people`.
- Independent seeds: 20260829, 20260830, 20260831.
- Reproducibility seed: 20260829 (primary run plus one fresh rerun).
- Scenario: `clean_baseline_linkage`; experiment-local YAML copies change only seed and input paths.
- Generation time is Phase 1 plus Phase 2 excluding the separately timed output-contract validator.
- Peak RSS is the maximum summed resident memory of the isolated worker process tree.
- Final artifact size includes the Phase-1 input and complete Phase-2 run package before cleanup.
- Name counts and identical-full-name sharing are entity-level truth metrics, so repeated records do not inflate them.
- Records/person, duplicate rate, and source overlap are calculated over canonical observed records and `entity_record_map.csv`.

The clean scenario requests 92% overlap with 96%/97% source coverage. For people eligible at the first snapshot, those margins require roughly 93% overlap, and the existing emitter correctly applies that feasibility floor. People who become eligible only at the later snapshot can enter source B, so the final intersection/union rate is measured from artifacts rather than assumed to equal either target.

## Environment

- CPU: AMD64 Family 25 Model 80 Stepping 0, AuthenticAMD (8 physical / 16 logical cores)
- RAM: 15.35 GiB
- OS: Windows-10-10.0.26200-SP0
- Python: 3.10.5 at `C:\Python310\python.exe`
- Git commit: `caf629e5e0dfaffe386daf473ce9cda52b5bf76a`
- Git worktree dirty at capture: `True`
- Source-tree SHA-256 fallback: `ebd4f5e420f701cda847d737c8eebf54ffbe941b669e44bdb5d580a34de84efb`

## Scaling summary

| Requested people | Successful seeds | Observed records (mean) | Generation seconds (mean) | Records/s (mean) | Peak RSS MiB (max) | Artifact MiB (mean) | Contract |
|---:|---:|---:|---:|---:|---:|---:|:---:|
| 10,000 | 3/3 | 19,511 | 36.59 | 533.21 | 223.95 | 15.84 | True |
| 50,000 | 3/3 | 97,561 | 199.20 | 489.78 | 673.40 | 78.89 | True |
| 100,000 | 3/3 | 195,144 | 449.97 | 434.22 | 1,197.82 | 157.45 | True |
| 250,000 | 3/3 | 487,841 | 1,570.17 | 311.66 | 2,455.15 | 392.91 | True |
| 500,000 | 3/3 | 975,637 | 4,413.64 | 221.07 | 4,658.03 | 785.11 | True |
| 1,000,000 | 0/3 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |

## Name-collision table

Values are arithmetic means across the three independent primary seeds. A person is counted when their entity-level formal full name is shared by at least one other distinct person; repeated observed records do not create collisions.

| Population | Unique first names | Unique last names | Unique full names | People in shared full-name groups | Full-name collision % |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 1,403 | 2,292 | 5,027 | 5,802 | 58.02 |
| 50,000 | 2,908 | 5,311 | 25,078 | 29,102 | 58.20 |
| 100,000 | 3,875 | 6,691 | 50,029 | 58,398 | 58.40 |
| 250,000 | 5,546 | 8,566 | 124,356 | 147,041 | 58.82 |
| 500,000 | 7,129 | 9,906 | 246,731 | 296,785 | 59.36 |
| 1,000,000 | Not measured | Not measured | Not measured | Not measured | Not measured |

## Population and validation details

| Requested people | Generated people (mean) | Households (mean) | Addresses/units (mean) | Events (mean) | Validator seconds (mean) | Contract |
|---:|---:|---:|---:|---:|---:|:---:|
| 10,000 | 10,000 | 10,000 | 10,505 | 505 | 1.27 | True |
| 50,000 | 50,000 | 50,000 | 52,506 | 2,506 | 6.43 | True |
| 100,000 | 100,000 | 100,000 | 105,031 | 5,031 | 13.37 | True |
| 250,000 | 250,000 | 250,000 | 262,557 | 12,557 | 33.20 | True |
| 500,000 | 500,000 | 500,000 | 525,023 | 25,023 | 64.20 | True |
| 1,000,000 | Not measured | Not measured | Not measured | Not measured | Not measured | Not measured |

## Observed-record quality details

| Population | Records/person mean | Records/person median | Records/person SD | Duplicate-record % | Source-overlap % |
|---:|---:|---:|---:|---:|---:|
| 10,000 | 1.9511 | 2.0000 | 0.3206 | 1.4812 | 92.2682 |
| 50,000 | 1.9512 | 2.0000 | 0.3211 | 1.4821 | 92.2761 |
| 100,000 | 1.9514 | 2.0000 | 0.3206 | 1.4823 | 92.2965 |
| 250,000 | 1.9514 | 2.0000 | 0.3209 | 1.4822 | 92.2896 |
| 500,000 | 1.9513 | 2.0000 | 0.3211 | 1.4823 | 92.2812 |
| 1,000,000 | Not measured | Not measured | Not measured | Not measured | Not measured |

## Reproducibility

`Not run` means the repeat did not finish before the deadline; it does not mean that the checksum comparison failed.

| Requested people | Seed | Repeat status | Phase-1 | Truth | Output | Deterministic truth/output |
|---:|---:|:---:|:---:|:---:|:---:|:---:|
| 10,000 | 20260829 | success | True | True | True | True |
| 50,000 | 20260829 | success | True | True | True | True |
| 100,000 | 20260829 | success | True | True | True | True |
| 250,000 | 20260829 | success | False | False | False | False |
| 500,000 | 20260829 | not_run | Not run | Not run | Not run | Not run |
| 1,000,000 | 20260829 | not_run | Not run | Not run | Not run | Not run |

## Semantics-preserving scaling repair

The indexed snapshot lookup was accepted only after same-seed Phase-1, truth, and observed checksums matched the recorded unoptimized baseline: **True**.
Unoptimized 10,000-person mean generation time: **96.98 seconds**.

## Failures and limitations

- No recorded run failures.
- Primary measurements are incomplete for: 1,000,000.
- Reproducibility reruns were not completed for: 500,000, 1,000,000.

## Output files

- `scaling_results.csv`: one row per primary or reproducibility execution.
- `scaling_summary.csv`: population-level aggregates over the three independent primary seeds.
- `scaling_reproducibility.csv`: checksum comparisons for the fixed-seed reruns.
- `scaling_environment.json`: machine, dependency, configuration, Git/tree, and disk metadata.
