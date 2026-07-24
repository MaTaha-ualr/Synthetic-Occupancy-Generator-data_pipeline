# E1: Multi-seed replication results

Primary analysis: fixed threshold 0.65; 10 pre-registered seeds (20260720-20260729); sample SD (ddof=1).

| Condition | Records A | Records B | True links | Candidate pairs | Candidate recall | Precision | Recall | F1 | Runtime s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Clean | 9,630.2 ± 9.3 | 10,004.3 ± 12.8 | 9,516.4 ± 13.2 | 66,050.5 ± 925.1 | 1.0000 ± 0.0000 | 0.9987 ± 0.0004 | 0.9985 ± 0.0004 | 0.9986 ± 0.0003 | 11.1529 ± 0.4468 |
| High Noise | 9,630.2 ± 9.3 | 10,004.3 ± 12.8 | 9,516.4 ± 13.2 | 62,887.5 ± 995.4 | 0.9976 ± 0.0005 | 0.9989 ± 0.0004 | 0.9569 ± 0.0018 | 0.9774 ± 0.0010 | 11.2477 ± 0.3087 |
| Low Overlap | 5,818.8 ± 5.6 | 5,363.4 ± 6.8 | 2,250.5 ± 8.1 | 20,632.5 ± 300.8 | 1.0000 ± 0.0000 | 0.9982 ± 0.0006 | 0.9983 ± 0.0011 | 0.9982 ± 0.0006 | 6.3315 ± 0.1396 |
| One To Many | 9,630.2 ± 9.3 | 19,045.3 ± 14.8 | 18,660.8 ± 19.2 | 126,600.1 ± 1,829.0 | 1.0000 ± 0.0000 | 0.9988 ± 0.0004 | 0.9984 ± 0.0002 | 0.9986 ± 0.0002 | 16.6748 ± 0.6396 |

## Validation and interpretation

The mean clean-minus-high-noise F1 gap is 0.0212; pooled between-seed SD is 0.0007; ratio = 28.63. Therefore, the gap does exceed 3x pooled SD.
The noise effect points in the expected direction in all seeds. Low overlap reduces true-link prevalence in all seeds. One-to-many increases Dataset B records and candidate workload in all seeds.

The per-seed-threshold sensitivity results are preserved in `e1_summary.csv` and `e1_per_run.csv`. They should be described as a robustness analysis, not substituted post hoc for the pre-registered fixed-threshold primary result.

## Provenance

- Generator regime: calibrated age-specific rates with ACS-initialized baseline households.
- Independent truth populations: `10` (one independently seeded Phase 1 population per evaluation seed).
- Repository HEAD at evaluation: `51cdc2eb294c0545db7fe17ca8adf2be7f77db8e`
- Matcher blob hash: `8bf454fd67c6015243b0b331c054f13e350cd6f1`
- Matcher implementation blob hash: `d6c7ac47e1cc538dcb5c30464e80eddf4898c7e7`
- Python: `3.10.5`; pandas: `2.3.3`; NumPy: `2.2.6`
- Exact generated YAMLs, per-run metrics, thresholds, artifact hashes, logs, summary, and validation checks are in this directory.
