# E7-E11 results index

This is the single entry point for the second-round paper experiments. Negative and inconclusive findings are retained alongside successful measurements.

| Experiment | Outcome | Paper value | Primary report |
| --- | --- | --- | --- |
| E7: external validity | Calibrated fidelity achieved | Overall and working-age mobility, household type, and divorce are close; scoped limitations remain | [E7 report](E7/E7_REPORT.md) |
| E8: measurement rigor | Complete | Replaces single runtimes and rounded reduction ratios with hardware, repetitions, and raw workload | [E8 report](E8/E8_REPORT.md) |
| E9: artifact archive | Locally archive-ready; DOI pending | Exact dates, seeds, commands, version, citation template, and deposition metadata | [E9 record](E9/REPRODUCIBILITY.md) |
| E10: capability comparison | Supports composition-level novelty | Useful related-work table if phrased as an evidence-scoped combination claim | [E10 report](E10/E10_REPORT.md) |
| E11: GeCo head-to-head | Mixed; joint novelty criterion not met | Supplemental negative result: distinctive co-address pressure, but no global-difficulty advantage and poor noise match | [E11 report](E11/E11_REPORT.md) |

## Recommended paper use

- Use E7 as evidence of **fidelity to declared public targets**, not equivalence to confidential administrative data. Retain the moderate, sparse-stratum, and semantic-comparability qualifications.
- Use E8 directly in the experimental setup and replace all rounded reduction-ratio claims with raw candidate counts and Cartesian spaces.
- Use E10 for the novelty section, explicitly claiming a distinctive **combination** of capabilities rather than invention of every component.
- E11 uses learned and Splink models calibrated once on clean SOG and frozen without GeCo-specific retuning. Under that transfer setting it preserves a real household/address false-positive signal, but candidate recall and residual noise mismatch preclude a global difficulty comparison.
- Do not cite a Zenodo DOI until E9's authenticated deposition is complete.

## Package integrity

`ARTIFACT_MANIFEST.csv` contains 234 SHA-256 records spanning E7-E11 outputs, evaluation source, pinned GeCo source, and all exact E1/E2 scenario configurations. `PACKAGE_VALIDATION.json` records the cross-experiment acceptance checks.
