# E2-E6 paper experiment results

This directory is the single access point for the preregistered matcher-suite, noise-gradient, threshold, cluster, and household-pressure experiments.

## Outcomes

- **E2 PASS:** the corrected rank-or-profile implementation found material matcher-loss spreads in 3 scenarios. `adoption_blended_family` had the lowest best-matcher fixed F1 (0.8547), so the benchmark is not saturated.
- **E3 PASS:** all three matcher families degraded strictly monotonically. At k=4, F1 was 0.8903 baseline, 0.8730 Splink, and 0.9293 learned.
- **E4 RESOLVED:** the baseline high-noise oracle recovered 40.6% of the fixed-threshold drop; most baseline degradation is intrinsic.
- **E5 VALID NEGATIVE:** B³ is useful and correctly implemented, but it mostly softens pairwise degradation. The tiny one-to-many change is not substantive evidence of cluster pressure.
- **E6 PASS WITH METHOD DEPENDENCE:** under models calibrated once on clean SOG and then frozen, co-residence raises baseline and Splink false-match pressure in household scenarios; the learned model largely rejects it.

## Most important paper claims

1. Independent matcher families retain the clean rank order but have materially different loss profiles in household-formation scenarios.
2. `adoption_blended_family` is the hardest corrected scenario by best-matcher fixed F1 (0.8547).
3. `couple_merge` baseline co-resident non-matches score 0.6000 versus 0.3566 for random non-matches and account for 85.9% of its false positives.
4. Noise k=4 gives the largest useful separation and is the evidence-backed candidate for a harder canonical high-noise setting.
5. Oracle columns must accompany fixed results: baseline high-noise F1 moves from 0.9774 fixed to 0.9861 oracle.

## Files

- `E2/E2_REPORT.md`: fourteen-scenario matcher table.
- `E3/E3_REPORT.md` and `E3/e3_noise_gradient.png`: monotone noise dial.
- `E4/E4_REPORT.md` and `E4/baseline_pr_curves.png`: fixed-versus-oracle and PR analysis.
- `E5/E5_REPORT.md`: closure and B³ metrics.
- `E6/E6_REPORT.md`: co-resident score distributions and false positives.
- `VALIDATION.json`: acceptance and integrity checks.
- `ARTIFACT_MANIFEST.csv`: SHA-256 and size of every compact artifact.

The Git publication boundary retains exact generated YAMLs, compact seed-level CSVs, model metadata, reports, figures, validations, and hashes. Regenerable scored-pair Parquet files, duplicate per-run JSON, and generation logs are intentionally excluded.

All headline values above are derived from the checked-in compact artifacts by this finalizer.
