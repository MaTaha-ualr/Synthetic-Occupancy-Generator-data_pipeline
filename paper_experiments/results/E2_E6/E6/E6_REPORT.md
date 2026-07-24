# E6: Household false-match pressure

**Calibration disclosure.** Splink and the learned matcher were calibrated once on the independent SOG clean calibration seed; their models and thresholds were then frozen for every scenario. No household-scenario retuning was performed. Splink's co-residence failure mode is interpreted under that fixed deployment choice, not as a universally calibrated comparison.

Co-resident non-matches are distinct truth entities with the same nonempty normalized observed street/city/state/postal tuple.

| Scenario | Matcher | Co-resident pairs | Median co-resident score | Median random score | Median true score | False positives | Co-resident FP share |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| clean_baseline_linkage | baseline | 40.1 | 0.571962 | 0.431179 | 1 | 10.5 | 70.9% |
| clean_baseline_linkage | learned | 40.1 | 0.000800624 | 7.25274e-05 | 0.999992 | 3.6 | 24.2% |
| clean_baseline_linkage | splink | 40.1 | 1 | 0.107799 | 1 | 43.1 | 93.2% |
| couple_merge | baseline | 257.2 | 0.6 | 0.356592 | 0.65 | 20.3 | 85.9% |
| couple_merge | learned | 257.2 | 2.44043e-05 | 4.50303e-05 | 0.994585 | 2.7 | 20.0% |
| couple_merge | splink | 257.2 | 1 | 0.00112249 | 0.998601 | 260.6 | 98.6% |
| family_birth | baseline | 915.0 | 0.6 | 0.358219 | 1 | 41.8 | 97.0% |
| family_birth | learned | 915.0 | 2.44043e-05 | 4.96242e-05 | 0.999992 | 3.2 | 44.4% |
| family_birth | splink | 915.0 | 1 | 0.00115814 | 1 | 920.8 | 99.4% |
| roommates_split | baseline | 47.8 | 0.573019 | 0.422738 | 1 | 13.8 | 61.6% |
| roommates_split | learned | 47.8 | 0.000633894 | 6.68457e-05 | 0.999992 | 5.8 | 20.8% |
| roommates_split | splink | 47.8 | 0.999975 | 0.0889729 | 1 | 67.4 | 70.1% |

## Acceptance interpretation

The score-distribution and false-positive shift is clear for the deterministic baseline and present for Splink, especially in `couple_merge` and `family_birth`; the learned model largely rejects co-residence-only evidence. E6 therefore supports household false-match pressure as a **matcher-dependent** effect, not a universal error guarantee.
