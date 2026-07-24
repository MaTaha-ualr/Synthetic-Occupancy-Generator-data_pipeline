# E2: Matcher suite across fourteen canonical scenarios

All values are mean ± sample SD across ten preregistered seeds.

| Scenario | Baseline F1 | Splink F1 | Learned F1 | Baseline B³F1 | Splink B³F1 | Learned B³F1 | Rank order |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| clean_baseline_linkage | 0.9992 ± 0.0002 | 0.9974 ± 0.0003 | 0.9998 ± 0.0002 | 0.9993 ± 0.0001 | 0.9984 ± 0.0001 | 0.9996 ± 0.0001 | learned > baseline > splink |
| single_movers | 0.9784 ± 0.0012 | 0.9773 ± 0.0012 | 0.9940 ± 0.0008 | 0.9902 ± 0.0006 | 0.9894 ± 0.0006 | 0.9972 ± 0.0004 | learned > baseline > splink |
| couple_merge | 0.8702 ± 0.0031 | 0.8591 ± 0.0032 | 0.8871 ± 0.0024 | 0.9184 ± 0.0020 | 0.9133 ± 0.0020 | 0.9303 ± 0.0015 | learned > baseline > splink |
| family_birth | 0.9323 ± 0.0027 | 0.8839 ± 0.0037 | 0.9377 ± 0.0028 | 0.9624 ± 0.0015 | 0.9349 ± 0.0021 | 0.9653 ± 0.0015 | learned > baseline > splink |
| divorce_custody | 0.9910 ± 0.0008 | 0.9875 ± 0.0007 | 0.9940 ± 0.0006 | 0.9943 ± 0.0005 | 0.9912 ± 0.0007 | 0.9961 ± 0.0004 | learned > baseline > splink |
| roommates_split | 0.9907 ± 0.0003 | 0.9889 ± 0.0006 | 0.9972 ± 0.0003 | 0.9904 ± 0.0004 | 0.9890 ± 0.0005 | 0.9950 ± 0.0003 | learned > baseline > splink |
| high_noise_identity_drift | 0.9807 ± 0.0011 | 0.9784 ± 0.0012 | 0.9931 ± 0.0007 | 0.9910 ± 0.0005 | 0.9897 ± 0.0006 | 0.9965 ± 0.0003 | learned > baseline > splink |
| low_overlap_sparse_coverage | 0.9971 ± 0.0008 | 0.9940 ± 0.0011 | 0.9988 ± 0.0004 | 0.9951 ± 0.0004 | 0.9943 ± 0.0005 | 0.9954 ± 0.0003 | learned > baseline > splink |
| asymmetric_source_coverage | 0.9970 ± 0.0008 | 0.9945 ± 0.0010 | 0.9990 ± 0.0004 | 0.9946 ± 0.0004 | 0.9937 ± 0.0005 | 0.9952 ± 0.0003 | learned > baseline > splink |
| high_duplication_dedup | 0.9960 ± 0.0005 | 0.9925 ± 0.0006 | 0.9983 ± 0.0005 | 0.9987 ± 0.0002 | 0.9971 ± 0.0003 | 0.9994 ± 0.0001 | learned > baseline > splink |
| three_source_partial_overlap | 0.9979 ± 0.0002 | 0.9953 ± 0.0005 | 0.9991 ± 0.0002 | 0.9983 ± 0.0002 | 0.9958 ± 0.0004 | 0.9988 ± 0.0001 | learned > baseline > splink |
| name_change_lifecycle | 0.9999 ± 0.0001 | 0.9984 ± 0.0003 | 0.9999 ± 0.0001 | 0.9999 ± 0.0001 | 0.9988 ± 0.0002 | 0.9999 ± 0.0001 | learned > baseline > splink |
| death_survivor_persistence | 0.9997 ± 0.0005 | 0.9983 ± 0.0009 | 0.9998 ± 0.0003 | 0.9998 ± 0.0002 | 0.9991 ± 0.0004 | 0.9999 ± 0.0001 | learned > baseline > splink |
| adoption_blended_family | 0.8460 ± 0.0005 | 0.7391 ± 0.0009 | 0.8547 ± 0.0007 | 0.9277 ± 0.0003 | 0.8469 ± 0.0005 | 0.9323 ± 0.0003 | learned > baseline > splink |

## Headline checks

- Material degradation-profile spreads (threshold 0.02 F1): `adoption_blended_family` (0.1133), `couple_merge` (0.0257), `family_birth` (0.0516).
- Hardest scenario by best-matcher F1: `adoption_blended_family` at 0.8547.
- Matcher ordering is stable (`learned > baseline > splink`); the positive result is profile separation, not rank reversal.
- Preregistered rank-or-profile criterion: **PASS**.

The acceptance implementation evaluates both preregistered alternatives; earlier code evaluated rank changes only. See `e2_validation.json`, `e2_per_run_metrics.csv`, and `scores/` for auditable details.
