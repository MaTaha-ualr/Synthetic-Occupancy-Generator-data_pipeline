# E4: Threshold and oracle analysis

Oracle values are in-sample descriptive upper bounds, not deployable performance estimates.

| Condition | Matcher | Fixed F1 | Oracle F1 | Oracle threshold | Fixed - oracle |
| --- | --- | ---: | ---: | ---: | ---: |
| clean | baseline | 0.9987 ± 0.0003 | 0.9987 ± 0.0003 | 0.6500 | 0.0000 |
| clean | learned | 0.9994 ± 0.0002 | 0.9995 ± 0.0002 | 0.7794 | -0.0001 |
| clean | splink | 0.9924 ± 0.0008 | 0.9924 ± 0.0008 | 0.8842 | -0.0000 |
| high_noise | baseline | 0.9774 ± 0.0010 | 0.9861 ± 0.0005 | 0.5500 | -0.0086 |
| high_noise | learned | 0.9929 ± 0.0005 | 0.9952 ± 0.0004 | 0.0693 | -0.0023 |
| high_noise | splink | 0.9715 ± 0.0010 | 0.9874 ± 0.0007 | 0.5663 | -0.0159 |
| low_overlap | baseline | 0.9983 ± 0.0006 | 0.9983 ± 0.0006 | 0.6500 | 0.0000 |
| low_overlap | learned | 0.9994 ± 0.0003 | 0.9996 ± 0.0003 | 0.7952 | -0.0001 |
| low_overlap | splink | 0.9895 ± 0.0015 | 0.9895 ± 0.0014 | 0.8952 | -0.0000 |
| one_to_many | baseline | 0.9986 ± 0.0002 | 0.9987 ± 0.0002 | 0.6477 | -0.0000 |
| one_to_many | learned | 0.9994 ± 0.0002 | 0.9995 ± 0.0001 | 0.7054 | -0.0001 |
| one_to_many | splink | 0.9946 ± 0.0004 | 0.9947 ± 0.0003 | 0.9154 | -0.0000 |

For the baseline matcher, clean fixed F1 was 0.9987; high-noise fixed F1 was 0.9774; and high-noise oracle F1 was 0.9861. Oracle calibration recovered 0.0086, or 40.6% of the 0.0212 fixed-threshold drop.

Full fourteen-scenario threshold results are in `e4_e2_threshold_summary.csv`.
