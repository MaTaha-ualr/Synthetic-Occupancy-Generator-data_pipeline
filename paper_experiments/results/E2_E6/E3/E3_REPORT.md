# E3: Noise-gradient difficulty dial

All values are mean ± sample SD across ten seeds; matcher thresholds are frozen from the clean calibration seed.

| k | Matcher | Candidate recall | Scoring recall | Total recall | F1 |
| ---: | --- | ---: | ---: | ---: | ---: |
| 0 | baseline | 1.0000 ± 0.0000 | 0.9983 ± 0.0004 | 0.9983 ± 0.0004 | 0.9986 ± 0.0004 |
| 0 | learned | 1.0000 ± 0.0000 | 0.9997 ± 0.0002 | 0.9997 ± 0.0002 | 0.9997 ± 0.0001 |
| 0 | splink | 1.0000 ± 0.0000 | 0.9982 ± 0.0004 | 0.9982 ± 0.0004 | 0.9967 ± 0.0003 |
| 0.5 | baseline | 0.9992 ± 0.0003 | 0.9829 ± 0.0019 | 0.9821 ± 0.0020 | 0.9904 ± 0.0011 |
| 0.5 | learned | 0.9992 ± 0.0003 | 0.9956 ± 0.0007 | 0.9948 ± 0.0009 | 0.9973 ± 0.0004 |
| 0.5 | splink | 0.9992 ± 0.0003 | 0.9827 ± 0.0019 | 0.9819 ± 0.0020 | 0.9885 ± 0.0011 |
| 1 | baseline | 0.9973 ± 0.0005 | 0.9658 ± 0.0020 | 0.9631 ± 0.0021 | 0.9807 ± 0.0011 |
| 1 | learned | 0.9973 ± 0.0005 | 0.9894 ± 0.0012 | 0.9867 ± 0.0013 | 0.9931 ± 0.0007 |
| 1 | splink | 0.9973 ± 0.0005 | 0.9647 ± 0.0021 | 0.9621 ± 0.0022 | 0.9784 ± 0.0012 |
| 2 | baseline | 0.9902 ± 0.0011 | 0.9249 ± 0.0031 | 0.9158 ± 0.0031 | 0.9557 ± 0.0017 |
| 2 | learned | 0.9902 ± 0.0011 | 0.9694 ± 0.0025 | 0.9598 ± 0.0026 | 0.9794 ± 0.0013 |
| 2 | splink | 0.9902 ± 0.0011 | 0.9201 ± 0.0030 | 0.9111 ± 0.0030 | 0.9513 ± 0.0017 |
| 4 | baseline | 0.9632 ± 0.0020 | 0.8334 ± 0.0044 | 0.8027 ± 0.0039 | 0.8903 ± 0.0025 |
| 4 | learned | 0.9632 ± 0.0020 | 0.9013 ± 0.0036 | 0.8681 ± 0.0041 | 0.9293 ± 0.0023 |
| 4 | splink | 0.9632 ± 0.0020 | 0.8072 ± 0.0055 | 0.7775 ± 0.0048 | 0.8730 ± 0.0030 |

## Acceptance checks

- baseline: strict monotone F1 = **True**; first k below 0.95 = `4.0`; below 0.90 = `4.0`.
- splink: strict monotone F1 = **True**; first k below 0.95 = `4.0`; below 0.90 = `4.0`.
- learned: strict monotone F1 = **True**; first k below 0.95 = `4.0`; below 0.90 = `None`.
- All three strictly monotone: **True**.

Scoring recall degrades before blocking recall: already at k=0.5 candidate recall remains 0.9992, while conditional scoring recall falls to 0.9872 baseline, 0.9826 Splink, and 0.9938 learned. The largest matcher separation occurs at k=4, the evidence-backed candidate for a more discriminating canonical high-noise setting.
