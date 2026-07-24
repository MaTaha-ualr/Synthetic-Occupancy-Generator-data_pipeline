# E8: measurement rigor

## Hardware

| Item | Value |
| --- | --- |
| CPU | AMD Ryzen 7 5800H with Radeon Graphics |
| Cores | 8 physical / 16 logical |
| Installed RAM | 15.35 GiB |
| OS | Microsoft Windows 11 Home Single Language 10.0.26200 build 26200 |
| Python | 3.10.5 (CPython) |
| pandas / NumPy | 2.3.3 / 2.2.6 |

## Repeated matcher runtime

One unreported warm-up and five timed repetitions were run for each condition on seed 20260720. CSV loading and threshold calculation are outside the timed region.

| Condition | Repetitions | Runtime, s (mean ± SD) | Min | Max |
| --- | ---: | ---: | ---: | ---: |
| Clean | 5 | 11.0783 ± 0.0701 | 10.9856 | 11.1832 |
| High Noise | 5 | 11.1071 ± 0.1417 | 10.9234 | 11.3179 |
| Low Overlap | 5 | 6.2036 ± 0.1234 | 6.0779 | 6.3450 |
| One To Many | 5 | 16.3669 ± 0.2040 | 16.0208 | 16.5610 |

## Raw workload over ten seeds

| Condition | Candidate pairs | Cartesian space | Candidate recall | Reduction ratio |
| --- | ---: | ---: | ---: | ---: |
| Clean | 66,050.5 ± 925.1 | 96,343,399.2 ± 146,649.5 | 1.0000 ± 0.0000 | 0.9993 ± 0.0000 |
| High Noise | 62,887.5 ± 995.4 | 96,343,399.2 ± 146,649.5 | 0.9976 ± 0.0005 | 0.9993 ± 0.0000 |
| Low Overlap | 20,632.5 ± 300.8 | 31,208,548.7 ± 47,340.1 | 1.0000 ± 0.0000 | 0.9993 ± 0.0000 |
| One To Many | 126,600.1 ± 1,829.0 | 183,410,111.6 ± 278,931.4 | 1.0000 ± 0.0000 | 0.9993 ± 0.0000 |

The raw counts replace the previously rounded, visually identical reduction ratios. Exact per-seed counts and every repeated runtime are retained in CSV files.
