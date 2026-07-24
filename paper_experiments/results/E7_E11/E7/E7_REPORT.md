# E7: achieved versus target distributions

This analysis tests fidelity to the checked-in public parameter tables. It does not test equivalence to confidential operational administrative data.

| Dimension | Target (source) | Achieved (mean ± SD) | Relative deviation | Verdict |
| --- | ---: | ---: | ---: | --- |
| Overall annual move rate | 11.763 (ACS 2024 B07001) | 11.370 ± 0.392 | -3.3% | close |
| Move rate, ages 18-34 | 22.583 (ACS 2024 B07001) | 21.679 ± 0.908 | -4.0% | close |
| Move rate, ages 35-64 | 8.892 (ACS 2024 B07001) | 8.483 ± 0.430 | -4.6% | close |
| Move rate, ages 65+ | 5.518 (ACS 2024 B07001) | 4.819 ± 0.740 | -12.7% | moderate |
| Household-type JS distance | 0.000 (ACS 2024 B11001) | 0.000 ± 0.000 | N/A | close |
| Household-size JS distance | N/A (requested ACS B11001 file) | N/A | N/A | not estimable |
| Cohabitation rate | 6.100 (CDC 2023 marriage rate (incompatible construct)) | 0.866 ± 0.326 | -85.8% | not comparable |
| Divorce rate | 2.400 (CDC 2023) | 2.305 ± 0.385 | -3.9% | close |
| Surname top-100 share | 27.301 (Phase 1 conditional Census surname pool) | 30.238 ± 0.718 | +10.8% | moderate |
| Surname rank-frequency slope | -0.782 (Phase 1 conditional Census surname pool) | -0.773 ± 0.018 | -1.2% | descriptive |
| Surname JS distance | 0.000 (Phase 1 conditional Census surname pool) | 0.090 ± 0.009 | N/A | moderate |
| Fertility 10-14 | 0.200 (NCHS 2024 provisional) | 0.000 ± 0.000 | -100.0% | not estimable |
| Fertility 15-19 | 12.700 (NCHS 2024 provisional) | 12.067 ± 5.115 | -5.0% | close |
| Fertility 20-24 | 56.700 (NCHS 2024 provisional) | 53.879 ± 16.248 | -5.0% | close |
| Fertility 25-29 | 91.400 (NCHS 2024 provisional) | 92.770 ± 21.482 | +1.5% | close |
| Fertility 30-34 | 95.400 (NCHS 2024 provisional) | 104.439 ± 10.607 | +9.5% | moderate |
| Fertility 35-39 | 55.000 (NCHS 2024 provisional) | 65.380 ± 13.032 | +18.9% | moderate |
| Fertility 40-44 | 12.800 (NCHS 2024 provisional) | 9.663 ± 3.483 | -24.5% | consistent within seed variability |
| Fertility 45-54 | 1.100 (NCHS 2024 provisional) | 1.630 ± 1.723 | +48.2% | not estimable |

## Paper-ready result

Across ten Phase 2 seeds, achieved annual mobility was 11.370% ± 0.392% against an ACS target of 11.763% (deviation -3.3%), and household-type Jensen-Shannon distance was 0.000. The requested household-size divergence is unavailable because the specified B11001 file contains household types, not size bins.

## Interpretation

- Mobility now uses calibrated age-specific household hazards. Overall and working-age achieved rates are close to ACS; the 65+ result remains moderate and is reported rather than hidden.
- The baseline family graph is initialized from the mutually exclusive ACS household-type shares, removing the former all-singleton population artifact.
- Divorce and age-specific fertility are enabled. Very sparse fertility strata are marked not estimable when fewer than five events are expected across all ten seeds. SOG `COHABIT` remains semantically distinct from CDC marriage and is not compared.
- Surname JS pools names outside the source top 100 into an OTHER category, preventing finite-sample tail truncation from dominating the distance while retaining the named high-frequency distribution.

## Dependence and provenance

The ten E1 truth seeds use 10 independently seeded Phase 1 population files.
Exact per-seed values, target hashes, truth-artifact hashes, component household shares, validation metadata, and the figure are preserved beside this report.
