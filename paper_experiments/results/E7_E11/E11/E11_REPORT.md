# E11: head-to-head comparison with upstream GeCo

This experiment used the checksum-pinned ANU GeCo archive itself, not a GeCo-like reimplementation. Its Python 2 source was mechanically converted with `lib2to3`; the exact patch and compiler log are retained in `upstream/`.

**Calibration disclosure.** The learned and Splink matchers were calibrated once on the independent SOG clean calibration seed and then frozen. No GeCo-specific or `couple_merge` retuning was performed. Consequently, the co-address collapse is evidence about fixed-model transfer behavior, not a claim that the arms received equally optimized matchers.

Calibration seed `20260719` selected **5 modifications per duplicate record** from `1..5` by seven-field true-pair similarity RMSE (0.279232). Evaluation used the disjoint seeds `20260720-20260729` and the frozen E2 thresholds.

Both arms contain 5,499 A records, 11,011 B records, 10,460 true links, and 60,549,489 Cartesian pairs per seed.

## Calibration adequacy

The selected setting is the upper boundary of the preregistered grid and its RMSE is large on a 0-1 similarity scale. GeCo's generic record corruption cannot reproduce the highly heterogeneous `couple_merge` signature: SOG mostly preserves first name and DOB while changing household/address and often surname attributes. The comparison is therefore **size matched but only poorly noise matched**.

| Field | SOG mean true-pair similarity | Selected GeCo | Difference |
| --- | ---: | ---: | ---: |
| first | 0.9819 | 0.6151 | -0.3668 |
| last | 0.7858 | 0.6134 | -0.1724 |
| dob | 0.9971 | 0.7691 | -0.2279 |
| street | 0.2628 | 0.7471 | +0.4843 |
| city | 0.2328 | 0.2644 | +0.0316 |
| state | 0.2463 | 0.5400 | +0.2937 |
| postal | 0.2285 | 0.1404 | -0.0880 |

## Matcher results

| Arm | Matcher | Candidate recall | F1 | B³ F1 | False positives | Co-address FP share |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| GeCo analog | baseline | 0.8665 ± 0.0025 | 0.5533 ± 0.0055 | 0.7168 ± 0.0024 | 0.1 ± 0.3 | 0.0% ± 0.0% |
| GeCo analog | learned | 0.8665 ± 0.0025 | 0.6841 ± 0.0048 | 0.7822 ± 0.0026 | 0.5 ± 0.7 | 0.0% ± 0.0% |
| GeCo analog | splink | 0.8665 ± 0.0025 | 0.7795 ± 0.0037 | 0.8382 ± 0.0022 | 4.7 ± 1.6 | 0.0% ± 0.0% |
| SOG couple_merge | baseline | 0.9981 ± 0.0004 | 0.8702 ± 0.0031 | 0.9184 ± 0.0020 | 20.3 ± 6.3 | 85.9% ± 13.7% |
| SOG couple_merge | learned | 0.9981 ± 0.0004 | 0.8871 ± 0.0024 | 0.9303 ± 0.0015 | 2.7 ± 3.1 | 20.0% ± 42.2% |
| SOG couple_merge | splink | 0.9981 ± 0.0004 | 0.8591 ± 0.0032 | 0.9133 ± 0.0020 | 260.6 ± 19.6 | 98.6% ± 1.2% |

## Preregistered novelty check

A matcher supports the E11 claim only when SOG has (1) an F1 or B³ F1 deficit greater than three pooled between-seed SDs and (2) a co-address false-positive share at least 20 percentage points above GeCo.

| Matcher | SOG F1 deficit | F1 / pooled SD | SOG B³ deficit | B³ / pooled SD | Co-address gap | Verdict |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| baseline | -0.3169 | -71.15× | -0.2015 | -90.26× | +85.9 pp | does not support |
| splink | -0.0796 | -23.22× | -0.0750 | -35.46× | +98.6 pp | does not support |
| learned | -0.2030 | -54.01× | -0.1481 | -69.73× | +20.0 pp | does not support |

**Overall preregistered verdict: MIXED.** No matcher meets both parts of the joint criterion. The result must not be presented as a successful head-to-head novelty demonstration.

The performance direction is opposite to the proposed novelty argument: every frozen matcher scores higher on SOG than on GeCo, while baseline and Splink still show a much larger co-address false-positive share on SOG. Because candidate recall is also materially lower for GeCo and the noise calibration is poor, this is not a defensible overall-difficulty ranking. It is a useful negative result showing a distinctive, matcher-dependent household error mode—not proof that SOG is globally harder.

## Scope and limitations

- The comparison exactly matches record counts, true-link counts, and Cartesian space. The attempted true-pair field-similarity calibration remains poor and is disclosed above; demographic and marginal field distributions are also unmatched.
- GeCo models independent originals and corrupted descendants; it has no household-membership truth. Its co-address value is therefore the same observed-address proxy used by E6, not a household error rate.
- This experiment tests one SOG scenario (`couple_merge`) against one explicit GeCo analog. It is evidence about that contrast, not a universal ranking of generators.

Exact per-seed metrics, candidate scores, input hashes, calibration vectors, source provenance, and validation checks are stored beside this report.
