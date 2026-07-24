# E5: Cluster-level evaluation

Truth clusters are `PersonKey` groups over all emitted records. Predicted clusters are connected components of frozen-threshold eligible edges; B³ includes singleton nodes.

| Condition | Pairwise F1 | Closure F1 | B³ precision | B³ recall | B³ F1 |
| --- | ---: | ---: | ---: | ---: | ---: |
| clean | 0.9987 | 0.9986 | 0.9994 | 0.9986 | 0.9990 |
| high_noise | 0.9774 | 0.9774 | 0.9994 | 0.9782 | 0.9887 |
| low_overlap | 0.9983 | 0.9982 | 0.9996 | 0.9909 | 0.9952 |
| one_to_many | 0.9986 | 0.9986 | 0.9994 | 0.9988 | 0.9991 |

## Canonical cluster-pressure scenarios

| Scenario | Pairwise F1 | Closure F1 | B³ F1 |
| --- | ---: | ---: | ---: |
| couple_merge | 0.8702 | 0.8701 | 0.9184 |
| divorce_custody | 0.9910 | 0.9914 | 0.9943 |
| family_birth | 0.9323 | 0.9322 | 0.9624 |
| high_duplication_dedup | 0.9960 | 0.9968 | 0.9987 |

## Acceptance interpretation

The one-to-many absolute B³ change (0.000080) is larger than its pairwise change (0.000016), and B³ moves upward. The metric implementation is valid, but the preregistered substantive cluster-pressure claim is **not supported** unless the regenerated effect is both directionally adverse and practically meaningful.
