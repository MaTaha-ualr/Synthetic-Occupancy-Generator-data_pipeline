# Branch Guide

## Quick Branch Selector
- Need old stable behavior: use `phase1-legacy`.
- Need latest features: use `main`.

## Branch Comparison
| Branch/Tag | Purpose | Key Schema/Features | Intended Audience |
|---|---|---|---|
| `main` | Current development and latest published pipeline behavior | Entity/record split (`n_entities`, `n_records`), redundancy controls, nickname variants, expanded schema and quality metrics | Users who want the newest workflow and outputs |
| `phase1-legacy` | Stable pre-Phase-2 baseline from commit `8c61fc1` | Legacy Phase-1 behavior with one-person-per-row assumptions | Users who need the original pipeline behavior |
| `v1-phase1-baseline` (tag) | Immutable snapshot of the legacy baseline | Points to commit `8c61fc1` | Long-term reproducibility and citation |

## Checkout Commands
```bash
# Latest features
git checkout main

# Legacy baseline branch
git checkout phase1-legacy

# Immutable baseline tag (detached HEAD)
git checkout v1-phase1-baseline
```

## Baseline Tag
- `v1-phase1-baseline` is an annotated tag fixed to commit `8c61fc1`.
- Use this tag when you need an exact historical baseline that cannot drift.

## phase1-legacy Includes
- Phase-1 generator with unique `PersonKey` and unique `AddressKey` per row.
- No entity/record split (`n_entities` vs `n_records`).
- No nickname-driven first-name variation.

## main Adds
- `n_entities` + `n_records` model.
- Redundancy controls (`enabled`, `min_records_per_entity`, `max_records_per_entity`, `shape`).
- New row/entity identity columns: `RecordKey`, `EntityRecordIndex`, `FormalFirstName`, `FormalFullName`, `FirstNameType`.
- Nickname-aware first-name rendering from `Names/nick names/`.
- Expanded manifest/quality metrics and updated tests/docs.