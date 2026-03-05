# Phase-2 Step-6 Scenario Selection Engine (As of March 5, 2026)

## Goal
Select scenario participants deterministically from the Phase-1 baseline using explicit, auditable primitives.

## Selection Primitives
Supported filters:
- `AgeBin`
- `Gender`
- `Ethnicity`
- `ResidenceType` (house/apartment)
- `RedundancyProfile` (`single_record` vs `multi_record`)
- `MobilityPropensityBucket` (`low`, `medium`, `high`)

Sampling modes:
- `all`
- `count`
- `pct`

## Determinism + Auditability
Selection is:
- seed-based (`scenario.seed`)
- deterministic (stable hashing + seeded sampling)
- logged to `scenario_selection_log.json`
- reproducible from Phase-1 CSV + scenario YAML + seed

## Output Artifact
Generated per run:
- `phase2/runs/<run_id>/scenario_population.parquet`

This includes:
- selected `PersonKey`
- demographic selectors (`AgeBin`, `Gender`, `Ethnicity`, `ResidenceType`)
- redundancy selectors (`RecordsPerEntity`, `RedundancyProfile`)
- latent traits:
  - `MobilityPropensityScore` / `MobilityPropensityBucket`
  - `PartnershipPropensityScore` / `PartnershipPropensityBucket`
  - `FertilityPropensityScore` / `FertilityPropensityBucket`

Selection log:
- `phase2/runs/<run_id>/scenario_selection_log.json`

## Enforced In Code
- Engine:
  - `src/sog_phase2/selection.py`
- Builder CLI:
  - `scripts/build_phase2_scenario_population.py`
- Run-contract validation:
  - `src/sog_phase2/output_contract.py`

Build command:
```bash
python scripts/build_phase2_scenario_population.py --run-id <run_id> --overwrite
```
