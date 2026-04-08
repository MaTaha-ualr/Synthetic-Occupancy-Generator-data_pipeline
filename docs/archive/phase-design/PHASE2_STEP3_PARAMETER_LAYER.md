# Phase-2 Step-3 Parameter Data Layer (As of March 5, 2026)

## Goal
Move Phase-2 rates/probabilities out of code and into explicit parameter tables that are source-citable.

## Implemented Parameter Directory
- `Data/phase2_params/`

Files created:
- `mobility_overall_acs_2024.csv`
- `mobility_by_age_cohort_acs_2024.csv`
- `marriage_divorce_rates_cdc_2023.csv`
- `fertility_by_age_nchs_2024.csv`
- `household_type_shares_acs_2024.csv`
- `phase2_priors_snapshot.json`
- `sources.json`
- `manifest.json`

## Source Basis
### 3A) Mobility
- U.S. Census Bureau ACS 1-year table `B07001` (2024, U.S. total via Census API)
- Context reference: Census migration/geographic mobility guidance page

### 3B) Marriage + Divorce
- CDC/NCHS FastStats marriage/divorce rates and counts (2023 provisional)
- CDC/NCHS NVSS marriage/divorce page caveat: divorce rates are based on reporting areas (not full-state universal reporting each year)

### 3C) Fertility by Age
- CDC/NCHS Vital Statistics Rapid Release Report No. 38 (Births, provisional 2024), Table 1 age-specific birth rates

### 3D) Household Types
- U.S. Census Bureau ACS 1-year table `B11001` (2024, U.S. total via Census API)

## Code Integration
- Builder script:
  - `scripts/build_phase2_params.py`
- Loader module:
  - `src/sog_phase2/params.py`

Phase-2 code should call the loader and use these tables as priors instead of embedding numeric constants.

## Next Step
Step-4 event grammar is in:
- `docs/PHASE2_STEP4_EVENT_GRAMMAR.md`
