# Phase-2 Parameter Layer

This folder contains external-data parameter tables used by Phase-2.

Do not hard-code these rates in simulation code. Load these files instead.

## Tables
- `mobility_overall_acs_2024.csv`
- `mobility_by_age_cohort_acs_2024.csv`
- `marriage_divorce_rates_cdc_2023.csv`
- `fertility_by_age_nchs_2024.csv`
- `household_type_shares_acs_2024.csv`
- `phase2_priors_snapshot.json`
- `sources.json`
- `manifest.json`

## Rebuild
```bash
python scripts/build_phase2_params.py
```

## Citation Metadata
See `sources.json` for source IDs, URLs, data years, access timestamps, and caveats.
