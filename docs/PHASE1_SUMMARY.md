# Phase-1 Delivery Summary (As of March 5, 2026)

## 1) Purpose of Phase-1
Phase-1 was designed to produce a reproducible synthetic people-and-address dataset from the SOG reference files, with:
- deterministic generation controls,
- configurable demographic distributions,
- configurable address and mailing behavior,
- built-in quality reporting and uniqueness checks.

## 2) What Was Implemented
The Phase-1 pipeline is complete and operational.

Implemented components:
- Raw data inputs:
  - `Addresses/States.csv`
  - `Addresses/Cities.csv`
  - `Addresses/StreetNames.csv`
  - `Names/FirstName_female.csv`
  - `Names/FirstName_male.csv`
  - `Names/FirstName_unisex.csv`
  - `Names/LastName.csv`
  - `Data/demographics_extracted/demographics_records_long.csv`
- Prepared cache builder:
  - Script: `scripts/build_prepared.py`
  - Core module: `src/sog_phase1/preprocess.py`
  - Output artifacts: parquet tables + parsed demographics JSON
- Dataset generator:
  - Script: `scripts/generate_phase1.py`
  - Core module: `src/sog_phase1/generator.py`
  - Produces one row per synthetic person with linked address fields
- Config loader and validation:
  - Module: `src/sog_phase1/config.py`
  - Validates core ranges, percentages, and structural requirements
- End-to-end smoke test:
  - `tests/test_phase1_pipeline.py`

## 3) Repository and Documentation Work Completed
Project documentation and onboarding were made beginner-ready:
- Main onboarding: `README.md`
- Full start-from-zero guide: `docs/BEGINNER_GUIDE.md`
- GitHub publishing guide: `docs/GITHUB_PUBLISH.md`
- Dependency files:
  - `requirements.txt`
  - `requirements-dev.txt`
- Ignore policy for generated artifacts:
  - `.gitignore`
- License:
  - MIT license added as `LICENSE`

## 4) Phase-1 Generation Design
High-level generation flow:
1. Build prepared cache from raw CSV inputs.
2. Read `configs/phase1.yaml`.
3. Validate config and normalize distributions where required.
4. Generate people attributes:
   - gender
   - ethnicity
   - first/last/middle names
   - suffix
   - DOB/Age/AgeBin
   - SSN and phone
5. Generate unique residence addresses with house/apartment mix.
6. Generate mailing behavior (blank or OHC-style PO BOX).
7. Write output in chunks (CSV or parquet parts).
8. Write run manifest and quality report.

## 5) Full-Name Duplication Logic Enhancement
The name-collision behavior was upgraded beyond pair-only duplication.

Previously:
- Target percentage was applied mostly as 2-person pairs.

Now:
- You can control duplicate group size range directly in config:
  - `name_duplication.collision_group_min_size`
  - `name_duplication.collision_group_max_size`
- Existing control remains:
  - `name_duplication.exact_full_name_people_pct`
- Collision groups are intentionally denser (multi-person groups), then validated in report outputs.

Current default config:
- `exact_full_name_people_pct: 58.0`
- `collision_group_min_size: 3`
- `collision_group_max_size: 7`

Important note:
- Min/max bounds apply to forced collision groups.
- Natural random collisions can still create a small number of groups outside requested bounds.

## 6) Output Artifacts Produced by Phase-1
Primary output:
- `outputs/Phase1_people_addresses.csv`

Run metadata:
- `outputs/Phase1_people_addresses.manifest.json`

Quality report:
- `outputs/Phase1_people_addresses.quality_report.json`

Prepared cache:
- `prepared/first_names.parquet`
- `prepared/last_names.parquet`
- `prepared/streets.parquet`
- `prepared/cities.parquet`
- `prepared/states.parquet`
- `prepared/demographics.json`
- `prepared/prepared_manifest.json`

## 7) Validation and Test Status
Automated test status:
- `python -m pytest -q tests/test_phase1_pipeline.py`
- Result: pass

Test coverage confirms:
- `PersonKey` uniqueness
- `AddressKey` uniqueness
- full residence address uniqueness
- distribution adherence within tolerance
- OHC-style apartment mailing consistency
- zip code format checks
- generated manifest and quality report existence

## 8) Latest Run Snapshot
From the latest generated quality report:
- Row count: `10,000`
- Seed: `20260303`
- Housing split: `7,000 houses / 3,000 apartments`
- Target duplicate-name people: `58.0%`
- Actual duplicate-name people: `58.06%`
- Forced duplicate groups: `829`
- Actual duplicate groups observed: `831`
- Uniqueness checks:
  - `PersonKey`: unique
  - `AddressKey`: unique
  - full address: unique

## 9) Git Milestones Completed
Recent commits on `main`:
- `68398c1` (2026-03-05): Initial commit with Phase-1 pipeline and docs
- `aafb752` (2026-03-05): MIT license + license doc updates
- `8c61fc1` (2026-03-05): Configurable full-name collision group sizing

## 10) Current Phase Boundary
Phase-1 is complete, documented, tested, and published.

Phase-2 can now start from this stable baseline:
- use current config/model patterns in `src/sog_phase1/` as template,
- keep quality-report-driven validation approach,
- extend in new Phase-2 modules/config paths without breaking Phase-1 outputs.

## 11) Phase-2 Extension (Local Implementation Status)
Phase-2 extension has now been implemented locally (not pushed), adding:
- entity/record split controls (`n_entities`, `n_records`),
- redundancy controls with bounded multiplicity (`min_records_per_entity`, `max_records_per_entity`, shape),
- row-level unique `RecordKey`,
- nickname-enabled display naming with formal-name anchors (`FormalFirstName`, `FormalFullName`),
- prepared nickname artifact (`prepared/nicknames.json`),
- expanded manifest/quality metrics for redundancy and nickname behavior.
