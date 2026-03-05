# Beginner Guide: Run SOG From Scratch

This guide assumes no prior knowledge of this project.

## 1) What This Project Does
SOG Phase-1 generates synthetic person and address records using reference CSVs in:
- `Addresses/`
- `Names/`
- `Data/demographics_extracted/`

You control generation behavior from one file:
- `configs/phase1.yaml`

## 2) Install Prerequisites
Install:
- Python 3.10 or newer
- Git

Check versions:
```bash
python --version
git --version
```

## 3) Download the Repository
If you have the GitHub URL:
```bash
git clone https://github.com/MaTaha-ualr/Synthetic-Occupancy-Generator-data_pipeline.git
cd Synthetic-Occupancy-Generator-data_pipeline
```

If you already downloaded a ZIP, open a terminal in the extracted `SOG` folder.

## 4) Create a Virtual Environment
Windows PowerShell:
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

macOS/Linux:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 5) Install Dependencies
Runtime dependencies:
```bash
pip install -r requirements.txt
```

Optional (tests):
```bash
pip install -r requirements-dev.txt
```

## 6) Build the Prepared Cache (Required)
Run once after fresh clone (or after changing raw CSVs):
```bash
python scripts/build_prepared.py --raw-root . --prepared-dir prepared
```

Expected outputs in `prepared/`:
- `first_names.parquet`
- `last_names.parquet`
- `streets.parquet`
- `cities.parquet`
- `states.parquet`
- `demographics.json`
- `prepared_manifest.json`

## 7) Generate Synthetic Data
Run:
```bash
python scripts/generate_phase1.py --config configs/phase1.yaml --prepared-dir prepared --overwrite
```

Default output files in `outputs/`:
- `Phase1_people_addresses.csv`
- `Phase1_people_addresses.manifest.json`
- `Phase1_people_addresses.quality_report.json`

## 8) Understand The Main Output
`Phase1_people_addresses.csv` contains one row per person.

Key column groups:
- Identity: `PersonKey`, `FirstName`, `MiddleName`, `LastName`, `Suffix`, `FullName`, `Gender`, `Ethnicity`
- Demographics: `DOB`, `Age`, `AgeBin`
- Contact: `SSN`, `Phone`
- Residence: `Residence*` columns
- Mailing: `Mailing*` columns

Notes:
- `ResidencePostalCode` and `MailingPostalCode` are 5-digit strings.
- `MailingAddressMode` is usually blank or `PO BOX`.
- `AddressKey` is 1:1 with `PersonKey` in this phase.

## 9) Customize Behavior In `configs/phase1.yaml`

### Most important settings
- `phase1.n_people`: row count
- `phase1.seed`: random seed for reproducibility
- `phase1.output`: output format/path/chunk size
- `phase1.name_duplication.exact_full_name_people_pct`: target percent of rows that should have duplicated exact full name
- `phase1.name_duplication.collision_group_min_size`: minimum people in one duplicate-name group
- `phase1.name_duplication.collision_group_max_size`: maximum people in one duplicate-name group
- `phase1.distributions.gender`: gender target percentages
- `phase1.distributions.ethnicity`: optional override (if `null`, uses prepared demographics)
- `phase1.age_bins`: active age ranges and percentages
- `phase1.address`: house/apartment mix and mailing behavior
- `phase1.fill_rates`: optional field fill rates (`0` to `1`)

Note: min/max collision size settings apply to forced duplicate groups. A few natural collisions can still occur outside the requested range.

### Safe first edits
1. Change `n_people` (example: `10000` -> `50000`)
2. Keep same `seed` if you need reproducible reruns
3. Keep `age_bins.auto_normalize: true` unless you want strict sum checks
4. For mostly pair duplicates, set `collision_group_min_size: 2` and `collision_group_max_size: 2`

## 10) Validate Output Quality
Open `outputs/Phase1_people_addresses.quality_report.json` and inspect:
- `distribution_checks`: expected vs achieved distributions
- `uniqueness_checks`: unique keys and full addresses
- `missingness_pct`: blank rates by field

Run automated test:
```bash
python -m pytest -q tests/test_phase1_pipeline.py
```

## 11) Typical Daily Workflow
```bash
# 1) Activate environment
# 2) Install dependencies (only when needed)
# 3) Edit configs/phase1.yaml
# 4) Generate
python scripts/generate_phase1.py --config configs/phase1.yaml --prepared-dir prepared --overwrite

# 5) Review quality report
```

Rebuild prepared cache only when raw source CSVs changed.

## 12) Troubleshooting
- Error: `Prepared cache is incomplete`
  - Re-run `build_prepared.py` first
- Error: output already exists
  - add `--overwrite` or change `phase1.output.path`
- Strange percentages
  - check `age_bins.enabled_bins_only`, `auto_normalize`, and disabled bins
- Import/module errors
  - confirm virtual environment is active and dependencies are installed
