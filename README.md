# SOG Synthetic Data Pipeline

SOG is a reproducible pipeline for generating synthetic person + address records from the included reference datasets.

This repository is structured so a first-time user can:
- download the project,
- run it with a few commands,
- customize generation settings in one YAML file,
- validate quality using built-in reports and tests.

## Branches
- Need old stable Phase-1 behavior: use `phase1-legacy`.
- Need latest features (entity/record redundancy + nickname variants): use `main`.
- Full branch and tag guide: [`BRANCHES.md`](BRANCHES.md).

## What You Can Generate
The main output is:

- `outputs/Phase1_people_addresses.csv`

Each row is one synthetic person record with:
- identity fields (`RecordKey`, `PersonKey`, names, gender, ethnicity),
- demographic fields (`DOB`, `Age`, `AgeBin`),
- contact fields (`SSN`, `Phone`),
- residence and mailing address fields.

Notes:
- `PersonKey` identifies the entity (person) and can repeat when redundancy is enabled.
- `RecordKey` is always unique per row.
- `AddressKey` is always unique per row.

The run also writes:
- `outputs/Phase1_people_addresses.manifest.json` (settings + run metadata)
- `outputs/Phase1_people_addresses.quality_report.json` (distribution checks, uniqueness checks, missingness)

## Repository Structure
```text
SOG/
|-- Addresses/                          # raw address reference CSVs
|-- Names/                              # raw name reference CSVs
|-- Data/demographics_extracted/        # raw demographic reference CSV
|-- configs/phase1.yaml                 # main control panel
|-- scripts/build_prepared.py           # raw CSV -> prepared parquet/json cache
|-- scripts/generate_phase1.py          # prepared cache + config -> generated dataset
|-- src/sog_phase1/                     # pipeline implementation
|-- tests/test_phase1_pipeline.py       # smoke/integration test
|-- BRANCHES.md                         # branch/tag purpose and checkout guide
|-- docs/BEGINNER_GUIDE.md              # full start-from-zero tutorial
|-- docs/GITHUB_PUBLISH.md              # step-by-step GitHub upload guide
|-- docs/PHASE1_SUMMARY.md              # detailed summary of completed Phase-1 work
|-- requirements.txt
`-- requirements-dev.txt
```

## Prerequisites
- Python 3.10+
- Git
- Terminal (PowerShell, Command Prompt, or bash)

## Quick Start (First Run)
1. Clone and enter the repository:
```bash
git clone https://github.com/MaTaha-ualr/Synthetic-Occupancy-Generator-data_pipeline.git
cd Synthetic-Occupancy-Generator-data_pipeline
```

2. Create and activate a virtual environment:

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

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Build prepared cache (one-time per raw data version):
```bash
python scripts/build_prepared.py --raw-root . --prepared-dir prepared
```

5. Generate Phase-1 output:
```bash
python scripts/generate_phase1.py --config configs/phase1.yaml --prepared-dir prepared --overwrite
```

## Configure Generation
Edit [`configs/phase1.yaml`](configs/phase1.yaml).

Most important fields:
- `n_entities`: number of unique people
- `n_records`: number of output rows (`>= n_entities`)
- `redundancy.enabled`: if `true`, repeated `PersonKey` rows are allowed; if `false`, `n_records` must equal `n_entities`
- `redundancy.min_records_per_entity` / `max_records_per_entity`: bounds for records per person
- `nicknames.enabled`: enable nickname-based first-name variation
- `nicknames.mode`: `per_record` or `per_person`
- `nicknames.usage_pct`: target percent of rows using nickname first names
- `seed`: reproducibility key (same seed + config -> same output pattern)
- `output.format`: `csv` or `parquet`
- `name_duplication.exact_full_name_people_pct`: percent of rows that should be in duplicate full-name groups
- `name_duplication.collision_group_min_size` / `collision_group_max_size`: min/max people allowed per exact full-name collision group
- `distributions.gender`: target gender split
- `age_bins`: active age groups + target percentages
- `address.houses_pct` / `address.apartments_pct`: housing mix
- `fill_rates`: probability of filling optional fields (`middle_name`, `suffix`, `phone`)

Collision size bounds are applied to forced collision groups. Natural random sampling can still create a few groups outside that range.

Detailed explanation is in [`docs/BEGINNER_GUIDE.md`](docs/BEGINNER_GUIDE.md).
Completed Phase-1 delivery summary is in [`docs/PHASE1_SUMMARY.md`](docs/PHASE1_SUMMARY.md).

## Validate Results
Run tests:
```bash
pip install -r requirements-dev.txt
python -m pytest -q tests/test_phase1_pipeline.py
```

Check generated quality report:
- `outputs/Phase1_people_addresses.quality_report.json`

Focus on:
- `distribution_checks` (expected vs achieved percentages)
- `uniqueness_checks` (`RecordKey`, `AddressKey`, full address uniqueness, person/address constraints)
- `missingness_pct` (blank-rate profile)

## Reproducibility Notes
- Keep `seed` fixed for repeatable runs.
- Keep raw CSVs unchanged if you need stable behavior across machines.
- Rebuild `prepared/` whenever raw input files change.

## Common Issues
- `Prepared cache is incomplete`:
  - Run `python scripts/build_prepared.py --raw-root . --prepared-dir prepared`
- `Output file already exists`:
  - Add `--overwrite` or change `phase1.output.path`
- `No module named ...`:
  - Activate your virtual environment and reinstall requirements

## Publish to GitHub
If you have not pushed this project yet, use:
- [`docs/GITHUB_PUBLISH.md`](docs/GITHUB_PUBLISH.md)

## License
This project is licensed under the MIT License. See [`LICENSE`](LICENSE).
