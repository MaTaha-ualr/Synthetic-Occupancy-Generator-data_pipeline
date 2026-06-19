# Phase 1 Runbook

Phase 1 generates the baseline synthetic people-and-address dataset that later
phases consume. It turns the raw name, address, demographic, and nickname source
files into a prepared cache, then uses that cache plus `configs/phase1.yaml` to
write the final person/address output.

Run the commands below from the repository root.

## What To Run First

### 1. Install Python dependencies

Use the repository-level requirements file before running Phase 1:

```powershell
python -m pip install -r requirements.txt
```

### 2. Build the prepared cache

Run this first on a fresh checkout, and rerun it whenever files under
`phase1/Addresses/`, `phase1/Names/`, or `phase1/Data/` change.

```powershell
python phase1/scripts/build_prepared.py
```

Expected result:

- The command prints a JSON manifest.
- `phase1/prepared/` is created or refreshed.
- `phase1/prepared/prepared_manifest.json` lists the prepared files and source
  row counts.

Prepared files:

| File | Purpose |
|---|---|
| `prepared/first_names.parquet` | Normalized first-name pool. |
| `prepared/last_names.parquet` | Normalized last-name and ethnicity-weight pool. |
| `prepared/streets.parquet` | Street-name pool. |
| `prepared/cities.parquet` | City pool. |
| `prepared/states.parquet` | State pool. |
| `prepared/demographics.json` | Demographic distribution source. |
| `prepared/nicknames.json` | Formal-name to nickname mappings. |
| `prepared/prepared_manifest.json` | Cache build summary. |

### 3. Review or edit the generation config

The default config is:

```text
phase1/configs/phase1.yaml
```

The current default run targets:

- `10,000` unique people.
- `14,000` requested base output rows.
- Reproducible seed `20260303`.
- CSV output at `phase1/outputs/Phase1_people_addresses.csv`.
- Redundant people enabled, so some `PersonKey` values appear more than once.
- Nicknames enabled, so `FirstName` may differ from `FormalFirstName`.
- Nickname formal backups enabled by default behavior: every person with a
  nickname row also gets a formal-name row.
- A 70/30 house/apartment residence mix.

For parameter-by-parameter details, see
`phase1/configs/phase1_config_parameters.md`.

### 4. Generate the Phase 1 dataset

```powershell
python phase1/scripts/generate_phase1.py --overwrite
```

Use `--overwrite` when regenerating the same output path. Without it, the script
stops if the output file or parquet parts directory already exists.

Expected result:

- The command prints a JSON result with output paths.
- `phase1/outputs/Phase1_people_addresses.csv` is written.
- `phase1/outputs/Phase1_people_addresses.manifest.json` is written.
- `phase1/outputs/Phase1_people_addresses.quality_report.json` is written.

The configured `phase1.n_records` value is the requested base row count. The
actual file can contain more rows when the generator appends formal backup rows
for nickname people. Compare `records_requested`, `records_written`, and
`formal_copy_records_added` in the command output or manifest.

## Output Files

### Main dataset

Default file:

```text
phase1/outputs/Phase1_people_addresses.csv
```

This is the flat dataset used by downstream phases. It has one row per generated
record and includes the generated person, residence address, mailing address,
and date fields.

Main columns:

| Column group | Columns |
|---|---|
| Record identity | `RecordKey`, `PersonKey`, `EntityRecordIndex`, `AddressKey` |
| Name fields | `FormalFirstName`, `FirstName`, `FirstNameType`, `MiddleName`, `LastName`, `Suffix`, `FormalFullName`, `FullName` |
| Demographics | `Gender`, `Ethnicity`, `DOB`, `Age`, `AgeBin`, `SSN`, `Phone` |
| Residence address | `ResidenceType`, `ResidenceStreetNumber`, `ResidenceStreetName`, `ResidenceUnitType`, `ResidenceUnitNumber`, `ResidenceCity`, `ResidenceState`, `ResidencePostalCode` |
| Residence dates | `ResidenceStartDate`, `ResidenceEndDate` |
| Mailing address | `MailingAddressMode`, `MailingStreetNumber`, `MailingStreetName`, `MailingUnitType`, `MailingUnitNumber`, `MailingCity`, `MailingState`, `MailingPostalCode` |

Important output behavior:

- Text values are uppercased in the generated dataset.
- `RecordKey` is unique per row.
- `PersonKey` identifies the generated person/entity. With redundancy enabled,
  one person can have multiple rows.
- `EntityRecordIndex` is the per-person row number.
- `AddressKey` is unique per generated residence row.
- `FormalFirstName` and `FormalFullName` preserve the formal name surface.
- `FirstName` and `FullName` are the display name surface after nickname logic.
- `FirstNameType` is `FORMAL` or `NICKNAME`.
- Every `PersonKey` with a `NICKNAME` row also has at least one `FORMAL` row.
  If needed, the generator appends a formal backup row with a new unique
  address.
- Blank mailing fields mean the row did not receive a separate mailing address.

### Manifest

Default file:

```text
phase1/outputs/Phase1_people_addresses.manifest.json
```

Use this file to confirm what was generated. It includes:

- `run_id` and `generated_at_utc`.
- `seed`, `n_people`, `records_requested`, `records_written`, and
  `formal_copy_records_added`.
- `output_format`, `output_path`, and `chunk_size`.
- House and apartment counts.
- Redundancy statistics such as min/max/mean records per person.
- Nickname metrics.
- Nickname formal-backup metrics.
- Name-duplication metrics.
- Normalized gender, ethnicity, age-bin, and housing distributions.
- A snapshot of the config used for the run.

### Quality report

Default file:

```text
phase1/outputs/Phase1_people_addresses.quality_report.json
```

Use this file to check whether the generated data matched the configured
targets. It includes:

- `entity_count` and `row_count`.
- `records_requested`, `records_written`, and `formal_copy_records_added`.
- Expected vs. achieved gender, ethnicity, and age-bin distributions.
- Distribution tolerance checks.
- Name-duplication and nickname metrics.
- Uniqueness checks.
- Missingness percentages by output column.

## Quick Inspection Commands

Show the first few output rows:

```powershell
Import-Csv phase1/outputs/Phase1_people_addresses.csv |
  Select-Object -First 5 |
  Format-Table -AutoSize
```

Count output rows:

```powershell
(Import-Csv phase1/outputs/Phase1_people_addresses.csv).Count
```

Check unique people vs. rows:

```powershell
$rows = Import-Csv phase1/outputs/Phase1_people_addresses.csv
$rows.Count
($rows | Select-Object -ExpandProperty PersonKey -Unique).Count
```

View the manifest and quality report:

```powershell
Get-Content phase1/outputs/Phase1_people_addresses.manifest.json
Get-Content phase1/outputs/Phase1_people_addresses.quality_report.json
```

## Copy Output For Phase 2

After reviewing the generated output, copy the current Phase 1 dataset into the
canonical Phase 2 baseline folder:

```powershell
Copy-Item phase1/outputs/Phase1_people_addresses.csv phase1/outputs_phase1/Phase1_people_addresses.csv -Force
Copy-Item phase1/outputs/Phase1_people_addresses.manifest.json phase1/outputs_phase1/Phase1_people_addresses.manifest.json -Force
Copy-Item phase1/outputs/Phase1_people_addresses.quality_report.json phase1/outputs_phase1/Phase1_people_addresses.quality_report.json -Force
```

`phase1/outputs_phase1/` is intended to hold the single canonical Phase 1 master
input for Phase 2 runs. The generated data files in that folder are gitignored;
only its README should be committed.

## Changing Output Format

Edit `phase1.output.format` in `phase1/configs/phase1.yaml`.

Supported values:

| Format | Result |
|---|---|
| `csv` | One comma-delimited file. |
| `txt` | One tab-delimited text file. |
| `xlsx` or `excel` | One Excel workbook with a `Phase1` sheet. |
| `parquet` | Chunk files in a `<output_stem>_parts` directory. |

If `phase1.output.path` ends in a known suffix such as `.csv`, the generator
changes the suffix to match the selected format. For example, setting
`format: txt` with the default path writes `Phase1_people_addresses.txt`.

## Common Tuning Points

Use `phase1/configs/phase1.yaml` for normal changes:

| Goal | Config keys |
|---|---|
| Change unique people | `phase1.n_people` |
| Change requested base rows | `phase1.n_records`, `phase1.redundancy.*` |
| Control repeated people | `phase1.redundancy.min_records_per_entity`, `max_records_per_entity`, `shape` |
| Control duplicate names | `phase1.name_duplication.*` |
| Turn nicknames on/off or change usage | `phase1.nicknames.*` |
| Change age mix | `phase1.age_bins.*` |
| Change gender or ethnicity mix | `phase1.distributions.*` |
| Change house/apartment mix | `phase1.address.houses_pct`, `phase1.address.apartments_pct` |
| Change PO-box mailing behavior | `phase1.address.mailing.*` |
| Change optional field fill rates | `phase1.fill_rates.*`, `phase1.suffix_distribution.*` |
| Change validation tolerance | `phase1.quality.*` |

Important relationship:

```text
n_people * min_records_per_entity <= n_records <= n_people * max_records_per_entity
```

When redundancy is disabled, `n_records` must equal `n_people`.

This relationship applies to the requested base rows. Nickname formal-backup
rows are appended after base row allocation, so `records_written` can be larger
than `n_records`.

## Tests

Run the Phase 1 test suite after changing generation logic or config behavior:

```powershell
python -m pytest -q phase1/tests/test_phase1_pipeline.py
```

The tests build a temporary prepared cache and generate small datasets in
multiple formats, then verify row counts, uppercase output, redundancy behavior,
nickname behavior, duplicate-name metrics, and output-format handling.

## Folder Reference

| Path | Description |
|---|---|
| `Addresses/` | Raw state, city, and street source files. |
| `Names/` | Raw first-name, last-name, and nickname source files. |
| `Data/` | Raw demographic source data. |
| `configs/phase1.yaml` | Main generation config. |
| `configs/phase1_config_parameters.md` | Detailed config documentation. |
| `scripts/build_prepared.py` | Builds `prepared/` from raw source files. |
| `scripts/generate_phase1.py` | Generates the final Phase 1 dataset. |
| `src/sog_phase1/` | Phase 1 implementation. |
| `tests/` | Phase 1 tests. |
| `prepared/` | Generated cache, gitignored. |
| `outputs/` | Generated run outputs, gitignored. |
| `outputs_phase1/` | Canonical Phase 1 baseline copy for Phase 2, data gitignored. |

## Troubleshooting

| Symptom | What to check |
|---|---|
| `ModuleNotFoundError` for pandas, yaml, pyarrow, or openpyxl | Run `python -m pip install -r requirements.txt`. |
| Output already exists | Rerun generation with `--overwrite`, or change `phase1.output.path`. |
| Prepared files are missing | Run `python phase1/scripts/build_prepared.py` first. |
| Config validation fails | Check `n_people`, `n_records`, and `redundancy.*` bounds. |
| Distribution checks are outside tolerance | Review `quality_report.json`, then adjust `phase1.quality.distribution_tolerance_pct` or the target distribution settings. |
