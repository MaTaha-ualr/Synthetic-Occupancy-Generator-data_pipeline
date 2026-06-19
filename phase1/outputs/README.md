# Phase 1 Output Files

This folder contains the generated Phase 1 people-and-address output. The data
files are produced by:

```powershell
python phase1/scripts/generate_phase1.py --overwrite
```

Run this from the repository root after building the prepared cache with:

```powershell
python phase1/scripts/build_prepared.py
```

The generated CSV and JSON files in this folder are runtime artifacts. They can
be deleted and regenerated. This README explains what those files mean.

## Files In This Folder

| File | What it is | How to use it |
|---|---|---|
| `Phase1_people_addresses.csv` | Main generated Phase 1 dataset. | Use this as the person/address input for downstream phases after reviewing it. |
| `Phase1_people_addresses.manifest.json` | Run manifest for the generated dataset. | Check the exact config snapshot, seed, row counts, output path, redundancy stats, nickname stats, and normalized target distributions. |
| `Phase1_people_addresses.quality_report.json` | Quality summary for the generated dataset. | Check expected vs. achieved distributions, uniqueness checks, duplicate-name metrics, nickname metrics, and missingness percentages. |
| `README.md` | Folder guide. | Read this when you need to understand the generated files and CSV columns. |

If `phase1.output.format` is changed in `phase1/configs/phase1.yaml`, the main
output may be written as `.txt`, `.xlsx`, or parquet parts instead of the default
CSV. The manifest and quality report follow the same output stem.

## Current Default Output Shape

With the default `phase1/configs/phase1.yaml`, the output is configured for:

| Setting | Default value |
|---|---:|
| Unique generated people | `10,000` |
| Requested base rows | `14,000` |
| Random seed | `20260303` |
| Output format | `csv` |
| Output path | `phase1/outputs/Phase1_people_addresses.csv` |
| Redundancy | Enabled |
| Nicknames | Enabled |
| House/apartment mix | `70%` houses, `30%` apartments |

Because redundancy is enabled, the CSV has more rows than people. A repeated
person keeps the same `PersonKey` but gets a different `RecordKey`,
`EntityRecordIndex`, and address row.

The configured row count is stored as `records_requested`. The actual CSV row
count is stored as `records_written`. When nicknames are enabled,
`records_written` can be larger because Phase 1 appends formal backup rows for
nickname people that do not already have a formal row.

## Main CSV

Default file:

```text
Phase1_people_addresses.csv
```

This is a flat, row-oriented dataset. Each row represents one generated record
for a synthetic person at a generated residence address, with optional mailing
address fields.

Important behavior:

- Text fields are written in uppercase.
- `RecordKey` is unique per row.
- `PersonKey` identifies the generated person/entity.
- `EntityRecordIndex` counts rows for the same `PersonKey`.
- `AddressKey` is unique per generated residence row.
- `FirstName` and `FullName` can differ from `FormalFirstName` and
  `FormalFullName` when nickname logic is applied.
- Every `PersonKey` with a `NICKNAME` row also has at least one `FORMAL` row.
  If needed, the generator appends an extra formal backup row with a new unique
  address.
- Blank mailing fields mean no separate mailing address was generated for that
  row.

## CSV Column Dictionary

| Column | Meaning |
|---|---|
| `RecordKey` | Unique row identifier for this generated output file. |
| `PersonKey` | Stable generated person/entity identifier. Repeated rows for the same person share this value. |
| `EntityRecordIndex` | Sequence number for a person's rows, starting at `1` for each `PersonKey`. |
| `AddressKey` | Unique generated residence-address identifier for the row. |
| `FormalFirstName` | Formal first name sampled from the prepared first-name pool. |
| `FirstName` | Display first name after nickname rules. This may be a nickname or the formal first name. |
| `FirstNameType` | Indicates whether `FirstName` is `FORMAL` or `NICKNAME`. |
| `MiddleName` | Optional generated middle name. Blank when the middle-name fill rate did not assign one. |
| `LastName` | Generated last name. |
| `Suffix` | Optional name suffix such as `JR`, `SR`, `III`, or `I`. Blank when not assigned. |
| `FormalFullName` | Full formal name assembled from formal first, middle, last, and suffix values. |
| `FullName` | Display full name assembled from display first name, middle, last, and suffix values. |
| `Gender` | Generated gender category used for name and demographic sampling. |
| `Ethnicity` | Generated ethnicity category. |
| `DOB` | Generated date of birth in `YYYY-MM-DD` format. |
| `Age` | Age derived from `DOB` at generation time. |
| `AgeBin` | Configured age-bin label, such as `AGE_0_17`, `AGE_18_34`, `AGE_35_64`, or `AGE_65_PLUS`. |
| `SSN` | Synthetic Social Security number formatted like `###-##-####`. This is generated data, not a real SSN. |
| `Phone` | Optional synthetic phone number. Blank when the phone fill rate did not assign one. |
| `ResidenceType` | Residence category, usually `HOUSE` or `APARTMENT`. |
| `ResidenceStreetNumber` | Generated residence street number. |
| `ResidenceStreetName` | Generated residence street name. |
| `ResidenceUnitType` | Apartment or unit designator such as `APT`, `UNIT`, or `STE`. Blank for house rows. |
| `ResidenceUnitNumber` | Generated apartment or unit number. Blank for house rows. |
| `ResidenceCity` | Generated residence city. |
| `ResidenceState` | Generated residence state. |
| `ResidencePostalCode` | Generated residence ZIP/postal code. |
| `ResidenceStartDate` | Generated residence start date in `YYYY-MM-DD` format. |
| `ResidenceEndDate` | Generated residence end date in `YYYY-MM-DD` format. Blank means open-ended/current residence. |
| `MailingAddressMode` | Mailing-address mode for the row, such as `PO BOX`. Blank means no separate mailing address. |
| `MailingStreetNumber` | Generated mailing street or PO-box number. Blank when no separate mailing address was generated. |
| `MailingStreetName` | Generated mailing street name. For PO-box rows this is usually `PO BOX`. |
| `MailingUnitType` | Mailing unit type if applicable. Usually blank for PO-box style mailing addresses. |
| `MailingUnitNumber` | Generated mailing unit or PO-box route value if applicable. |
| `MailingCity` | Generated mailing city. Blank when no separate mailing address was generated. |
| `MailingState` | Generated mailing state. Blank when no separate mailing address was generated. |
| `MailingPostalCode` | Generated mailing ZIP/postal code. Blank when no separate mailing address was generated. |

## Manifest JSON

Default file:

```text
Phase1_people_addresses.manifest.json
```

The manifest explains the run that produced the output. It is the best place to
confirm whether the CSV was generated with the expected settings.

Important fields:

| Field | Meaning |
|---|---|
| `generated_at_utc` | Time the output was generated. |
| `run_id` | Run identifier based on the output stem and generation timestamp. |
| `seed` | Random seed used for reproducible generation. |
| `n_people` | Number of unique generated people. |
| `n_records` / `records_requested` | Requested base output row count from the config. |
| `records_written` | Actual number of rows written after any nickname formal-backup rows are appended. |
| `formal_copy_records_added` | Number of formal backup rows appended for nickname people. |
| `nickname_formal_backup` | Summary showing nickname people, people that already had formal rows, and added formal backup rows. |
| `output_format` | Output format used by the run. |
| `output_path` | Full path to the generated main output. |
| `housing_counts` | Number of house and apartment rows written. |
| `redundancy` | Settings and achieved records-per-person statistics. |
| `nicknames` | Nickname mode, target usage, and achieved usage. |
| `name_duplication` | Configured and achieved duplicate-name metrics. |
| `normalization` | Normalized gender, ethnicity, age-bin, and housing target distributions. |
| `config_snapshot` | Phase 1 config values used for this run. |

## Quality Report JSON

Default file:

```text
Phase1_people_addresses.quality_report.json
```

The quality report summarizes whether the generated data matched the configured
targets and basic uniqueness expectations.

Important fields:

| Field | Meaning |
|---|---|
| `entity_count` | Number of unique generated people. |
| `row_count` | Actual number of output rows written. |
| `records_requested` | Requested base output row count from the config. |
| `records_written` | Actual number of rows written after any nickname formal-backup rows are appended. |
| `formal_copy_records_added` | Number of appended formal backup rows. |
| `nickname_formal_backup` | Summary of the formal-backup rule for nickname people. |
| `tolerance_pct` | Allowed percentage-point tolerance for distribution checks. |
| `expected_distributions_pct` | Target gender, ethnicity, and age-bin percentages. |
| `achieved_distributions_pct` | Actual percentages observed in the generated output. |
| `distribution_checks` | Per-category expected value, achieved value, delta, and pass/fail flag. |
| `name_duplication` | Duplicate-name metrics for configured name-collision behavior. |
| `nickname_metrics` | Nickname target and achieved usage metrics. |
| `uniqueness_checks` | Checks for row keys, person counts, address uniqueness, and duplicate conditions. |
| `missingness_pct` | Percent of blank/missing values by output column. |

## Quick Checks

Preview the first rows:

```powershell
Import-Csv phase1/outputs/Phase1_people_addresses.csv |
  Select-Object -First 5 |
  Format-Table -AutoSize
```

Count rows:

```powershell
(Import-Csv phase1/outputs/Phase1_people_addresses.csv).Count
```

Count unique people:

```powershell
$rows = Import-Csv phase1/outputs/Phase1_people_addresses.csv
($rows | Select-Object -ExpandProperty PersonKey -Unique).Count
```

Check whether any distribution checks failed:

```powershell
$quality = Get-Content phase1/outputs/Phase1_people_addresses.quality_report.json | ConvertFrom-Json
$quality.distribution_checks.PSObject.Properties.Value.PSObject.Properties.Value |
  Where-Object { $_.within_tolerance -eq $false }
```

## When To Regenerate

Regenerate these files when:

- `phase1/configs/phase1.yaml` changes.
- Raw source files under `phase1/Addresses/`, `phase1/Names/`, or
  `phase1/Data/` change.
- Prepared cache files under `phase1/prepared/` are rebuilt.
- You need a new seed, output size, output format, redundancy setting, nickname
  setting, or demographic/address distribution.

After reviewing the output, copy the approved CSV, manifest, and quality report
to `phase1/outputs_phase1/` when you want that run to become the canonical Phase
1 baseline for Phase 2.
