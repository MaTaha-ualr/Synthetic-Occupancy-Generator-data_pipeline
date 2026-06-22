# Phase 1 Config Parameters

This file explains the parameters in `phase1.yaml`. Phase 1 generates the baseline synthetic people and address dataset that later phases consume.

Most values ending in `_pct` are percentages from `0` to `100`. Values named `*_distribution` are weights: they are normalized by the generator, so they can sum to `100`, but they do not have to unless explicitly noted.

## Top-Level Run Settings

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.n_people` | `10000` | Number of unique people/entities to generate. This controls the number of distinct `PersonKey` values. |
| `phase1.n_records` | `14000` | Number of base output rows to request. This can be larger than `n_people` when redundancy is enabled. The final `records_written` can be larger when nickname formal-backup rows are added. |
| `phase1.seed` | `20260303` | Random seed for reproducible output. Reusing the same config, prepared cache, and seed should produce the same generated dataset. |

Important relationship: when `redundancy.enabled` is `true`, `n_records` must be greater than `n_people` and must fit inside the configured per-person row bounds:

```text
n_people * min_records_per_entity <= n_records <= n_people * max_records_per_entity
```

When `redundancy.enabled` is `false`, `n_records` must equal `n_people`.

Important output-count note: `n_records` is the requested base row count. When
nicknames are enabled, Phase 1 guarantees that every person with a nickname row
also has at least one formal-name row. If a nickname person does not already
have a formal row, the generator appends one extra formal backup row with a new
unique address. Check `records_requested`, `records_written`, and
`formal_copy_records_added` in the manifest or quality report to see the final
row count.

## `output`

Controls where and how the generated dataset is written.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.output.format` | `csv` | Output format. Valid values are `csv`, `parquet`, `txt`, `xlsx`, and `excel`. `excel` is accepted as an alias for `xlsx`. CSV writes one comma-delimited file. TXT writes one tab-delimited text file. Excel writes one `.xlsx` workbook with a `Phase1` sheet. Parquet writes chunk files into a `<output_stem>_parts` directory. |
| `phase1.output.path` | `outputs/Phase1_people_addresses.csv` | Output path relative to the Phase 1 project directory, which is `phase1/` when using `phase1/scripts/generate_phase1.py`. If the path has a known output suffix like `.csv` and the selected format needs another suffix, the generator writes the viewable file with the matching suffix such as `.txt` or `.xlsx`. |
| `phase1.output.chunk_size` | `5000` | Number of rows processed per write chunk. Larger chunks can be faster but use more memory. Smaller chunks reduce peak memory use. |

Format examples:

| Desired output | Set `phase1.output.format` to | Resulting file shape |
|---|---|---|
| CSV file | `csv` | `Phase1_people_addresses.csv` |
| Tab-delimited text file | `txt` | `Phase1_people_addresses.txt` |
| Excel workbook | `excel` or `xlsx` | `Phase1_people_addresses.xlsx` |
| Parquet dataset | `parquet` | `Phase1_people_addresses_parts/part_*.parquet` |

## `name_duplication`

Controls legitimate name collisions across different people. This does not duplicate rows. It makes separate people share one or more name surfaces so downstream matching can be tuned around first-name, last-name, or full-name ambiguity.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.name_duplication.first_name_people_pct` | `0.0` | Target percent of people that should participate in forced formal first-name collision groups. Set to `0` to disable first-name forcing. |
| `phase1.name_duplication.last_name_people_pct` | `0.0` | Target percent of people that should participate in forced last-name collision groups. Set to `0` to disable last-name forcing. |
| `phase1.name_duplication.full_name_people_pct` | `58.0` | Target percent of people that should participate in exact formal full-name collision groups. This copies first, middle, last, and suffix values together. |
| `phase1.name_duplication.exact_full_name_people_pct` | legacy alias | Backward-compatible alias for `full_name_people_pct`. Use `full_name_people_pct` in new configs. |
| `phase1.name_duplication.collision_group_min_size` | `3` | Minimum number of people in one duplicate-name group. Must be at least `2`. |
| `phase1.name_duplication.collision_group_max_size` | `7` | Maximum number of people in one duplicate-name group. Must be greater than or equal to `collision_group_min_size`. |

Use this section to make matching and deduplication harder without adding dirty data. For example, you can raise only `first_name_people_pct` to create common first-name ambiguity, raise only `last_name_people_pct` to create surname ambiguity, or raise `full_name_people_pct` when distinct people should legitimately share the same first, middle, last, and suffix values.

The generator reports separate forced and achieved metrics for first-name, last-name, formal full-name, and display full-name collisions in the manifest and quality report. Nickname settings can change the display `FirstName` and `FullName`, but these duplication controls operate on the formal name fields.

## `redundancy`

Controls how many rows each person can have in the flat Phase 1 output.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.redundancy.enabled` | `true` | Allows more output rows than unique people. If `false`, every person gets exactly one row. |
| `phase1.redundancy.min_records_per_entity` | `1` | Minimum number of rows each person must receive. Must be at least `1`. |
| `phase1.redundancy.max_records_per_entity` | `10` | Maximum number of rows each person can receive. Must be at least `min_records_per_entity`. |
| `phase1.redundancy.shape` | `heavy_tail` | How extra rows are distributed across people. Valid values are `balanced` and `heavy_tail`. |
| `phase1.redundancy.heavy_tail_alpha` | `1.3` | Controls concentration when `shape` is `heavy_tail`. Lower values concentrate extra rows among fewer people; higher values spread them more evenly. Ignored for `balanced`. |

`balanced` spreads extra rows around the population. With the default `10,000`
people and `14,000` requested base rows, it would only produce 1-2 base rows per
person because there are only 4,000 extra rows to distribute. `heavy_tail`
models systems where a smaller set of people have many more records than most
people and allows the default run to exercise the configured 1-10 redundancy
range.

## `nicknames`

Controls whether the emitted display first name can differ from the formal first name.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.nicknames.enabled` | `true` | Turns nickname substitution on or off. If `false`, `FirstName` stays the same as `FormalFirstName`. |
| `phase1.nicknames.source_dir` | `Names/nick names` | Source directory used by preprocessing to build `prepared/nicknames.json`. |
| `phase1.nicknames.mode` | `per_record` | Valid values are `per_record` and `per_person`. `per_record` can vary the display name row by row. `per_person` keeps one nickname choice for each nickname person, plus any required formal backup row. |
| `phase1.nicknames.usage_pct` | `35.0` | Target percent of rows or people using a nickname, depending on `mode`. In `per_record` mode it targets rows. In `per_person` mode it targets people. |

The output keeps both identity surfaces:

| Output field | Meaning |
|---|---|
| `FormalFirstName` | Canonical first name from the name source. |
| `FirstName` | Display first name after nickname logic. |
| `FirstNameType` | `FORMAL` or `NICKNAME`. |

Formal backup rule: whenever a person has at least one `NICKNAME` row, that same
`PersonKey` will also have at least one `FORMAL` row. If the requested rows do
not naturally include a formal row for that person, Phase 1 appends a formal
backup row. The backup row uses the same person-level fields and a new unique
address.

## `distributions`

Controls demographic category mixes and name-pool behavior.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.distributions.gender.female` | `50.5` | Target weight/percentage for generated female entities. |
| `phase1.distributions.gender.male` | `49.5` | Target weight/percentage for generated male entities. |
| `phase1.distributions.ethnicity` | `null` | Optional ethnicity distribution override. When `null`, ethnicity percentages are loaded from `prepared/demographics.json`. |
| `phase1.distributions.unisex_weight_multiplier` | `0.35` | Weight multiplier applied to unisex names when building male and female first-name pools. Lower values make unisex names less likely in those pools. |

The generator normalizes the gender and ethnicity values internally. If values sum to `100`, they behave like percentages. If they do not, they behave like relative weights.

## `age_bins`

Controls the generated age structure and the `AgeBin` output value.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.age_bins.enabled_bins_only` | `true` | If `true`, bins with `enabled: false` are ignored. If `false`, all listed bins are considered. |
| `phase1.age_bins.auto_normalize` | `true` | If `true`, enabled bin percentages are normalized to sum to `100`. If `false`, enabled bin `pct` values must already sum to exactly `100`. |
| `phase1.age_bins.pct_interpretation` | `absolute` | Metadata label written to the manifest. In the current generator, this does not change sampling math. |
| `phase1.age_bins.bins[*].id` | varies | Internal bin identifier. This becomes the `AgeBin` value in the output. |
| `phase1.age_bins.bins[*].label` | varies | Human-readable label for documentation and manifest metadata. |
| `phase1.age_bins.bins[*].min_age` | varies | Inclusive lower age bound for the bin. |
| `phase1.age_bins.bins[*].max_age` | varies | Inclusive upper age bound for the bin. |
| `phase1.age_bins.bins[*].pct` | varies | Target share/weight for the bin. |
| `phase1.age_bins.bins[*].enabled` | varies | Per-bin switch. Only affects generation when `enabled_bins_only` is `true`. |

Current bins:

| Bin id | Label | Age range | Percent | Enabled |
|---|---|---:|---:|---|
| `age_0_17` | `0-17 (Children)` | `0` to `17` | `21.8` | `true` |
| `age_18_34` | `18-34 (Young Adults)` | `18` to `34` | `22.4` | `true` |
| `age_35_64` | `35-64 (Adults)` | `35` to `64` | `38.1` | `true` |
| `age_65_plus` | `65+ (Seniors)` | `65` to `95` | `17.7` | `true` |

Dates of birth are sampled so that the generated age falls inside the selected bin as of the run date.

## `address`

Controls residence address generation, apartment formatting, and mailing-address divergence.

### Base Residence Mix

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.address.houses_pct` | `70` | Relative share of output rows that should be house residence rows. |
| `phase1.address.apartments_pct` | `30` | Relative share of output rows that should be apartment residence rows. |
| `phase1.address.house_number_min` | `100` | Lowest generated street number. Must be greater than `0`. |
| `phase1.address.house_number_max` | `99999` | Highest generated street number. Must be greater than or equal to `house_number_min`. |

The house/apartment values are normalized, so `70` and `30` mean a 70/30 split. Address capacity depends on the street-number range plus the prepared street, city, and state pools.

### Apartment Settings

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.address.apartment.units_per_building` | `60` | Maximum generated units per apartment building before the generator moves to another base address. Must be greater than `0`. |
| `phase1.address.apartment.unit_type_distribution.APT` | `70` | Weight for using `APT` as the unit type. |
| `phase1.address.apartment.unit_type_distribution.UNIT` | `20` | Weight for using `UNIT` as the unit type. |
| `phase1.address.apartment.unit_type_distribution.STE` | `10` | Weight for using `STE` as the unit type. |
| `phase1.address.apartment.unit_format_distribution.numeric_3digit` | `55` | Weight for numeric apartment values like `101`, `102`, and so on. |
| `phase1.address.apartment.unit_format_distribution.floor_letter` | `30` | Weight for floor-letter values like `1A`, `1B`, and so on. |
| `phase1.address.apartment.unit_format_distribution.wing_numeric` | `15` | Weight for wing-number values like `A01`, `A02`, and so on. |

The apartment unit distributions control formatting only. They do not change how many apartment rows are generated.

### Mailing Settings

The current config uses PO-box-style mailing addresses. Rows that do not receive a PO box have blank mailing fields.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.address.mailing.style` | `ohc_po_box` | Mailing-address model label. The current config uses PO-box behavior. |
| `phase1.address.mailing.house_po_box_pct` | `42` | Percent of house rows that receive a PO-box mailing address. |
| `phase1.address.mailing.apartment_po_box_pct` | `44` | Percent of apartment rows that receive a PO-box mailing address. |
| `phase1.address.mailing.apartment_shared_po_box_pct` | `100` | For apartment PO-box rows, percent that use a shared building-level PO-box profile instead of a row-specific one. |
| `phase1.address.mailing.po_box_zip_keep_pct` | `17` | Percent of PO-box mailing ZIP codes that stay the same as the residence ZIP. The rest are shifted. |
| `phase1.address.mailing.po_box_zip_shift_min` | `1` | Minimum ZIP-code shift when the mailing ZIP does not stay the same. |
| `phase1.address.mailing.po_box_zip_shift_max` | `799` | Maximum ZIP-code shift when the mailing ZIP does not stay the same. |
| `phase1.address.mailing.po_box_number_digits.d3` | `10` | Weight for a 3-digit PO-box number in `MailingStreetNumber`. |
| `phase1.address.mailing.po_box_number_digits.d4` | `42` | Weight for a 4-digit PO-box number in `MailingStreetNumber`. |
| `phase1.address.mailing.po_box_number_digits.d5` | `48` | Weight for a 5-digit PO-box number in `MailingStreetNumber`. |
| `phase1.address.mailing.po_box_route_digits.d3` | `15` | Weight for a 3-digit secondary PO-box route value in `MailingUnitNumber`. |
| `phase1.address.mailing.po_box_route_digits.d4` | `35` | Weight for a 4-digit secondary PO-box route value in `MailingUnitNumber`. |
| `phase1.address.mailing.po_box_route_digits.d5` | `25` | Weight for a 5-digit secondary PO-box route value in `MailingUnitNumber`. |
| `phase1.address.mailing.po_box_route_digits.d6` | `25` | Weight for a 6-digit secondary PO-box route value in `MailingUnitNumber`. |

## `fill_rates`

Controls optional person-level fields. These are assigned to people first, then copied to every row for that person.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.fill_rates.middle_name` | `65` | Percent of people that receive a middle name. |
| `phase1.fill_rates.suffix` | `33` | Percent of people that receive a suffix. |
| `phase1.fill_rates.phone` | `98` | Percent of people that receive a phone number. |

Because these are person-level settings, row-level missingness can drift when redundancy gives some people more rows than others.

## `suffix_distribution`

Controls which suffix is selected when a person is chosen to have a suffix by `fill_rates.suffix`.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.suffix_distribution.Jr` | `65` | Weight for suffix `Jr`. |
| `phase1.suffix_distribution.Sr` | `20` | Weight for suffix `Sr`. |
| `phase1.suffix_distribution.III` | `15` | Weight for suffix `III`. |
| `phase1.suffix_distribution.I` | `34` | Weight for suffix `I`. |

This section is about composition, not fill rate. For example, increasing `Jr` changes the mix of suffix values, while increasing `fill_rates.suffix` changes how many people have any suffix at all.

## `residence_dates`

Controls generated residence start and end dates.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.residence_dates.start_year_min` | `1995` | Earliest year a generated residence start date can use. Must be between `1900` and `2100`. |
| `phase1.residence_dates.open_ended_pct` | `84` | Percent of rows with a blank `ResidenceEndDate`, representing an open/current residence interval. |
| `phase1.residence_dates.min_duration_days` | `90` | Minimum number of days between start and end date when an end date is generated. Must be `0` or greater. |

Residence dates are currently sampled per row. They shape the date distribution, but they do not guarantee a coherent multi-row timeline for each repeated person.

## `quality`

Controls validation and reporting after generation.

| Parameter | Current value | What it does |
|---|---:|---|
| `phase1.quality.distribution_tolerance_pct` | `1.5` | Allowed absolute percentage-point difference between expected and achieved gender, ethnicity, and age-bin distributions in the quality report. |
| `phase1.quality.exact_uniqueness_check_max_rows` | `250000` | Maximum row count for expensive exact duplicate checks. If `records_written` is at or below this value, the generator re-reads the output and checks uniqueness exactly. Above this threshold, it relies on deterministic construction and summary checks. |

## Quick Tuning Guide

| If you want to change... | Edit these parameters |
|---|---|
| Number of unique people | `n_people` |
| Requested base row count | `n_records`, `redundancy.*` |
| How often people repeat | `redundancy.min_records_per_entity`, `redundancy.max_records_per_entity`, `redundancy.shape` |
| Name collisions | `name_duplication.*` |
| Nickname variation | `nicknames.enabled`, `nicknames.mode`, `nicknames.usage_pct` |
| Age mix | `age_bins.bins[*].pct`, `age_bins.bins[*].enabled` |
| Gender or ethnicity mix | `distributions.gender`, `distributions.ethnicity` |
| House vs apartment rows | `address.houses_pct`, `address.apartments_pct` |
| Mailing PO-box behavior | `address.mailing.*` |
| Optional fields | `fill_rates.*`, `suffix_distribution.*` |
| Validation strictness | `quality.*` |
