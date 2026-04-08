# Phase-1 and Phase-2 Parameter Reference

## Purpose

This document explains what the configuration parameters mean in the current repository implementation.

For a decision-oriented companion that explains which knobs to change for common benchmark goals, use:

- `reference/PARAMETER_TUNING_PLAYBOOK.md`

It covers three separate control surfaces:

1. `phase1/configs/phase1.yaml`
2. `phase2/scenarios/*.yaml`
3. `Data/phase2_params/*`

Those three layers are related, but they do different jobs:

- Phase-1 config builds the baseline population and record file.
- Phase-2 scenario YAML selects people from Phase-1, simulates truth events, and emits observed datasets.
- Phase-2 parameter files are the statistical prior package and provenance bundle used by the Phase-2 engine.

This guide is based on the current code paths in:

- `phase1/src/sog_phase1/config.py`
- `phase1/src/sog_phase1/generator.py`
- `phase1/src/sog_phase1/preprocess.py`
- `src/sog_phase2/pipeline.py`
- `src/sog_phase2/selection.py`
- `src/sog_phase2/simulator.py`
- `src/sog_phase2/emission.py`
- `src/sog_phase2/constraints.py`
- `src/sog_phase2/quality.py`
- `src/sog_phase2/params.py`

## Big Picture

### Phase-1 mental model

Phase-1 is the baseline generator. It creates a population with stable identity fields and a flat row-based output. In the current implementation:

- `n_people` controls how many distinct people exist.
- `n_records` controls how many total rows are emitted.
- If redundancy is enabled, one `PersonKey` can appear in multiple rows.
- Those repeated rows can vary in display name and address history depending on config.

So the main Phase-1 question is: "How many entities do I want, and how redundant should the flat file be?"

### Phase-2 mental model

Phase-2 is the scenario engine. It does not just "add noise." It does four separate things:

1. Select a subpopulation from Phase-1.
2. Simulate truth-layer events such as `MOVE`, `COHABIT`, `BIRTH`, `DIVORCE`, and `LEAVE_HOME`.
3. Emit one or more observed datasets from that truth layer.
4. Validate and score the resulting scenario.

So the main Phase-2 question is: "Which people enter the scenario, what truth events happen to them, and how do different systems observe them?"

### What `Data/phase2_params` is

`Data/phase2_params/` is not a scenario. It is the parameter bundle that gives Phase-2 a data-backed prior layer and provenance:

- mobility rates
- marriage/divorce rates
- fertility rates
- household type shares
- a compact JSON prior snapshot
- source citations

Some of those files are used directly today. Some are loaded and validated mainly for provenance and future expansion.

## Phase-1 YAML: `phase1/configs/phase1.yaml`

### Current shape

```yaml
phase1:
  n_people: 10000
  n_records: 14000
  seed: 20260303
  output: ...
  name_duplication: ...
  redundancy: ...
  nicknames: ...
  distributions: ...
  age_bins: ...
  address: ...
  fill_rates: ...
  suffix_distribution: ...
  residence_dates: ...
  quality: ...
```

### Top-level controls

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.n_people` | Number of unique entities to generate. | Controls how many unique `PersonKey` values exist. |
| `phase1.n_records` | Number of total output rows to emit. | Controls row-level redundancy. If larger than `n_people`, some people appear more than once. |
| `phase1.seed` | Deterministic seed for RNG. | Controls reproducibility of names, dates, addresses, nickname usage, and all sampling decisions. |

### Story behind `n_people` vs `n_records`

This is the most important Phase-1 distinction.

- `n_people` is entity cardinality.
- `n_records` is flat-file cardinality.

If `n_people = 10000` and `n_records = 14000`, then the generator creates 10,000 people but writes 14,000 rows. That means 4,000 extra rows are distributed across the same people according to the redundancy rules.

### `output`

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.output.format` | Output storage format. Valid values: `csv`, `parquet`. | Changes whether the main artifact is a CSV file or parquet parts. |
| `phase1.output.path` | Output path relative to the Phase-1 project root. | Controls where the dataset is written. |
| `phase1.output.chunk_size` | Maximum rows written per chunk. | Controls write batching and memory behavior, especially for large runs. |

### `name_duplication`

This section creates exact full-name collisions across different people. It is not row duplication. It is entity-level name collision pressure.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.name_duplication.exact_full_name_people_pct` | Target percent of people who should participate in exact duplicate-name groups. | Increases the chance that distinct people share the same full formal name. |
| `phase1.name_duplication.collision_group_min_size` | Smallest duplicate-name group size. | Prevents trivial one-pair-only collisions if you want more realistic common-name clusters. |
| `phase1.name_duplication.collision_group_max_size` | Largest duplicate-name group size. | Caps how large a same-name collision cluster can grow. |

### Story behind `name_duplication`

This section exists to make linkage harder even when the truth layer is clean. It models "common name pressure" rather than dirty data. Two different people can legitimately have the same name, and this block lets you control how often that happens.

### `redundancy`

This section controls how many rows each person gets in the Phase-1 flat file.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.redundancy.enabled` | Turns row-level redundancy on or off. | If `false`, each person gets exactly one row and `n_records` must equal `n_people`. |
| `phase1.redundancy.min_records_per_entity` | Minimum rows per person. | Enforces lower bound on repeated rows. |
| `phase1.redundancy.max_records_per_entity` | Maximum rows per person. | Enforces upper bound on repeated rows. |
| `phase1.redundancy.shape` | Distribution shape for extra rows. Valid values: `balanced`, `heavy_tail`. | `balanced` spreads repeated rows more evenly; `heavy_tail` concentrates them into fewer people. |
| `phase1.redundancy.heavy_tail_alpha` | Tail strength for `heavy_tail`. | Lower values create more concentration; higher values flatten it. Only matters when `shape=heavy_tail`. |

### Story behind `redundancy`

This block answers the question: "If I have more rows than people, how should those extra rows be distributed?"

- `balanced` is closer to mild administrative repetition.
- `heavy_tail` is closer to operational systems where a small number of people generate many more records than the rest.

### `nicknames`

This section controls whether the emitted display first name can differ from the formal first name.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.nicknames.enabled` | Enables nickname substitution. | If `false`, `FirstName` stays formal. |
| `phase1.nicknames.source_dir` | Location of nickname source files. | Controls where the nickname catalog is built from during preprocessing. |
| `phase1.nicknames.mode` | Valid values: `per_record`, `per_person`. | `per_person` chooses one display name per person; `per_record` can vary by row. |
| `phase1.nicknames.usage_pct` | Target percent of emitted rows or people using a nickname. | Controls how often `FirstNameType=NICKNAME` appears. |

### Story behind `nicknames`

This is one of the biggest realism levers in Phase-1. It changes the displayed identity surface without changing the underlying formal identity.

- `FormalFirstName` remains canonical.
- `FirstName` becomes the operational display name.

`per_person` is more stable. `per_record` is noisier and behaves more like multiple systems that do not all use the same preferred name.

### `distributions`

This section sets the demographic mixture of the population.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.distributions.gender` | Gender percentages. | Controls the gender mix of generated entities. |
| `phase1.distributions.ethnicity` | Ethnicity percentages. | Controls ethnicity mix if explicitly set. |
| `phase1.distributions.unisex_weight_multiplier` | Weight multiplier for unisex names in first-name pools. | Controls how strongly unisex names compete against sex-specific name tables. |

### Story behind `ethnicity: null`

In the shipped config, `ethnicity` is `null`. That means the generator falls back to the prepared demographics file rather than a hand-entered override. This is useful when you want the default population mix to come from the extracted demographic source package.

### `age_bins`

This section controls the age structure of the generated entities.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.age_bins.enabled_bins_only` | If `true`, disabled bins are ignored. | Lets you keep bins in the file without using them in the run. |
| `phase1.age_bins.auto_normalize` | If `true`, percentages do not need to sum to exactly 100. | The engine rescales them to 100 internally. |
| `phase1.age_bins.pct_interpretation` | Metadata label describing how the percentages should be read. | Currently recorded in the manifest, but does not change the generator math. |
| `phase1.age_bins.bins[*].id` | Internal age bin identifier. | Drives age-bin assignment and later Phase-2 selection filters. |
| `phase1.age_bins.bins[*].label` | Human-readable label. | Written into metadata and docs-facing artifacts. |
| `phase1.age_bins.bins[*].min_age` | Inclusive lower bound. | Controls sampled DOB range. |
| `phase1.age_bins.bins[*].max_age` | Inclusive upper bound. | Controls sampled DOB range. |
| `phase1.age_bins.bins[*].pct` | Target share for that age bin. | Controls how many people land in the bin. |
| `phase1.age_bins.bins[*].enabled` | Bin switch. | Only matters when `enabled_bins_only=true`. |

### Important note on `pct_interpretation`

`pct_interpretation` is informative today, not operational. The actual age-bin math is driven by `pct` values and `auto_normalize`. So readers should treat `pct_interpretation` as documentation metadata, not as a switch that changes sampling behavior.

### `address`

This section controls the residence and mailing-address generator.

#### Base address mix

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.address.houses_pct` | Share of rows emitted as houses. | Controls top-level residence type mix. |
| `phase1.address.apartments_pct` | Share of rows emitted as apartments. | Controls top-level residence type mix. |
| `phase1.address.house_number_min` | Smallest allowed street number. | Sets address-space lower bound. |
| `phase1.address.house_number_max` | Largest allowed street number. | Sets address-space upper bound and total address capacity. |

#### Apartment formatting

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.address.apartment.units_per_building` | Maximum synthetic units per apartment building. | Controls apartment address capacity. |
| `phase1.address.apartment.unit_type_distribution` | Weights for labels like `APT`, `UNIT`, `STE`. | Changes apartment unit label style. |
| `phase1.address.apartment.unit_format_distribution` | Weights for unit numbering styles. | Changes whether apartment units look numeric, floor-letter, or wing-number based. |

#### Mailing-address behavior

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.address.mailing.style` | Mailing address model. | In current config, `ohc_po_box` enables PO-box-like behavior. |
| `phase1.address.mailing.house_po_box_pct` | Percent of house rows receiving PO-box mailing addresses. | Controls how often house residences separate physical and mailing addresses. |
| `phase1.address.mailing.apartment_po_box_pct` | Percent of apartment rows receiving PO-box mailing addresses. | Same, but for apartments. |
| `phase1.address.mailing.apartment_shared_po_box_pct` | Share of apartment PO-box profiles that can be shared in the apartment logic. | Shapes apartment mailing consistency. |
| `phase1.address.mailing.po_box_zip_keep_pct` | Chance the mailing ZIP stays aligned to the residence ZIP. | Controls whether PO-box ZIPs stay local or drift. |
| `phase1.address.mailing.po_box_zip_shift_min` | Minimum ZIP perturbation when ZIP is changed. | Sets lower bound of ZIP drift. |
| `phase1.address.mailing.po_box_zip_shift_max` | Maximum ZIP perturbation when ZIP is changed. | Sets upper bound of ZIP drift. |
| `phase1.address.mailing.po_box_number_digits` | Distribution for PO-box number length. | Controls `MailingStreetNumber` digit length when PO-box mode is used. |
| `phase1.address.mailing.po_box_route_digits` | Distribution for secondary PO-box route/unit number length. | Controls `MailingUnitNumber` digit length when PO-box mode is used. |

### Story behind `address`

The address block is doing two different jobs:

1. It creates residence addresses.
2. It creates mailing-address divergence from residence addresses.

This is important because many real systems distinguish where a person lives from where mail should be sent. The mailing block exists to model that split without changing the residence truth itself.

### `fill_rates`

This section controls optional field completeness.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.fill_rates.middle_name` | Percent of people receiving a middle name. | Controls middle-name sparsity. |
| `phase1.fill_rates.suffix` | Percent of people receiving a suffix. | Controls suffix sparsity. |
| `phase1.fill_rates.phone` | Percent of people receiving a phone number. | Controls phone-field completeness. |

### `suffix_distribution`

This section controls which suffix values appear when the suffix field is populated.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.suffix_distribution.<suffix>` | Weight for each suffix token. | Controls the mix of suffix values such as `Jr`, `Sr`, `III`, `I`. |

### Story behind `fill_rates` and `suffix_distribution`

These are independent levers:

- `fill_rates.suffix` controls whether a suffix exists.
- `suffix_distribution` controls which suffix is chosen when it does exist.

That separation is useful because frequency and composition are not the same thing.

### `residence_dates`

This section controls the residence-history time window for Phase-1 rows.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.residence_dates.start_year_min` | Earliest year a residence interval can start. | Controls how far back residence history can reach. |
| `phase1.residence_dates.open_ended_pct` | Chance a residence interval has no end date. | Controls how many rows represent current or open residence periods. |
| `phase1.residence_dates.min_duration_days` | Minimum duration for closed intervals. | Prevents very short intervals when an end date is assigned. |

### Important note on current behavior

In the current implementation, residence intervals are sampled independently per row. That means this section controls the distribution of dates, but it does not yet guarantee a coherent person-level timeline across repeated rows. Readers should understand that this is a current implementation property, not an abstract YAML design promise.

### `quality`

This section controls post-generation validation and reporting thresholds.

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.quality.distribution_tolerance_pct` | Allowed deviation between expected and achieved percentage distributions. | Controls pass/fail status of quality distribution checks. |
| `phase1.quality.exact_uniqueness_check_max_rows` | Maximum row count at which the generator performs exact duplicate checks by re-reading the output. | Keeps validation affordable on large runs. |

### Phase-1 summary

If you only remember one thing about Phase-1 config, remember this:

- `distributions`, `age_bins`, and `fill_rates` shape who the people are.
- `name_duplication`, `redundancy`, and `nicknames` shape how hard the flat file is.
- `address` and `residence_dates` shape administrative history.
- `quality` decides how strictly the generator audits itself.

## Phase-2 Scenario YAML: `phase2/scenarios/*.yaml`

### Current shape

```yaml
scenario_id: single_movers
seed: 20260310
phase1:
  data_path: phase1/outputs_phase1/Phase1_people_addresses.csv
  manifest_path: phase1/outputs_phase1/Phase1_people_addresses.manifest.json
parameters: ...
simulation: ...
emission: ...
quality: ...
selection: ...
constraints: ...
```

### Story behind the Phase-2 scenario YAML

The Phase-2 YAML is the scenario contract. It answers six questions:

1. Which Phase-1 baseline should I start from?
2. Which people should enter this scenario?
3. What truth-layer life events happen to them?
4. How many observed systems do I want?
5. How noisy and redundant should those observed systems be?
6. What realism and quality rules should still hold?

### Top-level scenario keys

| Key | Meaning | What it changes |
|---|---|---|
| `scenario_id` | Scenario identifier. | Drives run naming, manifests, output folder names, and scenario-specific logic. |
| `seed` | Scenario RNG seed. | Controls deterministic selection, simulation, and emission. |
| `phase1` | Baseline input references. | Points Phase-2 to the Phase-1 CSV and manifest it should consume. |
| `parameters` | Truth-event rate and scenario-shape controls. | Drives how much simulated change happens. |
| `simulation` | Time axis of the truth simulation. | Controls when and how often the simulator steps forward. |
| `selection` | Subpopulation filter and sample controls. | Decides who enters the scenario. |
| `constraints` | Realism guardrails. | Prevents or flags implausible outcomes. |
| `emission` | Observed-dataset design. | Controls overlap, coverage, duplication, and noise. |
| `quality` | Post-run scenario quality thresholds. | Controls acceptable household size ranges and related checks. |

### `phase1`

| Key | Meaning | What it changes |
|---|---|---|
| `phase1.data_path` | Path to the Phase-1 CSV. | Supplies the baseline records used for selection and address reference. |
| `phase1.manifest_path` | Path to the Phase-1 manifest JSON. | Supplies provenance and contract consistency checks. |

### `parameters`

This section is the truth-dynamics control block. These values are annualized rates or scenario-specific structural parameters.

#### General event-rate parameters

| Key | Meaning | What it changes |
|---|---|---|
| `parameters.move_rate_pct` | Annual move rate as a percent. | Controls how often addresses change in truth. |
| `parameters.cohabit_rate_pct` | Annual cohabitation or household-formation rate. | Controls household merges such as couple formation. |
| `parameters.birth_rate_pct` | Annual birth-event rate as a percent. | Controls how often child entities are added. |
| `parameters.divorce_rate_pct` | Annual divorce or separation rate as a percent. | Controls household splits after partnership. |
| `parameters.leave_home_rate_pct` | Annual rate for children or young adults leaving home. | Controls `LEAVE_HOME` truth events. |
| `parameters.split_rate_pct` | Backward-compatible alias for `leave_home_rate_pct`. | Used by shipped roommate scenarios. |
| `parameters.use_priors_for_unspecified_rates` | If `true`, missing event rates inherit from `phase2_priors_snapshot.json`. | Lets the scenario rely on the statistical prior bundle instead of explicitly setting every rate. |

#### Scenario-specific roommate parameters

These currently matter only for `roommates_split`.

| Key | Meaning | What it changes |
|---|---|---|
| `parameters.enable_roommate_baseline_groups` | Enables pre-simulation roommate grouping. | Lets the scenario start with shared households already formed. |
| `parameters.roommate_group_share_pct` | Percent of eligible people placed into roommate groups. | Controls how common pre-simulation roommate households are. |
| `parameters.roommate_household_size_min` | Minimum roommate group size. | Controls lower bound of baseline roommate households. |
| `parameters.roommate_household_size_max` | Maximum roommate group size. | Controls upper bound of baseline roommate households. |
| `parameters.roommate_age_min` | Minimum eligible age for roommate grouping. | Restricts roommate grouping to a chosen age band. |
| `parameters.roommate_age_max` | Maximum eligible age for roommate grouping. | Restricts roommate grouping to a chosen age band. |

### Story behind `parameters`

This block changes the truth world, not the observed files.

- If you raise `move_rate_pct`, more people actually move.
- If you raise `name_typo_pct` later in `emission`, the same truth events happen, but the observed files become dirtier.

That distinction matters: `parameters` changes life events; `emission` changes what downstream systems record.

### `simulation`

| Key | Meaning | What it changes |
|---|---|---|
| `simulation.granularity` | Valid values: `monthly`, `daily`. | Changes how the simulator converts annual rates into step probabilities. |
| `simulation.start_date` | Start date of the simulation timeline. | Defines the origin of all generated truth events. |
| `simulation.periods` | Number of steps to simulate. | Controls total simulation horizon. |

### Story behind `simulation`

This block answers "How much time passes?" A 12-period monthly simulation is a one-year scenario. A 365-period daily simulation would expose the same annual rates through many smaller steps.

### `selection`

This section decides which Phase-1 people enter the scenario population before truth simulation starts.

#### `selection.sample`

| Key | Meaning | What it changes |
|---|---|---|
| `selection.sample.mode` | Valid values: `all`, `count`, `pct`. | Controls how the final selection size is interpreted. |
| `selection.sample.value` | Count or percent depending on `mode`. | Controls selected population size. |

#### `selection.filters`

| Key | Meaning | What it changes |
|---|---|---|
| `selection.filters.age_bins` | Allowed `AgeBin` values. | Restricts scenario to chosen age groups. |
| `selection.filters.genders` | Allowed gender values. | Restricts scenario by gender. |
| `selection.filters.ethnicities` | Allowed ethnicity values. | Restricts scenario by ethnicity. |
| `selection.filters.residence_types` | Allowed current residence types. | Restricts scenario to houses, apartments, and so on. |
| `selection.filters.redundancy_profiles` | Allowed redundancy buckets. Valid values: `single_record`, `multi_record`. | Restricts scenario by whether people had one or multiple Phase-1 rows. |
| `selection.filters.mobility_propensity_buckets` | Allowed mobility buckets. Valid values: `low`, `medium`, `high`. | Restricts scenario to people with low, medium, or high latent mobility propensity. |

#### `selection.thresholds`

| Key | Meaning | What it changes |
|---|---|---|
| `selection.thresholds.mobility_low_max` | Upper bound for the `low` mobility bucket. | Changes how mobility scores are bucketed. |
| `selection.thresholds.mobility_high_min` | Lower bound for the `high` mobility bucket. | Changes how mobility scores are bucketed. |
| `selection.thresholds.trait_low_max` | Upper bound for `low` partnership and fertility buckets. | Changes trait bucketing. |
| `selection.thresholds.trait_high_min` | Lower bound for `high` partnership and fertility buckets. | Changes trait bucketing. |

### Story behind `selection`

Phase-2 does not select directly from raw Phase-1 rows. It first builds an entity view:

- representative row for demographic identity
- latest row for residence type
- records-per-entity summary

It then adds latent scores:

- `MobilityPropensityScore`
- `PartnershipPropensityScore`
- `FertilityPropensityScore`

Those scores are deterministic given the seed and are influenced by age and mobility priors. The threshold fields control how those continuous scores become `low`, `medium`, and `high` buckets.

### `constraints`

This section defines realism rules and validation rules for the truth layer.

| Key | Meaning | What it changes |
|---|---|---|
| `constraints.min_marriage_age` | Minimum allowed age for cohabitation or marriage-like events when underage behavior is disallowed. | Controls age validity for partnership events. |
| `constraints.max_partner_age_gap` | Hard cap on partner age difference. | Rejects or flags very large age gaps unless relaxed. |
| `constraints.partner_age_gap_distribution` | Optional weighted gap distribution. | Alternative to a simple hard cap; its maximum key becomes the effective max gap. |
| `constraints.fertility_age_range.min` | Minimum age allowed for birth parent validation. | Controls birth realism checks. |
| `constraints.fertility_age_range.max` | Maximum age allowed for birth parent validation. | Controls birth realism checks. |
| `constraints.allow_underage_marriage` | Allow underage cohabitation or marriage events. | If `false`, such events are flagged as violations. |
| `constraints.allow_child_lives_alone` | Allow minors to leave home and live alone. | If `false`, underage `LEAVE_HOME` cases are flagged. |
| `constraints.enforce_non_overlapping_residence_intervals` | Require sequential non-overlapping residence history per person. | Turns on time-overlap validation for truth residence histories. |

### Story behind `constraints`

This block is the realism governor. It does not choose who gets simulated. It decides what kinds of truth histories are acceptable once the simulator has run.

### `emission`

This section is the observed-data design layer. It controls how truth becomes datasets.

There are two supported schemas:

1. legacy pairwise A/B schema
2. general `datasets:` schema for single-dataset or multi-dataset runs

#### Common emission control

| Key | Meaning | What it changes |
|---|---|---|
| `emission.crossfile_match_mode` | Valid values: `single_dataset`, `one_to_one`, `one_to_many`, `many_to_one`, `many_to_many`. | Defines the intended relationship pattern between observed files. |
| `emission.overlap_entity_pct` | Target percent of entities intended to overlap across datasets. | Controls how much shared entity population exists between sources. |

#### Legacy pairwise A/B fields

| Key | Meaning | What it changes |
|---|---|---|
| `emission.appearance_A_pct` | Share of eligible entities that appear in dataset A. | Controls source coverage for A. |
| `emission.appearance_B_pct` | Share of eligible entities that appear in dataset B. | Controls source coverage for B. |
| `emission.duplication_in_A_pct` | Extra duplicate record rate within A. | Controls within-file duplication on A. |
| `emission.duplication_in_B_pct` | Extra duplicate record rate within B. | Controls within-file duplication on B. |
| `emission.noise.A` | Noise profile for A. | Controls corruption level in A. |
| `emission.noise.B` | Noise profile for B. | Controls corruption level in B. |

#### General `datasets:` schema

| Key | Meaning | What it changes |
|---|---|---|
| `emission.datasets[*].dataset_id` | Unique dataset identifier. | Drives file naming, manifest entries, and pairwise crosswalk labels. |
| `emission.datasets[*].filename` | Output CSV name. | Controls artifact filename. |
| `emission.datasets[*].snapshot` | Valid values: `simulation_start`, `simulation_end`. | Controls whether the dataset is emitted from the beginning or end truth state. |
| `emission.datasets[*].appearance_pct` | Coverage rate for that dataset. | Controls how many eligible entities appear in the dataset. |
| `emission.datasets[*].duplication_pct` | Within-dataset duplication rate. | Controls extra repeated rows in that dataset. |
| `emission.datasets[*].noise` | Noise profile for that dataset. | Controls field corruption in that dataset. |

### Story behind `overlap`, `appearance`, and `duplication`

These are easy to mix up.

- `overlap_entity_pct` is about shared entity presence across datasets.
- `appearance_pct` is about how complete one dataset is by itself.
- `duplication_pct` is about repeated observed rows inside that dataset.

So one dataset can have high coverage and still be hard because it is noisy or duplicated. Another can be very clean but sparse. These knobs let you separate those effects.

### Emission noise glossary

Each dataset noise profile supports the following fields:

| Key | Meaning | What it changes |
|---|---|---|
| `name_typo_pct` | Simple name typo rate. | Introduces direct character-level name corruption. |
| `dob_shift_pct` | DOB shift rate. | Perturbs dates of birth. |
| `ssn_mask_pct` | SSN masking rate. | Removes or masks SSN content. |
| `phone_mask_pct` | Phone masking rate. | Removes or masks phone content. |
| `address_missing_pct` | Address missingness rate. | Blanks address fields. |
| `middle_name_missing_pct` | Middle-name missingness rate. | Removes middle names. |
| `phonetic_error_pct` | Phonetic substitution rate. | Creates sound-alike variants such as `PH` vs `F`. |
| `ocr_error_pct` | OCR confusion rate. | Introduces scan-like substitutions such as `O` vs `0` or `l` vs `1`. |
| `date_swap_pct` | Date transposition rate. | Swaps date components in DOB formatting. |
| `zip_digit_error_pct` | ZIP corruption rate. | Introduces one-digit ZIP errors. |
| `nickname_pct` | Nickname substitution rate. | Replaces formal first names with nicknames during emission. |
| `suffix_missing_pct` | Suffix-drop rate. | Removes `JR`, `SR`, `III`, and similar suffixes. |

### Story behind `emission`

This is where the benchmark becomes useful for ER:

- truth may be stable
- coverage may be incomplete
- duplication may be source-specific
- corruption may differ sharply by system

That separation is the point of the Phase-2 architecture. The simulator changes life events. The emitter changes what each system reveals or loses.

### `quality`

| Key | Meaning | What it changes |
|---|---|---|
| `quality.household_size_range.min` | Minimum acceptable household size. | Used by Phase-2 quality checks. |
| `quality.household_size_range.max` | Maximum acceptable household size. | Used by Phase-2 quality checks. |

### Story behind `quality`

Phase-2 quality is not just about formatting. It also checks whether the scenario still looks operationally sane after simulation and emission. Household size is a simple but useful guardrail against runaway scenario behavior.

## Phase-2 parameter package: `Data/phase2_params/`

### What lives there

The current parameter bundle requires these files:

- `mobility_overall_acs_2024.csv`
- `mobility_by_age_cohort_acs_2024.csv`
- `marriage_divorce_rates_cdc_2023.csv`
- `fertility_by_age_nchs_2024.csv`
- `household_type_shares_acs_2024.csv`
- `phase2_priors_snapshot.json`
- `sources.json`
- `manifest.json`

### What each file means

| File | Meaning | Current runtime use |
|---|---|---|
| `mobility_overall_acs_2024.csv` | Overall moved-in-past-year rate. | Loaded and validated; summarized into the priors snapshot. |
| `mobility_by_age_cohort_acs_2024.csv` | Mobility rates by age cohort. | Used directly by the selection engine to build mobility propensity scores. |
| `marriage_divorce_rates_cdc_2023.csv` | National marriage and divorce rates. | Loaded and validated; summarized into the priors snapshot. |
| `fertility_by_age_nchs_2024.csv` | Age-specific fertility rates. | Loaded and validated; summarized into the priors snapshot. |
| `household_type_shares_acs_2024.csv` | Household type shares. | Loaded and validated for bundle completeness and provenance; not currently a direct simulator input. |
| `phase2_priors_snapshot.json` | Compact runtime summary of the prior layer. | Used by the simulator when `use_priors_for_unspecified_rates=true`. |
| `sources.json` | Citation and provenance metadata. | Documents where the priors came from. |
| `manifest.json` | Bundle manifest. | Confirms what parameter files were shipped together. |

### `phase2_priors_snapshot.json`

The shipped prior snapshot currently contains:

- `mobility.overall_moved_past_year_pct`
- `mobility.age_cohort_moved_pct`
- `marriage_divorce.marriage_rate_per_1000`
- `marriage_divorce.divorce_rate_per_1000`
- `fertility.birth_rate_per_1000_by_age_group`
- `household_type_share.share_pct_by_type`

### Story behind the priors package

The raw CSVs are the sourced parameter tables. The JSON snapshot is the runtime-friendly summary. That is why the code loads the full bundle but only passes the compact priors snapshot into the simulator.

In practice:

- `selection.py` uses `mobility_by_age_cohort` directly.
- `simulator.py` uses `phase2_priors_snapshot.json` when it needs default annual rates.
- the other files make the parameter layer inspectable, reproducible, and refreshable.

## Runtime vs metadata vs provenance

Some keys and files are direct operational controls. Some are mainly descriptive.

### Direct runtime controls

- most Phase-1 numeric knobs
- Phase-2 `parameters`
- Phase-2 `simulation`
- Phase-2 `selection`
- Phase-2 `constraints`
- Phase-2 `emission`
- Phase-2 `quality`
- `mobility_by_age_cohort_acs_2024.csv`
- `phase2_priors_snapshot.json`

### Metadata or provenance first

- `phase1.age_bins.pct_interpretation`
- `phase2/scenarios/catalog.yaml`
- `Data/phase2_params/sources.json`
- `Data/phase2_params/manifest.json`
- raw Phase-2 CSV parameter tables that are currently summarized into the priors snapshot instead of being consumed directly by the simulator

## Practical reading order

If you are trying to understand the system quickly, read the config surfaces in this order:

1. `phase1/configs/phase1.yaml`
2. one Phase-2 scenario YAML such as `phase2/scenarios/single_movers.yaml`
3. `Data/phase2_params/phase2_priors_snapshot.json`
4. `src/sog_phase2/selection.py`
5. `src/sog_phase2/simulator.py`
6. `src/sog_phase2/emission.py`

That order mirrors the actual runtime flow:

baseline generation -> scenario selection -> truth simulation -> observed emission -> quality validation

## Short version

If you want the shortest possible interpretation of the knobs:

- Phase-1 controls who exists and how messy the baseline flat file is.
- Phase-2 `selection` controls who enters the scenario.
- Phase-2 `parameters` controls what happens to them in truth.
- Phase-2 `emission` controls what each dataset reveals, misses, duplicates, or corrupts.
- Phase-2 `constraints` controls what counts as unrealistic.
- `Data/phase2_params` explains where the rates came from and supplies the prior layer.
