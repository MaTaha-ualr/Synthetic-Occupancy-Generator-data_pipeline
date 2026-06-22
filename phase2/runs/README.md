# Anatomy of a Phase-2 Run Folder

Every benchmark run lands in `phase2/runs/<run_id>/`, where the run id is
`YYYY-MM-DD_<scenario_id>_seed<seed>` (e.g. `2026-03-10_single_movers_seed20260310`).

This file is the **column-by-column reference** for everything inside a run folder.
Each run folder also contains its own auto-generated `README.md` with the numbers for
*that* run. The folder is generated output and is intentionally **not** committed to git.

> **Mental model.** Phase-2 takes the clean Phase-1 population, simulates it forward
> through time to produce a fully-known **truth layer**, then emits deliberately messy
> **observed datasets** for an entity-resolution (ER) / record-linkage system to chew on.
> Because the truth is known, every run ships its own answer key.

---

## The two layers

| Layer | Purpose | Format |
| --- | --- | --- |
| **Truth layer** | Ground truth — what *really* happened. You grade against this. | Parquet |
| **Observed layer** | Noisy, duplicated, incomplete records — what an ER system actually receives. | CSV |

Plus **answer keys** that connect the two, and **metadata** files describing the run.

---

## Answer keys — read this first

There are two answer keys and they answer different questions.

### `entity_record_map.csv` — the canonical truth (use this)
Maps every observed record to the real person it came from. Two records are the same
real-world entity **iff they share a `PersonKey`**. This is transitively complete and
spans *all* datasets in the run (A, B, C, …).

| Column | Meaning |
| --- | --- |
| `PersonKey` | The true person identity. |
| `DatasetId` | Which observed dataset the record is in (`A`, `B`, `registry`, …). |
| `RecordKey` | The record's id within that dataset. |

**To build the gold-standard clusters:** group `RecordKey` by `PersonKey`.

### `truth_crosswalk.csv` — pairwise positional view (two-dataset runs only)
A backward-compatible A↔B link list.

| Column | Meaning |
| --- | --- |
| `PersonKey` | The true person. |
| `A_RecordKey` | A record from dataset A (or blank). |
| `B_RecordKey` | A record from dataset B (or blank). |

> ⚠️ **Important nuance.** The crosswalk pairs records *positionally* according to the
> scenario's `crossfile_match_mode`. When an entity has **within-file duplicates**, some
> of its records appear in a row with the opposite side left blank (e.g. a person with two
> A-records and one B-record yields one `(A1, B1)` row and one `(A2, "")` row). That blank
> does **not** mean A2 has no match in B — A2 is the same person as B1. For
> transitive/clustering evaluation, always prefer `entity_record_map.csv`.
>
> For runs with **3+ datasets**, there is no single `truth_crosswalk.csv`; instead you get
> one `truth_crosswalk__<left>__<right>.csv` per dataset pair, each with the same three
> columns.

---

## Observed datasets (CSV)

Filenames come from the scenario (`DatasetA.csv` / `DatasetB.csv` for the legacy A/B
schema, or `observed_<id>.csv` / a custom `filename` otherwise). Every observed dataset
has the same columns:

| Column | Meaning |
| --- | --- |
| `RecordKey` | Unique id of this row within the dataset (e.g. `A-000000001`). |
| `DatasetId` | The dataset this row belongs to. |
| `FirstName`, `MiddleName`, `LastName`, `Suffix` | Name parts — **possibly corrupted by noise** (typo, OCR, phonetic, nickname, dropped suffix/middle name). |
| `FullName` | Convenience concatenation of the (possibly-corrupted) name parts. |
| `Gender`, `Ethnicity` | Demographics (copied from truth, not noised). |
| `DOB` | Date of birth — possibly shifted ±3 days or month/day-swapped by noise. |
| `Age` | Age **as of this dataset's snapshot date** (recomputed from DOB, not a static copy). |
| `SSN` | May be masked to `***-**-1234`. |
| `Phone` | May be blanked. |
| `AddressKey` | Truth address id at the snapshot (blank if the address was dropped by noise). |
| `HouseNumber`, `StreetName`, `UnitType`, `UnitNumber` | Structured address parts. |
| `StreetAddress` | Composed street line. |
| `City`, `State`, `ZipCode` | Locality (ZIP may have a single-digit error). |
| `SourceSnapshotDate` | Which simulation snapshot this row was taken from (`simulation_start` or `simulation_end` date). |
| `SourceSystem` | Same as `DatasetId`; labels the originating "system". |

Why two records of the same person differ across A and B: different **snapshot dates**
(A is usually the start, B the end, so movers have different addresses and everyone is
older in B) and **independent noise** applied per dataset.

### `masterDataset.csv`

Every run also includes `masterDataset.csv`, a convenience table with two row types:
`RecordType=source_snapshot` stacks all observed dataset rows with one additional leading
`PersonKey` column, while `RecordType=residence_timeline` adds `DatasetId=TIMELINE` rows
from `truth_residence_history.parquet`.

Exact duplicate source payloads from the same dataset and same `PersonKey` are collapsed,
so the source-snapshot portion can be smaller than the sum of the source CSV row counts.
The timeline portion includes `ResidenceStartDate` and `ResidenceEndDate`, so simulated
intermediate address changes appear directly in `masterDataset.csv`.

---

## Truth layer (Parquet)

### `truth_people.parquet` — one row per real person
| Column | Meaning |
| --- | --- |
| `PersonKey` | Stable true identity. People who join via `BIRTH` get keys like `P_CHILD_0000001`. |
| `FormalFirstName`, `MiddleName`, `LastName`, `Suffix`, `FormalFullName` | Clean, canonical name. |
| `Gender`, `Ethnicity` | Demographics. |
| `DOB`, `Age`, `AgeBin` | Birth date, age at simulation start, and age bucket (`age_0_17`, `age_18_34`, `age_35_64`, `age_65_plus`). |
| `SSN`, `Phone` | Clean identifiers (the observed layer is what corrupts these). |

### `truth_households.parquet`
| Column | Meaning |
| --- | --- |
| `HouseholdKey` | Household id (`HH_BASE_*` initial solo, `HH_ROOM_BASE_*` roommates, `HH_SIM_*` formed during simulation). |
| `HouseholdType` | `solo`, `solo_<residence_type>`, `roommates`, `couple`, `post_divorce`, `solo` (post leave-home). |
| `HouseholdStartDate`, `HouseholdEndDate` | Lifespan; end is blank while the household is still active. |

### `truth_household_memberships.parquet`
| Column | Meaning |
| --- | --- |
| `PersonKey`, `HouseholdKey` | Who was in which household. |
| `HouseholdRole` | `HEAD`, `SPOUSE`, `ROOMMATE`, `CHILD`. |
| `MembershipStartDate`, `MembershipEndDate` | The interval of membership (non-overlapping per person; blank end = current). |

### `truth_residence_history.parquet`
| Column | Meaning |
| --- | --- |
| `PersonKey`, `AddressKey` | Who lived where. |
| `ResidenceStartDate`, `ResidenceEndDate` | The address interval (non-overlapping per person; blank end = current). |

### `truth_events.parquet` — the simulation's event log
One row per life event. Columns are a union of every event type's fields; only the
relevant ones are populated per row.

| Column | Used by | Meaning |
| --- | --- | --- |
| `EventKey` | all | Unique event id (`EVT_0000001`). |
| `EventType` | all | `MOVE`, `COHABIT`, `BIRTH`, `DIVORCE`, `LEAVE_HOME`. |
| `EventDate` | all | When it happened. |
| `SubjectPersonKey` | MOVE (solo), LEAVE_HOME | The person who moved/left. |
| `SubjectHouseholdKey` | MOVE (household) | The household that moved together. |
| `FromAddressKey`, `ToAddressKey` | MOVE, LEAVE_HOME | Address change. |
| `PersonKeyA`, `PersonKeyB` | COHABIT, DIVORCE | The two people in the relationship. |
| `NewHouseholdKey` | COHABIT | Household the couple formed. |
| `CohabitMode` | COHABIT | `move_to_A`, `move_to_B`, `new_address`. |
| `ChildPersonKey` | BIRTH, LEAVE_HOME | The child born / the young adult leaving. |
| `Parent1PersonKey`, `Parent2PersonKey` | BIRTH | Parents (Parent2 may be blank). |
| `CustodyMode` | DIVORCE | `joint`, `parent_a_primary`, `parent_b_primary`, `split`. |

### `scenario_population.parquet` — who was selected, and their behavioral dials
| Column | Meaning |
| --- | --- |
| `PersonKey` | Selected participant. |
| `ScenarioId`, `SelectionSeed` | Provenance. |
| `AgeBin`, `Gender`, `Ethnicity`, `ResidenceType` | Demographics used for filtering. |
| `RecordsPerEntity`, `RedundancyProfile` | How many Phase-1 records this person had (`single_record` / `multi_record`). |
| `MobilityPropensityScore` + `…Bucket` | Likelihood-to-move dial (0–1) and its `low`/`medium`/`high` bucket. |
| `PartnershipPropensityScore` + `…Bucket` | Likelihood-to-couple/divorce dial. |
| `FertilityPropensityScore` + `…Bucket` | Likelihood-to-have-a-child dial. |

These scores are deterministic functions of `(PersonKey, seed)` blended with age-based
priors, which is what makes the whole run reproducible.

---

## Metadata files

| File | Contents |
| --- | --- |
| `scenario.yaml` | The **fully resolved** scenario config the run was generated from (defaults filled in). Re-running this YAML with the same seed reproduces the run byte-for-byte. |
| `scenario_selection_log.json` | Audit trail of selection: per-filter survivor counts, the seed, a SHA-256 checksum of the selected `PersonKey`s, and a preview list. |
| `manifest.json` | Machine-readable inventory: input paths, output paths, `simulation_meta` (per-step event probabilities, annual-rate inputs), and `emission_meta` (match mode, dataset ids, coverage). |
| `quality_report.json` | Two blocks: **truth-consistency** checks (no overlapping intervals, event ages legal, household sizes within bounds) and **ER-benchmark metrics** (within-file duplicate rates, cross-file overlap, achieved match cardinality, attribute-drift rates). Plus a top-level `status` of `ok` / `quality_issues_detected`. |
| `README.md` | The per-run human summary (auto-generated). |

---

## The 12 observed-noise types

Configured per dataset under `emission.noise.<dataset>` (or `datasets[*].noise`). The
first six are on by default; the last six default to `0.0`.

| Noise (`*_pct`) | Effect |
| --- | --- |
| `name_typo` | Replace one character in first or last name. |
| `dob_shift` | Shift DOB by −3…+3 days. |
| `ssn_mask` | Mask SSN to `***-**-1234`. |
| `phone_mask` | Blank the phone. |
| `address_missing` | Drop the whole address (key + parts). |
| `middle_name_missing` | Drop the middle name. |
| `phonetic_error` | Swap a phonetic cluster (`ph`↔`f`, `ck`↔`k`, …). |
| `ocr_error` | OCR confusion (`O`↔`0`, `l`↔`1`↔`I`, `rn`↔`m`, …). |
| `date_swap` | Transpose DOB month and day when valid. |
| `zip_digit_error` | Nudge one ZIP digit by ±1. |
| `nickname` | Replace a formal first name with a nickname (from `phase1/prepared/nicknames.json`). |
| `suffix_missing` | Drop the Jr./Sr./III suffix. |

---

## Reproducing / regenerating

```powershell
# One command, full pipeline (selection → truth → observed → quality → validate → README):
python phase2/scripts/run_phase2_pipeline.py --scenario single_movers --overwrite

# Refresh just the per-run README(s) without regenerating data:
python phase2/scripts/write_run_readmes.py
```

For the meaning of every **input** knob (selection / constraints / simulation / emission /
quality), see [`phase2/docs/PARAMETER_REFERENCE.md`](../docs/PARAMETER_REFERENCE.md).
For the scenario menu, see [`phase2/scenarios/README.md`](../scenarios/README.md) and
[`phase2/scenarios/catalog.yaml`](../scenarios/catalog.yaml).
