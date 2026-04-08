# Phase-2 Step-1 Output Contract (As of March 5, 2026)

## Purpose
Define required Phase-2 outputs before scenario logic expands. This keeps all runs comparable and prevents schema drift.

This Step-1 contract builds on Step-0:
- `docs/PHASE2_STEP0_CONTRACT.md`

Packaging/layout for these outputs is defined in Step-2:
- `docs/PHASE2_STEP2_PACKAGING_MODEL.md`

Event vocabulary/schema details are defined in Step-4:
- `docs/PHASE2_STEP4_EVENT_GRAMMAR.md`

## Scenario Output Layout
Each Phase-2 scenario run writes to:
- `phase2/runs/<run_id>/`

Inside that directory, every scenario must emit the same two layers.

## 1A) Truth Layer (normalized simulation truth)
Required files:
- `truth_people.parquet`
- `truth_households.parquet`
- `truth_household_memberships.parquet`
- `truth_residence_history.parquet`
- `truth_events.parquet`
- `scenario_population.parquet`

Required minimum columns:
- `truth_people.parquet`:
  - `PersonKey`, `FormalFirstName`, `MiddleName`, `LastName`, `Suffix`, `FormalFullName`
  - `Gender`, `Ethnicity`, `DOB`, `Age`, `AgeBin`, `SSN`
- `truth_households.parquet`:
  - `HouseholdKey`, `HouseholdType`, `HouseholdStartDate`, `HouseholdEndDate`
- `truth_household_memberships.parquet`:
  - `PersonKey`, `HouseholdKey`, `HouseholdRole`, `MembershipStartDate`, `MembershipEndDate`
- `truth_residence_history.parquet`:
  - `PersonKey`, `AddressKey`, `ResidenceStartDate`, `ResidenceEndDate`
- `truth_events.parquet`:
  - `EventKey`, `EventType`, `EventDate`
  - `SubjectPersonKey`, `SubjectHouseholdKey`
  - `FromAddressKey`, `ToAddressKey`
  - `PersonKeyA`, `PersonKeyB`, `NewHouseholdKey`, `CohabitMode`
  - `ChildPersonKey`, `Parent1PersonKey`, `Parent2PersonKey`
  - `CustodyMode`
- `scenario_population.parquet`:
  - `PersonKey`, `ScenarioId`, `SelectionSeed`
  - `AgeBin`, `Gender`, `Ethnicity`, `ResidenceType`
  - `RecordsPerEntity`, `RedundancyProfile`
  - `MobilityPropensityScore`, `MobilityPropensityBucket`
  - `PartnershipPropensityScore`, `PartnershipPropensityBucket`
  - `FertilityPropensityScore`, `FertilityPropensityBucket`

`EventType` minimum set: `MOVE`, `COHABIT`, `BIRTH`, `DIVORCE`, `LEAVE_HOME`.

## 1B) Observed Layer (what ER sees)
Required files:
- `DatasetA.csv`
- `DatasetB.csv`
- `truth_crosswalk.csv`

Required minimum columns:
- `DatasetA.csv`: `A_RecordKey`
- `DatasetB.csv`: `B_RecordKey`
- `truth_crosswalk.csv`:
  - `A_RecordKey`, `B_RecordKey`
  - at least one identity key column from: `PersonKey` or `EntityKey`

## Machine-Enforced Contract
Contract definitions live in:
- `src/sog_phase2/output_contract.py`

Validator CLI:
```bash
python scripts/validate_phase2_outputs.py --run-id <run_id>
```

Run id format:
- `YYYY-MM-DD_<scenario_id>_seed<seed>`

Default runs root:
- `phase2/runs/`

Validation fails if:
- any required file is missing;
- required columns are missing;
- crosswalk has neither `PersonKey` nor `EntityKey`.
- Step-5 constraints report violations when active.
- selection metadata (`scenario_selection_log.json`) is invalid or inconsistent with `scenario_population.parquet`.
