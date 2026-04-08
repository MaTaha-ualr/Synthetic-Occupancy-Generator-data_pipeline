# Phase-2 Step-0 Contract Freeze (As of March 5, 2026)

## Purpose
This document freezes the Phase-1 baseline consumed by Phase-2. Treat it as an API contract between phases.

## Frozen Phase-1 Artifacts (Read-Only Inputs)
Phase-2 consumes these exact artifacts from Phase-1:
- `outputs_phase1/Phase1_people_addresses.csv`
- `outputs_phase1/Phase1_people_addresses.manifest.json`
- `outputs_phase1/Phase1_people_addresses.quality_report.json`

Notes:
- The generic names `manifest.json` and `quality_report.json` map to the concrete file names above.
- Phase-2 code can read these files, but must not modify them in place.
- Legacy location `outputs/Phase1_people_addresses.*` is acceptable while migrating; use one canonical location only.

## Phase-1 Semantics (Canonical Meanings)
- `PersonKey`: entity identity key (truth person).
- `RecordKey`: record-instance key (one representation row); unique per row.
- `EntityRecordIndex`: 1-based record ordinal within a `PersonKey`.
- `Redundancy`: multiple rows sharing one `PersonKey`, controlled by `redundancy.*`.

Interpretation detail:
- Entity-truth attributes are the formal identity columns (for example `FormalFirstName`, `LastName`, `DOB`, `SSN`).
- Representation attributes can vary across records (for example `FirstName` / `FullName` when nicknames are enabled).

## Canonical Input View for Phase-2
Decision: **Phase-2 simulation uses Entity view** (one row per `PersonKey`).

Reason:
- Phase-2 should model truth entities first, then emit record-level redundancy/noise later as a separate step.

## Deterministic Entity-View Derivation
Build Entity view from `outputs_phase1/Phase1_people_addresses.csv` using these rules:
1. Group rows by `PersonKey`.
2. Select a representative row per entity by lowest `EntityRecordIndex` (tie-breaker: lowest `RecordKey`).
3. Keep at least these entity-level fields:
   - `PersonKey`
   - `FormalFirstName`, `MiddleName`, `LastName`, `Suffix`, `FormalFullName`
   - `Gender`, `Ethnicity`
   - `DOB`, `Age`, `AgeBin`
   - `SSN`
4. Add `RecordsPerEntity` = count of source rows per `PersonKey`.
5. Optionally keep `RepresentativeRecordKey` for traceability.

Record view (`RecordKey` granularity) remains available for downstream emission and perturbation steps.

## Change-Control Guardrails
Any of the following requires explicit contract versioning:
- renaming/removing `PersonKey`, `RecordKey`, or `EntityRecordIndex`;
- changing `RecordKey` uniqueness semantics;
- changing one-to-many `PersonKey -> records` behavior;
- changing which Phase-1 artifacts are consumed by Phase-2.

## Step-0 Acceptance Checklist
- [x] Phase-1 baseline artifacts frozen as immutable Phase-2 inputs.
- [x] `PersonKey` / `RecordKey` / redundancy semantics documented.
- [x] Canonical Phase-2 input view selected as Entity view.
- [x] Deterministic Entity-view derivation rules defined.

## Next Step
Step-1 output definitions are in:
- `docs/PHASE2_STEP1_OUTPUT_CONTRACT.md`

Step-2 packaging definitions are in:
- `docs/PHASE2_STEP2_PACKAGING_MODEL.md`
