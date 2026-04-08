# Phase-2 Step-4 Event Grammar (As of March 5, 2026)

## Goal
Define the simulator vocabulary before implementation logic expands.

This grammar defines valid `truth_events.parquet` event types and required fields per event.

## Minimum Event Set
- `MOVE`
- `COHABIT`
- `BIRTH`
- `DIVORCE`
- `LEAVE_HOME`

Optional later (reserved, not active in Step-4 validator):
- `DEATH`
- `NAME_CHANGE`
- `ADOPTION`

## truth_events Schema
Required columns:
- `EventKey`
- `EventType`
- `EventDate`
- `SubjectPersonKey`
- `SubjectHouseholdKey`
- `FromAddressKey`
- `ToAddressKey`
- `PersonKeyA`
- `PersonKeyB`
- `NewHouseholdKey`
- `CohabitMode`
- `ChildPersonKey`
- `Parent1PersonKey`
- `Parent2PersonKey`
- `CustodyMode`

## Event Signatures
### MOVE
Signature:
- `MOVE(PersonKey or HouseholdKey, from AddressKey, to AddressKey, date)`

Row requirements:
- `EventType = MOVE`
- at least one of: `SubjectPersonKey` or `SubjectHouseholdKey`
- `FromAddressKey`, `ToAddressKey`
- `FromAddressKey != ToAddressKey`

### COHABIT
Signature:
- `COHABIT(PersonKey_A, PersonKey_B, new HouseholdKey, date, mode)`

Row requirements:
- `EventType = COHABIT`
- `PersonKeyA`, `PersonKeyB`, `NewHouseholdKey`, `CohabitMode`
- `PersonKeyA != PersonKeyB`

Allowed `CohabitMode`:
- `move_to_A`
- `move_to_B`
- `new_address`

### BIRTH
Signature:
- `BIRTH(parent1, parent2 optional, child PersonKey, date)`

Row requirements:
- `EventType = BIRTH`
- `Parent1PersonKey`, `ChildPersonKey`
- optional: `Parent2PersonKey`

### DIVORCE
Signature:
- `DIVORCE(PersonKey_A, PersonKey_B, date, custody_mode)`

Row requirements:
- `EventType = DIVORCE`
- `PersonKeyA`, `PersonKeyB`, `CustodyMode`
- `PersonKeyA != PersonKeyB`

Allowed `CustodyMode`:
- `joint`
- `parent_a_primary`
- `parent_b_primary`
- `split`

### LEAVE_HOME
Signature:
- `LEAVE_HOME(child PersonKey, date)`

Row requirements:
- `EventType = LEAVE_HOME`
- `ChildPersonKey`

## Enforced In Code
- Grammar spec + validator:
  - `src/sog_phase2/event_grammar.py`
- Run-level contract integration:
  - `src/sog_phase2/output_contract.py`

Validator command:
```bash
python scripts/validate_phase2_outputs.py --run-id <run_id>
```

## Next Step
Step-5 constraints layer is in:
- `docs/PHASE2_STEP5_CONSTRAINTS_LAYER.md`
