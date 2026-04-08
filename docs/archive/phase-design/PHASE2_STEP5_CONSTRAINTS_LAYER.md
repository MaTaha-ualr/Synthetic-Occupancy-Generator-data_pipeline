# Phase-2 Step-5 Eligibility + Constraints Layer (As of March 5, 2026)

## Goal
Prevent nonsensical synthetic outcomes by enforcing toggleable eligibility/consistency rules.

This layer covers:
- underage marriage/cohabitation pairing
- unrealistic partner age gaps
- births outside configured fertility ages
- children living alone when disallowed
- overlapping residence intervals for the same person

## Config Toggles
Scenario YAML supports:
- `min_marriage_age`
- `max_partner_age_gap`
- `partner_age_gap_distribution` (optional alternative to scalar max; max key becomes effective cap)
- `fertility_age_range.min`
- `fertility_age_range.max`
- `allow_underage_marriage` (default `false`)
- `allow_child_lives_alone` (default `false`)
- `enforce_non_overlapping_residence_intervals` (default `true`)

## Novelty Lever Behavior
Constraints are strict by default, but unrealistic edge cases can be enabled intentionally:
- set `allow_underage_marriage: true` for stress tests
- set `allow_child_lives_alone: true` for stress tests
- widen `fertility_age_range` or partner-gap settings as needed

## Enforced In Code
- Constraint config + validation logic:
  - `src/sog_phase2/constraints.py`
- Integrated into run validator:
  - `src/sog_phase2/output_contract.py`
- Scenario defaults:
  - `phase2/scenarios/*.yaml`

Run command:
```bash
python scripts/validate_phase2_outputs.py --run-id <run_id>
```

If violations are present, run validation fails and returns `constraints_validation` details.

## Next Step
Step-6 scenario selection engine is in:
- `docs/PHASE2_STEP6_SELECTION_ENGINE.md`
