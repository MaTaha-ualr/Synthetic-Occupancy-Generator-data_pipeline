# Phase-2 Step-10 Regression Tests (As of March 5, 2026)

## Goal
Turn scenario definitions into repeatable regression tests so Phase-2 remains stable as new scenario packs are added.

## Implemented Scenario Regression Assertions
- `single_movers`: produces at least one `MOVE` event.
- `couple_merge`: produces `COHABIT` events and paired people share residence at event date.
- `family_birth`: produces at least one `BIRTH` event and child keys exist in `truth_people`.
- `divorce_custody`: produces at least one `DIVORCE` event and creates `post_divorce` households.
- `roommates_split` (`one_to_many` emission mode): yields at least one entity with multiple B records.
- Crosswalk overlap check: observed overlap equals overlap entities claimed by emission settings for the scenario.

## Enforced In Code
- Regression test module:
  - `tests/test_phase2_scenario_regression.py`

## Run Command
```bash
python -m pytest -q tests/test_phase2_scenario_regression.py
```
