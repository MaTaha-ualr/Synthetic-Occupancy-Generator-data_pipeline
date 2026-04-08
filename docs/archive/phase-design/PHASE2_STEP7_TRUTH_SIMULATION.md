# Phase-2 Step-7 Truth-Layer Simulation (As of March 5, 2026)

## Goal
Generate Phase-2 truth tables with an event-driven stochastic simulator over explicit time steps.

## Calendar Model
- Supports `monthly` and `daily` granularity.
- Default is monthly.
- Configurable in scenario YAML:
  - `simulation.granularity`
  - `simulation.start_date`
  - `simulation.periods`

## Simulation Strategy
Per run:
1. Initialize baseline from selected `PersonKey` values in `scenario_population.parquet`.
2. Create baseline solo households (one household per selected person).
3. Iterate time steps and propose/commit events:
   - `MOVE`
   - `COHABIT`
   - `BIRTH`
   - `DIVORCE`
   - `LEAVE_HOME`
4. Apply constraints from `scenario.constraints`.
5. Write truth outputs:
   - `truth_people.parquet`
   - `truth_households.parquet`
   - `truth_household_memberships.parquet`
   - `truth_residence_history.parquet`
   - `truth_events.parquet`

## Consistency Guarantees
The simulator and quality report enforce:
- Non-overlapping residence intervals per person.
- Non-overlapping household membership intervals per person.
- Coupled partners remain co-located (same household/address) unless divorced.

## Enforced In Code
- Simulator:
  - `src/sog_phase2/simulator.py`
- Run script:
  - `scripts/generate_phase2_truth.py`

## Run Command
```bash
python scripts/generate_phase2_truth.py --run-id <run_id> --overwrite
```

Example:
```bash
python scripts/generate_phase2_truth.py --run-id 2026-03-10_single_movers_seed20260310 --overwrite
```
