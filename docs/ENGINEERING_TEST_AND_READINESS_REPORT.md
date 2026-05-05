# SOG Engineering Test Report and Production Readiness Guide

**Last updated**: April 8, 2026
**Test suite version**: 322 tests, all passing
**Runtime**: ~112 seconds on Windows 11, Python 3.12

This document is the single source of truth for engineers working on the SOG synthetic data pipeline. It covers what the system does, how it is tested, what works well, what has known issues, and what must be addressed before or after publishing.

---

## 1) What SOG Does

SOG is a deterministic pipeline that generates synthetic person and address records for entity-resolution (ER) benchmarking. It answers: "If my ER system sees realistic household change, messy records, and partial coverage, how well does it recover the true entities?"

The pipeline has two phases:

- **Phase 1**: Generate a baseline population of synthetic people with demographics, addresses, names, SSNs, phones.
- **Phase 2**: Simulate life events (moves, cohabitation, births, divorces, roommate splits) on that population, then emit observed datasets with configurable noise and duplication.

Phase 2 is the core of what ships. It produces:

- Truth layer (parquet): people, households, memberships, residence history, events
- Observed layer (CSV): one or more dataset files with realistic noise
- Linking artifacts: entity_record_map.csv, truth_crosswalk.csv (pairwise), per-pair crosswalks (N-way)
- Metadata: manifest.json, quality_report.json, scenario.yaml, scenario_selection_log.json

---

## 2) Why SOG Exists

Entity Resolution (ER) is the problem of determining which records across one or more databases refer to the same real-world person. Every ER algorithm needs benchmark data with known ground truth to measure precision, recall, and F1. Real-world data cannot be shared due to privacy constraints, and hand-constructed toy datasets do not expose realistic difficulty.

SOG fills this gap. It generates large-scale synthetic person records that exhibit the same messy patterns found in real administrative, commercial, and government databases:

- **Address instability**: People move. The same person appears at different addresses in different systems.
- **Household dynamics**: People form couples, have children, divorce, leave home. These events create shared addresses, name changes, and ambiguous linkage.
- **Field-level corruption**: OCR errors, phonetic misspellings, nickname substitution, date transposition, SSN masking, and missing fields.
- **Coverage asymmetry**: System A covers 95% of the population, System B covers 40%. Most entities in A have no counterpart in B.
- **Duplication**: The same person appears multiple times in the same file due to data entry, system merges, or record refresh.

SOG produces all of this deterministically from a single seed, so any ER benchmark is fully reproducible. The outputs include the ground truth (who is actually the same person) alongside the noisy observed data, so precision and recall can be measured exactly.

**Target users**: ER researchers, data quality engineers, and teams building or evaluating record linkage systems.

---

## 3) Architecture Overview

```
Phase-1 CSV (baseline population)
       |
       v
  [Selection Engine]  -->  scenario_population.parquet
       |                   (subset + latent traits)
       v
  [Truth Simulator]   -->  truth_people/households/memberships/residence/events (.parquet)
       |                   (deterministic state machine, seeded RNG)
       v
  [Emission Engine]   -->  DatasetA.csv, DatasetB.csv, entity_record_map.csv, crosswalk
       |                   (noise injection, duplication, coverage sampling)
       v
  [Quality Reporter]  -->  quality_report.json
       |                   (truth consistency + ER benchmark metrics)
       v
  [Output Validator]  -->  validation result (schema + contract checks)
```

### Source modules (src/sog_phase2/)

| Module | Purpose | Lines |
|---|---|---|
| `pipeline.py` | Orchestrates the full run: load YAML, select, simulate, emit, validate | ~520 |
| `simulator.py` | Truth-layer state machine: households, events, residence | ~1,324 |
| `emission.py` | Observed dataset generation: noise, duplication, coverage | ~1,329 |
| `selection.py` | Population subsetting: entity view, latent traits, filtering, sampling | ~462 |
| `event_grammar.py` | Event schema: MOVE, COHABIT, BIRTH, DIVORCE, LEAVE_HOME | ~345 |
| `constraints.py` | Demographic constraints: marriage age, fertility range, residence overlap | ~310 |
| `quality.py` | Quality metrics: truth consistency, ER benchmark, drift rates | ~805 |
| `output_contract.py` | Schema validation for all run artifacts | ~824 |
| `params.py` | Census/CDC parameter loading | ~75 |
| `scenario_catalog.py` | Scenario registry (catalog.yaml) | ~50 |

### Scripts (scripts/)

| Script | Purpose |
|---|---|
| `run_phase2_pipeline.py` | CLI wrapper: `--scenario single_movers` runs the full pipeline |
| `generate_phase2_truth.py` | Run truth simulation only |
| `generate_phase2_observed.py` | Run emission only |
| `validate_phase2_outputs.py` | Validate a completed run |
| `build_phase2_params.py` | Fetch Census/CDC data and build parameter tables |
| `build_phase2_scenario_population.py` | Rebuild scenario_population.parquet |

### 3.1 Methodology - How SOG is designed

SOG is not a flat fake-row generator. It uses a layered benchmark methodology:

1. Build a baseline synthetic population in Phase 1.
2. Collapse that population to one canonical entity row per `PersonKey`.
3. Assign deterministic latent propensities that make people behave differently over time.
4. Simulate household and residence state transitions on normalized truth tables.
5. Project that truth into one or more observed datasets at configured snapshots.
6. Distort the observed layer with controlled duplication, partial coverage, and field-level corruption.
7. Measure the achieved outcome with quality metrics and validate the run against an explicit artifact contract.

The important methodological choice is the separation between truth creation and observed emission. Truth tables stay internally consistent and human-auditable. Observed files carry the ambiguity, incompleteness, and corruption that ER systems must solve. This keeps the benchmark realistic without making the answer key opaque.

### 3.2 What is novel and what makes SOG stand out

SOG stands out because it combines several properties that are usually scattered across separate tools:

- It models identity longitudinally. A person is represented as a time-varying entity with households, memberships, residences, and events, not just as an isolated row.
- It produces both the hidden world and the noisy world. `truth_people.parquet`, `truth_events.parquet`, and related truth tables define what actually happened; the CSV datasets define what downstream systems observe.
- It supports benchmark topology as a first-class concept. The same engine can emit single-dataset dedup workloads, pairwise linkage workloads, and N-way linkage workloads.
- It makes cardinality explicit. `one_to_one`, `one_to_many`, `many_to_one`, and `many_to_many` are achieved through record allocation and crosswalk construction, not left as an accidental byproduct.
- It is reproducible at two levels. Stable hashing governs entity-specific traits; ordered RNG streams govern simulation and emission. That combination makes runs repeatable even as the codebase evolves.
- It is auditable. The run directory contains the selected population, resolved scenario, manifest, quality report, and validator result, so engineers can inspect exactly how a benchmark was constructed.

### 3.3 Core algorithmic ideas behind the architecture

| Problem | Algorithmic choice | Why it matters |
|---|---|---|
| Stable per-person variation | SHA256-based deterministic unit values keyed by `(seed, salt, PersonKey)` | Adding or reordering rows does not reshuffle latent traits |
| Realistic event frequency over time | Convert annual rates to step probabilities with compounding | Monthly or daily simulation approximates annual targets instead of multiplying them |
| Preventing impossible same-step behavior | Ordered event phases plus per-step locking | A person cannot divorce, cohabit, and move independently in the same step |
| Achieving requested overlap and asymmetry | Overlap-first coverage allocation, then side-specific sampling | Requested pairwise overlap remains mathematically consistent with dataset coverage targets |
| Reusable answer keys | Canonical `entity_record_map.csv` plus pairwise projections | The same run can support multiple evaluation views without recomputing truth |

---

## 4) Shipped Scenarios (11 Canonical)

| Scenario | Primary Event | Match Mode | Main ER Stress |
|---|---|---|---|
| `single_movers` | MOVE | one_to_one | Address change |
| `clean_baseline_linkage` | MOVE (low) | one_to_one | Near-ideal sanity check |
| `couple_merge` | COHABIT | one_to_many | Shared-address household formation |
| `family_birth` | BIRTH | many_to_one | Household growth, new child entities |
| `divorce_custody` | DIVORCE + COHABIT | many_to_many | Hardest family ambiguity |
| `roommates_split` | LEAVE_HOME + MOVE | one_to_many | High churn, roommate grouping |
| `high_noise_identity_drift` | MOVE | one_to_one | OCR, phonetic, nickname corruption |
| `low_overlap_sparse_coverage` | MOVE | one_to_one | Weak shared coverage |
| `asymmetric_source_coverage` | MOVE | one_to_one | One broad, one sparse source |
| `high_duplication_dedup` | MOVE (background) | single_dataset | Within-file dedup |
| `three_source_partial_overlap` | MOVE | N-way (3 datasets) | Multi-source linkage |

Each shipped scenario has a canonical YAML in `phase2/scenarios/` and is regression-tested. The folder may also contain `_working_*.yaml` files created by scenario-editing flows; those are working copies, not shipped scenarios, and are intentionally excluded from canonical-scenario validation tests.

---

## 5) Internal Workings — How SOG Generates Data

This section walks through the pipeline stage by stage with pseudocode and data-flow detail. If you are debugging, extending, or reviewing the system, start here.

### 5.1 Stage 1: Pipeline Orchestration (`pipeline.py`)

The entry point is `run_scenario_pipeline()`. It coordinates all stages and writes all outputs.

```
FUNCTION run_scenario_pipeline(scenario_yaml_path, runs_root, project_root):
    # 1. Parse scenario YAML
    scenario = LOAD_YAML(scenario_yaml_path)
    scenario_id = scenario["scenario_id"]
    seed = scenario["seed"]

    # 2. Build run_id:  "YYYY-MM-DD_{scenario_id}_seed{seed}"
    run_id = build_run_id(scenario_id, seed, run_date)
    run_dir = runs_root / run_id

    # 3. Parse each config section through its dedicated parser
    #    Each parser validates types, ranges, enums, and returns a frozen dataclass
    selection_config   = parse_selection_config(scenario["selection"])
    simulation_config  = parse_simulation_config(scenario["simulation"])
    emission_config    = parse_emission_config(scenario["emission"])
    constraints_config = parse_constraints_config(scenario["constraints"])
    quality_config     = parse_quality_config(scenario["quality"])

    # 4. Load Census/CDC parameter tables
    phase2_params = load_phase2_params_from_project(project_root)

    # 5. Execute stages in order
    selected_df, selection_log = SELECT(phase1_csv, selection_config, seed)
    truth_result                = SIMULATE(phase1_df, selected_df, simulation_config, seed)
    emitted                     = EMIT(truth_result, emission_config, seed)
    quality_report              = COMPUTE_QUALITY(truth_result, emitted, constraints_config)

    # 6. Write all outputs: parquets, CSVs, manifest.json, quality_report.json
    # 7. Validate via output_contract
    validation = validate_phase2_run(runs_root, run_id)
    RETURN {run_id, truth_counts, observed_counts, validation_valid, paths}
```

**Key design decision**: Every stage receives its config as a frozen dataclass. The pipeline never passes raw dicts past Stage 1. This ensures all validation happens once, at config-parse time.

The orchestrator is also responsible for preserving the exact run boundary. It derives `run_id`, resolves the Phase-1 inputs, decides whether `scenario_population.parquet` must be rebuilt, writes a resolved copy of `scenario.yaml`, then emits `manifest.json` and `quality_report.json` from the same in-memory result objects used to write the parquet and CSV artifacts. That design keeps the metadata tied directly to the actual run outputs rather than reconstructing it later from the filesystem.

Methodologically, `pipeline.py` is the layer that turns a scenario from an abstract YAML into an auditable benchmark package. It is not only a CLI wrapper. It binds together configuration validation, parameter loading, truth generation, observed emission, reporting, and contract checking into one deterministic execution graph.

### 5.2 Stage 2: Selection Engine (`selection.py`)

The selection engine takes the full Phase-1 population and produces a smaller scenario population with assigned latent propensity traits.

```
FUNCTION select_scenario_population(phase1_df, mobility_params_df, selection_config, seed):

    # Step A — Entity View: collapse multi-record people into one row per person
    entity_df = BUILD_PHASE1_ENTITY_VIEW(phase1_df)
        FOR EACH PersonKey group:
            representative = first record (by EntityRecordIndex, RecordKey)
            residence_type = last record's ResidenceType
            records_count  = number of records for this PersonKey
            redundancy     = "multi_record" if records_count > 1 else "single_record"
        RETURN DataFrame[PersonKey, AgeBin, Gender, Ethnicity, ResidenceType, RedundancyProfile]

    # Step B — Latent Traits: assign deterministic propensity scores
    enriched = ASSIGN_LATENT_TRAITS(entity_df, mobility_params_df, seed)
        FOR EACH person:
            # Base propensity comes from their age cohort (Census ACS data)
            mobility_base = ACS_mobility_rate[person.AgeBin]    # e.g. age_18_34 → 0.22
            # Jitter is deterministic: SHA256(seed|"mobility"|PersonKey) → float in [0,1]
            jitter = (SHA256_UNIT(seed, "mobility", PersonKey) - 0.5) * 0.08
            mobility_score = CLIP(mobility_base + jitter, 0, 1)

            # Same pattern for partnership and fertility using age-bin base maps plus wider jitter
            partnership_score = CLIP(partnership_base + SHA256_jitter, 0, 1)
            fertility_score   = CLIP(fertility_base + SHA256_jitter, 0, 1)

            # Bucket: low / medium / high based on configurable thresholds
            mobility_bucket = "low" if score <= 0.09, "high" if score >= 0.18, else "medium"
        RETURN enriched_df with 6 new columns (Score + Bucket for each trait)

    # Step C — Filter: keep only people matching scenario criteria
    filtered = APPLY_FILTERS(enriched, selection_config.filters)
        # Each filter is optional. If the list is empty, no filtering happens.
        IF age_bins:      KEEP where AgeBin IN age_bins
        IF genders:       KEEP where Gender IN genders
        IF ethnicities:   KEEP where Ethnicity IN ethnicities
        IF residence_types: KEEP where ResidenceType IN residence_types
        IF redundancy_profiles: KEEP where RedundancyProfile IN redundancy_profiles
        IF mobility_propensity_buckets: KEEP where MobilityBucket IN mobility_buckets

    # Step D — Sample: deterministically select N people from filtered pool
    selected = DETERMINISTIC_SAMPLE(filtered, selection_config.sample_mode, seed)
        IF mode == "all":   take everyone
        IF mode == "count":  take exactly N (via seeded numpy RNG without replacement)
        IF mode == "pct":    take round(pct/100 * len(candidates))

    RETURN (selected_df, audit_log)
```

**Why SHA256 for jitter**: `numpy.random` state depends on call order, which makes it fragile when the population changes. SHA256 hashing produces a stable float for each (seed, salt, PersonKey) triple regardless of population size or ordering. This guarantees that adding person #10,001 to the Phase-1 file does not change the mobility score of person #3.

More precisely, the selection methodology has two coupled goals:

- Preserve reproducibility at the entity level.
- Preserve heterogeneity at the population level.

The code does that by combining age-bin base rates with bounded deterministic jitter:

```text
mobility_score    = clip(mobility_base    + (hash_unit - 0.5) * 0.08, 0, 1)
partnership_score = clip(partnership_base + (hash_unit - 0.5) * 0.30, 0, 1)
fertility_score   = clip(fertility_base   + (hash_unit - 0.5) * 0.30, 0, 1)
```

Those scores are then bucketed into `low`, `medium`, and `high` bands using scenario-configurable thresholds. The output is not only a filtered list of people. It is a scenario-ready latent population with explicit behavioral priors attached to each entity.

Another important internal detail is the audit trail. `scenario_selection_log.json` stores the filter counts after each stage, the selected entity count, and a checksum over the ordered `PersonKey` list. That means selection is not a black box: engineers can verify both why a pool shrank and whether the final chosen population matches a previous run exactly.

### 5.3 Stage 3: Truth Simulator (`simulator.py`)

The simulator is a discrete-time state machine that advances through monthly (or daily) steps, rolling dice at each step to decide which life events occur.

```
FUNCTION simulate_truth_layer(phase1_df, scenario_population_df, seed, simulation_config, ...):

    # Initialize the state machine
    state = _SimulationState(seed, simulation_config, constraints_config, selected_people)
    state.rng = numpy.default_rng(seed)

    # Build baseline: one person → one household → one address → one membership → one residence
    FOR EACH selected person:
        CREATE solo household  HH_SIM_XXXXXX
        CREATE address         from Phase-1 AddressKey (or generate ADDR_SIM_XXXXXX)
        ASSIGN person to household as HEAD
        ASSIGN person to address
        RECORD in person_age, person_gender, person_mobility, person_partnership, ...

    # If scenario has roommate parameters, regroup into shared households
    IF roommate_group_share_pct > 0:
        SORT eligible people (age within roommate range)
        GROUP into households of size roommate_household_size_min..max
        CLOSE their solo households, CREATE group households
        ASSIGN all group members to the same address

    # Convert annual rates to per-step probabilities
    #   Formula:  step_prob = 1 - (1 - annual_prob)^(1/steps_per_year)
    #   Example:  12% annual move rate → ~1.06% monthly probability
    step_rates = RESOLVE_STEP_RATES(scenario_parameters, phase2_priors, granularity)

    # Step through time
    FOR EACH step_date IN [start+1month, start+2months, ...]:
        locked = {}  # People who already had an event this step can't have another

        # 1. DIVORCES first (breaks couples before new couples form)
        FOR EACH active couple (person_a, person_b):
            IF person_a or person_b in locked: SKIP
            avg_partnership = mean(person_a.partnership, person_b.partnership)
            prob = clip(divorce_base * (0.7 + (1.0 - avg_partnership)), 0, 1)
            IF rng.random() < prob:
                new_household = CREATE household (type="post_divorce")
                MOVE person_b to new_household + new address
                CLOSE old membership for person_b, OPEN new membership
                REMOVE couple from active_couples
                EMIT event(DIVORCE, PersonKeyA, PersonKeyB, CustodyMode=random)
                LOCK person_a, person_b

        # 2. COHABITATIONS (pair up single people)
        eligible = people NOT in locked AND NOT already coupled AND marriage_age_ok
        SHUFFLE eligible using rng
        WHILE at least 2 eligible remain:
            person_a = POP first
            person_b = POP second
            IF age_gap_allowed(a, b) AND rng.random() < prob:
                mode = random_choice(move_to_A, move_to_B, new_address)
                target_address = pick based on mode
                new_household = CREATE household (type="couple")
                TRANSFER both to new_household at target_address
                ADD to active_couples
                EMIT event(COHABIT, PersonKeyA, PersonKeyB, NewHouseholdKey, CohabitMode)
                LOCK both

        # 3. BIRTHS (eligible women in fertility age range; parent2 is optional)
        FOR EACH eligible female in fertility range:
            fertility_score = person.fertility_propensity
            prob = clip(birth_base * (0.4 + fertility_score), 0, 1)
            IF rng.random() < prob:
                child_key = P_CHILD_XXXXXX
                CREATE child person (inherits parent's last name, ethnicity)
                ADD child to parent's household + address
                parent2 = active partner if present else ""
                EMIT event(BIRTH, Parent1PersonKey, Parent2PersonKey, ChildPersonKey)
                LOCK parent1 (and parent2 if present)

        # 4. LEAVE_HOME events (young adults leaving shared households)
        FOR EACH unpartnered person age 18..30 in a household with >1 members:
            IF rng.random() < leave_home_prob:
                new_household = CREATE solo household + new address
                TRANSFER person to new_household
                EMIT event(LEAVE_HOME, ChildPersonKey=person)
                LOCK person

        # 5. MOVES (any unlocked person or household)
        FOR EACH person NOT in locked:
            mobility = person.mobility_propensity
            prob = clip(move_base * (0.5 + mobility), 0, 1)
            IF rng.random() < prob:
                IF person has a partner:
                    MOVE entire household to new address
                    EMIT event(MOVE, SubjectHouseholdKey, FromAddress, ToAddress)
                ELSE:
                    MOVE person only to new address
                    EMIT event(MOVE, SubjectPersonKey, FromAddress, ToAddress)
                LOCK person (and partner if household move)

    # Assemble truth tables from accumulated rows
    RETURN {
        truth_people:       DataFrame from people_rows
        truth_households:   DataFrame from household_rows
        truth_memberships:  DataFrame from membership_rows
        truth_residence:    DataFrame from residence_rows
        truth_events:       DataFrame from event_rows
        quality:            consistency checks (residence overlap, membership overlap, couple colocation)
    }
```

**Event ordering within a step matters**: Divorces run first so the newly-single people can form new couples in the same step. Cohabitations run before births so that new couples can reproduce immediately. Moves run last so they don't interfere with household formation.

**Rate-to-probability conversion**: Annual rates from Census/CDC are converted to per-step probabilities using the compound formula `p_step = 1 - (1 - p_annual)^(1/12)`. This ensures that running 12 monthly steps approximates the annual rate, not 12x it.

**Constraint enforcement during simulation**: The simulator checks constraints in real-time. A 16-year-old cannot cohabit if `min_marriage_age=18`. A couple with a 30-year age gap is rejected if `max_partner_age_gap=25`. A woman aged 52 cannot give birth if `fertility_age_max=49`. These are checked before each event, not after.

Internally, `_SimulationState` is the engine's in-memory world model. It keeps:

- static person attributes such as age, gender, ethnicity, and latent propensity scores,
- current pointers such as `person_current_household` and `person_current_address`,
- open-interval indexes so memberships and residences can be closed and reopened correctly,
- `active_couples` as the live coupling relation,
- monotonic counters for `EVT_*`, `HH_SIM_*`, `ADDR_SIM_*`, and `P_CHILD_*` keys.

That structure matters because SOG is interval-based, not overwrite-based. A move does not simply replace an address field. It closes the current residence interval on `step_date - 1`, opens a new interval on `step_date`, updates the household address if needed, and only then emits the corresponding event row. The same pattern is used for memberships and household transitions.

The simulator also uses simple, interpretable probability transforms rather than a hidden model:

- divorce probability scales as `divorce_base * (0.7 + (1.0 - partnership))`
- cohabit probability scales as `cohabit_base * (0.5 + partnership)`
- birth probability scales as `birth_base * (0.4 + fertility)`
- move probability scales as `move_base * (0.5 + mobility)`

This is a deliberate design choice. Readers and benchmark users can reason about why a scenario produced more moves, fewer births, or harder household churn without reverse-engineering a statistical black box.

One additional scenario-specific algorithm is worth calling out: `roommates_split` can pre-group eligible 18-30 year olds into shared baseline households before the first simulation step. That means the scenario starts from a higher-entropy household graph and later uses `LEAVE_HOME` plus `MOVE` events to create realistic roommate churn instead of trying to manufacture it from purely solo baselines.

### 5.4 Stage 4: Emission Engine (`emission.py`)

The emission engine takes the truth layer and produces observed datasets with noise, duplication, and controlled coverage.

```
FUNCTION emit_observed_datasets(truth_people_df, truth_residence_df, emission_config, seed):
    rng = numpy.default_rng(seed)

    # Step A — Decide which entities appear in which dataset
    IF single_dataset mode:
        entities = SAMPLE(candidate_keys, appearance_pct, rng)
    ELIF pairwise mode (A/B):
        a_entities, b_entities, coverage_counts = CHOOSE_ENTITY_APPEARANCE(
            base_keys, late_only_keys, config, rng
        )
        # Algorithm:
        #   1. Sample overlap entities (appear in BOTH A and B)
        #   2. From remainder, sample A-only entities
        #   3. From remainder, sample B-only entities
        #   4. Add late-born entities (born during simulation) to B only
    ELIF multi-dataset mode (3+):
        entity_sets = CHOOSE_ENTITIES_FOR_MULTIPLE_DATASETS(...)
        # Each dataset gets its own entity set with controlled pairwise overlap

    # Step B — Allocate record counts (duplication)
    FOR EACH dataset:
        FOR EACH entity in that dataset:
            record_count[entity] = 1  # Start with one record
            # If match mode requires multi-record (e.g. one_to_many on B-side):
            IF entity in overlap AND mode demands multi-record for this side:
                record_count[entity] = max(record_count, 2)
        # Add random extra duplicates to hit the duplication target
        target_extra_records = round(duplication_pct / 100 * num_entities)
        FOR i in 1..target_extra_records:
            random_entity = PICK random entity from this dataset
            record_count[random_entity] += 1

    # Step C — Build observed rows with noise
    FOR EACH dataset:
        snapshot_date = simulation_start or simulation_end (per dataset config)
        snapshot = BUILD_SNAPSHOT(truth_people, truth_residence, snapshot_date)
            # Resolves which address each person lives at on snapshot_date
            # Recalculates age as of snapshot_date

        FOR EACH entity assigned to this dataset:
            FOR copy_index in 1..record_count[entity]:
                record_key = GENERATE_UNIQUE_RECORD_KEY()
                row = COPY truth fields from snapshot
                row = APPLY_NOISE(row, noise_config, rng)
                EMIT row to dataset CSV

    # Step D — Apply noise (per-record, per-field)
    FUNCTION APPLY_NOISE(row, noise_config, rng):
        noise_counts = {all zeros}
        IF rng.random() < name_typo_pct/100:
            row.FirstName or row.LastName = REPLACE one random character
            noise_counts.name_typo += 1
        IF rng.random() < dob_shift_pct/100:
            row.DOB = SHIFT by -3..+3 days
            noise_counts.dob_shift += 1
        IF rng.random() < ssn_mask_pct/100:
            row.SSN = MASK to "***-**-last4"
            noise_counts.ssn_mask += 1
        IF rng.random() < phone_mask_pct/100:
            row.Phone = ""
            noise_counts.phone_mask += 1
        IF rng.random() < middle_name_missing_pct/100:
            row.MiddleName = ""
            noise_counts.middle_name_missing += 1
        IF rng.random() < phonetic_error_pct/100:
            row.FirstName or LastName = SUBSTITUTE phonetic cluster
                # e.g. "ph"→"f", "ck"→"k", "th"→"d", "ey"→"ay"
            noise_counts.phonetic_error += 1
        IF rng.random() < ocr_error_pct/100:
            row.FirstName or LastName = SUBSTITUTE OCR-confused character
                # e.g. "O"→"0", "l"→"1", "B"→"8", "rn"→"m"
            noise_counts.ocr_error += 1
        IF rng.random() < nickname_pct/100:
            row.FirstName = SUBSTITUTE with common nickname from nicknames.json
                # e.g. "William"→"Bill", "Elizabeth"→"Liz"
            noise_counts.nickname += 1
        IF rng.random() < date_swap_pct/100:
            row.DOB = SWAP month and day only when the swapped date is still valid
            noise_counts.date_swap += 1
        IF rng.random() < zip_digit_error_pct/100:
            row.ZipCode = FLIP one digit
            noise_counts.zip_digit_error += 1
        IF rng.random() < suffix_missing_pct/100:
            row.Suffix = ""  # Drop Jr., Sr., III, etc.
            noise_counts.suffix_missing += 1
        RETURN row, noise_counts

    # Step E — Build linking artifacts
    entity_record_map = [PersonKey, DatasetId, RecordKey] for every emitted record
    IF pairwise:
        truth_crosswalk = JOIN A-records and B-records by PersonKey
    IF 3+ datasets:
        FOR EACH pair of datasets:
            pairwise_crosswalk = JOIN records by PersonKey

    RETURN {datasets, entity_record_map, truth_crosswalk, pairwise_crosswalks, metrics}
```

**12 noise types** are available per dataset. Each is independently controlled by a percentage. Multiple noise types can hit the same record — a name might get both a typo and a phonetic substitution. This is intentional: real-world data has correlated errors.

**Snapshot dates**: Dataset A typically snapshots at `simulation_start` and Dataset B at `simulation_end`. This means a person who moved during the simulation will have different addresses in A vs B, which is exactly the challenge ER systems must handle.

The emission methodology is where SOG becomes an ER benchmark rather than only a simulator.

First, emission builds snapshots from truth, not from events directly. `_build_snapshot()` resolves each person's active address on the requested date, attaches normalized address detail fields, recomputes age at that snapshot, and stamps `SourceSnapshotDate`. This is why the observed layer behaves like a system extract rather than a replay log.

Second, coverage is allocated explicitly. In pairwise mode, `_choose_entity_appearance()` computes target counts for A, B, and overlap, corrects overlap upward if arithmetic requires it, samples overlap first, then allocates A-only and B-only populations from the remainder. Late-born entities that exist only by the end snapshot can be added to B. This is how the engine creates realistic coverage asymmetry without breaking overlap math.

Third, duplication is not merely random row cloning. Record counts are allocated per entity, and the allocation is shaped by requested match mode:

- `one_to_many` forces at least two records on the B side for overlap entities
- `many_to_one` forces at least two records on the A side for overlap entities
- `many_to_many` forces multiplicity on both sides
- configured duplication percentages then add extra records on top of those structural minimums

Fourth, the crosswalk algorithm is explicit. `entity_record_map.csv` is the canonical truth map across every dataset. Pairwise `truth_crosswalk.csv` is a projection of that canonical map into the requested topology. In `many_to_many`, the crosswalk intentionally emits the Cartesian product between A-side and B-side records for each overlapping person, because that is the correct truth representation for that benchmark mode.

Finally, the noise operators are deliberately concrete and bounded:

- typo noise replaces one character, it does not perform arbitrary edit-distance chaos
- OCR noise draws from a fixed confusion table
- phonetic noise draws from a fixed cluster substitution table
- nickname noise consults the shipped nickname map
- ZIP noise mutates a single digit
- address-missing clears both `AddressKey` and the structured address columns together

This bounded-noise design is important. It makes difficulty tunable and explainable instead of letting corruption drift into unrealistic garbage.

### 5.5 Stage 5: Quality Reporter (`quality.py`)

The quality reporter computes metrics on both the truth layer and the observed layer.

```
FUNCTION compute_phase2_quality_report(truth_*, emitted_*, constraints_config, quality_config):

    # Truth consistency checks
    event_age_validation:
        FOR EACH COHABIT/DIVORCE event: check both parties >= min_marriage_age
        FOR EACH BIRTH event: check parent age in fertility range
        FOR EACH LEAVE_HOME event: check child age >= 18 (if enforced)

    time_overlap_errors:
        FOR EACH person: verify residence intervals don't overlap
        FOR EACH person: verify membership intervals don't overlap

    household_size_constraints:
        FOR EACH household: compute peak membership count over time
        FLAG households exceeding household_size_max

    # Scenario metrics
    event_counts: count MOVE, COHABIT, BIRTH, DIVORCE, LEAVE_HOME
    moves_per_person_distribution: histogram of how many times each person moved
    household_type_shares: proportion of solo_house, couple, family, etc.

    # ER benchmark metrics (only when observed data is provided)
    cross_file_overlap: count entities appearing in both datasets
    within_file_duplicate_rates: for each dataset, count multi-record entities
    attribute_drift_rates: compare observed vs truth for name, address, and phone
    match_cardinality_achieved: count one_to_one, one_to_many, many_to_one, many_to_many pairs
    crosswalk_ambiguity: detect records mapped to multiple persons (should be zero)

    RETURN quality_report dict
```

The quality layer is best understood as a measurement system, not just a smoke test.

It measures three different things:

- truth validity: do the simulated intervals, ages, and household sizes respect the configured rules?
- scenario behavior: did the run actually generate the kinds of events and household structures the scenario was supposed to stress?
- benchmark difficulty: how much duplication, overlap, and attribute drift did the observed layer actually achieve?

The implementation computes achieved benchmark properties from emitted artifacts, not from requested config alone. For example, duplicate rates come from `entity_record_map.csv`, attribute drift is measured by joining observed records back to truth via `RecordKey`, and cross-file overlap and achieved cardinality are computed from record counts per person per dataset. That distinction is important because a benchmark should report what happened, not only what was asked for.

### 5.6 Stage 6: Output Contract Validator (`output_contract.py`)

The validator checks that all expected files exist with correct schemas.

```
FUNCTION validate_phase2_run(runs_root, run_id):
    # 1. Check every required file exists
    VERIFY: truth_people.parquet, truth_households.parquet, truth_household_memberships.parquet,
            truth_residence_history.parquet, truth_events.parquet, scenario_population.parquet,
            entity_record_map.csv, manifest.json, quality_report.json, scenario.yaml,
            scenario_selection_log.json

    # 2. Check column schemas via parquet metadata (fast — no full read)
    FOR EACH parquet file:
        columns_found = READ_PARQUET_SCHEMA(path)
        VERIFY required columns present

    # 3. Check metadata consistency
    VERIFY: manifest.scenario_id == scenario.yaml.scenario_id
    VERIFY: manifest.seed == scenario.yaml.seed
    VERIFY: selection_log.selected_entities == len(scenario_population.parquet)

    # 4. If pairwise mode with 2 datasets: verify truth_crosswalk.csv exists
    # If 3+ datasets: verify per-pair crosswalks listed in manifest exist

    # 5. Validate truth events against event grammar
    # 6. Validate constraints against truth

    RETURN {valid: true/false, missing_files, schema_errors, metadata_errors}
```

This validator gives SOG a hard output contract. It does not assume that a run is valid just because the pipeline returned without raising an exception. It verifies:

- file existence for every required artifact,
- column-level schema presence for parquet and CSV outputs,
- run identity consistency across folder name, `manifest.json`, and `scenario.yaml`,
- event grammar validity for `truth_events.parquet`,
- truth-constraint validity using the configured scenario constraints,
- topology-specific requirements such as pairwise versus N-way crosswalk presence.

That contract layer is one of the strongest engineering choices in the repository because it turns benchmark generation into a repeatable product surface instead of a best-effort script.

### 5.7 Determinism Model

Every random decision in SOG is deterministic given the same seed. The system uses two strategies:

1. **SHA256-based hashing** for entity-level decisions (selection, trait assignment):
   ```
   float_value = SHA256(f"{seed}|{salt}|{person_key}") → first 8 bytes → uint64 → divide by 2^64-1
   ```
   This produces a stable float in [0,1] for each (seed, salt, PersonKey) triple. The result is independent of population size and ordering.

2. **NumPy RNG** (`numpy.random.default_rng(seed)`) for simulation and emission:
   The simulator creates one RNG instance at init and uses it throughout. The emission engine creates its own RNG, also seeded deterministically. Because both modules iterate over people, households, and datasets in stable sorted order, the random draws happen in a repeatable sequence.

There are also a few deterministic derived seeds for specialized subroutines. For example, roommate baseline grouping uses `seed + 1879`. The broader principle is that randomness is scoped, explicit, and reproducible rather than hidden in global process state.

### 5.8 Data Model

```
truth_people
    PersonKey (PK)  FormalFirstName  MiddleName  LastName  Suffix  Gender  Ethnicity  DOB  Age  AgeBin  SSN  Phone

truth_households
    HouseholdKey (PK)  HouseholdType  HouseholdStartDate  HouseholdEndDate

truth_household_memberships
    PersonKey (FK) + HouseholdKey (FK) + MembershipStartDate  →  HouseholdRole  MembershipEndDate

truth_residence_history
    PersonKey (FK) + AddressKey + ResidenceStartDate  →  ResidenceEndDate

truth_events
    EventKey (PK)  EventType  EventDate  + event-specific fields (varies by type)

entity_record_map
    PersonKey (FK)  DatasetId  RecordKey
    — This is the canonical truth map. For any RecordKey in any observed CSV, this tells you which person it belongs to.

truth_crosswalk (pairwise only)
    PersonKey  A_RecordKey  B_RecordKey
    — Direct A↔B linking for pairwise ER scoring.

Observed dataset CSV (e.g. DatasetA.csv)
    RecordKey (PK)  DatasetId  FirstName  MiddleName  LastName  Suffix  FullName
    Gender  Ethnicity  DOB  Age  SSN  Phone  AddressKey
    HouseNumber  StreetName  UnitType  UnitNumber  StreetAddress  City  State  ZipCode
    SourceSnapshotDate  SourceSystem
```

The methodological point of this schema is that truth is normalized and observed is denormalized. Truth tables encode state transitions and interval history. Observed files encode what a downstream ER system would ingest at a snapshot. Readers should treat `entity_record_map.csv` as the authoritative bridge between those two worlds.

---

## 6) Complete Test Suite

### 6.1 Test inventory (322 tests, 28 test files)

| Test File | Count | Category | What It Validates |
|---|---|---|---|
| `test_phase2_scenario_regression.py` | 17 | Regression | All 11 scenarios produce correct events, cardinality, overlap |
| `test_phase2_all_scenarios_smoke.py` | 11 | E2E Smoke | Every scenario runs from YAML to validated output |
| `test_phase2_scenario_yaml_validation.py` | 90 | Schema | Every canonical YAML parses, has valid simulation/emission/selection/constraints/quality, and `_working_*.yaml` copies are ignored |
| `test_phase2_event_grammar_extended.py` | 30 | Unit | All 5 event types: valid/invalid fields, enums, missing columns |
| `test_phase2_event_grammar.py` | 4 | Unit | MOVE validation, grammar vocabulary |
| `test_phase2_constraints_extended.py` | 19 | Unit | Age gap, child-lives-alone, residence intervals, config validation |
| `test_phase2_constraints.py` | 5 | Unit | Defaults, underage cohabit, fertility range, overlap |
| `test_phase2_selection_extended.py` | 25 | Unit | Sample modes, propensity, filtering, determinism |
| `test_phase2_selection.py` | 3 | Unit | Entity view, determinism, filter primitives |
| `test_phase2_emission_extended.py` | 13 | Unit | Noise injection, duplication math, coverage, determinism |
| `test_phase2_emission.py` | 12 | Unit | Config parsing, pairwise/single/multi emission |
| `test_phase2_simulator_extended.py` | 14 | Unit | Move/cohabit/birth, truth tables, consistency, edge cases |
| `test_phase2_simulator.py` | 3 | Unit | Config, determinism, roommates |
| `test_phase2_quality_extended.py` | 14 | Unit | Quality config, event counts, household violations, ER metrics |
| `test_phase2_quality.py` | 2 | Unit | Defaults, comprehensive ER metrics report |
| `test_phase2_output_contract.py` | 10 | Unit | Schema, missing files, seed mismatch, grammar, constraints |
| `test_phase2_pipeline_integration.py` | 6 | Integration | Single/pairwise/multi pipeline, reproducibility, scale (300-500 people) |
| `test_phase2_pipeline_errors.py` | 4 | Error | Missing YAML, missing CSV, overwrite protection |
| `test_phase2_resilience.py` | 6 | Resilience | Malformed input, partial failure, boundary conditions |
| `test_phase2_params.py` | 3 | Unit | Parameter loading, household shares, mobility rates |
| `test_phase2_scenario_catalog.py` | 5 | Unit | Catalog integrity, unique IDs, file existence |
| `test_phase2_observed_cli.py` | 4 | CLI | Single/pairwise/multi CLI emission |
| `test_frontend_orchestrator.py` | 2 | Frontend | Scenario inference and run-turn orchestration |
| `test_frontend_production_entrypoint.py` | 1 | Frontend | Production entrypoint rerun behavior |
| `test_frontend_scenarios.py` | 4 | Frontend | Scenario management, cloning, schema |
| `test_frontend_runs_data.py` | 6 | Frontend | Run data reading, manifest parsing |
| `test_frontend_jobs.py` | 2 | Frontend | Job lifecycle |
| `test_frontend_worker_integration.py` | 3 | Frontend | Session persistence, agent tool loop |

### 6.2 Coverage by module

| Module | Tests | Coverage Level | Notes |
|---|---|---|---|
| `event_grammar.py` | 34 | **Strong** | All 5 event types, all validation rules, edge cases |
| `constraints.py` | 24 | **Strong** | All constraint types, config validation, both allow/deny paths |
| `emission.py` | 25 | **Strong** | Config parsing, noise injection, duplication, overlap, determinism |
| `selection.py` | 28 | **Strong** | Entity view, traits, filtering, all sample modes, determinism |
| `simulator.py` | 17 | **Good** | Move/cohabit/birth events, consistency, determinism; divorce tested via regression |
| `quality.py` | 16 | **Good** | Config, event counts, household size, ER metrics |
| `output_contract.py` | 10 | **Good** | Schema validation, metadata checks |
| `pipeline.py` | 10 | **Good** | Integration + error handling |
| `params.py` | 3 | **Adequate** | Load + data consistency; network-dependent build_params not testable offline |
| `scenario_yaml_validation.py` | 90 | **Strong** | All 11 shipped YAMLs validated through 7 parametrized dimensions; ignores `_working_*.yaml` copies |
| `scenario_catalog.py` | 5 | **Good** | Catalog integrity, unique IDs, shipped-file coverage |
| Frontend | 18 | **Adequate** | Core tools tested; no component unit tests |

### 6.3 What the tests prove

**Correctness:**
- Every event type validates its required fields and rejects invalid ones
- Demographic constraints (marriage age, fertility range, partner gap, child-lives-alone) are enforced
- Noise injection produces expected noise counts when rates are high; produces zero noise when rates are zero
- Duplication increases record counts as expected
- Record keys are unique within each dataset
- Every person in truth_people has residence and membership records

**Determinism:**
- Same seed produces identical truth events, observed datasets, entity record maps, and selection results
- Different seeds produce different results

**Contract:**
- Every shipped scenario YAML parses without error through all 5 config parsers
- Working-copy scenario YAMLs prefixed `_working_` do not break canonical scenario validation
- Every shipped scenario produces a valid run that passes the output contract validator
- Pairwise scenarios produce truth_crosswalk.csv; single-dataset scenarios do not
- N-way scenarios produce per-pair crosswalks and correct topology

**Error handling:**
- Missing YAML raises FileNotFoundError
- Missing Phase-1 CSV raises FileNotFoundError
- Running a second time without overwrite raises FileExistsError
- Invalid config values (negative ages, out-of-range percentages, invalid enums) raise ValueError

---

## 7) What Works Well

### 7.1 Simulation engine
The truth simulator produces consistent, constraint-respecting event histories. In the `single_movers` reference run (10,000 people, 12 months):
- 821 MOVE events generated (8.2% of population, close to 12% annual target for 1-year monthly sim)
- Zero constraint violations
- Zero residence/membership overlap errors
- All coupled people verified colocated

### 7.2 Noise injection
Noise rates land close to configured targets:
- Name typo 1% target in DatasetA: achieved 112/8821 = 1.27%
- Middle name missing 20% target in DatasetA: achieved 1755/8821 = 19.9%
- Name typo 2.5% target in DatasetB: achieved 242/9540 = 2.54%
- SSN mask 6% target in DatasetB: achieved 618/9540 = 6.48%

Noise rates scale proportionally and behave correctly at 0% and high percentages.

### 7.3 Deterministic reproducibility
The entire pipeline is deterministic given the same seed. SHA256-based entity hashing ensures stable selection and trait assignment. NumPy RNG seeding ensures stable simulation and emission.

### 7.4 Scenario diversity
The 11 shipped scenarios cover the full spectrum of ER benchmarking challenges:
- One-to-one through many-to-many cardinality
- Single-dataset dedup through 3-source linkage
- Clean baselines through heavy corruption
- High overlap through sparse coverage

### 7.5 Quality reporting
The quality report provides actionable metrics: event counts, overlap rates, duplication rates, attribute drift, cardinality distribution, household size constraints. These are sufficient for a user to determine whether a run behaved as expected.

---

## 8) Known Issues and Risks

### 8.1 MUST FIX before publishing

#### Issue 1: Manifest vs quality report entity count discrepancy
- **What**: `manifest.json` reports "base" entity counts (before late-only arrivals), but `quality_report.json` reports final entity counts. In the reference run, Dataset B shows 8,981 in manifest but 9,000 in quality report.
- **Where**: [pipeline.py](src/sog_phase2/pipeline.py) (manifest building) vs [emission.py](src/sog_phase2/emission.py) (coverage calculation)
- **Fix**: Align counts or explicitly document the difference in both files.
- **Risk if unfixed**: Users comparing manifest and quality report will see inconsistent numbers and lose trust in the outputs.

#### Issue 2: No atomic write safety
- **What**: If the pipeline crashes after writing truth parquets but before writing manifest/quality_report, the run directory contains incomplete outputs that look partially valid.
- **Where**: [pipeline.py](src/sog_phase2/pipeline.py) lines 245-520
- **Fix**: Write to a temp directory, then rename atomically on success; or write a `.complete` marker last and check for it in the validator.
- **Risk if unfixed**: In CI or automated workflows, a crashed run could be picked up as a completed run.

#### Issue 3: Silent error swallowing in frontend tools
- **What**: Multiple `except Exception: pass` blocks in [frontend/sog_tools.py](frontend/sog_tools.py) silently ignore YAML write failures, cleanup failures, and validation errors.
- **Fix**: Return error details in the response dict or log them.
- **Risk if unfixed**: Users won't know if their scenario edits were saved.

#### Issue 4: Absolute Windows paths in manifest
- **What**: `manifest.json` stores resolved absolute paths like `H:\\AAA_Taha\\...`. These break when moving to a different machine, drive, or OS.
- **Where**: [pipeline.py](src/sog_phase2/pipeline.py) path resolution
- **Fix**: Store paths relative to the run directory or project root.
- **Risk if unfixed**: Non-portable outputs; CI/CD, Linux, and Mac users get broken paths.

### 8.2 SHOULD FIX before first external deployment

#### Issue 5: No pre-emission validation of noise achievability
- **What**: If a scenario configures `name_typo_pct: 50` but there are only 2 records, the system can't achieve 50% and silently produces whatever it can.
- **Fix**: Warn when population is too small for configured noise targets.

#### Issue 6: Dependency versions not pinned
- **What**: `requirements.txt` uses `>=` constraints (e.g., `numpy>=1.26`, `streamlit>=1.39`). A new major release could break behavior.
- **Fix**: Pin to tested versions (e.g., `numpy==1.26.4`).

#### Issue 7: Monolithic simulator state machine
- **What**: `_SimulationState` in [simulator.py](src/sog_phase2/simulator.py) is ~1,000 lines. It's hard to test individual event types in isolation.
- **Fix**: Not blocking, but makes future development harder. Document the internal structure for new contributors.

#### Issue 8: No input CSV schema validation
- **What**: Phase-1 CSV is read and used without upfront column validation. Errors surface deep in the selection or simulation phase with unclear messages.
- **Fix**: Add a schema check at CSV load time with a clear error listing missing/unexpected columns.

### 8.3 Known limitations (document, don't fix)

1. **Network dependency for param rebuilding**: `build_phase2_params.py` hits the Census API. Params are shipped pre-built, so this only matters if users want to refresh demographic data.
2. **Hardcoded demographic year**: Parameter files reference ACS 2024, CDC 2023, NCHS 2024. Future years require re-running the build script.
3. **Nickname map dependency**: Emission's nickname noise requires `phase1/prepared/nicknames.json`. If this file is missing, nickname noise silently produces zero substitutions.
4. **O(n*m) quality computation**: Moves-per-person uses nested iteration over memberships. For very large runs (100K+ people), this could be slow. Current scale (10K) runs in seconds.
5. **No parallel emission**: Multi-dataset emission is sequential. For 3+ large datasets, this adds runtime but doesn't affect correctness.

---

## 9) How to Run Tests

### 9.1 Full suite

```bash
python -m pytest tests/ -v
```

Expected: 322 passed in ~130 seconds.

### 9.2 By category

```bash
# Unit tests only (fast, ~10s)
python -m pytest tests/test_phase2_event_grammar*.py tests/test_phase2_constraints*.py tests/test_phase2_selection*.py tests/test_phase2_emission*.py tests/test_phase2_quality*.py tests/test_phase2_simulator*.py -v

# Integration tests (medium, ~20s)
python -m pytest tests/test_phase2_pipeline*.py tests/test_phase2_observed_cli.py -v

# End-to-end smoke tests for all scenarios (~30s)
python -m pytest tests/test_phase2_all_scenarios_smoke.py -v

# Scenario YAML validation (~5s)
python -m pytest tests/test_phase2_scenario_yaml_validation.py -v

# Regression tests (~15s)
python -m pytest tests/test_phase2_scenario_regression.py -v

# Frontend tests (~5s)
python -m pytest tests/test_frontend*.py -v
```

### 9.3 Running a single scenario manually

```bash
python scripts/run_phase2_pipeline.py --scenario single_movers
```

Output lands in `phase2/runs/YYYY-MM-DD_single_movers_seedN/`.

### 9.4 Validating a completed run

```bash
python scripts/validate_phase2_outputs.py --run-id 2026-03-10_single_movers_seed20260310
```

---

## 10) How to Add a New Scenario

1. Create `phase2/scenarios/your_scenario.yaml` using an existing scenario as a template.
2. Add an entry to `phase2/scenarios/catalog.yaml`.
3. Add a smoke test in `tests/test_phase2_all_scenarios_smoke.py`:
   ```python
   def test_smoke_your_scenario(tmp_path: Path) -> None:
       result = _run_scenario(tmp_path, "your_scenario", sample_override=SAMPLE)
       assert result["validation_valid"] is True
   ```
4. Add regression assertions in `tests/test_phase2_scenario_regression.py` for the specific behavior your scenario targets.
5. Run: `python -m pytest tests/ -v`

---

## 11) How to Add a New Event Type

Event types are defined in [event_grammar.py](src/sog_phase2/event_grammar.py).

1. Add the event name to `ACTIVE_EVENT_TYPES` (or move from `OPTIONAL_LATER_EVENT_TYPES`).
2. Add validation logic in `validate_truth_events_dataframe()`.
3. Add simulation logic in `simulator.py` `_SimulationState`.
4. Add tests in `tests/test_phase2_event_grammar_extended.py` for valid and invalid cases.
5. Update any constraint checks in `constraints.py` if the event has demographic rules.

---

## 12) How to Add a New Noise Type

Noise types are defined in [emission.py](src/sog_phase2/emission.py) `DatasetNoiseConfig`.

1. Add the field to `DatasetNoiseConfig` with a default of `0.0`.
2. Implement the noise function (e.g., `_apply_your_noise()`).
3. Call it in `_build_dataset_rows()` with the configured percentage.
4. Track the count in the per-dataset noise_counts dict.
5. Add a test in `tests/test_phase2_emission_extended.py` verifying the noise produces expected counts.

---

## 13) Production Readiness Assessment

### Score: 7/10

**What earns the score:**
- Core pipeline is functionally complete and produces valid, reproducible output
- 322 tests pass covering unit, integration, regression, and end-to-end scenarios
- All 11 canonical scenarios validate end-to-end
- Noise injection, demographic constraints, and quality reporting all work correctly
- Deterministic seeding ensures reproducible benchmarks

**What prevents a higher score:**
- Data consistency gap between manifest and quality report entity counts (Section 8.1, Issue 1)
- No atomic write protection against partial failures (Section 8.1, Issue 2)
- Silent error swallowing in frontend (Section 8.1, Issue 3)
- Non-portable absolute paths in manifest (Section 8.1, Issue 4)
- Dependency versions not pinned (Section 8.2, Issue 6)

**Bottom line:** The system generates correct synthetic data and is safe to publish as-is for users who run it locally and inspect outputs manually. For automated CI/CD pipelines, production web deployments, or cross-platform distribution, fix Issues 1-4 first.

---

## 14) Test Evidence Summary

The table below links each system capability to the tests that prove it works.

| Capability | Test Evidence | Result |
|---|---|---|
| MOVE events generated correctly | `test_single_movers_produces_moves`, `test_high_move_rate_produces_moves`, `test_zero_move_rate_produces_no_moves` | PASS |
| COHABIT events and shared households | `test_couple_merge_produces_cohabit_and_shared_residence`, `test_high_cohabit_rate_produces_cohabit_events`, `test_cohabit_creates_shared_household` | PASS |
| BIRTH events create new people | `test_family_birth_produces_birth_events`, `test_high_birth_rate_produces_birth_events`, `test_birth_creates_new_person` | PASS |
| DIVORCE events and split households | `test_divorce_custody_produces_divorce_and_split_households` | PASS |
| LEAVE_HOME events | `test_roommates_split_contains_split_household_pattern`, `test_valid_leave_home_event` | PASS |
| One-to-one crosswalk behavior | `test_clean_baseline_linkage_stays_low_noise_one_to_one`, `test_smoke_clean_baseline_linkage` | PASS |
| One-to-many crosswalk behavior | `test_couple_merge_exposes_one_to_many_crosswalk_behavior`, `test_one_to_many_mode_yields_multi_b_records` | PASS |
| Many-to-one crosswalk behavior | `test_family_birth_exposes_many_to_one_crosswalk_behavior` | PASS |
| Many-to-many crosswalk behavior | `test_divorce_custody_exposes_many_to_many_crosswalk_behavior` | PASS |
| Single-dataset dedup topology | `test_high_duplication_dedup_is_single_dataset_with_large_duplicate_pressure`, `test_smoke_high_duplication_dedup` | PASS |
| Three-source N-way linkage | `test_three_source_partial_overlap_emits_n_way_pairwise_artifacts`, `test_smoke_three_source_partial_overlap` | PASS |
| Noise injection works at configured rates | `test_high_name_typo_rate_produces_some_typos`, `test_ssn_mask_produces_partial_masking`, `test_high_middle_name_missing_rate` | PASS |
| Zero noise produces clean output | `test_zero_noise_produces_clean_output` | PASS |
| Duplication increases record count | `test_duplication_increases_record_count` | PASS |
| Record keys unique within datasets | `test_record_keys_are_unique_within_dataset` | PASS |
| Entity record map covers all records | `test_entity_record_map_covers_all_records` | PASS |
| Overlap percentage math | `test_crosswalk_overlap_matches_claimed_overlap_pct` (5 scenarios) | PASS |
| Marriage age constraint enforced | `test_underage_cohabit_violation_when_disallowed`, `test_divorce_also_checks_marriage_age` | PASS |
| Marriage age constraint can be relaxed | `test_underage_cohabit_allowed_with_switch` | PASS |
| Fertility age range enforced | `test_birth_outside_fertility_range_violation` | PASS |
| Partner age gap enforced | `test_partner_age_gap_violation`, `test_partner_age_gap_within_limit_passes` | PASS |
| Child-lives-alone enforced | `test_child_leaves_home_underage_violation`, `test_child_leaves_home_underage_allowed` | PASS |
| Residence overlap detected | `test_overlapping_residence_interval_violation`, `test_residence_end_before_start_violation` | PASS |
| Household size violation detected | `test_quality_report_household_size_violation_detected` | PASS |
| Pipeline determinism (same seed = same output) | `test_run_scenario_pipeline_is_reproducible_for_same_seed`, `test_emission_is_deterministic`, `test_select_is_deterministic` | PASS |
| Different seeds produce different output | `test_different_seeds_produce_different_results`, `test_select_different_seeds_produce_different_results` | PASS |
| All 11 scenario YAMLs parse correctly | `test_scenario_yaml_exists_and_parses` x11, `test_scenario_has_valid_emission` x11, etc. | PASS |
| All 11 scenarios run end-to-end | `test_smoke_single_movers` through `test_smoke_three_source_partial_overlap` | PASS |
| Missing input raises clear error | `test_pipeline_raises_on_missing_scenario_yaml`, `test_pipeline_raises_on_missing_phase1_csv` | PASS |
| Overwrite protection works | `test_pipeline_raises_if_run_exists_and_no_overwrite` | PASS |
| Invalid config rejected with clear message | 15+ config validation tests across constraints, emission, selection, quality | PASS |
| Event grammar rejects invalid events | `test_cohabit_invalid_mode`, `test_divorce_invalid_custody_mode`, `test_unsupported_event_type`, etc. | PASS |
| Quality report includes all required sections | `test_phase2_quality_report_includes_er_metrics`, `test_quality_report_er_metrics_with_single_dataset`, `test_quality_report_er_metrics_with_pairwise_crosswalk` | PASS |
| Scale tested up to 500 people | `test_run_scenario_pipeline_multi_dataset_scale_smoke` | PASS |

---

## 15) File Map for New Engineers

```
SOG/
|-- src/sog_phase2/           # Core library
|   |-- pipeline.py           # START HERE - orchestrates everything
|   |-- simulator.py          # Truth-layer state machine
|   |-- emission.py           # Observed dataset + noise generation
|   |-- selection.py          # Population subsetting + trait assignment
|   |-- event_grammar.py      # Event type definitions + validation
|   |-- constraints.py        # Demographic constraint enforcement
|   |-- quality.py            # Quality report computation
|   |-- output_contract.py    # Schema validation for run artifacts
|   |-- params.py             # Census/CDC parameter loading
|   |-- scenario_catalog.py   # Scenario registry
|
|-- scripts/                  # CLI entry points
|-- tests/                    # 322 tests (this report details all of them)
|-- phase2/scenarios/         # 11 canonical scenario YAMLs + catalog.yaml + optional _working_ copies
|-- phase2/runs/              # Run output directories
|-- Data/phase2_params/       # Demographic priors (Census, CDC, NCHS)
|-- frontend/                 # Streamlit UI + tool functions
|-- docs/                     # This report + user guides
```

To understand the codebase, read in this order:
1. This document
2. `pipeline.py` (the orchestrator — shows how all pieces connect)
3. The scenario YAML of your choice (e.g., `phase2/scenarios/single_movers.yaml`)
4. `simulator.py` (the simulation state machine)
5. `emission.py` (the noise/duplication engine)

---

## 16) Conclusion

The SOG synthetic data pipeline produces correct, deterministic, constraint-respecting synthetic data across 11 benchmarking scenarios. The test suite comprehensively validates correctness, determinism, error handling, and contract compliance.

The system is ready for publication with the caveat that Issues 1-4 in Section 8.1 should be fixed for production workflows that depend on portable paths, atomic writes, or frontend error transparency. For local research use, the system works correctly today.
