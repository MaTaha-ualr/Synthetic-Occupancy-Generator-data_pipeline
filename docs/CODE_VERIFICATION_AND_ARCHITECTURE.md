# SOG Code Verification & Architecture Reference

_Last verified: 2026-06-22 against the working tree (`HEAD`)._

This document is a code-level companion to the existing prose docs (`SOG_BIBLE.md`,
`SOG_TECHNICAL_WALKTHROUGH.md`, the per-phase `README.md` files). Those describe
_how to operate_ SOG. This one records _what the code actually does, module by
module_, and the result of an end-to-end verification pass. It was produced by
reading every source module and running the full test suite.

---

## 1. Verification summary

| Check | Result |
|---|---|
| Full test suite (`python -m pytest -q`) | **395 passed** once `openpyxl` is installed (see note below). 0 logic failures. |
| Test runtime | ~140 s for the full suite. |
| Phase-1 unit/pipeline tests | Pass (multi-format output, redundancy, nicknames, name-duplication, uniqueness). |
| Phase-2 unit + integration + regression + all-scenarios smoke | Pass. |
| Frontend tests (`tests/`) | Pass. |
| Determinism | Every stochastic path is seeded (`np.random.default_rng(seed)`) or hashed (`hashlib.sha256`). Reproducible by construction. |

### The one environmental gotcha

On a fresh checkout the suite reports **1 failure**:

```
FAILED phase1/tests/test_phase1_pipeline.py::
  test_phase1_writes_selectable_text_and_excel_outputs[excel-.xlsx-xlsx]
E   ModuleNotFoundError: No module named 'openpyxl'
```

This is **not a code bug**. `openpyxl` is declared in `requirements.txt`
(`openpyxl>=3.1`) but was missing from the local `.venv`. The xlsx output path in
`phase1/src/sog_phase1/generator.py` requires it only when writing Excel. After
`pip install openpyxl`, that test passes and the suite is fully green. If you ever
see this failure, run `pip install -r requirements.txt` against the active venv.

---

## 2. What SOG is

SOG (Synthetic Occupancy Generator) is a **reproducible synthetic-data pipeline
for entity-resolution (ER) benchmarking**. It manufactures realistic but fully
synthetic person/household data together with a known ground truth, so ER and
record-linkage systems can be scored against an exact answer key.

It runs in two phases plus an optional frontend:

- **Phase 1** — generate a static baseline population of people + addresses.
- **Phase 2** — select a subset, simulate life events over time (the "truth"),
  then emit one or more noisy "observed" datasets plus the truth crosswalks that
  link observed records back to real entities.
- **Frontend** — a Streamlit "Benchmark Studio" with an Anthropic-powered chat
  assistant that drives the Phase-2 pipeline and visualizes results.

```
phase1 (baseline) ──> phase1/outputs_phase1/Phase1_people_addresses.csv
                                  │  (canonical baseline input)
                                  ▼
phase2 selection ─> truth simulation ─> observed emission ─> quality ─> validation
                                  │
                                  ▼
                       phase2/runs/<run_id>/  (truth + observed + crosswalks + reports)
```

---

## 3. Phase 1 — baseline population generator

Source: `phase1/src/sog_phase1/`. CLIs: `phase1/scripts/build_prepared.py`,
`phase1/scripts/generate_phase1.py`. Control file: `phase1/configs/phase1.yaml`.

### 3.1 `preprocess.py` — raw → prepared cache
Reads the raw reference CSV/JSON files (names, last names + ethnicity weights,
streets/cities/states, demographics, nicknames) with a lenient CSV reader
(`_read_csv_lenient`, tolerates BOM, ragged rows, blank lines) and writes a
normalized **prepared cache** of parquet/JSON files under `phase1/prepared/`
plus a `prepared_manifest.json`. Demographics parsing extracts gender, age-bin,
and ethnicity distributions and maps ethnicity labels to the last-name
ethnicity vocabulary. Run once per change to raw inputs.

### 3.2 `config.py` — config load + validation
`load_phase1_config` reads `phase1.yaml`; `validate_phase1_core` enforces every
invariant (counts, seed range, output format/aliases, Excel row ceiling, address
percentages and number ranges, name-duplication bounds, redundancy bounds and
the relationship `n_people·min ≤ n_records ≤ n_people·max`, nickname mode/usage,
fill rates, mailing styles incl. the legacy fallback, residence-date bounds).
`normalize_distribution` auto-normalizes any distribution to sum to 100 (or
errors if `auto_normalize` is off and it doesn't).

### 3.3 `redundancy.py` — records-per-entity allocation
`allocate_records_per_entity` distributes "extra" records across entities in one
of two shapes:
- `balanced` — round-robin over entities with remaining capacity.
- `heavy_tail` — power-law weighting (`1/rank^alpha`) so a few entities get many
  records (realistic for ER duplication stress tests).

### 3.4 `nicknames.py` — display-name substitution
Builds a weighted nickname catalog by gender category (female/male/unisex with a
priority fallback order) and picks a display first name per record/person. If a
nickname is used, `FirstNameType=NICKNAME`; the generator guarantees every person
with a nickname row also gets at least one `FORMAL` row (formal-backup rows).

### 3.5 `generator.py` — the main generator (≈1,680 lines)
The heart of Phase 1. Key components:

- **`AddressGenerator`** — generates collision-free addresses by treating
  (house-number × street × city × state) as a single integer space and walking it
  with a seed-derived **coprime stride + offset** (a full-period linear congruential
  walk). This guarantees unique residence addresses without storing a set.
  Apartments pack `units_per_building` units per building slot. Mailing addresses
  follow an "OHC PO BOX" model (blank or PO BOX, with deterministic box/route
  numbers and ZIP keep/shift logic), with a legacy "same-as-residence" model
  retained for back-compat.
- **Entity attribute draws** — gender, ethnicity, age-bin drawn with
  `_draw_exact_categories` (floor + largest-remainder so achieved counts match the
  target proportions almost exactly, with a randomized tie-break). First names are
  drawn from gender-conditioned weighted pools; last names from
  ethnicity-conditioned pools. DOB sampled within the age bin; SSN/phone are
  deterministic functions of `PersonKey`+seed (SSN avoids the invalid `666` area).
- **Forced name duplication** (`_apply_forced_name_duplicates`) — deliberately
  injects first-name/last-name/full-name collisions at configured rates, within
  compatible buckets (same gender / same ethnicity), to create realistic
  ambiguous-name ER challenges.
- **Redundancy expansion** — repeats entities into multiple records per the
  redundancy allocation, then appends nickname formal-backup rows.
- **Output** — chunked writing to CSV / TXT / XLSX / parquet-parts; all text is
  uppercased. Emits the dataset plus a **`.manifest.json`** (full run metadata +
  config snapshot) and a **`.quality_report.json`** (expected-vs-achieved
  distributions with tolerance checks, collision metrics, missingness, and
  uniqueness checks). For datasets ≤ `exact_uniqueness_check_max_rows`, uniqueness
  is verified exactly by re-reading the written file; above that it is asserted
  "deterministic by construction".

### 3.6 Output contract (Phase 1)
One row per record. Keys: `RecordKey` (unique row), `PersonKey` (entity, repeats
under redundancy), `EntityRecordIndex` (per-person index), `AddressKey` (unique
residence). Plus name surfaces (`Formal*` vs display `FirstName`/`FullName`),
demographics, residence + mailing address blocks, and residence dates.

---

## 4. Phase 2 — truth simulation + observed emission

Source: `phase2/src/sog_phase2/`. Orchestrated by `pipeline.py:run_scenario_pipeline`
(also exposed as standalone CLIs and via the frontend). Six stages:

```
1 load+configure → 2 selection → 3 truth simulation →
4 observed emission → 5 quality+manifest → 6 contract validation
```

### 4.1 `params.py` — source-backed priors
Loads the Phase-2 parameter package from `phase2/Data/phase2_params/` (ACS
mobility, CDC marriage/divorce, NCHS fertility, household-type shares, a priors
snapshot JSON, plus sources/manifest). Validates required files and columns.

### 4.2 `selection.py` — deterministic participant selection
Collapses Phase-1 rows into one entity view per `PersonKey` (representative +
latest record, records-per-entity → `RedundancyProfile`). Assigns three **latent
trait scores** (mobility, partnership, fertility) as age-cohort base rates plus a
**deterministic SHA-256 jitter** keyed on `(seed, salt, PersonKey)` — so traits
are reproducible and independent of row order. Applies filters (age/gender/
ethnicity/residence/redundancy/mobility-bucket), then a deterministic seeded
sample (`all`/`count`/`pct`). Emits a selection audit log incl. a SHA-256 checksum
of the selected key set.

### 4.3 `constraints.py` — realism constraints + novelty levers
`ConstraintConfig` carries marriage age floor, partner age-gap (scalar max or a
weighted distribution), fertility age range, and three **novelty switches**
(`allow_underage_marriage`, `allow_child_lives_alone`,
`enforce_non_overlapping_residence_intervals`) used to deliberately produce
unrealistic stress-test edge cases. `validate_constraints_against_truth` scans the
generated truth for violations (used both as a quality signal and a hard check).

### 4.4 `event_grammar.py` — the truth-event contract
Defines the 8 active event types — `MOVE, COHABIT, BIRTH, DIVORCE, LEAVE_HOME,
DEATH, NAME_CHANGE, ADOPTION` — their required/optional fields, enum modes
(`CohabitMode`, `CustodyMode`), and the full `truth_events` column set.
`validate_truth_events_dataframe` checks every event row against its signature.

### 4.5 `simulator.py` — event-driven truth simulation (≈1,710 lines)
A discrete-time, monthly-or-daily, event-driven simulator built around
`_SimulationState`, which tracks people, households, memberships, residence
intervals, active couples, and per-person current household/address.

- Seeds each entity from the Phase-1 baseline (optionally grouping young adults
  into roommate households for the `roommates_split` scenario).
- Per step, applies event handlers in a fixed order — deaths, name changes,
  divorces, cohabits, births, adoptions, leave-home, moves — each gated by a
  **per-step probability** derived from annual scenario rates
  (`_annual_to_step_probability`) and modulated by latent traits. A `locked` set
  ensures at most one event per person per step (determinism + no double-moves).
- Maintains referential and temporal integrity: memberships/residences are closed
  the day before a transition (`_previous_day`), empty households are closed,
  couples stay co-located, deceased people exit all structures.
- Emits five truth tables: `truth_people`, `truth_households`,
  `truth_household_memberships`, `truth_residence_history`, `truth_events`, plus a
  consistency-check block (non-overlapping intervals, couple co-location) and a
  `simulation_meta` recording the exact step probabilities used.

### 4.6 `emission.py` — observed-dataset generator (≈2,230 lines)
Turns truth into noisy observed records — the actual ER benchmark inputs.

- Builds a **snapshot** of each entity at the emission date (correct address from
  residence history, age on snapshot, applied name changes / cohabit name
  changes).
- Selects which entities appear in each dataset (appearance %, duplication %,
  optional fixed record counts), supporting **1..N datasets** plus a legacy
  pairwise A/B mode.
- Injects configurable **noise**: phonetic, OCR, typo, nickname substitution,
  date/DOB swaps + shifts, ZIP errors, SSN masking. Address fields are
  reconstructed from a known address book / synthesized deterministically.
- Produces: the per-dataset observed CSVs, an **`entity_record_map`** (observed
  record → true `PersonKey`), a **`master_dataset`**, a two-dataset
  `truth_crosswalk`, and **pairwise crosswalks** for every dataset pair — the
  answer keys ER systems are scored against.

### 4.7 `quality.py` — quality report
`compute_phase2_quality_report` aggregates truth consistency (event-age validity,
time-overlap errors, household-size constraints), observed coverage, and
cross-file match metrics into the run's `quality_report.json`. `_compute_quality_status`
(in `pipeline.py`) reduces this to `ok` / `quality_issues_detected`.

### 4.8 `output_contract.py` — schemas, run IDs, validation
Defines canonical run-id format (`YYYY-MM-DD_<scenario_id>_seed<seed>`), the
expected artifact paths, and the per-file output schemas. `validate_phase2_run`
checks every expected artifact exists with the required columns — the final gate
in the pipeline.

### 4.9 Support modules
- `progress.py` — lightweight 6-step progress reporter (CLI/frontend).
- `run_readme.py` — writes a human-readable `README.md` into each run folder.
- `scenario_catalog.py` — loads/queries `phase2/scenarios/catalog.yaml`
  (status counts, lookup by id), cached with `lru_cache`.

### 4.10 Scenarios
`phase2/scenarios/*.yaml` are the canonical, source-controlled scenario configs
(e.g. `single_movers`, `couple_merge`, `family_birth`, `divorce_custody`,
`roommates_split`, `clean_baseline_linkage`, `high_noise_identity_drift`,
`high_duplication_dedup`, `low_overlap_sparse_coverage`,
`asymmetric_source_coverage`, `three_source_partial_overlap`, plus
adoption/name-change/death). Each declares phase1 inputs, selection, simulation,
constraints, parameters (event rates), emission, and quality. `catalog.yaml`
tracks which are runnable vs planned.

---

## 5. Frontend — Streamlit Benchmark Studio

Source: `frontend/`. Launch: `run_frontend.ps1` → `streamlit run
frontend/chatbot_production.py`, which `runpy`-executes `frontend/chatbot.py`
(single real app; the production wrapper exists so Streamlit reruns don't get
stuck on module cache).

- `chatbot.py` — the live app (UI, chat loop, custom CSS "Benchmark Studio"
  theme). `chatbot_v2.py` / `chatbot_clean.py` are alternate/earlier variants.
- `pipeline_bridge.py` — thin **synchronous** wrapper over
  `sog_phase2.pipeline.run_scenario_pipeline` (runs in-process inside an
  `st.spinner`; no job queue).
- `async_runner.py`, `session_manager.py`, `presets.py` — run management, session
  state, scenario presets.
- `agents/` — an Anthropic-tool-using assistant split into `orchestrator.py`
  routing to `config_agent`, `run_agent`, `analyst_agent`, `export_agent` (all
  extending `base.py`).
- `sog_tools.py` — the tool surface the agents call to inspect scenarios, launch
  runs, and read results.
- `visualizations/` — Plotly/matplotlib charts (`demographics`, `difficulty`,
  `quality`) with a shared `theme.py`.

Requires `ANTHROPIC_API_KEY` (read via `.env`/`python-dotenv`) for the chat
assistant; the pipeline itself runs without it.

---

## 6. Determinism & reproducibility (verified)

- Every random draw uses `np.random.default_rng(seed)` seeded from the scenario/
  config seed; trait assignment and several tie-breaks use `hashlib.sha256` keyed
  on `(seed, salt, key)`. Re-running a run id reproduces byte-for-byte outputs.
- Sorting is stable (`kind="mergesort"`) and key-normalized
  (`_stable_key` treats numeric PersonKeys numerically), so output ordering is
  independent of input row order.
- The Phase-1 address walk uses a seed-derived coprime stride for guaranteed-unique
  addresses without state.

---

## 7. Observations (non-blocking)

These are notes for future maintainers, not defects — nothing here breaks tests or
correctness:

1. **`openpyxl` install drift** (see §1). Keep the venv synced with
   `requirements.txt`; consider a CI step that fails on missing declared deps.
2. **Heavy-tail allocation cost** — `redundancy._allocate_heavy_tail` calls
   `rng.choice` once per extra record (O(extra · entities)). Fine at the default
   scale (10k people); could be vectorized if much larger runs are needed.
3. **Row-wise simulation/emission loops** — `simulator.py` and `emission.py`
   iterate rows in Python for clarity and exact determinism. Correct and fast
   enough for current scenario sizes; the main lever if very large populations are
   ever simulated.
4. **Multiple frontend chatbot variants** (`chatbot.py`, `chatbot_v2.py`,
   `chatbot_clean.py`) coexist. `chatbot.py` is the live one (per the README and
   `chatbot_production.py`); the others are kept for reference and could be archived
   to reduce confusion.

---

## 8. How to re-run this verification

```powershell
# from repo root, with the venv active
pip install -r requirements.txt          # ensures openpyxl etc. are present
python -m pytest -q                        # full suite — expect all green

# end-to-end smoke of one scenario (after building the Phase-1 baseline)
python phase2/scripts/run_phase2_pipeline.py --scenario phase2/scenarios/single_movers.yaml
```

The Phase-2 integration, all-scenarios smoke, and scenario-regression tests
already exercise selection → simulation → emission → quality → validation
end-to-end, so a green `pytest` run is itself a full functional verification.
