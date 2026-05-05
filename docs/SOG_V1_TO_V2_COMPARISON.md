# SOG Version 1 to Version 2 Comparison

Last updated: April 5, 2026

This document is the engineering comparison between the original SOG paper-era system ("Version 1") and the current repository implementation ("Version 2"). It is meant to answer four practical questions:

1. What did the original Version 1 system actually do?
2. What does Version 2 now support in the repository?
3. In what ways is Version 2 materially stronger?
4. Which Version 1 ideas are still deferred, narrowed, or not first-class in Version 2?

This is intentionally separate from the user guides. The user guides explain how to run SOG. This memo explains how the system evolved.

## 1) Purpose and Sources

This memo uses only two evidence sources:

- the original ICIQ-09 SOG paper (referenced externally; not committed to this repository)
- the current repository contracts, code, and active documentation

For Version 2, the main repository-backed sources are:

- `docs/SOG_COMPLETE_USER_GUIDE.md`
- `docs/ENGINEERING_TEST_AND_READINESS_REPORT.md`
- `docs/SCENARIO_SUPPORT_MATRIX.md`
- `phase2/scenarios/catalog.yaml`
- `src/sog_phase2/event_grammar.py`
- `src/sog_phase2/emission.py`
- `src/sog_phase2/output_contract.py`

The standard used in this memo is conservative:

- if the paper described it, Version 1 gets credit for it
- if the active contract, event grammar, scenario catalog, or shipped repo surface does not support it today, Version 2 does not get credit for it

---

## 2) Executive Summary

Version 1 was a strong research prototype centered on generating realistic occupancy histories for ER instruction and research. Its main strength was conceptual clarity:

- internal identity versus external identity
- occupancy histories over time
- Single and Couple scenario logic
- realistic name, address, phone, SSN, and PO Box structure
- a clear plan for later disruption into ER test data

Version 2 is a much broader and more operational system. It is no longer just an occupancy-history generator. It is now a deterministic synthetic benchmarking pipeline with:

- a Phase 1 baseline generator
- a Phase 2 truth simulator
- a configurable observed-emission engine
- single-dataset, pairwise, and N-way benchmark modes
- configurable linkage cardinalities
- quality reporting
- output validation
- regression-tested scenario packaging

The right engineering conclusion is:

- Version 2 is substantially stronger for real ER benchmarking work
- Version 2 is not a strict feature-by-feature superset of every Version 1 occupancy-detail semantic
- the remaining gaps are identifiable and narrow rather than architectural confusion

The clearest Version 1 capabilities that Version 2 still owes are:

- first-class PO Box occupancy output
- first-class `DEATH` lifecycle simulation
- first-class `NAME_CHANGE` lifecycle simulation
- an observed-layer export model that mirrors full longitudinal occupancy histories rather than snapshot-based benchmark exports

---

## 3) What Version 1 Actually Was

### 3.1 Core framing

The original paper presents SOG as a Java-based Synthetic Occupancy Generator built to support entity resolution instruction and research. The problem statement is clear: real occupancy data is hard to share because of privacy concerns, yet ER systems need realistic benchmark data with known truth.

The paper's central idea is that SOG creates a realistic internal view of identity that can later be disrupted into an external ER view. That internal view is made of occupancy histories, meaning chronologically consecutive records for the same person across names and addresses over time.

### 3.2 Identity model

Version 1 is built around the contrast between:

- an internal view of identity: the complete, correct occupancy history
- an external view of identity: the fragmented and uncertain set of records seen by an ER system

This was a strong conceptual foundation. It directly motivated why SOG existed: to create synthetic truth that could later be stripped of its obvious identifiers and used as an ER benchmark.

### 3.3 Data content and seed preparation

The paper describes Version 1 as preparing seed data from real name and address samples and then generating synthetic identities and occupancies from those seeds.

The paper explicitly discusses:

- parsed first, middle, last, and suffix name tables
- street-address parsing into structured components
- telephone number generation tied to geographic locality
- PO Box parsing and PO Box output fields
- unique identity creation with SSN and DOB

The original output model is detailed and concrete. The paper says an occupancy record had 30 items, including:

- occupancy identifiers
- history sequence identifiers
- identity number
- name parts
- SSN
- DOB
- single/couple status
- structured street-address fields
- telephone number
- occupancy start and end dates
- PO Box fields

That means Version 1 was not just a toy generator. It had a fairly rich occupancy-record schema.

### 3.4 Scenario logic

The paper describes two scenario families:

- `Single`
- `Couple`

The `Single` scenario represented one person's consecutive occupancy history over time.

The `Couple` scenario included multiple narrative variants:

- two people begin with separate occupancies
- they later share occupancies
- they may stay together to the end of the simulation
- they may separate before the end of the simulation
- the shared occupancy may end because one partner dies

The paper also embeds name-transition logic into this narrative, especially around female surname changes during couple formation and separation.

### 3.5 What Version 1 did not yet deliver

The paper is explicit that disruption into ER test files was the next phase of work, not the fully delivered scope of the paper itself.

It discusses:

- splitting occupancy histories into files
- removing identity keys
- producing change-of-address records
- preserving enough inferred or asserted association to allow re-linking

But the paper describes those as future or current research direction, not as the fully operational system already shipped in that publication.

This point matters. Version 1 was strong as a generator and benchmark concept, but it did not yet deliver the full modern benchmark pipeline that Version 2 now provides.

---

## 4) What Version 2 Is Now

Version 2 is a repository-backed synthetic data platform with a much more explicit architecture than the paper-era Version 1.

### 4.1 High-level architecture

Version 2 is split into two major phases:

- Phase 1: baseline person and address generation
- Phase 2: truth simulation and observed emission for ER benchmarking

The current pipeline structure is:

1. build a baseline population
2. select a scenario population
3. simulate truth events and truth state tables
4. emit one or more observed datasets
5. compute quality metrics
6. validate the output contract

This architecture is documented in `docs/SOG_COMPLETE_USER_GUIDE.md` and `docs/ENGINEERING_TEST_AND_READINESS_REPORT.md`.

### 4.2 Phase 1 in Version 2

Phase 1 in Version 2 produces the canonical baseline used by Phase 2. It creates synthetic person and address records with:

- names
- demographics
- SSN
- phone
- address structure

The baseline is not the final benchmark output. It is the seed population from which Phase 2 selects and simulates.

That is a major architectural difference from Version 1. In Version 2, baseline generation and benchmark simulation are cleanly separated.

### 4.3 Phase 2 truth layer in Version 2

Phase 2 truth simulation is where Version 2 moves well beyond the paper-era system.

The truth layer produces normalized parquet tables for:

- people
- households
- household memberships
- residence history
- events
- scenario population

The active truth event grammar in `src/sog_phase2/event_grammar.py` currently supports:

- `MOVE`
- `COHABIT`
- `BIRTH`
- `DIVORCE`
- `LEAVE_HOME`

This means Version 2 is no longer limited to Single and Couple occupancy narratives. It can simulate a broader family and household event surface.

### 4.4 Phase 2 observed layer in Version 2

Version 2 also has a fully delivered observed-emission layer. This is one of the biggest improvements over Version 1.

The observed layer supports:

- one observed file for deduplication
- two observed files for pairwise linkage
- three or more observed files for N-way linkage

It also supports configurable linkage cardinalities:

- `single_dataset`
- `one_to_one`
- `one_to_many`
- `many_to_one`
- `many_to_many`

This is defined in `src/sog_phase2/emission.py`.

### 4.5 Noise and disruption in Version 2

Unlike the paper-era system, Version 2 has an active observed-noise surface. The current emission schema supports independent control over multiple noise types per dataset, including:

- `name_typo_pct`
- `dob_shift_pct`
- `ssn_mask_pct`
- `phone_mask_pct`
- `address_missing_pct`
- `middle_name_missing_pct`
- `phonetic_error_pct`
- `ocr_error_pct`
- `date_swap_pct`
- `zip_digit_error_pct`
- `nickname_pct`
- `suffix_missing_pct`

This is a concrete operational realization of the disruption concept that Version 1 mainly framed as future work.

### 4.6 Output packaging in Version 2

Version 2 packages runs under `phase2/runs/<run_id>/` and writes a formal artifact set that includes:

- truth parquet outputs
- observed dataset CSVs
- `entity_record_map.csv`
- `truth_crosswalk.csv` for pairwise compatibility
- per-pair crosswalks for N-way linkage
- `quality_report.json`
- `manifest.json`
- `scenario.yaml`
- `scenario_selection_log.json`

This is enforced by `src/sog_phase2/output_contract.py`.

### 4.7 Validation and test surface in Version 2

Version 2 also includes:

- an output validator
- a scenario catalog
- regression-tested shipped scenarios
- documented readiness and engineering status

The active readiness report says the system has a large passing test suite and treats validation and regression as first-class engineering requirements. That is a major maturity improvement over the paper-era prototype framing.

---

## 5) Version 1 vs Version 2 by Dimension

| Dimension | Version 1 | Version 2 | Assessment |
|---|---|---|---|
| Main implementation shape | Java research system described in paper | Python repository pipeline with modules, scripts, contracts, and tests | Version 2 is broader and more operational |
| Primary conceptual unit | occupancy history | baseline + truth layer + observed layer | Version 2 is more structured |
| Identity framing | internal view vs external view | truth vs observed with canonical mapping artifacts | Version 2 preserves the core idea and operationalizes it |
| Baseline generation | seed-based synthetic identity and occupancy generation | separate Phase 1 synthetic baseline plus Phase 2 simulation | Version 2 has stronger layering |
| Scenario surface | Single and Couple | 11 canonical scenarios plus parameterized custom families | Version 2 is much broader |
| Truth events | occupancy moves and couple narratives | explicit event grammar for move, cohabit, birth, divorce, leave-home | Version 2 is broader on active family dynamics |
| Disruption into benchmark data | future work in the paper | active observed emission with overlap, duplication, noise, and mappings | Version 2 is substantially stronger |
| Topology | split-file idea | single-dataset, pairwise, N-way | Version 2 is substantially stronger |
| Cardinality control | not formalized as a shipped benchmark contract | one-to-one, one-to-many, many-to-one, many-to-many, dedup | Version 2 is substantially stronger |
| Artifact contract | one occupancy-record output model in the paper | normalized truth outputs plus observed outputs plus validator-backed metadata | Version 2 is substantially stronger |
| Reproducibility packaging | conceptual simulation parameters | run-id packaging, manifest, scenario log, validator | Version 2 is much stronger |
| Testing posture | research paper, not a repo-level test contract | regression and readiness-oriented engineering surface | Version 2 is much stronger |
| PO Box support | explicit output fields | not exposed in active observed contract | Version 1 still has an advantage here |
| Death lifecycle | explicitly described in Couple variant | deferred as optional-later event | Version 1 still has an advantage here |
| Name-change lifecycle | embedded in Couple narrative | deferred as optional-later event | Version 1 still has an advantage here |
| Observed longitudinal export | occupancy histories themselves are the main output | observed layer is snapshot-based; full longitudinal history lives in truth tables | architectural divergence rather than simple upgrade |

---

## 6) What Version 2 Supports Today

This section exists to make the document clearer about the actual ground covered by Version 2, not just the differences from Version 1.

### 6.1 Shipped canonical scenarios

According to `phase2/scenarios/catalog.yaml`, Version 2 currently ships these canonical scenarios:

| Scenario | Primary intent | Topology | Cardinality | Primary events |
|---|---|---|---|---|
| `single_movers` | address-change linkage benchmark | pairwise | `one_to_one` | `MOVE` |
| `clean_baseline_linkage` | low-noise baseline sanity check | pairwise | `one_to_one` | `MOVE` |
| `couple_merge` | household formation benchmark | pairwise | `one_to_many` | `COHABIT` |
| `family_birth` | household growth benchmark | pairwise | `many_to_one` | `BIRTH` |
| `divorce_custody` | family split and custody ambiguity benchmark | pairwise | `many_to_many` | `DIVORCE`, `COHABIT` |
| `roommates_split` | roommate churn and shared-address ambiguity benchmark | pairwise | `one_to_many` | `LEAVE_HOME`, `MOVE` |
| `high_noise_identity_drift` | severe field-level corruption benchmark | pairwise | `one_to_one` | `MOVE` |
| `low_overlap_sparse_coverage` | weak-overlap linkage benchmark | pairwise | configurable | `MOVE` |
| `asymmetric_source_coverage` | broad-versus-sparse source benchmark | pairwise | configurable | `MOVE` |
| `high_duplication_dedup` | single-file dedup benchmark | single-dataset | `dedup` | background move context only |
| `three_source_partial_overlap` | three-source linkage benchmark | N-way | configurable | `MOVE` |

### 6.2 Supported parameterized scenario families

Version 2 also supports benchmark families that are not only fixed named YAMLs. The engine supports:

- `custom_single_dataset_dedup`
- `custom_pairwise_linkage`
- `custom_multi_dataset_linkage`

That means end users can edit topology, dataset count, overlap, duplication, dataset labels, and match mode without waiting for a new hardcoded scenario.

### 6.3 Supported benchmark topologies

Version 2 currently supports three output shapes:

- single-file deduplication benchmarks
- pairwise two-file linkage benchmarks
- N-way linkage benchmarks with three or more datasets

This is one of the clearest areas where Version 2 exceeds the paper-era system.

### 6.4 Supported benchmark cardinalities

Version 2 currently supports:

- `one_to_one`
- `one_to_many`
- `many_to_one`
- `many_to_many`
- dedup

This is a major operational capability that Version 1 did not ship as a generalized benchmark contract.

### 6.5 Active truth and observed artifacts

Version 2 now has two distinct artifact planes.

The truth plane contains:

- `truth_people.parquet`
- `truth_households.parquet`
- `truth_household_memberships.parquet`
- `truth_residence_history.parquet`
- `truth_events.parquet`
- `scenario_population.parquet`

The observed plane contains:

- one or more observed dataset CSVs
- `entity_record_map.csv`
- `truth_crosswalk.csv` for pairwise runs
- per-pair `truth_crosswalk__<left>__<right>.csv` files for N-way runs

This truth/observed separation is one of the strongest architectural improvements in Version 2.

### 6.6 Active observed field surface

The current active observed contract includes full street-address detail columns such as:

- `HouseNumber`
- `StreetName`
- `UnitType`
- `UnitNumber`
- `StreetAddress`
- `City`
- `State`
- `ZipCode`

It also includes identity and contact fields such as names, DOB, SSN, phone, snapshot date, and source system fields.

That means Version 2 is not opaque or key-only. It produces benchmark-facing records with real ER-relevant fields.

---

## 7) Detailed Improvements from Version 1 to Version 2

### 7.1 From a generator to a benchmark platform

Version 1 was primarily a generator plus a research vision for disruption and re-linking. Version 2 is a full benchmark platform.

The difference is important:

- Version 1 generated the source histories
- Version 2 generates source histories, emits benchmark datasets, validates them, and packages the answer keys

That is the single largest architectural advance.

### 7.2 From two scenario families to a scenario catalog

Version 1 gave users Single and Couple narratives. Version 2 gives users a catalog that covers:

- simple address change
- household formation
- household growth
- household split and custody ambiguity
- roommate churn
- heavy identity drift
- weak overlap
- asymmetric coverage
- single-file dedup
- three-source linkage

This moves SOG from "interesting synthetic data generator" to "explicit ER benchmark product surface."

### 7.3 From loosely described disruption to configurable observed emission

The paper described splitting and disruption as the next phase. Version 2 actually implements:

- dataset appearance control
- overlap control
- within-file duplication control
- per-dataset noise control
- dataset-level snapshot selection
- pairwise and N-way truth projections

This is where Version 2 most clearly surpasses Version 1 in practical usefulness.

### 7.4 From narrative histories to normalized truth state

Version 1 was history-centric. Version 2 is state-centric and event-centric.

That means Version 2 keeps explicit normalized truth tables for:

- people
- households
- memberships
- residence intervals
- event records

This is better for testing, validation, and extending the simulator, because the benchmark no longer depends on one flat record format to carry every meaning.

### 7.5 From prototype output to contract-enforced packaging

Version 2 has explicit run packaging, file requirements, and validation rules. That makes it better suited to repeated engineering use and CI-style regression protection.

This matters because benchmark systems break in subtle ways when artifact structure drifts. Version 2 actively guards against that.

### 7.6 From implicit realism to source-backed priors

Version 2 introduces a more explicit parameter layer using public-source priors for mobility, partnership, fertility, and household behavior. This is documented in the active user and readiness docs.

That means realism in Version 2 is not only a narrative goal. It is also tied to explicit parameter tables and scenario controls.

---

## 8) Capabilities Present in Version 1 but Not Fully Present in Version 2

This is the section that keeps the document honest. Version 2 is stronger overall, but it does not reproduce every Version 1 detail as an active first-class feature.

### 8.1 PO Box handling

The Version 1 paper explicitly includes:

- PO Box indicator
- PO Box ID
- PO Box number
- PO Box city
- PO Box state
- PO Box ZIP

The active Version 2 observed contract does not expose an equivalent PO Box field set. Its active observed address contract is centered on street-address style columns.

So the right statement is:

- Version 1 explicitly modeled PO Box occupancy output
- Version 2 does not currently expose PO Box occupancy as a first-class observed-output contract

This should be treated as a real gap, but a narrow one.

### 8.2 Death-ending couple behavior

The paper's Couple scenario explicitly includes a path where shared occupancy ends due to the death of one partner.

In Version 2:

- `DEATH` exists only in `OPTIONAL_LATER_EVENT_TYPES`
- it is not active in the current event grammar
- the scenario catalog marks death-oriented scenarios as future engine extension work

So Version 2 has not yet reintroduced this lifecycle behavior as an active supported simulation path.

### 8.3 First-class name-change lifecycle

Version 1 embeds name transition into the occupancy-history narrative, especially during couple formation and separation.

In Version 2:

- `NAME_CHANGE` exists only as an optional-later event type
- there is no active shipped name-change scenario
- observed name drift today is better understood as noise and household ambiguity rather than a complete legal or social name-change lifecycle model

So Version 2 does not yet offer a first-class name-change truth model that fully corresponds to the paper's narrative logic.

### 8.4 Observed longitudinal history export

This is a subtler but important difference.

Version 1's main output was an occupancy-history record stream over time. In Version 2:

- the truth layer still preserves longitudinal history through residence intervals, membership intervals, and event tables
- but the observed benchmark layer is snapshot-based

That is not necessarily a weakness. For ER benchmarking, snapshot-based observed exports are often the right design. But it is still a real divergence from the Version 1 output model.

The best phrasing is:

- Version 2 preserves longitudinal truth
- Version 2 does not export the observed layer primarily as a full occupancy-history stream

---

## 9) Net Assessment: Why Version 2 Is Stronger

Version 2 is clearly better for actual ER benchmarking work.

It is stronger because it turns the Version 1 idea into a concrete engineering system:

- baseline generation
- scenario-driven truth simulation
- explicit observed emission
- configurable benchmark topology
- configurable benchmark cardinality
- formal answer keys
- quality reporting
- contract validation
- regression coverage

Version 2 is also broader in supported user intent. A user can now ask for:

- one-file dedup
- pairwise linkage
- three-source linkage
- high-noise identity drift
- household formation ambiguity
- family-growth ambiguity
- divorce/custody ambiguity
- weak-overlap stress
- asymmetric source coverage

That is a much stronger benchmark product surface than the paper-era system.

The correct overall classification is:

- Version 2 is a substantial functional superset for ER benchmarking workflows
- Version 2 is not a perfect semantic superset of every occupancy-specific behavior described in Version 1

That is a healthy state for the project. It means the architecture improved materially, and the remaining historical gaps are specific roadmap items rather than uncertainty about system direction.

---

## 10) Current Known Gaps and Future Work

The clearest historically grounded future work items are:

- first-class `DEATH` lifecycle simulation
- first-class `NAME_CHANGE` lifecycle simulation
- first-class PO Box support in the active observed-output contract

The repository already captures future engine-extension scenario families for:

- `name_change_lifecycle`
- `death_survivor_persistence`
- `adoption_blended_family`

Those appear in:

- `docs/SCENARIO_SUPPORT_MATRIX.md`
- `phase2/scenarios/catalog.yaml`

If those are implemented, Version 2 would move closer to being not only stronger than Version 1 in architecture and benchmarking surface, but also closer to reproducing the full occupancy-history semantics that the original paper described.

---

## 11) Final Conclusion

Version 1 made the core case for SOG. It defined the problem correctly and introduced the right conceptual framing:

- occupancy histories matter for ER
- internal truth and external ER views are different
- synthetic benchmarks are necessary when real occupancy data cannot be shared

Version 2 takes that foundation and turns it into an engineering-grade benchmarking pipeline.

Compared with Version 1, Version 2 is:

- more modular
- more testable
- more benchmark-oriented
- more configurable
- more reproducible
- more explicit about outputs and validation
- much broader in scenario and topology support

The remaining missing pieces are real, but they are focused:

- PO Box output parity
- death lifecycle parity
- name-change lifecycle parity
- possible future expansion of observed longitudinal export modes

So the best final summary is:

- Version 1 was the conceptual and research foundation
- Version 2 is the operational ER benchmark system
- Version 2 is clearly better overall
- Version 2 still has a few historically meaningful features to reclaim from Version 1
