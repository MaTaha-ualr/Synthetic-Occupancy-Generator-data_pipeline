# SOG Technical Walkthrough

## Purpose

This document is the technical stakeholder walkthrough for SOG as it exists in the current repository.

It is intentionally different from:

- `docs/ENGINEERING_TEST_AND_READINESS_REPORT.md`
- `docs/SOG_V1_TO_V2_COMPARISON.md`
- `docs/SOG_COMPLETE_USER_GUIDE.md`

Those documents are still useful, but they answer different questions:

- the readiness report is about engineering status, test coverage, and production posture
- the comparison memo is about Version 1 versus Version 2
- the user guide is about operating the system

This walkthrough is for engineers and technical stakeholders who want the full story of the current system:

- what problem SOG solves
- how the system is architected
- how the modules work together
- what is genuinely novel in the engineering
- how the current system differs from the original paper-era design

## 1) Executive Thesis

SOG is no longer just a synthetic occupancy generator.

In its current form, it is a deterministic synthetic benchmarking platform for entity resolution, built around one core engineering idea:

1. generate a believable underlying world
2. simulate how that world changes over time
3. emit how different systems would observe that world imperfectly
4. preserve the answer key in a form that can be audited and scored exactly

That separation matters.

Many synthetic-data tools generate only flat rows. SOG instead separates:

- baseline population construction
- truth-layer simulation
- observed-layer corruption and coverage
- evaluation artifacts and contract validation

That is the main architectural leap from the original paper-era SOG to the current repository implementation.

## 2) The Problem SOG Solves

Entity resolution systems must decide which records refer to the same real person across one file or many files.

In real operational data, that problem is hard because identity is not static:

- people move
- households merge and split
- children appear
- records are duplicated
- systems cover different populations
- names, dates, addresses, and identifiers are incomplete or corrupted

Real data cannot usually be distributed for benchmarking because of privacy, compliance, and operational risk.

Traditional fake-data generators are not enough because they often miss one or more of the following:

- time
- household structure
- cross-system asymmetry
- realistic corruption modes
- exact ground truth

SOG exists to close that gap. It produces synthetic data that is useful not just for demonstration, but for actual benchmarking, debugging, and regression testing of ER systems.

## 3) The SOG Journey End to End

The cleanest way to understand SOG is as a journey through layered transformations. The important point is that each layer changes the level of abstraction:

- prepared assets turn raw seed material into canonical lookup inputs
- Phase 1 turns lookup inputs into a starting synthetic population
- selection turns a population into a scenario-specific cohort
- simulation turns a cohort into a hidden longitudinal world
- emission turns that world into one or more imperfect operational views
- packaging turns those views into a governed benchmark artifact

```text
Raw source tables and prepared caches
        ->
Phase-1 baseline population
        ->
Phase-2 scenario selection
        ->
Phase-2 truth simulation
        ->
Observed dataset emission
        ->
Quality reporting and contract validation
```

Each layer exists because it answers a different engineering question.

### 3.1 Layer A: Prepared Inputs

Before SOG generates anything, it prepares seed assets such as:

- first names
- last names
- street names
- cities
- states
- demographics
- nicknames

This layer is easy to overlook, but it is one of the reasons the rest of the system remains tractable.

Its job is to ensure that later stages are consuming canonical lookup material rather than raw CSV quirks. In practical terms, this means:

- source parsing is separated from generation logic
- the generator works from clean lookup tables instead of mixed-format input files
- name and address assets can be audited independently of simulation behavior
- problems in the source package do not have to be debugged inside the benchmark engine

This is the first signal that SOG is engineered as a pipeline rather than as one monolithic script.

### 3.2 Layer B: Phase-1 Baseline Generation

Phase 1 creates the starting synthetic population.

Its job is to answer:

- who exists?
- what stable identity attributes do they have?
- what administrative-style rows represent them at the beginning?

The baseline output includes fields such as:

- names
- demographic attributes
- SSN
- phone
- address structure
- residence dates

Phase 1 also supports controlled row redundancy:

- one person can have one row
- or one person can have several rows

That matters because many administrative systems do not present one person as one clean pristine row. Even before Phase 2 adds household and cross-system difficulty, the baseline can already encode the fact that one entity may correspond to multiple administrative records.

In the current architecture, Phase 1 is not the final benchmark. It is the seed population from which Phase 2 chooses participants and begins simulation.

That is a major design choice. The baseline is the starting world, not the benchmark outcome.

Another important point for technical reviewers is what Phase 1 does not currently do. It does not act as the main household-behavior engine. In the current design:

- one person can have multiple baseline records and multiple residence rows
- different people do not generally share the same baseline residence address
- shared households and co-residence are introduced later by Phase 2 events such as `COHABIT`, `BIRTH`, `DIVORCE`, and roommate grouping

This keeps Phase 1 simpler and makes Phase 2 the place where relationship-driven ambiguity is deliberately created.

### 3.3 Layer C: Scenario Selection

Phase 2 does not simulate on the entire Phase-1 file by default. It first creates a scenario population.

That step does more than filter rows. It changes the data model from record-centric to entity-centric and attaches behavioral priors to the entities that survive selection.

At a high level, selection does five things:

1. collapse Phase-1 rows into an entity view
2. retain summary attributes such as age bin, gender, ethnicity, residence type, and redundancy profile
3. assign deterministic latent propensities
4. filter and sample deterministically
5. emit an audit trail that explains what happened

The latent propensity layer is one of the more important engineering ideas in the repository.

Each selected person gets stable scores such as:

- mobility propensity
- partnership propensity
- fertility propensity

These scores are not random in the fragile sense of "depends on what happened before in the loop."

They are deterministic per person and per seed. The implementation uses a stable hash keyed by `(seed, salt, PersonKey)` to generate bounded per-entity jitter, then combines that jitter with age-linked base priors. That gives SOG stable heterogeneity:

- two runs with the same seed produce the same person-level tendencies
- adding other people to the baseline does not reshuffle everyone else's latent traits
- the simulator receives entity-specific behavioral priors without losing reproducibility

This is how SOG avoids the common failure mode where reproducibility collapses as soon as population size changes.

### 3.4 Layer D: Truth Simulation

Once a scenario population is selected, SOG simulates the hidden world.

This truth layer is normalized into separate tables, including:

- truth people
- truth households
- truth household memberships
- truth residence history
- truth events

This is another major architectural decision.

Instead of storing only row snapshots, SOG stores the evolving world as state plus events. In practice, that means the simulator is maintaining:

- the current active household state
- the current active residence state
- membership intervals
- historical ledgers for everything that has changed
- event records that describe why a change happened

That makes it possible to answer questions like:

- which household was this person in at a given time?
- when did the household form?
- when did an address change happen?
- was a child created before or after a move?

The current truth event grammar includes:

- `MOVE`
- `COHABIT`
- `BIRTH`
- `DIVORCE`
- `LEAVE_HOME`

The simulator steps through time with configurable granularity, usually monthly, and converts annual rates into per-step probabilities. This is a subtle but important point:

- scenario authors think in annual rates
- the simulator executes in discrete steps

SOG bridges that gap mathematically rather than hand-waving it. It does not simply divide by twelve and call that good enough. It applies compounding logic so that annual priors are translated correctly into monthly or daily event chances.

The simulator also imposes event ordering and per-step locking so the world does not fall into impossible same-step behavior. At a conceptual level, the loop is doing things like:

1. resolve divorces before new coupling
2. resolve cohabitation before later move logic
3. resolve births only for currently valid household contexts
4. resolve leave-home events for eligible members
5. resolve moves for the remaining eligible people or households

and once someone has already had a conflicting event in the step, they can be locked out of incompatible additional events in the same interval

That is why the truth layer remains interpretable even when the scenarios become complex.

### 3.5 Layer E: Observed Emission

The truth layer is still not what downstream ER systems would see.

Observed emission is the stage where SOG creates one or more dataset files that behave like operational systems.

This layer controls:

- which entities appear in which datasets
- how much overlap exists across datasets
- how much duplication exists within datasets
- which snapshot each dataset represents
- how much field-level corruption each dataset contains

This is where the benchmark becomes realistic.

Two systems can now disagree because:

- they do not cover the same population
- they capture different points in time
- they duplicate different subsets of people
- they corrupt different fields at different rates

Internally, emission does not just "write CSVs from truth." It first reconstructs snapshot views of the truth world for each requested dataset date, then decides:

- which entities are eligible to appear on that snapshot
- which entities will actually appear in each dataset
- which entities belong to the overlap core versus side-specific coverage
- how many records each entity will produce in each dataset
- which field-level perturbations will be applied after projection

The emission engine supports multiple benchmark topologies:

- single-dataset deduplication
- pairwise linkage
- N-way linkage

It also supports multiple cross-file cardinality modes:

- `single_dataset`
- `one_to_one`
- `one_to_many`
- `many_to_one`
- `many_to_many`

That is not just packaging. It means the benchmark topology itself is now a first-class part of the system design.

It also means that dataset shape is deliberate. A one-to-many scenario is not merely a high-duplication accident. It is achieved through the record-allocation logic and later reflected in the generated crosswalks.

### 3.6 Layer F: Quality, Manifest, and Contract Validation

At the end of the run, SOG does not just dump files.

It writes a complete run package including:

- observed CSVs
- truth parquet tables
- entity-to-record map
- crosswalks
- manifest
- quality report
- resolved scenario YAML
- selection log

Then it validates the artifact contract.

This layer matters because benchmark systems need to be explainable after the fact. The `quality_report.json` captures what the run actually achieved, while the validator ensures the run directory is structurally complete and internally consistent.

This is one of the strongest engineering signals in the repository:

- outputs are not implicit
- run structure is not accidental
- the answer key is not hidden in ad hoc internal state

SOG treats a scenario run as a governed artifact package, not just as generated data.

## 4) Why The Module Split Matters

The current codebase is split by responsibility rather than by script history. That matters because SOG is not difficult in only one way. It has four different kinds of engineering difficulty:

1. baseline population construction
2. scenario cohort formation
3. longitudinal truth simulation
4. observed benchmark projection and governance

If those concerns were mixed together, it would be much harder to review, extend, and explain the system.

### `phase1/src/sog_phase1/`

This area owns baseline generation.

- `preprocess.py`
  - prepares source tables and caches
  - normalizes names, addresses, and supporting assets before generation
- `config.py`
  - parses and validates Phase-1 YAML
  - turns raw configuration into typed generation parameters
- `generator.py`
  - synthesizes the baseline dataset
  - allocates identity attributes, address structure, dates, and emitted records
- `nicknames.py`
  - stores nickname catalog logic separately from the main generator
  - keeps name-variant policy from turning into scattered string logic
- `redundancy.py`
  - owns record-allocation policy for repeated entities
  - isolates "how many rows should this person get?" from "what should the row values be?"

This split is important because identity construction, row redundancy, and prepared-source normalization are different problems.

### `src/sog_phase2/`

This area owns scenario generation and benchmarking.

- `pipeline.py`
  - orchestrates a full scenario run end to end
  - owns the execution boundary of a benchmark artifact
- `selection.py`
  - builds one-entity-per-person views from Phase 1
  - assigns latent traits, filters, samples, and logs the chosen cohort
- `simulator.py`
  - maintains the truth-layer state machine
  - owns household, membership, residence, and event mutation logic
- `event_grammar.py`
  - defines the active truth event surface
  - validates that event rows are structurally valid
- `constraints.py`
  - encodes realism and admissibility checks
  - provides a place for demographic and temporal rules to live outside the simulator loop
- `emission.py`
  - projects truth into observed datasets
  - owns snapshot extraction, appearance, overlap, duplication, field noise, crosswalks, and entity-record maps
- `quality.py`
  - computes what the run actually achieved
  - measures both truth consistency and benchmark-surface behavior
- `output_contract.py`
  - defines what a valid run must contain
  - validates file presence, schemas, naming, and topology-specific artifact rules
- `params.py`
  - loads the public-prior parameter bundle
  - decouples the engine from raw demographic source files
- `scenario_catalog.py`
  - defines the supported canonical scenario surface
  - separates shipped scenarios from working copies and experiments

This separation creates a cleaner engineering surface in four ways:

1. each layer can be reasoned about independently
2. each layer can be tested independently
3. adding a new scenario rarely requires rewriting the whole pipeline
4. outputs can be audited against explicit contracts

In other words, the module split is not cosmetic. It is what allows SOG to behave like a platform instead of a prototype.

## 5) What Is Novel In The Engineering

The most important novelty in the current implementation is not one single algorithm. It is the way several engineering ideas are combined into one benchmark system.

### 5.1 Truth and observed are explicitly separated

This is the architectural center of gravity.

SOG first models what actually happened.

Only afterward does it model what systems observed imperfectly.

That gives three benefits:

- the truth layer stays auditable
- the observed layer can be made arbitrarily difficult without breaking the answer key
- evaluation artifacts can be derived cleanly from the truth layer

This is more important than it sounds. Many synthetic systems generate only the observed-looking surface and never keep a normalized truth world behind it. SOG does the reverse: it first commits to a hidden world with explicit state and events, then emits imperfect observations from that world.

### 5.2 The system is deterministic without being behaviorally flat

Many deterministic systems become too rigid and boring.

SOG avoids that by combining:

- seeded RNG for run-level choices
- stable per-person latent variation
- explicit time-stepped stochastic simulation

The result is a system that is reproducible but still heterogeneous.

The engineering novelty here is layered determinism:

- stable hashing is used where invariance to population ordering matters
- seeded RNG is used where sequential event realization matters

That is a more careful design than using one global random stream for everything.

### 5.3 Benchmark topology is part of the model, not an afterthought

Most synthetic-data systems emit one flat dataset and leave benchmarking interpretation to the user.

SOG treats topology as a configuration surface:

- one dataset for dedup
- two datasets for pairwise linkage
- three or more datasets for N-way linkage

That makes the system relevant to a much wider class of ER engineering problems.

It also means that the answer key surface is designed with the topology in mind. Pairwise crosswalks and N-way projections are not post hoc utilities. They are part of the emitted benchmark contract.

### 5.4 Cardinality is controlled deliberately

The system does not merely hope that duplication and overlap produce useful linkage patterns.

It can deliberately target:

- `one_to_one`
- `one_to_many`
- `many_to_one`
- `many_to_many`

That matters because different ER systems fail differently under each cardinality regime.

In SOG, those regimes are created through explicit record allocation and later reflected in the crosswalk structure. In other words, the cardinality target is part of generation logic, not only part of downstream evaluation vocabulary.

### 5.5 Public priors and local simulation are combined cleanly

The parameter layer draws from Census and CDC style source packages, but the simulator does not merely replay those raw numbers.

Instead it uses:

- public-source rates as priors
- deterministic latent traits as local variation
- scenario rates as benchmark-specific overrides

That means the system is grounded without becoming a rigid replay of national aggregates.

This is the right balance for a benchmark engine. It stays anchored to recognizable demographic behavior, but it still allows scenario authors to intentionally stress specific ER failure modes.

### 5.6 Runs are auditable artifacts

A finished run is self-describing.

That is more important than it sounds.

For synthetic benchmarks, the question is not only "can you generate data?"

It is also:

- can another engineer explain why this benchmark is hard?
- can a team reproduce it exactly?
- can the same benchmark be re-run after a code change?
- can the scoring inputs be trusted?

SOG answers those questions with the run package itself.

The novelty is therefore not "it generates synthetic records." The novelty is that it generates:

- a coherent hidden world
- multiple imperfect observations of that world
- a benchmark topology
- the exact mapping layer needed to evaluate against the truth
- a governed artifact package that explains the whole run

### 5.7 The orchestrator is a contract boundary, not just a wrapper

`pipeline.py` is easy to underestimate because orchestration code is often treated as glue. In SOG it is more important than that.

The pipeline layer is where the system:

- parses the scenario YAML into typed config objects
- loads the Phase-2 priors bundle
- resolves the run ID and run directory
- executes selection, simulation, emission, and reporting in a fixed order
- writes the resolved scenario and output package
- invokes the output validator against the finished run

That means `pipeline.py` defines the execution boundary of a benchmark artifact. It is the layer that turns:

- a scenario definition
- a Phase-1 baseline
- a seed

into:

- a deterministic run directory
- a truth plane
- an observed plane
- an evaluation plane
- a governed metadata package

That is a stronger role than "main script." It is the contract boundary where SOG becomes a reproducible product surface.

## 6) The Current System As A Walkthrough Narrative

If you had to explain SOG in a meeting without opening the code, the cleanest story is this. The goal is to walk from configuration to benchmark difficulty in a way that makes the engineering logic visible.

### Step 1: Start with a synthetic but structured population

SOG first constructs people, names, demographics, addresses, and administrative-style rows.

This gives the system an initial world to work from, but it is important to be precise about what kind of world it is:

- it is identity-bearing
- it is record-shaped
- it is not yet a household-dynamics benchmark

That distinction matters. The baseline is not where the hard linkage ambiguity is supposed to peak. It is where the engine establishes plausible people and starting records that later stages can transform.

### Step 2: Choose the part of the world that matters for the benchmark

Instead of simulating everyone, SOG filters and samples a deterministic scenario population.

That lets one scenario focus on young movers, another on family growth, and another on sparse source overlap.

The important technical point is that this is not just demographic filtering. It is also behavior shaping. Once latent propensities are attached, the selected cohort is not only "people of the right age band" but "people with stable local priors for movement, coupling, and fertility."

This is how scenarios avoid being only broad demographic slices.

### Step 3: Evolve that world over time

The simulator then changes the selected world through life and household events.

Now the system has chronology, causality, and state transition.

This is the moment where SOG diverges sharply from flat synthetic row generators. When the simulator runs, later ambiguity becomes explainable:

- a shared address may exist because of cohabitation
- a new child entity may exist because of a birth event
- the same person may appear under incompatible address evidence because a move occurred between snapshots
- a household split may leave residual ambiguity because divorce or leaving home changed who lives together

The simulator is therefore not only adding noise. It is manufacturing the causal structure that makes the later observed layer difficult in realistic ways.

### Step 4: Ask what different systems would actually see

Different observed datasets can now:

- see different subsets of people
- capture different time snapshots
- carry different duplication pressure
- corrupt different fields

This is what turns a truth simulation into an ER benchmark.

The practical insight is that observed disagreement is not monolithic. It can come from:

- different coverage
- different observation timing
- different record cardinality
- different field corruption

That is important for engineers because different ER systems break on different combinations of those pressures. SOG is useful precisely because it can isolate and combine them deliberately.

### Step 5: Preserve the answer key and the explanation

The output package includes both:

- the hidden world
- the noisy observed world

plus the mappings that let evaluation happen exactly.

That means a completed run is not only data. It is also an explanation of the data.

That is the journey. SOG is the machinery that turns that journey into a reproducible benchmark package.

## 7) How Version 2 Differs From The Original Paper-Era SOG

The original paper-era SOG was a strong research prototype centered on occupancy histories.

Its strongest ideas were:

- internal identity versus external identity
- longitudinal occupancy histories
- scenario-driven synthetic identity generation
- a clear path toward later ER disruption

The current repository implementation keeps that conceptual DNA, but changes the scope and the engineering posture.

### 7.1 What stayed the same

The following ideas clearly survive from Version 1:

- identity should be modeled longitudinally
- the hidden truth and the observed ER surface are not the same thing
- occupancy and household behavior matter for linkage
- synthetic generation should still be auditable and explainable

This continuity matters. Version 2 is not valuable because it discarded the original framing. It is valuable because it retained the right conceptual core and then operationalized it.

### 7.2 What changed materially

Version 2 is broader and more operational.

It adds:

- a dedicated Phase-1 baseline generator
- a dedicated Phase-2 truth simulator
- a dedicated observed emission engine
- configurable dataset topology
- configurable cardinality
- explicit quality reporting
- output contract validation
- shipped scenario catalog and packaging

In plain terms:

- Version 1 was a generator concept with strong research framing
- Version 2 is a benchmark platform

The most important structural change is this: Version 1 mainly described how to generate the synthetic world. Version 2 explicitly separates world generation from benchmark projection.

That is why the current system can support:

- multiple topologies
- explicit crosswalks
- configurable overlap and coverage
- governed run packaging

### 7.3 Where Version 2 is stronger than Version 1

Version 2 is materially stronger in several ways:

- it supports single-dataset, pairwise, and N-way workflows
- it operationalizes disruption into actual configurable emission logic
- it exposes overlap, duplication, and noise as deliberate benchmark controls
- it packages answer keys and manifests as first-class outputs
- it is engineered around deterministic reruns and scenario catalogs

These are not marginal changes. They change what the system can be used for.

The more detailed comparison looks like this:

| Dimension | Version 1 emphasis | Version 2 emphasis | Practical effect |
|---|---|---|---|
| Main abstraction | occupancy history | baseline plus truth plus observed | V2 separates world simulation from benchmark projection |
| Scenario surface | Single and Couple narrative families | 11 shipped canonical scenarios plus configurable families | V2 covers a broader ER difficulty range |
| Truth representation | history-centric records | normalized people, households, memberships, residences, events | V2 is easier to audit and extend |
| Observed layer | framed as future or research direction | actively implemented emission engine | V2 ships real benchmark datasets, not only generator output |
| Evaluation packaging | conceptual | explicit entity-record map and crosswalk artifacts | V2 is much easier to score and compare |
| Governance | research framing | manifest, quality report, scenario log, output contract | V2 is much easier to review and reproduce |

For engineers, the practical translation is:

- Version 1 gave the project the right conceptual model
- Version 2 gives the project a reusable experimental surface

That is why the current codebase is better suited to comparative ER benchmarking, regression testing, and scenario expansion.

### 7.4 Where Version 2 is not a perfect superset

The current system is stronger overall, but not a literal feature-by-feature superset of every paper-era detail.

The clearest remaining gaps are:

- no first-class `DEATH` event in the active truth grammar
- no first-class `NAME_CHANGE` event in the active truth grammar
- no direct paper-style longitudinal PO Box occupancy model as a first-class exported truth construct
- the current observed layer is benchmark-oriented and snapshot-oriented rather than a direct export of full occupancy histories

These are not architectural failures. They are boundary decisions and roadmap items.

The important point for technical stakeholders is this:

Version 2 did not drift away from the paper. It operationalized and generalized it.

## 8) What Ships Today

The current repository ships a real benchmark surface, not just a code skeleton.

### Shipped truth event surface

- `MOVE`
- `COHABIT`
- `BIRTH`
- `DIVORCE`
- `LEAVE_HOME`

### Shipped scenario surface

The active scenario catalog includes 11 canonical scenarios covering:

- clean linkage baselines
- movers
- couple formation
- family growth
- divorce and custody-like ambiguity
- roommate churn
- high-noise identity drift
- sparse overlap
- asymmetric coverage
- single-dataset dedup
- three-source linkage

### Shipped observed benchmark surface

- single dataset
- pairwise A/B datasets
- N-way datasets
- configurable overlap
- configurable duplication
- configurable field-level corruption

### Shipped governance surface

- manifests
- quality reports
- selection logs
- resolved scenario YAML
- output contract validation

That means the current system is not only conceptually strong. It is also operationally concrete.

## 9) What Engineers Usually Care About In A Review

For an engineering-minded audience, the most important evaluation questions are usually these:

### Is the architecture coherent?

Yes. The system is layered cleanly around baseline generation, selection, truth simulation, emission, and validation.

### Is the system auditable?

Yes. The run package contains the truth layer, the observed layer, and the metadata needed to explain both.

### Is it deterministic enough to benchmark against?

Yes. Given the same baseline, scenario definition, and seed, SOG is designed to rerun deterministically.

### Is it configurable without turning into an ungoverned research sandbox?

Yes. The configuration surfaces are explicit:

- Phase-1 YAML
- Phase-2 scenario YAML
- Phase-2 priors bundle
- scenario catalog
- output contract

### Is the current implementation actually beyond the original paper?

Yes. The biggest difference is that Version 2 delivers the observed benchmarking layer, scenario packaging, and evaluation artifacts as real engineered surfaces instead of stopping at occupancy generation as a concept.

## 10) Bottom Line For The Meeting

If you need a short technical conclusion for stakeholders, it is this:

SOG in its current form is a deterministic, modular, auditable synthetic benchmarking platform for entity resolution.

Its core innovation is not merely that it generates fake people. Its core innovation is that it models:

- a hidden longitudinal truth world
- a configurable imperfect observed world
- and the exact mappings between them

That makes it valuable to engineers because it can be used to:

- benchmark ER systems
- debug failure modes
- compare algorithms across difficulty regimes
- run reproducible scenario regressions
- evolve the scenario catalog without rewriting the whole platform

The current system keeps the conceptual heart of the original SOG paper, but it has grown into something more operational and more useful:

- less like a one-off generator
- more like a reusable benchmark engine

## 11) Related Documents

For the readiness and testing posture, use:

- `docs/ENGINEERING_TEST_AND_READINESS_REPORT.md`

For the precise Version 1 versus Version 2 comparison, use:

- `docs/SOG_V1_TO_V2_COMPARISON.md`

For parameter-level detail, use:

- `docs/reference/PHASE1_PHASE2_PARAMETER_REFERENCE.md`

For tuning guidance, use:

- `docs/reference/PARAMETER_TUNING_PLAYBOOK.md`

For end-user scenario guidance, use:

- `docs/SCENARIO_USE_CASES_AND_TESTING.md`
