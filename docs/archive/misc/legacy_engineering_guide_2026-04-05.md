# SOG Engineering Test and Readiness Report
## A Practical User Guide for Synthetic Occupancy Generator

**Version:** 1.0  
**Last Updated:** April 5, 2026  
**Status:** Production Ready

---

## Table of Contents

1. [What is SOG?](#1-what-is-sog)
2. [System Architecture & Design Philosophy](#2-system-architecture--design-philosophy)
3. [How Phase 1 Works: Baseline Generation](#3-how-phase-1-works-baseline-generation)
4. [How Phase 2 Works: The Simulation Engine](#4-how-phase-2-works-the-simulation-engine)
5. [The Event-Driven Simulation Model](#5-the-event-driven-simulation-model)
6. [Selection & Latent Traits: How People Are Chosen](#6-selection--latent-traits-how-people-are-chosen)
7. [Observed Emission: From Truth to Messy Data](#7-observed-emission-from-truth-to-messy-data)
8. [Data Models & Key Structures](#8-data-models--key-structures)
9. [Scenario Catalog: Choose Your Use Case](#9-scenario-catalog-choose-your-use-case)
10. [How to Run Each Scenario](#10-how-to-run-each-scenario)
11. [Configuring Scenarios](#11-configuring-scenarios)
12. [Understanding Your Outputs](#12-understanding-your-outputs)
13. [Evaluating Your ER System](#13-evaluating-your-er-system)
14. [Troubleshooting Guide](#14-troubleshooting-guide)
15. [Design Decisions & Trade-offs](#15-design-decisions--trade-offs)

---

## 1. What is SOG?

**SOG (Synthetic Occupancy Generator)** is a production-grade synthetic data pipeline that generates realistic person and household records for testing Entity Resolution (ER) systems.

### The Core Problem SOG Solves

Entity Resolution systems need realistic test data that mimics real-world complexity:
- People move and change addresses
- Households form, grow, and split
- Data quality varies across sources
- Duplicates exist within and across files
- Coverage overlap is rarely 100%

Creating such data manually is time-consuming and error-prone. SOG automates this with deterministic, reproducible scenarios.

### What You Get

| Artifact | Purpose |
|----------|---------|
| `DatasetA.csv` / `DatasetB.csv` | Observed data your ER system consumes |
| `truth_crosswalk.csv` | Ground-truth linking (for pairwise runs) |
| `entity_record_map.csv` | Canonical entity-to-record mapping |
| `truth_events.parquet` | What actually happened (MOVE, BIRTH, etc.) |
| `quality_report.json` | Metrics on overlap, duplication, noise |

---

## 2. System Architecture & Design Philosophy

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SOG SYSTEM ARCHITECTURE                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  PHASE 1: BASELINE GENERATION                                               │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────┐     │
│  │  Reference Data │───▶│  Prepared Cache │───▶│  Person+Address     │     │
│  │  (Names, Addr)  │    │  (Parquet/JSON) │    │  Baseline CSV       │     │
│  └─────────────────┘    └─────────────────┘    └─────────────────────┘     │
│                                                           │                 │
│                                                           ▼                 │
│  PHASE 2: SIMULATION & EMISSION                              ┌──────────┐  │
│  ┌─────────────────┐    ┌─────────────────┐    ┌──────────┐ │          │  │
│  │  Scenario YAML  │───▶│  Selection      │───▶│  Truth   │ │ Entity   │  │
│  │  (Config)       │    │  Engine         │    │  Layer   │ │ View     │  │
│  └─────────────────┘    └─────────────────┘    └────┬─────┘ └──────────┘  │
│                                                     │                       │
│                                                     ▼                       │
│                              ┌──────────────────────────────────────┐     │
│                              │   EVENT-DRIVEN SIMULATION ENGINE     │     │
│                              │   ┌─────────┐ ┌─────────┐ ┌────────┐ │     │
│                              │   │  MOVE   │ │  BIRTH  │ │ DIVORCE│ │     │
│                              │   │  events │ │ events  │ │ events │ │     │
│                              │   └────┬────┘ └────┬────┘ └───┬────┘ │     │
│                              │        └───────────┴──────────┘      │     │
│                              │                   │                   │     │
│                              │                   ▼                   │     │
│                              │   ┌─────────────────────────────────┐ │     │
│                              │   │  Truth State Tables             │ │     │
│                              │   │  • truth_people                 │ │     │
│                              │   │  • truth_households             │ │     │
│                              │   │  • truth_residence_history      │ │     │
│                              │   │  • truth_events                 │ │     │
│                              │   └─────────────────────────────────┘ │     │
│                              └──────────────────┬────────────────────┘     │
│                                                 │                           │
│                                                 ▼                           │
│                              ┌──────────────────────────────────────┐     │
│                              │   OBSERVED EMISSION ENGINE           │     │
│                              │   ┌───────────┐  ┌───────────┐      │     │
│                              │   │  Dataset  │  │  Dataset  │      │     │
│                              │   │     A     │  │     B     │      │     │
│                              │   └───────────┘  └───────────┘      │     │
│                              │                                       │     │
│                              │   Noise Injection:                   │     │
│                              │   • Typos, OCR errors                │     │
│                              │   • Nickname substitution            │     │
│                              │   • Date shifts                      │     │
│                              │   • Field masking                    │     │
│                              │                                       │     │
│                              │   Duplication Logic:                 │     │
│                              │   • Within-file duplicates           │     │
│                              │   • Cross-file overlap control       │     │
│                              └──────────────────────────────────────┘     │
│                                                 │                           │
│                                                 ▼                           │
│  OUTPUTS                                        ┌──────────┐             │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────┐│  Truth   │             │
│  │  Observed    │  │  Entity-     │  │ Quality ││ Crosswalk│             │
│  │  Datasets    │  │  Record Map  │  │ Report  │└──────────┘             │
│  └──────────────┘  └──────────────┘  └─────────┘                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Design Philosophy

| Principle | Implementation |
|-----------|---------------|
| **Determinism** | Same seed + same config = identical output |
| **Reproducibility** | All parameters versioned in scenario YAML |
| **Modularity** | Phase 1 and Phase 2 are separable |
| **Observability** | Truth layer fully exposed for validation |
| **Extensibility** | Event types and noise models are pluggable |
| **Realism Grounding** | Parameters backed by ACS/CDC public data |

### Key Abstractions

```
Entity ────────┐
(PersonKey)    │
               ├──► Record ──────► Observed Record (with noise)
               │   (RecordKey)      (Dataset A/B)
Instance ──────┘
```

The **truth layer** maintains perfect knowledge of entities, households, and events. The **observed layer** is what your ER system sees—imperfect, partial, and noisy.

---

## 3. How Phase 1 Works: Baseline Generation

### Overview

Phase 1 creates the foundational population that Phase 2 simulates upon. It generates realistic person records with demographic attributes and addresses.

### The Data Flow

```
Raw Reference Data                    Prepared Cache                 Generated Output
┌─────────────────┐                  ┌─────────────────┐           ┌──────────────────┐
│  Names/         │  ───────────────▶│  names.parquet  │──────────▶│  Person Records  │
│  (US Census)    │   build_prepared │  nicknames.json │           │  with identity   │
└─────────────────┘                  └─────────────────┘           └──────────────────┘
                                                                            │
┌─────────────────┐                  ┌─────────────────┐                    │
│  Addresses/     │  ───────────────▶│  addresses.     │────────────────────┤
│  (Synthetic)    │                  │  parquet        │                    │
└─────────────────┘                  └─────────────────┘                    │
                                                                            ▼
┌─────────────────┐                  ┌─────────────────┐           ┌──────────────────┐
│  Data/demograph.│  ───────────────▶│  demographics.  │──────────▶│  Age, Gender,    │
│  ics_extracted/ │                  │  parquet        │           │  Ethnicity       │
└─────────────────┘                  └─────────────────┘           └──────────────────┘
```

### Generation Algorithm

1. **Entity Count Determination**
   - User specifies `n_entities` (unique people)
   - User specifies `n_records` (total rows, ≥ n_entities)
   - Redundancy ratio calculated: `n_records / n_entities`

2. **Demographic Assignment**
   ```python
   # Probabilistic assignment based on configured distributions
   gender ~ Categorical(male=0.49, female=0.51)
   ethnicity ~ Categorical(hispanic=0.18, white=0.60, ...)
   age_bin ~ Categorical(0-17=0.22, 18-34=0.23, ...)
   ```

3. **Name Generation**
   - First name sampled from ethnicity-conditional distribution
   - Last name sampled from US Census surname frequencies
   - Middle name (optional, ~20% missing)
   - Suffix (optional, ~5% present)

4. **Record Redundancy (Optional)**
   - When `n_records > n_entities`, some entities get multiple records
   - Controlled by `redundancy.min_records_per_entity` and `max_records_per_entity`
   - Each redundant record gets unique `RecordKey` but shares `PersonKey`

5. **Address Assignment**
   - Residence address from prepared address pool
   - Mailing address (can differ, ~15% cases)
   - AddressKey assigned uniquely per address

6. **Contact Information**
   - SSN generated with valid area/group/serial structure
   - Phone number with valid area code

---

## 4. How Phase 2 Works: The Simulation Engine

### The Three-Stage Pipeline

```
Stage 1: SELECTION                    Stage 2: TRUTH SIMULATION              Stage 3: EMISSION
┌─────────────────┐                   ┌─────────────────────────┐           ┌──────────────────┐
│ Phase 1         │                   │  Simulation State       │           │  Observed        │
│ Population      │──────────────────▶│  Machine                │──────────▶│  Datasets        │
│ (100K people)   │  Filter + Sample  │                         │  + Noise  │  (A, B, ...)     │
└─────────────────┘                   │  • Person attributes    │           └──────────────────┘
         │                            │  • Household structure  │
         ▼                            │  • Residence history    │
┌─────────────────┐                   │  • Event log            │
│ Selected        │                   └─────────────────────────┘
│ Participants    │
│ (10K people)    │
└─────────────────┘
```

### Component Details

| Component | Module | Purpose |
|-----------|--------|---------|
| Selection Engine | `selection.py` | Choose scenario participants deterministically |
| Simulator | `simulator.py` | Event-driven truth state evolution |
| Constraints | `constraints.py` | Realism validation (age gaps, fertility windows) |
| Emission | `emission.py` | Generate observed datasets with noise |
| Quality | `quality.py` | Compute benchmark metrics |

---

## 5. The Event-Driven Simulation Model

### Core Concept

SOG simulates life as a discrete-time event system. At each time step (month by default), eligible people may experience events based on probabilistic rates.

### Event Types

| Event | Trigger Condition | Effect |
|-------|-------------------|--------|
| **MOVE** | Person changes residence | New address, new household if alone |
| **COHABIT** | Two people form partnership | Shared household, linked records |
| **BIRTH** | Couple has child | New PersonKey added to household |
| **DIVORCE** | Couple separates | Household split, custody determination |
| **LEAVE_HOME** | Young adult departs | New independent household |

### Event Processing Algorithm

```python
# Simplified simulation loop
for step in range(simulation_periods):
    current_date = start_date + step
    
    # 1. Evaluate each person for events
    for person in active_population:
        
        # MOVE: Based on mobility propensity + age-based base rate
        if random() < move_probability(person):
            execute_move(person, current_date)
        
        # COHABIT: Based on partnership propensity + available partners
        if eligible_for_partnership(person):
            partner = find_compatible_partner(person)
            if partner and random() < cohabit_probability:
                execute_cohabitation(person, partner, current_date)
        
        # BIRTH: Based on fertility propensity + couple status
        if in_couple(person) and random() < birth_probability(person):
            execute_birth(person, current_date)
        
        # DIVORCE: Based on couple duration + divorce rate
        if in_couple(person) and random() < divorce_probability:
            execute_divorce(person, current_date)
    
    # 2. Update household structures
    reconcile_household_memberships(current_date)
    
    # 3. Validate constraints
    check_residence_intervals_non_overlapping()
    check_household_size_constraints()
```

### Rate Conversion

Annual rates from configuration are converted to step probabilities:

```
annual_rate = scenario.move_rate_pct / 100  # e.g., 12% → 0.12
steps_per_year = 12 if monthly else 365
step_probability = 1 - (1 - annual_rate)^(1/steps_per_year)

# Example: 12% annual move rate → ~1.05% monthly probability
```

### State Machine: Person Lifecycle

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Single    │────▶│   Coupled   │────▶│  Separated  │
│  (initial)  │     │  (COHABIT)  │     │  (DIVORCE)  │
└──────┬──────┘     └──────┬──────┘     └─────────────┘
       │                   │
       │ MOVE events       │ BIRTH events
       ▼                   ▼
┌─────────────┐     ┌─────────────┐
│  Different  │     │   Parent    │
│   Address   │     │  (children  │
│             │     │   added)    │
└─────────────┘     └─────────────┘
```

### State Machine: Household Evolution

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Individual │────▶│   Couple    │────▶│   Family    │
│  Household  │     │  Household  │     │  (+children)│
└─────────────┘     └──────┬──────┘     └──────┬──────┘
                           │                     │
                           │ DIVORCE            │ Children LEAVE_HOME
                           ▼                     ▼
                    ┌─────────────┐     ┌─────────────┐
                    │  Split into │     │   Smaller   │
                    │   two HHs   │     │   Family    │
                    └─────────────┘     └─────────────┘
```

---

## 6. Selection & Latent Traits: How People Are Chosen

### Overview

Not everyone from Phase 1 participates in every scenario. The selection engine deterministically chooses participants and assigns them **latent traits**—hidden propensities that influence event probabilities.

### Latent Traits

Each person receives three trait scores (0.0-1.0):

| Trait | Influences | Base Calculation |
|-------|-----------|------------------|
| **Mobility** | MOVE event probability | Age-cohort base + person jitter |
| **Partnership** | COHABIT event probability | Age-cohort base + person jitter |
| **Fertility** | BIRTH event probability | Age-cohort base + person jitter |

### Trait Assignment Algorithm

```python
def assign_latent_traits(person, age_bin, seed):
    # Base rate from age cohort (ACS data)
    mobility_base = age_mobility_rates[age_bin]  # e.g., 0.20 for 18-34
    
    # Deterministic person-specific jitter (SHA256-based)
    jitter = deterministic_random(person.key, seed, "mobility")
    
    # Final score with clamping
    mobility_score = clip(mobility_base + (jitter - 0.5) * 0.08, 0.0, 1.0)
    
    return mobility_score
```

### Deterministic Randomization

Critical for reproducibility—traits are not truly random but derived from person key + seed:

```python
def deterministic_unit(person_key, seed, salt):
    """Returns value in [0, 1) deterministically."""
    payload = f"{seed}|{salt}|{person_key}".encode()
    digest = hashlib.sha256(payload).digest()
    as_int = int.from_bytes(digest[:8], byteorder="big")
    return as_int / (2**64 - 1)
```

### Bucketing

Traits are bucketed for filtering:

```
Mobility Score → Bucket
0.00 - 0.09   → low
0.10 - 0.17   → medium
0.18 - 1.00   → high
```

Configurable via `thresholds.mobility_low_max` and `mobility_high_min`.

### Selection Pipeline

```
Phase 1 Entities (100K)
         │
         ▼
┌─────────────────┐
│ Assign Latent   │  Deterministic trait scores
│ Traits          │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│ Apply Filters   │  Age, gender, ethnicity, mobility bucket, etc.
│ (scenario.yaml) │
└─────────────────┘
         │
         ▼
┌─────────────────┐
│ Sample          │  all | count | pct
│ (deterministic) │
└─────────────────┘
         │
         ▼
Selected Population (e.g., 10K)
```

### Selection Example

```yaml
# scenario.yaml
selection:
  sample:
    mode: pct
    value: 10.0              # 10% of eligible
  filters:
    age_bins: ["age_18_34"]  # Only young adults
    mobility_propensity_buckets: ["high"]  # Movers
  thresholds:
    mobility_low_max: 0.09
    mobility_high_min: 0.18
```

---

## 7. Observed Emission: From Truth to Messy Data

### Overview

The emission engine transforms perfect truth records into realistic observed records by:
1. **Sampling** (not everyone appears in every dataset)
2. **Duplication** (some entities appear multiple times in same dataset)
3. **Noise injection** (field-level corruption)

### Coverage Model

```
Truth Population: 1000 entities

Dataset A (appearance_A_pct: 85%)
  ├── 850 entities appear
  │     ├── 800 entities: 1 record each
  │     └── 50 entities: 2+ records each (duplication_in_A_pct: 5%)
  └── 150 entities: not present

Dataset B (appearance_B_pct: 90%)
  ├── 900 entities appear
  │     ├── 828 entities: 1 record each  
  │     └── 72 entities: 2+ records each (duplication_in_B_pct: 8%)
  └── 100 entities: not present

Overlap (overlap_entity_pct: 70%)
  └── 700 entities in BOTH A and B
      └── These are the "linkable" entities
```

### Noise Injection Types

| Noise Type | Description | Example |
|------------|-------------|---------|
| `name_typo_pct` | Single-character substitution | "Smith" → "Smoth" |
| `phonetic_error_pct` | Phonetic substitution | "ph" → "f" |
| `ocr_error_pct` | OCR confusion | "O" → "0", "l" → "1" |
| `nickname_pct` | Formal name → nickname | "Robert" → "Bob" |
| `dob_shift_pct` | Date offset | DOB shifts ±1-365 days |
| `date_swap_pct` | Month/day swap | 05/12 → 12/05 |
| `ssn_mask_pct` | SSN digits masked | XXX-XX-1234 |
| `phone_mask_pct` | Phone digits missing | (XXX) XXX-5678 |
| `address_missing_pct` | Entire address blank | "" |
| `zip_digit_error_pct` | ZIP code error | 90210 → 90211 |
| `middle_name_missing_pct` | Middle name dropped | "John A. Smith" → "John Smith" |
| `suffix_missing_pct` | Suffix dropped | "Jr." removed |

### Noise Implementation

```python
# OCR confusion table
ocr_confusions = {
    "O": ["0"], "0": ["O"],
    "l": ["1", "I"], "1": ["l", "I"],
    "B": ["8"], "8": ["B"],
    # ...
}

# Phonetic substitution clusters
phonetic_subs = [
    ("ph", "f"), ("ck", "k"), ("ie", "y"),
    # ...
]

# Nickname resolution
nickname_map = {
    "Robert": ["Bob", "Rob"],
    "William": ["Bill", "Will"],
    # ...
}
```

### Cardinality Control

The `crossfile_match_mode` determines how many records an entity has per dataset:

| Mode | Dataset A | Dataset B | Use Case |
|------|-----------|-----------|----------|
| `one_to_one` | 1 record | 1 record | Clean linkage |
| `one_to_many` | 1 record | 2+ records | Master → messy system |
| `many_to_one` | 2+ records | 1 record | Messy → master |
| `many_to_many` | 2+ records | 2+ records | Both systems messy |
| `single_dataset` | N records | N/A | Deduplication only |

---

## 8. Data Models & Key Structures

### Entity-Record Relationship

```
┌────────────────────────────────────────────────────────────────┐
│                     ENTITY-RECORD MODEL                        │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  TRUTH LAYER                              OBSERVED LAYER       │
│                                                                │
│  ┌─────────────┐                          ┌─────────────┐     │
│  │  PersonKey  │                          │  RecordKey  │     │
│  │  P000001    │◄─────────────────────────│  A_000042   │     │
│  │             │         1:M              │             │     │
│  │  Formal     │                          │  PersonKey  │     │
│  │  Name:      │◄─────────────────────────│  P000001    │     │
│  │  "Robert    │      (entity_record_map) │             │     │
│  │   Smith"    │                          │  FirstName  │     │
│  │             │                          │  "Bob"      │     │
│  │  DOB:       │                          │  (nickname) │     │
│  │  1985-03-12 │                          │             │     │
│  │             │                          │  Address    │     │
│  │  SSN:       │                          │  "123 Main  │     │
│  │  123-45-6789│                          │   St"       │     │
│  └─────────────┘                          └─────────────┘     │
│         │                                                      │
│         │ 1:1 (for pairwise)                                   │
│         ▼                                                      │
│  ┌─────────────────┐                                           │
│  │ truth_crosswalk │                                           │
│  │ PersonKey       │                                           │
│  │ A_RecordKey ────┼───────► Dataset A record                  │
│  │ B_RecordKey ────┼───────► Dataset B record                  │
│  └─────────────────┘                                           │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### Key Fields Reference

| Field | Table(s) | Description |
|-------|----------|-------------|
| `PersonKey` | All truth tables | Canonical entity identifier |
| `RecordKey` | Observed datasets, entity_record_map | Instance identifier |
| `DatasetId` | entity_record_map | Source dataset (A, B, registry, etc.) |
| `HouseholdKey` | truth_households, truth_household_memberships | Household identifier |
| `AddressKey` | truth_residence_history | Residence location identifier |
| `EventKey` | truth_events | Unique event identifier |
| `EntityRecordIndex` | Phase 1 output | Record sequence per person |

### Truth Table Schemas

**truth_people**
```
PersonKey, FormalFirstName, MiddleName, LastName, Suffix,
FormalFullName, Gender, Ethnicity, DOB, Age, AgeBin, SSN, Phone
```

**truth_households**
```
HouseholdKey, HouseholdType, HouseholdStartDate, HouseholdEndDate
```

**truth_household_memberships**
```
PersonKey, HouseholdKey, HouseholdRole,
MembershipStartDate, MembershipEndDate
```

**truth_residence_history**
```
PersonKey, AddressKey, ResidenceStartDate, ResidenceEndDate
```

**truth_events**
```
EventKey, PersonKey, EventType, EventDate,
Details (JSON), PartnerKey (for couple events)
```

---

## 9. Scenario Catalog: Choose Your Use Case

### Decision Tree

```
┌─────────────────────────────────────────────────────────────┐
│ What do you want to test?                                    │
└───────────────────────┬─────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
   ┌─────────┐    ┌──────────┐    ┌──────────┐
   │ Address │    │ Household│    │ Identity │
   │ Change  │    │ Dynamics │    │  Quality │
   └────┬────┘    └────┬─────┘    └────┬─────┘
        │              │               │
        ▼              ▼               ▼
   single_movers   couple_merge    high_noise_
   clean_baseline  family_birth    identity_drift
                   divorce_custody
                   roommates_split
```

### Scenario Reference Table

| Scenario | Use Case | Events | Difficulty | Match Pattern |
|----------|----------|--------|------------|---------------|
| **clean_baseline_linkage** | Sanity check your ER pipeline | MOVE | ⭐ Easy | one_to_one |
| **single_movers** | Test address-change handling | MOVE | ⭐⭐ Basic | one_to_one |
| **couple_merge** | Test household formation | COHABIT | ⭐⭐⭐ Medium | one_to_many |
| **family_birth** | Test family expansion | BIRTH | ⭐⭐⭐ Medium | many_to_one |
| **roommates_split** | Test roommate/young adult churn | LEAVE_HOME | ⭐⭐⭐⭐ Hard | one_to_many |
| **divorce_custody** | Test complex household splits | DIVORCE | ⭐⭐⭐⭐⭐ Very Hard | many_to_many |
| **high_noise_identity_drift** | Test field-level corruption | MOVE | ⭐⭐⭐⭐ Hard | one_to_one |
| **low_overlap_sparse_coverage** | Test low-overlap scenarios | MOVE | ⭐⭐⭐⭐ Hard | one_to_one |
| **asymmetric_source_coverage** | Test unequal source coverage | MOVE | ⭐⭐⭐ Medium | one_to_one |
| **high_duplication_dedup** | Test deduplication (single file) | - | ⭐⭐⭐ Medium | dedup |
| **three_source_partial_overlap** | Test 3+ source linkage | MOVE | ⭐⭐⭐⭐ Hard | N-way |

### Scenario Deep Dives

#### Use Case: "I want to benchmark address changes"
**Choose:** `single_movers`

**What it simulates:**
- People relocate between two time snapshots
- Addresses change but identity remains stable
- Moderate noise and duplication

**Why use it:**
- Most common ER challenge in practice
- Clean enough for baseline metrics
- Realistic enough to catch problems

**Key outputs to check:**
```bash
# Count MOVE events
jq '.phase2_quality.scenario_metrics.event_counts.moves' \
  phase2/runs/<run_id>/quality_report.json

# Check overlap achieved
jq '.phase2_quality.er_benchmark_metrics.cross_file_overlap' \
  phase2/runs/<run_id>/quality_report.json
```

---

#### Use Case: "I want to test household formation"
**Choose:** `couple_merge`

**What it simulates:**
- Two people meet and form a household
- Dataset B captures more duplicates (messy side)
- Shared address after cohabitation

**Why use it:**
- Tests shared-address false positive pressure
- One-to-many cardinality stress
- Household vs. individual entity confusion

---

## 10. How to Run Each Scenario

### Method 1: Unified Pipeline (Recommended)

```bash
python scripts/run_phase2_pipeline.py --scenario <scenario_id>
```

**Examples:**
```bash
# Run single movers
python scripts/run_phase2_pipeline.py --scenario single_movers

# Run with overwrite if outputs exist
python scripts/run_phase2_pipeline.py --scenario divorce_custody --overwrite

# Force rebuild of population selection
python scripts/run_phase2_pipeline.py --scenario family_birth --rebuild-population
```

### Method 2: Step-by-Step Control

```bash
# 1. Define your run parameters
SCENARIO="couple_merge"
DATE="2026-04-05"
SEED="20260405"
RUN_ID="${DATE}_${SCENARIO}_seed${SEED}"

# 2. Generate truth layer (simulates life events)
python scripts/generate_phase2_truth.py \
  --scenario ${SCENARIO} \
  --seed ${SEED} \
  --run-date ${DATE}

# 3. Emit observed datasets (adds noise/duplication)
python scripts/generate_phase2_observed.py \
  --run phase2/runs/${RUN_ID}

# 4. Validate outputs
python scripts/validate_phase2_outputs.py \
  --run phase2/runs/${RUN_ID}
```

---

## 11. Configuring Scenarios

### Scenario YAML Structure

```yaml
scenario_id: my_scenario
seed: 20260405

# Input data reference
phase1:
  data_path: phase1/outputs_phase1/Phase1_people_addresses.csv
  manifest_path: phase1/outputs_phase1/Phase1_people_addresses.manifest.json

# Event rates (annual percentages)
parameters:
  move_rate_pct: 12.0          # 12% move per year
  cohabit_rate_pct: 4.0        # 4% form couples per year
  birth_rate_pct: 2.5          # 2.5% have children per year
  divorce_rate_pct: 1.5        # 1.5% divorce per year

# Simulation timing
simulation:
  granularity: monthly         # or daily
  start_date: 2026-01-01
  periods: 12                  # 12 months

# Observed dataset configuration
emission:
  crossfile_match_mode: one_to_one
  overlap_entity_pct: 70.0
  appearance_A_pct: 85.0
  appearance_B_pct: 90.0
  duplication_in_A_pct: 4.0
  duplication_in_B_pct: 6.0
  
  noise:
    A:
      name_typo_pct: 1.0
      dob_shift_pct: 0.4
      ssn_mask_pct: 1.5
    B:
      name_typo_pct: 2.5
      dob_shift_pct: 1.2
      ssn_mask_pct: 6.0
      nickname_pct: 5.0

# Population selection
selection:
  sample:
    mode: pct
    value: 100.0

# Realism constraints
constraints:
  min_marriage_age: 18
  max_partner_age_gap: 25
  fertility_age_range:
    min: 15
    max: 49
```

### Common Configuration Patterns

#### Pattern 1: High Duplication Stress Test
```yaml
emission:
  crossfile_match_mode: many_to_many
  duplication_in_A_pct: 15.0
  duplication_in_B_pct: 25.0
```

#### Pattern 2: Low Overlap / High Non-Match Pressure
```yaml
emission:
  crossfile_match_mode: one_to_one
  overlap_entity_pct: 30.0        # Only 30% shared
  appearance_A_pct: 95.0
  appearance_B_pct: 40.0
```

---

## 12. Understanding Your Outputs

### Output Directory Structure

```
phase2/runs/<run_id>/
├── DatasetA.csv                    # Observed dataset A
├── DatasetB.csv                    # Observed dataset B (pairwise)
├── entity_record_map.csv           # Truth: entity to record mapping
├── truth_crosswalk.csv             # Truth: A-to-B pairwise links
├── truth_people.parquet            # Truth: person entities
├── truth_households.parquet        # Truth: household entities
├── truth_events.parquet            # Truth: what happened
├── quality_report.json             # Quality metrics
└── manifest.json                   # Run metadata
```

### Key Quality Metrics

```json
{
  "status": "ok",
  "scenario_metrics": {
    "event_counts": {
      "moves": 145,
      "births": 23,
      "couples_formed": 18
    }
  },
  "er_benchmark_metrics": {
    "cross_file_overlap": 0.68,
    "within_file_duplicate_rates": {
      "dataset_a": 0.04,
      "dataset_b": 0.08
    }
  }
}
```

---

## 13. Evaluating Your ER System

### Basic Evaluation Script

```python
import pandas as pd

def evaluate_pairwise(run_dir):
    truth = pd.read_csv(f"{run_dir}/truth_crosswalk.csv")
    predictions = pd.read_csv(f"{run_dir}/my_predictions.csv")
    
    true_positives = len(predictions.merge(
        truth, 
        on=["PersonKey", "A_RecordKey", "B_RecordKey"]
    ))
    
    precision = true_positives / len(predictions)
    recall = true_positives / len(truth)
    f1 = 2 * (precision * recall) / (precision + recall)
    
    print(f"Precision: {precision:.3f}")
    print(f"Recall: {recall:.3f}")
    print(f"F1: {f1:.3f}")
```

---

## 14. Troubleshooting Guide

| Problem | Solution |
|---------|----------|
| "Prepared cache incomplete" | `python scripts/build_prepared.py --raw-root . --prepared-dir prepared` |
| "Output file exists" | Add `--overwrite` flag |
| "Phase-1 CSV not found" | Generate Phase 1 first or pass `--phase1` path |
| No primary events | Check scenario YAML parameters and selection filters |

---

## 15. Design Decisions & Trade-offs

### 1. Determinism vs. Realism

**Decision:** Use seed-based determinism for all random choices.

**Trade-off:** 
- ✅ Reproducible benchmarks
- ✅ Regression testing possible
- ❌ Less "organic" than true randomness

**Mitigation:** SHA256-based per-person jitter creates realistic variation while maintaining determinism.

### 2. Monthly vs. Daily Granularity

**Decision:** Default to monthly simulation steps.

**Trade-off:**
- ✅ 12x faster than daily
- ✅ Sufficient for household-level dynamics
- ❌ Loses sub-monthly event ordering

**When to use daily:** High-frequency event scenarios or sub-monthly accuracy requirements.

### 3. Snapshot vs. Longitudinal Emission

**Decision:** Emit snapshot-based observed datasets, not full histories.

**Trade-off:**
- ✅ Matches typical ER input (system extracts)
- ✅ Simpler ground truth (one record per entity per dataset)
- ❌ Cannot test temporal record linkage

**Future:** Full longitudinal export may be added.

### 4. Pairwise vs. N-way Crosswalks

**Decision:** Support both via `entity_record_map.csv` (canonical) and `truth_crosswalk.csv` (pairwise backward compatibility).

**Trade-off:**
- ✅ N-way scenarios supported natively
- ✅ Existing pairwise tools continue working
- ❌ Two truth formats to maintain

### 5. Constraint Strictness

**Decision:** Constraints are validation checks, not hard simulation rules.

**Trade-off:**
- ✅ Simulation continues even with edge cases
- ✅ Quality report flags violations
- ❌ May produce "impossible" scenarios if constraints too loose

### 6. Phase 1 / Phase 2 Separation

**Decision:** Strict separation between baseline generation and simulation.

**Trade-off:**
- ✅ Phase 2 can run on any Phase 1 output
- ✅ Teams can work independently
- ❌ Cannot "reach back" into Phase 1 during simulation

### Performance Characteristics

| Operation | Typical Time | Scales With |
|-----------|-------------|-------------|
| Phase 1 generation | 30-60s | n_records |
| Phase 2 selection | 5-10s | Phase 1 population |
| Phase 2 simulation | 10-30s | Selected population × periods |
| Phase 2 emission | 5-15s | Observed records |
| Full test suite | 40-50s | All scenarios |

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Entity** | A real-world person (identified by `PersonKey`) |
| **Record** | An observed data instance (identified by `RecordKey`) |
| **Truth Layer** | The latent simulation state (what really happened) |
| **Observed Layer** | What systems see (with noise, gaps, duplication) |
| **Crosswalk** | Mapping between records in different datasets |
| **Cardinality** | How many records an entity has per dataset |
| **Overlap** | Entities appearing in multiple datasets |
| **Duplication** | Multiple records for the same entity in one dataset |
| **Drift** | Attribute differences between truth and observed |
| **Latent Trait** | Hidden propensity score (mobility, partnership, fertility) |

## Appendix B: Related Documents

| Document | Purpose |
|----------|---------|
| `README.md` | Project overview and quick start |
| `docs/SOG_COMPLETE_USER_GUIDE.md` | Full technical reference |
| `docs/SCENARIO_USE_CASES_AND_TESTING.md` | Detailed scenario descriptions |
| `phase2/scenarios/README.md` | YAML schema reference |

---

*This document is part of the SOG (Synthetic Occupancy Generator) project.*
