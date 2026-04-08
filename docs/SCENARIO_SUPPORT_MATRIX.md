# SOG Scenario Support Matrix

Last updated: April 5, 2026

This document is the planning layer for Phase-2 scenarios. It answers a different question than the built-in use-case guide:

- `SCENARIO_USE_CASES_AND_TESTING.md` tells a user which current built-in scenario to run today.
- This document tells the team which end-user scenario intents are already supported, which are supported through parameter editing, which still need canonical templates, and which require new engine work.

The machine-readable source of truth is:

- `phase2/scenarios/catalog.yaml`

Use this matrix to decide what to productize next without pretending every desired scenario is already runnable.

---

## 1) Status Definitions

- `supported`: runnable now with the current engine and covered by tests.
- `planned`: should be added as a canonical scenario template next, but is not yet shipped as a built-in scenario.
- `future_engine_extension`: not safely supportable yet without new truth events or simulator logic.

There are also two delivery modes:

- `canonical_yaml`: a named scenario already exists under `phase2/scenarios/`.
- `parameterized`: the engine supports it now, but the user reaches it by editing parameters rather than selecting a shipped template.

---

## 2) Supported Now

### Canonical built-ins

| Scenario | End-user intent | Topology | Cardinality | Status |
|---|---|---|---|---|
| `single_movers` | Address-change benchmark | pairwise | `one_to_one` | supported |
| `clean_baseline_linkage` | Low-noise baseline linkage sanity check | pairwise | `one_to_one` | supported |
| `couple_merge` | Household formation benchmark | pairwise | `one_to_many` | supported |
| `family_birth` | Household growth benchmark | pairwise | `many_to_one` | supported |
| `divorce_custody` | Family split / custody ambiguity benchmark | pairwise | `many_to_many` | supported |
| `roommates_split` | Roommate churn and shared-address ambiguity benchmark | pairwise | `one_to_many` | supported |
| `high_noise_identity_drift` | Heavy field-level corruption with low structural ambiguity | pairwise | `one_to_one` | supported |
| `low_overlap_sparse_coverage` | High non-match pressure with weak shared coverage | pairwise | `one_to_one` | supported |
| `asymmetric_source_coverage` | Broad-vs-sparse source linkage benchmark | pairwise | `one_to_one` | supported |
| `high_duplication_dedup` | Single-file high-duplication dedup benchmark | single-dataset | `dedup` | supported |
| `three_source_partial_overlap` | Three-source linkage with partial shared evidence | N-way | `one_to_one` | supported |

### Supported through parameter editing

| Scenario family | End-user intent | Topology | Status |
|---|---|---|---|
| `custom_single_dataset_dedup` | Deduplicate one file instead of linking two | single-dataset | supported |
| `custom_pairwise_linkage` | Customize overlap, duplication, dataset labels, and match mode for two datasets | pairwise | supported |
| `custom_multi_dataset_linkage` | Link three or more systems in one run | N-way | supported |

These are already supported by the engine even though they are not yet shipped as named YAML templates. Users reach them by editing `emission.crossfile_match_mode`, `emission.datasets[*]`, and the noise/coverage controls.

---

## 3) Productization Status

The current engine-backed scenario surface is now fully productized for the supported templates above.

What remains before any future scenario family is marked supported:

- scenario preflight checks for infeasible user-edited configs,
- threshold-based acceptance expansion for every new family added later,
- and new truth-event semantics for the future-engine-extension scenarios.

---

## 4) Future Engine Extensions

These scenario families should not be declared supported until the truth simulator and event grammar grow.

| Scenario family | End-user intent | Required engine work |
|---|---|---|
| `name_change_lifecycle` | Benchmark continuity through legitimate name changes | activate `NAME_CHANGE` and define observed-name persistence rules |
| `death_survivor_persistence` | Benchmark stale records after death and household aftermath | activate `DEATH` and define persistence/retirement behavior |
| `adoption_blended_family` | Benchmark changing parent-child relationships | activate `ADOPTION` and define blended-family transition rules |

---

## 5) Implementation Sequence

If the goal is “users can ask for the scenario they mean and SOG supports it correctly,” the next implementation order should be:

1. Ship the `planned` scenarios that require only YAML templates and tests.
2. Add scenario preflight checks so invalid or infeasible edits fail before a run starts.
3. Add threshold-based acceptance tests for every supported scenario family.
4. Only then move the `future_engine_extension` scenarios into implementation.

---

## 6) Repo Contract

Keep the scenario support matrix honest with these rules:

- every `canonical_yaml` scenario in `catalog.yaml` must exist under `phase2/scenarios/`,
- every shipped YAML under `phase2/scenarios/` must be listed in `catalog.yaml`,
- `supported` means runnable now, not aspirational,
- new scenario families should land in the catalog before they land in the UI.
