# E7-E11 preregistration and analysis contract

Frozen on 2026-07-22 before computing E7, E8, or E11 outcomes. The repository
baseline at freeze time is `fb66cc61395794a4dbea22b27e6c2eb5850f43a7`.

This document separates fidelity to public parameter tables from external
validity against confidential operational data. Only the former is evaluated.
No result in this package establishes equivalence to an administrative system.

## Shared replication contract

- Evaluation seeds: `20260720` through `20260729`.
- Sample dispersion: sample standard deviation (`ddof=1`).
- E1 truth is analyzed once per seed using the clean emission condition. The
  other three emission conditions share the same truth layer and are not
  treated as independent demographic replications.
- Existing E1 and E2 outputs are read without regeneration. Their file hashes
  are retained in the result package.
- Missing, semantically incompatible, or structurally disabled estimands are
  marked `not estimable` or `not comparable`; they are never replaced by zero
  without an explicit zero numerator and valid denominator.

## E7: achieved versus target distributions

### Mobility

The achieved annual mover rate is the number of baseline people with at least
one post-baseline residence interval divided by the number of baseline people.
Multiple moves by one person count once. Age is fixed at the baseline truth
table value. Reported cohorts are `0-17`, `18-34`, `35-64`, and `65+`.

The ACS `18-34` target is the population-weighted combination of the published
`18-24` and `25-34` rows. The overall target is the `moved_past_year_pct` row in
`mobility_overall_acs_2024.csv`. Relative deviation is
`100 * (achieved - target) / target`.

The scenario's configured annual input is reported separately because an input
that differs from the public target is not evidence of failed stochastic
calibration; it is a parameterization mismatch.

### Household composition

The initial active household snapshot is reconstructed at `2026-01-01` from
membership intervals. It is mapped to the five mutually exclusive B11001 bins:

- married-couple family: household with active `HEAD` and `SPOUSE` roles;
- single-parent male/female: household with at least one `CHILD`, no `SPOUSE`,
  and the head's recorded gender;
- nonfamily living alone: one active member;
- nonfamily not alone: all other multi-person households.

The primary scalar is Jensen-Shannon distance (base 2, range 0-1) between the
five achieved and target shares. Component-wise deviations are also retained.
The on-disk ACS B11001 table contains household types, not household-size bins;
therefore a public-target household-size divergence is `not estimable` from the
specified source and is reported as a source-contract gap.

### Cohabitation, divorce, and fertility

An annual event rate is the number of the relevant events divided by its stated
population denominator. Fertility denominators are baseline females in each
age band and rates are per 1,000.

The CDC file provides marriage and divorce rates per 1,000 total population.
SOG `COHABIT` is not a marriage event, so cohabitation versus CDC marriage is
`not comparable`. E1 explicitly configures no cohabitation, divorce, or birth
process. Their achieved zeros are recorded as structural observations, but
they are not treated as validation of the corresponding public targets.

### Surname profile

The source target is the exact conditional surname distribution used by Phase
1: each ethnicity-specific surname weight vector is normalized, then mixed by
the baseline population's ethnicity shares. Generated surnames are measured on
one row per `PersonKey`. Report:

- top-10, top-50, and top-100 probability mass;
- log-log rank-frequency slope fitted over ranks 10 through 1,000 (or the
  available upper rank if smaller);
- Jensen-Shannon distance over the union of source and achieved surnames.

Because all E1 seeds reuse one Phase 1 population, seed SD for the surname and
initial-household metrics is expected to be zero. This dependence is disclosed.

### Descriptive verdict rules

- Scalar relative deviation: `close` at <=5%, `moderate` at >5% and <=20%, and
  `large deviation` above 20%.
- Jensen-Shannon distance: `close` at <=0.05, `moderate` at >0.05 and <=0.15,
  and `large deviation` above 0.15.
- These are descriptive tolerances, not hypothesis tests.

## E8: measurement rigor

- Hardware captures CPU model, logical and physical cores, installed RAM, OS,
  Python, pandas, and NumPy.
- Runtime measures the exact E1 baseline `score_candidates` path, including
  preparation, blocking, feature calculation, and scoring but excluding CSV
  loading and threshold metrics.
- Use the seed-`20260720` run for each of the four conditions. Perform one
  unreported warm-up followed by five timed repetitions per condition in a
  deterministic round-robin order.
- Verify that every repetition returns the same candidate count and candidate
  score/label digest. Report mean, sample SD, min, and max.
- Raw candidate pairs and Cartesian spaces are reported for all ten seeds as
  mean and sample SD. Cartesian space is `records_A * records_B`.

## E9: artifact archiving

The package will contain exact scenario YAMLs, evaluation source, environment,
run date, seed list, file hashes, archive instructions, and a proposed immutable
version string. A local annotated tag may be created only after all E7-E11
results are committed. GitHub release publication and Zenodo deposition require
authenticated external services; they are not described as complete unless a
real release URL and DOI are returned.

## E10: capability comparison

The feature table is a structured literature review, not an experiment. A
filled cell must be supported by a primary paper, official manual, or official
project documentation. Symbols mean:

- `●`: explicit first-class capability documented by the source;
- `◐`: partial, indirect, configurable by extension, or only approximated;
- `○`: not documented in the reviewed source.

`○` means "not evidenced in the reviewed sources," not proof that no private or
later implementation exists. Each row includes a short operational definition,
and disputed cells receive footnotes.

## E11: SOG versus GeCo

### Upstream implementation

Use the ANU GeCo source archive linked from the official project page,
`geco-data-generator-corruptor.tar.gz`, SHA-256
`676b143d208b44a306002c05fb2b17d1a21c40e1972d4be7db8b03c130d0ab0c`.
The original is Python 2. A mechanical Python 3 syntax port and any required
runtime-compatibility edits are preserved as a patch; generator and corruption
logic are not redesigned.

### Matched comparison

- SOG arm: E2 `couple_merge`, seeds `20260720-20260729`.
- GeCo arm per seed: Dataset A has 5,499 original records. Dataset B has 10,460
  corrupted records descended from A plus 551 independently generated unmatched
  records. Thus both arms have 5,499 A rows, 11,011 B rows, 10,460 true links,
  and 60,549,489 Cartesian pairs.
- The seven E2 matcher fields are generated through GeCo frequency/function
  attributes and corrupted through GeCo corruptors. The exact field adapter and
  configuration are retained.
- On calibration seed `20260719`, choose the integer modifications-per-record
  value from `{1,2,3,4,5}` minimizing RMSE between the arms' mean seven-field
  true-pair similarity vectors. Freeze it before evaluating the ten seeds.
- Apply the frozen E2 baseline, Splink, and learned models and thresholds to
  both arms. No GeCo-specific threshold retuning is allowed.

Report pairwise F1, B-cubed F1, candidate recall, field-vector calibration RMSE,
and the share of false positives whose two records have the same complete
normalized address. This last metric is an address-sharing proxy consistent
with E6; GeCo has no household membership truth.

The novelty result is `supported` only if, for at least one matcher, SOG has
both (a) an F1 or B-cubed F1 deficit larger than three times the pooled
between-seed SD and (b) an address-sharing false-positive share at least 20
percentage points above GeCo. Otherwise E11 is reported as mixed or negative.
