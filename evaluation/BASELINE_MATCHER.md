# E0: Baseline matcher specification

This file is the complete reimplementation contract for the deliberately
simple, hand-weighted entity-resolution baseline used in the paper. It is a
transparent reference comparator, not a claim of state-of-the-art matching.

## Input normalization

All text is stripped, upper-cased, and reduced to ASCII letters and digits.
Dates parse through `pandas.to_datetime` and are represented as `YYYYMMDD`.
Missing or unparsable values receive similarity zero. Soundex is the classic
four-character American Soundex mapping implemented in
`evaluation/baseline_matcher.py`.

## Candidate generation (union of five blocking keys)

A cross-source pair is scored if it shares at least one nonempty key:

1. normalized date of birth;
2. Soundex(last name) + first-name initial;
3. Soundex(first name) + Soundex(last name);
4. normalized postal code + Soundex(last name); or
5. four-digit birth year + Soundex(last name).

Duplicate candidates produced by multiple keys are counted only once.

## Field similarities and weights

| Field | Similarity in [0, 1] | Weight |
| --- | --- | ---: |
| First name | Sorensen-Dice coefficient over character bigram sets | 0.20 |
| Last name | Sorensen-Dice coefficient over character bigram sets | 0.25 |
| Date of birth | 1 exact; 0.85 month/day transposed in same year; 0.65 same year and same month or day; 0.35 same year; otherwise 0 | 0.20 |
| Street | Sorensen-Dice coefficient over character bigram sets | 0.15 |
| City | Exact normalized equality | 0.05 |
| State | Exact normalized equality | 0.05 |
| Postal code | Exact normalized equality | 0.10 |

The weights sum to 1.00. For candidate pair `(a,b)`, the score is

`S(a,b) = 0.20 first + 0.25 last + 0.20 DOB + 0.15 street + 0.05 city + 0.05 state + 0.10 postal`.

Every candidate with `S(a,b) >= threshold` is predicted as a link. Pairwise
truth contains the Cartesian product of Dataset A and Dataset B records for
each shared `PersonKey`, so duplicated records are evaluated correctly.

## Threshold protocol

The primary threshold is **0.65**. It was selected on the pre-registered clean
condition by an inclusive grid search from 0.30 through 0.95 in increments of
0.01, maximizing pairwise F1; ties are resolved by higher precision and then
the higher threshold. E1 holds 0.65 fixed for every seed and condition. A
secondary sensitivity analysis independently selects a threshold on each
seed's clean condition using the identical grid and applies it to that seed's
four conditions.

The matcher was frozen for E0 in commit
`5efe06a2514214ac11610d91a6dde11a0d5d15a8`. The E1 provenance report records
the same revision and the matcher entry-point blob hash.
