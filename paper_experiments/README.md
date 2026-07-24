# Reproducible SOG Paper Experiments

This toolkit evaluates one transparent entity resolution baseline under four controlled conditions:

1. `paper_clean`: low-noise one-to-one reference condition.
2. `paper_high_noise`: identical truth and coverage, but severe corruption in Dataset B.
3. `paper_low_overlap`: identical truth and noise, but reduced source appearance and overlap.
4. `paper_one_to_many`: identical truth, coverage, and noise, but one-to-many cardinality with increased Dataset B duplication.

E0 freezes and documents the baseline matcher. E1 runs ten pre-registered seeds
(`20260720` through `20260729`). Within a seed, all four conditions use the same
Phase-1 baseline and Phase-2 truth; between seeds, Phase-2 truth is independently
simulated. The evaluator rejects any within-seed truth-signature mismatch.

## Installation

Extract this archive into the repository root. The resulting paths should include:

- `paper_experiments/scenarios/paper_clean.yaml`
- `paper_experiments/run_experiments.ps1`
- `paper_experiments/evaluate_baseline.py`

Activate the repository virtual environment, then run the E1 matrix:

```powershell
python paper_experiments/run_e1.py
```

The per-process `Bypass` setting allows the script to run on Windows systems
whose default execution policy blocks local PowerShell scripts; it does not
change the machine or user execution policy.

Use `--skip-generation` to re-evaluate already finalized run directories. The
older `run_experiments.ps1` remains as a reproducibility path for the original
single-seed pilot only.

## Preserved outputs

All compact, citable E1 artifacts live in
`paper_experiments/results/E1_multiseed/`:

- `E1_REPORT.md`: paper-ready primary table and provenance;
- `INTERPRETATION.md`: claim boundaries, limitations, and paper-ready prose;
- `e1_per_run.csv`: all fixed and per-seed-threshold measurements;
- `e1_summary.csv`: mean and sample SD by condition;
- `e1_thresholds.csv`: selected threshold for every seed;
- `e1_validation.json`: pre-registered direction and variance checks;
- `e1_artifact_hashes.csv`: hashes linking metrics to generated datasets; and
- `configs/`: exact generated Phase-2 inputs.

Bulky run folders remain in the repository's already ignored `phase2/runs/`
location. Their run IDs and SHA-256 hashes are preserved in the compact result
package.

## Git publication boundary

Git contains the source, preregistrations, canonical and generated YAML
configurations, compact seed-level CSV/JSON summaries, fitted-model metadata,
reports, figures, validations, and SHA-256 manifests needed to audit every
reported value. Regenerable scored-pair Parquet files, duplicate per-run JSON,
generation logs, generated Phase-1 populations, and local timing/test scratch
files are excluded by `.gitignore`. Re-run the documented entry points to
recreate those local intermediates.

The full matcher contract is in `evaluation/BASELINE_MATCHER.md`; its stable
entry point is `evaluation/baseline_matcher.py`. The primary analysis holds the
pre-registered 0.65 threshold fixed. The sensitivity analysis retunes only on
each seed's clean condition and then applies that threshold to all four paired
conditions.

The true pair set is derived from `entity_record_map.csv`, including all valid A-to-B record combinations for duplicated entities. Reported metrics include candidate recall, candidate reduction ratio, pairwise precision, recall, F1, counts, runtime, and SHA-256 artifact hashes.

## E2-E6 matcher and difficulty experiments

The extended paper evaluation is indexed at
`paper_experiments/results/E2_E6/RESULTS_INDEX.md`. It contains:

- E2: three matcher families across all fourteen canonical scenarios and ten seeds;
- E3: a five-level, ten-seed noise gradient;
- E4: fixed versus oracle thresholds and precision-recall curves;
- E5: transitive-closure pairwise and B-cubed cluster metrics; and
- E6: co-resident non-match score distributions and false-positive pressure.

Reproduction entry points are `run_e2.py`, `run_e3.py`, `run_e4_e6.py`, and
`finalize_e2_e6.py`. The preregistered topology, calibration, threshold, and
metric definitions are in `E2_E6_PREREGISTRATION.md`.

## E7-E11 validity, rigor, archive, and external comparison

The second-round package has one entry point:
`paper_experiments/results/E7_E11/RESULTS_INDEX.md`. It preserves successful,
negative, and inconclusive findings together:

- E7: achieved-versus-target public-statistics calibration;
- E8: hardware, repeated runtime, and raw workload counts;
- E9: archive metadata, citation template, and Zenodo deposition status;
- E10: evidence-scoped capability comparison and related-work inventory; and
- E11: a checksum-pinned upstream-GeCo head-to-head analysis.

Run `run_e7.py`, `run_e8.py`, and `run_e11.py`; then run
`finalize_e7_e11.py` to validate the package and rebuild its SHA-256 manifest.
The estimands and decision rules are frozen in `E7_E11_PREREGISTRATION.md`.
