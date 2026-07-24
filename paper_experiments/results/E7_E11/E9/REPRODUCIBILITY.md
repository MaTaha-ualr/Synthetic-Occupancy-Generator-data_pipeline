# E9: reproducibility and archive record

## Experiment identity

- Artifact version/tag: `paper-artifact-2026.07.23`
- E1, E7, and E8 corrected run date: `2026-07-22`
- E2 corrected run date: `2026-07-24`
- E3 corrected run date: `2026-07-25`
- E11 corrected calibration/analysis date: `2026-07-27`
- Evaluation seeds: `20260720` through `20260729`
- Calibration seed where applicable: `20260719`
- Repository baseline named by the E7-E11 preregistration:
  `fb66cc61395794a4dbea22b27e6c2eb5850f43a7`
- Artifact commit: the commit targeted by the annotated tag. Resolve it with
  `git rev-list -n 1 paper-artifact-2026.07.23`.

The E1 four-condition configurations and the E2 fourteen-scenario
configurations are retained under `paper_experiments/results/E1_multiseed/`
and `paper_experiments/results/E2_E6/E2/configs/`, respectively. E11's held-out
calibration scenario is in `E11/configs/`. The artifact manifest at the E7-E11
root records a SHA-256 for every retained file.

## Exact commands

Run from the repository root in Windows PowerShell. The checked-in `.venv` was
used because it contains the frozen E2 Splink stack.

```powershell
.\.venv\Scripts\python.exe paper_experiments\run_e7.py
.\.venv\Scripts\python.exe paper_experiments\run_e8.py
.\.venv\Scripts\python.exe paper_experiments\run_e11.py --force
.\.venv\Scripts\python.exe -m pytest evaluation\tests -q
```

E10 is a literature-derived table and has no compute command. Its reviewed
sources and per-cell definitions are retained in CSV beside the report.

For E8, `--reuse-timings` may regenerate the report from the preserved timing
CSV, but it must not be used when independently reproducing runtime.

## Hardware and software

E8's exact machine record is in `../E8/e8_hardware.json`. E11's Python package
versions are in `../E11/environment.json`; the frozen E2 matcher versions and
thresholds remain in `../../E2_E6/E2/environment.json` and
`../../E2_E6/E2/models/thresholds.json`.

E11 additionally retains:

- the unmodified upstream GeCo archive under
  `paper_experiments/vendor/geco/`, with SHA-256
  `676b143d208b44a306002c05fb2b17d1a21c40e1972d4be7db8b03c130d0ab0c`;
- the mechanical Python 3 compatibility patch and MPL-2.0 license in
  `../E11/upstream/`;
- compact per-seed metrics, input hashes, and generator audit metadata.

Raw candidate-score Parquet files and duplicate per-run JSON records are
regenerable intermediates and are intentionally outside the Git publication
boundary.

## Paper-ready reproducibility paragraph

> Experiments were run on 21-22 July 2026 with preregistered evaluation seeds
> 20260720-20260729 and held-out calibration seed 20260719. We report sample
> standard deviations across seeds and retain exact scenario configurations,
> raw per-seed metrics, repeated runtimes, source and result hashes, matcher
> thresholds, and environment metadata in artifact version
> `paper-artifact-2026.07.23`. The immutable archive DOI and commit should be
> inserted from the release record before submission.

## Publication boundary

The local artifact can be committed and annotated-tagged without external
credentials. A GitHub release and Zenodo DOI cannot be claimed until an
authenticated upload returns real URLs and a DOI. `archive_status.json` is the
machine-readable status record; `zenodo_metadata.json` is ready for deposition.
