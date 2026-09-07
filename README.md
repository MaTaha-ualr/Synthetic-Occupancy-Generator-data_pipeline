<p align="center">
  <img src="docs/assets/sog-readme-hero.svg" alt="Synthetic Occupancy Generator — reproducible data for entity-resolution research" width="100%">
</p>

<p align="center">
  <a href="https://github.com/MaTaha-ualr/Synthetic-Occupancy-Generator-data_pipeline/tree/paper-artifact-2026.07.24"><img alt="Frozen paper artifact" src="https://img.shields.io/badge/paper_artifact-2026.07.24-4f46e5?style=flat-square"></a>
  <a href="phase2/scenarios/README.md"><img alt="14 canonical scenarios" src="https://img.shields.io/badge/canonical_scenarios-14-2563eb?style=flat-square"></a>
  <a href="#validation"><img alt="439 repository tests passed" src="https://img.shields.io/badge/repository_tests-439_passed-059669?style=flat-square"></a>
  <a href="docs/BEGINNER_GUIDE.md"><img alt="Python 3.10 or newer" src="https://img.shields.io/badge/python-3.10%2B-f59e0b?style=flat-square"></a>
  <a href="LICENSE"><img alt="MIT License" src="https://img.shields.io/badge/license-MIT-334155?style=flat-square"></a>
</p>

<p align="center">
  <strong>Deterministic synthetic populations, longitudinal truth, and noisy multi-source records for entity-resolution research.</strong>
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#benchmark-studio">Benchmark Studio</a> ·
  <a href="#population-scaling-benchmark">Scaling benchmark</a> ·
  <a href="#reproducibility-release">Paper artifact</a> ·
  <a href="#documentation">Documentation</a>
</p>

---

## Why SOG

Synthetic Occupancy Generator (SOG) builds an inspectable baseline population, simulates household and life events, emits imperfect observations into multiple sources, and preserves the ground truth required to evaluate entity-resolution systems.

| Population construction | Linkage stress | Evaluation truth |
|---|---|---|
| People, households, addresses, demographics, and configurable name distributions | Identity drift, missingness, overlap, duplication, household change, and source-specific corruption | Entity-record mappings, event histories, residence histories, manifests, checksums, and quality reports |

The pipeline is intended for deduplication, cross-source linkage, longitudinal identity resolution, household reconstruction, matcher benchmarking, and reproducible research.

## Architecture

SOG keeps configuration, truth, and observations separate throughout the workflow. A validated YAML scenario is always the authoritative experiment specification.

```text
Phase 1 configuration
        │
        ▼
Baseline people + households + addresses
        │
        ▼
Validated canonical or approved working YAML
        │
        ▼
Phase 2 truth simulation ──► events + histories + entity truth
        │
        ▼
Source emission ───────────► noisy observations + linkage maps
        │
        ▼
Output-contract validation + manifests + benchmark artifacts
```

<p align="center">
  <a href="docs/assets/figure%201.png">
    <img src="docs/assets/figure%201.png" alt="SOG end-to-end architecture" width="100%">
  </a>
</p>

### Highlights

- Seeded Phase 1 and Phase 2 workflows with recorded configuration and provenance.
- Fourteen unchanged canonical scenarios covering clean linkage, movers, household transitions, sparse coverage, duplication, name change, death persistence, and related stressors.
- Truth-aware output contracts for people, households, memberships, residences, events, and entity-record mappings.
- Configurable field corruption, missingness, overlap, duplication, topology, and cardinality.
- Baseline, learned, and Splink evaluation workflows with pairwise and cluster metrics.
- A local Streamlit Benchmark Studio for guided authoring, validation, execution, inspection, and export.
- An optional proposal-only natural-language layer that cannot bypass validation or write generated data.

## Quick start

### 1. Install

```powershell
git clone https://github.com/MaTaha-ualr/Synthetic-Occupancy-Generator-data_pipeline.git
Set-Location Synthetic-Occupancy-Generator-data_pipeline

python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Generate the Phase 1 population

```powershell
python phase1/scripts/build_prepared.py
python phase1/scripts/generate_phase1.py --overwrite
```

Phase 2 resolves the generated `phase1/outputs` files automatically, including scenarios that retain the historical `phase1/outputs_phase1` path.

### 3. Run a validated Phase 2 scenario

```powershell
python phase2/scripts/build_phase2_params.py
python phase2/scripts/run_phase2_pipeline.py `
  --scenario clean_baseline_linkage `
  --run-date 2026-03-10
```

Each run directory contains observed datasets, truth tables, the resolved scenario, manifest, quality report, and a run-specific README. Begin with [`clean_baseline_linkage`](phase2/scenarios/clean_baseline_linkage.yaml), then use the [scenario support matrix](phase2/docs/SCENARIO_SUPPORT_MATRIX.md) to select other benchmark conditions.

## Benchmark Studio

Launch the local interface:

```powershell
.\run_frontend.ps1
```

Open [http://localhost:8501](http://localhost:8501). The interface places experiment authoring and review in the primary canvas, with workflow status, provider setup, session information, and the YAML authority boundary in a supporting rail.

### Optional natural-language authoring

Copy [`.env.example`](.env.example) to `.env`, then provide an NVIDIA developer key through `NVIDIA_API_KEY` or the masked field in the interface. The default provider model is `nvidia/nemotron-3.5-lightning-30b-a3b`; the endpoint and model remain configurable. `ANTHROPIC_API_KEY` is optional and is used only for the separate conversational analysis/export assistant.

```text
Plain-English requirements
        ↓
Read-only structured proposal
        ↓
Existing schema + semantic validation
        ↓
Explicit user approval
        ↓
Resolved canonical YAML
        ↓
Existing deterministic SOG pipeline
```

The model has no generation tools and cannot modify truth tables, records, events, or benchmark outputs. See the [scenario authoring security and validation guide](docs/SCENARIO_AUTHORING.md) and [frontend runbook](docs/FRONTEND_RUNBOOK.md).

## Population-scaling benchmark

[`experiments/run_scaling_benchmark.py`](experiments/run_scaling_benchmark.py) provides a resumable benchmark across 10,000, 50,000, 100,000, 250,000, 500,000, and 1,000,000 requested people using three fixed seeds, a 1.4 records-per-person Phase 1 target, and the unchanged `clean_baseline_linkage` scenario.

```powershell
# Complete or resume all planned runs
python experiments/run_scaling_benchmark.py

# Rebuild reports from recorded rows without rerunning generation
python experiments/run_scaling_benchmark.py --report-only
```

The checked-in baseline is intentionally transparent about incomplete measurements: all three primary seeds completed through 500,000 people; the 1,000,000-person runs are not reported as measured. Reproducibility reruns completed through 250,000 people, and the 250,000-person checksum mismatch is retained as a finding rather than hidden. No paper result or extrapolated value is substituted for a missing run.

| Benchmark artifact | Purpose |
|---|---|
| [scaling results](experiments/scaling_results.csv) | Per-run population, throughput, memory, artifact, validation, name-collision, duplication, and overlap metrics |
| [scaling summary](experiments/scaling_summary.csv) | Aggregate metrics by requested population |
| [reproducibility checks](experiments/scaling_reproducibility.csv) | Same-seed truth and output checksum comparisons |
| [environment](experiments/scaling_environment.json) | CPU, RAM, OS, Python, dependencies, Git revision, and benchmark configuration |
| [report](experiments/scaling_report.md) | Human-readable results, name-collision table, limitations, and reproduction commands |

Generated trial payloads belong under the gitignored `experiments/scaling_artifacts/` directory. The summarized CSV, JSON, and Markdown evidence above is versioned.

## Reproducibility release

The paper evidence remains frozen at [`paper-artifact-2026.07.24`](https://github.com/MaTaha-ualr/Synthetic-Occupancy-Generator-data_pipeline/tree/paper-artifact-2026.07.24). This feature release does not rewrite its canonical scenarios or benchmark results.

| Frozen release evidence | Validated result |
|---|---:|
| Evaluation and Phase 2 tests | 378 passed |
| Hashed bundle outputs | 159 |
| Seed-level configuration mappings | 230 |
| Bootstrap replicates | 20,000 |

Start with the [paper revision bundle](paper_revision_bundle/README.md), then inspect its [artifact manifest](paper_revision_bundle/artifact_manifest.json), [matcher protocol](paper_revision_bundle/matcher_protocol.json), and editable [paper tables](paper_revision_bundle/paper_tables/).

> [!NOTE]
> The repository contains a versioned Git artifact, not a DOI-backed public archive. The bundle records preregistration boundaries, calibration details, null findings, and unresolved limitations explicitly.

## Documentation

| Goal | Start here |
|---|---|
| Understand the system | [SOG Bible](docs/SOG_BIBLE.md) · [PDF](The_SOG_Bible_v2.pdf) |
| Follow a guided demonstration | [Professor walkthrough](docs/SOG_PROFESSOR_WALKTHROUGH.md) |
| Install and run SOG | [Beginner guide](docs/BEGINNER_GUIDE.md) |
| Configure Phase 1 | [Phase 1 guide](phase1/README.md) |
| Work with Phase 2 | [Phase 2 guide](phase2/README.md) |
| Choose a canonical scenario | [Scenario use cases](phase2/docs/SCENARIO_USE_CASES_AND_TESTING.md) |
| Tune parameters | [Parameter tuning playbook](docs/reference/PARAMETER_TUNING_PLAYBOOK.md) |
| Operate the frontend | [Frontend runbook](docs/FRONTEND_RUNBOOK.md) |
| Author from plain English | [Scenario authoring guide](docs/SCENARIO_AUTHORING.md) |
| Review ownership and status | [Handoff](docs/HANDOFF.md) |

<details>
<summary><strong>Repository map</strong></summary>

```text
.
├── phase1/                  baseline population generation
├── phase2/                  truth simulation, source emission, and validation
├── evaluation/              matcher protocols and evaluation metrics
├── frontend/                local Streamlit Benchmark Studio
├── experiments/             population-scaling runner and summarized evidence
├── paper_experiments/       frozen paper experiment runners and results
├── paper_revision_bundle/   frozen paper evidence and editable tables
├── docs/                    guides, references, and architecture notes
├── tests/                   frontend and orchestration tests
├── requirements.txt
└── run_frontend.ps1
```

</details>

## Validation

Run the active repository suite:

```powershell
python -m pytest -q
```

The release-preparation run completed with **439 tests passed**.

Validate the frozen paper bundle and release contract separately:

```powershell
python paper_experiments/validate_paper_revision_bundle.py `
  --release-tag paper-artifact-2026.07.24
```

## Maintainer

Maintained by **Ammar Ahmed Taha Mohammed** ([@MaTaha-ualr](https://github.com/MaTaha-ualr)).

## License

Released under the [MIT License](LICENSE).
