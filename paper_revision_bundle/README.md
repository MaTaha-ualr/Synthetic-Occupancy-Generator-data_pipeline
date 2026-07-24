# Paper revision bundle

Generated at `2026-07-24T06:12:57.537539+00:00` while HEAD was `67195a1b86dfe2b1bf20eb41133dfc90cf4e56d9` on branch `main`.

This bundle is generated from checked-in unrounded E1-E11 results plus a new, seed-disjoint household Splink calibration-transfer experiment. No historical result file was overwritten.

## Commands executed

Material commands that generated or verified scientific outputs are listed in execution order. Read-only repository inspection commands are not scientific runs.

1. `.venv\Scripts\python.exe paper_experiments\run_household_transfer.py --workers 2` — first launch terminated by the five-second shell-wrapper timeout before a result.
2. `.venv\Scripts\python.exe paper_experiments\run_household_transfer.py --workers 2` — orchestration exited nonzero after the concurrent generators wrote complete validated calibration artifacts.
3. `.venv\Scripts\python.exe paper_experiments\run_household_transfer.py --workers 2` — passed on resume; verified/reused three seed-20260719 household calibration runs, trained three scenario-specific Splink models, and evaluated 60 existing household runs.
   - `C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\.venv\Scripts\python.exe C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\phase2\scripts\run_phase2_pipeline.py --scenario-yaml C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\paper_revision_bundle\_provenance\household_transfer\configs\paper_revision_household_adoption_blended_family_calibration_s20260719.yaml --runs-root C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\paper_revision_bundle\_provenance\household_transfer\runs --run-date 2026-07-23 --rebuild-population --no-progress` — reused_validated (return code 0, 0.0 s).
   - `C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\.venv\Scripts\python.exe C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\phase2\scripts\run_phase2_pipeline.py --scenario-yaml C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\paper_revision_bundle\_provenance\household_transfer\configs\paper_revision_household_couple_merge_calibration_s20260719.yaml --runs-root C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\paper_revision_bundle\_provenance\household_transfer\runs --run-date 2026-07-23 --rebuild-population --no-progress` — reused_validated (return code 0, 0.0 s).
   - `C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\.venv\Scripts\python.exe C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\phase2\scripts\run_phase2_pipeline.py --scenario-yaml C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\paper_revision_bundle\_provenance\household_transfer\configs\paper_revision_household_family_birth_calibration_s20260719.yaml --runs-root C:\Users\Rudra\Downloads\Synthetic-Occupancy-Generator-data_pipeline-main\Synthetic-Occupancy-Generator-data_pipeline-main\paper_revision_bundle\_provenance\household_transfer\runs --run-date 2026-07-23 --rebuild-population --no-progress` — reused_validated (return code 0, 0.0 s).
4. `.venv\Scripts\python.exe -m py_compile paper_experiments\run_household_transfer.py paper_experiments\build_paper_revision_bundle.py paper_experiments\validate_paper_revision_bundle.py` — passed preflight compilation (repeated after targeted fixes).
5. `.venv\Scripts\python.exe -m pytest evaluation/tests phase2/tests -q --junitxml=paper_revision_bundle/_provenance/pytest.xml` — passed; 378 passed, 0 failed, 0 skipped in 143.929 seconds.
6. `.venv\Scripts\python.exe paper_experiments\build_paper_revision_bundle.py --test-junit paper_revision_bundle/_provenance/pytest.xml` — first derivation stopped because the assumed E3 `k=1` directory had no truth files; executable `run_e3.py` showed that `k=1` deliberately reuses the E2 high-noise run.
7. `.venv\Scripts\python.exe paper_experiments\build_paper_revision_bundle.py --test-junit paper_revision_bundle/_provenance/pytest.xml` — passed after mapping E3 to its actual source runs; derived all requested CSV/JSON/table files.
8. `.venv\Scripts\python.exe paper_experiments\validate_paper_revision_bundle.py` — passed final read-only bundle validation.

## Run failures

- No requested scientific output remains failed. The first short shell-wrapper launch timed out after five seconds and was terminated before completion; the resumable long-running launch replaced it.
- During the first concurrent calibration-generation launch, `family_birth` and `adoption_blended_family` subprocesses returned nonzero after writing complete manifests with `quality_status: ok` and `validation_valid: true`. The continuation verified and reused those artifacts. This exit/artifact mismatch is retained as a run discrepancy rather than hidden.
- Early read-only setup probes found that the outer download wrapper is not the Git worktree, `pdftotext` is not installed, and historical score-parquet cache directories are empty. The nested repository, Python PDF fallback, current Phase 2 run artifacts, and regenerated scores were used; these probes changed no result.

## Requested files

- `artifact_manifest.json`: Git/environment/test provenance plus output and source hashes.
- `matcher_protocol.json`: executable blocking, features, similarities, fitting, calibration, thresholds, and seed separation.
- `config_mapping.csv`: 230 seed-level Table I-III YAML/truth mappings with complete hashes and config differences.
- `derived_claims.json`: unrounded formulas, values, displayed values, and exact rank/tie handling.
- `paired_statistics.csv`: seed rows and paired t/Wilcoxon/sign summaries with Holm adjustment for the 28 Table II contrasts.
- `oracle_bootstrap.json`: 20,000 paired-seed bootstrap replicates and threshold-procedure disclosure.
- `noise_trend.csv`: per-seed/per-level results plus discrete monotonicity and endpoint tests.
- `cluster_metrics.csv`: per-seed and summary pairwise, B3, and closure metrics for E1 and E2 E5 inputs.
- `household_transfer_experiment.csv`: clean-frozen versus scenario-specific Splink calibration.
- `household_full_matrix.csv`: all three matchers for all six household scenarios.
- `runtime_results.csv`: E1 independent-seed variability separated from E8 fixed-population repeats.
- `public_target_conversion.csv`: units, denominators, conversions, per-seed outcomes, construction status, and JS definition.
- `paper_tables/`: CSV, LaTeX `longtable`, and Word-compatible DOCX for Tables I-VI and Figure 2 data.

Paper-table row counts: `table_i`=4, `table_ii`=42, `table_iii`=15, `table_iv`=18, `table_v`=27, `table_vi`=11, `figure_2_data`=15.

## Hash definitions

A truth-layer digest is the SHA-256 of sorted `filename NUL file-SHA-256 NUL` entries for truth people, households, household memberships, residence history, events, and the entity-record map. The component hashes are retained in `config_mapping.csv`.

## Unresolved discrepancies and claim boundaries

- The current relevant test command reports 378 passed, not the manuscript's stale 368-test statement.
- The repository has release tag `paper-artifact-2026.07.23`, but the checked-in archive status still has no minted DOI. Do not claim a DOI-backed public archive until one exists.
- The target package intentionally retains preregistered provisional 2024 NCHS fertility values. A later final 2024 release is not silently substituted; updating it requires a versioned parameter change and dependent rerun.
- Scenario-specific Splink calibration is a new supplemental analysis, not part of the original preregistration. It uses one held-out calibration population (seed 20260719) and ten disjoint evaluation seeds.
- Each scenario-specific Splink model left at least one fuzzy-street m-probability untrained because that comparison level was not observed during EM; Splink 4.0.16 used its default at prediction time. Exact affected levels are machine-readable in `matcher_protocol.json`.
- Splink's saved artifact does not expose realized unsupervised u-sample and EM row counts. `matcher_protocol.json` reports this as unavailable instead of inventing labeled training counts; threshold-calibration pair counts are exact.
- E4 oracle thresholds are selected per seed over exact observed score boundaries, not the legacy 0.01 baseline calibration grid. They are descriptive in-sample upper bounds.
- Household-type fidelity is largely by construction. Household-size divergence remains unestimable from B11001, and ACS margins of error are absent from the checked-in target files.
- The CDC marriage statistic is not semantically comparable with the simulator's COHABIT event. Divorce and fertility denominators/eligibility are documented explicitly.
- The capability matrix is regenerated from checked-in E10 evidence but universal literature-completeness claims still require external verification.
- Apparent Table II ties at four decimals are retained as displayed ties even when unrounded means differ. See `derived_claims.json`.

## Reproduction note

For a clean reproduction, run the passing household, test, build, and validation commands above from the repository root with the repository `.venv`. The household script is resumable: it reuses calibration runs/models and per-run audits only when their source hashes match.
