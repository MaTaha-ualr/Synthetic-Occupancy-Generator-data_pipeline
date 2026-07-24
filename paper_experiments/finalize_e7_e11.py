"""Validate and index the E7-E11 artifact package."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
ROOT = REPO / "paper_experiments/results/E7_E11"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def validate() -> dict[str, object]:
    e7 = load_json(ROOT / "E7/e7_validation.json")
    e8 = load_json(ROOT / "E8/e8_validation.json")
    e11 = load_json(ROOT / "E11/e11_validation.json")
    archive = load_json(ROOT / "E9/archive_status.json")
    e7_summary = pd.read_csv(ROOT / "E7/e7_summary.csv")
    e8_runtime = pd.read_csv(ROOT / "E8/e8_runtime_repetitions.csv")
    e8_workload = pd.read_csv(ROOT / "E8/e8_workload_per_seed.csv")
    e10 = pd.read_csv(ROOT / "E10/e10_capability_matrix.csv")
    e11_runs = pd.read_csv(ROOT / "E11/e11_per_run.csv")
    checks = {
        "e7_seed_count_is_10": e7.get("seed_count") == 10,
        "e7_no_comparable_large_deviations": e7.get("comparable_large_deviation_count") == 0,
        "e7_has_10_independent_populations": e7.get("independent_phase1_population_count") == 10,
        "e7_summary_nonempty": len(e7_summary) > 0,
        "e8_has_20_runtime_rows": len(e8_runtime) == 20,
        "e8_has_40_workload_rows": len(e8_workload) == 40,
        "e8_candidate_counts_stable": e8.get("candidate_count_stable_within_condition") is True,
        "e8_score_digests_stable": e8.get("score_label_digest_stable_within_condition") is True,
        "e10_has_11_capabilities": len(e10) == 11,
        "e10_has_6_system_columns": len([name for name in e10 if name not in ("feature", "operational_definition")]) == 6,
        "e11_has_60_metric_rows": len(e11_runs) == 60,
        "e11_size_contract_exact": e11.get("all_size_contracts_exact") is True,
        "e11_reproduces_e2": e11.get("sog_metrics_reproduce_e2") is True,
        "e11_upstream_hash_matches": e11.get("upstream_archive_hash_matches_preregistration") is True,
        "e11_null_or_bad_result_retained": e11.get("bad_or_null_result_retained") is True,
        "e9_doi_not_fabricated": archive["zenodo_archive"]["doi"] is None and archive["zenodo_archive"]["published"] is False,
    }
    validation = {
        "all_internal_checks_pass": all(checks.values()),
        "checks": checks,
        "experiment_verdicts": {
            "E7": "passes fidelity-to-parameterization calibration; scoped limitations retained",
            "E8": "complete",
            "E9": "local archive-ready; GitHub release and Zenodo DOI pending authentication",
            "E10": "supports capability-composition novelty only",
            "E11": e11["novelty_verdict"],
        },
    }
    (ROOT / "PACKAGE_VALIDATION.json").write_text(json.dumps(validation, indent=2), encoding="utf-8")
    if not validation["all_internal_checks_pass"]:
        failed = [name for name, passed in checks.items() if not passed]
        raise RuntimeError(f"E7-E11 package validation failed: {failed}")
    return validation


def artifact_paths() -> list[tuple[str, Path]]:
    selected: list[tuple[str, Path]] = []
    for path in ROOT.rglob("*"):
        if path.is_file() and path.name != "ARTIFACT_MANIFEST.csv":
            selected.append(("E7_E11_result", path))
    explicit = [
        REPO / "paper_experiments/README.md",
        REPO / "paper_experiments/E7_E11_PREREGISTRATION.md",
        REPO / "paper_experiments/run_e7.py",
        REPO / "paper_experiments/run_e8.py",
        REPO / "paper_experiments/run_e11.py",
        REPO / "paper_experiments/finalize_e7_e11.py",
        REPO / "evaluation/external_validity.py",
        REPO / "evaluation/geco_adapter.py",
        REPO / "evaluation/metrics.py",
        REPO / "evaluation/tests/test_external_validity.py",
        REPO / "evaluation/tests/test_geco_adapter.py",
        REPO / "evaluation/tests/test_metrics.py",
    ]
    selected.extend(("evaluation_source", path) for path in explicit)
    selected.extend(
        ("pinned_upstream", path)
        for path in (REPO / "paper_experiments/vendor/geco").rglob("*")
        if path.is_file()
    )
    selected.extend(
        ("E1_scenario_config", path)
        for path in (REPO / "paper_experiments/results/E1_multiseed/configs").glob("*.yaml")
    )
    selected.extend(
        ("E2_scenario_config", path)
        for path in (REPO / "paper_experiments/results/E2_E6/E2/configs").glob("*.yaml")
    )
    unique = {(scope, path.resolve()) for scope, path in selected}
    return sorted(unique, key=lambda value: (value[0], str(value[1]).lower()))


def write_manifest() -> int:
    paths = artifact_paths()
    with (ROOT / "ARTIFACT_MANIFEST.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=("scope", "path", "bytes", "sha256"))
        writer.writeheader()
        for scope, path in paths:
            writer.writerow({
                "scope": scope,
                "path": path.relative_to(REPO).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            })
    return len(paths)


def write_index(manifest_count: int) -> None:
    lines = [
        "# E7-E11 results index",
        "",
        "This is the single entry point for the second-round paper experiments. Negative and inconclusive findings are retained alongside successful measurements.",
        "",
        "| Experiment | Outcome | Paper value | Primary report |",
        "| --- | --- | --- | --- |",
        "| E7: external validity | Calibrated fidelity achieved | Overall and working-age mobility, household type, and divorce are close; scoped limitations remain | [E7 report](E7/E7_REPORT.md) |",
        "| E8: measurement rigor | Complete | Replaces single runtimes and rounded reduction ratios with hardware, repetitions, and raw workload | [E8 report](E8/E8_REPORT.md) |",
        "| E9: artifact archive | Locally archive-ready; DOI pending | Exact dates, seeds, commands, version, citation template, and deposition metadata | [E9 record](E9/REPRODUCIBILITY.md) |",
        "| E10: capability comparison | Supports composition-level novelty | Useful related-work table if phrased as an evidence-scoped combination claim | [E10 report](E10/E10_REPORT.md) |",
        "| E11: GeCo head-to-head | Mixed; joint novelty criterion not met | Supplemental negative result: distinctive co-address pressure, but no global-difficulty advantage and poor noise match | [E11 report](E11/E11_REPORT.md) |",
        "",
        "## Recommended paper use",
        "",
        "- Use E7 as evidence of **fidelity to declared public targets**, not equivalence to confidential administrative data. Retain the moderate, sparse-stratum, and semantic-comparability qualifications.",
        "- Use E8 directly in the experimental setup and replace all rounded reduction-ratio claims with raw candidate counts and Cartesian spaces.",
        "- Use E10 for the novelty section, explicitly claiming a distinctive **combination** of capabilities rather than invention of every component.",
        "- E11 uses learned and Splink models calibrated once on clean SOG and frozen without GeCo-specific retuning. Under that transfer setting it preserves a real household/address false-positive signal, but candidate recall and residual noise mismatch preclude a global difficulty comparison.",
        "- Do not cite a Zenodo DOI until E9's authenticated deposition is complete.",
        "",
        "## Package integrity",
        "",
        f"`ARTIFACT_MANIFEST.csv` contains {manifest_count} SHA-256 records spanning E7-E11 outputs, evaluation source, pinned GeCo source, and all exact E1/E2 scenario configurations. `PACKAGE_VALIDATION.json` records the cross-experiment acceptance checks.",
    ]
    (ROOT / "RESULTS_INDEX.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ROOT.mkdir(parents=True, exist_ok=True)
    validate()
    # Write the index before the manifest so the index itself is hashed.
    write_index(0)
    manifest_count = write_manifest()
    write_index(manifest_count)
    # Rebuild after updating the embedded count.
    manifest_count = write_manifest()
    print(json.dumps({"package_valid": True, "manifest_records": manifest_count}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
