"""Per-run README generation.

Every Phase-2 run folder gets a ``README.md`` that summarizes *that specific run*
(scenario, seed, counts, files) in plain language, so a folder dropped on someone's
desk is self-explanatory without reading the source.

This module is additive and read-only with respect to the rest of the pipeline:
it only reads ``manifest.json`` / ``quality_report.json`` / ``scenario.yaml`` that
already exist in the run folder and writes a single ``README.md`` next to them.
The authoritative column-by-column reference lives in ``phase2/runs/README.md``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


# One-line descriptions for the fixed-name artifacts a run may contain.
_FILE_DESCRIPTIONS: dict[str, str] = {
    "truth_people.parquet": "Ground-truth person registry (one row per real person, including children born during the simulation).",
    "truth_households.parquet": "Ground-truth households with their lifespan (start/end dates).",
    "truth_household_memberships.parquet": "Who belonged to which household over which date interval, with role.",
    "truth_residence_history.parquet": "Each person's address timeline (non-overlapping intervals).",
    "truth_events.parquet": "The simulated life events (MOVE/COHABIT/BIRTH/DIVORCE/LEAVE_HOME/DEATH/NAME_CHANGE/ADOPTION) that drove every change.",
    "scenario_population.parquet": "The deterministically selected participants plus their latent propensity scores.",
    "entity_record_map.csv": "CANONICAL answer key: maps every observed RecordKey (in any dataset) to its true PersonKey.",
    "masterDataset.csv": "Deduplicated convenience stack of observed source rows, with PersonKey attached for deterministic entity-based ordering.",
    "truth_crosswalk.csv": "Pairwise (A/B) answer key for two-dataset runs — a positional view; see README note on duplicates.",
    "scenario.yaml": "The fully resolved scenario configuration this run was generated from.",
    "scenario_selection_log.json": "Audit log of participant selection (filter counts, seed, PersonKey checksum).",
    "manifest.json": "Machine-readable run manifest (inputs, outputs, simulation metadata).",
    "quality_report.json": "Truth-consistency checks plus ER-benchmark metrics for this run.",
}


def build_run_readme_text(run_dir: Path) -> str:
    """Build the markdown text for a single run's README from its own artifacts."""
    run_dir = Path(run_dir)
    manifest = _read_json(run_dir / "manifest.json")
    quality = _read_json(run_dir / "quality_report.json")
    scenario = _read_yaml(run_dir / "scenario.yaml")

    run_id = manifest.get("run_id") or quality.get("run_id") or run_dir.name
    scenario_id = manifest.get("scenario_id") or quality.get("scenario_id") or scenario.get("scenario_id", "?")
    seed = manifest.get("seed", quality.get("seed", scenario.get("seed", "?")))
    generated = manifest.get("generated_at_utc") or quality.get("generated_at_utc") or "?"

    emission_meta = manifest.get("emission_meta", {}) if isinstance(manifest.get("emission_meta"), dict) else {}
    match_mode = emission_meta.get("crossfile_match_mode", "?")
    sim_start = emission_meta.get("simulation_start_date", "?")
    sim_end = emission_meta.get("simulation_end_date", "?")
    dataset_ids = emission_meta.get("dataset_ids", [])

    truth_counts = quality.get("truth_counts", {}) if isinstance(quality.get("truth_counts"), dict) else {}
    sim_quality = quality.get("simulation_quality", {}) if isinstance(quality.get("simulation_quality"), dict) else {}
    event_counts = sim_quality.get("event_counts", {}) if isinstance(sim_quality.get("event_counts"), dict) else {}
    status = quality.get("status", "?")

    observed_outputs = manifest.get("observed_outputs", {}) if isinstance(manifest.get("observed_outputs"), dict) else {}
    datasets = observed_outputs.get("datasets", []) if isinstance(observed_outputs.get("datasets"), list) else []

    # ER overlap (only present for >=2 dataset runs)
    er = (
        quality.get("phase2_quality", {})
        .get("er_benchmark_metrics", {})
        if isinstance(quality.get("phase2_quality"), dict)
        else {}
    )
    overlap = er.get("cross_file_overlap") if isinstance(er, dict) else None

    lines: list[str] = []
    lines.append(f"# Run: `{run_id}`")
    lines.append("")
    lines.append(
        "Auto-generated summary of this Phase-2 benchmark run. "
        "For the full column-by-column reference of every file below, see "
        "[`phase2/runs/README.md`](../README.md)."
    )
    lines.append("")
    lines.append("## At a glance")
    lines.append("")
    lines.append("| Field | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Scenario | `{scenario_id}` |")
    lines.append(f"| Seed | `{seed}` |")
    lines.append(f"| Generated (UTC) | {generated} |")
    lines.append(f"| Cross-file match mode | `{match_mode}` |")
    lines.append(f"| Simulation window | {sim_start} → {sim_end} |")
    lines.append(f"| Observed datasets | {', '.join(str(d) for d in dataset_ids) or '—'} |")
    lines.append(f"| Quality status | **{status}** |")
    lines.append("")

    if truth_counts:
        lines.append("## Truth layer (ground truth)")
        lines.append("")
        lines.append("| Table | Rows |")
        lines.append("| --- | --- |")
        for key in (
            "truth_people",
            "truth_households",
            "truth_household_memberships",
            "truth_residence_history",
            "truth_events",
        ):
            if key in truth_counts:
                lines.append(f"| `{key}` | {truth_counts[key]} |")
        lines.append("")

    if event_counts:
        lines.append("### Simulated events")
        lines.append("")
        pretty = ", ".join(f"`{k}`={v}" for k, v in sorted(event_counts.items()))
        lines.append(pretty)
        lines.append("")

    if datasets:
        lines.append("## Observed layer (what an ER system receives)")
        lines.append("")
        lines.append("| Dataset | File |")
        lines.append("| --- | --- |")
        for item in datasets:
            if isinstance(item, dict):
                lines.append(f"| `{item.get('dataset_id','?')}` | `{item.get('filename','?')}` |")
        lines.append("")
        if isinstance(overlap, dict):
            lines.append(
                f"Cross-file overlap: **{overlap.get('overlap_entities','?')}** shared entities "
                f"({overlap.get('overlap_pct_of_union', 0):.1f}% of the union)."
            )
            lines.append("")

    # File inventory (only list files that actually exist).
    lines.append("## Files in this folder")
    lines.append("")
    lines.append("| File | What it is |")
    lines.append("| --- | --- |")
    present = sorted(p.name for p in run_dir.iterdir() if p.is_file() and p.name != "README.md")
    for name in present:
        desc = _FILE_DESCRIPTIONS.get(name)
        if desc is None:
            if name.startswith("truth_crosswalk__"):
                desc = "Per-pair answer key for a specific dataset pair (3+ dataset runs)."
            elif name.startswith("observed_") and name.endswith(".csv"):
                desc = "An observed (noisy) dataset emitted from a truth snapshot."
            elif name.endswith(".csv"):
                desc = "An observed (noisy) dataset emitted from a truth snapshot."
            else:
                desc = "Run artifact."
        lines.append(f"| `{name}` | {desc} |")
    lines.append("")
    lines.append("## How to use the answer keys")
    lines.append("")
    lines.append(
        "- Use **`entity_record_map.csv`** as the canonical ground truth: every record that "
        "shares a `PersonKey` is the same real person (transitively complete, across all datasets)."
    )
    lines.append(
        "- **`masterDataset.csv`** is the convenience observed table: it stacks all observed "
        "datasets, adds `PersonKey`, collapses exact same-source duplicate payloads, and also "
        "adds `DatasetId=TIMELINE` rows from `truth_residence_history.parquet`. Filter "
        "`RecordType=source_snapshot` for only A/B source rows, or `RecordType=residence_timeline` "
        "to see every simulated residence interval."
    )
    lines.append(
        "- **`truth_crosswalk.csv`** is a backward-compatible *pairwise* view for two-dataset runs. "
        "It pairs records positionally, so when an entity has within-file duplicates some of its "
        "records appear with the opposite side left blank. For clustering/transitive scoring prefer "
        "`entity_record_map.csv`."
    )
    lines.append("")
    lines.append("_Regenerate this file with_ `python phase2/scripts/write_run_readmes.py`.")
    lines.append("")
    return "\n".join(lines)


def write_run_readme(run_dir: Path) -> Path:
    """Write ``README.md`` into ``run_dir`` and return the path."""
    run_dir = Path(run_dir)
    target = run_dir / "README.md"
    target.write_text(build_run_readme_text(run_dir), encoding="utf-8")
    return target
