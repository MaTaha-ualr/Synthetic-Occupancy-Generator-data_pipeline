"""Unified Phase-2 pipeline entry point.

Runs the complete pipeline in a single call:
  selection → truth simulation → observed emission → quality → manifest → validation

This module is additive — it does not modify any existing module or script.
All existing CLI scripts (generate_phase2_truth.py, generate_phase2_observed.py,
validate_phase2_outputs.py) are preserved exactly as-is.
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Path helpers (extracted from generate_phase2_truth.py:22-42)
# ---------------------------------------------------------------------------

def _resolve_with_legacy_fallback(project_root: Path, configured_path: str) -> Path:
    candidate = (project_root / configured_path).resolve()
    if candidate.exists():
        return candidate
    legacy_map = {
        "outputs_phase1/Phase1_people_addresses.csv": "phase1/outputs_phase1/Phase1_people_addresses.csv",
        "outputs_phase1/Phase1_people_addresses.manifest.json": "phase1/outputs_phase1/Phase1_people_addresses.manifest.json",
        "outputs_phase1/Phase1_people_addresses.quality_report.json": "phase1/outputs_phase1/Phase1_people_addresses.quality_report.json",
        "outputs/Phase1_people_addresses.csv": "phase1/outputs_phase1/Phase1_people_addresses.csv",
        "outputs/Phase1_people_addresses.manifest.json": "phase1/outputs_phase1/Phase1_people_addresses.manifest.json",
        "outputs/Phase1_people_addresses.quality_report.json": "phase1/outputs_phase1/Phase1_people_addresses.quality_report.json",
    }
    normalized = configured_path.replace("\\", "/")
    if normalized in legacy_map:
        legacy_candidate = (project_root / legacy_map[normalized]).resolve()
        if legacy_candidate.exists():
            return legacy_candidate
    return candidate


# ---------------------------------------------------------------------------
# Date helpers (extracted from generate_phase2_observed.py:37-50)
# ---------------------------------------------------------------------------

def _add_months(value: date, months: int) -> date:
    month_index = (value.month - 1) + months
    year = value.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(value.day, int(pd.Period(f"{year}-{month:02d}").days_in_month))
    return date(year, month, day)


def _simulation_end_date(start_date: date, periods: int, granularity: str) -> date:
    if periods <= 0:
        return start_date
    if granularity == "monthly":
        return _add_months(start_date, periods)
    return start_date + pd.Timedelta(days=periods).to_pytimedelta()


# ---------------------------------------------------------------------------
# Quality status helper (deduplicated from both scripts)
# ---------------------------------------------------------------------------

def _compute_quality_status(
    constraints_validation: dict[str, Any],
    phase2_quality: dict[str, Any],
) -> str:
    truth_consistency = phase2_quality.get("truth_consistency", {})
    event_age_count = int(
        (truth_consistency.get("event_age_validation") or {}).get("invalid_event_age_count", 0)
    )
    no_overlap = bool(
        (truth_consistency.get("time_overlap_errors") or {}).get("no_time_overlap_errors", True)
    )
    household_ok = bool(
        (truth_consistency.get("household_size_constraints") or {}).get("within_config_constraints", True)
    )
    constraints_valid = bool(constraints_validation.get("valid", True))
    if constraints_valid and event_age_count == 0 and no_overlap and household_ok:
        return "ok"
    return "quality_issues_detected"


# ---------------------------------------------------------------------------
# YAML loader
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping in {path}")
    return data


# ---------------------------------------------------------------------------
# Emission YAML payload builder (field renaming for scenario.yaml write)
# Mirrors the exact renaming in generate_phase2_observed.py:177-186
# ---------------------------------------------------------------------------

def _build_emission_yaml_payload(emission_cfg: Any) -> dict[str, Any]:
    payload = asdict(emission_cfg)
    is_legacy = bool(payload.pop("is_legacy_pairwise", False))
    datasets = payload.pop("datasets", [])
    if is_legacy:
        payload["appearance_A_pct"] = payload.pop("appearance_a_pct")
        payload["appearance_B_pct"] = payload.pop("appearance_b_pct")
        payload["duplication_in_A_pct"] = payload.pop("duplication_in_a_pct")
        payload["duplication_in_B_pct"] = payload.pop("duplication_in_b_pct")
        payload["noise"] = {
            "A": payload.pop("dataset_a_noise"),
            "B": payload.pop("dataset_b_noise"),
        }
        return payload
    payload.pop("appearance_a_pct", None)
    payload.pop("appearance_b_pct", None)
    payload.pop("duplication_in_a_pct", None)
    payload.pop("duplication_in_b_pct", None)
    payload.pop("dataset_a_noise", None)
    payload.pop("dataset_b_noise", None)
    payload["datasets"] = datasets
    return payload


# ---------------------------------------------------------------------------
# Main pipeline function
# ---------------------------------------------------------------------------

def run_scenario_pipeline(
    *,
    scenario_yaml_path: Path,
    runs_root: Path,
    project_root: Path,
    run_date: str | None = None,
    overwrite: bool = False,
    rebuild_population: bool = False,
) -> dict[str, Any]:
    """Run the complete Phase-2 pipeline in a single call.

    Stages:
      1. Load & configure (YAML → parsed configs → run_id)
      2. Selection (generate_scenario_population_from_files)
      3. Truth simulation (simulate_truth_layer)
      4. Observed emission (emit_observed_datasets)
      5. Quality + manifest (single write, no two-pass patching)
      6. Contract validation (validate_phase2_run)

    Returns a dict with: run_id, run_dir, truth_counts, event_counts,
    observed_counts, quality_status, validation_valid, paths.
    """
    src_dir = project_root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    from sog_phase2.constraints import parse_constraints_config, validate_constraints_against_truth
    from sog_phase2.emission import parse_emission_config, emit_observed_datasets
    from sog_phase2.output_contract import (
        build_run_id,
        expected_observed_dataset_paths,
        expected_pairwise_crosswalk_paths,
        expected_phase2_run_artifact_paths,
        parse_run_id,
        validate_phase2_run,
    )
    from sog_phase2.params import load_phase2_params_from_project
    from sog_phase2.quality import compute_phase2_quality_report, parse_quality_config
    from sog_phase2.selection import generate_scenario_population_from_files, parse_selection_config
    from sog_phase2.simulator import parse_simulation_config, simulate_truth_layer

    # ------------------------------------------------------------------
    # Stage 1: Load & configure
    # ------------------------------------------------------------------
    scenario_yaml_path = scenario_yaml_path.resolve()
    if not scenario_yaml_path.exists():
        raise FileNotFoundError(f"Scenario YAML not found: {scenario_yaml_path}")

    scenario = _load_yaml(scenario_yaml_path)
    scenario_id = str(scenario.get("scenario_id", "")).strip()
    if not scenario_id:
        raise ValueError("scenario_id is required in scenario YAML")

    run_seed = int(scenario.get("seed", 0))

    if run_date is None:
        seed_text = str(run_seed)
        if len(seed_text) == 8 and seed_text.isdigit():
            parsed_seed_date = pd.to_datetime(seed_text, format="%Y%m%d", errors="coerce")
            if not pd.isna(parsed_seed_date):
                run_date = parsed_seed_date.date().isoformat()
    if run_date is None:
        run_date = datetime.now(timezone.utc).date().isoformat()

    run_id = build_run_id(scenario_id, run_seed, run_date)
    run_info = parse_run_id(run_id)

    phase1_cfg = scenario.get("phase1", {})
    if not isinstance(phase1_cfg, dict):
        raise ValueError("scenario.phase1 must be a mapping")
    phase1_data_path_cfg = str(phase1_cfg.get("data_path", "")).strip()
    phase1_manifest_path_cfg = str(phase1_cfg.get("manifest_path", "")).strip()
    if not phase1_data_path_cfg:
        raise ValueError("scenario.phase1.data_path is required")
    if not phase1_manifest_path_cfg:
        raise ValueError("scenario.phase1.manifest_path is required")

    phase1_csv_path = _resolve_with_legacy_fallback(project_root, phase1_data_path_cfg)
    if not phase1_csv_path.exists():
        raise FileNotFoundError(f"Phase-1 CSV not found: {phase1_csv_path}")
    phase1_manifest_path = _resolve_with_legacy_fallback(project_root, phase1_manifest_path_cfg)
    if not phase1_manifest_path.exists():
        raise FileNotFoundError(f"Phase-1 manifest not found: {phase1_manifest_path}")

    phase2_params = load_phase2_params_from_project(project_root)
    run_date_parsed = pd.to_datetime(run_date, errors="coerce")
    if pd.isna(run_date_parsed):
        raise ValueError(f"Invalid run_date: {run_date}")

    selection_config = parse_selection_config(scenario.get("selection"))
    constraints_config = parse_constraints_config(scenario.get("constraints"))
    quality_config = parse_quality_config(scenario.get("quality"))
    simulation_config = parse_simulation_config(
        scenario.get("simulation"),
        default_start_date=run_date_parsed.date(),
    )
    emission_config = parse_emission_config(scenario.get("emission"))

    artifact_paths = expected_phase2_run_artifact_paths(runs_root.resolve(), run_id)
    run_dir = artifact_paths["truth_people"].parent
    run_dir.mkdir(parents=True, exist_ok=True)

    simulation_end = _simulation_end_date(
        simulation_config.start_date,
        simulation_config.periods,
        simulation_config.granularity,
    )

    # ------------------------------------------------------------------
    # Stage 2: Selection
    # ------------------------------------------------------------------
    scenario_population_path = artifact_paths["scenario_population"]
    selection_log_path = artifact_paths["scenario_selection_log_json"]

    if rebuild_population or not scenario_population_path.exists():
        selected_df, selection_log = generate_scenario_population_from_files(
            phase1_csv_path=phase1_csv_path,
            mobility_params_df=phase2_params["mobility_by_age_cohort"],
            selection_config=selection_config,
            seed=run_seed,
            scenario_id=scenario_id,
        )
        selected_df.to_parquet(scenario_population_path, index=False)
        selection_log["run_id"] = run_id
        selection_log["scenario_yaml_path"] = str(scenario_yaml_path)
        selection_log["phase1_csv_path"] = str(phase1_csv_path)
        selection_log["scenario_population_path"] = str(scenario_population_path)
        selection_log_path.write_text(json.dumps(selection_log, indent=2), encoding="utf-8")

    scenario_population_df = pd.read_parquet(scenario_population_path)
    phase1_df = pd.read_csv(phase1_csv_path, dtype=str)

    # ------------------------------------------------------------------
    # Stage 3: Truth simulation
    # ------------------------------------------------------------------
    truth_outputs = {
        "truth_people": artifact_paths["truth_people"],
        "truth_households": artifact_paths["truth_households"],
        "truth_household_memberships": artifact_paths["truth_household_memberships"],
        "truth_residence_history": artifact_paths["truth_residence_history"],
        "truth_events": artifact_paths["truth_events"],
    }
    if not overwrite:
        existing = [str(p) for p in truth_outputs.values() if p.exists()]
        if existing:
            raise FileExistsError(
                "Truth-layer outputs already exist. Re-run with overwrite=True: "
                + ", ".join(existing)
            )

    truth_result = simulate_truth_layer(
        phase1_df=phase1_df,
        scenario_population_df=scenario_population_df,
        scenario_id=scenario_id,
        seed=run_seed,
        simulation_config=simulation_config,
        constraints_config=constraints_config,
        scenario_parameters=scenario.get("parameters"),
        phase2_priors=phase2_params.get("priors_snapshot"),
    )

    for logical_name, path in truth_outputs.items():
        truth_result[logical_name].to_parquet(path, index=False)

    # ------------------------------------------------------------------
    # Stage 4: Observed emission
    # ------------------------------------------------------------------
    observed_dataset_paths = expected_observed_dataset_paths(run_dir, emission_config)
    pairwise_crosswalk_paths = expected_pairwise_crosswalk_paths(run_dir, emission_config)
    entity_record_map_path = artifact_paths["entity_record_map"]
    crosswalk_path = artifact_paths["truth_crosswalk"] if len(emission_config.datasets) == 2 else None

    if not overwrite:
        observed_targets = [entity_record_map_path, *observed_dataset_paths.values(), *pairwise_crosswalk_paths.values()]
        if crosswalk_path is not None:
            observed_targets.append(crosswalk_path)
        existing_obs = [str(p) for p in observed_targets if p.exists()]
        if existing_obs:
            raise FileExistsError(
                "Observed outputs already exist. Re-run with overwrite=True: "
                + ", ".join(existing_obs)
            )

    emitted = emit_observed_datasets(
        truth_people_df=truth_result["truth_people"],
        truth_residence_history_df=truth_result["truth_residence_history"],
        simulation_start_date=simulation_config.start_date,
        simulation_end_date=simulation_end,
        emission_config=emission_config,
        seed=run_seed,
        phase1_df=phase1_df,
    )

    for dataset in emission_config.datasets:
        emitted["datasets"][dataset.dataset_id].to_csv(
            observed_dataset_paths[f"dataset__{dataset.dataset_id}"],
            index=False,
        )
    emitted["entity_record_map"].to_csv(entity_record_map_path, index=False)
    if crosswalk_path is not None and emitted["truth_crosswalk"] is not None:
        emitted["truth_crosswalk"].to_csv(crosswalk_path, index=False)
    for pair_key, path in pairwise_crosswalk_paths.items():
        crosswalk_df = emitted["pairwise_crosswalks"].get(pair_key)
        if crosswalk_df is not None:
            crosswalk_df.to_csv(path, index=False)

    # ------------------------------------------------------------------
    # Stage 5: Quality + manifest (single consolidated write)
    # ------------------------------------------------------------------
    generated_at_utc = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

    constraints_validation = validate_constraints_against_truth(
        truth_people_df=truth_result["truth_people"],
        truth_events_df=truth_result["truth_events"],
        truth_residence_history_df=truth_result["truth_residence_history"],
        config=constraints_config,
    )

    phase2_quality = compute_phase2_quality_report(
        truth_people_df=truth_result["truth_people"],
        truth_households_df=truth_result["truth_households"],
        truth_household_memberships_df=truth_result["truth_household_memberships"],
        truth_residence_history_df=truth_result["truth_residence_history"],
        truth_events_df=truth_result["truth_events"],
        constraints_config=constraints_config,
        quality_config=quality_config,
        observed_datasets=emitted["datasets"],
        entity_record_map_df=emitted["entity_record_map"],
        truth_crosswalk_df=emitted["truth_crosswalk"],
        observed_relationship_mode=emission_config.crossfile_match_mode,
    )

    quality_status = _compute_quality_status(constraints_validation, phase2_quality)

    truth_counts = {
        "truth_people": int(len(truth_result["truth_people"])),
        "truth_households": int(len(truth_result["truth_households"])),
        "truth_household_memberships": int(len(truth_result["truth_household_memberships"])),
        "truth_residence_history": int(len(truth_result["truth_residence_history"])),
        "truth_events": int(len(truth_result["truth_events"])),
    }

    quality_report = {
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "scenario_id": scenario_id,
        "seed": run_seed,
        "truth_counts": truth_counts,
        "simulation_quality": truth_result["quality"],
        "phase2_quality": phase2_quality,
        "constraints_validation": constraints_validation,
        "observed_quality": emitted["metrics"],
        "status": quality_status,
    }
    artifact_paths["quality_report_json"].write_text(
        json.dumps(quality_report, indent=2), encoding="utf-8"
    )

    # Write resolved scenario.yaml (includes all sections)
    resolved_scenario = dict(scenario)
    resolved_scenario["scenario_id"] = scenario_id
    resolved_scenario["seed"] = run_seed
    resolved_scenario["phase1"] = {
        "data_path": phase1_data_path_cfg,
        "manifest_path": phase1_manifest_path_cfg,
    }
    resolved_scenario["simulation"] = {
        "granularity": simulation_config.granularity,
        "start_date": simulation_config.start_date.isoformat(),
        "periods": simulation_config.periods,
    }
    resolved_scenario["emission"] = _build_emission_yaml_payload(emission_config)
    resolved_scenario["quality"] = {
        "household_size_range": {
            "min": quality_config.household_size_min,
            "max": quality_config.household_size_max,
        }
    }
    artifact_paths["scenario_yaml"].write_text(
        yaml.safe_dump(resolved_scenario, sort_keys=False), encoding="utf-8"
    )

    # Write consolidated manifest.json
    manifest = {
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "scenario_id": scenario_id,
        "seed": run_seed,
        "phase1_input_csv": phase1_data_path_cfg,
        "phase1_input_manifest": phase1_manifest_path_cfg,
        "phase1_input_csv_resolved": str(phase1_csv_path),
        "phase1_input_manifest_resolved": str(phase1_manifest_path),
        "scenario_yaml_source_path": str(scenario_yaml_path),
        "scenario_population_path": str(scenario_population_path),
        "truth_outputs": {name: str(path) for name, path in truth_outputs.items()},
        "simulation_meta": truth_result["simulation_meta"],
        "observed_outputs": {
            "datasets": [
                {
                    "dataset_id": dataset.dataset_id,
                    "filename": dataset.filename,
                    "path": str(observed_dataset_paths[f"dataset__{dataset.dataset_id}"]),
                }
                for dataset in emission_config.datasets
            ],
            "entity_record_map": str(entity_record_map_path),
            "truth_crosswalk": str(crosswalk_path) if crosswalk_path is not None else "",
            "pairwise_crosswalks": [
                {
                    "dataset_ids": [first.dataset_id, second.dataset_id],
                    "filename": pairwise_crosswalk_paths[f"{first.dataset_id}__{second.dataset_id}"].name,
                    "path": str(pairwise_crosswalk_paths[f"{first.dataset_id}__{second.dataset_id}"]),
                }
                for idx, first in enumerate(emission_config.datasets)
                for second in emission_config.datasets[idx + 1 :]
                if f"{first.dataset_id}__{second.dataset_id}" in pairwise_crosswalk_paths
            ],
        },
        "emission_meta": {
            "scenario_yaml_path": str(scenario_yaml_path),
            "simulation_start_date": simulation_config.start_date.isoformat(),
            "simulation_end_date": simulation_end.isoformat(),
            "crossfile_match_mode": emission_config.crossfile_match_mode,
            "dataset_ids": [dataset.dataset_id for dataset in emission_config.datasets],
            "coverage": emitted["metrics"]["coverage"],
        },
    }
    dataset_path_by_id = {
        dataset.dataset_id: str(observed_dataset_paths[f"dataset__{dataset.dataset_id}"])
        for dataset in emission_config.datasets
    }
    if "A" in dataset_path_by_id:
        manifest["observed_outputs"]["dataset_a"] = dataset_path_by_id["A"]
    if "B" in dataset_path_by_id:
        manifest["observed_outputs"]["dataset_b"] = dataset_path_by_id["B"]
    artifact_paths["manifest_json"].write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )

    # ------------------------------------------------------------------
    # Stage 6: Contract validation
    # ------------------------------------------------------------------
    validation = validate_phase2_run(runs_root.resolve(), run_id)

    return {
        "run_id": run_id,
        "scenario_id": scenario_id,
        "seed": run_seed,
        "run_dir": str(run_dir),
        "truth_counts": truth_counts,
        "event_counts": truth_result["quality"]["event_counts"],
        "observed_counts": {
            "datasets": {
                dataset_id: int(len(df))
                for dataset_id, df in emitted["datasets"].items()
            },
            "entity_record_map_rows": int(len(emitted["entity_record_map"])),
            "crosswalk_rows": int(len(emitted["truth_crosswalk"])) if emitted["truth_crosswalk"] is not None else 0,
            "pairwise_crosswalk_rows": {
                pair_key: int(len(df))
                for pair_key, df in emitted["pairwise_crosswalks"].items()
            },
        },
        "quality_status": quality_status,
        "validation_valid": bool(validation.get("valid", False)),
        "paths": {
            "run_dir": str(run_dir),
            "datasets": dataset_path_by_id,
            "entity_record_map": str(entity_record_map_path),
            "truth_crosswalk": str(crosswalk_path) if crosswalk_path is not None else "",
            "pairwise_crosswalks": {
                pair_key: str(path)
                for pair_key, path in pairwise_crosswalk_paths.items()
            },
            "quality_report": str(artifact_paths["quality_report_json"]),
            "manifest": str(artifact_paths["manifest_json"]),
        },
    }
