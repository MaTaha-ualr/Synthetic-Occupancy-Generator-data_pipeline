from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected YAML mapping in {path}")
    return data


def _resolve_with_legacy_fallback(project_root: Path, configured_path: str) -> Path:
    candidate = (project_root / configured_path).resolve()
    if candidate.exists():
        return candidate

    legacy_map = {
        "outputs_phase1/Phase1_people_addresses.csv": "outputs/Phase1_people_addresses.csv",
        "outputs_phase1/Phase1_people_addresses.manifest.json": "outputs/Phase1_people_addresses.manifest.json",
        "outputs_phase1/Phase1_people_addresses.quality_report.json": "outputs/Phase1_people_addresses.quality_report.json",
    }
    normalized = configured_path.replace("\\", "/")
    if normalized in legacy_map:
        legacy_candidate = (project_root / legacy_map[normalized]).resolve()
        if legacy_candidate.exists():
            return legacy_candidate
    return candidate


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]
    src_dir = project_root / "src"
    sys.path.insert(0, str(src_dir))

    from sog_phase2.constraints import parse_constraints_config, validate_constraints_against_truth
    from sog_phase2.output_contract import (
        build_run_id,
        expected_phase2_run_artifact_paths,
        parse_run_id,
    )
    from sog_phase2.params import load_phase2_params_from_project
    from sog_phase2.quality import compute_phase2_quality_report, parse_quality_config
    from sog_phase2.selection import generate_scenario_population_from_files, parse_selection_config
    from sog_phase2.simulator import parse_simulation_config, simulate_truth_layer

    parser = argparse.ArgumentParser(
        description="Generate Phase-2 truth-layer outputs via event-driven simulation."
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Run identifier in format YYYY-MM-DD_<scenario_id>_seed<seed>.",
    )
    parser.add_argument(
        "--scenario",
        default=None,
        help="Scenario ID (legacy mode when --run-id is omitted).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed (legacy mode when --run-id is omitted).",
    )
    parser.add_argument(
        "--run-date",
        default=None,
        help="Run date in YYYY-MM-DD (legacy mode when --run-id is omitted).",
    )
    parser.add_argument(
        "--scenario-yaml",
        type=Path,
        default=None,
        help="Scenario YAML path (default: phase2/scenarios/<scenario_id>.yaml).",
    )
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=project_root / "phase2" / "runs",
        help="Root directory containing run folders.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing truth-layer outputs if they exist.",
    )
    parser.add_argument(
        "--rebuild-population",
        action="store_true",
        help="Rebuild scenario_population.parquet before simulation.",
    )
    parser.add_argument(
        "--phase1",
        type=Path,
        default=None,
        help="Override Phase-1 CSV path for this run.",
    )
    parser.add_argument(
        "--phase1-manifest",
        type=Path,
        default=None,
        help="Override Phase-1 manifest path for this run.",
    )
    args = parser.parse_args()

    run_id_text = str(args.run_id or "").strip()
    if not run_id_text:
        scenario_text = str(args.scenario or "").strip()
        if not scenario_text:
            parser.error("Provide --run-id or (--scenario and --seed).")
        if args.seed is None:
            parser.error("Provide --seed when using --scenario.")

        run_date_text = str(args.run_date or "").strip()
        if not run_date_text:
            seed_text = str(int(args.seed))
            if len(seed_text) == 8 and seed_text.isdigit():
                parsed_seed_date = pd.to_datetime(seed_text, format="%Y%m%d", errors="coerce")
                if not pd.isna(parsed_seed_date):
                    run_date_text = parsed_seed_date.date().isoformat()
        if not run_date_text:
            run_date_text = datetime.now(timezone.utc).date().isoformat()
        run_id_text = build_run_id(scenario_text, int(args.seed), run_date_text)

    run_info = parse_run_id(run_id_text)
    scenario_id = run_info["scenario_id"]
    run_seed = int(run_info["seed"])
    run_date = pd.to_datetime(run_info["run_date"], errors="coerce")
    if pd.isna(run_date):
        raise ValueError(f"Invalid run date in run_id: {run_info['run_date']}")

    scenario_yaml_path = args.scenario_yaml
    if scenario_yaml_path is None:
        scenario_yaml_path = project_root / "phase2" / "scenarios" / f"{scenario_id}.yaml"
    scenario_yaml_path = scenario_yaml_path.resolve()
    if not scenario_yaml_path.exists():
        raise FileNotFoundError(f"Scenario YAML does not exist: {scenario_yaml_path}")

    scenario = _load_yaml(scenario_yaml_path)
    yaml_scenario_id = str(scenario.get("scenario_id", "")).strip()
    if yaml_scenario_id and yaml_scenario_id != scenario_id:
        raise ValueError(
            f"Scenario YAML scenario_id '{yaml_scenario_id}' does not match run_id scenario_id '{scenario_id}'"
        )

    phase1_cfg = scenario.get("phase1", {})
    if not isinstance(phase1_cfg, dict):
        raise ValueError("scenario.phase1 must be a mapping")
    phase1_data_path_cfg = str(phase1_cfg.get("data_path", "")).strip()
    phase1_manifest_path_cfg = str(phase1_cfg.get("manifest_path", "")).strip()
    if args.phase1 is not None:
        phase1_data_path_cfg = str(args.phase1)
    if args.phase1_manifest is not None:
        phase1_manifest_path_cfg = str(args.phase1_manifest)
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
    selection_config = parse_selection_config(scenario.get("selection"))
    constraints_config = parse_constraints_config(scenario.get("constraints"))
    quality_config = parse_quality_config(scenario.get("quality"))
    simulation_config = parse_simulation_config(
        scenario.get("simulation"),
        default_start_date=run_date.date(),
    )

    artifact_paths = expected_phase2_run_artifact_paths(args.runs_root.resolve(), run_info["run_id"])
    run_dir = artifact_paths["truth_people"].parent
    run_dir.mkdir(parents=True, exist_ok=True)

    scenario_population_path = artifact_paths["scenario_population"]
    selection_log_path = artifact_paths["scenario_selection_log_json"]

    should_rebuild_population = args.rebuild_population or not scenario_population_path.exists()
    if should_rebuild_population:
        selected_df, selection_log = generate_scenario_population_from_files(
            phase1_csv_path=phase1_csv_path,
            mobility_params_df=phase2_params["mobility_by_age_cohort"],
            selection_config=selection_config,
            seed=run_seed,
            scenario_id=scenario_id,
        )
        selected_df.to_parquet(scenario_population_path, index=False)
        selection_log["run_id"] = run_info["run_id"]
        selection_log["scenario_yaml_path"] = str(scenario_yaml_path)
        selection_log["phase1_csv_path"] = str(phase1_csv_path)
        selection_log["scenario_population_path"] = str(scenario_population_path)
        selection_log_path.write_text(json.dumps(selection_log, indent=2), encoding="utf-8")

    scenario_population_df = pd.read_parquet(scenario_population_path)
    phase1_df = pd.read_csv(phase1_csv_path, dtype=str)

    generated_at_utc = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
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

    truth_outputs = {
        "truth_people": artifact_paths["truth_people"],
        "truth_households": artifact_paths["truth_households"],
        "truth_household_memberships": artifact_paths["truth_household_memberships"],
        "truth_residence_history": artifact_paths["truth_residence_history"],
        "truth_events": artifact_paths["truth_events"],
    }
    if not args.overwrite:
        existing = [str(path) for path in truth_outputs.values() if path.exists()]
        if existing:
            raise FileExistsError(
                "Truth-layer outputs already exist. Re-run with --overwrite to replace: "
                + ", ".join(existing)
            )

    for logical_name, path in truth_outputs.items():
        truth_result[logical_name].to_parquet(path, index=False)

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
    resolved_scenario["quality"] = {
        "household_size_range": {
            "min": quality_config.household_size_min,
            "max": quality_config.household_size_max,
        }
    }
    artifact_paths["scenario_yaml"].write_text(
        yaml.safe_dump(resolved_scenario, sort_keys=False),
        encoding="utf-8",
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
    )
    truth_consistency = phase2_quality.get("truth_consistency", {})
    event_age_count = int((truth_consistency.get("event_age_validation") or {}).get("invalid_event_age_count", 0))
    no_overlap = bool((truth_consistency.get("time_overlap_errors") or {}).get("no_time_overlap_errors", True))
    household_ok = bool(
        (truth_consistency.get("household_size_constraints") or {}).get("within_config_constraints", True)
    )
    quality_status = (
        "ok"
        if (constraints_validation["valid"] and event_age_count == 0 and no_overlap and household_ok)
        else "quality_issues_detected"
    )

    manifest = {
        "generated_at_utc": generated_at_utc,
        "run_id": run_info["run_id"],
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
    }
    artifact_paths["manifest_json"].write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    quality_report = {
        "generated_at_utc": generated_at_utc,
        "run_id": run_info["run_id"],
        "scenario_id": scenario_id,
        "seed": run_seed,
        "truth_counts": {
            "truth_people": int(len(truth_result["truth_people"])),
            "truth_households": int(len(truth_result["truth_households"])),
            "truth_household_memberships": int(len(truth_result["truth_household_memberships"])),
            "truth_residence_history": int(len(truth_result["truth_residence_history"])),
            "truth_events": int(len(truth_result["truth_events"])),
        },
        "simulation_quality": truth_result["quality"],
        "phase2_quality": phase2_quality,
        "constraints_validation": constraints_validation,
        "status": quality_status,
    }
    artifact_paths["quality_report_json"].write_text(
        json.dumps(quality_report, indent=2),
        encoding="utf-8",
    )

    result = {
        "run_id": run_info["run_id"],
        "scenario_id": scenario_id,
        "seed": run_seed,
        "run_dir": str(run_dir),
        "truth_counts": quality_report["truth_counts"],
        "event_counts": truth_result["quality"]["event_counts"],
        "constraints_valid": bool(constraints_validation["valid"]),
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
