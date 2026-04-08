from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml


def _load_yaml(path: Path) -> dict:
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
        # Pre-reorganization paths → new location under phase1/
        "outputs_phase1/Phase1_people_addresses.csv": "phase1/outputs_phase1/Phase1_people_addresses.csv",
        "outputs_phase1/Phase1_people_addresses.manifest.json": "phase1/outputs_phase1/Phase1_people_addresses.manifest.json",
        "outputs_phase1/Phase1_people_addresses.quality_report.json": "phase1/outputs_phase1/Phase1_people_addresses.quality_report.json",
        # Even older pre-reorganization paths
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


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]
    src_dir = project_root / "src"
    sys.path.insert(0, str(src_dir))

    from sog_phase2.output_contract import expected_phase2_run_artifact_paths, parse_run_id
    from sog_phase2.params import load_phase2_params_from_project
    from sog_phase2.selection import (
        generate_scenario_population_from_files,
        parse_selection_config,
    )

    parser = argparse.ArgumentParser(
        description="Build deterministic Phase-2 scenario population from Phase-1 baseline."
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Run identifier in format YYYY-MM-DD_<scenario_id>_seed<seed>.",
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
        help="Overwrite scenario_population.parquet and scenario_selection_log.json if they exist.",
    )
    args = parser.parse_args()

    run_info = parse_run_id(args.run_id)
    scenario_id = run_info["scenario_id"]
    run_seed = int(run_info["seed"])

    scenario_yaml = args.scenario_yaml
    if scenario_yaml is None:
        scenario_yaml = project_root / "phase2" / "scenarios" / f"{scenario_id}.yaml"
    scenario_yaml = scenario_yaml.resolve()
    if not scenario_yaml.exists():
        raise FileNotFoundError(f"Scenario YAML does not exist: {scenario_yaml}")

    scenario = _load_yaml(scenario_yaml)
    yaml_scenario_id = str(scenario.get("scenario_id", "")).strip()
    yaml_seed = int(scenario.get("seed", run_seed))
    if yaml_scenario_id and yaml_scenario_id != scenario_id:
        raise ValueError(
            f"Scenario YAML scenario_id '{yaml_scenario_id}' does not match run_id scenario_id '{scenario_id}'"
        )
    if yaml_seed != run_seed:
        raise ValueError(
            f"Scenario YAML seed {yaml_seed} does not match run_id seed {run_seed}"
        )

    phase1_cfg = scenario.get("phase1", {})
    if not isinstance(phase1_cfg, dict):
        raise ValueError("scenario.phase1 must be a mapping")
    phase1_csv_rel = str(phase1_cfg.get("data_path", "")).strip()
    if not phase1_csv_rel:
        raise ValueError("scenario.phase1.data_path is required")
    phase1_csv_path = _resolve_with_legacy_fallback(project_root, phase1_csv_rel)
    if not phase1_csv_path.exists():
        raise FileNotFoundError(f"Phase-1 CSV not found: {phase1_csv_path}")

    selection_config = parse_selection_config(scenario.get("selection"))
    phase2_params = load_phase2_params_from_project(project_root)

    selected_df, selection_log = generate_scenario_population_from_files(
        phase1_csv_path=phase1_csv_path,
        mobility_params_df=phase2_params["mobility_by_age_cohort"],
        selection_config=selection_config,
        seed=run_seed,
        scenario_id=scenario_id,
    )

    artifact_paths = expected_phase2_run_artifact_paths(args.runs_root.resolve(), args.run_id)
    scenario_population_path = artifact_paths["scenario_population"]
    selection_log_path = artifact_paths["scenario_selection_log_json"]
    scenario_population_path.parent.mkdir(parents=True, exist_ok=True)

    if not args.overwrite:
        for path in (scenario_population_path, selection_log_path):
            if path.exists():
                raise FileExistsError(f"Output file exists. Re-run with --overwrite: {path}")

    selected_df.to_parquet(scenario_population_path, index=False)

    selection_log["run_id"] = run_info["run_id"]
    selection_log["scenario_yaml_path"] = str(scenario_yaml)
    selection_log["phase1_csv_path"] = str(phase1_csv_path)
    selection_log["scenario_population_path"] = str(scenario_population_path)
    selection_log_path.write_text(json.dumps(selection_log, indent=2), encoding="utf-8")

    result = {
        "run_id": run_info["run_id"],
        "scenario_id": scenario_id,
        "selection_seed": run_seed,
        "selected_entities": int(len(selected_df)),
        "scenario_population_path": str(scenario_population_path),
        "selection_log_path": str(selection_log_path),
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
