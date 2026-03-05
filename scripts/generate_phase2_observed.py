from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import sys
from datetime import date, datetime, timezone
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


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]
    src_dir = project_root / "src"
    sys.path.insert(0, str(src_dir))

    from sog_phase2.constraints import parse_constraints_config
    from sog_phase2.emission import emit_observed_datasets, parse_emission_config
    from sog_phase2.output_contract import expected_phase2_run_artifact_paths, parse_run_id
    from sog_phase2.quality import compute_phase2_quality_report, parse_quality_config
    from sog_phase2.simulator import parse_simulation_config

    parser = argparse.ArgumentParser(
        description="Generate Phase-2 observed datasets (DatasetA, DatasetB, truth_crosswalk) from truth layer."
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Run identifier in format YYYY-MM-DD_<scenario_id>_seed<seed>.",
    )
    parser.add_argument(
        "--run",
        default=None,
        help="Run folder path or run identifier (legacy alias for --run-id).",
    )
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=project_root / "phase2" / "runs",
        help="Root directory containing run folders.",
    )
    parser.add_argument(
        "--scenario-yaml",
        type=Path,
        default=None,
        help="Scenario YAML path. Default uses run-local scenario.yaml if present.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite DatasetA/DatasetB/truth_crosswalk if they already exist.",
    )
    args = parser.parse_args()

    run_arg = str(args.run_id or args.run or "").strip()
    if not run_arg:
        parser.error("Provide --run-id or --run.")
    if any(sep in run_arg for sep in ("\\", "/")):
        run_arg = Path(run_arg).name

    run_info = parse_run_id(run_arg)
    artifact_paths = expected_phase2_run_artifact_paths(args.runs_root.resolve(), run_info["run_id"])
    run_dir = artifact_paths["truth_people"].parent
    run_dir.mkdir(parents=True, exist_ok=True)

    truth_people_path = artifact_paths["truth_people"]
    truth_residence_path = artifact_paths["truth_residence_history"]
    for required in (truth_people_path, truth_residence_path):
        if not required.exists():
            raise FileNotFoundError(
                f"Truth-layer prerequisite missing: {required}. Run generate_phase2_truth first."
            )

    scenario_yaml_path = args.scenario_yaml
    if scenario_yaml_path is None:
        if artifact_paths["scenario_yaml"].exists():
            scenario_yaml_path = artifact_paths["scenario_yaml"]
        else:
            scenario_yaml_path = project_root / "phase2" / "scenarios" / f"{run_info['scenario_id']}.yaml"
    scenario_yaml_path = scenario_yaml_path.resolve()
    if not scenario_yaml_path.exists():
        raise FileNotFoundError(f"Scenario YAML not found: {scenario_yaml_path}")
    scenario = _load_yaml(scenario_yaml_path)
    yaml_scenario_id = str(scenario.get("scenario_id", "")).strip()
    if yaml_scenario_id and yaml_scenario_id != run_info["scenario_id"]:
        raise ValueError(
            f"Scenario YAML scenario_id '{yaml_scenario_id}' does not match run_id scenario_id '{run_info['scenario_id']}'"
        )

    simulation_cfg = parse_simulation_config(scenario.get("simulation"))
    emission_cfg = parse_emission_config(scenario.get("emission"))
    quality_cfg = parse_quality_config(scenario.get("quality"))
    constraints_cfg = parse_constraints_config(scenario.get("constraints"))
    simulation_start = simulation_cfg.start_date
    simulation_end = _simulation_end_date(
        simulation_cfg.start_date,
        simulation_cfg.periods,
        simulation_cfg.granularity,
    )

    dataset_a_path = artifact_paths["dataset_a"]
    dataset_b_path = artifact_paths["dataset_b"]
    crosswalk_path = artifact_paths["truth_crosswalk"]
    if not args.overwrite:
        existing = [str(path) for path in (dataset_a_path, dataset_b_path, crosswalk_path) if path.exists()]
        if existing:
            raise FileExistsError(
                "Observed outputs already exist. Re-run with --overwrite to replace: "
                + ", ".join(existing)
            )

    truth_people_df = pd.read_parquet(truth_people_path)
    truth_residence_df = pd.read_parquet(truth_residence_path)
    emitted = emit_observed_datasets(
        truth_people_df=truth_people_df,
        truth_residence_history_df=truth_residence_df,
        simulation_start_date=simulation_start,
        simulation_end_date=simulation_end,
        emission_config=emission_cfg,
        seed=int(run_info["seed"]),
    )

    emitted["dataset_a"].to_csv(dataset_a_path, index=False)
    emitted["dataset_b"].to_csv(dataset_b_path, index=False)
    emitted["truth_crosswalk"].to_csv(crosswalk_path, index=False)

    resolved_scenario = dict(scenario)
    resolved_scenario["scenario_id"] = run_info["scenario_id"]
    resolved_scenario["seed"] = int(run_info["seed"])
    resolved_scenario["simulation"] = {
        "granularity": simulation_cfg.granularity,
        "start_date": simulation_cfg.start_date.isoformat(),
        "periods": simulation_cfg.periods,
    }
    emission_payload = asdict(emission_cfg)
    emission_payload["appearance_A_pct"] = emission_payload.pop("appearance_a_pct")
    emission_payload["appearance_B_pct"] = emission_payload.pop("appearance_b_pct")
    emission_payload["duplication_in_A_pct"] = emission_payload.pop("duplication_in_a_pct")
    emission_payload["duplication_in_B_pct"] = emission_payload.pop("duplication_in_b_pct")
    emission_payload["noise"] = {
        "A": emission_payload.pop("dataset_a_noise"),
        "B": emission_payload.pop("dataset_b_noise"),
    }
    resolved_scenario["emission"] = emission_payload
    resolved_scenario["quality"] = {
        "household_size_range": {
            "min": quality_cfg.household_size_min,
            "max": quality_cfg.household_size_max,
        }
    }
    artifact_paths["scenario_yaml"].write_text(
        yaml.safe_dump(resolved_scenario, sort_keys=False),
        encoding="utf-8",
    )

    generated_at_utc = (
        datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    )

    manifest = _load_json(artifact_paths["manifest_json"])
    manifest.update(
        {
            "generated_at_utc": generated_at_utc,
            "run_id": run_info["run_id"],
            "scenario_id": run_info["scenario_id"],
            "seed": int(run_info["seed"]),
            "observed_outputs": {
                "dataset_a": str(dataset_a_path),
                "dataset_b": str(dataset_b_path),
                "truth_crosswalk": str(crosswalk_path),
            },
            "emission_meta": {
                "scenario_yaml_path": str(scenario_yaml_path),
                "simulation_start_date": simulation_start.isoformat(),
                "simulation_end_date": simulation_end.isoformat(),
                "crossfile_match_mode": emission_cfg.crossfile_match_mode,
                "coverage": emitted["metrics"]["coverage"],
            },
        }
    )
    _write_json(artifact_paths["manifest_json"], manifest)

    truth_households_df = pd.read_parquet(artifact_paths["truth_households"])
    truth_memberships_df = pd.read_parquet(artifact_paths["truth_household_memberships"])
    truth_events_df = pd.read_parquet(artifact_paths["truth_events"])
    phase2_quality = compute_phase2_quality_report(
        truth_people_df=truth_people_df,
        truth_households_df=truth_households_df,
        truth_household_memberships_df=truth_memberships_df,
        truth_residence_history_df=truth_residence_df,
        truth_events_df=truth_events_df,
        constraints_config=constraints_cfg,
        quality_config=quality_cfg,
        dataset_a_df=emitted["dataset_a"],
        dataset_b_df=emitted["dataset_b"],
        truth_crosswalk_df=emitted["truth_crosswalk"],
    )
    truth_consistency = phase2_quality.get("truth_consistency", {})
    event_age_count = int((truth_consistency.get("event_age_validation") or {}).get("invalid_event_age_count", 0))
    no_overlap = bool((truth_consistency.get("time_overlap_errors") or {}).get("no_time_overlap_errors", True))
    household_ok = bool(
        (truth_consistency.get("household_size_constraints") or {}).get("within_config_constraints", True)
    )
    status = "ok" if (event_age_count == 0 and no_overlap and household_ok) else "quality_issues_detected"

    quality = _load_json(artifact_paths["quality_report_json"])
    quality.update(
        {
            "generated_at_utc": generated_at_utc,
            "run_id": run_info["run_id"],
            "scenario_id": run_info["scenario_id"],
            "seed": int(run_info["seed"]),
            "observed_quality": emitted["metrics"],
            "phase2_quality": phase2_quality,
            "status": status,
        }
    )
    _write_json(artifact_paths["quality_report_json"], quality)

    result = {
        "run_id": run_info["run_id"],
        "scenario_id": run_info["scenario_id"],
        "seed": int(run_info["seed"]),
        "dataset_a_rows": int(len(emitted["dataset_a"])),
        "dataset_b_rows": int(len(emitted["dataset_b"])),
        "crosswalk_rows": int(len(emitted["truth_crosswalk"])),
        "match_mode": emission_cfg.crossfile_match_mode,
        "paths": {
            "DatasetA": str(dataset_a_path),
            "DatasetB": str(dataset_b_path),
            "truth_crosswalk": str(crosswalk_path),
        },
    }
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
