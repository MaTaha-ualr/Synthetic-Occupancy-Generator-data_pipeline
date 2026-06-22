"""Single CLI entry point for the complete Phase-2 pipeline.

Runs selection → truth simulation → observed emission → quality → validation
in one command.  Replaces running three scripts in sequence.

Usage:
    python phase2/scripts/run_phase2_pipeline.py --scenario-yaml phase2/scenarios/single_movers.yaml
    python phase2/scripts/run_phase2_pipeline.py --scenario single_movers --overwrite
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root / "phase2" / "src"))

    from sog_phase2.pipeline import run_scenario_pipeline

    parser = argparse.ArgumentParser(
        description="Run the complete Phase-2 pipeline in a single call.",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--scenario-yaml",
        type=Path,
        help="Path to scenario YAML file (absolute or relative to project root).",
    )
    group.add_argument(
        "--scenario",
        help="Scenario ID — resolves to phase2/scenarios/<id>.yaml.",
    )
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=project_root / "phase2" / "runs",
        help="Root directory for run outputs (default: phase2/runs).",
    )
    parser.add_argument(
        "--run-date",
        default=None,
        help="Run date in YYYY-MM-DD (default: derived from seed or today).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing run outputs.",
    )
    parser.add_argument(
        "--rebuild-population",
        action="store_true",
        help="Rebuild scenario_population.parquet even if it already exists.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable stderr progress output.",
    )
    args = parser.parse_args()

    if args.scenario_yaml is not None:
        scenario_yaml_path = args.scenario_yaml
        if not scenario_yaml_path.is_absolute():
            scenario_yaml_path = (project_root / scenario_yaml_path).resolve()
    else:
        scenario_yaml_path = project_root / "phase2" / "scenarios" / f"{args.scenario}.yaml"

    result = run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml_path,
        runs_root=args.runs_root,
        project_root=project_root,
        run_date=args.run_date,
        overwrite=args.overwrite,
        rebuild_population=args.rebuild_population,
        show_progress=not args.no_progress,
    )

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
