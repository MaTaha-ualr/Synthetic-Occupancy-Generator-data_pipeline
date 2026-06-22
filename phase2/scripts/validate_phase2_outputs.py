from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[2]
    src_dir = project_root / "phase2" / "src"
    sys.path.insert(0, str(src_dir))

    from sog_phase2.output_contract import validate_phase2_run
    from sog_phase2.progress import ProgressReporter

    parser = argparse.ArgumentParser(description="Validate required Phase-2 run artifacts and outputs.")
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
        help="Root directory containing Phase-2 run subdirectories.",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable stderr progress output.",
    )
    args = parser.parse_args()

    progress = ProgressReporter(
        total=2,
        label="Phase 2 validation",
        enabled=not args.no_progress,
    )

    with progress.step("Resolve run"):
        run_id_arg = str(args.run_id or "").strip()
        run_path_arg = str(args.run or "").strip()
        runs_root = args.runs_root.resolve()
        if run_id_arg:
            if any(sep in run_id_arg for sep in ("\\", "/")):
                parser.error("--run-id expects a run identifier; use --run for a folder path.")
            run_arg = run_id_arg
        elif run_path_arg:
            candidate = Path(run_path_arg).expanduser()
            if candidate.is_absolute() or any(sep in run_path_arg for sep in ("\\", "/")):
                run_path = candidate.resolve()
                runs_root = run_path.parent
                run_arg = run_path.name
            else:
                run_arg = run_path_arg
        else:
            parser.error("Provide --run-id or --run.")

    with progress.step("Validate artifacts"):
        result = validate_phase2_run(
            runs_root=runs_root,
            run_id=run_arg,
        )
    print(json.dumps(result, indent=2))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
