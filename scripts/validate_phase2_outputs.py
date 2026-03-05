from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]
    src_dir = project_root / "src"
    sys.path.insert(0, str(src_dir))

    from sog_phase2.output_contract import validate_phase2_run

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
    args = parser.parse_args()

    run_arg = str(args.run_id or args.run or "").strip()
    if not run_arg:
        parser.error("Provide --run-id or --run.")
    if any(sep in run_arg for sep in ("\\", "/")):
        run_arg = Path(run_arg).name

    result = validate_phase2_run(
        runs_root=args.runs_root.resolve(),
        run_id=run_arg,
    )
    print(json.dumps(result, indent=2))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
