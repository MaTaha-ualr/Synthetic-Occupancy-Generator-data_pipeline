"""Write/refresh the per-run README.md for one or all Phase-2 run folders.

The unified pipeline (run_phase2_pipeline.py) writes a per-run README automatically.
Use this script to (re)generate READMEs for runs produced by the legacy three-script
flow, or to backfill folders created before this feature existed.

Usage:
    # All run folders under phase2/runs/
    python phase2/scripts/write_run_readmes.py

    # A single run
    python phase2/scripts/write_run_readmes.py --run-id 2026-03-10_single_movers_seed20260310
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(project_root / "phase2" / "src"))

    from sog_phase2.run_readme import write_run_readme
    from sog_phase2.progress import ProgressReporter

    parser = argparse.ArgumentParser(description="Generate per-run README.md files.")
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=project_root / "phase2" / "runs",
        help="Root directory containing run folders (default: phase2/runs).",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Only write the README for this single run id (default: every run folder).",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable stderr progress output.",
    )
    args = parser.parse_args()

    runs_root = args.runs_root.resolve()
    if not runs_root.exists():
        parser.error(f"runs root does not exist: {runs_root}")

    if args.run_id:
        targets = [runs_root / args.run_id]
    else:
        targets = sorted(p for p in runs_root.iterdir() if p.is_dir())

    progress = ProgressReporter(
        total=max(1, len(targets)),
        label="Run READMEs",
        enabled=not args.no_progress,
    )

    written = 0
    for run_dir in targets:
        with progress.step(f"Write {run_dir.name}"):
            if not run_dir.is_dir():
                print(f"skip (missing): {run_dir}")
                continue
            # A run folder is recognizable by having a manifest or any truth file.
            if not (run_dir / "manifest.json").exists() and not (run_dir / "truth_people.parquet").exists():
                print(f"skip (not a run folder): {run_dir.name}")
                continue
            path = write_run_readme(run_dir)
            written += 1
            print(f"wrote {path}")

    print(f"done: {written} README.md file(s) written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
