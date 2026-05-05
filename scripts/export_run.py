"""Convert a Phase-2 run's artifacts into another tabular format.

Examples:
    # Convert every tabular artifact in a run to xlsx, write into the run dir
    python scripts/export_run.py --run-id 2026-03-10_single_movers_seed20260310 --format xlsx

    # Convert just one artifact (by filename) and place the result elsewhere
    python scripts/export_run.py --run-id <id> --format jsonl \
        --artifact DatasetA.csv --output phase2/.sog_exports/

    # Bundle the whole run as a zip with everything re-encoded as tsv
    python scripts/export_run.py --run-id <id> --format tsv --bundle \
        --output phase2/.sog_exports/<id>_tsv.zip

Supported formats: csv, tsv, txt, xlsx, json, jsonl, parquet.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(project_root / "src"))

    from sog_phase2.format_export import (
        TABULAR_FORMATS,
        bundle_run_as_zip,
        convert_artifact_to_path,
        is_tabular_artifact,
        list_run_artifacts,
        normalize_format,
    )

    parser = argparse.ArgumentParser(
        description=(
            "Re-serialize Phase-2 run artifacts into a different tabular format. "
            "Canonical CSV/parquet outputs are not modified."
        ),
    )
    parser.add_argument("--run-id", required=True, help="Phase-2 run identifier.")
    parser.add_argument(
        "--format",
        required=False,
        choices=TABULAR_FORMATS,
        help="Target format for tabular artifacts. Required unless --list is given.",
    )
    parser.add_argument(
        "--runs-root",
        type=Path,
        default=project_root / "phase2" / "runs",
        help="Root directory containing run folders (default: phase2/runs).",
    )
    parser.add_argument(
        "--artifact",
        action="append",
        default=None,
        help=(
            "Specific artifact filename to convert (may be repeated). "
            "Defaults to every tabular file in the run."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=(
            "Output directory (or zip path with --bundle). "
            "Defaults to the run directory for per-file conversion."
        ),
    )
    parser.add_argument(
        "--bundle",
        action="store_true",
        help="Produce a single zip containing all converted artifacts plus meta files.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List artifacts in the run and exit (ignores --format).",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Refuse to overwrite an existing converted artifact.",
    )

    args = parser.parse_args()

    run_dir = (args.runs_root / args.run_id).resolve()
    if not run_dir.is_dir():
        parser.error(f"Run directory not found: {run_dir}")

    if args.list:
        print(json.dumps(list_run_artifacts(run_dir), indent=2))
        return 0

    if not args.format:
        parser.error("--format is required unless --list is used.")
    fmt = normalize_format(args.format)

    if args.bundle:
        if args.artifact:
            parser.error("--artifact cannot be combined with --bundle.")
        zip_bytes = bundle_run_as_zip(run_dir, fmt)
        if args.output is not None:
            target = args.output
            if target.is_dir() or (not target.suffix and not target.exists()):
                target = target / f"{args.run_id}_{fmt}.zip"
            target.parent.mkdir(parents=True, exist_ok=True)
        else:
            target = run_dir.parent / f"{args.run_id}_{fmt}.zip"
        target.write_bytes(zip_bytes)
        print(json.dumps({"bundle_path": str(target), "bytes": len(zip_bytes)}, indent=2))
        return 0

    if args.artifact:
        sources: list[Path] = []
        for name in args.artifact:
            candidate = run_dir / name
            if not candidate.exists():
                parser.error(f"Artifact not found in run: {candidate}")
            sources.append(candidate)
    else:
        sources = [
            Path(entry["path"])
            for entry in list_run_artifacts(run_dir)
            if entry["tabular"] == "true"
        ]
        if not sources:
            parser.error(f"No tabular artifacts found in {run_dir}")

    dest_dir = args.output if args.output is not None else None
    written: list[str] = []
    skipped: list[str] = []
    for src in sources:
        if not is_tabular_artifact(src) and args.artifact is None:
            skipped.append(str(src))
            continue
        target = convert_artifact_to_path(
            src,
            fmt,
            dest_dir=dest_dir,
            overwrite=not args.no_overwrite,
        )
        written.append(str(target))

    print(
        json.dumps(
            {
                "run_id": args.run_id,
                "format": fmt,
                "written": written,
                "skipped": skipped,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
