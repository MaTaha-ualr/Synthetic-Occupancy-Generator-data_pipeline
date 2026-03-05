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

    from sog_phase1.preprocess import build_prepared_cache

    parser = argparse.ArgumentParser(description="Build prepared parquet/json cache from raw SOG CSV files.")
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=project_root,
        help="Raw SOG root containing Addresses/, Names/, Data/.",
    )
    parser.add_argument(
        "--prepared-dir",
        type=Path,
        default=project_root / "prepared",
        help="Output directory for prepared cache.",
    )
    args = parser.parse_args()

    manifest = build_prepared_cache(args.raw_root.resolve(), args.prepared_dir.resolve())
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
