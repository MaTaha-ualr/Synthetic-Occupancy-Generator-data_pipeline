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

    from sog_phase1.generator import generate_phase1_dataset

    parser = argparse.ArgumentParser(description="Generate phase1 people+addresses dataset.")
    parser.add_argument(
        "--config",
        type=Path,
        default=project_root / "configs" / "phase1.yaml",
        help="Path to phase1 yaml config.",
    )
    parser.add_argument(
        "--prepared-dir",
        type=Path,
        default=project_root / "prepared",
        help="Prepared cache directory (from build_prepared.py).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output files/directories.",
    )
    args = parser.parse_args()

    result = generate_phase1_dataset(
        project_root=project_root,
        config_path=args.config.resolve(),
        prepared_dir=args.prepared_dir.resolve(),
        overwrite=args.overwrite,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
