"""Thin synchronous wrapper around sog_phase2.pipeline.run_scenario_pipeline.

Runs the pipeline in-process (no subprocess). Streamlit handles the 10-60s
blocking call inside st.spinner — no job queue or worker process needed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_src_dir = str(PROJECT_ROOT / "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)

from sog_phase2.pipeline import run_scenario_pipeline  # noqa: E402


def run_pipeline_sync(
    scenario_yaml_path: Path,
    runs_root: Path | None = None,
    overwrite: bool = False,
    rebuild_population: bool = False,
) -> dict[str, Any]:
    """Run the full Phase-2 pipeline synchronously and return a result dict."""
    if runs_root is None:
        runs_root = PROJECT_ROOT / "phase2" / "runs"
    return run_scenario_pipeline(
        scenario_yaml_path=scenario_yaml_path,
        runs_root=runs_root,
        project_root=PROJECT_ROOT,
        overwrite=overwrite,
        rebuild_population=rebuild_population,
    )
