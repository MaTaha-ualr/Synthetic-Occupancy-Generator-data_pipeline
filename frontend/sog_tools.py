"""Tool implementations called by Claude via the Anthropic tool-use API.

Each function is a pure Python callable that returns a JSON-serializable dict.
No Streamlit dependency. The session_id parameter is used to scope working
copies of scenario YAMLs so concurrent sessions don't clobber each other.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = PROJECT_ROOT / "phase2" / "scenarios"
RUNS_ROOT = PROJECT_ROOT / "phase2" / "runs"

_src_dir = str(PROJECT_ROOT / "src")
if _src_dir not in sys.path:
    sys.path.insert(0, _src_dir)


# ---------------------------------------------------------------------------
# list_scenarios
# ---------------------------------------------------------------------------

def list_scenarios() -> dict[str, Any]:
    """Return available scenario template IDs with a brief description."""
    _descriptions = {
        "single_movers": "People who move addresses — tests residential mobility matching",
        "couple_merge": "Couples moving in together — tests cohabitation linkage",
        "family_birth": "Births within households — tests parent-child record linkage",
        "divorce_custody": "Divorcing couples — tests split-household and custody matching",
        "roommates_split": "Roommate households splitting — tests shared-address false-positive signals",
    }
    scenarios = []
    for yaml_path in sorted(SCENARIOS_DIR.glob("*.yaml")):
        if yaml_path.stem.startswith("_"):
            continue  # skip working copies
        try:
            import yaml
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            continue
        scenario_id = str(raw.get("scenario_id", yaml_path.stem))
        params = raw.get("parameters", {})
        scenarios.append({
            "scenario_id": scenario_id,
            "description": _descriptions.get(scenario_id, ""),
            "key_parameters": params,
            "emission_match_mode": raw.get("emission", {}).get("crossfile_match_mode", ""),
        })
    return {"scenarios": scenarios, "count": len(scenarios)}


# ---------------------------------------------------------------------------
# read_scenario
# ---------------------------------------------------------------------------

def read_scenario(scenario_id: str, session_id: str = "") -> dict[str, Any]:
    """Return the full parsed config and raw YAML text of a scenario template."""
    # Prefer the working copy if one exists for this session
    if session_id:
        working_path = SCENARIOS_DIR / f"_working_{session_id}_{scenario_id}.yaml"
        if working_path.exists():
            path = working_path
        else:
            path = SCENARIOS_DIR / f"{scenario_id}.yaml"
    else:
        path = SCENARIOS_DIR / f"{scenario_id}.yaml"

    if not path.exists():
        return {"error": f"Scenario not found: {scenario_id}"}

    import yaml
    try:
        text = path.read_text(encoding="utf-8")
        parsed = yaml.safe_load(text) or {}
    except Exception as exc:
        return {"error": f"Failed to read scenario: {exc}"}

    return {
        "scenario_id": scenario_id,
        "yaml_text": text,
        "parsed": parsed,
        "is_working_copy": session_id != "" and path.stem.startswith("_working"),
    }


# ---------------------------------------------------------------------------
# update_scenario
# ---------------------------------------------------------------------------

def update_scenario(
    scenario_id: str,
    patches: dict[str, Any],
    session_id: str = "",
) -> dict[str, Any]:
    """Apply dot-path patches to a scenario and write a session-scoped working copy.

    patches: flat dict of dot-notation keys → values
    e.g. {"parameters.move_rate_pct": 25.0, "emission.noise.B.phonetic_error_pct": 3.0}
    """
    source_path = SCENARIOS_DIR / f"{scenario_id}.yaml"
    if not source_path.exists():
        return {"error": f"Scenario not found: {scenario_id}"}

    import yaml
    try:
        base = yaml.safe_load(source_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return {"error": f"Failed to load scenario: {exc}"}

    # Apply dot-path patches
    applied = {}
    errors = []
    for dot_path, value in patches.items():
        try:
            _set_dotpath(base, dot_path, value)
            applied[dot_path] = value
        except Exception as exc:
            errors.append(f"{dot_path}: {exc}")

    # Validate with existing parse functions
    validation_errors = _validate_scenario_dict(base)
    errors.extend(validation_errors)

    # Write working copy
    working_path = SCENARIOS_DIR / f"_working_{session_id}_{scenario_id}.yaml"
    try:
        working_path.write_text(yaml.safe_dump(base, sort_keys=False), encoding="utf-8")
    except Exception as exc:
        return {"error": f"Failed to write working copy: {exc}"}

    import yaml as _yaml  # re-import for dump
    return {
        "scenario_id": scenario_id,
        "applied_patches": applied,
        "validation_errors": errors,
        "working_copy_path": str(working_path),
        "yaml_preview": _yaml.safe_dump(base, sort_keys=False),
    }


def _set_dotpath(obj: dict, dot_path: str, value: Any) -> None:
    """Set a value in a nested dict using a dot-notation path."""
    parts = dot_path.split(".")
    for part in parts[:-1]:
        if part not in obj or not isinstance(obj[part], dict):
            obj[part] = {}
        obj = obj[part]
    obj[parts[-1]] = value


def _validate_scenario_dict(scenario: dict) -> list[str]:
    """Run sog_phase2 parse validators and return list of error strings."""
    errors = []
    try:
        from sog_phase2.selection import parse_selection_config
        from sog_phase2.simulator import parse_simulation_config
        from sog_phase2.emission import parse_emission_config
        from sog_phase2.quality import parse_quality_config
        from sog_phase2.constraints import parse_constraints_config
        from sog_phase2.output_contract import validate_scenario_id

        sid = str(scenario.get("scenario_id", "")).strip()
        if sid:
            try:
                validate_scenario_id(sid)
            except Exception as exc:
                errors.append(f"scenario_id: {exc}")

        for parser, key in [
            (parse_selection_config, "selection"),
            (parse_simulation_config, "simulation"),
            (parse_emission_config, "emission"),
            (parse_quality_config, "quality"),
            (parse_constraints_config, "constraints"),
        ]:
            try:
                parser(scenario.get(key))
            except Exception as exc:
                errors.append(f"{key}: {exc}")
    except ImportError as exc:
        errors.append(f"Import error during validation: {exc}")
    return errors


# ---------------------------------------------------------------------------
# run_scenario
# ---------------------------------------------------------------------------

def run_scenario(
    scenario_id: str,
    seed: int | None = None,
    session_id: str = "",
    overwrite: bool = True,
) -> dict[str, Any]:
    """Run the full Phase-2 pipeline for a scenario.

    Uses the session working copy if one exists, otherwise the template.
    Returns result dict on success or {"status": "error", "message": ...} on failure.
    """
    # Resolve YAML path (prefer working copy)
    working_path = SCENARIOS_DIR / f"_working_{session_id}_{scenario_id}.yaml"
    if session_id and working_path.exists():
        yaml_path = working_path
        using_working_copy = True
    else:
        yaml_path = SCENARIOS_DIR / f"{scenario_id}.yaml"
        using_working_copy = False

    if not yaml_path.exists():
        return {"status": "error", "message": f"Scenario YAML not found: {yaml_path}"}

    # Apply seed override if provided
    if seed is not None:
        import yaml
        try:
            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
            raw["seed"] = int(seed)
            # Write to a temp override YAML
            override_path = SCENARIOS_DIR / f"_working_{session_id}_{scenario_id}_seeded.yaml"
            override_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
            yaml_path = override_path
        except Exception as exc:
            return {"status": "error", "message": f"Failed to apply seed: {exc}"}

    try:
        from frontend.pipeline_bridge import run_pipeline_sync
        result = run_pipeline_sync(
            scenario_yaml_path=yaml_path,
            overwrite=overwrite,
        )
        result["status"] = "ok"
        result["used_working_copy"] = using_working_copy
        # Clean up working copy after successful run
        _cleanup_working_copies(session_id, scenario_id)
        return result
    except FileExistsError as exc:
        return {
            "status": "error",
            "message": str(exc),
            "hint": "Run again with overwrite=True to replace existing outputs.",
        }
    except Exception as exc:
        return {"status": "error", "message": str(exc)}


def _cleanup_working_copies(session_id: str, scenario_id: str) -> None:
    """Remove session-scoped working YAML copies after a run."""
    for path in SCENARIOS_DIR.glob(f"_working_{session_id}_{scenario_id}*.yaml"):
        try:
            path.unlink()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# get_run_results
# ---------------------------------------------------------------------------

def get_run_results(run_id: str) -> dict[str, Any]:
    """Return quality metrics, event counts, and download paths for a completed run."""
    run_dir = RUNS_ROOT / run_id
    if not run_dir.exists():
        return {"error": f"Run directory not found: {run_id}"}

    result: dict[str, Any] = {"run_id": run_id}

    # Read quality report
    qr_path = run_dir / "quality_report.json"
    if qr_path.exists():
        try:
            qr = json.loads(qr_path.read_text(encoding="utf-8"))
            result["quality_status"] = qr.get("status", "unknown")
            result["truth_counts"] = qr.get("truth_counts", {})
            result["event_counts"] = (
                qr.get("simulation_quality", {}).get("event_counts", {})
            )
            result["observed_quality"] = qr.get("observed_quality", {})
        except Exception as exc:
            result["quality_report_error"] = str(exc)

    # Read manifest
    manifest_path = run_dir / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            result["scenario_id"] = manifest.get("scenario_id", "")
            result["seed"] = manifest.get("seed", "")
            result["generated_at_utc"] = manifest.get("generated_at_utc", "")
        except Exception as exc:
            result["manifest_error"] = str(exc)

    # Build download paths (only include files that exist)
    download_paths = {}
    for label, filename in [
        ("DatasetA", "DatasetA.csv"),
        ("DatasetB", "DatasetB.csv"),
        ("truth_crosswalk", "truth_crosswalk.csv"),
        ("quality_report", "quality_report.json"),
    ]:
        p = run_dir / filename
        if p.exists():
            download_paths[label] = str(p)
    result["download_paths"] = download_paths

    return result
