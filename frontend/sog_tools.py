"""Tool implementations called by the configured assistant model.

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
_frontend_dir = str(PROJECT_ROOT / "frontend")
if _frontend_dir not in sys.path:
    sys.path.insert(0, _frontend_dir)


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


def _list_scenarios_with_catalog() -> dict[str, Any]:
    """Return runnable scenario templates enriched with catalog metadata."""
    try:
        from sog_phase2.scenario_catalog import get_scenario_catalog_by_id, get_scenario_catalog_summary

        catalog_by_id = get_scenario_catalog_by_id()
        catalog_summary = get_scenario_catalog_summary()
    except Exception:
        catalog_by_id = {}
        catalog_summary = {}

    scenarios = []
    for yaml_path in sorted(SCENARIOS_DIR.glob("*.yaml")):
        if yaml_path.stem.startswith("_") or yaml_path.name == "catalog.yaml":
            continue
        try:
            import yaml

            raw = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        scenario_id = str(raw.get("scenario_id", yaml_path.stem))
        params = raw.get("parameters", {})
        catalog_entry = catalog_by_id.get(scenario_id, {})
        scenarios.append(
            {
                "scenario_id": scenario_id,
                "title": str(catalog_entry.get("title", scenario_id)).strip(),
                "description": str(catalog_entry.get("summary", "")).strip(),
                "status": str(catalog_entry.get("status", "supported")).strip() or "supported",
                "delivery_mode": str(catalog_entry.get("delivery_mode", "canonical_yaml")).strip() or "canonical_yaml",
                "topology": str(catalog_entry.get("topology", "pairwise")).strip() or "pairwise",
                "cardinality": str(
                    catalog_entry.get(
                        "cardinality",
                        raw.get("emission", {}).get("crossfile_match_mode", ""),
                    )
                ).strip(),
                "user_intents": list(catalog_entry.get("user_intents", []))
                if isinstance(catalog_entry.get("user_intents", []), list)
                else [],
                "key_parameters": params,
                "emission_match_mode": raw.get("emission", {}).get("crossfile_match_mode", ""),
            }
        )

    planned_scenarios = [
        entry
        for entry in catalog_by_id.values()
        if str(entry.get("status", "")).strip() != "supported"
    ]
    return {
        "scenarios": scenarios,
        "planned_scenarios": planned_scenarios,
        "catalog_summary": catalog_summary,
        "count": len(scenarios),
    }


list_scenarios = _list_scenarios_with_catalog


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
    working_path = SCENARIOS_DIR / f"_working_{session_id}_{scenario_id}.yaml"
    if session_id and working_path.exists():
        source_path = working_path
    else:
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
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            result["scenario_id"] = manifest.get("scenario_id", "")
            result["seed"] = manifest.get("seed", "")
            result["generated_at_utc"] = manifest.get("generated_at_utc", "")
        except Exception as exc:
            result["manifest_error"] = str(exc)

    download_paths = {}
    observed_outputs = manifest.get("observed_outputs", {}) if isinstance(manifest, dict) else {}
    if isinstance(observed_outputs, dict):
        datasets = observed_outputs.get("datasets", [])
        if isinstance(datasets, list):
            for item in datasets:
                if not isinstance(item, dict):
                    continue
                dataset_id = str(item.get("dataset_id", "")).strip()
                path = Path(str(item.get("path", "")).strip()) if str(item.get("path", "")).strip() else None
                if dataset_id and path and path.exists():
                    download_paths[dataset_id] = str(path)
        pairwise_crosswalks = observed_outputs.get("pairwise_crosswalks", [])
        if isinstance(pairwise_crosswalks, list):
            for item in pairwise_crosswalks:
                if not isinstance(item, dict):
                    continue
                dataset_ids = item.get("dataset_ids", [])
                raw_path = str(item.get("path", "")).strip()
                if not isinstance(dataset_ids, list) or len(dataset_ids) != 2 or not raw_path:
                    continue
                path = Path(raw_path)
                if path.exists():
                    label = f"truth_crosswalk__{str(dataset_ids[0]).strip()}__{str(dataset_ids[1]).strip()}"
                    download_paths[label] = str(path)
        for label in ("dataset_a", "dataset_b", "entity_record_map", "truth_crosswalk"):
            raw_path = str(observed_outputs.get(label, "")).strip()
            if raw_path:
                path = Path(raw_path)
                if path.exists():
                    download_paths[label] = str(path)

    if not download_paths:
        for label, filename in [
            ("DatasetA", "DatasetA.csv"),
            ("DatasetB", "DatasetB.csv"),
            ("entity_record_map", "entity_record_map.csv"),
            ("truth_crosswalk", "truth_crosswalk.csv"),
        ]:
            p = run_dir / filename
            if p.exists():
                download_paths[label] = str(p)

    qr_file = run_dir / "quality_report.json"
    if qr_file.exists():
        download_paths["quality_report"] = str(qr_file)
    result["download_paths"] = download_paths

    return result


# ---------------------------------------------------------------------------
# validate_scenario
# ---------------------------------------------------------------------------

def validate_scenario(scenario_id: str, session_id: str = "") -> dict[str, Any]:
    """Validate a scenario YAML without writing or running anything."""
    working_path = SCENARIOS_DIR / f"_working_{session_id}_{scenario_id}.yaml"
    if session_id and working_path.exists():
        path = working_path
    else:
        path = SCENARIOS_DIR / f"{scenario_id}.yaml"

    if not path.exists():
        return {"valid": False, "errors": [f"Scenario not found: {scenario_id}"]}

    import yaml
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return {"valid": False, "errors": [f"YAML parse error: {exc}"]}

    errors = _validate_scenario_dict(raw)
    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "scenario_id": raw.get("scenario_id", scenario_id),
    }


# ---------------------------------------------------------------------------
# create_scenario_from_template
# ---------------------------------------------------------------------------

def create_scenario_from_template(
    template_id: str,
    new_id: str,
    patches: dict[str, Any],
    session_id: str = "",
) -> dict[str, Any]:
    """Copy a template scenario, apply patches, validate, and save as a working copy."""
    template_path = SCENARIOS_DIR / f"{template_id}.yaml"
    if not template_path.exists():
        return {"error": f"Template not found: {template_id}"}

    import yaml
    try:
        base = yaml.safe_load(template_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return {"error": f"Failed to read template: {exc}"}

    base["scenario_id"] = new_id

    applied: dict[str, Any] = {}
    errors: list[str] = []
    for dot_path, value in patches.items():
        try:
            _set_dotpath(base, dot_path, value)
            applied[dot_path] = value
        except Exception as exc:
            errors.append(f"{dot_path}: {exc}")

    validation_errors = _validate_scenario_dict(base)
    errors.extend(validation_errors)

    working_path = SCENARIOS_DIR / f"_working_{session_id}_{new_id}.yaml"
    try:
        working_path.write_text(yaml.safe_dump(base, sort_keys=False), encoding="utf-8")
    except Exception as exc:
        return {"error": f"Failed to write working copy: {exc}"}

    return {
        "scenario_id": new_id,
        "template_id": template_id,
        "applied_patches": applied,
        "validation_errors": errors,
        "valid": len(errors) == 0,
        "working_copy_path": str(working_path),
    }


# ---------------------------------------------------------------------------
# get_schema_info
# ---------------------------------------------------------------------------

def get_schema_info(section: str) -> dict[str, Any]:
    """Return the live schema dict for a scenario config section."""
    from sog_phase2 import (
        get_constraints_schema,
        get_emission_schema,
        get_quality_schema,
        get_selection_schema,
        get_simulation_schema,
        get_truth_event_grammar,
    )

    schemas = {
        "selection": get_selection_schema,
        "simulation": get_simulation_schema,
        "emission": get_emission_schema,
        "quality": get_quality_schema,
        "constraints": get_constraints_schema,
        "events": get_truth_event_grammar,
    }
    if section not in schemas:
        return {
            "error": f"Unknown section: {section!r}",
            "available_sections": list(schemas.keys()),
        }
    return {"section": section, "schema": schemas[section]()}


# ---------------------------------------------------------------------------
# submit_run_async / poll_run_status
# ---------------------------------------------------------------------------

def submit_run_async(
    scenario_id: str,
    session_id: str = "",
    overwrite: bool = False,
) -> dict[str, Any]:
    """Submit a scenario run as a background job. Returns immediately with job_id."""
    from async_runner import submit_run

    working_path = SCENARIOS_DIR / f"_working_{session_id}_{scenario_id}.yaml"
    if session_id and working_path.exists():
        yaml_path = working_path
    else:
        yaml_path = SCENARIOS_DIR / f"{scenario_id}.yaml"

    if not yaml_path.exists():
        return {"error": f"Scenario not found: {scenario_id}"}

    job_id = submit_run(
        scenario_yaml_path=yaml_path,
        scenario_id=scenario_id,
        overwrite=overwrite,
    )
    return {
        "job_id": job_id,
        "scenario_id": scenario_id,
        "status": "submitted",
        "message": f"Job {job_id} submitted. Use poll_run_status to check progress.",
    }


def poll_run_status(job_id: str) -> dict[str, Any]:
    """Poll the current status of a submitted async job."""
    from async_runner import poll_status
    return poll_status(job_id)


# ---------------------------------------------------------------------------
# list_recent_runs
# ---------------------------------------------------------------------------

def list_recent_runs(
    limit: int = 10,
    scenario_id_filter: str = "",
) -> dict[str, Any]:
    """List recent completed runs, optionally filtered by scenario_id."""
    runs = []
    if not RUNS_ROOT.exists():
        return {"runs": [], "count": 0}

    dirs = sorted(RUNS_ROOT.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
    for run_dir in dirs:
        if len(runs) >= limit:
            break
        if not run_dir.is_dir():
            continue
        manifest_path = run_dir / "manifest.json"
        if not manifest_path.exists():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            sid = manifest.get("scenario_id", "")
            if scenario_id_filter and sid != scenario_id_filter:
                continue
            qr_path = run_dir / "quality_report.json"
            quality_status = "unknown"
            if qr_path.exists():
                try:
                    qr = json.loads(qr_path.read_text(encoding="utf-8"))
                    quality_status = qr.get("status", "unknown")
                except Exception:
                    pass
            runs.append({
                "run_id": run_dir.name,
                "scenario_id": sid,
                "seed": manifest.get("seed"),
                "generated_at_utc": manifest.get("generated_at_utc"),
                "quality_status": quality_status,
            })
        except Exception:
            pass

    return {"runs": runs, "count": len(runs)}


# ---------------------------------------------------------------------------
# compare_runs
# ---------------------------------------------------------------------------

def compare_runs(run_id_a: str, run_id_b: str) -> dict[str, Any]:
    """Diff two runs — quality metrics and scenario parameters."""
    dir_a = RUNS_ROOT / run_id_a
    dir_b = RUNS_ROOT / run_id_b

    if not dir_a.exists():
        return {"error": f"Run not found: {run_id_a}"}
    if not dir_b.exists():
        return {"error": f"Run not found: {run_id_b}"}

    result: dict[str, Any] = {"run_id_a": run_id_a, "run_id_b": run_id_b}

    # Compare quality reports
    for label, rdir in [("a", dir_a), ("b", dir_b)]:
        qr_path = rdir / "quality_report.json"
        if qr_path.exists():
            try:
                qr = json.loads(qr_path.read_text(encoding="utf-8"))
                result.setdefault("quality", {})[label] = {
                    "status": qr.get("status"),
                    "truth_counts": qr.get("truth_counts", {}),
                    "event_counts": qr.get("simulation_quality", {}).get("event_counts", {}),
                    "observed_quality": qr.get("observed_quality", {}),
                }
            except Exception as exc:
                result.setdefault("quality", {})[label] = {"error": str(exc)}

    # Compare scenario configs
    import yaml
    for label, rdir in [("a", dir_a), ("b", dir_b)]:
        scen_path = rdir / "scenario.yaml"
        if scen_path.exists():
            try:
                scen = yaml.safe_load(scen_path.read_text(encoding="utf-8")) or {}
                emission = scen.get("emission", {}) if isinstance(scen, dict) else {}
                datasets = emission.get("datasets", []) if isinstance(emission, dict) else []
                emission_summary: dict[str, Any] = {
                    k: v for k, v in emission.items() if k not in {"noise", "datasets"}
                }
                if isinstance(datasets, list) and datasets:
                    emission_summary["datasets"] = [
                        {
                            "dataset_id": item.get("dataset_id", ""),
                            "filename": item.get("filename", ""),
                            "snapshot": item.get("snapshot", ""),
                            "appearance_pct": item.get("appearance_pct", ""),
                            "duplication_pct": item.get("duplication_pct", ""),
                        }
                        for item in datasets
                        if isinstance(item, dict)
                    ]
                result.setdefault("scenario", {})[label] = {
                    "seed": scen.get("seed"),
                    "parameters": scen.get("parameters", {}),
                    "emission": emission_summary,
                    "noise_A": emission.get("noise", {}).get("A", {}) if isinstance(emission.get("noise", {}), dict) else {},
                    "noise_B": emission.get("noise", {}).get("B", {}) if isinstance(emission.get("noise", {}), dict) else {},
                    "dataset_noises": {
                        str(item.get("dataset_id", "")).strip(): item.get("noise", {})
                        for item in datasets
                        if isinstance(item, dict) and str(item.get("dataset_id", "")).strip()
                    },
                }
            except Exception as exc:
                result.setdefault("scenario", {})[label] = {"error": str(exc)}

    return result


# ---------------------------------------------------------------------------
# summarize_run_for_er
# ---------------------------------------------------------------------------

def summarize_run_for_er(run_id: str) -> dict[str, Any]:
    """Compute an ER-focused summary with difficulty rating (0-6 score)."""
    run_dir = RUNS_ROOT / run_id
    if not run_dir.exists():
        return {"error": f"Run not found: {run_id}"}

    qr_path = run_dir / "quality_report.json"
    if not qr_path.exists():
        return {"error": f"quality_report.json not found for {run_id}"}

    try:
        qr = json.loads(qr_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"error": f"Failed to read quality report: {exc}"}

    import yaml
    scenario: dict[str, Any] = {}
    scen_path = run_dir / "scenario.yaml"
    if scen_path.exists():
        try:
            scenario = yaml.safe_load(scen_path.read_text(encoding="utf-8")) or {}
        except Exception:
            pass

    emission = scenario.get("emission", {})
    match_mode = emission.get("crossfile_match_mode", "one_to_one")
    datasets_cfg = emission.get("datasets", [])
    if isinstance(datasets_cfg, list) and datasets_cfg:
        overlap = emission.get("overlap_entity_pct", 100.0 if len(datasets_cfg) == 1 else 70.0)
        duplication_values = [float((item or {}).get("duplication_pct", 0.0) or 0.0) for item in datasets_cfg if isinstance(item, dict)]
        name_noise_values = [
            sum(
                ((item.get("noise", {}) or {}).get(field, 0) for field in ["name_typo_pct", "phonetic_error_pct", "ocr_error_pct", "nickname_pct"])
            )
            for item in datasets_cfg
            if isinstance(item, dict)
        ]
        dup_a = duplication_values[0] if duplication_values else 0.0
        dup_b = duplication_values[1] if len(duplication_values) > 1 else 0.0
        name_noise_a = name_noise_values[0] if name_noise_values else 0.0
        name_noise_b = name_noise_values[1] if len(name_noise_values) > 1 else 0.0
        total_name_noise = float(sum(name_noise_values))
    else:
        noise_a = emission.get("noise", {}).get("A", {})
        noise_b = emission.get("noise", {}).get("B", {})
        overlap = emission.get("overlap_entity_pct", 100.0)
        dup_a = emission.get("duplication_in_A_pct", 0.0)
        dup_b = emission.get("duplication_in_B_pct", 0.0)
        name_noise_a = sum([
            noise_a.get("name_typo_pct", 0),
            noise_a.get("phonetic_error_pct", 0),
            noise_a.get("ocr_error_pct", 0),
            noise_a.get("nickname_pct", 0),
        ])
        name_noise_b = sum([
            noise_b.get("name_typo_pct", 0),
            noise_b.get("phonetic_error_pct", 0),
            noise_b.get("ocr_error_pct", 0),
            noise_b.get("nickname_pct", 0),
        ])
        total_name_noise = name_noise_a + name_noise_b

    # Compute difficulty score
    score = 0
    if match_mode != "single_dataset":
        if overlap < 40:
            score += 3
        elif overlap < 60:
            score += 2
        elif overlap < 75:
            score += 1

    if total_name_noise > 10:
        score += 2
    elif total_name_noise > 5:
        score += 1

    if dup_a > 10 or dup_b > 10:
        score += 1

    if match_mode == "many_to_many":
        score += 1

    rating = {0: "VERY EASY", 1: "EASY", 2: "MEDIUM", 3: "HARD", 4: "VERY HARD"}.get(
        min(score, 4), "EXTREME"
    )

    truth_counts = qr.get("truth_counts", {})
    event_counts = qr.get("simulation_quality", {}).get("event_counts", {})
    total_events = sum(event_counts.values()) if event_counts else 0

    return {
        "run_id": run_id,
        "scenario_id": scenario.get("scenario_id", ""),
        "difficulty_score": score,
        "difficulty_rating": rating,
        "summary": {
            "population": truth_counts.get("truth_people", 0),
            "total_events": total_events,
            "event_counts": event_counts,
            "overlap_pct": overlap,
            "match_mode": match_mode,
            "duplication_a_pct": dup_a,
            "duplication_b_pct": dup_b,
            "name_noise_a": name_noise_a,
            "name_noise_b": name_noise_b,
            "quality_status": qr.get("status", "unknown"),
        },
    }


# ---------------------------------------------------------------------------
# list_difficulty_presets / apply_difficulty_preset
# ---------------------------------------------------------------------------

def list_difficulty_presets() -> dict[str, Any]:
    """List all available difficulty presets with descriptions."""
    from presets import list_presets
    return {"presets": list_presets()}


def apply_difficulty_preset(
    scenario_id: str,
    preset_name: str,
    session_id: str = "",
) -> dict[str, Any]:
    """Apply a named difficulty preset to a scenario (creates a working copy)."""
    from presets import get_preset
    try:
        preset = get_preset(preset_name)
    except ValueError as exc:
        return {"error": str(exc)}

    return create_scenario_from_template(
        template_id=scenario_id,
        new_id=f"{scenario_id}_{preset_name}",
        patches=preset["patches"],
        session_id=session_id,
    )


# ---------------------------------------------------------------------------
# generate_chart / generate_dashboard
# ---------------------------------------------------------------------------

def generate_chart(
    run_id: str,
    chart_type: str,
    fmt: str = "png",
) -> dict[str, Any]:
    """Generate a single visualization chart for a run."""
    try:
        try:
            from frontend.visualizations.core import ChartGenerator, ChartSpec
        except ImportError:
            from visualizations.core import ChartGenerator, ChartSpec

        generator = ChartGenerator()
        effective_fmt = fmt
        if chart_type == "noise_radar" and fmt == "png":
            # Radar is Plotly-based; default to HTML so we do not silently write
            # HTML content into a .png when static export dependencies are absent.
            effective_fmt = "html"
        spec = ChartSpec(chart_type=chart_type, run_id=run_id, fmt=effective_fmt)
        path = generator.generate(spec)
        insight = generator.get_insight(spec)
        return {
            "success": True,
            "chart_path": str(path),
            "chart_type": chart_type,
            "insight": insight,
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


def generate_dashboard(
    run_id: str,
    include_charts: list[str] | None = None,
) -> dict[str, Any]:
    """Generate a set of charts for a run. Returns list of {chart_path, insight}."""
    default_charts = ["difficulty_scorecard", "noise_radar", "overlap_venn", "missing_matrix"]
    charts_to_gen = include_charts or default_charts
    run_dir = RUNS_ROOT / run_id
    scenario_path = run_dir / "scenario.yaml"
    if include_charts is None and scenario_path.exists():
        try:
            import yaml

            scenario = yaml.safe_load(scenario_path.read_text(encoding="utf-8")) or {}
            if scenario.get("emission", {}).get("crossfile_match_mode") == "single_dataset":
                charts_to_gen = [chart for chart in default_charts if chart != "overlap_venn"]
        except Exception:
            pass
    results = []
    for ct in charts_to_gen:
        chart_fmt = "html" if ct == "noise_radar" else "png"
        r = generate_chart(run_id, ct, fmt=chart_fmt)
        if r.get("success"):
            results.append({"chart_path": r["chart_path"], "insight": r.get("insight", ""), "chart_type": ct})
    return {"charts": results, "count": len(results), "run_id": run_id}
