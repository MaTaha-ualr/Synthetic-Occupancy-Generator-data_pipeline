from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
import json
from pathlib import Path
import re
from typing import Any

import pandas as pd
import pyarrow.parquet as pq
import yaml

from .constraints import (
    get_constraints_schema,
    parse_constraints_config,
    validate_constraints_for_run,
)
from .event_grammar import (
    TRUTH_EVENTS_REQUIRED_COLUMNS,
    get_truth_event_grammar,
    validate_truth_events_parquet,
)
from .selection import get_selection_schema, parse_selection_config
from .simulator import get_simulation_schema, parse_simulation_config
from .emission import get_emission_schema, parse_emission_config
from .quality import get_quality_schema, parse_quality_config

SCENARIO_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
RUN_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RUN_ID_PATTERN = re.compile(
    r"^(?P<run_date>\d{4}-\d{2}-\d{2})_(?P<scenario_id>[A-Za-z0-9][A-Za-z0-9_-]*)_seed(?P<seed>\d+)$"
)


@dataclass(frozen=True)
class OutputSchema:
    filename: str
    file_format: str
    required_columns: tuple[str, ...]
    required_any_of: tuple[tuple[str, ...], ...] = ()
    layer: str = "truth"


PHASE2_TRUTH_OUTPUTS: dict[str, OutputSchema] = {
    "truth_people": OutputSchema(
        filename="truth_people.parquet",
        file_format="parquet",
        required_columns=(
            "PersonKey",
            "FormalFirstName",
            "MiddleName",
            "LastName",
            "Suffix",
            "FormalFullName",
            "Gender",
            "Ethnicity",
            "DOB",
            "Age",
            "AgeBin",
            "SSN",
        ),
        layer="truth",
    ),
    "truth_households": OutputSchema(
        filename="truth_households.parquet",
        file_format="parquet",
        required_columns=(
            "HouseholdKey",
            "HouseholdType",
            "HouseholdStartDate",
            "HouseholdEndDate",
        ),
        layer="truth",
    ),
    "truth_household_memberships": OutputSchema(
        filename="truth_household_memberships.parquet",
        file_format="parquet",
        required_columns=(
            "PersonKey",
            "HouseholdKey",
            "HouseholdRole",
            "MembershipStartDate",
            "MembershipEndDate",
        ),
        layer="truth",
    ),
    "truth_residence_history": OutputSchema(
        filename="truth_residence_history.parquet",
        file_format="parquet",
        required_columns=(
            "PersonKey",
            "AddressKey",
            "ResidenceStartDate",
            "ResidenceEndDate",
        ),
        layer="truth",
    ),
    "truth_events": OutputSchema(
        filename="truth_events.parquet",
        file_format="parquet",
        required_columns=TRUTH_EVENTS_REQUIRED_COLUMNS,
        layer="truth",
    ),
    "scenario_population": OutputSchema(
        filename="scenario_population.parquet",
        file_format="parquet",
        required_columns=(
            "PersonKey",
            "ScenarioId",
            "SelectionSeed",
            "AgeBin",
            "Gender",
            "Ethnicity",
            "ResidenceType",
            "RecordsPerEntity",
            "RedundancyProfile",
            "MobilityPropensityScore",
            "MobilityPropensityBucket",
            "PartnershipPropensityScore",
            "PartnershipPropensityBucket",
            "FertilityPropensityScore",
            "FertilityPropensityBucket",
        ),
        layer="truth",
    ),
}

PHASE2_STATIC_OBSERVED_OUTPUTS: dict[str, OutputSchema] = {
    "entity_record_map": OutputSchema(
        filename="entity_record_map.csv",
        file_format="csv",
        required_columns=("PersonKey", "DatasetId", "RecordKey"),
        layer="observed",
    ),
    "truth_crosswalk": OutputSchema(
        filename="truth_crosswalk.csv",
        file_format="csv",
        required_columns=("A_RecordKey", "B_RecordKey"),
        required_any_of=(("PersonKey", "EntityKey"),),
        layer="observed",
    ),
}

PHASE2_OUTPUT_CONTRACT: dict[str, OutputSchema] = {
    **PHASE2_TRUTH_OUTPUTS,
    **PHASE2_STATIC_OBSERVED_OUTPUTS,
}

PHASE2_RUN_META_FILES: dict[str, str] = {
    "scenario_yaml": "scenario.yaml",
    "scenario_selection_log_json": "scenario_selection_log.json",
    "manifest_json": "manifest.json",
    "quality_report_json": "quality_report.json",
}

OBSERVED_ADDRESS_COLUMNS: tuple[str, ...] = (
    "HouseNumber",
    "StreetName",
    "UnitType",
    "UnitNumber",
    "StreetAddress",
    "City",
    "State",
    "ZipCode",
)


def validate_scenario_id(scenario_id: str) -> str:
    value = str(scenario_id).strip()
    if not value:
        raise ValueError("scenario_id is required")
    if not SCENARIO_ID_PATTERN.fullmatch(value):
        raise ValueError(
            "scenario_id must match ^[A-Za-z0-9][A-Za-z0-9_-]*$ "
            "(letters, digits, underscore, dash)"
        )
    return value


def validate_seed(seed: Any) -> int:
    value = int(seed)
    if value < 0:
        raise ValueError("seed must be >= 0")
    return value


def parse_run_id(run_id: str) -> dict[str, Any]:
    rid = str(run_id).strip()
    if not rid:
        raise ValueError("run_id is required")
    match = RUN_ID_PATTERN.fullmatch(rid)
    if not match:
        raise ValueError(
            "run_id must match YYYY-MM-DD_<scenario_id>_seed<seed> "
            "(example: 2026-03-10_single_movers_seed20260310)"
        )
    scenario_id = validate_scenario_id(match.group("scenario_id"))
    seed = validate_seed(match.group("seed"))
    run_date = match.group("run_date")
    return {
        "run_id": rid,
        "run_date": run_date,
        "scenario_id": scenario_id,
        "seed": seed,
    }


def build_run_id(scenario_id: str, seed: Any, run_date: str) -> str:
    sid = validate_scenario_id(scenario_id)
    s = validate_seed(seed)
    rd = str(run_date).strip()
    if not RUN_DATE_PATTERN.fullmatch(rd):
        raise ValueError("run_date must match YYYY-MM-DD")
    return f"{rd}_{sid}_seed{s}"


def expected_phase2_run_output_paths(runs_root: Path, run_id: str) -> dict[str, Path]:
    run_info = parse_run_id(run_id)
    run_dir = runs_root.resolve() / run_info["run_id"]
    return {
        logical_name: run_dir / schema.filename
        for logical_name, schema in PHASE2_OUTPUT_CONTRACT.items()
    }


def expected_phase2_run_artifact_paths(runs_root: Path, run_id: str) -> dict[str, Path]:
    run_info = parse_run_id(run_id)
    run_dir = runs_root.resolve() / run_info["run_id"]
    paths = expected_phase2_run_output_paths(runs_root=runs_root, run_id=run_id)
    for logical_name, filename in PHASE2_RUN_META_FILES.items():
        paths[logical_name] = run_dir / filename
    return paths


def expected_observed_dataset_paths(run_dir: Path, emission_config: Any) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for dataset in getattr(emission_config, "datasets", ()):
        paths[f"dataset__{dataset.dataset_id}"] = run_dir / dataset.filename
    return paths


def pairwise_crosswalk_filename(first_dataset_id: str, second_dataset_id: str) -> str:
    return f"truth_crosswalk__{first_dataset_id}__{second_dataset_id}.csv"


def expected_pairwise_crosswalk_paths(run_dir: Path, emission_config: Any) -> dict[str, Path]:
    datasets = list(getattr(emission_config, "datasets", ()))
    if len(datasets) <= 2:
        return {}
    return {
        f"{first.dataset_id}__{second.dataset_id}": run_dir / pairwise_crosswalk_filename(first.dataset_id, second.dataset_id)
        for first, second in combinations(datasets, 2)
    }


def get_observed_output_schemas(emission_config: Any) -> dict[str, OutputSchema]:
    schemas: dict[str, OutputSchema] = {
        "entity_record_map": PHASE2_STATIC_OBSERVED_OUTPUTS["entity_record_map"],
    }
    datasets = list(getattr(emission_config, "datasets", ()))
    for dataset in datasets:
        required_columns = (
            "DatasetId",
            "FirstName",
            "LastName",
            "DOB",
            "AddressKey",
            *OBSERVED_ADDRESS_COLUMNS,
            "SourceSnapshotDate",
        )
        required_any_of: tuple[tuple[str, ...], ...] = (("RecordKey",),)
        if dataset.filename == "DatasetA.csv":
            required_any_of = (("RecordKey", "A_RecordKey"),)
        elif dataset.filename == "DatasetB.csv":
            required_any_of = (("RecordKey", "B_RecordKey"),)
        schemas[f"dataset__{dataset.dataset_id}"] = OutputSchema(
            filename=dataset.filename,
            file_format="csv",
            required_columns=required_columns,
            required_any_of=required_any_of,
            layer="observed",
        )
    if len(datasets) == 2:
        schemas["truth_crosswalk"] = PHASE2_STATIC_OBSERVED_OUTPUTS["truth_crosswalk"]
    return schemas


def get_phase2_output_contract() -> dict[str, dict[str, Any]]:
    truth_outputs = {
        logical_name: {
            "filename": schema.filename,
            "file_format": schema.file_format,
            "required_columns": list(schema.required_columns),
            "required_any_of": [list(group) for group in schema.required_any_of],
            "layer": schema.layer,
        }
        for logical_name, schema in PHASE2_TRUTH_OUTPUTS.items()
    }
    canonical_observed_outputs = {
        "entity_record_map": {
            "filename": PHASE2_STATIC_OBSERVED_OUTPUTS["entity_record_map"].filename,
            "file_format": PHASE2_STATIC_OBSERVED_OUTPUTS["entity_record_map"].file_format,
            "required_columns": list(PHASE2_STATIC_OBSERVED_OUTPUTS["entity_record_map"].required_columns),
            "required_any_of": [
                list(group) for group in PHASE2_STATIC_OBSERVED_OUTPUTS["entity_record_map"].required_any_of
            ],
            "layer": PHASE2_STATIC_OBSERVED_OUTPUTS["entity_record_map"].layer,
        }
    }
    pairwise_observed_outputs = {
        "truth_crosswalk": {
            "filename": PHASE2_STATIC_OBSERVED_OUTPUTS["truth_crosswalk"].filename,
            "file_format": PHASE2_STATIC_OBSERVED_OUTPUTS["truth_crosswalk"].file_format,
            "required_columns": list(PHASE2_STATIC_OBSERVED_OUTPUTS["truth_crosswalk"].required_columns),
            "required_any_of": [
                list(group) for group in PHASE2_STATIC_OBSERVED_OUTPUTS["truth_crosswalk"].required_any_of
            ],
            "layer": PHASE2_STATIC_OBSERVED_OUTPUTS["truth_crosswalk"].layer,
        }
    }
    legacy_contract = {
        logical_name: {
            "filename": schema.filename,
            "file_format": schema.file_format,
            "required_columns": list(schema.required_columns),
            "required_any_of": [list(group) for group in schema.required_any_of],
            "layer": schema.layer,
        }
        for logical_name, schema in PHASE2_OUTPUT_CONTRACT.items()
    }
    return {
        "run_id_pattern": "YYYY-MM-DD_<scenario_id>_seed<seed>",
        "runs_root_default": "phase2/runs",
        "required_outputs": {
            **truth_outputs,
            **canonical_observed_outputs,
        },
        "pairwise_observed_outputs": pairwise_observed_outputs,
        "observed_outputs_dynamic": {
            "dataset_files": "Derived from scenario.emission.datasets[*].filename",
            "entity_record_map": "Always required for canonical observed truth mapping",
            "truth_crosswalk": "Required only for two-dataset runs as a backward-compatible pairwise artifact",
            "pairwise_crosswalks": "Optional per-pair crosswalk files for runs with more than two datasets",
        },
        "legacy_internal_contract": legacy_contract,
        "required_meta_files": PHASE2_RUN_META_FILES,
        "truth_event_grammar": get_truth_event_grammar(),
        "constraints_schema": get_constraints_schema(),
        "selection_schema": get_selection_schema(),
        "simulation_schema": get_simulation_schema(),
        "emission_schema": get_emission_schema(),
        "quality_schema": get_quality_schema(),
    }


def _read_columns(path: Path, file_format: str) -> list[str]:
    if file_format == "csv":
        return [str(col) for col in pd.read_csv(path, nrows=0).columns.tolist()]
    if file_format == "parquet":
        return [str(name) for name in pq.read_schema(path).names]
    raise ValueError(f"Unsupported file format: {file_format}")


def _read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _read_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def _validate_scenario_yaml(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    data = _read_yaml(path)
    if not isinstance(data, dict):
        return None, ["scenario.yaml must contain a YAML mapping"]

    scenario_id = data.get("scenario_id")
    seed = data.get("seed")
    phase1 = data.get("phase1")
    selection = data.get("selection")
    simulation = data.get("simulation")
    emission = data.get("emission")
    quality = data.get("quality")

    if scenario_id is None:
        errors.append("scenario.yaml missing required key: scenario_id")
    if seed is None:
        errors.append("scenario.yaml missing required key: seed")
    if phase1 is None:
        errors.append("scenario.yaml missing required key: phase1")
    elif not isinstance(phase1, dict):
        errors.append("scenario.yaml key 'phase1' must be a mapping")
    else:
        if not str(phase1.get("data_path", "")).strip():
            errors.append("scenario.yaml phase1.data_path is required")
        if not str(phase1.get("manifest_path", "")).strip():
            errors.append("scenario.yaml phase1.manifest_path is required")

    if selection is None:
        errors.append("scenario.yaml missing required key: selection")
    elif not isinstance(selection, dict):
        errors.append("scenario.yaml key 'selection' must be a mapping")
    else:
        try:
            parse_selection_config(selection)
        except Exception as exc:
            errors.append(f"scenario.yaml invalid selection config: {exc}")

    if simulation is not None and not isinstance(simulation, dict):
        errors.append("scenario.yaml key 'simulation' must be a mapping when provided")
    else:
        try:
            parse_simulation_config(simulation if isinstance(simulation, dict) else {})
        except Exception as exc:
            errors.append(f"scenario.yaml invalid simulation config: {exc}")

    if emission is not None and not isinstance(emission, dict):
        errors.append("scenario.yaml key 'emission' must be a mapping when provided")
    else:
        try:
            parse_emission_config(emission if isinstance(emission, dict) else {})
        except Exception as exc:
            errors.append(f"scenario.yaml invalid emission config: {exc}")

    if quality is not None and not isinstance(quality, dict):
        errors.append("scenario.yaml key 'quality' must be a mapping when provided")
    else:
        try:
            parse_quality_config(quality if isinstance(quality, dict) else {})
        except Exception as exc:
            errors.append(f"scenario.yaml invalid quality config: {exc}")

    return data, errors


def _validate_manifest_json(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    data = _read_json(path)
    if not isinstance(data, dict):
        return None, ["manifest.json must contain a JSON object"]

    required = (
        "run_id",
        "scenario_id",
        "seed",
        "phase1_input_csv",
        "phase1_input_manifest",
    )
    for key in required:
        if key not in data:
            errors.append(f"manifest.json missing required key: {key}")
        elif not str(data[key]).strip():
            errors.append(f"manifest.json key '{key}' must be non-empty")
    return data, errors


def _validate_quality_report_json(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    data = _read_json(path)
    if not isinstance(data, dict):
        return None, ["quality_report.json must contain a JSON object"]
    return data, []


def _validate_scenario_selection_log_json(path: Path) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    data = _read_json(path)
    if not isinstance(data, dict):
        return None, ["scenario_selection_log.json must contain a JSON object"]

    required = (
        "run_id",
        "scenario_id",
        "selection_seed",
        "counts",
        "selected_personkey_sha256",
    )
    for key in required:
        if key not in data:
            errors.append(f"scenario_selection_log.json missing required key: {key}")
    counts = data.get("counts")
    if counts is not None and not isinstance(counts, dict):
        errors.append("scenario_selection_log.json key 'counts' must be a JSON object")
    elif isinstance(counts, dict) and "selected_entities" not in counts:
        errors.append("scenario_selection_log.json counts.selected_entities is required")
    return data, errors


def validate_phase2_run(runs_root: Path, run_id: str) -> dict[str, Any]:
    run_info = parse_run_id(run_id)
    run_dir = runs_root.resolve() / run_info["run_id"]
    paths = expected_phase2_run_artifact_paths(runs_root=runs_root, run_id=run_id)

    missing_files: list[str] = []
    schema_errors: dict[str, Any] = {}
    metadata_errors: dict[str, list[str]] = {}
    files: dict[str, Any] = {}

    for logical_name, schema in PHASE2_TRUTH_OUTPUTS.items():
        path = paths[logical_name]
        exists = path.exists()
        files[logical_name] = {
            "path": str(path),
            "file_format": schema.file_format,
            "layer": schema.layer,
            "exists": exists,
        }
        if not exists:
            missing_files.append(logical_name)
            continue

        try:
            available_columns = _read_columns(path, schema.file_format)
        except Exception as exc:  # pragma: no cover
            schema_errors[logical_name] = {
                "path": str(path),
                "read_error": str(exc),
            }
            continue

        missing_required = [
            column for column in schema.required_columns if column not in available_columns
        ]
        missing_any_of = [
            list(group)
            for group in schema.required_any_of
            if not any(col in available_columns for col in group)
        ]
        if missing_required or missing_any_of:
            schema_errors[logical_name] = {
                "path": str(path),
                "missing_required_columns": missing_required,
                "missing_any_of_groups": missing_any_of,
                "available_columns": available_columns,
            }
            continue

        if logical_name == "truth_events":
            grammar_result = validate_truth_events_parquet(path)
            if not grammar_result["valid"]:
                schema_errors[logical_name] = {
                    "path": str(path),
                    "grammar_errors": grammar_result["errors"],
                    "grammar_error_count": grammar_result["error_count"],
                }

    scenario_payload: dict[str, Any] | None = None
    manifest_payload: dict[str, Any] | None = None
    quality_payload: dict[str, Any] | None = None
    selection_log_payload: dict[str, Any] | None = None

    for logical_name, filename in PHASE2_RUN_META_FILES.items():
        path = paths[logical_name]
        exists = path.exists()
        files[logical_name] = {
            "path": str(path),
            "file_format": "yaml" if logical_name == "scenario_yaml" else "json",
            "layer": "meta",
            "exists": exists,
        }
        if not exists:
            missing_files.append(logical_name)
            continue

        try:
            if logical_name == "scenario_yaml":
                scenario_payload, issues = _validate_scenario_yaml(path)
            elif logical_name == "scenario_selection_log_json":
                selection_log_payload, issues = _validate_scenario_selection_log_json(path)
            elif logical_name == "manifest_json":
                manifest_payload, issues = _validate_manifest_json(path)
            else:
                quality_payload, issues = _validate_quality_report_json(path)
        except Exception as exc:  # pragma: no cover
            metadata_errors[logical_name] = [f"Unable to parse {filename}: {exc}"]
            continue

        if issues:
            metadata_errors[logical_name] = issues

    run_id_checks: list[str] = []
    if scenario_payload:
        try:
            scenario_sid = validate_scenario_id(scenario_payload.get("scenario_id"))
            if scenario_sid != run_info["scenario_id"]:
                run_id_checks.append(
                    "scenario.yaml scenario_id does not match run_id scenario segment"
                )
        except Exception as exc:
            run_id_checks.append(f"scenario.yaml invalid scenario_id: {exc}")

        try:
            scenario_seed = validate_seed(scenario_payload.get("seed"))
            if scenario_seed != run_info["seed"]:
                run_id_checks.append("scenario.yaml seed does not match run_id seed segment")
        except Exception as exc:
            run_id_checks.append(f"scenario.yaml invalid seed: {exc}")

    if manifest_payload:
        if str(manifest_payload.get("run_id", "")).strip() != run_info["run_id"]:
            run_id_checks.append("manifest.json run_id does not match folder run_id")

        try:
            manifest_sid = validate_scenario_id(manifest_payload.get("scenario_id"))
            if manifest_sid != run_info["scenario_id"]:
                run_id_checks.append(
                    "manifest.json scenario_id does not match run_id scenario segment"
                )
        except Exception as exc:
            run_id_checks.append(f"manifest.json invalid scenario_id: {exc}")

        try:
            manifest_seed = validate_seed(manifest_payload.get("seed"))
            if manifest_seed != run_info["seed"]:
                run_id_checks.append("manifest.json seed does not match run_id seed segment")
        except Exception as exc:
            run_id_checks.append(f"manifest.json invalid seed: {exc}")

    if scenario_payload and manifest_payload:
        phase1_cfg = scenario_payload.get("phase1", {})
        if isinstance(phase1_cfg, dict):
            scenario_csv = str(phase1_cfg.get("data_path", "")).strip()
            scenario_manifest = str(phase1_cfg.get("manifest_path", "")).strip()
            manifest_csv = str(manifest_payload.get("phase1_input_csv", "")).strip()
            manifest_manifest = str(manifest_payload.get("phase1_input_manifest", "")).strip()
            if scenario_csv and manifest_csv and scenario_csv != manifest_csv:
                run_id_checks.append(
                    "scenario.yaml phase1.data_path does not match manifest.json phase1_input_csv"
                )
            if scenario_manifest and manifest_manifest and scenario_manifest != manifest_manifest:
                run_id_checks.append(
                    "scenario.yaml phase1.manifest_path does not match "
                    "manifest.json phase1_input_manifest"
                )

    if run_id_checks:
        metadata_errors["reproducibility"] = run_id_checks

    selection_checks: list[str] = []
    if selection_log_payload is not None:
        if str(selection_log_payload.get("run_id", "")).strip() != run_info["run_id"]:
            selection_checks.append("scenario_selection_log.json run_id does not match folder run_id")
        if str(selection_log_payload.get("scenario_id", "")).strip() != run_info["scenario_id"]:
            selection_checks.append(
                "scenario_selection_log.json scenario_id does not match run_id scenario segment"
            )
        try:
            selection_seed = validate_seed(selection_log_payload.get("selection_seed"))
            if selection_seed != run_info["seed"]:
                selection_checks.append(
                    "scenario_selection_log.json selection_seed does not match run_id seed segment"
                )
        except Exception as exc:
            selection_checks.append(f"scenario_selection_log.json invalid selection_seed: {exc}")

        counts = selection_log_payload.get("counts", {})
        if isinstance(counts, dict):
            selected_entities = counts.get("selected_entities")
            if selected_entities is not None and paths["scenario_population"].exists():
                try:
                    expected_selected = int(selected_entities)
                    actual_selected = len(pd.read_parquet(paths["scenario_population"]))
                    if expected_selected != actual_selected:
                        selection_checks.append(
                            "scenario_selection_log.json selected_entities does not match "
                            "scenario_population row count"
                        )
                except Exception as exc:
                    selection_checks.append(
                        f"Unable to compare selected_entities with scenario_population: {exc}"
                    )
    if selection_checks:
        metadata_errors["selection"] = selection_checks

    if scenario_payload is not None and isinstance(scenario_payload, dict):
        try:
            emission_cfg = parse_emission_config(scenario_payload.get("emission"))
        except Exception as exc:
            metadata_errors.setdefault("scenario_yaml", []).append(
                f"Unable to derive observed output contract from emission config: {exc}"
            )
        else:
            observed_paths = {"entity_record_map": paths["entity_record_map"]}
            observed_paths.update(expected_observed_dataset_paths(run_dir, emission_cfg))
            if len(emission_cfg.datasets) == 2:
                observed_paths["truth_crosswalk"] = paths["truth_crosswalk"]
            observed_schemas = get_observed_output_schemas(emission_cfg)

            for logical_name, schema in observed_schemas.items():
                path = observed_paths[logical_name]
                exists = path.exists()
                files[logical_name] = {
                    "path": str(path),
                    "file_format": schema.file_format,
                    "layer": schema.layer,
                    "exists": exists,
                }
                if not exists:
                    missing_files.append(logical_name)
                    continue

                try:
                    available_columns = _read_columns(path, schema.file_format)
                except Exception as exc:  # pragma: no cover
                    schema_errors[logical_name] = {
                        "path": str(path),
                        "read_error": str(exc),
                    }
                    continue

                missing_required = [
                    column for column in schema.required_columns if column not in available_columns
                ]
                missing_any_of = [
                    list(group)
                    for group in schema.required_any_of
                    if not any(col in available_columns for col in group)
                ]
                if missing_required or missing_any_of:
                    schema_errors[logical_name] = {
                        "path": str(path),
                        "missing_required_columns": missing_required,
                        "missing_any_of_groups": missing_any_of,
                        "available_columns": available_columns,
                    }

            observed_outputs = manifest_payload.get("observed_outputs", {}) if isinstance(manifest_payload, dict) else {}
            pairwise_crosswalks = observed_outputs.get("pairwise_crosswalks", []) if isinstance(observed_outputs, dict) else []
            if pairwise_crosswalks not in (None, []) and not isinstance(pairwise_crosswalks, list):
                metadata_errors.setdefault("manifest_json", []).append(
                    "manifest.json observed_outputs.pairwise_crosswalks must be a list when provided"
                )
            elif isinstance(pairwise_crosswalks, list):
                pairwise_schema = PHASE2_STATIC_OBSERVED_OUTPUTS["truth_crosswalk"]
                for item in pairwise_crosswalks:
                    if not isinstance(item, dict):
                        metadata_errors.setdefault("manifest_json", []).append(
                            "manifest.json observed_outputs.pairwise_crosswalks entries must be objects"
                        )
                        continue
                    dataset_ids = item.get("dataset_ids", [])
                    raw_path = str(item.get("path", "")).strip()
                    if not isinstance(dataset_ids, list) or len(dataset_ids) != 2 or not all(str(v).strip() for v in dataset_ids):
                        metadata_errors.setdefault("manifest_json", []).append(
                            "manifest.json pairwise_crosswalk entries must include exactly two dataset_ids"
                        )
                        continue
                    logical_name = f"pairwise_crosswalk__{str(dataset_ids[0]).strip()}__{str(dataset_ids[1]).strip()}"
                    path = Path(raw_path) if raw_path else Path()
                    exists = bool(raw_path) and path.exists()
                    files[logical_name] = {
                        "path": raw_path,
                        "file_format": pairwise_schema.file_format,
                        "layer": pairwise_schema.layer,
                        "exists": exists,
                    }
                    if not exists:
                        missing_files.append(logical_name)
                        continue
                    try:
                        available_columns = _read_columns(path, pairwise_schema.file_format)
                    except Exception as exc:  # pragma: no cover
                        schema_errors[logical_name] = {
                            "path": str(path),
                            "read_error": str(exc),
                        }
                        continue
                    missing_required = [
                        column for column in pairwise_schema.required_columns if column not in available_columns
                    ]
                    missing_any_of = [
                        list(group)
                        for group in pairwise_schema.required_any_of
                        if not any(col in available_columns for col in group)
                    ]
                    if missing_required or missing_any_of:
                        schema_errors[logical_name] = {
                            "path": str(path),
                            "missing_required_columns": missing_required,
                            "missing_any_of_groups": missing_any_of,
                            "available_columns": available_columns,
                        }

    constraints_validation: dict[str, Any] | None = None
    constraints_config_errors: list[str] = []
    scenario_constraints_raw = {}
    if scenario_payload and isinstance(scenario_payload, dict):
        raw = scenario_payload.get("constraints")
        if raw is None:
            scenario_constraints_raw = {}
        elif isinstance(raw, dict):
            scenario_constraints_raw = raw
        else:
            constraints_config_errors.append("scenario.yaml constraints must be a mapping when provided")

    if not constraints_config_errors:
        try:
            constraints_cfg = parse_constraints_config(scenario_constraints_raw)
        except Exception as exc:
            constraints_config_errors.append(str(exc))
            constraints_cfg = None

        if constraints_cfg is not None:
            required_truth_files = ("truth_people", "truth_events", "truth_residence_history")
            truth_files_ready = all(paths[name].exists() for name in required_truth_files)
            no_schema_errors_on_truth = all(name not in schema_errors for name in required_truth_files)
            if truth_files_ready and no_schema_errors_on_truth:
                constraints_validation = validate_constraints_for_run(
                    run_dir=run_dir,
                    config=constraints_cfg,
                )
                if not constraints_validation["valid"]:
                    metadata_errors["constraints"] = [
                        f"{constraints_validation['violation_count']} constraint violations detected"
                    ]

    if constraints_config_errors:
        metadata_errors["constraints_config"] = constraints_config_errors

    valid = not missing_files and not schema_errors and not metadata_errors
    return {
        "run_id": run_info["run_id"],
        "run_dir": str(run_dir),
        "scenario_id": run_info["scenario_id"],
        "seed": run_info["seed"],
        "valid": valid,
        "missing_files": missing_files,
        "schema_errors": schema_errors,
        "metadata_errors": metadata_errors,
        "files": files,
        "reproducible_from": [
            "phase1_csv",
            "phase1_manifest",
            "scenario_yaml",
            "seed",
        ],
        "quality_report_loaded": quality_payload is not None,
        "constraints_validation": constraints_validation,
    }
