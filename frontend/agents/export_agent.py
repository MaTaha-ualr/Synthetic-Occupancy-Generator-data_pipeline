"""ExportAgent - format SOG observed outputs for downstream ER tools.

Converts one or more observed dataset CSVs, plus canonical mapping artifacts,
into formats ready for Splink, Zingg, or a generic zip bundle.
Never modifies source data.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from .base import AgentResponse, BaseAgent

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNS_ROOT = PROJECT_ROOT / "phase2" / "runs"
EXPORTS_ROOT = PROJECT_ROOT / "phase2" / ".sog_exports"

_SYSTEM_PROMPT = """\
You are ExportAgent, the output formatter for SOG.

YOUR JOB: Convert SOG run artifacts into formats usable by specific ER tools.
Never modify source data. Return file paths only. ONE sentence responses.

SUPPORTED FORMATS:
- splink: Renames columns to Splink conventions, writes parquet + settings template
- zingg: Writes CSV with Zingg field config JSON
- zip: Bundles all run artifacts into a single downloadable zip

RESPONSE STYLE:
- "Exported to Splink format: /path/to/export/"
- "Zip bundle ready: /path/to/bundle.zip (12.3 MB)"
- ONE sentence.
"""

_TOOLS = [
    {
        "name": "export_for_splink",
        "description": "Export run datasets in Splink-compatible format.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "output_dir": {"type": "string"},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "export_for_zingg",
        "description": "Export run datasets with Zingg field configuration.",
        "input_schema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "output_dir": {"type": "string"},
            },
            "required": ["run_id"],
        },
    },
    {
        "name": "package_run",
        "description": "Bundle all run artifacts into a zip file.",
        "input_schema": {
            "type": "object",
            "properties": {"run_id": {"type": "string"}},
            "required": ["run_id"],
        },
    },
    {
        "name": "list_recent_runs",
        "description": "List recent runs to find run_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer"},
                "scenario_id_filter": {"type": "string"},
            },
        },
    },
]


_SPLINK_ALIASES: dict[str, tuple[str, ...]] = {
    "unique_id": ("unique_id", "A_RecordKey", "B_RecordKey", "RecordKey"),
    "first_name": ("first_name", "FirstName", "FormalFirstName"),
    "middle_name": ("middle_name", "MiddleName"),
    "last_name": ("last_name", "LastName"),
    "suffix": ("suffix", "Suffix"),
    "full_name": ("full_name", "FullName", "FormalFullName"),
    "dob": ("dob", "DOB"),
    "age": ("age", "Age"),
    "ssn": ("ssn", "SSN"),
    "phone": ("phone", "Phone"),
    "address_key": ("address_key", "AddressKey"),
    "street_address": ("street_address", "StreetAddress"),
    "city": ("city", "City"),
    "state": ("state", "State"),
    "zip_code": ("zip_code", "ZipCode"),
    "gender": ("gender", "Gender"),
    "ethnicity": ("ethnicity", "Ethnicity"),
    "source_snapshot_date": ("source_snapshot_date", "SourceSnapshotDate"),
    "source_system": ("source_system", "SourceSystem"),
}

_ZINGG_ALIASES: dict[str, tuple[str, ...]] = {
    "RecordKey": ("RecordKey", "A_RecordKey", "B_RecordKey", "unique_id"),
    "FirstName": ("FirstName", "FormalFirstName", "first_name"),
    "MiddleName": ("MiddleName", "middle_name"),
    "LastName": ("LastName", "last_name"),
    "Suffix": ("Suffix", "suffix"),
    "FullName": ("FullName", "FormalFullName", "full_name"),
    "DOB": ("DOB", "dob"),
    "Age": ("Age", "age"),
    "SSN": ("SSN", "ssn"),
    "Phone": ("Phone", "phone"),
    "AddressKey": ("AddressKey", "address_key"),
    "StreetAddress": ("StreetAddress", "street_address"),
    "City": ("City", "city"),
    "State": ("State", "state"),
    "ZipCode": ("ZipCode", "zip_code"),
    "Gender": ("Gender", "gender"),
    "Ethnicity": ("Ethnicity", "ethnicity"),
    "SourceSnapshotDate": ("SourceSnapshotDate", "source_snapshot_date"),
    "SourceSystem": ("SourceSystem", "source_system"),
}


def _discover_observed_dataset_files(run_dir: Path) -> list[tuple[str, Path]]:
    manifest_path = run_dir / "manifest.json"
    manifest: dict[str, Any] = {}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {}
    observed_outputs = manifest.get("observed_outputs", {}) if isinstance(manifest, dict) else {}
    datasets = observed_outputs.get("datasets", []) if isinstance(observed_outputs, dict) else []
    discovered: list[tuple[str, Path]] = []
    if isinstance(datasets, list):
        for item in datasets:
            if not isinstance(item, dict):
                continue
            dataset_id = str(item.get("dataset_id", "")).strip()
            raw_path = str(item.get("path", "")).strip()
            if not dataset_id or not raw_path:
                continue
            path = Path(raw_path)
            if path.exists():
                discovered.append((dataset_id, path))
    if discovered:
        return discovered

    fallback = []
    for label, filename in [("DatasetA", "DatasetA.csv"), ("DatasetB", "DatasetB.csv")]:
        path = run_dir / filename
        if path.exists():
            fallback.append((label, path))
    return fallback


def _discover_pairwise_crosswalk_files(run_dir: Path) -> list[tuple[list[str], Path]]:
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        return []
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    observed_outputs = manifest.get("observed_outputs", {}) if isinstance(manifest, dict) else {}
    items = observed_outputs.get("pairwise_crosswalks", []) if isinstance(observed_outputs, dict) else []
    discovered: list[tuple[list[str], Path]] = []
    if isinstance(items, list):
        for item in items:
            if not isinstance(item, dict):
                continue
            dataset_ids = item.get("dataset_ids", [])
            raw_path = str(item.get("path", "")).strip()
            if not isinstance(dataset_ids, list) or len(dataset_ids) != 2 or not raw_path:
                continue
            path = Path(raw_path)
            if path.exists():
                discovered.append(([str(dataset_ids[0]), str(dataset_ids[1])], path))
    return discovered


def _rename_first_present(df, aliases: dict[str, tuple[str, ...]]):
    rename_map: dict[str, str] = {}
    for target, candidates in aliases.items():
        for candidate in candidates:
            if candidate in df.columns:
                rename_map[candidate] = target
                break
    return df.rename(columns=rename_map)


def _normalize_for_splink(df, dataset_label: str):
    normalized = _rename_first_present(df.copy(), _SPLINK_ALIASES)
    if "unique_id" not in normalized.columns:
        raise ValueError("No record key column found for Splink export")
    normalized["unique_id"] = normalized["unique_id"].astype(str).str.strip()
    normalized["source_dataset"] = dataset_label
    preferred = [
        "unique_id",
        "first_name",
        "middle_name",
        "last_name",
        "suffix",
        "full_name",
        "dob",
        "age",
        "ssn",
        "phone",
        "address_key",
        "street_address",
        "city",
        "state",
        "zip_code",
        "gender",
        "ethnicity",
        "source_snapshot_date",
        "source_system",
        "source_dataset",
    ]
    ordered = [col for col in preferred if col in normalized.columns]
    extras = [col for col in normalized.columns if col not in ordered]
    return normalized[ordered + extras]


def _normalize_for_zingg(df, dataset_label: str):
    normalized = _rename_first_present(df.copy(), _ZINGG_ALIASES)
    if "RecordKey" not in normalized.columns:
        raise ValueError("No record key column found for Zingg export")
    normalized["RecordKey"] = normalized["RecordKey"].astype(str).str.strip()
    normalized["SourceDataset"] = dataset_label
    preferred = [
        "RecordKey",
        "FirstName",
        "MiddleName",
        "LastName",
        "Suffix",
        "FullName",
        "DOB",
        "Age",
        "SSN",
        "Phone",
        "AddressKey",
        "StreetAddress",
        "City",
        "State",
        "ZipCode",
        "Gender",
        "Ethnicity",
        "SourceSnapshotDate",
        "SourceSystem",
        "SourceDataset",
    ]
    ordered = [col for col in preferred if col in normalized.columns]
    extras = [col for col in normalized.columns if col not in ordered]
    return normalized[ordered + extras]


def export_for_splink(run_id: str, output_dir: str | None = None) -> dict[str, Any]:
    """Export observed datasets with Splink-compatible column names."""
    run_dir = RUNS_ROOT / run_id
    if not run_dir.exists():
        return {"error": f"Run not found: {run_id}"}

    out_dir = Path(output_dir) if output_dir else EXPORTS_ROOT / run_id / "splink"
    out_dir.mkdir(parents=True, exist_ok=True)

    files_written = []
    try:
        import pandas as pd

        for label, src in _discover_observed_dataset_files(run_dir):
            df = _normalize_for_splink(pd.read_csv(src, dtype=str), label)
            safe_label = label.replace("/", "_")
            out_path = out_dir / f"{safe_label}_splink.parquet"
            df.to_parquet(out_path, index=False)
            files_written.append(str(out_path))

        xwalk = run_dir / "truth_crosswalk.csv"
        if xwalk.exists():
            dst = out_dir / "truth_crosswalk.csv"
            shutil.copy2(xwalk, dst)
            files_written.append(str(dst))
        else:
            for dataset_ids, src in _discover_pairwise_crosswalk_files(run_dir):
                dst = out_dir / src.name
                shutil.copy2(src, dst)
                files_written.append(str(dst))

        settings = {
            "link_type": "link_only",
            "unique_id_column_name": "unique_id",
            "blocking_rules_to_generate_predictions": [
                "l.last_name = r.last_name",
                "l.dob = r.dob",
            ],
            "comparisons": [
                {"output_column_name": "first_name"},
                {"output_column_name": "last_name"},
                {"output_column_name": "dob"},
            ],
        }
        settings_path = out_dir / "splink_settings_template.json"
        settings_path.write_text(json.dumps(settings, indent=2), encoding="utf-8")
        files_written.append(str(settings_path))

        return {
            "success": True,
            "output_dir": str(out_dir),
            "files": files_written,
            "note": "Exports use a normalized schema with unique_id and source_dataset columns.",
        }
    except ImportError:
        return {"error": "pandas not available"}
    except Exception as exc:
        return {"error": str(exc)}


def export_for_zingg(run_id: str, output_dir: str | None = None) -> dict[str, Any]:
    """Export datasets with Zingg field configuration JSON."""
    run_dir = RUNS_ROOT / run_id
    if not run_dir.exists():
        return {"error": f"Run not found: {run_id}"}

    out_dir = Path(output_dir) if output_dir else EXPORTS_ROOT / run_id / "zingg"
    out_dir.mkdir(parents=True, exist_ok=True)

    files_written = []
    try:
        import pandas as pd

        for label, src in _discover_observed_dataset_files(run_dir):
            df = _normalize_for_zingg(pd.read_csv(src, dtype=str), label)
            dst = out_dir / src.name
            df.to_csv(dst, index=False)
            files_written.append(str(dst))

        xwalk = run_dir / "truth_crosswalk.csv"
        if xwalk.exists():
            dst = out_dir / "truth_crosswalk.csv"
            shutil.copy2(xwalk, dst)
            files_written.append(str(dst))
        else:
            for dataset_ids, src in _discover_pairwise_crosswalk_files(run_dir):
                dst = out_dir / src.name
                shutil.copy2(src, dst)
                files_written.append(str(dst))

        zingg_config = {
            "fieldDefinition": [
                {"fieldName": "RecordKey", "matchType": "DONT_USE"},
                {"fieldName": "FirstName", "matchType": "FUZZY"},
                {"fieldName": "MiddleName", "matchType": "FUZZY"},
                {"fieldName": "LastName", "matchType": "FUZZY"},
                {"fieldName": "Suffix", "matchType": "FUZZY"},
                {"fieldName": "DOB", "matchType": "EXACT"},
                {"fieldName": "SSN", "matchType": "EXACT"},
                {"fieldName": "Phone", "matchType": "EXACT"},
                {"fieldName": "AddressKey", "matchType": "EXACT"},
                {"fieldName": "Gender", "matchType": "EXACT"},
                {"fieldName": "Ethnicity", "matchType": "EXACT"},
                {"fieldName": "SourceDataset", "matchType": "DONT_USE"},
            ],
            "labelDataSampleSize": 1000,
            "trainingSamples": 10000,
        }
        config_path = out_dir / "zingg_field_config.json"
        config_path.write_text(json.dumps(zingg_config, indent=2), encoding="utf-8")
        files_written.append(str(config_path))

        return {
            "success": True,
            "output_dir": str(out_dir),
            "files": files_written,
        }
    except Exception as exc:
        return {"error": str(exc)}


def package_run(run_id: str) -> dict[str, Any]:
    """Zip all run artifacts into a single bundle."""
    run_dir = RUNS_ROOT / run_id
    if not run_dir.exists():
        return {"error": f"Run not found: {run_id}"}

    EXPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    zip_base = str(EXPORTS_ROOT / run_id)
    try:
        zip_path = shutil.make_archive(zip_base, "zip", run_dir)
        size_mb = Path(zip_path).stat().st_size / (1024 * 1024)
        return {
            "success": True,
            "zip_path": zip_path,
            "size_mb": round(size_mb, 2),
        }
    except Exception as exc:
        return {"error": str(exc)}


class ExportAgent(BaseAgent):
    """Packages SOG run outputs for ER tools."""

    def __init__(self, api_key: str | None = None, provider: str | None = None):
        super().__init__(api_key=api_key, provider=provider)

    def get_system_prompt(self) -> str:
        return _SYSTEM_PROMPT

    def get_tools(self) -> list[dict[str, Any]]:
        return _TOOLS

    def dispatch_tool(self, name: str, inputs: dict[str, Any], session_id: str) -> dict[str, Any]:
        import sog_tools as t

        try:
            if name == "export_for_splink":
                return export_for_splink(**inputs)
            if name == "export_for_zingg":
                return export_for_zingg(**inputs)
            if name == "package_run":
                return package_run(**inputs)
            if name == "list_recent_runs":
                return t.list_recent_runs(**inputs)
            return {"error": f"Unknown tool: {name}"}
        except Exception as exc:
            return {"error": str(exc)}

    def run(
        self,
        user_input: str,
        session_id: str,
        context: dict[str, Any],
    ) -> AgentResponse:
        """Process an export request."""
        messages: list[dict[str, Any]] = []

        if context.get("last_run_id"):
            messages.append(
                {
                    "role": "user",
                    "content": f"[Context] Last run: {context['last_run_id']}",
                }
            )
            messages.append({"role": "assistant", "content": "Understood."})

        messages.append({"role": "user", "content": user_input})

        try:
            text, data = self.run_tool_loop(messages, session_id)
            updates: dict[str, Any] = {}
            if data.get("zip_path"):
                updates["last_export_path"] = data["zip_path"]
            elif data.get("output_dir"):
                updates["last_export_dir"] = data["output_dir"]

            return AgentResponse(
                success=True,
                message=text,
                data=data,
                session_updates=updates,
            )
        except Exception as exc:
            return AgentResponse(
                success=False,
                message=f"Export failed: {exc}",
            )
