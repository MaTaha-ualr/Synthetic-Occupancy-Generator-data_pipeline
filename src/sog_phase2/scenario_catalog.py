from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCENARIO_CATALOG_PATH = PROJECT_ROOT / "phase2" / "scenarios" / "catalog.yaml"


@lru_cache(maxsize=1)
def load_scenario_catalog(path: Path | None = None) -> dict[str, Any]:
    catalog_path = path or SCENARIO_CATALOG_PATH
    if not catalog_path.exists():
        raise FileNotFoundError(f"Scenario catalog not found: {catalog_path}")
    payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"Scenario catalog must be a YAML mapping: {catalog_path}")
    entries = payload.get("entries", [])
    if not isinstance(entries, list):
        raise ValueError("Scenario catalog key 'entries' must be a list")
    return payload


def get_scenario_catalog_entries() -> list[dict[str, Any]]:
    entries = load_scenario_catalog().get("entries", [])
    normalized: list[dict[str, Any]] = []
    for item in entries:
        if isinstance(item, dict) and str(item.get("scenario_id", "")).strip():
            normalized.append(item)
    return normalized


def get_scenario_catalog_by_id() -> dict[str, dict[str, Any]]:
    return {
        str(entry["scenario_id"]).strip(): entry
        for entry in get_scenario_catalog_entries()
    }


def get_scenario_catalog_summary() -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in get_scenario_catalog_entries():
        status = str(entry.get("status", "unknown")).strip() or "unknown"
        counts[status] = counts.get(status, 0) + 1
    return counts
