"""Format converters for SOG run artifacts.

Post-run, additive utility: reads the canonical CSV/parquet outputs that
``run_scenario_pipeline`` writes and re-serializes them into other tabular
formats (xlsx, tsv/txt, json, jsonl, parquet). Meta files (.json, .yaml) are
copied through unchanged.

The canonical run artifacts on disk are not modified. The output contract
validator continues to operate on the original CSV/parquet files.
"""

from __future__ import annotations

import io
import json
import shutil
import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd


TABULAR_FORMATS: tuple[str, ...] = (
    "csv",
    "tsv",
    "txt",
    "xlsx",
    "json",
    "jsonl",
    "parquet",
)

_TABULAR_SOURCE_SUFFIXES: frozenset[str] = frozenset({".csv", ".parquet"})

_FORMAT_EXTENSION: dict[str, str] = {
    "csv": ".csv",
    "tsv": ".tsv",
    "txt": ".txt",
    "xlsx": ".xlsx",
    "json": ".json",
    "jsonl": ".jsonl",
    "parquet": ".parquet",
}

_FORMAT_MIME: dict[str, str] = {
    "csv": "text/csv",
    "tsv": "text/tab-separated-values",
    "txt": "text/plain",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "json": "application/json",
    "jsonl": "application/x-ndjson",
    "parquet": "application/octet-stream",
}


def normalize_format(fmt: str) -> str:
    value = str(fmt or "").strip().lower().lstrip(".")
    if value not in TABULAR_FORMATS:
        raise ValueError(
            f"Unsupported format '{fmt}'. Choose one of: {', '.join(TABULAR_FORMATS)}"
        )
    return value


def extension_for(fmt: str) -> str:
    return _FORMAT_EXTENSION[normalize_format(fmt)]


def mime_for(fmt: str) -> str:
    return _FORMAT_MIME[normalize_format(fmt)]


def is_tabular_artifact(path: Path) -> bool:
    """True for files whose contents can be re-serialized into another format."""
    return Path(path).suffix.lower() in _TABULAR_SOURCE_SUFFIXES


def _read_tabular(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Cannot read non-tabular file as table: {path}")


def dataframe_to_bytes(df: pd.DataFrame, fmt: str, *, sheet_name: str = "data") -> bytes:
    """Serialize a DataFrame to bytes in the requested format."""
    fmt = normalize_format(fmt)
    if fmt == "csv":
        return df.to_csv(index=False).encode("utf-8")
    if fmt in {"tsv", "txt"}:
        return df.to_csv(index=False, sep="\t").encode("utf-8")
    if fmt == "json":
        return df.to_json(orient="records", indent=2, date_format="iso").encode("utf-8")
    if fmt == "jsonl":
        return df.to_json(orient="records", lines=True, date_format="iso").encode("utf-8")
    if fmt == "parquet":
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        return buffer.getvalue()
    if fmt == "xlsx":
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name=sheet_name[:31] or "data", index=False)
        return buffer.getvalue()
    raise ValueError(f"Unsupported format: {fmt}")


def convert_artifact_bytes(src_path: Path, fmt: str) -> bytes:
    """Read a tabular artifact from disk and return it serialized as ``fmt``."""
    src_path = Path(src_path)
    fmt = normalize_format(fmt)
    if not is_tabular_artifact(src_path):
        return src_path.read_bytes()
    if src_path.suffix.lower() == _FORMAT_EXTENSION[fmt]:
        return src_path.read_bytes()
    df = _read_tabular(src_path)
    return dataframe_to_bytes(df, fmt, sheet_name=src_path.stem)


def convert_artifact_to_path(
    src_path: Path,
    fmt: str,
    *,
    dest_dir: Path | None = None,
    overwrite: bool = True,
) -> Path:
    """Convert a single artifact and write the result next to it (or to ``dest_dir``)."""
    src_path = Path(src_path)
    fmt = normalize_format(fmt)
    target_dir = Path(dest_dir) if dest_dir is not None else src_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{src_path.stem}{_FORMAT_EXTENSION[fmt]}"
    if target_path.exists() and not overwrite:
        raise FileExistsError(f"Target already exists: {target_path}")
    if not is_tabular_artifact(src_path):
        if src_path.resolve() != target_path.resolve():
            shutil.copy2(src_path, target_path)
        return target_path
    target_path.write_bytes(convert_artifact_bytes(src_path, fmt))
    return target_path


def _iter_run_files(run_dir: Path) -> Iterable[Path]:
    for child in sorted(Path(run_dir).iterdir()):
        if child.is_file():
            yield child


def bundle_run_as_zip(
    run_dir: Path,
    fmt: str,
    *,
    include_meta: bool = True,
) -> bytes:
    """Convert all tabular artifacts in ``run_dir`` to ``fmt`` and zip the result.

    Non-tabular files (manifest.json, scenario.yaml, quality_report.json, etc.)
    are included verbatim when ``include_meta`` is true. Tabular files are
    re-serialized; their original filenames keep their stem and gain the new
    extension.
    """
    run_dir = Path(run_dir)
    if not run_dir.is_dir():
        raise FileNotFoundError(f"Run directory not found: {run_dir}")
    fmt = normalize_format(fmt)
    target_ext = _FORMAT_EXTENSION[fmt]

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for src in _iter_run_files(run_dir):
            if is_tabular_artifact(src):
                arcname = f"{src.stem}{target_ext}"
                archive.writestr(arcname, convert_artifact_bytes(src, fmt))
            elif include_meta:
                archive.write(src, arcname=src.name)
    return buffer.getvalue()


def list_run_artifacts(run_dir: Path) -> list[dict[str, str]]:
    """Return a manifest-style listing of files in a run directory.

    Each entry: ``{"name": ..., "path": ..., "tabular": "true"|"false"}``.
    Useful for CLI listings or frontend pickers.
    """
    run_dir = Path(run_dir)
    if not run_dir.is_dir():
        return []
    out: list[dict[str, str]] = []
    for child in _iter_run_files(run_dir):
        out.append(
            {
                "name": child.name,
                "path": str(child),
                "tabular": "true" if is_tabular_artifact(child) else "false",
            }
        )
    return out


__all__ = [
    "TABULAR_FORMATS",
    "bundle_run_as_zip",
    "convert_artifact_bytes",
    "convert_artifact_to_path",
    "dataframe_to_bytes",
    "extension_for",
    "is_tabular_artifact",
    "list_run_artifacts",
    "mime_for",
    "normalize_format",
]
