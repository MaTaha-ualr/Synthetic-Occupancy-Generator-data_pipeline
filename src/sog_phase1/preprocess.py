from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


LAST_NAME_ETHNICITY_ORDER = [
    "American Indian & Alaskan Native",
    "Asian & Pacific Islander",
    "Black",
    "Hispanic",
    "Two or More Races",
    "White",
]

ETHNICITY_LABEL_TO_LAST_NAME = {
    "White (Non-Hispanic)": "White",
    "Hispanic / Latino": "Hispanic",
    "Black / African American": "Black",
    "Asian": "Asian & Pacific Islander",
    "Multiracial / Other": "Two or More Races",
}


def _read_csv_lenient(path: Path, *, expected_columns: int | None = None) -> tuple[list[str], list[dict[str, str]]]:
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header_raw = next(reader, None)
        if not header_raw:
            return [], []

        header = [str(col).strip() for col in header_raw]
        expected = expected_columns if expected_columns is not None else len(header)
        header = header[:expected]

        for raw in reader:
            if not raw or not any(str(cell).strip() for cell in raw):
                continue
            values = [str(cell).strip() for cell in raw]
            if len(values) < expected:
                values.extend([""] * (expected - len(values)))
            elif len(values) > expected:
                values = values[:expected]
            rows.append(dict(zip(header, values)))
    return header, rows


def _clean_name(value: str) -> str:
    return value.strip().title()


def _build_first_names(raw_root: Path, prepared_dir: Path) -> dict[str, Any]:
    specs = [
        ("Names/FirstName_female.csv", "name_Female", "number_Female", "female"),
        ("Names/FirstName_male.csv", "name_Male", "number_Male", "male"),
        ("Names/FirstName_unisex.csv", "name", "number", "unisex"),
    ]
    records: list[dict[str, Any]] = []
    for rel_path, name_col, weight_col, sex in specs:
        path = raw_root / rel_path
        _, rows = _read_csv_lenient(path, expected_columns=2)
        for row in rows:
            name = _clean_name(row.get(name_col, ""))
            if not name:
                continue
            weight_text = row.get(weight_col, "").strip()
            if not weight_text:
                continue
            weight = float(weight_text)
            if weight <= 0:
                continue
            records.append({"name": name, "sex": sex, "weight": weight})

    df = pd.DataFrame.from_records(records, columns=["name", "sex", "weight"])
    df = df.groupby(["name", "sex"], as_index=False)["weight"].sum()
    out_path = prepared_dir / "first_names.parquet"
    df.to_parquet(out_path, index=False)
    return {"path": str(out_path), "rows": int(len(df))}


def _build_last_names(raw_root: Path, prepared_dir: Path) -> dict[str, Any]:
    path = raw_root / "Names/LastName.csv"
    _, rows = _read_csv_lenient(path, expected_columns=8)
    records: list[dict[str, Any]] = []
    for row in rows:
        last_name = row.get("last_name", "").strip().upper()
        ethnicity = row.get("ethnicity", "").strip()
        if not last_name or not ethnicity:
            continue
        weight_text = row.get("ethnicity_occurrences", "").strip()
        if not weight_text:
            continue
        weight = float(weight_text)
        if weight <= 0:
            continue
        records.append({"last_name": last_name, "ethnicity": ethnicity, "weight": weight})

    df = pd.DataFrame.from_records(records, columns=["last_name", "ethnicity", "weight"])
    df = df.groupby(["last_name", "ethnicity"], as_index=False)["weight"].sum()
    out_path = prepared_dir / "last_names.parquet"
    df.to_parquet(out_path, index=False)
    return {
        "path": str(out_path),
        "rows": int(len(df)),
        "weight_column": "ethnicity_occurrences",
    }


def _build_simple_dimension(
    raw_path: Path,
    prepared_dir: Path,
    *,
    source_column: str,
    target_file: str,
    target_column: str,
    transform: str = "strip",
) -> dict[str, Any]:
    _, rows = _read_csv_lenient(raw_path, expected_columns=1)
    values: list[str] = []
    for row in rows:
        val = row.get(source_column, "").strip()
        if transform == "upper":
            val = val.upper()
        elif transform == "title":
            val = val.title()
        if val:
            values.append(val)

    # Keep first occurrence order and remove duplicates.
    values = list(dict.fromkeys(values))
    df = pd.DataFrame({target_column: values})
    out_path = prepared_dir / target_file
    df.to_parquet(out_path, index=False)
    return {"path": str(out_path), "rows": int(len(df))}


def _parse_numeric_percent(text: str) -> float | None:
    cleaned = text.strip()
    match = re.search(r"(\d+(?:\.\d+)?)", cleaned)
    if not match:
        return None
    return float(match.group(1))


def _parse_age_range(label: str) -> tuple[int, int] | None:
    m = re.search(r"(\d+)\s*-\s*(\d+)", label)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r"(\d+)\s*\+", label)
    if m:
        return int(m.group(1)), 95
    return None


def _build_demographics(raw_root: Path, prepared_dir: Path) -> dict[str, Any]:
    path = raw_root / "Data/demographics_extracted/demographics_records_long.csv"
    _, rows = _read_csv_lenient(path, expected_columns=5)

    parsed_records: list[dict[str, Any]] = []
    for row in rows:
        metric = row.get("metric", "").strip()
        value = row.get("value", "").strip()
        numeric_value = None
        if metric in {"percent", "within_lgbtq_percent"}:
            numeric_value = _parse_numeric_percent(value)
        parsed_records.append(
            {
                "section": row.get("section", "").strip(),
                "category": row.get("category", "").strip(),
                "label": row.get("label", "").strip(),
                "metric": metric,
                "value_raw": value,
                "numeric_value": numeric_value,
            }
        )

    gender_distribution: dict[str, float] = {}
    age_bins: list[dict[str, Any]] = []
    ethnicity_distribution_raw: dict[str, float] = {}

    for item in parsed_records:
        section = item["section"]
        metric = item["metric"]
        numeric_value = item["numeric_value"]
        label = item["label"]

        if numeric_value is None:
            continue

        if section == "GENDER BREAKDOWN" and metric == "percent":
            gender_distribution[label] = numeric_value

        if section == "AGE GROUPS" and metric == "percent":
            age_range = _parse_age_range(label)
            if age_range is None:
                continue
            min_age, max_age = age_range
            age_bins.append(
                {
                    "label": label,
                    "min_age": min_age,
                    "max_age": max_age,
                    "pct": numeric_value,
                }
            )

        if section == "ETHNICITY / RACE BREAKDOWN" and metric == "percent":
            ethnicity_distribution_raw[label] = numeric_value

    ethnicity_distribution_for_last_name: dict[str, float] = {k: 0.0 for k in LAST_NAME_ETHNICITY_ORDER}
    for label, value in ethnicity_distribution_raw.items():
        target = ETHNICITY_LABEL_TO_LAST_NAME.get(label)
        if target is None:
            continue
        ethnicity_distribution_for_last_name[target] = ethnicity_distribution_for_last_name.get(target, 0.0) + value

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": "Data/demographics_extracted/demographics_records_long.csv",
        "gender_distribution_pct": gender_distribution,
        "age_bins_pct": age_bins,
        "ethnicity_distribution_pct_raw_labels": ethnicity_distribution_raw,
        "ethnicity_distribution_pct_for_last_name": ethnicity_distribution_for_last_name,
        "parsed_records": parsed_records,
    }

    out_path = prepared_dir / "demographics.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {"path": str(out_path), "records": len(parsed_records)}


def _build_nicknames(nicknames_source_dir: Path, prepared_dir: Path) -> dict[str, Any]:
    source_path = nicknames_source_dir / "all_nick_names.json"
    payload: dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": str(source_path),
        "categories": {},
        "summary": {
            "categories_present": [],
            "formal_name_count": 0,
            "nickname_name_count": 0,
        },
    }

    if not source_path.exists():
        out_path = prepared_dir / "nicknames.json"
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return {
            "path": str(out_path),
            "source_exists": False,
            "categories": 0,
            "formal_names": 0,
            "nickname_names": 0,
        }

    raw = json.loads(source_path.read_text(encoding="utf-8"))
    datasets = raw.get("datasets", {})
    categories: dict[str, dict[str, Any]] = {}
    total_formal = 0
    total_nicknames = 0

    for category in ("female", "male", "unisex"):
        bucket: dict[str, Any] = {}
        groups = datasets.get(category, {}).get("groups", [])
        for group in groups:
            formal = _clean_name(str(group.get("formal_name", "")))
            if not formal:
                continue
            nickname_rows = group.get("nicknames", [])
            agg: dict[str, float] = {}
            for row in nickname_rows:
                nick = _clean_name(str(row.get("name", "")))
                if not nick or nick == formal:
                    continue
                weight = float(row.get("count", 0.0))
                if weight <= 0:
                    continue
                agg[nick] = agg.get(nick, 0.0) + weight
            if not agg:
                continue
            items = sorted(agg.items(), key=lambda kv: kv[1], reverse=True)
            bucket[formal] = {
                "nicknames": [name for name, _ in items],
                "weights": [weight for _, weight in items],
            }
            total_formal += 1
            total_nicknames += len(items)
        categories[category] = bucket

    payload["categories"] = categories
    payload["summary"] = {
        "categories_present": [k for k, v in categories.items() if v],
        "formal_name_count": total_formal,
        "nickname_name_count": total_nicknames,
    }

    out_path = prepared_dir / "nicknames.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {
        "path": str(out_path),
        "source_exists": True,
        "categories": len(payload["summary"]["categories_present"]),
        "formal_names": total_formal,
        "nickname_names": total_nicknames,
    }


def build_prepared_cache(
    raw_root: Path,
    prepared_dir: Path,
    *,
    nicknames_source_dir: Path | None = None,
) -> dict[str, Any]:
    prepared_dir.mkdir(parents=True, exist_ok=True)
    nick_src = nicknames_source_dir or (raw_root / "Names" / "nick names")

    outputs = {
        "first_names": _build_first_names(raw_root, prepared_dir),
        "last_names": _build_last_names(raw_root, prepared_dir),
        "streets": _build_simple_dimension(
            raw_root / "Addresses/StreetNames.csv",
            prepared_dir,
            source_column="StreetName",
            target_file="streets.parquet",
            target_column="street_base_name",
            transform="upper",
        ),
        "cities": _build_simple_dimension(
            raw_root / "Addresses/Cities.csv",
            prepared_dir,
            source_column="City",
            target_file="cities.parquet",
            target_column="city_name",
            transform="title",
        ),
        "states": _build_simple_dimension(
            raw_root / "Addresses/States.csv",
            prepared_dir,
            source_column="State",
            target_file="states.parquet",
            target_column="state_name",
            transform="title",
        ),
        "demographics": _build_demographics(raw_root, prepared_dir),
        "nicknames": _build_nicknames(nick_src, prepared_dir),
    }

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "raw_root": str(raw_root),
        "prepared_dir": str(prepared_dir),
        "nicknames_source_dir": str(nick_src),
        "outputs": outputs,
    }
    manifest_path = prepared_dir / "prepared_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    manifest["manifest_path"] = str(manifest_path)
    return manifest
