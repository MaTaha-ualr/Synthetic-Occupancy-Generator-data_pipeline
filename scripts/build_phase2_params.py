from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _fetch_census_row(*, year: int, table_vars: list[str]) -> dict[str, int]:
    response = requests.get(
        f"https://api.census.gov/data/{year}/acs/acs1",
        params={"get": ",".join(table_vars), "for": "us:1"},
        timeout=30,
    )
    response.raise_for_status()
    header, values = response.json()
    return {key: int(value) for key, value in zip(header, values)}


def build_phase2_params(output_dir: Path) -> dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)
    now = _now_utc_iso()
    acs_year = 2024

    mobility_vars = [
        "B07001_001E",
        "B07001_017E",
        "B07001_033E",
        "B07001_049E",
        "B07001_065E",
        "B07001_081E",
    ]
    for idx in range(2, 17):
        mobility_vars.append(f"B07001_{idx:03d}E")
    for idx in range(18, 33):
        mobility_vars.append(f"B07001_{idx:03d}E")

    mobility = _fetch_census_row(year=acs_year, table_vars=mobility_vars)
    mob_total = mobility["B07001_001E"]
    mob_same_house = mobility["B07001_017E"]
    mob_moved = mob_total - mob_same_house

    mobility_overall = [
        {
            "geography": "United States",
            "geography_code": "us:1",
            "year": acs_year,
            "metric_id": "moved_past_year_pct",
            "metric_label": "Moved in past year (any move)",
            "numerator": mob_moved,
            "denominator": mob_total,
            "value_pct": round((mob_moved / mob_total) * 100.0, 6),
            "table_id": "B07001",
            "source_variable": "B07001_001E and B07001_017E",
            "source_id": "census_acs1_2024_b07001_api",
        },
        {
            "geography": "United States",
            "geography_code": "us:1",
            "year": acs_year,
            "metric_id": "moved_within_same_county_pct",
            "metric_label": "Moved within same county in past year",
            "numerator": mobility["B07001_033E"],
            "denominator": mob_total,
            "value_pct": round((mobility["B07001_033E"] / mob_total) * 100.0, 6),
            "table_id": "B07001",
            "source_variable": "B07001_033E",
            "source_id": "census_acs1_2024_b07001_api",
        },
        {
            "geography": "United States",
            "geography_code": "us:1",
            "year": acs_year,
            "metric_id": "moved_from_different_county_same_state_pct",
            "metric_label": "Moved from different county within same state in past year",
            "numerator": mobility["B07001_049E"],
            "denominator": mob_total,
            "value_pct": round((mobility["B07001_049E"] / mob_total) * 100.0, 6),
            "table_id": "B07001",
            "source_variable": "B07001_049E",
            "source_id": "census_acs1_2024_b07001_api",
        },
        {
            "geography": "United States",
            "geography_code": "us:1",
            "year": acs_year,
            "metric_id": "moved_from_different_state_pct",
            "metric_label": "Moved from different state in past year",
            "numerator": mobility["B07001_065E"],
            "denominator": mob_total,
            "value_pct": round((mobility["B07001_065E"] / mob_total) * 100.0, 6),
            "table_id": "B07001",
            "source_variable": "B07001_065E",
            "source_id": "census_acs1_2024_b07001_api",
        },
        {
            "geography": "United States",
            "geography_code": "us:1",
            "year": acs_year,
            "metric_id": "moved_from_abroad_pct",
            "metric_label": "Moved from abroad in past year",
            "numerator": mobility["B07001_081E"],
            "denominator": mob_total,
            "value_pct": round((mobility["B07001_081E"] / mob_total) * 100.0, 6),
            "table_id": "B07001",
            "source_variable": "B07001_081E",
            "source_id": "census_acs1_2024_b07001_api",
        },
    ]
    pd.DataFrame(mobility_overall).to_csv(output_dir / "mobility_overall_acs_2024.csv", index=False)

    cohort_defs = [
        ("age_0_17", "Ages 0-17 (ACS proxy from 1-4 + 5-17)", [2, 3]),
        ("age_18_24", "Ages 18-24", [4, 5]),
        ("age_25_34", "Ages 25-34", [6, 7]),
        ("age_35_64", "Ages 35-64", [8, 9, 10, 11, 12, 13]),
        ("age_65_plus", "Ages 65+", [14, 15, 16]),
    ]
    mobility_by_age = []
    for cohort_id, cohort_label, ids in cohort_defs:
        total = sum(mobility[f"B07001_{idx:03d}E"] for idx in ids)
        same_house = sum(mobility[f"B07001_{idx + 16:03d}E"] for idx in ids)
        moved = total - same_house
        mobility_by_age.append(
            {
                "geography": "United States",
                "geography_code": "us:1",
                "year": acs_year,
                "age_cohort_id": cohort_id,
                "age_cohort_label": cohort_label,
                "population": total,
                "moved_population": moved,
                "same_house_population": same_house,
                "moved_past_year_pct": round((moved / total) * 100.0, 6),
                "table_id": "B07001",
                "source_id": "census_acs1_2024_b07001_api",
            }
        )
    pd.DataFrame(mobility_by_age).to_csv(output_dir / "mobility_by_age_cohort_acs_2024.csv", index=False)

    household = _fetch_census_row(
        year=acs_year,
        table_vars=[
            "B11001_001E",
            "B11001_003E",
            "B11001_005E",
            "B11001_006E",
            "B11001_007E",
            "B11001_008E",
            "B11001_009E",
        ],
    )
    hh_total = household["B11001_001E"]
    household_rows = [
        (
            "married_couple_family",
            "Married-couple family household",
            "B11001_003E",
        ),
        (
            "single_parent_male_householder",
            "Single-parent family household (male householder, no spouse present)",
            "B11001_005E",
        ),
        (
            "single_parent_female_householder",
            "Single-parent family household (female householder, no spouse present)",
            "B11001_006E",
        ),
        (
            "nonfamily_household",
            "Nonfamily household",
            "B11001_007E",
        ),
        (
            "nonfamily_living_alone",
            "Nonfamily household with householder living alone",
            "B11001_008E",
        ),
        (
            "nonfamily_not_alone",
            "Nonfamily household with householder not living alone",
            "B11001_009E",
        ),
    ]
    household_output = []
    for hh_id, hh_label, var in household_rows:
        count = household[var]
        household_output.append(
            {
                "geography": "United States",
                "geography_code": "us:1",
                "year": acs_year,
                "household_type_id": hh_id,
                "household_type_label": hh_label,
                "household_count": count,
                "share_of_all_households_pct": round((count / hh_total) * 100.0, 6),
                "table_id": "B11001",
                "source_variable": var,
                "source_id": "census_acs1_2024_b11001_api",
            }
        )
    pd.DataFrame(household_output).to_csv(output_dir / "household_type_shares_acs_2024.csv", index=False)

    marriage_divorce = [
        {
            "geography": "United States",
            "year": 2023,
            "status": "provisional",
            "metric_id": "marriage_rate_per_1000",
            "metric_label": "Marriage rate per 1,000 total population",
            "value": 6.1,
            "count": 2041926,
            "unit": "rate_per_1000_population",
            "coverage_note": "All states report marriage counts to NVSS.",
            "source_id": "cdc_faststats_marriage_divorce_2023",
        },
        {
            "geography": "United States",
            "year": 2023,
            "status": "provisional",
            "metric_id": "divorce_rate_per_1000",
            "metric_label": "Divorce rate per 1,000 population",
            "value": 2.4,
            "count": 672502,
            "unit": "rate_per_1000_population",
            "coverage_note": (
                "Divorce data come from reporting areas only; several states do not report "
                "divorce data to NVSS each year."
            ),
            "source_id": "cdc_faststats_marriage_divorce_2023",
        },
    ]
    pd.DataFrame(marriage_divorce).to_csv(output_dir / "marriage_divorce_rates_cdc_2023.csv", index=False)

    fertility_rows = [
        ("10-14", 1725, 0.2, ""),
        ("15-19", 137020, 12.7, ""),
        ("15-17", 34405, 5.3, ""),
        ("18-19", 102615, 23.9, ""),
        ("20-24", 610548, 56.7, ""),
        ("25-29", 989140, 91.4, ""),
        ("30-34", 1110643, 95.4, ""),
        ("35-39", 621464, 55.0, ""),
        ("40-44", 141204, 12.8, ""),
        ("45-54", 10929, 1.1, "Rate is computed against the 45-49 female population in source table."),
    ]
    fertility = [
        {
            "age_group": age_group,
            "birth_count": count,
            "birth_rate_per_1000_women": rate,
            "geography": "United States",
            "year": 2024,
            "status": "provisional",
            "unit": "births_per_1000_women_in_age_group",
            "source_id": "nchs_vsrr38_births_provisional_2024",
            "note": note,
        }
        for age_group, count, rate, note in fertility_rows
    ]
    pd.DataFrame(fertility).to_csv(output_dir / "fertility_by_age_nchs_2024.csv", index=False)

    priors = {
        "generated_at_utc": now,
        "geography": "United States",
        "params_version": "phase2_params_v1",
        "mobility": {
            "year": 2024,
            "overall_moved_past_year_pct": round((mob_moved / mob_total) * 100.0, 6),
            "age_cohort_moved_pct": {
                row["age_cohort_id"]: row["moved_past_year_pct"] for row in mobility_by_age
            },
        },
        "marriage_divorce": {
            "year": 2023,
            "marriage_rate_per_1000": 6.1,
            "divorce_rate_per_1000": 2.4,
        },
        "fertility": {
            "year": 2024,
            "birth_rate_per_1000_by_age_group": {
                row["age_group"]: row["birth_rate_per_1000_women"] for row in fertility
            },
        },
        "household_type_share": {
            "year": 2024,
            "share_pct_by_type": {
                row["household_type_id"]: row["share_of_all_households_pct"]
                for row in household_output
                if row["household_type_id"]
                in {
                    "married_couple_family",
                    "single_parent_male_householder",
                    "single_parent_female_householder",
                    "nonfamily_household",
                }
            },
        },
    }
    (output_dir / "phase2_priors_snapshot.json").write_text(
        json.dumps(priors, indent=2),
        encoding="utf-8",
    )

    sources = {
        "generated_at_utc": now,
        "sources": [
            {
                "source_id": "census_acs1_2024_b07001_api",
                "title": "ACS 1-Year 2024 Table B07001: Geographical Mobility in the Past Year by Age",
                "publisher": "U.S. Census Bureau",
                "url": "https://api.census.gov/data/2024/acs/acs1?get=B07001_001E,B07001_017E,B07001_033E,B07001_049E,B07001_065E,B07001_081E&for=us:1",
                "table_metadata_url": "https://api.census.gov/data/2024/acs/acs1/groups/B07001.json",
                "data_year": 2024,
                "accessed_utc": now,
                "notes": "Used to compute moved-in-past-year rates overall and by age cohort.",
            },
            {
                "source_id": "census_acs1_2024_b11001_api",
                "title": "ACS 1-Year 2024 Table B11001: Household Type (Including Living Alone)",
                "publisher": "U.S. Census Bureau",
                "url": "https://api.census.gov/data/2024/acs/acs1?get=B11001_001E,B11001_003E,B11001_005E,B11001_006E,B11001_007E,B11001_008E,B11001_009E&for=us:1",
                "table_metadata_url": "https://api.census.gov/data/2024/acs/acs1/groups/B11001.json",
                "data_year": 2024,
                "accessed_utc": now,
                "notes": "Used to derive household-type share priors.",
            },
            {
                "source_id": "census_acs_mobility_release_2024",
                "title": "United States Migration/Geographic Mobility At A Glance: ACS 1-Year Estimates",
                "publisher": "U.S. Census Bureau",
                "url": "https://www.census.gov/topics/population/migration/guidance/acs-1yr.html",
                "accessed_utc": now,
                "notes": "Contextual ACS mobility brief; API table values are used for numeric inputs.",
            },
            {
                "source_id": "cdc_faststats_marriage_divorce_2023",
                "title": "FastStats: Marriage and Divorce",
                "publisher": "CDC / NCHS",
                "url": "https://www.cdc.gov/nchs/fastats/marriage-divorce.htm",
                "data_year": 2023,
                "reviewed_date": "2025-03-17",
                "accessed_utc": now,
                "notes": "Provides provisional U.S. marriage/divorce rates and counts.",
            },
            {
                "source_id": "cdc_nchs_marriage_divorce_reporting_limitations",
                "title": "National Marriage and Divorce Rate Trends (reporting limitations note)",
                "publisher": "CDC / NCHS",
                "url": "https://www.cdc.gov/nchs/nvss/marriage-divorce.htm",
                "accessed_utc": now,
                "notes": "Used for caveat that divorce data are from reporting areas only.",
            },
            {
                "source_id": "nchs_vsrr38_births_provisional_2024",
                "title": "Vital Statistics Rapid Release Report No. 38: Births, Provisional Data for 2024 (Table 1)",
                "publisher": "CDC / NCHS",
                "url": "https://www.cdc.gov/nchs/data/vsrr/vsrr038.pdf",
                "data_year": 2024,
                "published_month": "2025-04",
                "accessed_utc": now,
                "notes": "Used for maternal age-specific birth rates and counts.",
            },
        ],
    }
    (output_dir / "sources.json").write_text(json.dumps(sources, indent=2), encoding="utf-8")

    manifest = {
        "generated_at_utc": now,
        "params_dir": str(output_dir.resolve()),
        "files": [
            "mobility_overall_acs_2024.csv",
            "mobility_by_age_cohort_acs_2024.csv",
            "marriage_divorce_rates_cdc_2023.csv",
            "fertility_by_age_nchs_2024.csv",
            "household_type_shares_acs_2024.csv",
            "phase2_priors_snapshot.json",
            "sources.json",
        ],
    }
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> int:
    script_path = Path(__file__).resolve()
    project_root = script_path.parents[1]

    parser = argparse.ArgumentParser(description="Build Phase-2 parameter tables from public source priors.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root / "Data" / "phase2_params",
        help="Output directory for Phase-2 parameter tables.",
    )
    args = parser.parse_args()

    manifest = build_phase2_params(args.output_dir.resolve())
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
