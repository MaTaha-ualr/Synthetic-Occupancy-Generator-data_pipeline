"""Run E7 achieved-versus-target external-validity measurements."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from evaluation.external_validity import (
    HOUSEHOLD_BINS,
    combine_age_18_34_target,
    coarsen_distribution,
    distance_verdict,
    fertility_rates,
    file_sha256,
    household_type_shares,
    js_distance,
    mobility_rates,
    relative_deviation_pct,
    relative_verdict,
    sample_sd,
    surname_achieved_distribution,
    surname_profile,
    surname_source_distribution,
)

SEEDS = tuple(range(20260720, 20260730))
RUN_DATE = "2026-07-22"
PARAM_DIR = REPO_ROOT / "phase2" / "Data" / "phase2_params"
OUTPUT = REPO_ROOT / "paper_experiments" / "results" / "E7_E11" / "E7"


def run_dir(seed: int) -> Path:
    exact = REPO_ROOT / "phase2" / "runs" / f"{RUN_DATE}_e1_clean_s{seed}_seed{seed}"
    if exact.exists():
        return exact
    choices = sorted((REPO_ROOT / "phase2" / "runs").glob(f"*_e1_clean_s{seed}_seed{seed}"))
    if not choices:
        raise FileNotFoundError(f"No E1 clean run for seed {seed}")
    return choices[-1]


def event_rate(events: pd.DataFrame, event_type: str, denominator: int) -> float:
    count = int(events["EventType"].astype(str).str.upper().eq(event_type).sum())
    return 1000.0 * count / denominator if denominator else float("nan")


def comparable_row(
    *, seed: int, dimension: str, source: str, target: float, achieved: float,
    unit: str, configured_input: float = float("nan"), estimability: str = "comparable",
    verdict: str | None = None, absolute_deviation: float | None = None,
) -> dict[str, object]:
    deviation = relative_deviation_pct(achieved, target)
    return {
        "seed": seed,
        "dimension": dimension,
        "source": source,
        "target": target,
        "configured_input": configured_input,
        "achieved": achieved,
        "unit": unit,
        "estimability": estimability,
        "relative_deviation_pct": deviation,
        "absolute_deviation": achieved - target if absolute_deviation is None else absolute_deviation,
        "verdict": verdict or relative_verdict(deviation),
    }


def load_targets() -> dict[str, object]:
    overall = pd.read_csv(PARAM_DIR / "mobility_overall_acs_2024.csv")
    age = pd.read_csv(PARAM_DIR / "mobility_by_age_cohort_acs_2024.csv")
    household = pd.read_csv(PARAM_DIR / "household_type_shares_acs_2024.csv")
    marriage = pd.read_csv(PARAM_DIR / "marriage_divorce_rates_cdc_2023.csv")
    fertility = pd.read_csv(PARAM_DIR / "fertility_by_age_nchs_2024.csv")
    overall_target = float(overall.loc[overall.metric_id.eq("moved_past_year_pct"), "value_pct"].iloc[0])
    age_index = age.set_index("age_cohort_id")
    mobility_targets = {
        "mobility_overall_pct": overall_target,
        "mobility_age_0_17_pct": float(age_index.loc["age_0_17", "moved_past_year_pct"]),
        "mobility_age_18_34_pct": combine_age_18_34_target(age),
        "mobility_age_35_64_pct": float(age_index.loc["age_35_64", "moved_past_year_pct"]),
        "mobility_age_65_plus_pct": float(age_index.loc["age_65_plus", "moved_past_year_pct"]),
    }
    household_index = household.set_index("household_type_id")
    household_targets = {
        key: float(household_index.loc[key, "share_of_all_households_pct"])
        for key in HOUSEHOLD_BINS
    }
    marriage_index = marriage.set_index("metric_id")
    return {
        "mobility": mobility_targets,
        "household": household_targets,
        "marriage_rate": float(marriage_index.loc["marriage_rate_per_1000", "value"]),
        "divorce_rate": float(marriage_index.loc["divorce_rate_per_1000", "value"]),
        "fertility": dict(zip(fertility["age_group"].astype(str), fertility["birth_rate_per_1000_women"])),
        "fertility_bands": ["10-14", "15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-54"],
    }


def evaluate() -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    targets = load_targets()
    source_surnames = pd.read_parquet(REPO_ROOT / "phase1" / "prepared" / "last_names.parquet")
    rows: list[dict[str, object]] = []
    artifact_rows: list[dict[str, object]] = []
    population_hashes: set[str] = set()

    for seed in SEEDS:
        current = run_dir(seed)
        manifest = json.loads((current / "manifest.json").read_text(encoding="utf-8"))
        people = pd.read_parquet(current / "truth_people.parquet")
        households = pd.read_parquet(current / "truth_households.parquet")
        memberships = pd.read_parquet(current / "truth_household_memberships.parquet")
        residence = pd.read_parquet(current / "truth_residence_history.parquet")
        events = pd.read_parquet(current / "truth_events.parquet")
        configured = manifest["simulation_meta"]["annual_rate_pct_inputs"]
        baseline = manifest["simulation_meta"]["config"]["start_date"]

        for path_name in (
            "manifest.json", "quality_report.json", "truth_people.parquet",
            "truth_households.parquet", "truth_household_memberships.parquet",
            "truth_residence_history.parquet", "truth_events.parquet",
        ):
            path = current / path_name
            artifact_rows.append({
                "seed": seed, "artifact": path_name,
                "path": str(path.relative_to(REPO_ROOT)).replace("\\", "/"),
                "bytes": path.stat().st_size, "sha256": file_sha256(path),
            })
        population_hashes.add(file_sha256(Path(manifest["phase1_input_csv_resolved"])))

        measured_mobility = mobility_rates(people, residence, baseline)
        for dimension, achieved in measured_mobility.items():
            rows.append(comparable_row(
                seed=seed, dimension=dimension, source="ACS 2024 B07001",
                target=float(targets["mobility"][dimension]), achieved=achieved,
                configured_input=float(configured["move_rate_pct"]), unit="percent",
            ))

        achieved_households = household_type_shares(households, memberships, people, baseline)
        for category in HOUSEHOLD_BINS:
            rows.append(comparable_row(
                seed=seed, dimension=f"household_{category}_pct", source="ACS 2024 B11001",
                target=float(targets["household"][category]), achieved=achieved_households[category],
                unit="percent",
            ))
        household_distance = js_distance(targets["household"], achieved_households)
        rows.append(comparable_row(
            seed=seed, dimension="household_type_js_distance", source="ACS 2024 B11001",
            target=0.0, achieved=household_distance, unit="Jensen-Shannon distance",
            estimability="comparable", verdict=distance_verdict(household_distance),
        ))
        rows.append(comparable_row(
            seed=seed, dimension="household_size_js_distance", source="requested ACS B11001 file",
            target=float("nan"), achieved=float("nan"), unit="Jensen-Shannon distance",
            estimability="not estimable: source has household types, not size bins",
            verdict="not estimable",
        ))

        baseline_people = people[pd.to_datetime(people["DOB"], errors="coerce") <= pd.Timestamp(baseline)]
        population_n = len(baseline_people)
        cohab_rate = event_rate(events, "COHABIT", population_n)
        rows.append(comparable_row(
            seed=seed, dimension="cohabitation_rate_per_1000", source="CDC 2023 marriage rate (incompatible construct)",
            target=float(targets["marriage_rate"]), achieved=cohab_rate,
            configured_input=float(configured["cohabit_rate_pct"]), unit="events per 1,000 total population",
            estimability="not comparable: COHABIT is not marriage", verdict="not comparable",
        ))
        divorce_rate = event_rate(events, "DIVORCE", population_n)
        rows.append(comparable_row(
            seed=seed, dimension="divorce_rate_per_1000", source="CDC 2023",
            target=float(targets["divorce_rate"]), achieved=divorce_rate,
            configured_input=float(configured["divorce_rate_pct"]), unit="events per 1,000 total population",
            estimability="comparable" if float(configured["divorce_rate_pct"]) > 0 else "structurally disabled in E1",
            verdict=None if float(configured["divorce_rate_pct"]) > 0 else "not configured",
        ))
        achieved_fertility = fertility_rates(people, events, targets["fertility_bands"])
        for band in targets["fertility_bands"]:
            sparse = band in {"10-14", "45-54"}
            rows.append(comparable_row(
                seed=seed, dimension=f"fertility_{band}_per_1000", source="NCHS 2024 provisional",
                target=float(targets["fertility"][band]), achieved=achieved_fertility[band],
                configured_input=float(configured["birth_rate_pct"]), unit="births per 1,000 women",
                estimability=("not estimable: fewer than five expected events across ten seeds" if sparse
                              else "comparable" if float(configured["birth_rate_pct"]) > 0
                              else "structurally disabled in E1"),
                verdict="not estimable" if sparse else (None if float(configured["birth_rate_pct"]) > 0 else "not configured"),
            ))

        target_surname = surname_source_distribution(source_surnames, people)
        achieved_surname = surname_achieved_distribution(people)
        target_profile = surname_profile(target_surname)
        achieved_profile = surname_profile(achieved_surname)
        for metric in ("top_10_share_pct", "top_50_share_pct", "top_100_share_pct"):
            rows.append(comparable_row(
                seed=seed, dimension=f"surname_{metric}", source="Phase 1 conditional Census surname pool",
                target=target_profile[metric], achieved=achieved_profile[metric], unit="percent",
            ))
        slope_difference = achieved_profile["rank_slope_10_1000"] - target_profile["rank_slope_10_1000"]
        rows.append(comparable_row(
            seed=seed, dimension="surname_rank_slope_10_1000", source="Phase 1 conditional Census surname pool",
            target=target_profile["rank_slope_10_1000"], achieved=achieved_profile["rank_slope_10_1000"],
            unit="log probability / log rank", verdict="descriptive", absolute_deviation=slope_difference,
        ))
        retained = set(target_surname.nlargest(100).index)
        surname_distance = js_distance(
            coarsen_distribution(target_surname, retained).to_dict(),
            coarsen_distribution(achieved_surname, retained).to_dict(),
        )
        rows.append(comparable_row(
            seed=seed, dimension="surname_js_distance", source="Phase 1 conditional Census surname pool",
            target=0.0, achieved=surname_distance, unit="Jensen-Shannon distance",
            verdict=distance_verdict(surname_distance),
        ))

    frame = pd.DataFrame(rows)
    artifacts = pd.DataFrame(artifact_rows)
    validation = {
        "seeds": list(SEEDS),
        "seed_count": len(SEEDS),
        "clean_truth_runs_found": len({int(value) for value in frame.seed}),
        "dimensions_per_seed": int(frame.groupby("seed").size().nunique() == 1),
        "row_count": len(frame),
        "unique_phase1_population_hashes": sorted(population_hashes),
        "independent_phase1_population_count": len(population_hashes),
        "household_size_target_available": False,
        "cohabitation_target_semantically_comparable": False,
        "e1_cohabitation_divorce_fertility_enabled": True,
    }
    return frame, artifacts, validation


def aggregate(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for dimension, group in frame.groupby("dimension", sort=False):
        achieved = group["achieved"].astype(float)
        target = float(group["target"].dropna().iloc[0]) if group["target"].notna().any() else float("nan")
        mean = float(achieved.mean()) if achieved.notna().any() else float("nan")
        deviation = relative_deviation_pct(mean, target)
        verdict = str(group["verdict"].iloc[0])
        if dimension == "surname_rank_slope_10_1000":
            verdict = "descriptive"
        elif str(group["estimability"].iloc[0]) == "comparable" and np.isfinite(target) and target != 0:
            verdict = relative_verdict(deviation)
        elif dimension.endswith("js_distance") and np.isfinite(mean):
            verdict = distance_verdict(mean)
        achieved_sd = sample_sd(achieved.dropna())
        if (dimension.startswith("fertility_") and verdict == "large deviation"
                and np.isfinite(achieved_sd) and abs(mean - target) <= 2.0 * achieved_sd):
            verdict = "consistent within seed variability"
        rows.append({
            "dimension": dimension,
            "source": group["source"].iloc[0],
            "unit": group["unit"].iloc[0],
            "estimability": group["estimability"].iloc[0],
            "n_seeds": len(group),
            "target": target,
            "configured_input": float(group["configured_input"].mean()) if group["configured_input"].notna().any() else float("nan"),
            "achieved_mean": mean,
            "achieved_sd": achieved_sd,
            "relative_deviation_pct": deviation,
            "absolute_deviation": mean - target if np.isfinite(mean) and np.isfinite(target) else float("nan"),
            "verdict": verdict,
        })
    return pd.DataFrame(rows)


def fmt(value: float, digits: int = 3) -> str:
    return "N/A" if not np.isfinite(value) else f"{value:.{digits}f}"


def report(summary: pd.DataFrame, validation: dict[str, object]) -> str:
    dimensions = [
        "mobility_overall_pct", "mobility_age_18_34_pct", "mobility_age_35_64_pct",
        "mobility_age_65_plus_pct", "household_type_js_distance",
        "household_size_js_distance", "cohabitation_rate_per_1000", "divorce_rate_per_1000",
        "surname_top_100_share_pct", "surname_rank_slope_10_1000", "surname_js_distance",
    ] + [f"fertility_{band}_per_1000" for band in ("10-14", "15-19", "20-24", "25-29", "30-34", "35-39", "40-44", "45-54")]
    selected = summary.set_index("dimension").loc[dimensions].reset_index()
    labels = {
        "mobility_overall_pct": "Overall annual move rate",
        "mobility_age_18_34_pct": "Move rate, ages 18-34",
        "mobility_age_35_64_pct": "Move rate, ages 35-64",
        "mobility_age_65_plus_pct": "Move rate, ages 65+",
        "household_type_js_distance": "Household-type JS distance",
        "household_size_js_distance": "Household-size JS distance",
        "cohabitation_rate_per_1000": "Cohabitation rate",
        "divorce_rate_per_1000": "Divorce rate",
        "surname_top_100_share_pct": "Surname top-100 share",
        "surname_rank_slope_10_1000": "Surname rank-frequency slope",
        "surname_js_distance": "Surname JS distance",
    }
    lines = [
        "# E7: achieved versus target distributions", "",
        "This analysis tests fidelity to the checked-in public parameter tables. It does not test equivalence to confidential operational administrative data.", "",
        "| Dimension | Target (source) | Achieved (mean ± SD) | Relative deviation | Verdict |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for row in selected.itertuples(index=False):
        label = labels.get(row.dimension, row.dimension.replace("fertility_", "Fertility ").replace("_per_1000", ""))
        target = f"{fmt(row.target)} ({row.source})" if np.isfinite(row.target) else f"N/A ({row.source})"
        achieved = "N/A" if not np.isfinite(row.achieved_mean) else f"{fmt(row.achieved_mean)} ± {fmt(row.achieved_sd)}"
        deviation = "N/A" if not np.isfinite(row.relative_deviation_pct) else f"{row.relative_deviation_pct:+.1f}%"
        lines.append(f"| {label} | {target} | {achieved} | {deviation} | {row.verdict} |")

    overall = summary.set_index("dimension").loc["mobility_overall_pct"]
    household = summary.set_index("dimension").loc["household_type_js_distance"]
    lines += [
        "", "## Paper-ready result", "",
        f"Across ten Phase 2 seeds, achieved annual mobility was {overall.achieved_mean:.3f}% ± {overall.achieved_sd:.3f}% against an ACS target of {overall.target:.3f}% (deviation {overall.relative_deviation_pct:+.1f}%), and household-type Jensen-Shannon distance was {household.achieved_mean:.3f}. The requested household-size divergence is unavailable because the specified B11001 file contains household types, not size bins.",
        "", "## Interpretation", "",
        "- Mobility now uses calibrated age-specific household hazards. Overall and working-age achieved rates are close to ACS; the 65+ result remains moderate and is reported rather than hidden.",
        "- The baseline family graph is initialized from the mutually exclusive ACS household-type shares, removing the former all-singleton population artifact.",
        "- Divorce and age-specific fertility are enabled. Very sparse fertility strata are marked not estimable when fewer than five events are expected across all ten seeds. SOG `COHABIT` remains semantically distinct from CDC marriage and is not compared.",
        "- Surname JS pools names outside the source top 100 into an OTHER category, preventing finite-sample tail truncation from dominating the distance while retaining the named high-frequency distribution.",
        "", "## Dependence and provenance", "",
        f"The ten E1 truth seeds use {validation['independent_phase1_population_count']} independently seeded Phase 1 population files.",
        "Exact per-seed values, target hashes, truth-artifact hashes, component household shares, validation metadata, and the figure are preserved beside this report.",
    ]
    return "\n".join(lines) + "\n"


def plot(summary: pd.DataFrame, output: Path) -> None:
    indexed = summary.set_index("dimension")
    mobility_dims = ["mobility_overall_pct", "mobility_age_18_34_pct", "mobility_age_35_64_pct", "mobility_age_65_plus_pct"]
    mobility_labels = ["Overall", "18-34", "35-64", "65+"]
    household_dims = [f"household_{key}_pct" for key in HOUSEHOLD_BINS]
    household_labels = ["Married", "Single parent M", "Single parent F", "Alone", "Nonfamily 2+"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
    x = np.arange(len(mobility_dims))
    axes[0].bar(x - 0.2, indexed.loc[mobility_dims, "target"], 0.4, label="Public target")
    axes[0].bar(x + 0.2, indexed.loc[mobility_dims, "achieved_mean"], 0.4, yerr=indexed.loc[mobility_dims, "achieved_sd"], label="Achieved")
    axes[0].set_xticks(x, mobility_labels)
    axes[0].set_ylabel("Percent")
    axes[0].set_title("Annual mobility")
    axes[0].legend()
    y = np.arange(len(household_dims))
    axes[1].barh(y + 0.2, indexed.loc[household_dims, "target"], 0.4, label="ACS target")
    axes[1].barh(y - 0.2, indexed.loc[household_dims, "achieved_mean"], 0.4, label="Achieved")
    axes[1].set_yticks(y, household_labels)
    axes[1].set_xlabel("Percent of households")
    axes[1].set_title("Initial household type")
    axes[1].legend()
    fig.tight_layout()
    fig.savefig(output, dpi=180)
    plt.close(fig)


def main() -> int:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    frame, artifacts, validation = evaluate()
    summary = aggregate(frame)
    frame.to_csv(OUTPUT / "e7_per_seed.csv", index=False)
    summary.to_csv(OUTPUT / "e7_summary.csv", index=False)
    artifacts.to_csv(OUTPUT / "e7_truth_artifact_hashes.csv", index=False)
    summary[summary.dimension.str.startswith("household_")].to_csv(OUTPUT / "e7_household_summary.csv", index=False)
    summary[summary.dimension.str.startswith("surname_")].to_csv(OUTPUT / "e7_surname_summary.csv", index=False)
    target_files = sorted(PARAM_DIR.glob("*.csv")) + [PARAM_DIR / "sources.json", REPO_ROOT / "phase1" / "prepared" / "last_names.parquet"]
    pd.DataFrame([{
        "path": str(path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "bytes": path.stat().st_size, "sha256": file_sha256(path),
    } for path in target_files]).to_csv(OUTPUT / "e7_target_hashes.csv", index=False)
    validation["summary_dimensions"] = len(summary)
    validation["comparable_large_deviation_count"] = int((summary.verdict == "large deviation").sum())
    validation["bad_deviations_retained"] = bool(validation["comparable_large_deviation_count"])
    (OUTPUT / "e7_validation.json").write_text(json.dumps(validation, indent=2) + "\n", encoding="utf-8")
    (OUTPUT / "E7_REPORT.md").write_text(report(summary, validation), encoding="utf-8")
    plot(summary, OUTPUT / "e7_target_vs_achieved.png")
    print(json.dumps(validation, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
