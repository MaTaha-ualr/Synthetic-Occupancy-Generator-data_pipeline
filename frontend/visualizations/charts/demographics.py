"""Demographics charts: age_distribution, event_type_bar."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from visualizations.theme import (
    EVENT_COLORS,
    INK,
    INK_SOFT,
    LINE,
    PAPER_BG,
    PAPER_PANEL,
    SLATE,
    SLATE_COLOR,
)


def _load_pyplot():
    """Use a non-interactive backend for tests and headless runs."""
    import matplotlib

    matplotlib.use("Agg", force=True)

    import matplotlib.pyplot as plt

    return plt


def generate_age_distribution(run_dir: Path, fmt: str = "png") -> tuple[Any, dict[str, Any]]:
    """Histogram of ages with event-type median markers."""
    import pandas as pd
    import yaml

    plt = _load_pyplot()

    people_path = run_dir / "truth_people.parquet"
    events_path = run_dir / "truth_events.parquet"
    scen_path = run_dir / "scenario.yaml"

    if not people_path.exists():
        raise FileNotFoundError(f"truth_people.parquet not found in {run_dir}")

    people = pd.read_parquet(people_path)

    sim_end = pd.Timestamp.now()
    if scen_path.exists():
        try:
            scenario = yaml.safe_load(scen_path.read_text(encoding="utf-8")) or {}
            start = scenario.get("simulation", {}).get("start_date")
            periods = scenario.get("simulation", {}).get("periods", 12)
            granularity = scenario.get("simulation", {}).get("granularity", "monthly")
            if start:
                offset = pd.DateOffset(months=periods) if granularity == "monthly" else pd.DateOffset(days=periods)
                sim_end = pd.Timestamp(str(start)) + offset
        except Exception:
            pass

    dob_col = next((col for col in people.columns if "dob" in col.lower() or "birth" in col.lower()), None)
    if dob_col is None:
        raise ValueError("No DOB column found in truth_people.parquet")

    people["_age"] = ((sim_end - pd.to_datetime(people[dob_col], errors="coerce")).dt.days / 365.25).round(0)
    ages = people["_age"].dropna().astype(int)
    ages = ages[(ages >= 0) & (ages <= 110)]

    data: dict[str, Any] = {
        "peak_age": int(ages.mode().iloc[0]) if len(ages) else 0,
        "age_std": float(ages.std()) if len(ages) else 0,
    }

    fig, ax = plt.subplots(figsize=(8, 4.2))
    fig.patch.set_facecolor(PAPER_BG)
    ax.set_facecolor(PAPER_PANEL)

    ax.hist(ages, bins=range(0, 111, 5), color=SLATE[600], alpha=0.78, edgecolor=PAPER_BG, linewidth=0.6)

    if events_path.exists():
        try:
            events = pd.read_parquet(events_path)
            pk_col = next((col for col in people.columns if "personkey" in col.lower() or "person_key" in col.lower()), None)
            ev_pk_col = next((col for col in events.columns if "personkey" in col.lower() or "person_key" in col.lower()), None)
            if pk_col and ev_pk_col and dob_col:
                merged = events.merge(people[[pk_col, dob_col, "_age"]], left_on=ev_pk_col, right_on=pk_col, how="left")
                event_type_col = next((col for col in events.columns if "event" in col.lower() and "type" in col.lower()), None)
                if event_type_col:
                    for event_type, color in EVENT_COLORS.items():
                        subset = merged[merged[event_type_col] == event_type]["_age"].dropna()
                        if len(subset) >= 3:
                            median_age = subset.median()
                            ax.axvline(
                                median_age,
                                color=color,
                                linestyle="--",
                                linewidth=1.6,
                                alpha=0.95,
                                label=f"{event_type} (med {median_age:.0f})",
                            )
        except Exception:
            pass

    ax.set_xlabel("Age", color=INK_SOFT)
    ax.set_ylabel("Count", color=INK_SOFT)
    ax.set_title("Age Distribution with Life Event Medians", color=SLATE_COLOR, fontsize=13)
    ax.tick_params(colors=INK_SOFT)
    for spine in ax.spines.values():
        spine.set_edgecolor(LINE)
    if ax.get_legend_handles_labels()[0]:
        ax.legend(fontsize=8, facecolor=PAPER_BG, labelcolor=INK, edgecolor=LINE)

    return fig, data


def generate_event_type_bar(run_dir: Path, fmt: str = "png") -> tuple[Any, dict[str, Any]]:
    """Horizontal bar chart of event type counts."""
    import json

    plt = _load_pyplot()

    qr_path = run_dir / "quality_report.json"
    if not qr_path.exists():
        raise FileNotFoundError(f"quality_report.json not found in {run_dir}")

    qr = json.loads(qr_path.read_text(encoding="utf-8"))
    event_counts: dict[str, int] = qr.get("simulation_quality", {}).get("event_counts", {})

    if not event_counts:
        raise ValueError("No event_counts found in quality_report.json")

    sorted_events = sorted(event_counts.items(), key=lambda item: item[1], reverse=True)
    labels = [event for event, _count in sorted_events]
    counts = [count for _event, count in sorted_events]
    bar_colors = [EVENT_COLORS.get(label, SLATE[600]) for label in labels]

    total = sum(counts)
    dominant = labels[0] if labels else ""
    data = {
        "dominant_event_type": dominant,
        "dominant_event_count": counts[0] if counts else 0,
        "total_events": total,
        "event_counts": event_counts,
    }

    fig, ax = plt.subplots(figsize=(7.2, max(2.8, len(labels) * 0.62)))
    fig.patch.set_facecolor(PAPER_BG)
    ax.set_facecolor(PAPER_PANEL)

    bars = ax.barh(labels, counts, color=bar_colors, edgecolor=PAPER_BG, height=0.62)
    for bar, count in zip(bars, counts):
        ax.text(
            bar.get_width() + max(counts) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{count:,}",
            va="center",
            ha="left",
            color=INK_SOFT,
            fontsize=9,
        )

    ax.set_xlabel("Event Count", color=INK_SOFT)
    ax.set_title(f"Life Events | {total:,} total", color=SLATE_COLOR, fontsize=13)
    ax.tick_params(colors=INK_SOFT)
    ax.invert_yaxis()
    for spine in ax.spines.values():
        spine.set_edgecolor(LINE)
    ax.set_xlim(0, max(counts) * 1.15)

    return fig, data
