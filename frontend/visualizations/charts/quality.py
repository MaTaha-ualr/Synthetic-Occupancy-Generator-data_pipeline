"""Quality charts: noise_radar, overlap_venn, missing_matrix."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from visualizations.theme import (
    BLUE,
    COPPER,
    DATASET_A,
    DATASET_B,
    GOLD,
    INK,
    INK_FAINT,
    INK_SOFT,
    LINE,
    MINT,
    PAPER_BG,
    PAPER_PANEL,
    PAPER_PANEL_ALT,
    RED,
    SLATE_COLOR,
    missing_cmap,
)


def _load_pyplot():
    """Use a non-interactive backend for tests and headless runs."""
    import matplotlib

    matplotlib.use("Agg", force=True)

    import matplotlib.pyplot as plt

    return plt


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert hex color to a Plotly rgba string."""
    color = hex_color.lstrip("#")
    red = int(color[0:2], 16)
    green = int(color[2:4], 16)
    blue = int(color[4:6], 16)
    return f"rgba({red},{green},{blue},{alpha})"


def _observed_dataset_files(run_dir: Path) -> list[tuple[str, Path]]:
    import json

    manifest_path = run_dir / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            datasets = manifest.get("observed_outputs", {}).get("datasets", [])
            if isinstance(datasets, list):
                discovered = []
                for item in datasets:
                    if not isinstance(item, dict):
                        continue
                    dataset_id = str(item.get("dataset_id", "")).strip()
                    raw_path = str(item.get("path", "")).strip()
                    if dataset_id and raw_path:
                        path = Path(raw_path)
                        if path.exists():
                            discovered.append((dataset_id, path))
                if discovered:
                    return discovered
        except Exception:
            pass

    fallback = []
    for dataset_id, filename in [("A", "DatasetA.csv"), ("B", "DatasetB.csv")]:
        path = run_dir / filename
        if path.exists():
            fallback.append((dataset_id, path))
    return fallback


def generate_noise_radar(run_dir: Path, fmt: str = "html") -> tuple[Any, dict[str, Any]]:
    """Dual radar chart for Dataset A vs B across the main noise dimensions."""
    import plotly.graph_objects as go
    import yaml

    scen_path = run_dir / "scenario.yaml"
    if not scen_path.exists():
        raise FileNotFoundError(f"scenario.yaml not found in {run_dir}")

    scenario = yaml.safe_load(scen_path.read_text(encoding="utf-8")) or {}
    emission = scenario.get("emission", {})
    datasets_cfg = emission.get("datasets", [])
    traces: list[tuple[str, dict[str, Any], str, str]] = []
    if isinstance(datasets_cfg, list) and datasets_cfg:
        colors = [DATASET_A, DATASET_B, MINT, GOLD, RED, SLATE_COLOR]
        dashes = ["solid", "dot", "dash", "dashdot", "longdash", "solid"]
        for idx, item in enumerate(datasets_cfg):
            if not isinstance(item, dict):
                continue
            dataset_id = str(item.get("dataset_id", f"D{idx + 1}")).strip() or f"D{idx + 1}"
            traces.append(
                (
                    dataset_id,
                    item.get("noise", {}) or {},
                    colors[idx % len(colors)],
                    dashes[idx % len(dashes)],
                )
            )
    else:
        noise_a = emission.get("noise", {}).get("A", {})
        noise_b = emission.get("noise", {}).get("B", {})
        traces = [
            ("Dataset A", noise_a, DATASET_A, "solid"),
            ("Dataset B", noise_b, DATASET_B, "dot"),
        ]

    fields = [
        "name_typo_pct",
        "dob_shift_pct",
        "ssn_mask_pct",
        "phone_mask_pct",
        "address_missing_pct",
        "middle_name_missing_pct",
        "phonetic_error_pct",
        "ocr_error_pct",
        "date_swap_pct",
        "zip_digit_error_pct",
        "nickname_pct",
        "suffix_missing_pct",
    ]
    labels = [
        "name_typo",
        "dob_shift",
        "ssn_mask",
        "phone_mask",
        "addr_miss",
        "middle_miss",
        "phonetic",
        "ocr",
        "date_swap",
        "zip_err",
        "nickname",
        "suffix_miss",
    ]

    values_by_trace = {
        label: [noise.get(field, 0.0) for field in fields]
        for label, noise, _color, _dash in traces
    }
    max_val = max([1.0, *[max(values) if values else 0.0 for values in values_by_trace.values()]])
    dataset_name_noise = {
        label: sum(
            noise.get(field, 0)
            for field in ["name_typo_pct", "phonetic_error_pct", "ocr_error_pct", "nickname_pct"]
        )
        for label, noise, _color, _dash in traces
    }
    data = {
        "dataset_name_noise": dataset_name_noise,
        "datasets": [
            {
                "dataset_id": label,
                "name_noise_pct": dataset_name_noise[label],
                "phonetic_pct": float(noise.get("phonetic_error_pct", 0.0) or 0.0),
                "nickname_pct": float(noise.get("nickname_pct", 0.0) or 0.0),
            }
            for label, noise, _color, _dash in traces
        ],
    }
    if traces:
        first_label, first_noise, _first_color, _first_dash = traces[0]
        data["name_noise_a"] = dataset_name_noise[first_label]
        data["phonetic_a"] = float(first_noise.get("phonetic_error_pct", 0.0) or 0.0)
        data["nickname_a"] = float(first_noise.get("nickname_pct", 0.0) or 0.0)
    if len(traces) > 1:
        second_label, second_noise, _second_color, _second_dash = traces[1]
        data["name_noise_b"] = dataset_name_noise[second_label]
        data["phonetic_b"] = float(second_noise.get("phonetic_error_pct", 0.0) or 0.0)
        data["nickname_b"] = float(second_noise.get("nickname_pct", 0.0) or 0.0)

    fig = go.Figure()
    for idx, (label, _noise, color, dash) in enumerate(traces):
        values = values_by_trace[label]
        fig.add_trace(
            go.Scatterpolar(
                r=values + [values[0]],
                theta=labels + [labels[0]],
                fill="toself",
                name=label,
                line=dict(color=color, width=2.5, dash=dash),
                fillcolor=_hex_to_rgba(color, max(0.08, 0.18 - (idx * 0.02))),
            )
        )
    fig.update_layout(
        polar=dict(
            bgcolor=PAPER_PANEL,
            radialaxis=dict(
                visible=True,
                range=[0, max_val * 1.2],
                gridcolor=LINE,
                linecolor=LINE,
                tickfont=dict(color=INK_SOFT),
            ),
            angularaxis=dict(
                gridcolor=LINE,
                linecolor=LINE,
                tickfont=dict(color=INK_SOFT, size=11),
            ),
        ),
        paper_bgcolor=PAPER_BG,
        plot_bgcolor=PAPER_BG,
        font=dict(color=INK, family="Manrope, sans-serif"),
        title=dict(text="Noise Profile by Observed Dataset", font=dict(color=SLATE_COLOR, size=16)),
        legend=dict(
            bgcolor=_hex_to_rgba(PAPER_BG, 0.92),
            bordercolor=LINE,
            borderwidth=1,
            font=dict(color=INK),
        ),
        margin=dict(t=64, b=48, l=60, r=60),
    )
    return fig, data


def generate_overlap_venn(run_dir: Path, fmt: str = "png") -> tuple[Any, dict[str, Any]]:
    """Summarize observed entity overlap for single, pairwise, or multi-dataset runs."""
    import json

    import matplotlib.patches as mpatches

    plt = _load_pyplot()

    qr_path = run_dir / "quality_report.json"
    if not qr_path.exists():
        raise FileNotFoundError(f"quality_report.json not found in {run_dir}")

    qr = json.loads(qr_path.read_text(encoding="utf-8"))
    topology = (
        qr.get("phase2_quality", {})
        .get("er_benchmark_metrics", {})
        .get("topology", {})
    )
    dataset_count = int(topology.get("dataset_count", 2) or 2)
    cross_file = (
        qr.get("phase2_quality", {})
        .get("er_benchmark_metrics", {})
        .get("cross_file_overlap", {})
    )
    multi_dataset = (
        qr.get("phase2_quality", {})
        .get("er_benchmark_metrics", {})
        .get("multi_dataset_overlap", {})
    )
    coverage = qr.get("observed_quality", {}).get("coverage", {})

    if dataset_count == 1:
        per_dataset = (
            qr.get("phase2_quality", {})
            .get("er_benchmark_metrics", {})
            .get("per_dataset", {})
        )
        dataset_ids = topology.get("dataset_ids", [])
        dataset_id = dataset_ids[0] if dataset_ids else "dataset"
        metrics = per_dataset.get(dataset_id, {})
        row_count = int(metrics.get("row_count", 0) or 0)
        entity_count = int(metrics.get("entity_count", 0) or 0)
        duplicate_rows = int(metrics.get("duplicate_rows", 0) or 0)
        data = {
            "mode": "single_dataset",
            "dataset_id": dataset_id,
            "rows": row_count,
            "entity_count": entity_count,
            "duplicate_rows": duplicate_rows,
        }
        fig, ax = plt.subplots(figsize=(7.2, 4.2))
        fig.patch.set_facecolor(PAPER_BG)
        ax.set_facecolor(PAPER_BG)
        circle = mpatches.Circle((0, 0), 1.4, color=BLUE, alpha=0.22, zorder=2)
        ax.add_patch(circle)
        ax.text(0, 0.3, f"{dataset_id}", ha="center", va="center", color=SLATE_COLOR, fontsize=14, fontweight="bold")
        ax.text(0, 0.0, f"Rows: {row_count:,}", ha="center", va="center", color=INK, fontsize=10)
        ax.text(0, -0.25, f"Entities: {entity_count:,}", ha="center", va="center", color=INK_SOFT, fontsize=10)
        ax.text(0, -0.5, f"Duplicate rows: {duplicate_rows:,}", ha="center", va="center", color=COPPER, fontsize=10)
        ax.set_xlim(-2.2, 2.2)
        ax.set_ylim(-1.8, 1.8)
        ax.set_aspect("equal")
        ax.axis("off")
        ax.set_title("Observed Dataset Summary", color=SLATE_COLOR, fontsize=14, pad=12)
        return fig, data

    if dataset_count > 2:
        pairwise = multi_dataset.get("pairwise_overlap", {}) if isinstance(multi_dataset, dict) else {}
        pair_items = []
        for pair_key, payload in pairwise.items():
            if not isinstance(payload, dict):
                continue
            ids = payload.get("dataset_ids", [])
            label = " vs ".join(ids) if isinstance(ids, list) and ids else pair_key.replace("__", " vs ")
            pair_items.append(
                (
                    label,
                    float(payload.get("overlap_pct_of_union", 0.0) or 0.0),
                    int(payload.get("overlap_entities", 0) or 0),
                )
            )
        pair_items.sort(key=lambda item: item[1], reverse=True)
        labels = [item[0] for item in pair_items]
        values = [item[1] for item in pair_items]
        counts = [item[2] for item in pair_items]
        all_overlap_entities = int(multi_dataset.get("all_dataset_overlap_entities", 0) or 0)
        union_entities = int(multi_dataset.get("union_entities", 0) or 0)
        all_overlap_pct = float(multi_dataset.get("all_dataset_overlap_pct_of_union", 0.0) or 0.0)

        data = {
            "mode": "multi_dataset",
            "dataset_count": dataset_count,
            "all_dataset_overlap_entities": all_overlap_entities,
            "all_dataset_overlap_pct": all_overlap_pct,
            "union_entities": union_entities,
            "pairwise_overlap_pairs": [
                {"pair": label, "overlap_pct": value, "overlap_entities": count}
                for label, value, count in pair_items
            ],
        }

        fig_height = max(4.4, 2.6 + (0.45 * max(len(pair_items), 1)))
        fig, ax = plt.subplots(figsize=(8.6, fig_height))
        fig.patch.set_facecolor(PAPER_BG)
        ax.set_facecolor(PAPER_PANEL_ALT)
        if pair_items:
            ypos = list(range(len(pair_items)))
            palette = [BLUE, COPPER, MINT, GOLD, RED, SLATE_COLOR]
            colors = [palette[idx % len(palette)] for idx in range(len(pair_items))]
            ax.barh(ypos, values, color=colors)
            ax.set_yticks(ypos)
            ax.set_yticklabels(labels, color=INK, fontsize=10)
            ax.invert_yaxis()
            ax.set_xlim(0, 100)
            ax.set_xlabel("Pairwise overlap % of union", color=INK_SOFT, fontsize=10)
            for idx, value in enumerate(values):
                ax.text(min(value + 1.5, 97), idx, f"{value:.0f}%  ({counts[idx]:,})", va="center", color=SLATE_COLOR, fontsize=9)
        else:
            ax.text(0.5, 0.5, "No pairwise overlap metrics available", transform=ax.transAxes, ha="center", va="center", color=INK_SOFT)
        for spine in ax.spines.values():
            spine.set_edgecolor(LINE)
        ax.tick_params(colors=INK_SOFT)
        ax.set_title("Cross-Dataset Overlap Summary", color=SLATE_COLOR, fontsize=14, pad=12)
        ax.text(
            0.0,
            1.02,
            f"Shared across all {dataset_count} datasets: {all_overlap_entities:,} entities ({all_overlap_pct:.0f}% of union {union_entities:,})",
            transform=ax.transAxes,
            ha="left",
            va="bottom",
            color=INK_SOFT,
            fontsize=9,
        )
        return fig, data

    if cross_file:
        both = int(cross_file.get("overlap_entities", cross_file.get("entities_in_both", 0)) or 0)
        a_entities = int(cross_file.get("a_entities", 0) or 0)
        b_entities = int(cross_file.get("b_entities", 0) or 0)
        only_a = int(cross_file.get("a_only_entities", max(a_entities - both, 0)) or 0)
        only_b = int(cross_file.get("b_only_entities", max(b_entities - both, 0)) or 0)
        total = int(cross_file.get("union_entities", both + only_a + only_b) or 0) or 1
    else:
        both = int(coverage.get("overlap_entities", coverage.get("entities_in_both", 0)) or 0)
        only_a = int(coverage.get("a_only_entities", coverage.get("entities_only_in_a", 0)) or 0)
        only_b = int(coverage.get("b_only_entities", coverage.get("entities_only_in_b", 0)) or 0)
        only_b += int(coverage.get("late_only_in_b", 0) or 0)
        total = both + only_a + only_b or 1
    overlap_pct = 100 * both / total

    data = {
        "overlap_pct": overlap_pct,
        "entities_in_both": both,
        "entities_only_in_a": only_a,
        "entities_only_in_b": only_b,
        "total_unique": total,
    }

    fig, ax = plt.subplots(figsize=(8, 5))
    fig.patch.set_facecolor(PAPER_BG)
    ax.set_facecolor(PAPER_BG)

    radius = 1.5
    offset = 0.7
    circle_a = mpatches.Circle((-offset, 0), radius, color=BLUE, alpha=0.24, zorder=2)
    circle_b = mpatches.Circle((offset, 0), radius, color=COPPER, alpha=0.24, zorder=2)
    ax.add_patch(circle_a)
    ax.add_patch(circle_b)

    ax.text(
        -offset - 0.8,
        0,
        f"A only\n{only_a:,}\n({100 * only_a / total:.0f}%)",
        ha="center",
        va="center",
        color=BLUE,
        fontsize=10,
        zorder=3,
    )
    ax.text(
        0,
        0,
        f"Both\n{both:,}\n({overlap_pct:.0f}%)",
        ha="center",
        va="center",
        color=SLATE_COLOR,
        fontsize=10,
        fontweight="bold",
        zorder=3,
    )
    ax.text(
        offset + 0.8,
        0,
        f"B only\n{only_b:,}\n({100 * only_b / total:.0f}%)",
        ha="center",
        va="center",
        color=COPPER,
        fontsize=10,
        zorder=3,
    )

    ax.set_xlim(-3.5, 3.5)
    ax.set_ylim(-2.2, 2.2)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title("Entity Overlap: Dataset A vs B", color=SLATE_COLOR, fontsize=14, pad=12)
    ax.text(0, -1.9, f"Total unique entities: {total:,}", ha="center", color=INK_SOFT, fontsize=9)

    return fig, data


def generate_missing_matrix(run_dir: Path, fmt: str = "png") -> tuple[Any, dict[str, Any]]:
    """Heatmap matrix of missing data rates per field for Dataset A and B."""
    import numpy as np
    import pandas as pd

    plt = _load_pyplot()

    results: dict[str, Any] = {}
    fields = ["FirstName", "MiddleName", "LastName", "Suffix", "DOB", "SSN", "Phone", "StreetAddress", "ZipCode"]

    dataset_files = _observed_dataset_files(run_dir)[:2]
    if not dataset_files:
        dataset_files = [("A", run_dir / "DatasetA.csv"), ("B", run_dir / "DatasetB.csv")]

    missing_by_dataset: dict[str, dict[str, float]] = {}
    for dataset_id, fpath in dataset_files:
        store: dict[str, float] = {}
        if fpath.exists():
            try:
                df = pd.read_csv(fpath, dtype=str)
                for field in fields:
                    if field in df.columns:
                        store[field] = 100 * df[field].isna().sum() / max(len(df), 1)
                    else:
                        store[field] = 0.0
            except Exception:
                for field in fields:
                    store[field] = 0.0
        missing_by_dataset[dataset_id] = store

    present_fields = [
        field for field in fields
        if sum(dataset_missing.get(field, 0.0) for dataset_missing in missing_by_dataset.values()) > 0
    ]
    if not present_fields:
        present_fields = fields[:5]

    vals_filtered = np.array(
        [
            [missing_by_dataset[dataset_id].get(field, 0.0) for dataset_id, _path in dataset_files]
            for field in present_fields
        ]
    )

    if dataset_files:
        primary_dataset_id = dataset_files[-1][0]
        worst_idx = int(np.argmax([missing_by_dataset[primary_dataset_id].get(field, 0) for field in present_fields]))
        results["worst_missing_field"] = present_fields[worst_idx]
        results["worst_missing_pct"] = missing_by_dataset[primary_dataset_id].get(present_fields[worst_idx], 0)

    fig, ax = plt.subplots(figsize=(4.2 + len(dataset_files) * 1.2, max(3, len(present_fields) * 0.52 + 1)))
    fig.patch.set_facecolor(PAPER_BG)
    ax.set_facecolor(PAPER_PANEL_ALT)

    cmap = missing_cmap()
    im = ax.imshow(vals_filtered, aspect="auto", cmap=cmap, vmin=0, vmax=50)

    ax.set_xticks(range(len(dataset_files)))
    ax.set_xticklabels([dataset_id for dataset_id, _path in dataset_files], color=INK, fontsize=10)
    ax.set_yticks(range(len(present_fields)))
    ax.set_yticklabels(present_fields, color=INK_SOFT, fontsize=9)
    ax.tick_params(colors=INK_SOFT)
    for spine in ax.spines.values():
        spine.set_edgecolor(LINE)

    for i in range(len(present_fields)):
        for j in range(len(dataset_files)):
            value = vals_filtered[i, j]
            if value > 0.1:
                text_color = PAPER_BG if value > 20 else SLATE_COLOR
                ax.text(j, i, f"{value:.1f}%", ha="center", va="center", color=text_color, fontsize=8)

    ax.set_title("Missing Data by Field", color=SLATE_COLOR, fontsize=13, pad=10)
    cbar = plt.colorbar(im, ax=ax, label="Missing %")
    cbar.ax.yaxis.label.set_color(INK_SOFT)
    cbar.ax.tick_params(colors=INK_FAINT)
    cbar.outline.set_edgecolor(LINE)

    return fig, results
