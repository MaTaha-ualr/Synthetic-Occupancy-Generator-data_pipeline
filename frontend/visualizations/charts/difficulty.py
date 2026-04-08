"""Difficulty charts: difficulty_scorecard."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from visualizations.theme import (
    COPPER,
    GOLD,
    INK,
    INK_SOFT,
    LINE,
    MINT,
    PAPER_BG,
    PAPER_PANEL,
    RED,
    SLATE_COLOR,
)


def _load_pyplot():
    """Use a non-interactive backend for tests and headless runs."""
    import matplotlib

    matplotlib.use("Agg", force=True)

    import matplotlib.pyplot as plt

    return plt


def generate_difficulty_scorecard(run_dir: Path, fmt: str = "png") -> tuple[Any, dict[str, Any]]:
    """Three gauge charts for overlap, noise, and duplication."""
    import json

    import numpy as np
    import yaml

    plt = _load_pyplot()

    qr_path = run_dir / "quality_report.json"
    scen_path = run_dir / "scenario.yaml"

    if not qr_path.exists():
        raise FileNotFoundError(f"quality_report.json not found in {run_dir}")

    qr = json.loads(qr_path.read_text(encoding="utf-8"))
    scenario: dict[str, Any] = {}
    if scen_path.exists():
        scenario = yaml.safe_load(scen_path.read_text(encoding="utf-8")) or {}

    emission = scenario.get("emission", {})
    datasets_cfg = emission.get("datasets", [])
    match_mode = emission.get("crossfile_match_mode", "one_to_one")
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
        total_noise = float(sum(name_noise_values))
    else:
        noise_a = emission.get("noise", {}).get("A", {})
        noise_b = emission.get("noise", {}).get("B", {})
        overlap = emission.get("overlap_entity_pct", 100.0)
        dup_a = emission.get("duplication_in_A_pct", 0.0)
        dup_b = emission.get("duplication_in_B_pct", 0.0)
        name_noise_a = sum(
            noise_a.get(field, 0)
            for field in ["name_typo_pct", "phonetic_error_pct", "ocr_error_pct", "nickname_pct"]
        )
        name_noise_b = sum(
            noise_b.get(field, 0)
            for field in ["name_typo_pct", "phonetic_error_pct", "ocr_error_pct", "nickname_pct"]
        )
        total_noise = name_noise_a + name_noise_b

    score = 0
    if match_mode != "single_dataset":
        if overlap < 40:
            score += 3
        elif overlap < 60:
            score += 2
        elif overlap < 75:
            score += 1
    if total_noise > 10:
        score += 2
    elif total_noise > 5:
        score += 1
    if dup_a > 10 or dup_b > 10:
        score += 1
    if match_mode == "many_to_many":
        score += 1

    rating = {0: "VERY EASY", 1: "EASY", 2: "MEDIUM", 3: "HARD", 4: "VERY HARD"}.get(min(score, 4), "EXTREME")

    data = {
        "difficulty_rating": rating,
        "difficulty_score": score,
        "overlap_pct": overlap,
        "name_noise_a": name_noise_a,
        "name_noise_b": name_noise_b,
        "duplication_a_pct": dup_a,
        "duplication_b_pct": dup_b,
        "match_mode": match_mode,
    }

    def _gauge_color(value: float, breakpoints: list[tuple[float, str]]) -> str:
        for threshold, color in breakpoints:
            if value <= threshold:
                return color
        return breakpoints[-1][1]

    overlap_color = _gauge_color(overlap, [(40, RED), (60, COPPER), (75, GOLD), (100, MINT)])
    noise_color = _gauge_color(total_noise, [(3, MINT), (6, GOLD), (10, COPPER), (100, RED)])
    dup_max = max(dup_a, dup_b)
    dup_color = _gauge_color(dup_max, [(5, MINT), (12, GOLD), (100, COPPER)])

    fig, axes = plt.subplots(1, 3, figsize=(10.4, 4.2))
    fig.patch.set_facecolor(PAPER_BG)

    def _draw_gauge(ax, value: float, max_val: float, label: str, color: str, fmt_spec: str = "{:.0f}%") -> None:
        ax.set_facecolor(PAPER_PANEL)
        theta = np.linspace(np.pi, 0, 200)
        ax.plot(np.cos(theta), np.sin(theta), color=LINE, linewidth=12, solid_capstyle="round")
        ratio = min(value / max_val, 1.0)
        theta_val = np.linspace(np.pi, np.pi - ratio * np.pi, 200)
        ax.plot(np.cos(theta_val), np.sin(theta_val), color=color, linewidth=12, solid_capstyle="round")
        angle = np.pi - ratio * np.pi
        ax.annotate(
            "",
            xy=(0.6 * np.cos(angle), 0.6 * np.sin(angle)),
            xytext=(0, 0),
            arrowprops=dict(arrowstyle="-|>", color=SLATE_COLOR, lw=2),
        )
        ax.text(0, -0.25, fmt_spec.format(value), ha="center", va="center", color=color, fontsize=18, fontweight="bold")
        ax.text(0, -0.55, label, ha="center", va="center", color=INK_SOFT, fontsize=10)
        ax.set_xlim(-1.3, 1.3)
        ax.set_ylim(-0.8, 1.2)
        ax.set_aspect("equal")
        ax.axis("off")

    overlap_value = overlap if match_mode != "single_dataset" else float((datasets_cfg[0] or {}).get("appearance_pct", 100.0)) if isinstance(datasets_cfg, list) and datasets_cfg else 100.0
    overlap_label = "Overlap %" if match_mode != "single_dataset" else "Coverage %"
    _draw_gauge(axes[0], overlap_value, 100, overlap_label, overlap_color)
    _draw_gauge(axes[1], total_noise, 20, "Name Noise %", noise_color)
    _draw_gauge(axes[2], dup_max, 30, "Max Dup %", dup_color)

    rating_colors = {
        "VERY EASY": MINT,
        "EASY": MINT,
        "MEDIUM": GOLD,
        "HARD": COPPER,
        "VERY HARD": RED,
        "EXTREME": RED,
    }
    rating_color = rating_colors.get(rating, INK)
    fig.suptitle(f"ER Difficulty Scorecard | {rating} | {match_mode}", color=rating_color, fontsize=14, y=1.02)
    fig.text(0.5, -0.03, f"Score: {score}/7", ha="center", color=INK_SOFT, fontsize=10)
    plt.tight_layout()

    return fig, data
