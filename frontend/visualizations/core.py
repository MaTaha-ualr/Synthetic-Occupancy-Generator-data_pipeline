"""ChartGenerator - central dispatcher for all SOG visualizations.

Usage:
    from visualizations.core import ChartGenerator, ChartSpec
    gen = ChartGenerator()
    path = gen.generate(ChartSpec(chart_type="noise_radar", run_id="2026-04-04_...", fmt="html"))
    insight = gen.get_insight(spec)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNS_ROOT = PROJECT_ROOT / "phase2" / "runs"
CHARTS_DIR = PROJECT_ROOT / "phase2" / ".sog_charts"

if str(PROJECT_ROOT / "frontend") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "frontend"))


@dataclass
class ChartSpec:
    chart_type: str
    run_id: str
    fmt: str = "png"
    title: str = ""


class ChartGenerator:
    """Generates and caches SOG charts."""

    def __init__(self, charts_dir: Path | None = None):
        self.charts_dir = charts_dir or CHARTS_DIR
        self.charts_dir.mkdir(parents=True, exist_ok=True)
        self._setup_matplotlib_theme()

    def _setup_matplotlib_theme(self) -> None:
        """Apply the shared SOG chart theme to matplotlib."""
        try:
            import matplotlib

            from visualizations.theme import BLUE, INK, INK_SOFT, LINE, PAPER_BG, PAPER_PANEL, SLATE_COLOR

            matplotlib.rcParams.update(
                {
                    "figure.facecolor": PAPER_BG,
                    "axes.facecolor": PAPER_PANEL,
                    "savefig.facecolor": PAPER_BG,
                    "savefig.edgecolor": PAPER_BG,
                    "text.color": INK,
                    "axes.labelcolor": INK_SOFT,
                    "xtick.color": INK_SOFT,
                    "ytick.color": INK_SOFT,
                    "axes.edgecolor": LINE,
                    "grid.color": (0.72, 0.68, 0.64, 0.35),
                    "grid.alpha": 0.3,
                    "axes.grid": False,
                    "font.family": "sans-serif",
                    "axes.titlecolor": SLATE_COLOR,
                    "axes.titleweight": "bold",
                    "figure.titleweight": "bold",
                    "legend.edgecolor": LINE,
                    "legend.facecolor": PAPER_BG,
                    "legend.labelcolor": INK,
                    "axes.prop_cycle": matplotlib.cycler(color=[BLUE, "#f1683f", "#223451", "#2b7a68"]),
                }
            )
        except Exception:
            pass

    def generate(self, spec: ChartSpec) -> Path:
        """Generate chart and return its cache path."""
        ext = "html" if spec.fmt == "html" else "png"
        cache_path = self.charts_dir / f"{spec.run_id}_{spec.chart_type}.{ext}"

        if cache_path.exists():
            import time

            if time.time() - cache_path.stat().st_mtime < 3600:
                return cache_path

        run_dir = RUNS_ROOT / spec.run_id
        if not run_dir.exists():
            raise FileNotFoundError(f"Run directory not found: {spec.run_id}")

        fig, _data = self._dispatch(spec, run_dir)
        self._save(fig, cache_path, spec)
        return cache_path

    def get_insight(self, spec: ChartSpec) -> str:
        """Return a pre-computed insight string for this chart."""
        from visualizations.prompts import generate_chart_insight

        run_dir = RUNS_ROOT / spec.run_id
        if not run_dir.exists():
            return ""
        try:
            _fig, data = self._dispatch(spec, run_dir)
            return generate_chart_insight(spec.chart_type, data)
        except Exception:
            return ""

    def _dispatch(self, spec: ChartSpec, run_dir: Path) -> tuple[Any, dict[str, Any]]:
        """Route to the correct chart generator."""
        ct = spec.chart_type
        fmt = spec.fmt

        if ct == "noise_radar":
            from visualizations.charts.quality import generate_noise_radar

            return generate_noise_radar(run_dir, fmt)

        if ct == "overlap_venn":
            from visualizations.charts.quality import generate_overlap_venn

            return generate_overlap_venn(run_dir, fmt)

        if ct == "missing_matrix":
            from visualizations.charts.quality import generate_missing_matrix

            return generate_missing_matrix(run_dir, fmt)

        if ct == "difficulty_scorecard":
            from visualizations.charts.difficulty import generate_difficulty_scorecard

            return generate_difficulty_scorecard(run_dir, fmt)

        if ct == "age_distribution":
            from visualizations.charts.demographics import generate_age_distribution

            return generate_age_distribution(run_dir, fmt)

        if ct == "event_type_bar":
            from visualizations.charts.demographics import generate_event_type_bar

            return generate_event_type_bar(run_dir, fmt)

        raise ValueError(
            f"Unknown chart_type: {ct!r}. "
            "Supported: noise_radar, overlap_venn, missing_matrix, "
            "difficulty_scorecard, age_distribution, event_type_bar"
        )

    def _save(self, fig: Any, path: Path, spec: ChartSpec) -> None:
        """Save figure to disk."""
        if hasattr(fig, "write_html"):
            if path.suffix.lower() == ".html":
                fig.write_html(str(path), include_plotlyjs="cdn", full_html=True)
                return
            try:
                fig.write_image(str(path))
                return
            except Exception as exc:
                raise RuntimeError(
                    "Static image export for interactive charts requires Plotly image "
                    "export support. Request fmt='html' instead."
                ) from exc

        import matplotlib.pyplot as plt
        from visualizations.theme import PAPER_BG

        fig.savefig(
            str(path),
            dpi=150,
            bbox_inches="tight",
            facecolor=PAPER_BG,
            edgecolor="none",
        )
        plt.close(fig)
