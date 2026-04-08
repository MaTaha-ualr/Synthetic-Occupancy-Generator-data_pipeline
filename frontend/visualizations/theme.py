"""Shared visual theme for SOG Benchmark Studio.

A refined color system using:
- Slate (neutral grays)
- Indigo (primary actions)  
- Amber (accent highlights)
"""

from __future__ import annotations

from matplotlib.colors import LinearSegmentedColormap

# ============================================================================
# COLOR SYSTEM
# ============================================================================

# Neutral Scale
SLATE = {
    50: "#f8fafc",
    100: "#f1f5f9",
    200: "#e2e8f0",
    300: "#cbd5e1",
    400: "#94a3b8",
    500: "#64748b",
    600: "#475569",
    700: "#334155",
    800: "#1e293b",
    900: "#0f172a",
}

# Primary Scale
INDIGO = {
    50: "#eef2ff",
    100: "#e0e7ff",
    200: "#c7d2fe",
    300: "#a5b4fc",
    400: "#818cf8",
    500: "#6366f1",
    600: "#4f46e5",
    700: "#4338ca",
    800: "#3730a3",
    900: "#312e81",
}

# Accent Scale
AMBER = {
    200: "#fde68a",
    300: "#fcd34d",
    400: "#fbbf24",
    500: "#f59e0b",
    600: "#d97706",
}

# Semantic Colors
SUCCESS = "#10b981"
WARNING = "#f59e0b"
ERROR = "#ef4444"
INFO = "#3b82f6"

# ============================================================================
# THEME VARIABLES (for matplotlib/plotly)
# ============================================================================

PAPER_BG = SLATE[50]
PAPER_PANEL = "#ffffff"
PAPER_PANEL_ALT = SLATE[100]

INK = SLATE[800]
INK_SOFT = SLATE[500]
INK_FAINT = SLATE[400]

LINE = SLATE[200]
LINE_STRONG = SLATE[300]

# Primary colors for datasets
DATASET_A = INDIGO[500]
DATASET_B = AMBER[500]
BLUE = DATASET_A
COPPER = DATASET_B
GOLD = AMBER[400]
MINT = SUCCESS
RED = ERROR
SLATE_COLOR = SLATE[700]

# Event type colors
EVENT_COLORS = {
    "MOVE": INDIGO[500],
    "COHABIT": "#2b7a68",  # MINT
    "BIRTH": AMBER[500],
    "DIVORCE": ERROR,
    "LEAVE_HOME": AMBER[600],
}

# Difficulty/severity colors
SEVERITY = {
    "good": SUCCESS,
    "low": SUCCESS,
    "medium": WARNING,
    "high": AMBER[500],
    "critical": ERROR,
    "severe": ERROR,
}

# ============================================================================
# COLORMAP FUNCTIONS
# ============================================================================


def gauge_band(low: bool = False) -> list[tuple[float, str]]:
    """Return threshold bands for gauges.
    
    Args:
        low: If True, lower values are better (e.g., error rates)
    
    Returns:
        List of (threshold, color) tuples
    """
    if low:
        return [
            (3, SUCCESS),
            (6, WARNING),
            (10, AMBER[500]),
            (100, ERROR),
        ]
    return [
        (40, ERROR),
        (60, AMBER[500]),
        (75, WARNING),
        (100, SUCCESS),
    ]


def missingness_cmap() -> LinearSegmentedColormap:
    """Colormap for missingness heatmaps.
    
    Returns a blue-orange diverging colormap where:
    - Low values (good coverage) = Blue
    - High values (high missingness) = Orange
    """
    return LinearSegmentedColormap.from_list(
        "sog_missingness",
        [SLATE[100], INDIGO[400], AMBER[500]],
    )


def missing_cmap() -> LinearSegmentedColormap:
    """Backward-compatible alias for older chart code."""
    return missingness_cmap()


def overlap_cmap() -> LinearSegmentedColormap:
    """Colormap for overlap visualization.
    
    Returns a green gradient where higher overlap is better.
    """
    return LinearSegmentedColormap.from_list(
        "sog_overlap",
        [ERROR, WARNING, SUCCESS],
    )


def difficulty_cmap() -> LinearSegmentedColormap:
    """Colormap for difficulty scores.
    
    Returns a blue-amber gradient.
    """
    return LinearSegmentedColormap.from_list(
        "sog_difficulty",
        [INDIGO[400], INDIGO[500], AMBER[400], AMBER[500]],
    )


# ============================================================================
# PLOTLY TEMPLATE
# ============================================================================

PLOTLY_TEMPLATE = {
    "layout": {
        "font": {"family": "Inter, sans-serif", "color": INK},
        "paper_bgcolor": PAPER_BG,
        "plot_bgcolor": PAPER_PANEL,
        "colorway": [INDIGO[500], AMBER[500], "#2b7a68", ERROR, INFO],
        "title": {"font": {"size": 18, "weight": 600}},
        "xaxis": {
            "gridcolor": LINE,
            "linecolor": LINE_STRONG,
            "tickfont": {"size": 11},
        },
        "yaxis": {
            "gridcolor": LINE,
            "linecolor": LINE_STRONG,
            "tickfont": {"size": 11},
        },
        "margin": {"t": 60, "r": 40, "b": 60, "l": 60},
    }
}


# ============================================================================
# MATPLOTLIB RC PARAMS
# ============================================================================

MPL_RC = {
    "font.family": "sans-serif",
    "font.sans-serif": ["Inter", "system-ui", "sans-serif"],
    "font.size": 10,
    "axes.facecolor": PAPER_PANEL,
    "axes.edgecolor": LINE_STRONG,
    "axes.labelcolor": INK_SOFT,
    "axes.grid": True,
    "axes.grid.axis": "y",
    "grid.color": LINE,
    "grid.linestyle": "-",
    "grid.alpha": 0.5,
    "figure.facecolor": PAPER_BG,
    "figure.edgecolor": "none",
    "savefig.facecolor": PAPER_BG,
    "savefig.edgecolor": "none",
    "text.color": INK,
    "xtick.color": INK_SOFT,
    "ytick.color": INK_SOFT,
}


def apply_theme():
    """Apply the SOG theme to matplotlib."""
    import matplotlib.pyplot as plt
    plt.rcParams.update(MPL_RC)
