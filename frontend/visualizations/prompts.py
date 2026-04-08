"""Template-based chart insight strings and blocking strategy recommendations.

Pre-computed — no LLM call needed. AnalystAgent uses these to give
actionable context alongside every chart.
"""

from __future__ import annotations

from typing import Any


def generate_chart_insight(chart_type: str, data: dict[str, Any]) -> str:
    """Return a one-sentence actionable insight for the given chart type."""
    generators = {
        "noise_radar": _noise_radar_insight,
        "overlap_venn": _overlap_venn_insight,
        "missing_matrix": _missing_matrix_insight,
        "difficulty_scorecard": _scorecard_insight,
        "age_distribution": _age_distribution_insight,
        "event_type_bar": _event_type_bar_insight,
    }
    fn = generators.get(chart_type)
    if fn is None:
        return "Chart shows key run characteristics."
    try:
        return fn(data)
    except Exception:
        return "Chart generated."


def recommend_blocking_strategy(run_summary: dict[str, Any]) -> str:
    """Return a one-sentence blocking strategy recommendation."""
    noise_a = run_summary.get("name_noise_a", 0)
    noise_b = run_summary.get("name_noise_b", 0)
    total_noise = noise_a + noise_b
    overlap = run_summary.get("overlap_pct", 100)
    dup_b = run_summary.get("duplication_b_pct", 0)

    if total_noise > 8:
        return "Use fuzzy blocking (Soundex/Metaphone) — name noise is too high for exact matching."
    if overlap < 50:
        return "Prioritize precision: use restrictive blocking keys to reduce false positive candidates."
    if dup_b > 15:
        return "Deduplicate Dataset B before linking — high within-dataset duplication will inflate false positives."
    return "Standard blocking on LastName + DOB year should work for this difficulty level."


# ──────────────────────────────────────────────────────────────────────────────
# Per-chart insight generators
# ──────────────────────────────────────────────────────────────────────────────

def _noise_radar_insight(data: dict[str, Any]) -> str:
    noise_a = data.get("name_noise_a", 0)
    noise_b = data.get("name_noise_b", 0)
    phonetic = data.get("phonetic_b", 0)
    nickname = data.get("nickname_b", 0)

    if phonetic + nickname > 5:
        detail = "Phonetic + nickname errors in B will break name-based blocking."
    elif noise_b > noise_a * 2:
        detail = "B is much noisier than A — weight B fields lower in similarity scoring."
    else:
        detail = "A and B have similar noise profiles."
    return f"Total name noise — A: {noise_a:.1f}%, B: {noise_b:.1f}%. {detail}"


def _overlap_venn_insight(data: dict[str, Any]) -> str:
    mode = data.get("mode", "pairwise")
    if mode == "single_dataset":
        dataset_id = data.get("dataset_id", "dataset")
        rows = int(data.get("rows", 0) or 0)
        entity_count = int(data.get("entity_count", 0) or 0)
        duplicate_rows = int(data.get("duplicate_rows", 0) or 0)
        return (
            f"{dataset_id} has {rows:,} rows for {entity_count:,} entities; "
            f"{duplicate_rows:,} duplicate rows define the deduplication workload."
        )
    if mode == "multi_dataset":
        all_overlap = float(data.get("all_dataset_overlap_pct", 0.0) or 0.0)
        pairwise = data.get("pairwise_overlap_pairs", []) or []
        weakest = min(pairwise, key=lambda item: float(item.get("overlap_pct", 0.0) or 0.0)) if pairwise else None
        if weakest is None:
            return (
                f"{all_overlap:.0f}% of the union appears in every dataset; "
                "inspect pairwise overlap metrics before choosing blocking keys."
            )
        return (
            f"{all_overlap:.0f}% of the union appears in every dataset; "
            f"the weakest pair is {weakest.get('pair', 'unknown')} at {float(weakest.get('overlap_pct', 0.0) or 0.0):.0f}% overlap."
        )

    overlap = data.get("overlap_pct", 100)
    a_only = data.get("entities_only_in_a", 0)
    b_only = data.get("entities_only_in_b", 0)

    if overlap < 40:
        pressure = "severe recall pressure — most entities don't have a match"
    elif overlap < 60:
        pressure = "high recall pressure — expect many non-matches"
    elif overlap < 75:
        pressure = "moderate overlap — typical for linked government datasets"
    else:
        pressure = "high overlap — recall should be achievable with good blocking"

    return (
        f"{overlap:.0f}% entity overlap ({pressure}); "
        f"{a_only:,} A-only and {b_only:,} B-only records will never match."
    )


def _missing_matrix_insight(data: dict[str, Any]) -> str:
    worst_field = data.get("worst_missing_field_b", "")
    worst_pct = data.get("worst_missing_pct_b", 0)
    if worst_field and worst_pct > 20:
        return (
            f"'{worst_field}' is missing {worst_pct:.0f}% of the time in B "
            "— avoid using it as a blocking key."
        )
    return "Missing data levels are low; most fields are usable for blocking."


def _scorecard_insight(data: dict[str, Any]) -> str:
    rating = data.get("difficulty_rating", "UNKNOWN")
    score = data.get("difficulty_score", 0)
    return (
        f"ER difficulty: {rating} (score {score}/7) — "
        + recommend_blocking_strategy(data)
    )


def _age_distribution_insight(data: dict[str, Any]) -> str:
    peak_age = data.get("peak_age", 0)
    std = data.get("age_std", 0)
    if std > 20:
        spread = "wide age spread increases name collision risk across cohorts"
    else:
        spread = "narrow age spread — age-based blocking will have fewer collisions"
    return f"Population peaks near age {peak_age}; {spread}."


def _event_type_bar_insight(data: dict[str, Any]) -> str:
    dominant = data.get("dominant_event_type", "")
    count = data.get("dominant_event_count", 0)
    total = data.get("total_events", 0)
    if dominant and total:
        pct = 100 * count / total
        return f"{dominant} events dominate ({pct:.0f}% of {total} total) — address changes are the primary matching challenge."
    return f"{total} total life events simulated."
