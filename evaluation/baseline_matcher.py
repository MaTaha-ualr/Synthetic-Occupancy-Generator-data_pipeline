"""Transparent entity-resolution baseline used by the paper experiments.

This is the stable, cited entry point.  The implementation is shared with the
original single-seed evaluator so historical results remain reproducible.
"""

from paper_experiments.evaluate_baseline import (  # noqa: F401
    blocking_keys,
    build_true_pairs,
    choose_threshold,
    dice,
    dob_similarity,
    load_scenario_inputs,
    metrics_at_threshold,
    normalize,
    normalize_date,
    pair_score,
    prepare,
    score_candidates,
    sha256_file,
    soundex,
)

# Paper-visible contract. These constants deliberately duplicate no hidden
# configuration: pair_score and blocking_keys above are the executable truth.
FIELD_WEIGHTS = {
    "first_name": 0.20,
    "last_name": 0.25,
    "date_of_birth": 0.20,
    "street": 0.15,
    "city": 0.05,
    "state": 0.05,
    "postal": 0.10,
}
THRESHOLD_GRID = (0.30, 0.95, 0.01)
FIXED_THRESHOLD = 0.65

