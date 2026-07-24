from __future__ import annotations

import numpy as np
import pandas as pd

from evaluation.metrics import RunData, b3_cluster_metrics, cluster_metrics, oracle_from_curve, pair_metrics, threshold_curve


def test_pair_metrics_counts_true_pairs_missing_from_candidates_as_false_negatives() -> None:
    result = pair_metrics(
        np.asarray([0.9, 0.8, 0.1]),
        np.asarray([True, False, True]),
        threshold=0.5,
        true_count=3,
    )
    assert result["tp"] == 1
    assert result["fp"] == 1
    assert result["fn"] == 2
    assert result["recall"] == 1 / 3


def test_oracle_tie_breaks_by_precision_then_threshold() -> None:
    scores = np.asarray([0.9, 0.8, 0.7])
    labels = np.asarray([True, False, True])
    oracle = oracle_from_curve(threshold_curve(scores, labels, true_count=2))
    assert oracle["oracle_threshold"] == 0.7
    assert oracle["oracle_f1"] == 0.8


def test_b3_includes_singletons_and_transitive_closure() -> None:
    nodes = pd.DataFrame([
        {"node": "A::1", "source": "A", "record_key": "1", "person": "p1"},
        {"node": "B::1", "source": "B", "record_key": "1", "person": "p1"},
        {"node": "A::2", "source": "A", "record_key": "2", "person": "p2"},
    ])
    run = RunData(
        run_dir=None,  # type: ignore[arg-type]
        scenario="tiny", seed=1, topology="link", datasets={},
        entity_map=pd.DataFrame(), nodes=nodes, true_pairs={("A::1", "B::1")},
    )
    perfect = cluster_metrics(run, [("A::1", "B::1")])
    assert perfect["closure_f1"] == 1.0
    assert perfect["b3_f1"] == 1.0

    fragmented = cluster_metrics(run, [])
    assert fragmented["closure_recall"] == 0.0
    assert fragmented["b3_precision"] == 1.0
    assert fragmented["b3_recall"] == 2 / 3


def test_linear_b3_matches_full_cluster_metrics() -> None:
    nodes = pd.DataFrame([
        {"node": "A::1", "source": "A", "record_key": "1", "person": "p1"},
        {"node": "B::1", "source": "B", "record_key": "1", "person": "p1"},
        {"node": "A::2", "source": "A", "record_key": "2", "person": "p2"},
        {"node": "B::2", "source": "B", "record_key": "2", "person": "p2"},
    ])
    run = RunData(
        run_dir=None,  # type: ignore[arg-type]
        scenario="tiny", seed=1, topology="link", datasets={},
        entity_map=pd.DataFrame(), nodes=nodes,
        true_pairs={("A::1", "B::1"), ("A::2", "B::2")},
    )
    edges = [("A::1", "B::1"), ("B::1", "A::2")]
    full = cluster_metrics(run, edges)
    linear = b3_cluster_metrics(run, edges)
    for metric in ("b3_precision", "b3_recall", "b3_f1", "predicted_clusters", "truth_clusters"):
        assert linear[metric] == full[metric]
