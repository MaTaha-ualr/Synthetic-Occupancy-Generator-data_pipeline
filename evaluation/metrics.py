"""Topology-aware pair generation and pair/cluster metrics for E2-E6."""

from __future__ import annotations

import json
import time
from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Callable, Iterable

import numpy as np
import pandas as pd

from evaluation.baseline_matcher import blocking_keys, dice, dob_similarity, normalize, prepare

FEATURES = ("first", "last", "dob", "street", "city", "state", "postal")
BASELINE_WEIGHTS = np.asarray([0.20, 0.25, 0.20, 0.15, 0.05, 0.05, 0.10])


@dataclass
class RunData:
    run_dir: Path
    scenario: str
    seed: int
    topology: str
    datasets: dict[str, pd.DataFrame]
    entity_map: pd.DataFrame
    nodes: pd.DataFrame
    true_pairs: set[tuple[str, str]]


def _node(source: str, record: str) -> str:
    return f"{source}::{record}"


def _eligible(source_l: str, source_r: str, topology: str) -> bool:
    return source_l == source_r if topology == "dedupe" else source_l != source_r


def load_run(run_dir: Path) -> RunData:
    run_dir = run_dir.resolve()
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    observed = manifest["observed_outputs"]
    datasets: dict[str, pd.DataFrame] = {}
    for item in observed["datasets"]:
        frame = pd.read_csv(run_dir / item["filename"], dtype=str, keep_default_na=False)
        datasets[str(item["dataset_id"])] = frame
    topology = "dedupe" if len(datasets) == 1 else "link"
    entity_map = pd.read_csv(run_dir / "entity_record_map.csv", dtype=str, keep_default_na=False)
    map_work = entity_map[["PersonKey", "DatasetId", "RecordKey"]].copy()
    map_work = map_work[(map_work.PersonKey != "") & (map_work.RecordKey != "")]
    person_by_node = {
        _node(str(row.DatasetId), str(row.RecordKey)): str(row.PersonKey)
        for row in map_work.itertuples(index=False)
    }
    node_rows: list[dict[str, str]] = []
    for source, frame in datasets.items():
        key_col = "RecordKey" if "RecordKey" in frame.columns else f"{source}_RecordKey"
        if key_col not in frame.columns:
            raise ValueError(f"{run_dir.name}/{source}: no RecordKey column")
        for record in frame[key_col].astype(str):
            node_id = _node(source, record)
            if node_id not in person_by_node:
                raise ValueError(f"{run_dir.name}: {node_id} absent from entity_record_map.csv")
            node_rows.append({"node": node_id, "source": source, "record_key": record, "person": person_by_node[node_id]})
    nodes = pd.DataFrame(node_rows).drop_duplicates("node")
    grouped = nodes.groupby("person", sort=False)
    true_pairs: set[tuple[str, str]] = set()
    source_by_node = dict(zip(nodes.node, nodes.source))
    for _, group in grouped:
        ids = sorted(group.node.tolist())
        for left, right in combinations(ids, 2):
            if _eligible(source_by_node[left], source_by_node[right], topology):
                true_pairs.add((left, right))
    return RunData(
        run_dir=run_dir,
        scenario=str(manifest["scenario_id"]),
        seed=int(manifest["seed"]),
        topology=topology,
        datasets=datasets,
        entity_map=entity_map,
        nodes=nodes,
        true_pairs=true_pairs,
    )


def _field_features(left: dict[str, list], li: int, right: dict[str, list], ri: int) -> list[float]:
    return [
        dice(left["first_bigrams"][li], right["first_bigrams"][ri]),
        dice(left["last_bigrams"][li], right["last_bigrams"][ri]),
        dob_similarity(left["dob_parts"][li], right["dob_parts"][ri]),
        dice(left["street_bigrams"][li], right["street_bigrams"][ri]),
        float(bool(left["city"][li]) and left["city"][li] == right["city"][ri]),
        float(bool(left["state"][li]) and left["state"][li] == right["state"][ri]),
        float(bool(left["zip"][li]) and left["zip"][li] == right["zip"][ri]),
    ]


def build_candidates(run: RunData) -> tuple[pd.DataFrame, float]:
    started = time.perf_counter()
    prepared: dict[str, dict[str, list]] = {source: prepare(frame, source) for source, frame in run.datasets.items()}
    refs: list[tuple[str, int, str]] = []
    block_index: dict[str, list[int]] = defaultdict(list)
    for source, values in prepared.items():
        for idx, record in enumerate(values["keys"]):
            ref_idx = len(refs)
            refs.append((source, idx, _node(source, record)))
            for key in blocking_keys(values, idx):
                block_index[key].append(ref_idx)
    candidate_ids: set[tuple[int, int]] = set()
    for members in block_index.values():
        unique = sorted(set(members))
        for left, right in combinations(unique, 2):
            if _eligible(refs[left][0], refs[right][0], run.topology):
                candidate_ids.add((left, right))
    rows: list[dict[str, object]] = []
    for left_ref, right_ref in sorted(candidate_ids):
        source_l, li, node_l = refs[left_ref]
        source_r, ri, node_r = refs[right_ref]
        if node_r < node_l:
            source_l, source_r, li, ri, node_l, node_r = source_r, source_l, ri, li, node_r, node_l
        feats = _field_features(prepared[source_l], li, prepared[source_r], ri)
        address_l = "|".join((prepared[source_l][name][li] for name in ("street", "city", "state", "zip")))
        address_r = "|".join((prepared[source_r][name][ri] for name in ("street", "city", "state", "zip")))
        row: dict[str, object] = {
            "node_l": node_l, "node_r": node_r, "source_l": source_l, "source_r": source_r,
            "record_l": node_l.split("::", 1)[1], "record_r": node_r.split("::", 1)[1],
            "is_match": (node_l, node_r) in run.true_pairs,
            "co_resident_nonmatch": bool(address_l.replace("|", "") and address_l == address_r and (node_l, node_r) not in run.true_pairs),
        }
        row.update(dict(zip(FEATURES, feats)))
        row["baseline_score"] = float(np.dot(BASELINE_WEIGHTS, np.asarray(feats)))
        rows.append(row)
    return pd.DataFrame(rows), time.perf_counter() - started


def pair_metrics(scores: np.ndarray, labels: np.ndarray, threshold: float, true_count: int) -> dict[str, float | int]:
    predicted = scores >= threshold
    tp = int(np.logical_and(predicted, labels).sum())
    fp = int(np.logical_and(predicted, ~labels).sum())
    fn = int(true_count - tp)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / true_count if true_count else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"tp": tp, "fp": fp, "fn": fn, "predicted": tp + fp, "precision": precision, "recall": recall, "f1": f1}


def threshold_curve(scores: np.ndarray, labels: np.ndarray, true_count: int) -> pd.DataFrame:
    order = np.argsort(-scores, kind="stable")
    ordered_scores, ordered_labels = scores[order], labels[order].astype(np.int64)
    cumulative_tp = np.cumsum(ordered_labels)
    positions = np.r_[np.where(np.diff(ordered_scores) != 0)[0], len(scores) - 1] if len(scores) else np.asarray([], dtype=int)
    rows = [{"threshold": float("inf"), "predicted": 0, "precision": 1.0, "recall": 0.0, "f1": 0.0}]
    for pos in positions:
        predicted, tp = int(pos + 1), int(cumulative_tp[pos])
        precision = tp / predicted
        recall = tp / true_count if true_count else 0.0
        f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
        rows.append({"threshold": float(ordered_scores[pos]), "predicted": predicted, "precision": precision, "recall": recall, "f1": f1})
    return pd.DataFrame(rows)


def oracle_from_curve(curve: pd.DataFrame) -> dict[str, float]:
    finite = curve[np.isfinite(curve.threshold)].copy()
    best = finite.sort_values(["f1", "precision", "threshold"], ascending=False).iloc[0]
    return {"oracle_threshold": float(best.threshold), "oracle_precision": float(best.precision), "oracle_recall": float(best.recall), "oracle_f1": float(best.f1)}


class _UnionFind:
    def __init__(self, nodes: Iterable[str]):
        self.parent = {node: node for node in nodes}

    def find(self, node: str) -> str:
        root = node
        while self.parent[root] != root:
            root = self.parent[root]
        while node != root:
            parent = self.parent[node]
            self.parent[node] = root
            node = parent
        return root

    def union(self, left: str, right: str) -> None:
        lroot, rroot = self.find(left), self.find(right)
        if lroot != rroot:
            self.parent[rroot] = lroot


def b3_cluster_metrics(run: RunData, predicted_edges: Iterable[tuple[str, str]]) -> dict[str, float | int]:
    """Compute B-cubed without materializing every transitive closure pair.

    This is equivalent to the B-cubed portion of :func:`cluster_metrics`, but
    remains linear in the number of nodes after union-find construction.  It is
    intended for external matchers that may create very large components.
    """

    truth_by_node = dict(zip(run.nodes.node, run.nodes.person))
    uf = _UnionFind(run.nodes.node)
    for left, right in predicted_edges:
        uf.union(left, right)
    pred_by_node = {node: uf.find(node) for node in run.nodes.node}
    pred_sizes: dict[str, int] = defaultdict(int)
    truth_sizes: dict[str, int] = defaultdict(int)
    intersections: dict[tuple[str, str], int] = defaultdict(int)
    for node in run.nodes.node:
        predicted = pred_by_node[node]
        truth = truth_by_node[node]
        pred_sizes[predicted] += 1
        truth_sizes[truth] += 1
        intersections[(predicted, truth)] += 1
    precision_sum = 0.0
    recall_sum = 0.0
    for node in run.nodes.node:
        predicted = pred_by_node[node]
        truth = truth_by_node[node]
        overlap = intersections[(predicted, truth)]
        precision_sum += overlap / pred_sizes[predicted]
        recall_sum += overlap / truth_sizes[truth]
    count = len(run.nodes)
    precision = precision_sum / count if count else 0.0
    recall = recall_sum / count if count else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "b3_precision": precision,
        "b3_recall": recall,
        "b3_f1": f1,
        "predicted_clusters": len(pred_sizes),
        "truth_clusters": len(truth_sizes),
    }


def cluster_metrics(run: RunData, predicted_edges: Iterable[tuple[str, str]]) -> dict[str, float | int]:
    source_by_node = dict(zip(run.nodes.node, run.nodes.source))
    truth_by_node = dict(zip(run.nodes.node, run.nodes.person))
    uf = _UnionFind(run.nodes.node)
    for left, right in predicted_edges:
        uf.union(left, right)
    pred_by_node = {node: uf.find(node) for node in run.nodes.node}
    pred_members: dict[str, set[str]] = defaultdict(set)
    truth_members: dict[str, set[str]] = defaultdict(set)
    for node in run.nodes.node:
        pred_members[pred_by_node[node]].add(node)
        truth_members[truth_by_node[node]].add(node)
    b3_p: list[float] = []
    b3_r: list[float] = []
    for node in run.nodes.node:
        pred = pred_members[pred_by_node[node]]
        truth = truth_members[truth_by_node[node]]
        overlap = len(pred & truth)
        b3_p.append(overlap / len(pred))
        b3_r.append(overlap / len(truth))
    precision, recall = float(np.mean(b3_p)), float(np.mean(b3_r))
    b3_f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    closure: set[tuple[str, str]] = set()
    for members in pred_members.values():
        for left, right in combinations(sorted(members), 2):
            if _eligible(source_by_node[left], source_by_node[right], run.topology):
                closure.add((left, right))
    tp = len(closure & run.true_pairs)
    fp = len(closure - run.true_pairs)
    fn = len(run.true_pairs - closure)
    pair_p = tp / (tp + fp) if tp + fp else 0.0
    pair_r = tp / (tp + fn) if tp + fn else 0.0
    pair_f1 = 2 * pair_p * pair_r / (pair_p + pair_r) if pair_p + pair_r else 0.0
    return {
        "closure_tp": tp, "closure_fp": fp, "closure_fn": fn,
        "closure_precision": pair_p, "closure_recall": pair_r, "closure_f1": pair_f1,
        "b3_precision": precision, "b3_recall": recall, "b3_f1": b3_f1,
        "predicted_clusters": len(pred_members), "truth_clusters": len(truth_members),
    }
