from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class RedundancyStats:
    min_records_per_entity: int
    max_records_per_entity: int
    mean_records_per_entity: float
    distribution: dict[int, int]


def allocate_records_per_entity(
    *,
    n_entities: int,
    n_records: int,
    min_records_per_entity: int,
    max_records_per_entity: int,
    shape: str,
    heavy_tail_alpha: float,
    rng: np.random.Generator,
) -> np.ndarray:
    if n_entities <= 0:
        raise ValueError("n_entities must be > 0")
    if n_records <= 0:
        raise ValueError("n_records must be > 0")
    if min_records_per_entity < 1:
        raise ValueError("min_records_per_entity must be >= 1")
    if max_records_per_entity < min_records_per_entity:
        raise ValueError("max_records_per_entity must be >= min_records_per_entity")

    base_total = n_entities * min_records_per_entity
    max_total = n_entities * max_records_per_entity
    if n_records < base_total or n_records > max_total:
        raise ValueError(
            "n_records is outside feasible range for the configured per-entity bounds: "
            f"[{base_total}, {max_total}]"
        )

    counts = np.full(n_entities, min_records_per_entity, dtype=np.int64)
    remaining = int(n_records - base_total)
    if remaining == 0:
        return counts

    capacities = np.full(
        n_entities, max_records_per_entity - min_records_per_entity, dtype=np.int64
    )
    mode = str(shape).strip().lower()
    if mode == "balanced":
        _allocate_balanced(counts, capacities, remaining, rng)
    elif mode == "heavy_tail":
        _allocate_heavy_tail(counts, capacities, remaining, heavy_tail_alpha, rng)
    else:
        raise ValueError("redundancy.shape must be one of: balanced, heavy_tail")

    return counts


def summarize_records_per_entity(counts: np.ndarray) -> RedundancyStats:
    if len(counts) == 0:
        return RedundancyStats(
            min_records_per_entity=0,
            max_records_per_entity=0,
            mean_records_per_entity=0.0,
            distribution={},
        )
    unique, freqs = np.unique(counts.astype(np.int64), return_counts=True)
    dist = {int(k): int(v) for k, v in zip(unique, freqs)}
    return RedundancyStats(
        min_records_per_entity=int(counts.min()),
        max_records_per_entity=int(counts.max()),
        mean_records_per_entity=float(counts.mean()),
        distribution=dist,
    )


def _allocate_balanced(
    counts: np.ndarray,
    capacities: np.ndarray,
    remaining: int,
    rng: np.random.Generator,
) -> None:
    eligible = np.where(capacities > 0)[0]
    if len(eligible) == 0:
        return

    rng.shuffle(eligible)
    cursor = 0
    while remaining > 0 and len(eligible) > 0:
        idx = int(eligible[cursor])
        counts[idx] += 1
        capacities[idx] -= 1
        remaining -= 1

        cursor += 1
        if cursor >= len(eligible):
            eligible = np.where(capacities > 0)[0]
            if len(eligible) == 0:
                break
            rng.shuffle(eligible)
            cursor = 0


def _allocate_heavy_tail(
    counts: np.ndarray,
    capacities: np.ndarray,
    remaining: int,
    heavy_tail_alpha: float,
    rng: np.random.Generator,
) -> None:
    if heavy_tail_alpha <= 0:
        raise ValueError("redundancy.heavy_tail_alpha must be > 0")

    n_entities = len(counts)
    rank_order = np.arange(n_entities, dtype=np.int64)
    rng.shuffle(rank_order)
    ranks = np.empty(n_entities, dtype=np.int64)
    ranks[rank_order] = np.arange(1, n_entities + 1, dtype=np.int64)
    base_weights = 1.0 / np.power(ranks.astype(float), float(heavy_tail_alpha))

    while remaining > 0:
        eligible = np.where(capacities > 0)[0]
        if len(eligible) == 0:
            break
        weights = base_weights[eligible]
        weights = weights / float(weights.sum())
        picked = int(rng.choice(eligible, p=weights))
        counts[picked] += 1
        capacities[picked] -= 1
        remaining -= 1
