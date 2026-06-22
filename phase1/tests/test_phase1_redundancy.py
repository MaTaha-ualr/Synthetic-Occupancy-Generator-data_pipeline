from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from sog_phase1.redundancy import allocate_records_per_entity, summarize_records_per_entity


def test_default_redundancy_config_exercises_full_configured_range() -> None:
    config_path = PROJECT_ROOT / "configs" / "phase1.yaml"
    with config_path.open("r", encoding="utf-8") as handle:
        phase1 = yaml.safe_load(handle)["phase1"]

    redundancy = phase1["redundancy"]
    min_records = int(redundancy["min_records_per_entity"])
    max_records = int(redundancy["max_records_per_entity"])
    rng = np.random.default_rng(int(phase1["seed"]))

    counts = allocate_records_per_entity(
        n_entities=int(phase1["n_people"]),
        n_records=int(phase1["n_records"]),
        min_records_per_entity=min_records,
        max_records_per_entity=max_records,
        shape=str(redundancy["shape"]),
        heavy_tail_alpha=float(redundancy["heavy_tail_alpha"]),
        rng=rng,
    )
    stats = summarize_records_per_entity(counts)

    assert counts.sum() == int(phase1["n_records"])
    assert stats.min_records_per_entity == min_records
    assert stats.max_records_per_entity == max_records
    assert set(stats.distribution) == set(range(min_records, max_records + 1))
