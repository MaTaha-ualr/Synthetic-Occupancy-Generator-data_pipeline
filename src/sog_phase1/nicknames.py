from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np


@dataclass(frozen=True)
class NicknamePool:
    names: np.ndarray
    probs: np.ndarray


@dataclass(frozen=True)
class NicknameCatalog:
    by_category: dict[str, dict[str, NicknamePool]]


def build_nickname_catalog(prepared_nicknames: dict[str, Any] | None) -> NicknameCatalog:
    if not prepared_nicknames:
        return NicknameCatalog(by_category={})
    raw_categories = prepared_nicknames.get("categories", {})
    by_category: dict[str, dict[str, NicknamePool]] = {}
    for category, entries in raw_categories.items():
        cat_key = str(category).strip().lower()
        pools: dict[str, NicknamePool] = {}
        if not isinstance(entries, dict):
            continue
        for formal_name, payload in entries.items():
            formal = _clean_name(formal_name)
            names = [str(v).strip().title() for v in payload.get("nicknames", [])]
            weights = [float(v) for v in payload.get("weights", [])]
            if not names or not weights or len(names) != len(weights):
                continue
            filtered_names: list[str] = []
            filtered_weights: list[float] = []
            for name, weight in zip(names, weights):
                if not name or weight <= 0:
                    continue
                if name == formal:
                    continue
                filtered_names.append(name)
                filtered_weights.append(weight)
            if not filtered_names:
                continue
            arr_weights = np.asarray(filtered_weights, dtype=float)
            probs = arr_weights / float(arr_weights.sum())
            pools[formal] = NicknamePool(
                names=np.asarray(filtered_names, dtype=object),
                probs=probs,
            )
        by_category[cat_key] = pools
    return NicknameCatalog(by_category=by_category)


def pick_display_first_name(
    *,
    formal_first_name: str,
    gender: str,
    use_nickname: bool,
    catalog: NicknameCatalog,
    rng: np.random.Generator,
) -> tuple[str, str]:
    formal = _clean_name(formal_first_name)
    if not use_nickname:
        return formal, "FORMAL"
    pool = _pool_for_formal_name(catalog, formal, gender)
    if pool is None:
        return formal, "FORMAL"
    picked = str(rng.choice(pool.names, p=pool.probs))
    if picked == formal:
        return formal, "FORMAL"
    return picked, "NICKNAME"


def _pool_for_formal_name(
    catalog: NicknameCatalog,
    formal_name: str,
    gender: str,
) -> NicknamePool | None:
    category_order = _gender_category_priority(gender)
    for category in category_order:
        by_formal = catalog.by_category.get(category, {})
        pool = by_formal.get(formal_name)
        if pool is not None:
            return pool
    return None


def _gender_category_priority(gender: str) -> list[str]:
    g = str(gender).strip().lower()
    if g == "female":
        return ["female", "unisex", "male"]
    if g == "male":
        return ["male", "unisex", "female"]
    return ["unisex", "female", "male"]


def _clean_name(value: str) -> str:
    return str(value).strip().title()


def resolve_nickname_source_dir(project_root: Path, configured_source_dir: str) -> Path:
    source = Path(configured_source_dir)
    if source.is_absolute():
        return source
    return (project_root / source).resolve()
