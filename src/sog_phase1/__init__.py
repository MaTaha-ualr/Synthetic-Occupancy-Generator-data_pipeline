"""Phase-1 synthetic people + address pipeline."""

from .preprocess import build_prepared_cache
from .generator import generate_phase1_dataset

__all__ = ["build_prepared_cache", "generate_phase1_dataset"]
