from pathlib import Path

from evaluation.geco_adapter import (
    GECO_ARCHIVE_SHA256,
    generate_geco_run,
    mean_true_pair_features,
    port_geco_archive,
    sha256_file,
)


REPO = Path(__file__).resolve().parents[2]
ARCHIVE = REPO / "paper_experiments/vendor/geco/geco-data-generator-corruptor.tar.gz"


def test_pinned_archive_and_small_generation(tmp_path):
    assert sha256_file(ARCHIVE) == GECO_ARCHIVE_SHA256
    ported = port_geco_archive(ARCHIVE, tmp_path)
    assert "corruptor.py" in ported.patch_text
    run = generate_geco_run(
        ported.source_dir,
        seed=1234,
        modifications_per_record=2,
        matched_originals=20,
        duplicate_records=35,
        unmatched_b_records=3,
    )
    assert len(run.datasets["A"]) == 20
    assert len(run.datasets["B"]) == 38
    assert len(run.true_pairs) == 35
    assert mean_true_pair_features(run).shape == (7,)


def test_geco_generation_is_deterministic(tmp_path):
    ported = port_geco_archive(ARCHIVE, tmp_path)
    first = generate_geco_run(
        ported.source_dir, seed=99, modifications_per_record=1,
        matched_originals=12, duplicate_records=18, unmatched_b_records=2,
    )
    second = generate_geco_run(
        ported.source_dir, seed=99, modifications_per_record=1,
        matched_originals=12, duplicate_records=18, unmatched_b_records=2,
    )
    assert first.datasets["A"].equals(second.datasets["A"])
    assert first.datasets["B"].equals(second.datasets["B"])
    assert first.true_pairs == second.true_pairs
