"""Read-only structural and provenance validation for paper_revision_bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import zipfile
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
BUNDLE = REPO / "paper_revision_bundle"
REQUIRED = (
    "artifact_manifest.json",
    "matcher_protocol.json",
    "config_mapping.csv",
    "derived_claims.json",
    "paired_statistics.csv",
    "oracle_bootstrap.json",
    "noise_trend.csv",
    "cluster_metrics.csv",
    "household_transfer_experiment.csv",
    "household_full_matrix.csv",
    "runtime_results.csv",
    "public_target_conversion.csv",
    "README.md",
)
TABLE_STEMS = (
    "table_i",
    "table_ii",
    "table_iii",
    "table_iv",
    "table_v",
    "table_vi",
    "figure_2_data",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=REPO,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--release-tag",
        help=(
            "Require an annotated tag at clean HEAD and validate the recorded "
            "payload-commit ancestry."
        ),
    )
    args = parser.parse_args()
    for name in REQUIRED:
        require((BUNDLE / name).is_file(), f"missing required bundle file: {name}")
    for stem in TABLE_STEMS:
        for extension in (".csv", ".tex", ".docx"):
            path = BUNDLE / "paper_tables" / f"{stem}{extension}"
            require(path.is_file() and path.stat().st_size > 0, f"missing table: {path}")
            if extension == ".docx":
                require(zipfile.is_zipfile(path), f"invalid DOCX container: {path}")
                with zipfile.ZipFile(path) as archive:
                    require(
                        archive.testzip() is None
                        and "word/document.xml" in archive.namelist(),
                        f"invalid DOCX payload: {path}",
                    )

    manifest = json.loads((BUNDLE / "artifact_manifest.json").read_text(encoding="utf-8"))
    protocol = json.loads((BUNDLE / "matcher_protocol.json").read_text(encoding="utf-8"))
    claims = json.loads((BUNDLE / "derived_claims.json").read_text(encoding="utf-8"))
    bootstrap = json.loads((BUNDLE / "oracle_bootstrap.json").read_text(encoding="utf-8"))
    require(manifest["tests"]["failed"] == 0, "test manifest reports failures")
    require(manifest["tests"]["passed"] > 0, "test manifest has no passing tests")
    require(manifest["tests"]["time_seconds"] > 0, "test duration missing")
    require(manifest["git"]["branch"] == "main", "bundle was not built from main")
    require(manifest["git"]["commit_sha"], "missing full commit SHA")
    require(manifest["git"]["release_tag"], "missing release tag")
    if args.release_tag:
        release = manifest["git"].get("release", {})
        require(
            manifest["git"]["release_tag"] == args.release_tag,
            "manifest release tag does not match requested release tag",
        )
        require(
            release.get("tag") == args.release_tag,
            "release metadata tag does not match requested release tag",
        )
        require(
            release.get("final_commit_sha_embedded") is False,
            "release metadata must document the Git commit self-reference constraint",
        )
        require(
            release.get("expected_post_tag_worktree_status") == "clean",
            "release metadata does not require a clean worktree",
        )
        tag_ref = f"refs/tags/{args.release_tag}"
        require(
            run_git("cat-file", "-t", tag_ref) == "tag",
            f"release tag is not annotated: {args.release_tag}",
        )
        head = run_git("rev-parse", "HEAD")
        tagged_commit = run_git("rev-list", "-n", "1", args.release_tag)
        require(
            tagged_commit == head,
            f"release tag resolves to {tagged_commit}, not current HEAD {head}",
        )
        payload_commit = release.get("payload_commit_sha")
        require(payload_commit, "release payload commit is missing")
        require(
            run_git("rev-parse", f"{payload_commit}^{{commit}}") == payload_commit,
            "release payload commit is not a full commit SHA",
        )
        ancestor = subprocess.run(
            ["git", "merge-base", "--is-ancestor", payload_commit, head],
            cwd=REPO,
            text=True,
            capture_output=True,
        )
        require(
            ancestor.returncode == 0,
            "recorded payload commit is not an ancestor of the tagged release commit",
        )
        require(
            not run_git("status", "--porcelain=v1"),
            "release validation requires a clean worktree",
        )
        tracked_paths = [
            "paper_experiments/build_paper_revision_bundle.py",
            "paper_experiments/run_household_transfer.py",
            "paper_experiments/validate_paper_revision_bundle.py",
            "paper_revision_bundle/artifact_manifest.json",
            *[
                record["path"]
                for record in manifest["bundle_artifacts_excluding_manifest"]
            ],
        ]
        tracked = subprocess.run(
            ["git", "ls-files", "--error-unmatch", "--", *tracked_paths],
            cwd=REPO,
            text=True,
            capture_output=True,
        )
        require(
            tracked.returncode == 0,
            "release scripts or bundle outputs are not tracked:\n"
            f"{tracked.stdout}{tracked.stderr}",
        )
    require(
        protocol["baseline"]["all_training_and_calibration_data_disjoint_from_evaluation_seeds"],
        "baseline separation confirmation false",
    )
    require(
        protocol["splink"]["all_training_and_calibration_data_disjoint_from_evaluation_seeds"],
        "Splink separation confirmation false",
    )
    require(
        protocol["learned"]["all_training_and_calibration_data_disjoint_from_evaluation_seeds"],
        "learned separation confirmation false",
    )
    require(
        len(claims["matcher_spread_all_fourteen_scenarios"]["scenarios"]) == 14,
        "derived matcher spread does not contain 14 scenarios",
    )
    require(bootstrap["replicates"] >= 10_000, "bootstrap has fewer than 10,000 replicates")

    mapping = pd.read_csv(BUNDLE / "config_mapping.csv")
    require(len(mapping) == 230, f"config mapping expected 230 rows, found {len(mapping)}")
    require(mapping["yaml_sha256"].str.fullmatch(r"[0-9a-f]{64}").all(), "invalid YAML hash")
    require(
        mapping["truth_layer_sha256"].str.fullmatch(r"[0-9a-f]{64}").all(),
        "invalid truth-layer hash",
    )
    for current in mapping.itertuples(index=False):
        path = REPO / current.yaml_path
        require(path.is_file(), f"mapped YAML absent: {path}")
        require(sha256_file(path) == current.yaml_sha256, f"mapped YAML hash drift: {path}")

    paired = pd.read_csv(BUNDLE / "paired_statistics.csv")
    require(
        len(paired[paired["comparison_id"] == "E1_clean_minus_high_noise"]) == 11,
        "E1 paired rows incomplete",
    )
    e2_summaries = paired[
        (paired["row_type"] == "summary")
        & paired["comparison_id"].str.startswith("E2_")
    ]
    require(len(e2_summaries) == 28, "expected 28 E2 contrast summaries")
    require(
        e2_summaries["paired_t_holm_p_value"].notna().all(),
        "Holm-adjusted t p-values missing",
    )

    noise = pd.read_csv(BUNDLE / "noise_trend.csv")
    require(
        len(noise[noise["row_type"] == "per_matcher_seed_level"]) == 150,
        "noise detail rows incomplete",
    )
    require(
        len(noise[noise["row_type"] == "matcher_summary"]) == 3,
        "noise summary rows incomplete",
    )

    cluster = pd.read_csv(BUNDLE / "cluster_metrics.csv")
    require(
        len(cluster[cluster["row_type"] == "per_seed"]) == 540,
        "cluster per-seed rows incomplete",
    )
    require(
        len(cluster[cluster["row_type"] == "summary"]) == 54,
        "cluster summary rows incomplete",
    )

    transfer = pd.read_csv(BUNDLE / "household_transfer_experiment.csv")
    require(
        len(transfer[transfer["row_type"] == "per_seed"]) == 60,
        "household transfer per-seed rows incomplete",
    )
    require(
        len(transfer[transfer["row_type"] == "paired_delta_summary"]) == 3,
        "household transfer paired summaries incomplete",
    )
    require(
        transfer.loc[
            transfer["row_type"] == "per_seed", "evaluation_seed_disjoint"
        ].astype(bool).all(),
        "household transfer uses calibration seed in evaluation",
    )

    full = pd.read_csv(BUNDLE / "household_full_matrix.csv")
    require(
        len(full[full["row_type"] == "per_seed"]) == 180,
        "household full matrix per-seed rows incomplete",
    )
    require(
        len(full[full["row_type"] == "arm_summary"]) == 18,
        "household full matrix summaries incomplete",
    )

    runtime = pd.read_csv(BUNDLE / "runtime_results.csv")
    require(len(runtime) == 8, "runtime file should have four E1 and four E8 rows")
    public = pd.read_csv(BUNDLE / "public_target_conversion.csv")
    require(len(public) == 27, "public target conversion should cover 27 E7 dimensions")
    require(
        public["achieved_per_seed_json"].map(lambda value: len(json.loads(value)) == 10).all(),
        "public target per-seed results incomplete",
    )

    for record in manifest["bundle_artifacts_excluding_manifest"]:
        path = REPO / record["path"]
        require(path.is_file(), f"manifested output absent: {path}")
        require(sha256_file(path) == record["sha256"], f"manifested output hash drift: {path}")

    diff_check = subprocess.run(
        ["git", "diff", "--check"], cwd=REPO, text=True, capture_output=True
    )
    require(diff_check.returncode == 0, f"git diff --check failed:\n{diff_check.stdout}{diff_check.stderr}")
    print(
        "paper_revision_bundle validation passed: "
        f"{len(mapping)} config mappings, {manifest['tests']['passed']} tests, "
        f"{len(manifest['bundle_artifacts_excluding_manifest'])} hashed outputs"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
