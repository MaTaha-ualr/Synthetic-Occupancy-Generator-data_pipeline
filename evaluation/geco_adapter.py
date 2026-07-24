"""Checksum-pinned adapter for the ANU GeCo generator/corruptor.

The upstream 2012 archive is Python 2.  This module performs a mechanical
``lib2to3`` conversion in a temporary directory, records the resulting unified
diff, and then uses GeCo's own GenerateDataSet and CorruptDataSet classes.  The
adapter supplies seven record-linkage fields through GeCo's documented
frequency/function extension points; it does not replace GeCo's generation or
corruption engine.
"""

from __future__ import annotations

import contextlib
import difflib
import hashlib
import importlib
import io
import random
import re
import subprocess
import sys
import tarfile
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from evaluation.baseline_matcher import prepare
from evaluation.metrics import FEATURES, RunData, _field_features

GECO_ARCHIVE_SHA256 = "676b143d208b44a306002c05fb2b17d1a21c40e1972d4be7db8b03c130d0ab0c"
GECO_SOURCE_URL = "https://dmm.anu.edu.au/geco/geco-data-generator-corruptor.tar.gz"
GECO_SOURCE_FILES = (
    "basefunctions.py",
    "attrgenfunct.py",
    "contdepfunct.py",
    "generator.py",
    "corruptor.py",
)
GECO_FIELDS = ("first", "last", "dob", "street", "city", "state", "postal")
OUTPUT_COLUMNS = (
    "RecordKey",
    "FirstName",
    "LastName",
    "DOB",
    "StreetAddress",
    "City",
    "State",
    "ZipCode",
)


@dataclass(frozen=True)
class PortedGeCo:
    source_dir: Path
    patch_text: str
    converter_stdout: str
    converter_stderr: str


class _DiscardText(io.TextIOBase):
    """A non-buffering sink for GeCo's record-by-record diagnostic prints."""

    def write(self, value: str) -> int:  # pragma: no cover - trivial sink
        return len(value)

    def flush(self) -> None:  # pragma: no cover - trivial sink
        return None


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_frame(frame: pd.DataFrame) -> str:
    """Hash a dataframe using a version-readable canonical CSV rendering."""

    payload = frame.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _safe_members(archive: tarfile.TarFile, destination: Path) -> Iterable[tarfile.TarInfo]:
    root = destination.resolve()
    for member in archive.getmembers():
        target = (destination / member.name).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise ValueError(f"unsafe GeCo archive member: {member.name}") from exc
        if member.issym() or member.islnk():
            raise ValueError(f"links are not accepted in GeCo archive: {member.name}")
        yield member


def port_geco_archive(archive_path: Path, work_dir: Path) -> PortedGeCo:
    """Verify, safely extract, mechanically port, and compile the GeCo source."""

    archive_path = archive_path.resolve()
    if sha256_file(archive_path) != GECO_ARCHIVE_SHA256:
        raise ValueError("GeCo archive SHA-256 does not match the preregistered source")
    extract_root = work_dir.resolve() / "extracted"
    extract_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:gz") as archive:
        for member in _safe_members(archive, extract_root):
            archive.extract(member, extract_root)
    source_dir = extract_root / "geco-data-generator-corruptor"
    originals = {
        name: (source_dir / name).read_text(encoding="utf-8")
        for name in GECO_SOURCE_FILES
    }
    command = [sys.executable, "-m", "lib2to3", "-w", "-n"] + [
        str(source_dir / name) for name in GECO_SOURCE_FILES
    ]
    converted = subprocess.run(command, text=True, capture_output=True, check=True)
    compile_result = subprocess.run(
        [sys.executable, "-m", "py_compile"] + [str(source_dir / name) for name in GECO_SOURCE_FILES],
        text=True,
        capture_output=True,
        check=True,
    )
    patch_lines: list[str] = []
    for name in GECO_SOURCE_FILES:
        revised = (source_dir / name).read_text(encoding="utf-8")
        patch_lines.extend(
            difflib.unified_diff(
                originals[name].splitlines(keepends=True),
                revised.splitlines(keepends=True),
                fromfile=f"a/geco-data-generator-corruptor/{name}",
                tofile=f"b/geco-data-generator-corruptor/{name}",
            )
        )
    return PortedGeCo(
        source_dir=source_dir,
        patch_text="".join(patch_lines),
        converter_stdout=converted.stdout,
        converter_stderr=converted.stderr + compile_result.stderr,
    )


def import_geco(source_dir: Path) -> tuple[Any, Any, Any]:
    """Import the mechanically converted upstream modules."""

    source = str(source_dir.resolve())
    if source not in sys.path:
        sys.path.insert(0, source)
    basefunctions = importlib.import_module("basefunctions")
    generator = importlib.import_module("generator")
    corruptor = importlib.import_module("corruptor")
    return basefunctions, generator, corruptor


def _record_number(record_id: str) -> int:
    match = re.match(r"rec-(\d+)-(?:org|dup-\d+)$", record_id)
    if not match:
        raise ValueError(f"unexpected GeCo record identifier: {record_id}")
    return int(match.group(1))


def _parent_number(record_id: str) -> int:
    return _record_number(record_id)


def _make_attribute_objects(source_dir: Path, generator: Any) -> list[Any]:
    lookup = source_dir / "lookup-files"
    cities = (
        "austin", "boston", "chicago", "dallas", "denver", "detroit",
        "houston", "miami", "nashville", "new york", "phoenix", "portland",
        "san diego", "san jose", "seattle", "tampa",
    )
    states = ("AZ", "CA", "CO", "FL", "IL", "MA", "MI", "NY", "TN", "TX", "WA")
    roads = (
        "ash", "cedar", "cherry", "elm", "highland", "hill", "lake",
        "lincoln", "main", "maple", "oak", "park", "pine", "river",
        "sunset", "walnut", "washington", "willow",
    )

    def birth_date() -> str:
        return f"{random.randint(1940, 2005):04d}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"

    def street_address() -> str:
        suffix = random.choice(("st", "ave", "rd", "blvd", "ln"))
        return f"{random.randint(1, 9999)} {random.choice(roads)} {suffix}"

    def city() -> str:
        return random.choice(cities)

    def state() -> str:
        return random.choice(states)

    return [
        generator.GenerateFreqAttribute(
            attribute_name="first", freq_file_name=str(lookup / "givenname_freq.csv"),
            has_header_line=False, unicode_encoding="ascii",
        ),
        generator.GenerateFreqAttribute(
            attribute_name="last", freq_file_name=str(lookup / "surname-freq.csv"),
            has_header_line=False, unicode_encoding="ascii",
        ),
        generator.GenerateFuncAttribute(attribute_name="dob", function=birth_date),
        generator.GenerateFuncAttribute(attribute_name="street", function=street_address),
        generator.GenerateFuncAttribute(attribute_name="city", function=city),
        generator.GenerateFuncAttribute(attribute_name="state", function=state),
        generator.GenerateFreqAttribute(
            attribute_name="postal", freq_file_name=str(lookup / "postcode_act_freq.csv"),
            has_header_line=False, unicode_encoding="ascii",
        ),
    ]


def generate_geco_run(
    source_dir: Path,
    *,
    seed: int,
    modifications_per_record: int,
    matched_originals: int = 5_499,
    duplicate_records: int = 10_460,
    unmatched_b_records: int = 551,
) -> RunData:
    """Generate the preregistered A/B arm through GeCo's upstream classes."""

    if modifications_per_record not in range(1, 6):
        raise ValueError("modifications_per_record must be in 1..5")
    basefunctions, generator, corruptor = import_geco(source_dir)
    total_originals = matched_originals + unmatched_b_records
    sink = _DiscardText()
    with contextlib.redirect_stdout(sink):
        random.seed(seed)
        attributes = _make_attribute_objects(source_dir, generator)
        data_generator = generator.GenerateDataSet(
            output_file_name="unused-geco-adapter.csv",
            write_header_line=True,
            rec_id_attr_name="rec-id",
            number_of_records=total_originals,
            attribute_name_list=list(GECO_FIELDS),
            attribute_data_list=attributes,
            unicode_encoding="ascii",
        )
        all_originals = data_generator.generate()
        ordered_ids = sorted(all_originals, key=_record_number)
        matched_ids = ordered_ids[:matched_originals]
        unmatched_ids = ordered_ids[matched_originals:]
        originals_to_corrupt = {record_id: all_originals[record_id][:] for record_id in matched_ids}

        edit_uniform = corruptor.CorruptValueEdit(
            position_function=corruptor.position_mod_uniform,
            char_set_funct=basefunctions.char_set_ascii,
            insert_prob=0.25,
            delete_prob=0.25,
            substitute_prob=0.25,
            transpose_prob=0.25,
        )
        edit_normal = corruptor.CorruptValueEdit(
            position_function=corruptor.position_mod_normal,
            char_set_funct=basefunctions.char_set_ascii,
            insert_prob=0.25,
            delete_prob=0.25,
            substitute_prob=0.25,
            transpose_prob=0.25,
        )
        missing = corruptor.CorruptMissingValue()
        attr_prob = {
            "first": 0.15,
            "last": 0.20,
            "dob": 0.15,
            "street": 0.20,
            "city": 0.10,
            "state": 0.05,
            "postal": 0.15,
        }
        attr_methods = {
            name: [(0.70, edit_uniform), (0.15, edit_normal), (0.15, missing)]
            for name in GECO_FIELDS
        }
        # A separate deterministic corruption substream makes the calibration
        # candidates share the same original population.
        random.seed(seed + 100_000)
        data_corruptor = corruptor.CorruptDataSet(
            number_of_org_records=matched_originals,
            number_of_mod_records=duplicate_records,
            attribute_name_list=list(GECO_FIELDS),
            max_num_dup_per_rec=3,
            num_dup_dist="zipf",
            max_num_mod_per_attr=1,
            num_mod_per_rec=modifications_per_record,
            attr_mod_prob_dict=attr_prob,
            attr_mod_data_dict=attr_methods,
        )
        corrupted = data_corruptor.corrupt_records(originals_to_corrupt)

    duplicate_ids = sorted(
        (record_id for record_id in corrupted if "-dup-" in record_id),
        key=lambda value: (_parent_number(value), int(value.rsplit("-", 1)[1])),
    )
    if len(duplicate_ids) != duplicate_records:
        raise RuntimeError(f"GeCo produced {len(duplicate_ids)} duplicates, expected {duplicate_records}")
    if len(unmatched_ids) != unmatched_b_records:
        raise RuntimeError("GeCo unmatched-record count differs from the adapter contract")

    def output_row(prefix: str, record_id: str, values: list[str]) -> dict[str, str]:
        return dict(zip(OUTPUT_COLUMNS, [f"{prefix}_{record_id}", *map(str, values)]))

    a_rows = [output_row("A", record_id, all_originals[record_id]) for record_id in matched_ids]
    b_rows = [output_row("B", record_id, corrupted[record_id]) for record_id in duplicate_ids]
    b_rows.extend(output_row("B", record_id, all_originals[record_id]) for record_id in unmatched_ids)
    datasets = {"A": pd.DataFrame(a_rows), "B": pd.DataFrame(b_rows)}

    entity_rows: list[dict[str, str]] = []
    for record_id in matched_ids:
        entity_rows.append({"PersonKey": f"G{_parent_number(record_id):05d}", "DatasetId": "A", "RecordKey": f"A_{record_id}"})
    for record_id in duplicate_ids:
        entity_rows.append({"PersonKey": f"G{_parent_number(record_id):05d}", "DatasetId": "B", "RecordKey": f"B_{record_id}"})
    for record_id in unmatched_ids:
        entity_rows.append({"PersonKey": f"GU{_parent_number(record_id):05d}", "DatasetId": "B", "RecordKey": f"B_{record_id}"})
    entity_map = pd.DataFrame(entity_rows)
    nodes = entity_map.rename(columns={"DatasetId": "source", "RecordKey": "record_key", "PersonKey": "person"})
    nodes["node"] = nodes.source + "::" + nodes.record_key
    nodes = nodes[["node", "source", "record_key", "person"]]
    true_pairs: set[tuple[str, str]] = set()
    for _, group in nodes.groupby("person", sort=False):
        for left, right in combinations(sorted(group.node.tolist()), 2):
            if left.split("::", 1)[0] != right.split("::", 1)[0]:
                true_pairs.add((left, right))
    if len(true_pairs) != duplicate_records:
        raise RuntimeError(f"GeCo truth contains {len(true_pairs)} links, expected {duplicate_records}")
    return RunData(
        run_dir=Path(f"geco_seed_{seed}"),
        scenario="geco_matched_analog",
        seed=seed,
        topology="link",
        datasets=datasets,
        entity_map=entity_map,
        nodes=nodes,
        true_pairs=true_pairs,
    )


def mean_true_pair_features(run: RunData) -> np.ndarray:
    """Mean the frozen E2 seven-field similarities over every truth edge."""

    prepared = {source: prepare(frame, source) for source, frame in run.datasets.items()}
    locations = {
        f"{source}::{record}": (source, index)
        for source, values in prepared.items()
        for index, record in enumerate(values["keys"])
    }
    rows: list[list[float]] = []
    for left_node, right_node in sorted(run.true_pairs):
        left_source, left_index = locations[left_node]
        right_source, right_index = locations[right_node]
        rows.append(
            _field_features(
                prepared[left_source], left_index,
                prepared[right_source], right_index,
            )
        )
    if not rows:
        return np.zeros(len(FEATURES), dtype=float)
    return np.asarray(rows, dtype=float).mean(axis=0)
