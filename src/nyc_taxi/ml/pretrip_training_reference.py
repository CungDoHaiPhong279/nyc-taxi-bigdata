"""Read-only guards for the single-loss V2.1 experiment against locked V2.

The caller must first verify the complete reference bundle manifest. This
module then reads only manifest-listed, bounded metadata and two StringIndexer
Parquets. It never discovers or opens source/sample/holdout paths and imports
neither Spark nor production inference.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import stat
from typing import Any, Mapping

from nyc_taxi.ml.pretrip_features import CATEGORICAL_FEATURE_COLUMNS, FEATURE_COLUMNS
from nyc_taxi.ml.pretrip_training_model import BASELINE_POLICY, GBT_PARAMETERS


REFERENCE_MODEL_TREE_SHA256 = "2efee5c2c25769430078f15d9d2ab10db9022cba6fe9ee92dbe7559c8566bd05"
REFERENCE_SAMPLE_IDENTITY_SHA256 = "7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86"
REFERENCE_COUNTS = {"train": 26573, "validation": 23526}
REFERENCE_BASELINE_MAE_USD = 5.1312024993624075
REFERENCE_GATE_THRESHOLD_USD = 4.618082249426167
MAX_REFERENCE_MEMBER_BYTES = 2 * 1024 * 1024


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _members(manifest: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    if (manifest.get("version") != "pretrip-v2-training-artifacts-1"
            or manifest.get("model_tree_sha256") != REFERENCE_MODEL_TREE_SHA256
            or any(manifest.get(name) is not False for name in ("holdout_opened", "promoted", "deployed"))):
        raise ValueError("v21_reference_manifest_identity_drift")
    members = {}
    for entry in manifest.get("files", ()):
        name = entry.get("path")
        if not isinstance(name, str):
            raise ValueError("v21_reference_manifest_path_invalid")
        relative = PurePosixPath(name)
        if (not name or "\\" in name or ":" in name or relative.is_absolute()
                or any(part in ("", ".", "..") for part in name.split("/"))
                or relative.as_posix() != name or name in members):
            raise ValueError("v21_reference_manifest_path_invalid")
        members[name] = entry
    return members


def _member(root: Path, members: Mapping[str, Any], name: str) -> Path:
    """Hash only an explicitly selected member; reject symlinks/junctions."""
    entry = members.get(name)
    if entry is None:
        raise ValueError("v21_reference_required_member_missing")
    expected_bytes = entry.get("bytes")
    if (type(expected_bytes) is not int or not 0 <= expected_bytes <= MAX_REFERENCE_MEMBER_BYTES
            or not isinstance(entry.get("sha256"), str) or len(entry["sha256"]) != 64):
        raise ValueError("v21_reference_member_limit_or_hash_invalid")
    path = root
    for part in (None, *PurePosixPath(name).parts):
        if part is not None:
            path = path / part
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
            raise ValueError("v21_reference_link_rejected")
    if not stat.S_ISREG(info.st_mode) or info.st_size != expected_bytes:
        raise ValueError("v21_reference_member_size_mismatch")
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            checksum.update(block)
    if checksum.hexdigest() != entry["sha256"]:
        raise ValueError("v21_reference_member_hash_mismatch")
    return path


def _json(root: Path, members: Mapping[str, Any], name: str) -> Any:
    return json.loads(_member(root, members, name).read_text(encoding="utf-8"))


def _single_stage_member(members: Mapping[str, Any], stage: int, kind: str, extension: str) -> str:
    prefix = f"pipeline/stages/{stage}_"
    matches = [name for name in members
               if name.startswith(prefix) and len(PurePosixPath(name).parts) == 5
               and PurePosixPath(name).parts[3] == kind
               and PurePosixPath(name).name.startswith("part-") and name.endswith(extension)]
    if len(matches) != 1:
        raise ValueError("v21_reference_stage_member_not_unique")
    return matches[0]


def _labels(root: Path, members: Mapping[str, Any], stage: int) -> list[str]:
    import pyarrow.parquet as pq

    name = _single_stage_member(members, stage, "data", ".parquet")
    # ParquetFile reads this exact file, without dataset discovery/partitioning.
    table = pq.ParquetFile(_member(root, members, name)).read()
    if table.column_names != ["labelsArray"] or table.num_rows != 1:
        raise ValueError("v21_reference_indexer_schema_drift")
    arrays = table.column("labelsArray").to_pylist()[0]
    if not isinstance(arrays, list) or len(arrays) != 1 or not isinstance(arrays[0], list):
        raise ValueError("v21_reference_indexer_labels_invalid")
    labels = arrays[0]
    if (not labels or any(not isinstance(value, str) or not value.isascii() or not value.isdigit()
                          or str(int(value)) != value or not 1 <= int(value) <= 263 for value in labels)
            or labels != sorted(set(labels))):
        raise ValueError("v21_reference_indexer_dictionary_drift")
    return labels


def read_reference(root: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    """Read the pinned squared bundle after the caller's full manifest check.

    Returned data are in-memory metadata only. No August file, sample or raw
    Parquet is opened, even when preparation lineage mentions an August hash.
    """
    root = Path(root).absolute()
    members = _members(manifest)
    config = _json(root, members, "training_config.json")
    lineage = _json(root, members, "sample_lineage.json")
    evaluation = _json(root, members, "metrics.json")
    baseline = _json(root, members, "baselines.json")
    if (config.get("gbt_parameters") != GBT_PARAMETERS
            or config.get("gbt_parameters", {}).get("lossType") != "squared"
            or config.get("baseline_policy") != BASELINE_POLICY
            or config.get("features") != list(FEATURE_COLUMNS)
            or config.get("holdout_opened") is not False):
        raise ValueError("v21_reference_configuration_drift")
    if (lineage.get("sample_identity_sha256") != REFERENCE_SAMPLE_IDENTITY_SHA256
            or config.get("sample_identity_sha256") != REFERENCE_SAMPLE_IDENTITY_SHA256
            or lineage.get("sample_counts") != REFERENCE_COUNTS
            or lineage.get("feature_columns") != list(FEATURE_COLUMNS)
            or lineage.get("journey_overlap_train_validation") != 0
            or lineage.get("holdout_data_accessed") is not False
            or lineage.get("raw_sources_accessed") is not False):
        raise ValueError("v21_reference_sample_identity_drift")
    if (any(baseline.get(name) != value for name, value in BASELINE_POLICY.items())
            or baseline.get("train_count") != REFERENCE_COUNTS["train"]):
        raise ValueError("v21_reference_baseline_policy_drift")
    for split, count in REFERENCE_COUNTS.items():
        split_metrics = evaluation.get(split, {})
        if (set(split_metrics) != {"global_median", "hierarchical_median", "gbt"}
                or any(value.get("count") != count for value in split_metrics.values())):
            raise ValueError("v21_reference_metric_count_drift")
    validation = evaluation["validation"]
    best_mae = min(validation[name]["mae_usd"] for name in ("global_median", "hierarchical_median"))
    if (best_mae != REFERENCE_BASELINE_MAE_USD
            or validation["hierarchical_median"]["mae_usd"] != REFERENCE_BASELINE_MAE_USD
            or config.get("gate", {}).get("baseline_mae_usd") != REFERENCE_BASELINE_MAE_USD
            or config.get("gate", {}).get("required_mae_improvement_percent") != 10.0
            or config.get("gate", {}).get("pass") is not False
            or best_mae * 0.9 != REFERENCE_GATE_THRESHOLD_USD):
        raise ValueError("v21_reference_validation_gate_drift")
    gbt_metadata_name = _single_stage_member(members, 4, "metadata", ".txt")
    gbt_metadata = _json(root, members, gbt_metadata_name)
    if (not gbt_metadata.get("class", "").endswith(".GBTRegressionModel")
            or gbt_metadata.get("paramMap", {}).get("lossType") != "squared"):
        raise ValueError("v21_reference_effective_loss_drift")
    dictionaries = {name: _labels(root, members, index)
                    for index, name in enumerate(CATEGORICAL_FEATURE_COLUMNS)}
    return {"model_tree_sha256": REFERENCE_MODEL_TREE_SHA256,
            "sample_identity_sha256": REFERENCE_SAMPLE_IDENTITY_SHA256,
            "gbt_parameters": config["gbt_parameters"], "baseline_policy": config["baseline_policy"],
            "runtime": config["runtime"], "lineage": lineage,
            "baselines": baseline, "metrics": evaluation, "dictionaries": dictionaries,
            "dictionary_sha256": _digest(dictionaries),
            "baseline_validation_mae_usd": best_mae,
            "gate_threshold_mae_usd": REFERENCE_GATE_THRESHOLD_USD,
            "metadata_hashes": {name: members[name]["sha256"] for name in
                                ("training_config.json", "sample_lineage.json", "metrics.json", "baselines.json")}}


def verify_dictionaries(reference: Mapping[str, Any], labels_by_feature: Mapping[str, Any]) -> dict[str, Any]:
    """Check fitted train dictionaries without fitting or loading another model."""
    if set(labels_by_feature) != set(CATEGORICAL_FEATURE_COLUMNS):
        raise ValueError("v21_train_categorical_dictionary_drift")
    actual = {name: list(labels_by_feature[name]) for name in CATEGORICAL_FEATURE_COLUMNS}
    if actual != reference["dictionaries"]:
        raise ValueError("v21_train_categorical_dictionary_drift")
    return {"matches_locked_v2": True, "dictionary_sha256": _digest(actual),
            "category_counts": {name: len(values) for name, values in actual.items()}}


def verify_current(reference: Mapping[str, Any], loaded: Mapping[str, Any], baseline: Mapping[str, Any]) -> dict[str, Any]:
    """Require the identical Arrow-joined samples, train-only medians and zones."""
    if (loaded.get("lineage") != reference["lineage"]
            or {name: len(loaded[name]) for name in ("train", "validation")} != REFERENCE_COUNTS):
        raise ValueError("v21_current_sample_or_lineage_drift")
    if baseline != reference["baselines"]:
        raise ValueError("v21_current_train_baseline_drift")
    dictionaries = {name: sorted({row[name] for row in loaded["train"]})
                    for name in CATEGORICAL_FEATURE_COLUMNS}
    audit = verify_dictionaries(reference, dictionaries)
    return {"reference_model_tree_sha256": reference["model_tree_sha256"],
            "sample_identity_sha256": reference["sample_identity_sha256"],
            "sample_hashes_and_full_lineage_equal": True, "baseline_artifact_equal": True,
            "train_dictionary": audit, "baseline_validation_mae_usd": REFERENCE_BASELINE_MAE_USD,
            "gate_threshold_mae_usd": REFERENCE_GATE_THRESHOLD_USD,
            "holdout_data_accessed": False, "raw_sources_accessed": False,
            "overlap_scope": "train and validation retained samples only"}
