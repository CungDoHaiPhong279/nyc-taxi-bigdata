"""Separate, pinned August-only Arrow boundary for frozen-candidate evaluation.

Metadata verification never opens a Parquet file. The actual loader opens only
the two fixed test files, joining their verified Arrow order before Spark sees
any row. Preparation's overlap proof is reused explicitly, not re-created from
aggregate train/validation identities or misrepresented as a raw-data audit.
"""

from __future__ import annotations

from datetime import datetime
import hashlib
from itertools import zip_longest
import json
import math
from numbers import Real
from pathlib import Path
from typing import Any

from nyc_taxi.ml.pretrip_features import FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS
from nyc_taxi.ml import pretrip_training_data as common


DEFAULT_CONFIG = common.DEFAULT_CONFIG
EXPECTED_ROWS = 21_833
OVERLAP_SCOPE = "retained hash sample only; NOT full eligible cohort"


class HoldoutDataError(common.TrainingDataError):
    """Stable, non-sensitive fail-closed holdout verification code."""


def verify_holdout_metadata(
    sample_dir: Path, *, candidate_lineage: dict[str, Any],
    config_dir: Path = DEFAULT_CONFIG, expected_count: int = EXPECTED_ROWS,
) -> dict[str, Any]:
    """Verify pinned preparation JSON/config only, before freezing evaluation.

``expected_count`` is injectable solely for small synthetic test fixtures. The
evaluation CLI must keep its real-data count fixed at ``EXPECTED_ROWS``.
"""
    if type(expected_count) is not int or not 1 <= expected_count <= 50_000:
        raise HoldoutDataError("invalid_holdout_expected_count")
    directory = Path(sample_dir)
    identities, _ = common._identities(Path(config_dir))
    paths = (directory / "preparation_report.json", directory / "preparation_checkpoint.json")
    signatures = {path: common._signature(path) for path in paths}
    report, report_sha = common._metadata(paths[0])
    checkpoint, checkpoint_sha = common._metadata(paths[1])
    if (report_sha != candidate_lineage.get("preparation_report_sha256")
            or checkpoint_sha != candidate_lineage.get("preparation_checkpoint_sha256")):
        raise HoldoutDataError("candidate_preparation_metadata_identity_mismatch")
    common._verify_metadata(report, checkpoint, identities)
    for name, value in identities.items():
        if candidate_lineage.get(name) != value:
            raise HoldoutDataError("candidate_feature_or_timezone_identity_mismatch")
    if (candidate_lineage.get("feature_columns") != list(FEATURE_COLUMNS)
            or candidate_lineage.get("sampling") != report["sampling"]
            or candidate_lineage.get("preparation_input_lineage") != report.get("inputs")
            or candidate_lineage.get("holdout_data_accessed") is not False
            or candidate_lineage.get("raw_sources_accessed") is not False
            or candidate_lineage.get("journey_overlap_train_validation") != 0):
        raise HoldoutDataError("candidate_preparation_lineage_mismatch")
    candidate_hashes = candidate_lineage.get("sample_hashes")
    if (not isinstance(candidate_hashes, dict) or set(candidate_hashes) != {"train", "validation"}
            or candidate_lineage.get("sample_counts") != {
                name: report["counts"]["selected_rows"][name] for name in ("train", "validation")
            }):
        raise HoldoutDataError("candidate_train_validation_lineage_mismatch")
    for split in ("train", "validation"):
        if candidate_hashes[split] != {
            key: report["outputs"][split][key]
            for key in ("features_sha256", "labels_sha256", "journey_set_sha256")
        }:
            raise HoldoutDataError("candidate_train_validation_lineage_mismatch")
    prior_identity = hashlib.sha256(json.dumps(
        candidate_hashes, sort_keys=True, separators=(",", ":"),
    ).encode("ascii")).hexdigest()
    if candidate_lineage.get("sample_identity_sha256") != prior_identity:
        raise HoldoutDataError("candidate_train_validation_identity_mismatch")

    for document in (report, checkpoint):
        entry = document.get("outputs", {}).get("test")
        if (not isinstance(entry, dict) or entry.get("features") != "test_features.parquet"
                or entry.get("labels") != "test_labels.parquet"
                or type(entry.get("rows")) is not int or entry["rows"] != expected_count
                or type(document["counts"]["selected_rows"].get("test")) is not int
                or document["counts"]["selected_rows"]["test"] != expected_count
                or entry.get("row_alignment") != "identical row ordering; labels journey_key ascending"
                or not all(common._sha_string(entry.get(name)) for name in (
                    "features_sha256", "labels_sha256", "journey_set_sha256",
                ))):
            raise HoldoutDataError("holdout_output_manifest_mismatch")
        if document.get("split_policy", {}).get("test") != "2026-08-01 <= actual pickup < 2026-09-01":
            raise HoldoutDataError("holdout_actual_pickup_split_policy_mismatch")
        if (document.get("overlap_scope") != OVERLAP_SCOPE
                or document.get("overlap_counts") != {
                    "train_test": 0, "train_validation": 0, "validation_test": 0,
                }):
            raise HoldoutDataError("preparation_sample_overlap_proof_mismatch")
    if report["outputs"]["test"] != checkpoint["outputs"]["test"]:
        raise HoldoutDataError("holdout_report_checkpoint_mismatch")
    if any(common._signature(path) != signature for path, signature in signatures.items()):
        raise HoldoutDataError("preparation_metadata_changed_during_verification")
    return {
        "identities": identities,
        "preparation_report_sha256": report_sha,
        "preparation_checkpoint_sha256": checkpoint_sha,
        "expected_rows": expected_count,
        "test_output": report["outputs"]["test"],
        "sampling": report["sampling"], "split_policy": report["split_policy"],
        "candidate_train_validation_sample_identity_sha256": prior_identity,
        "preparation_input_lineage": report["inputs"],
        "preparation_overlap_proof": {
            "overlap_counts": report["overlap_counts"], "scope": OVERLAP_SCOPE,
            "verification": "reused pinned preparation evidence; no new raw or cross-split audit",
        },
        "holdout_parquet_opened": False,
    }


def _validate_august_row(
    feature_row: dict[str, Any], label_row: dict[str, Any],
    previous_key: str | None, calendar,
) -> tuple[dict[str, Any], str]:
    key = label_row["journey_key"]
    if not common._sha_string(key) or (previous_key is not None and key <= previous_key):
        raise HoldoutDataError("holdout_journey_keys_not_strictly_ascending_unique")
    digest = hashlib.sha256(f"42:{key}".encode("ascii")).digest()
    if int.from_bytes(digest, "big") % 1_000_000 >= 10_000:
        raise HoldoutDataError("holdout_journey_not_in_pinned_hash_sample")
    pickup = label_row["pickup_datetime_local"]
    if (not isinstance(pickup, datetime) or pickup.tzinfo is not None
            or pickup.year != 2026 or pickup.month != 8):
        raise HoldoutDataError("holdout_actual_pickup_window_mismatch")
    fare = label_row["fare_amount"]
    if isinstance(fare, bool) or not isinstance(fare, Real) or not math.isfinite(fare) or fare <= 0:
        raise HoldoutDataError("holdout_label_not_finite_positive")
    common._zone(feature_row["pickup_zone"])
    common._zone(feature_row["dropoff_zone"])
    # Independently rederive expected values, never call preparation's builder.
    hour, dow = 2 * math.pi * pickup.hour / 24, 2 * math.pi * pickup.weekday() / 7
    expected = (
        math.sin(hour), math.cos(hour), math.sin(dow), math.cos(dow),
        float(pickup.weekday() >= 5), float(calendar.is_holiday(pickup.date())),
    )
    for name, wanted in zip(NUMERIC_FEATURE_COLUMNS, expected, strict=True):
        value = feature_row[name]
        if (isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(value)
                or not math.isclose(value, wanted, rel_tol=1e-12, abs_tol=1e-12)):
            raise HoldoutDataError("holdout_feature_label_time_alignment_mismatch")
    return {**feature_row, "fare_amount": float(fare), "journey_key": key,
            "pickup_datetime_local": pickup}, key


def load_holdout_samples(
    sample_dir: Path, *, candidate_lineage: dict[str, Any],
    config_dir: Path = DEFAULT_CONFIG, expected_count: int = EXPECTED_ROWS,
    verified_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Load only the two pinned August files, pairing rows in Arrow order.

The evaluator must freeze ``verify_holdout_metadata``'s return value before this
call, then pass it as ``verified_metadata`` to detect metadata/config drift.
No fit, model, Spark, source file, train/validation sample or discovery exists.
"""
    import pyarrow.parquet as pq

    directory = Path(sample_dir)
    metadata = verify_holdout_metadata(
        directory, candidate_lineage=candidate_lineage, config_dir=config_dir,
        expected_count=expected_count,
    )
    if verified_metadata is not None and verified_metadata != metadata:
        raise HoldoutDataError("frozen_holdout_metadata_changed")
    identities, calendar = common._identities(Path(config_dir))
    feature_path, label_path = directory / "test_features.parquet", directory / "test_labels.parquet"
    signatures = {path: common._signature(path) for path in (feature_path, label_path)}
    actual_hashes = {"features_sha256": common._sha256(feature_path),
                     "labels_sha256": common._sha256(label_path)}
    entry = metadata["test_output"]
    if any(actual_hashes[name] != entry[name] for name in actual_hashes):
        raise HoldoutDataError("holdout_file_checksum_mismatch")
    feature_file = label_file = None
    try:
        feature_file, label_file = pq.ParquetFile(feature_path), pq.ParquetFile(label_path)
        common._validate_schemas(feature_file.schema_arrow, label_file.schema_arrow)
        if (feature_file.metadata.num_rows != expected_count
                or label_file.metadata.num_rows != expected_count):
            raise HoldoutDataError("holdout_row_count_mismatch")
        rows: list[dict[str, Any]] = []
        previous_key, key_hash, sentinel = None, hashlib.sha256(), object()
        for feature_row, label_row in zip_longest(
            common._iter_rows(feature_file), common._iter_rows(label_file), fillvalue=sentinel,
        ):
            if feature_row is sentinel or label_row is sentinel:
                raise HoldoutDataError("holdout_pair_row_alignment_mismatch")
            if len(rows) >= expected_count:
                raise HoldoutDataError("holdout_row_count_mismatch")
            row, key = _validate_august_row(feature_row, label_row, previous_key, calendar)
            if rows:
                key_hash.update(b"\n")
            key_hash.update(key.encode("ascii"))
            rows.append(row)
            previous_key = key
        if len(rows) != expected_count or key_hash.hexdigest() != entry["journey_set_sha256"]:
            raise HoldoutDataError("holdout_journey_set_or_count_mismatch")
    finally:
        if feature_file is not None:
            feature_file.close()
        if label_file is not None:
            label_file.close()
    if any(common._signature(path) != signature for path, signature in signatures.items()):
        raise HoldoutDataError("holdout_file_changed_during_loading")
    # Detect preparation/config races too; does not open any extra Parquet.
    if metadata != verify_holdout_metadata(
        directory, candidate_lineage=candidate_lineage, config_dir=config_dir,
        expected_count=expected_count,
    ):
        raise HoldoutDataError("frozen_holdout_metadata_changed")
    august_hashes = {**actual_hashes, "journey_set_sha256": key_hash.hexdigest()}
    identity = hashlib.sha256(json.dumps(
        {"test": august_hashes}, sort_keys=True, separators=(",", ":"),
    ).encode("ascii")).hexdigest()
    return {"test": rows, "lineage": {
        **identities, **metadata,
        "sample_counts": {"test": len(rows)}, "sample_hashes": {"test": august_hashes},
        "sample_identity_sha256": identity,
        "holdout_data_accessed": True, "holdout_parquet_opened": True,
        "raw_sources_accessed": False, "train_validation_samples_accessed": False,
        "row_alignment": "Arrow ordered zip before Spark; pinned file hashes and independent time checks",
        "overlap_scope": "reused pinned retained-sample preparation proof only; not rechecked raw/cohort",
    }}
