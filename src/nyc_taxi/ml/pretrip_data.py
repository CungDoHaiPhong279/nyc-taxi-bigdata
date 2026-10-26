"""Bounded, local-only preparation for the zone/time pretrip v2 pilot.

This module does not start Spark, fit preprocessing, contact a service, or read
Gold. Completed TLC records supply an offline label/cohort and stable journey
identity; none of those outcome fields become predictors. Hash sampling occurs
before deduplication, so duplicate journeys always share a sampling decision.
Only the retained sample receives deduplication/conflict/overlap checks: those
checks are deliberately NOT described as a full-cohort overlap audit.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import hashlib
import json
import math
from numbers import Real
from pathlib import Path
from typing import Any

from nyc_taxi.ml.pretrip_budget import ManagedBudgetExceeded, ManagedFileBudget


SOURCE_MONTHS = ("2026-06", "2026-07", "2026-08")
SPLITS = ("train", "validation", "test")
DEFAULT_CAPS = {"train": 100_000, "validation": 50_000, "test": 50_000}
READ_COLUMNS = (
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "PULocationID", "DOLocationID", "RatecodeID", "fare_amount",
)
IDENTITY_VERSION = "pretrip-v2-journey-sha256-1"
SAMPLE_METHOD = "sha256(seed + ':' + journey_key) modulo 1000000 < 10000"
COHORT_STAGES = (
    "valid_local_pickup", "pickup_in_pilot_window", "finite_positive_fare",
    "standard_final_ratecode_1", "named_zones_1_263",
    "valid_completed_journey_identity",
)


class PreparationError(ValueError):
    """An explicit preparation stop; existing outputs are never removed."""


def _positive_integer(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    if not math.isfinite(value) or int(value) != value or value <= 0:
        return None
    return int(value)


def _local_timestamp(value: Any) -> bool:
    return isinstance(value, datetime) and value.tzinfo is None


def split_for_pickup(pickup_datetime: datetime) -> str | None:
    """Split on actual TLC NYC wall time, never on the source filename.

    Aware values are rejected instead of silently converting an instant to a
    different wall clock. June/July/August 2026 are train/validation/test.
    """
    if not _local_timestamp(pickup_datetime):
        raise PreparationError("pickup must be a naive TLC local datetime")
    boundaries = (
        (datetime(2026, 6, 1), datetime(2026, 7, 1), "train"),
        (datetime(2026, 7, 1), datetime(2026, 8, 1), "validation"),
        (datetime(2026, 8, 1), datetime(2026, 9, 1), "test"),
    )
    return next((name for start, end, name in boundaries
                 if start <= pickup_datetime < end), None)


def journey_key(row: Mapping[str, Any]) -> str:
    """Stable completed-trip identity, excluding labels/load IDs/source files.

    Dropoff is allowed only in this OFFLINE identity, not in the feature dict.
    An identity cannot distinguish coincident trips with these same fields;
    conflicting eligible labels therefore cause a stop, not arbitrary dedup.
    """
    vendor = _positive_integer(row.get("VendorID"))
    pickup = row.get("tpep_pickup_datetime")
    dropoff = row.get("tpep_dropoff_datetime")
    pu = _positive_integer(row.get("PULocationID"))
    do = _positive_integer(row.get("DOLocationID"))
    if (vendor is None or pu is None or do is None or
            not _local_timestamp(pickup) or not _local_timestamp(dropoff) or
            dropoff < pickup):
        raise PreparationError("invalid completed-trip identity fields")
    canonical = [
        IDENTITY_VERSION, vendor, pickup.isoformat(timespec="microseconds"),
        dropoff.isoformat(timespec="microseconds"), pu, do,
    ]
    return hashlib.sha256(json.dumps(
        canonical, separators=(",", ":"), ensure_ascii=True,
    ).encode("ascii")).hexdigest()


def is_sampled(
    key: str, *, seed: int = 42, sample_modulus: int = 1_000_000,
    sample_buckets: int = 10_000,
) -> bool:
    """One stable decision per journey, independent of row/source ordering."""
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise PreparationError("sample seed must be an integer")
    if (isinstance(sample_modulus, bool) or not isinstance(sample_modulus, int)
            or sample_modulus <= 0 or isinstance(sample_buckets, bool)
            or not isinstance(sample_buckets, int)
            or not 0 < sample_buckets <= sample_modulus):
        raise PreparationError("invalid hash-sampling bounds")
    if (not isinstance(key, str) or len(key) != 64 or
            any(char not in "0123456789abcdef" for char in key)):
        raise PreparationError("journey key must be a lowercase SHA256")
    value = int.from_bytes(hashlib.sha256(
        f"{seed}:{key}".encode("ascii"),
    ).digest(), "big")
    return value % sample_modulus < sample_buckets


def _cohort_failure(row: Mapping[str, Any]) -> str | None:
    pickup = row.get("tpep_pickup_datetime")
    if not _local_timestamp(pickup):
        return "valid_local_pickup"
    if split_for_pickup(pickup) is None:
        return "pickup_in_pilot_window"
    fare = row.get("fare_amount")
    if (isinstance(fare, bool) or not isinstance(fare, Real)
            or not math.isfinite(fare) or fare <= 0):
        return "finite_positive_fare"
    if _positive_integer(row.get("RatecodeID")) != 1:
        return "standard_final_ratecode_1"
    pu = _positive_integer(row.get("PULocationID"))
    do = _positive_integer(row.get("DOLocationID"))
    if pu is None or do is None or pu > 263 or do > 263:
        return "named_zones_1_263"
    dropoff = row.get("tpep_dropoff_datetime")
    if (_positive_integer(row.get("VendorID")) is None
            or not _local_timestamp(dropoff) or dropoff < pickup):
        return "valid_completed_journey_identity"
    return None


def _write_json(path: Path, value: dict[str, Any], budget: ManagedFileBudget, *, exclusive: bool = False) -> None:
    # Only private files created by this invocation are updated. No row-level
    # data, input values, exception messages or service configuration are logged.
    budget.write_json(path, value, exclusive=exclusive)


def prepare_samples(
    source_paths: Mapping[str, Path | str], output_dir: Path | str, *,
    disk_budget_bytes: int,
    seed: int = 42, sample_modulus: int = 1_000_000,
    sample_buckets: int = 10_000, caps: Mapping[str, int] | None = None,
    batch_size: int = 8192, calendar: Any = None,
) -> dict[str, Any]:
    """Stream seven Parquet columns into capped, isolated sample outputs.

    The caller must enforce its runtime-root/path guard before calling. This
    function also refuses existing output directories, input symlinks and a
    writable output path traversing a symlink. Failures retain a metadata-only
    FAILED checkpoint and any partial output, never remove or overwrite them.
    Caps stop preparation rather than truncating or changing seed/sample rate.
    Explicit disk_budget_bytes covers unique sources and every owned live file,
    including Parquet footer and atomic metadata temps; not physical C/VHDX.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    from nyc_taxi.ml.pretrip_features import (
        CONTRACT_VERSION, FEATURE_COLUMNS, build_training_features, load_calendar,
        timezone_identity,
    )
    from nyc_taxi.ml import pretrip_features

    if set(source_paths) != set(SOURCE_MONTHS):
        raise PreparationError("exactly June, July and August 2026 sources are required")
    selected_caps = dict(DEFAULT_CAPS if caps is None else caps)
    if (set(selected_caps) != set(SPLITS) or
            any(isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in selected_caps.values())):
        raise PreparationError("each split requires a positive integer cap")
    if (isinstance(batch_size, bool) or not isinstance(batch_size, int)
            or not 1 <= batch_size <= 65_536):
        raise PreparationError("batch_size must be within 1..65536")
    # Validate sampling before creating even the checkpoint/output directory.
    is_sampled("0" * 64, seed=seed, sample_modulus=sample_modulus,
               sample_buckets=sample_buckets)
    selected_calendar = load_calendar() if calendar is None else calendar
    inputs = {month: Path(source_paths[month]) for month in SOURCE_MONTHS}
    for path in inputs.values():
        if path.is_symlink() or not path.is_file():
            raise PreparationError("each source must be an existing regular Parquet file")
    if len({path.resolve() for path in inputs.values()}) != len(SOURCE_MONTHS):
        raise PreparationError("source files must be distinct")
    source_snapshots = {month: _source_snapshot(path) for month, path in inputs.items()}
    builder_path = Path(pretrip_features.__file__).resolve()
    contract_path = builder_path.parents[3] / "config" / "pretrip_v2_contract.json"
    destination = Path(output_dir).absolute()
    if destination.exists() or destination.is_symlink():
        raise PreparationError("output directory already exists; choose a new attempt directory")
    if any(parent.is_symlink() for parent in destination.parents):
        raise PreparationError("output directory must not traverse a symlink")
    # Refuse before mkdir if sources already consume the limit or the dedicated
    # metadata/FAILED-checkpoint pool cannot be reserved. No implicit budget.
    budget = ManagedFileBudget(inputs.values(), destination, disk_budget_bytes)
    destination.mkdir(parents=True, exist_ok=False)
    counts: dict[str, Any] = {
        "input_rows": 0, "waterfall_kept": dict.fromkeys(COHORT_STAGES, 0),
        "excluded_first_reason": dict.fromkeys(COHORT_STAGES, 0),
        "eligible_by_split": dict.fromkeys(SPLITS, 0),
        "sampled_rows_before_dedup": dict.fromkeys(SPLITS, 0),
        "duplicate_rows_removed": dict.fromkeys(SPLITS, 0),
        "selected_rows": dict.fromkeys(SPLITS, 0),
        "source_input_rows": dict.fromkeys(SOURCE_MONTHS, 0),
    }
    report: dict[str, Any] = {
        "status": "INCOMPLETE", "identity_version": IDENTITY_VERSION,
        "feature_contract_version": CONTRACT_VERSION,
        "calendar_version": selected_calendar.version,
        "calendar_sha256": selected_calendar.sha256,
        "feature_contract_sha256": _file_sha256(contract_path),
        "feature_builder_sha256": _file_sha256(builder_path),
        "timezone_identity": timezone_identity(),
        "inputs": {}, "source_snapshot_verified": False,
        "input_checksum_method": "separate full local-file streaming SHA256 pass; no download",
        "read_columns": list(READ_COLUMNS), "predictor_scope": "zone-time-only",
        "sampling": {
            "algorithm": "sha256(seed + ':' + journey_key) modulo modulus < buckets",
            "seed": seed, "modulus": sample_modulus, "buckets": sample_buckets,
            "fraction": sample_buckets / sample_modulus,
            "placement": "after cohort and before deduplication",
        },
        "split_policy": {
            "timezone": "America/New_York", "raw_timestamp_policy": "naive TLC wall time",
            "train": "2026-06-01 <= actual pickup < 2026-07-01",
            "validation": "2026-07-01 <= actual pickup < 2026-08-01",
            "test": "2026-08-01 <= actual pickup < 2026-09-01",
        },
        "caps": selected_caps, "batch_size": batch_size, "counts": counts,
        "deduplication": "same stable journey key and eligible fare; conflicting fares stop",
        "overlap_scope": "retained hash sample only; NOT full eligible cohort",
        "holdout_policy": "test sample prepared only; no fitting, prediction or metrics",
        "cohort_difference_from_v1": "no recorded-distance eligibility/filter or feature",
        "standard_rate_caveat": "retrospective final RatecodeID=1; zones do not prove request-time rate",
        "outputs": {},
    }
    checkpoint = destination / "preparation_checkpoint.json"
    # Bounds are enforced before retaining the next distinct journey, so memory
    # is O(batch_size + sum(caps)); there is no full-fact pandas conversion/cache.
    retained: dict[str, dict[str, Any]] = {}
    feature_keys: tuple[str, ...] | None = None
    try:
        _write_json(checkpoint, report, budget, exclusive=True)
        for month, path in inputs.items():
            _assert_source_snapshot(path, source_snapshots[month])
            source_digest = _file_sha256(path)
            _assert_source_snapshot(path, source_snapshots[month])
            report["inputs"][month] = {
                "bytes": source_snapshots[month][0], "sha256": source_digest,
                "mtime_ns": source_snapshots[month][1],
            }
            _write_json(checkpoint, report, budget)
            parquet = pq.ParquetFile(path)
            missing = sorted(set(READ_COLUMNS) - set(parquet.schema_arrow.names))
            if missing:
                raise PreparationError("source Parquet is missing required columns")
            for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                field_type = parquet.schema_arrow.field(name).type
                if not pa.types.is_timestamp(field_type) or field_type.tz is not None:
                    raise PreparationError("source timestamps must be timezone-naive TLC wall time")
            for batch in parquet.iter_batches(
                batch_size=batch_size, columns=list(READ_COLUMNS), use_threads=False,
            ):
                for row in batch.to_pylist():
                    counts["input_rows"] += 1
                    counts["source_input_rows"][month] += 1
                    reason = _cohort_failure(row)
                    for stage in COHORT_STAGES:
                        if stage == reason:
                            counts["excluded_first_reason"][stage] += 1
                            break
                        counts["waterfall_kept"][stage] += 1
                    if reason is not None:
                        continue
                    split = split_for_pickup(row["tpep_pickup_datetime"])
                    counts["eligible_by_split"][split] += 1
                    key = journey_key(row)
                    if not is_sampled(key, seed=seed, sample_modulus=sample_modulus,
                                      sample_buckets=sample_buckets):
                        continue
                    counts["sampled_rows_before_dedup"][split] += 1
                    fare = float(row["fare_amount"])
                    previous = retained.get(key)
                    if previous is not None:
                        if previous["split"] != split:
                            raise PreparationError("retained journey overlaps temporal splits")
                        if previous["fare_amount"] != fare:
                            raise PreparationError("conflicting eligible fares for a retained journey")
                        counts["duplicate_rows_removed"][split] += 1
                        continue
                    if counts["selected_rows"][split] >= selected_caps[split]:
                        raise PreparationError("retained split exceeds its predeclared cap")
                    features = build_training_features(
                        row["tpep_pickup_datetime"], int(row["PULocationID"]),
                        int(row["DOLocationID"]), calendar=selected_calendar,
                    )
                    if not isinstance(features, dict) or tuple(features) != FEATURE_COLUMNS:
                        raise PreparationError("v2 feature contract must contain exactly eight predictors")
                    if feature_keys is None:
                        feature_keys = tuple(features)
                    elif tuple(features) != feature_keys:
                        raise PreparationError("v2 feature columns changed during preparation")
                    forbidden = set(READ_COLUMNS) - {"PULocationID", "DOLocationID"}
                    if forbidden.intersection(features) or any(
                        name in features for name in (
                            "trip_distance", "actual_duration", "duration", "total_amount",
                            "tip_amount", "payment_type", "dropoff_datetime", "ratecode_id",
                        )
                    ):
                        raise PreparationError("outcome or identity columns leaked into predictors")
                    retained[key] = {
                        "features": features, "fare_amount": fare, "split": split,
                        "pickup_datetime_local": row["tpep_pickup_datetime"],
                    }
                    counts["selected_rows"][split] += 1
                _write_json(checkpoint, report, budget)
            _assert_source_snapshot(path, source_snapshots[month])
        for month, path in inputs.items():
            _assert_source_snapshot(path, source_snapshots[month])
        if any(counts["selected_rows"][split] == 0 for split in SPLITS):
            raise PreparationError("every temporal split must retain at least one sampled journey")
        # Feature data and labels/identity metadata are physically separate.
        # Stable sorting makes selected sample content independent of batch/file order.
        report["feature_columns"] = list(feature_keys or ())
        for split in SPLITS:
            keys = sorted(key for key, row in retained.items() if row["split"] == split)
            features_path = destination / f"{split}_features.parquet"
            labels_path = destination / f"{split}_labels.parquet"
            feature_writer = label_writer = None
            feature_sink = label_sink = None
            output_error = None
            try:
                for start in range(0, len(keys), batch_size):
                    selected_keys = keys[start:start + batch_size]
                    feature_table = pa.Table.from_pylist([
                        retained[key]["features"] for key in selected_keys
                    ])
                    label_table = pa.Table.from_pylist([
                        {
                            "journey_key": key,
                            "pickup_datetime_local": retained[key]["pickup_datetime_local"],
                            "fare_amount": retained[key]["fare_amount"],
                        } for key in selected_keys
                    ])
                    if feature_writer is None:
                        feature_sink = budget.open_file(features_path)
                        feature_writer = pq.ParquetWriter(feature_sink, feature_table.schema)
                        label_sink = budget.open_file(labels_path)
                        label_writer = pq.ParquetWriter(label_sink, label_table.schema)
                    feature_writer.write_table(feature_table)
                    label_writer.write_table(label_table)
            except BaseException as error:
                output_error = error
                raise
            finally:
                # close() writes Parquet footers through the same guarded sink.
                # A secondary footer failure must not hide the original stop.
                close_error = None
                for output in (feature_writer, label_writer, feature_sink, label_sink):
                    if output is not None:
                        try:
                            output.close()
                        except Exception as error:
                            if close_error is None:
                                close_error = error
                if output_error is None and close_error is not None:
                    raise close_error
            report["outputs"][split] = {
                "features": features_path.name, "labels": labels_path.name,
                "rows": len(keys),
                "features_sha256": _file_sha256(features_path),
                "labels_sha256": _file_sha256(labels_path),
                "journey_set_sha256": hashlib.sha256("\n".join(keys).encode("ascii")).hexdigest(),
                "row_alignment": "identical row ordering; labels journey_key ascending",
            }
        report["overlap_counts"] = {
            "train_validation": 0, "train_test": 0, "validation_test": 0,
        }
        for month, path in inputs.items():
            _assert_source_snapshot(path, source_snapshots[month])
        report["source_snapshot_verified"] = True
        report["status"] = "PREPARED_NOT_TRAINED"
        _write_json(destination / "preparation_report.json", report, budget, exclusive=True)
        _write_json(checkpoint, report, budget)
        return report
    except Exception as error:
        report["status"] = "FAILED"
        report["error_type"] = type(error).__name__
        report["stop_reason"] = budget.stop_reason or "preparation_failed"
        # Do not serialize exception text: it can contain paths or input values.
        try:
            _write_json(checkpoint, report, budget)
        except ManagedBudgetExceeded:
            # A future expanded report cannot spend the space required by the
            # bounded FAILED checkpoint. Partial Parquet remains for diagnosis.
            compact = {"status": "FAILED", "error_type": type(error).__name__,
                       "stop_reason": budget.stop_reason or "preparation_failed"}
            _write_json(checkpoint, compact, budget)
        error.budget_usage = budget.snapshot()
        if budget.stop_reason is not None:
            error.stop_reason = budget.stop_reason
        raise


def _file_sha256(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def _source_snapshot(path: Path) -> tuple[int, int, int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino


def _assert_source_snapshot(path: Path, expected: tuple[int, int, int, int]) -> None:
    if _source_snapshot(path) != expected:
        raise PreparationError("source changed between checksum and Parquet preparation")
