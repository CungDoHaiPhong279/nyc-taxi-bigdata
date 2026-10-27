"""Verified train/validation-only boundary for the zone/time v2 pilot.

Only four fixed sample Parquet filenames and two preparation metadata documents
are opened. No directory discovery, raw source access, August/holdout file stat,
Spark session, service or model exists here. Features and labels are joined by
their verified shared row order before the caller creates a Spark DataFrame.
"""

from __future__ import annotations

from datetime import date, datetime
import hashlib
from itertools import zip_longest
import json
import math
from numbers import Real
from pathlib import Path
from typing import Any

from nyc_taxi.ml import pretrip_features
from nyc_taxi.ml.pretrip_features import (
    CONTRACT_VERSION, FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS, HolidayCalendar,
)


DEFAULT_CONFIG = Path(__file__).resolve().parents[3] / "config"
SPLITS = ("train", "validation")
CAPS = {"train": 100_000, "validation": 50_000}
LABEL_COLUMNS = ("journey_key", "pickup_datetime_local", "fare_amount")
MAX_METADATA_BYTES = 16 * 1024
BATCH_SIZE = 4096


class TrainingDataError(ValueError):
    """A stable, non-sensitive stop code, never an individual sample value."""


def _sha256(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def _signature(path: Path) -> tuple[int, int, int, int]:
    try:
        stat = path.stat()
    except OSError:
        raise TrainingDataError("required_sample_file_unavailable") from None
    return stat.st_size, stat.st_mtime_ns, stat.st_dev, stat.st_ino


def _metadata(path: Path) -> tuple[dict[str, Any], str]:
    if _signature(path)[0] > MAX_METADATA_BYTES:
        raise TrainingDataError("preparation_metadata_exceeds_limit")
    try:
        raw = path.read_bytes()
        document = json.loads(raw)
    except (OSError, ValueError):
        raise TrainingDataError("invalid_preparation_metadata") from None
    if not isinstance(document, dict):
        raise TrainingDataError("invalid_preparation_metadata")
    return document, hashlib.sha256(raw).hexdigest()


def _sha_string(value: Any) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and all(char in "0123456789abcdef" for char in value))


def _identities(config_dir: Path) -> tuple[dict[str, Any], HolidayCalendar]:
    contract_path = config_dir / "pretrip_v2_contract.json"
    calendar_path = config_dir / "pretrip_v2_calendar.json"
    try:
        contract_raw = contract_path.read_bytes()
        contract = pretrip_features.load_contract(contract_path)
        calendar_raw = calendar_path.read_bytes()
        calendar_data = json.loads(calendar_raw)
        calendar_hash = hashlib.sha256(calendar_raw).hexdigest()
        pin = contract["calendar"]
        if (calendar_hash != pin["sha256"]
                or calendar_data["calendar_version"] != pin["version"]
                or calendar_data["timezone"] != "America/New_York"
                or calendar_data["coverage_start"] != pin["coverage_start"]
                or calendar_data["coverage_end"] != pin["coverage_end"]):
            raise TrainingDataError("calendar_identity_mismatch")
        holidays = [date.fromisoformat(row["date"]) for row in calendar_data["holidays"]]
        start, end = date.fromisoformat(pin["coverage_start"]), date.fromisoformat(pin["coverage_end"])
        if (start > end or len(holidays) != len(set(holidays))
                or any(not start <= day <= end for day in holidays)):
            raise TrainingDataError("invalid_calendar_coverage")
        calendar = HolidayCalendar(pin["version"], calendar_hash, start, end, frozenset(holidays))
        timezone = pretrip_features.timezone_identity()
        identities = {
            "feature_contract_version": CONTRACT_VERSION,
            "feature_contract_sha256": hashlib.sha256(contract_raw).hexdigest(),
            "feature_builder_sha256": _sha256(Path(pretrip_features.__file__).resolve()),
            "calendar_version": calendar.version, "calendar_sha256": calendar.sha256,
            "timezone_identity": timezone,
        }
        return identities, calendar
    except TrainingDataError:
        raise
    except (OSError, ValueError, KeyError, TypeError):
        raise TrainingDataError("feature_contract_or_calendar_unavailable") from None


def _verify_metadata(
    report: dict[str, Any], checkpoint: dict[str, Any], identities: dict[str, Any],
) -> dict[str, int]:
    counts = {}
    required_sampling = {"seed": 42, "modulus": 1_000_000, "buckets": 10_000}
    for document in (report, checkpoint):
        if document.get("status") != "PREPARED_NOT_TRAINED" or document.get("source_snapshot_verified") is not True:
            raise TrainingDataError("preparation_not_verified_complete")
        if document.get("identity_version") != "pretrip-v2-journey-sha256-1":
            raise TrainingDataError("journey_identity_version_mismatch")
        for name in ("feature_contract_version", "feature_contract_sha256", "feature_builder_sha256",
                     "calendar_version", "calendar_sha256"):
            if document.get(name) != identities[name]:
                raise TrainingDataError("preparation_feature_identity_mismatch")
        timezone = document.get("timezone_identity", {})
        if (not isinstance(timezone, dict) or timezone.get("timezone") != "America/New_York"
                or timezone.get("tzif_sha256") != identities["timezone_identity"]["tzif_sha256"]):
            raise TrainingDataError("preparation_tzdb_identity_mismatch")
        if document.get("feature_columns") != list(FEATURE_COLUMNS):
            raise TrainingDataError("feature_allowlist_mismatch")
        sampling = document.get("sampling", {})
        if (not isinstance(sampling, dict) or any(
            type(sampling.get(name)) is not int or sampling[name] != value
            for name, value in required_sampling.items()
        ) or sampling.get("algorithm") != "sha256(seed + ':' + journey_key) modulo modulus < buckets"
                or sampling.get("fraction") != 0.01
                or sampling.get("placement") != "after cohort and before deduplication"):
            raise TrainingDataError("sampling_policy_mismatch")
        if document.get("caps") != {"train": 100_000, "validation": 50_000, "test": 50_000}:
            raise TrainingDataError("preparation_caps_mismatch")
        raw_counts, outputs = document.get("counts"), document.get("outputs")
        if (not isinstance(raw_counts, dict) or not isinstance(raw_counts.get("selected_rows"), dict)
                or not isinstance(outputs, dict)):
            raise TrainingDataError("sample_output_manifest_mismatch")
        for split in SPLITS:
            count = raw_counts["selected_rows"].get(split)
            if type(count) is not int or not 1 <= count <= CAPS[split]:
                raise TrainingDataError("sample_count_outside_pilot_cap")
            entry = outputs.get(split)
            if (not isinstance(entry, dict) or entry.get("features") != f"{split}_features.parquet"
                    or entry.get("labels") != f"{split}_labels.parquet"
                    or type(entry.get("rows")) is not int or entry["rows"] != count
                    or not all(_sha_string(entry.get(name)) for name in (
                        "features_sha256", "labels_sha256", "journey_set_sha256",
                    ))):
                raise TrainingDataError("sample_output_manifest_mismatch")
            counts[split] = count
    for name in ("sampling", "caps", "split_policy"):
        if report.get(name) != checkpoint.get(name):
            raise TrainingDataError("report_checkpoint_mismatch")
    for split in SPLITS:
        if (report["counts"]["selected_rows"][split] != checkpoint["counts"]["selected_rows"][split]
                or report["outputs"][split] != checkpoint["outputs"][split]):
            raise TrainingDataError("report_checkpoint_mismatch")
    policy = report.get("split_policy", {})
    if (not isinstance(policy, dict) or policy.get("timezone") != "America/New_York"
            or policy.get("raw_timestamp_policy") != "naive TLC wall time"
            or policy.get("train") != "2026-06-01 <= actual pickup < 2026-07-01"
            or policy.get("validation") != "2026-07-01 <= actual pickup < 2026-08-01"):
        raise TrainingDataError("actual_pickup_split_policy_mismatch")
    return counts


def _zone(value: Any) -> int:
    if not isinstance(value, str) or not value.isascii() or not value.isdigit():
        raise TrainingDataError("invalid_named_zone_category")
    result = int(value)
    if not 1 <= result <= 263 or str(result) != value:
        raise TrainingDataError("invalid_named_zone_category")
    return result


def _validate_row(
    features: dict[str, Any], label: dict[str, Any], split: str,
    previous_key: str | None, calendar: HolidayCalendar,
) -> tuple[dict[str, Any], str]:
    key = label["journey_key"]
    if not _sha_string(key) or (previous_key is not None and key <= previous_key):
        raise TrainingDataError("journey_keys_not_strictly_ascending_unique")
    digest = hashlib.sha256(f"42:{key}".encode("ascii")).digest()
    if int.from_bytes(digest, "big") % 1_000_000 >= 10_000:
        raise TrainingDataError("journey_not_in_pinned_hash_sample")
    pickup = label["pickup_datetime_local"]
    month = 6 if split == "train" else 7
    if (not isinstance(pickup, datetime) or pickup.tzinfo is not None
            or pickup.year != 2026 or pickup.month != month):
        raise TrainingDataError("sample_actual_pickup_split_mismatch")
    fare = label["fare_amount"]
    if isinstance(fare, bool) or not isinstance(fare, Real) or not math.isfinite(fare) or fare <= 0:
        raise TrainingDataError("sample_label_not_finite_positive")
    _zone(features["pickup_zone"])
    _zone(features["dropoff_zone"])
    # Deliberately rederive independently instead of calling the builder that
    # generated these sample columns. This catches row-order/time misalignment.
    hour = 2 * math.pi * pickup.hour / 24
    dow = 2 * math.pi * pickup.weekday() / 7
    expected = (
        math.sin(hour), math.cos(hour), math.sin(dow), math.cos(dow),
        float(pickup.weekday() >= 5), float(calendar.is_holiday(pickup.date())),
    )
    for name, expected_value in zip(NUMERIC_FEATURE_COLUMNS, expected, strict=True):
        value = features[name]
        if (isinstance(value, bool) or not isinstance(value, Real) or not math.isfinite(value)
                or not math.isclose(value, expected_value, rel_tol=1e-12, abs_tol=1e-12)):
            raise TrainingDataError("feature_label_time_alignment_mismatch")
    return {**features, "fare_amount": float(fare), "journey_key": key,
            "pickup_datetime_local": pickup}, key


def _validate_schemas(feature_schema, label_schema) -> None:
    import pyarrow as pa

    if (tuple(feature_schema.names) != FEATURE_COLUMNS
            or any(not pa.types.is_float64(feature_schema.field(name).type)
                   for name in NUMERIC_FEATURE_COLUMNS)
            or any(not pa.types.is_string(feature_schema.field(name).type)
                   for name in ("pickup_zone", "dropoff_zone"))):
        raise TrainingDataError("sample_feature_schema_or_leakage_mismatch")
    if (tuple(label_schema.names) != LABEL_COLUMNS
            or not pa.types.is_string(label_schema.field("journey_key").type)
            or label_schema.field("pickup_datetime_local").type != pa.timestamp("us")
            or not pa.types.is_float64(label_schema.field("fare_amount").type)):
        raise TrainingDataError("sample_label_schema_mismatch")


def _iter_rows(parquet):
    for batch in parquet.iter_batches(batch_size=BATCH_SIZE, use_threads=False):
        yield from batch.to_pylist()


def load_training_samples(
    sample_dir: Path, *, config_dir: Path = DEFAULT_CONFIG,
) -> dict[str, Any]:
    """Return bounded train/validation rows and verified aggregate lineage.

    Reports can contain holdout metadata; this function never resolves a path
    from those fields or opens/stats any test, August, raw or acquisition file.
    The caller must project exactly FEATURE_COLUMNS when fitting a model.
    """
    import pyarrow.parquet as pq

    directory = Path(sample_dir)
    identities, calendar = _identities(Path(config_dir))
    report, report_sha = _metadata(directory / "preparation_report.json")
    checkpoint, checkpoint_sha = _metadata(directory / "preparation_checkpoint.json")
    counts = _verify_metadata(report, checkpoint, identities)
    result: dict[str, Any] = {}
    all_keys: set[str] = set()
    sample_hashes = {}
    all_signatures = {}
    for split in SPLITS:
        feature_path = directory / f"{split}_features.parquet"
        label_path = directory / f"{split}_labels.parquet"
        signatures = {path: _signature(path) for path in (feature_path, label_path)}
        all_signatures.update(signatures)
        entry = report["outputs"][split]
        actual_hashes = {"features_sha256": _sha256(feature_path), "labels_sha256": _sha256(label_path)}
        if any(actual_hashes[name] != entry[name] for name in actual_hashes):
            raise TrainingDataError("sample_file_checksum_mismatch")
        feature_file = label_file = None
        try:
            feature_file, label_file = pq.ParquetFile(feature_path), pq.ParquetFile(label_path)
            _validate_schemas(feature_file.schema_arrow, label_file.schema_arrow)
            if (feature_file.metadata.num_rows != counts[split]
                    or label_file.metadata.num_rows != counts[split]):
                raise TrainingDataError("sample_row_count_mismatch")
            sentinel = object()
            previous_key = None
            rows = []
            key_hash = hashlib.sha256()
            for feature_row, label_row in zip_longest(
                _iter_rows(feature_file), _iter_rows(label_file), fillvalue=sentinel,
            ):
                if feature_row is sentinel or label_row is sentinel:
                    raise TrainingDataError("sample_pair_row_alignment_mismatch")
                if len(rows) >= counts[split]:
                    raise TrainingDataError("sample_row_count_mismatch")
                row, key = _validate_row(feature_row, label_row, split, previous_key, calendar)
                if key in all_keys:
                    raise TrainingDataError("train_validation_journey_overlap")
                all_keys.add(key)
                if rows:
                    key_hash.update(b"\n")
                key_hash.update(key.encode("ascii"))
                rows.append(row)
                previous_key = key
            if len(rows) != counts[split] or key_hash.hexdigest() != entry["journey_set_sha256"]:
                raise TrainingDataError("sample_journey_set_or_count_mismatch")
            result[split] = rows
            sample_hashes[split] = {**actual_hashes, "journey_set_sha256": key_hash.hexdigest()}
        finally:
            if feature_file is not None:
                feature_file.close()
            if label_file is not None:
                label_file.close()
        if any(_signature(path) != signature for path, signature in signatures.items()):
            raise TrainingDataError("sample_file_changed_during_loading")
    if any(_signature(path) != signature for path, signature in all_signatures.items()):
        raise TrainingDataError("sample_file_changed_during_loading")
    result["lineage"] = {
        **identities, "preparation_report_sha256": report_sha,
        "preparation_checkpoint_sha256": checkpoint_sha,
        "sample_counts": counts, "sample_hashes": sample_hashes,
        "sample_identity_sha256": hashlib.sha256(json.dumps(
            sample_hashes, sort_keys=True, separators=(",", ":"),
        ).encode("ascii")).hexdigest(),
        "feature_columns": list(FEATURE_COLUMNS),
        "sampling": report["sampling"],
        "journey_overlap_train_validation": 0,
        "holdout_data_accessed": False, "raw_sources_accessed": False,
        "overlap_scope": "train and validation retained samples only",
        "preparation_input_lineage": report.get("inputs", {}),
    }
    return result
