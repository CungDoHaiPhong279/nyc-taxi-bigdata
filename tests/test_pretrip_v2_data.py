"""Tiny local Parquet fixtures: no network, Spark, model, or production data."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ml import pretrip_data as data
from nyc_taxi.ml import pretrip_features
from nyc_taxi.ml.pretrip_features import FEATURE_COLUMNS
from nyc_taxi.ml.pretrip_budget import METADATA_RESERVE_BYTES, ManagedBudgetExceeded


RAW_SCHEMA = pa.schema([
    pa.field("VendorID", pa.int64()),
    pa.field("tpep_pickup_datetime", pa.timestamp("us")),
    pa.field("tpep_dropoff_datetime", pa.timestamp("us")),
    pa.field("PULocationID", pa.int64()), pa.field("DOLocationID", pa.int64()),
    pa.field("RatecodeID", pa.float64()), pa.field("fare_amount", pa.float64()),
    # Explicit potential leakage fields exist in the fixture but must NOT be read.
    pa.field("trip_distance", pa.float64()), pa.field("total_amount", pa.float64()),
    pa.field("payment_type", pa.int64()),
])


def record(month=6, *, index=0, **updates):
    pickup = datetime(2026, month, 5, 10) + timedelta(minutes=index)
    row = {
        "VendorID": 1, "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=10),
        "PULocationID": 164, "DOLocationID": 43,
        "RatecodeID": 1.0, "fare_amount": 12.5,
        "trip_distance": 7.0, "total_amount": 100.0, "payment_type": 1,
    }
    row.update(updates)
    return row


def sources(tmp_path, *, additional=None, rows=None):
    root = tmp_path / "raw"
    root.mkdir()
    result = {}
    for month in (6, 7, 8):
        key = f"2026-{month:02}"
        records = [record(month)] if rows is None else rows.get(key, [])
        records = [*records, *((additional or {}).get(key, []))]
        path = root / f"yellow_tripdata_{key}.parquet"
        pq.write_table(pa.Table.from_pylist(records, schema=RAW_SCHEMA), path)
        result[key] = path
    return result


def prepare(tmp_path, paths, **kwargs):
    kwargs.setdefault("disk_budget_bytes", 1024 * 1024)
    return data.prepare_samples(
        paths, tmp_path / "attempt", sample_modulus=1, sample_buckets=1,
        batch_size=2, **kwargs,
    )


def output_rows(tmp_path, split, kind="features"):
    return pq.read_table(tmp_path / "attempt" / f"{split}_{kind}.parquet").to_pylist()


@pytest.mark.parametrize(("value", "expected"), [
    (datetime(2026, 5, 31, 23, 59, 59), None),
    (datetime(2026, 6, 1), "train"),
    (datetime(2026, 6, 30, 23, 59, 59), "train"),
    (datetime(2026, 7, 1), "validation"),
    (datetime(2026, 8, 1), "test"),
    (datetime(2026, 8, 31, 23, 59, 59), "test"),
    (datetime(2026, 9, 1), None),
])
def test_actual_local_pickup_split_boundaries(value, expected):
    assert data.split_for_pickup(value) == expected


def test_split_and_identity_refuse_aware_timestamp_without_silent_utc_conversion():
    with pytest.raises(data.PreparationError, match="naive"):
        data.split_for_pickup(datetime(2026, 6, 1, tzinfo=timezone.utc))
    with pytest.raises(data.PreparationError, match="identity"):
        data.journey_key(record(tpep_pickup_datetime=datetime(2026, 6, 1, tzinfo=timezone.utc)))


def test_journey_identity_is_independent_of_label_distance_source_and_load_id():
    left = record()
    right = record(fare_amount=1000, trip_distance=999, source_month="wrong", load_id=2)
    assert data.journey_key(left) == data.journey_key(right)
    assert data.journey_key(left) != data.journey_key(record(index=1))
    # Independent, explicit canonical identity specification.
    canonical = [
        "pretrip-v2-journey-sha256-1", 1, "2026-06-05T10:00:00.000000",
        "2026-06-05T10:10:00.000000", 164, 43,
    ]
    expected = hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode("ascii")).hexdigest()
    assert data.journey_key(left) == expected


def test_seeded_one_percent_sampling_precedes_dedup_and_ignores_input_order():
    keys = [data.journey_key(record(index=index)) for index in range(1000)]
    actual = {key for key in keys if data.is_sampled(key)}
    expected = {
        key for key in keys
        if int.from_bytes(hashlib.sha256(f"42:{key}".encode("ascii")).digest(), "big")
        % 1_000_000 < 10_000
    }
    assert actual == expected
    assert actual == {key for key in reversed([*keys, *keys]) if data.is_sampled(key)}
    assert actual != {key for key in keys if data.is_sampled(key, seed=43)}
    assert 0 < len(actual) < len(keys)


@pytest.mark.parametrize("kwargs", [
    {"sample_modulus": 0}, {"sample_buckets": 0},
    {"sample_buckets": 1_000_001}, {"seed": True},
])
def test_invalid_sampling_settings_fail_explicitly(kwargs):
    with pytest.raises(data.PreparationError):
        data.is_sampled("0" * 64, **kwargs)


def test_preparation_reads_only_seven_columns_and_physically_separates_labels(tmp_path, monkeypatch):
    paths = sources(tmp_path)
    original = pq.ParquetFile
    projected = []

    class ProjectedFile:
        def __init__(self, path):
            self.inner = original(path)
            self.schema_arrow = self.inner.schema_arrow

        def iter_batches(self, **kwargs):
            projected.append(tuple(kwargs["columns"]))
            assert kwargs["use_threads"] is False
            yield from self.inner.iter_batches(**kwargs)

    monkeypatch.setattr(pq, "ParquetFile", ProjectedFile)
    report = prepare(tmp_path, paths)
    assert projected == [data.READ_COLUMNS] * 3
    assert report["status"] == "PREPARED_NOT_TRAINED"
    assert report["source_snapshot_verified"] is True
    for month, path in paths.items():
        assert report["inputs"][month]["bytes"] == path.stat().st_size
        assert report["inputs"][month]["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        assert report["inputs"][month]["mtime_ns"] == path.stat().st_mtime_ns
    feature_path = Path(pretrip_features.__file__).resolve()
    contract_path = feature_path.parents[3] / "config" / "pretrip_v2_contract.json"
    assert report["feature_builder_sha256"] == hashlib.sha256(feature_path.read_bytes()).hexdigest()
    assert report["feature_contract_sha256"] == hashlib.sha256(contract_path.read_bytes()).hexdigest()
    assert report["timezone_identity"]["timezone"] == "America/New_York"
    assert len(report["timezone_identity"]["tzif_sha256"]) == 64
    assert report["counts"]["selected_rows"] == dict(train=1, validation=1, test=1)
    assert report["overlap_counts"] == dict(train_validation=0, train_test=0, validation_test=0)
    assert "NOT full" in report["overlap_scope"]
    assert "no fitting" in report["holdout_policy"]
    for split in data.SPLITS:
        features = output_rows(tmp_path, split)
        labels = output_rows(tmp_path, split, "labels")
        assert set(features[0]) == set(FEATURE_COLUMNS)
        assert features[0]["pickup_zone"] == "164"
        assert features[0]["dropoff_zone"] == "43"
        assert set(labels[0]) == {"journey_key", "pickup_datetime_local", "fare_amount"}
        assert labels[0]["fare_amount"] == 12.5
        assert not set(data.READ_COLUMNS).intersection(features[0])
        assert not {"trip_distance", "total_amount", "payment_type"}.intersection(features[0])
    serialized = json.dumps(report)
    assert data.journey_key(record()) not in serialized


def test_split_uses_pickup_not_source_month_and_duplicate_journey_is_skipped(tmp_path):
    paths = sources(tmp_path, additional={
        "2026-06": [record(7), record(8)],
        "2026-07": [record(6)],
    })
    report = prepare(tmp_path, paths)
    assert report["counts"]["input_rows"] == 6
    assert report["counts"]["selected_rows"] == dict(train=1, validation=1, test=1)
    assert report["counts"]["duplicate_rows_removed"] == dict(train=1, validation=1, test=1)
    assert [output_rows(tmp_path, split, "labels")[0]["pickup_datetime_local"].month
            for split in data.SPLITS] == [6, 7, 8]


def test_dedup_and_sample_content_deterministic_across_row_order_and_batch_size(tmp_path):
    left = tmp_path / "left"
    right = tmp_path / "right"
    left.mkdir()
    right.mkdir()
    values = {f"2026-{month:02}": [record(month, index=index) for index in (3, 1, 3, 2)]
              for month in (6, 7, 8)}
    first = data.prepare_samples(
        sources(left, rows=values), left / "attempt",
        disk_budget_bytes=1024 * 1024,
        sample_modulus=1, sample_buckets=1, batch_size=1,
    )
    second = data.prepare_samples(
        sources(right, rows={key: list(reversed(rows)) for key, rows in values.items()}),
        right / "attempt", disk_budget_bytes=1024 * 1024,
        sample_modulus=1, sample_buckets=1, batch_size=3,
    )
    assert first["counts"] == second["counts"]
    for split in data.SPLITS:
        assert first["outputs"][split]["journey_set_sha256"] == second["outputs"][split]["journey_set_sha256"]
        for kind in ("features", "labels"):
            assert output_rows(left, split, kind) == output_rows(right, split, kind)


def test_filter_waterfall_keeps_finite_extreme_fares_without_distance_filter(tmp_path):
    bad_fares = [None, float("nan"), float("inf"), float("-inf"), 0, -1]
    extras = [record(index=index + 1, fare_amount=value) for index, value in enumerate(bad_fares)]
    extras += [
        record(index=10, RatecodeID=2), record(index=11, PULocationID=264),
        record(index=12, DOLocationID=0), record(index=13, VendorID=None),
        record(index=14, tpep_dropoff_datetime=datetime(2026, 6, 1)),
        record(index=15, fare_amount=1e12, trip_distance=float("nan")),
        record(index=16, fare_amount=0.01, trip_distance=0),
        record(5, index=17),
    ]
    report = prepare(tmp_path, sources(tmp_path, additional={"2026-06": extras}))
    assert report["counts"]["input_rows"] == 17
    assert report["counts"]["selected_rows"] == dict(train=3, validation=1, test=1)
    assert report["counts"]["excluded_first_reason"] == {
        "valid_local_pickup": 0, "pickup_in_pilot_window": 1,
        "finite_positive_fare": 6, "standard_final_ratecode_1": 1,
        "named_zones_1_263": 2, "valid_completed_journey_identity": 2,
    }
    assert sorted(row["fare_amount"] for row in output_rows(tmp_path, "train", "labels")) == [0.01, 12.5, 1e12]


def test_conflicting_retained_journey_labels_stop_and_keep_failed_checkpoint(tmp_path):
    paths = sources(tmp_path, additional={"2026-06": [record(fare_amount=99)]})
    with pytest.raises(data.PreparationError, match="conflicting"):
        prepare(tmp_path, paths)
    checkpoint = json.loads((tmp_path / "attempt" / "preparation_checkpoint.json").read_text())
    assert checkpoint["status"] == "FAILED"
    assert checkpoint["error_type"] == "PreparationError"
    assert "error_message" not in checkpoint
    assert paths["2026-06"].is_file()


def test_cap_overrun_stops_instead_of_truncating_or_changing_seed(tmp_path):
    paths = sources(tmp_path, additional={"2026-06": [record(index=1)]})
    with pytest.raises(data.PreparationError, match="cap"):
        prepare(tmp_path, paths, caps=dict(train=1, validation=1, test=1))
    checkpoint = json.loads((tmp_path / "attempt" / "preparation_checkpoint.json").read_text())
    assert checkpoint["status"] == "FAILED"
    assert checkpoint["counts"]["selected_rows"]["train"] == 1
    assert checkpoint["sampling"]["seed"] == 42


def test_empty_split_stops_not_silently_discarded(tmp_path):
    paths = sources(tmp_path, rows={"2026-06": [record()], "2026-07": [record(7)]})
    with pytest.raises(data.PreparationError, match="every temporal split"):
        prepare(tmp_path, paths)
    assert not (tmp_path / "attempt" / "preparation_report.json").exists()


def test_output_directory_reuse_fails_without_overwriting_any_files(tmp_path):
    paths = sources(tmp_path)
    prepare(tmp_path, paths)
    checkpoint = tmp_path / "attempt" / "preparation_checkpoint.json"
    before = checkpoint.read_bytes()
    with pytest.raises(data.PreparationError, match="already exists"):
        prepare(tmp_path, paths)
    assert checkpoint.read_bytes() == before


def test_missing_source_columns_stop_with_retained_checkpoint(tmp_path):
    paths = sources(tmp_path)
    table = pq.read_table(paths["2026-06"]).drop(["RatecodeID"])
    pq.write_table(table, paths["2026-06"])
    with pytest.raises(data.PreparationError, match="missing required"):
        prepare(tmp_path, paths)
    assert json.loads((tmp_path / "attempt" / "preparation_checkpoint.json").read_text())["status"] == "FAILED"


def test_timezone_aware_raw_parquet_schema_is_rejected(tmp_path):
    paths = sources(tmp_path)
    table = pq.read_table(paths["2026-06"])
    aware = table.set_column(
        table.schema.get_field_index("tpep_pickup_datetime"), "tpep_pickup_datetime",
        pa.array([datetime(2026, 6, 5, 10, tzinfo=timezone.utc)], type=pa.timestamp("us", tz="UTC")),
    )
    pq.write_table(aware, paths["2026-06"])
    with pytest.raises(data.PreparationError, match="timezone-naive"):
        prepare(tmp_path, paths)


def test_source_change_during_checksum_stops_with_failed_checkpoint(tmp_path, monkeypatch):
    paths = sources(tmp_path)
    original = data._file_sha256

    def changing_hash(path):
        checksum = original(path)
        if path == paths["2026-06"]:
            with path.open("ab") as stream:
                stream.write(b"changed-fixture")
        return checksum

    monkeypatch.setattr(data, "_file_sha256", changing_hash)
    with pytest.raises(data.PreparationError, match="source changed"):
        prepare(tmp_path, paths)
    checkpoint = json.loads((tmp_path / "attempt" / "preparation_checkpoint.json").read_text())
    assert checkpoint["status"] == "FAILED"
    assert checkpoint["source_snapshot_verified"] is False
    assert "2026-06" not in checkpoint["inputs"]


def test_source_change_during_parquet_decode_stops_not_claiming_verified_snapshot(tmp_path, monkeypatch):
    import os

    paths = sources(tmp_path)
    original = pq.ParquetFile

    class ChangingFile:
        def __init__(self, path):
            self.path = path
            self.inner = original(path)
            self.schema_arrow = self.inner.schema_arrow

        def iter_batches(self, **kwargs):
            yield from self.inner.iter_batches(**kwargs)
            if self.path == paths["2026-06"]:
                stat = self.path.stat()
                os.utime(self.path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

    monkeypatch.setattr(pq, "ParquetFile", ChangingFile)
    with pytest.raises(data.PreparationError, match="source changed"):
        prepare(tmp_path, paths)
    checkpoint = json.loads((tmp_path / "attempt" / "preparation_checkpoint.json").read_text())
    assert checkpoint["status"] == "FAILED"
    assert checkpoint["source_snapshot_verified"] is False
    assert checkpoint["inputs"]["2026-06"]["sha256"] == hashlib.sha256(paths["2026-06"].read_bytes()).hexdigest()


@pytest.mark.parametrize("source_keys", [
    ("2026-06", "2026-07"), ("2025-06", "2026-07", "2026-08"),
])
def test_requires_exact_three_month_source_configuration(tmp_path, source_keys):
    paths = sources(tmp_path)
    changed = {key: paths.get(key, paths["2026-06"]) for key in source_keys}
    with pytest.raises(data.PreparationError, match="exactly"):
        prepare(tmp_path, changed)
    assert not (tmp_path / "attempt").exists()


def _task_used_bytes(paths, output):
    unique = {(path.stat().st_dev, path.stat().st_ino): path for path in paths.values()}
    return sum(path.stat().st_size for path in unique.values()) + sum(
        path.stat().st_size for path in output.rglob("*") if path.is_file()
    )


def test_existing_sources_consume_budget_before_output_directory_is_created(tmp_path):
    paths = sources(tmp_path)
    source_bytes = sum(path.stat().st_size for path in paths.values())
    with pytest.raises(ManagedBudgetExceeded) as caught:
        prepare(tmp_path, paths, disk_budget_bytes=source_bytes - 1)
    assert caught.value.stop_reason == "source_files_exceed_budget"
    assert caught.value.budget_usage["used_bytes"] == source_bytes
    assert not (tmp_path / "attempt").exists()
    with pytest.raises(ManagedBudgetExceeded, match="metadata_reserve_unavailable"):
        prepare(tmp_path, paths, disk_budget_bytes=source_bytes + METADATA_RESERVE_BYTES - 1)
    assert not (tmp_path / "attempt").exists()


@pytest.mark.parametrize("data_allowance", [0, 7, 200, 2000])
def test_parquet_budget_stop_keeps_failed_checkpoint_without_exceeding_limit(tmp_path, data_allowance):
    paths = sources(tmp_path)
    source_bytes = sum(path.stat().st_size for path in paths.values())
    limit = source_bytes + METADATA_RESERVE_BYTES + data_allowance
    with pytest.raises(Exception) as caught:
        prepare(tmp_path, paths, disk_budget_bytes=limit)
    checkpoint = json.loads((tmp_path / "attempt/preparation_checkpoint.json").read_text())
    assert checkpoint["status"] == "FAILED"
    assert checkpoint["stop_reason"] == "managed_file_budget_exceeded"
    usage = checkpoint["disk_budget"]
    actual = _task_used_bytes(paths, tmp_path / "attempt")
    assert actual == usage["used_bytes"] <= limit
    assert usage["remaining_bytes"] == limit - actual
    assert usage["peak_used_bytes"] <= limit
    assert usage["sources_bytes"] == source_bytes
    assert caught.value.budget_usage["used_bytes"] == actual
    assert caught.value.stop_reason == "managed_file_budget_exceeded"
    assert not list((tmp_path / "attempt").glob("*.tmp"))
    assert all(path.is_file() for path in paths.values())


def test_successful_preparation_reports_actual_byte_usage_and_guarded_metadata_peak(tmp_path):
    paths = sources(tmp_path)
    report = prepare(tmp_path, paths)
    usage = report["disk_budget"]
    actual = _task_used_bytes(paths, tmp_path / "attempt")
    checkpoint = json.loads((tmp_path / "attempt/preparation_checkpoint.json").read_text())
    assert actual == usage["used_bytes"] == checkpoint["disk_budget"]["used_bytes"]
    assert usage["remaining_bytes"] == 1024 * 1024 - actual
    assert usage["peak_used_bytes"] <= usage["limit_bytes"]
    assert usage["peak_used_bytes"] > actual  # old + temporary checkpoint counted
    assert usage["reserved_bytes"] > 0


def test_preparation_requires_explicit_byte_budget_instead_of_silent_default(tmp_path):
    paths = sources(tmp_path)
    with pytest.raises(TypeError, match="disk_budget_bytes"):
        data.prepare_samples(paths, tmp_path / "attempt")
    assert not (tmp_path / "attempt").exists()


def test_parquet_exact_data_ceiling_passes_and_one_byte_less_fails_on_footer(tmp_path):
    paths = sources(tmp_path)
    source_bytes = sum(path.stat().st_size for path in paths.values())
    reference = prepare(tmp_path, paths)
    data_bytes = sum(path.stat().st_size for path in (tmp_path / "attempt").glob("*.parquet"))
    limit = source_bytes + METADATA_RESERVE_BYTES + data_bytes
    completed = data.prepare_samples(
        paths, tmp_path / "exact", disk_budget_bytes=limit,
        sample_modulus=1, sample_buckets=1, batch_size=2,
    )
    assert completed["counts"] == reference["counts"]
    assert completed["disk_budget"]["available_write_bytes"] == 0
    assert completed["disk_budget"]["used_bytes"] == _task_used_bytes(paths, tmp_path / "exact")
    with pytest.raises(Exception) as caught:
        data.prepare_samples(
            paths, tmp_path / "one-byte-short", disk_budget_bytes=limit - 1,
            sample_modulus=1, sample_buckets=1, batch_size=2,
        )
    assert caught.value.stop_reason == "managed_file_budget_exceeded"
    checkpoint = json.loads((tmp_path / "one-byte-short/preparation_checkpoint.json").read_text())
    assert checkpoint["status"] == "FAILED"
    assert checkpoint["counts"]["selected_rows"] == dict(train=1, validation=1, test=1)
    assert checkpoint["disk_budget"]["peak_used_bytes"] <= limit - 1
    assert checkpoint["disk_budget"]["used_bytes"] == _task_used_bytes(paths, tmp_path / "one-byte-short")
