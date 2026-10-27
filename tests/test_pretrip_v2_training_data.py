"""Tiny train/validation fixtures; holdout and raw paths are explicit traps."""

from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ml import pretrip_features as features
from nyc_taxi.ml import pretrip_training_data as training


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sampled_key(split, index):
    counter = index * 100_000
    while True:
        key = hashlib.sha256(f"synthetic-journey-{split}-{counter}".encode("ascii")).hexdigest()
        if int.from_bytes(hashlib.sha256(f"42:{key}".encode("ascii")).digest(), "big") % 1_000_000 < 10_000:
            return key
        counter += 1


def write_pair(directory, split, pairs):
    pq.write_table(pa.Table.from_pylist([row[0] for row in pairs]), directory / f"{split}_features.parquet")
    pq.write_table(pa.Table.from_pylist([row[1] for row in pairs]), directory / f"{split}_labels.parquet")


def refresh_reports(directory, report):
    for split in ("train", "validation"):
        labels = pq.read_table(directory / f"{split}_labels.parquet").to_pylist()
        report["counts"]["selected_rows"][split] = len(labels)
        report["outputs"][split] = {
            "features": f"{split}_features.parquet", "labels": f"{split}_labels.parquet",
            "rows": len(labels),
            "features_sha256": checksum(directory / f"{split}_features.parquet"),
            "labels_sha256": checksum(directory / f"{split}_labels.parquet"),
            "journey_set_sha256": hashlib.sha256("\n".join(
                row["journey_key"] for row in labels
            ).encode("ascii")).hexdigest(),
            "row_alignment": "identical row ordering; labels journey_key ascending",
        }
    for name in ("preparation_report.json", "preparation_checkpoint.json"):
        (directory / name).write_text(json.dumps(report), encoding="utf-8")


@pytest.fixture
def sample(tmp_path):
    directory = tmp_path / "sample"
    directory.mkdir()
    config = tmp_path / "config"
    config.mkdir()
    for name in ("pretrip_v2_contract.json", "pretrip_v2_calendar.json"):
        (config / name).write_bytes((training.DEFAULT_CONFIG / name).read_bytes())
    calendar = features.load_calendar()
    for split, month in (("train", 6), ("validation", 7)):
        pairs = []
        for index in range(4):
            pickup = datetime(2026, month, 5, 9) + timedelta(hours=index)
            feature_row = features.build_training_features(pickup, 164, 43, calendar=calendar)
            label = {"journey_key": sampled_key(split, index),
                     "pickup_datetime_local": pickup, "fare_amount": 10.0 + index}
            pairs.append((feature_row, label))
        pairs.sort(key=lambda pair: pair[1]["journey_key"])
        write_pair(directory, split, pairs)
    # Neither trap is a valid Parquet file. A loader trying to touch one fails.
    (directory / "test_features.parquet").write_bytes(b"NEVER OPEN HOLDOUT")
    (directory / "test_labels.parquet").write_bytes(b"NEVER OPEN HOLDOUT")
    (directory / "yellow_tripdata_2026-08.parquet").write_bytes(b"NEVER OPEN RAW AUGUST")
    report = {
        "status": "PREPARED_NOT_TRAINED", "source_snapshot_verified": True,
        "identity_version": "pretrip-v2-journey-sha256-1",
        "feature_contract_version": features.CONTRACT_VERSION,
        "feature_contract_sha256": checksum(config / "pretrip_v2_contract.json"),
        "feature_builder_sha256": checksum(Path(features.__file__).resolve()),
        "calendar_version": calendar.version, "calendar_sha256": calendar.sha256,
        "timezone_identity": features.timezone_identity(),
        "feature_columns": list(features.FEATURE_COLUMNS),
        "sampling": {"seed": 42, "modulus": 1_000_000, "buckets": 10_000,
                     "fraction": 0.01, "placement": "after cohort and before deduplication",
                     "algorithm": "sha256(seed + ':' + journey_key) modulo modulus < buckets"},
        "caps": {"train": 100_000, "validation": 50_000, "test": 50_000},
        "split_policy": {
            "timezone": "America/New_York", "raw_timestamp_policy": "naive TLC wall time",
            "train": "2026-06-01 <= actual pickup < 2026-07-01",
            "validation": "2026-07-01 <= actual pickup < 2026-08-01",
            "test": "2026-08-01 <= actual pickup < 2026-09-01",
        },
        "counts": {"selected_rows": {"train": 4, "validation": 4, "test": 27}},
        "inputs": {"2026-08": {"bytes": 123, "sha256": "f" * 64}},
        "outputs": {"test": {
            "features": "../../raw/August-do-not-resolve.parquet",
            "labels": "test_labels.parquet", "rows": 27,
        }},
    }
    refresh_reports(directory, report)
    return directory, config, report


def mutate_table(sample, split, kind, transform):
    directory, _, report = sample
    path = directory / f"{split}_{kind}.parquet"
    rows = pq.read_table(path).to_pylist()
    transformed = transform(rows)
    pq.write_table(pa.Table.from_pylist(rows if transformed is None else transformed), path)
    refresh_reports(directory, report)


def load(sample):
    return training.load_training_samples(sample[0], config_dir=sample[1])


def test_loads_only_four_samples_and_metadata_never_stats_opens_or_discovers_holdout_raw(sample, monkeypatch):
    original_open, original_stat, original_glob = Path.open, Path.stat, Path.glob
    visited = []
    forbidden = {"test_features.parquet", "test_labels.parquet", "yellow_tripdata_2026-08.parquet"}

    def guard_open(path, *args, **kwargs):
        assert path.name not in forbidden and "August-do-not-resolve" not in path.name
        assert "raw" not in path.parts
        visited.append(path.name)
        return original_open(path, *args, **kwargs)

    def guard_stat(path, *args, **kwargs):
        assert path.name not in forbidden and "August-do-not-resolve" not in path.name
        assert "raw" not in path.parts
        return original_stat(path, *args, **kwargs)

    def guard_glob(*args, **kwargs):
        raise AssertionError("loader must not discover raw/holdout files")

    monkeypatch.setattr(Path, "open", guard_open)
    monkeypatch.setattr(Path, "stat", guard_stat)
    monkeypatch.setattr(Path, "glob", guard_glob)
    monkeypatch.setattr(Path, "rglob", guard_glob)
    result = load(sample)
    assert set(result) == {"train", "validation", "lineage"}
    assert len(result["train"]) == len(result["validation"]) == 4
    assert result["lineage"]["sample_counts"] == {"train": 4, "validation": 4}
    assert result["lineage"]["holdout_data_accessed"] is False
    assert result["lineage"]["raw_sources_accessed"] is False
    assert "test_features.parquet" not in visited
    assert original_glob is not None
    for split in ("train", "validation"):
        assert all(set(row) == set(features.FEATURE_COLUMNS) | {
            "fare_amount", "journey_key", "pickup_datetime_local",
        } for row in result[split])
        assert all(isinstance(row["pickup_datetime_local"], datetime) and
                   row["pickup_datetime_local"].tzinfo is None for row in result[split])


@pytest.mark.parametrize("field", [
    "feature_contract_sha256", "feature_builder_sha256", "calendar_sha256", "calendar_version",
])
def test_rejects_preparation_identity_mismatch_before_opening_parquet(sample, field):
    directory, _, report = sample
    report[field] = "wrong"
    refresh_reports(directory, report)
    with pytest.raises(training.TrainingDataError, match="identity_mismatch"):
        load(sample)


def test_rejects_tzif_identity_mismatch(sample):
    directory, _, report = sample
    report["timezone_identity"]["tzif_sha256"] = "0" * 64
    refresh_reports(directory, report)
    with pytest.raises(training.TrainingDataError, match="tzdb_identity"):
        load(sample)


@pytest.mark.parametrize("change", ["status", "snapshot", "seed", "caps"])
def test_rejects_unverified_preparation_or_changed_sample_policy(sample, change):
    directory, _, report = sample
    if change == "status":
        report["status"] = "FAILED"
    elif change == "snapshot":
        report["source_snapshot_verified"] = False
    elif change == "seed":
        report["sampling"]["seed"] = 43
    else:
        report["caps"]["train"] = 200_000
    refresh_reports(directory, report)
    with pytest.raises(training.TrainingDataError):
        load(sample)


def test_rejects_checkpoint_report_mismatch(sample):
    directory, _, report = sample
    changed = deepcopy(report)
    changed["outputs"]["train"]["journey_set_sha256"] = "0" * 64
    (directory / "preparation_checkpoint.json").write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(training.TrainingDataError, match="report_checkpoint"):
        load(sample)


def test_rejects_file_byte_mutation_before_parquet_decode(sample):
    directory, _, _ = sample
    with (directory / "train_features.parquet").open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(training.TrainingDataError, match="file_checksum"):
        load(sample)


def test_rejects_wrong_journey_set_even_if_parquet_hash_matches(sample):
    directory, _, report = sample
    for filename in ("preparation_report.json", "preparation_checkpoint.json"):
        changed = deepcopy(report)
        changed["outputs"]["train"]["journey_set_sha256"] = "0" * 64
        (directory / filename).write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(training.TrainingDataError, match="journey_set"):
        load(sample)


def test_rejects_feature_label_row_order_mismatch_even_when_all_hashes_are_updated(sample):
    mutate_table(sample, "train", "features", lambda rows: list(reversed(rows)))
    with pytest.raises(training.TrainingDataError, match="time_alignment"):
        load(sample)


@pytest.mark.parametrize("column", ["fare_amount", "trip_distance", "dropoff_datetime", "payment_type"])
def test_rejects_extra_outcome_or_distance_columns_in_feature_file(sample, column):
    def add_column(rows):
        for row in rows:
            row[column] = 1.0
    mutate_table(sample, "train", "features", add_column)
    with pytest.raises(training.TrainingDataError, match="schema_or_leakage"):
        load(sample)


@pytest.mark.parametrize("value", ["264", "0", "0164", "not-zone"])
def test_rejects_unknown_or_noncanonical_zone_category(sample, value):
    def invalid_zone(rows):
        rows[0]["pickup_zone"] = value
    mutate_table(sample, "train", "features", invalid_zone)
    with pytest.raises(training.TrainingDataError, match="zone_category"):
        load(sample)


@pytest.mark.parametrize("value", [0.0, -1.0, float("nan"), float("inf")])
def test_rejects_nonpositive_or_nonfinite_label_without_dropping_rows(sample, value):
    def invalid_label(rows):
        rows[0]["fare_amount"] = value
    mutate_table(sample, "train", "labels", invalid_label)
    with pytest.raises(training.TrainingDataError, match="finite_positive"):
        load(sample)


def test_keeps_extreme_finite_positive_fare_no_hidden_cap(sample):
    def extreme(rows):
        rows[0]["fare_amount"] = 1e12
    mutate_table(sample, "train", "labels", extreme)
    result = load(sample)
    assert max(row["fare_amount"] for row in result["train"]) == 1e12
    assert len(result["train"]) == 4


def test_rejects_actual_pickup_in_holdout_month_without_accessing_holdout_file(sample):
    def wrong_split(rows):
        rows[0]["pickup_datetime_local"] = datetime(2026, 8, 1)
    mutate_table(sample, "validation", "labels", wrong_split)
    with pytest.raises(training.TrainingDataError, match="actual_pickup_split"):
        load(sample)


def test_rejects_aware_label_timestamp_schema(sample):
    directory, _, report = sample
    path = directory / "train_labels.parquet"
    table = pq.read_table(path)
    column = table["pickup_datetime_local"].cast(pa.timestamp("us", tz="America/New_York"))
    table = table.set_column(table.schema.get_field_index("pickup_datetime_local"), "pickup_datetime_local", column)
    pq.write_table(table, path)
    refresh_reports(directory, report)
    with pytest.raises(training.TrainingDataError, match="label_schema"):
        load(sample)


def test_rejects_nonascending_or_duplicate_keys(sample):
    def duplicate(rows):
        rows[1]["journey_key"] = rows[0]["journey_key"]
    mutate_table(sample, "train", "labels", duplicate)
    with pytest.raises(training.TrainingDataError, match="ascending_unique"):
        load(sample)


def test_rejects_key_not_in_seed42_one_percent_sample(sample):
    directory, _, report = sample
    feature_rows = pq.read_table(directory / "train_features.parquet").to_pylist()
    label_rows = pq.read_table(directory / "train_labels.parquet").to_pylist()
    key = hashlib.sha256(b"not-selected-synthetic").hexdigest()
    while int.from_bytes(hashlib.sha256(f"42:{key}".encode("ascii")).digest(), "big") % 1_000_000 < 10_000:
        key = hashlib.sha256(key.encode("ascii")).hexdigest()
    label_rows[0]["journey_key"] = key
    pairs = sorted(zip(feature_rows, label_rows), key=lambda pair: pair[1]["journey_key"])
    write_pair(directory, "train", pairs)
    refresh_reports(directory, report)
    with pytest.raises(training.TrainingDataError, match="pinned_hash_sample"):
        load(sample)


def test_rejects_train_validation_key_overlap(sample):
    directory, _, report = sample
    train = pq.read_table(directory / "train_labels.parquet").to_pylist()
    features_rows = pq.read_table(directory / "validation_features.parquet").to_pylist()
    labels_rows = pq.read_table(directory / "validation_labels.parquet").to_pylist()
    labels_rows[0]["journey_key"] = train[0]["journey_key"]
    pairs = sorted(zip(features_rows, labels_rows), key=lambda pair: pair[1]["journey_key"])
    write_pair(directory, "validation", pairs)
    refresh_reports(directory, report)
    with pytest.raises(training.TrainingDataError, match="journey_overlap"):
        load(sample)


@pytest.mark.parametrize(("column", "value"), [("pickup_zone", 164), ("pickup_hour_sin", "numeric-text")])
def test_rejects_wrong_categorical_or_numeric_arrow_type(sample, column, value):
    def wrong_type(rows):
        for row in rows:
            row[column] = value
    mutate_table(sample, "train", "features", wrong_type)
    with pytest.raises(training.TrainingDataError, match="schema_or_leakage"):
        load(sample)


def test_rejects_declared_row_count_not_matching_paired_parquet_footer(sample):
    directory, _, report = sample
    changed = deepcopy(report)
    changed["counts"]["selected_rows"]["train"] = 3
    changed["outputs"]["train"]["rows"] = 3
    for name in ("preparation_report.json", "preparation_checkpoint.json"):
        (directory / name).write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(training.TrainingDataError, match="row_count_mismatch"):
        load(sample)


def test_rejects_calendar_bytes_changed_from_contract_pin(sample):
    path = sample[1] / "pretrip_v2_calendar.json"
    with path.open("ab") as stream:
        stream.write(b" ")
    with pytest.raises(training.TrainingDataError, match="calendar_identity"):
        load(sample)
