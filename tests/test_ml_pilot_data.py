"""Small Spark fixtures for ML-only cohort/identity/split/sample contracts."""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
import os
import sys

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType,
    TimestampNTZType, TimestampType,
)

from nyc_taxi.ml import pilot_data as data


@pytest.fixture(scope="module")
def spark():
    environment = pytest.MonkeyPatch()
    environment.setenv("PYSPARK_PYTHON", sys.executable)
    environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
    session = None
    try:
        session = (
            SparkSession.builder.master("local[1]").appName("nyc-taxi-ml-data-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC").getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        yield session
    finally:
        try:
            if session is not None:
                session.stop()
        finally:
            environment.undo()


LINEAGE = ("_source_year", "_source_month", "_source_file", "_ingestion_batch_id", "_silver_batch_id", "_gold_batch_id", "tag")
SCHEMA = StructType([
    StructField(name, TimestampNTZType() if name in data.TIMESTAMP_COLUMNS else
                DoubleType() if name in data.FLOAT_COLUMNS else
                StringType() if name in {"store_and_fwd_flag", "_source_file", "_ingestion_batch_id", "_silver_batch_id", "_gold_batch_id", "tag"} else LongType(), True)
    for name in (*data.BUSINESS_COLUMNS, *LINEAGE)
])


def row(**updates):
    result = dict(
        vendor_id=1, pickup_datetime_local=datetime(2025, 1, 2, 10),
        dropoff_datetime_local=datetime(2025, 1, 2, 10, 10), passenger_count=1,
        trip_distance=1.0, ratecode_id=1, store_and_fwd_flag="N",
        pickup_location_id=1, dropoff_location_id=2, payment_type=1,
        fare_amount=10.0, extra=0.0, mta_tax=0.5, tip_amount=2.0, tolls_amount=0.0,
        improvement_surcharge=1.0, total_amount=13.5, congestion_surcharge=0.0,
        airport_fee=0.0, cbd_congestion_fee=0.0, _source_year=2025, _source_month=1,
        _source_file="data/raw/taxi/yellow_tripdata_2025-01.parquet",
        _ingestion_batch_id="bronze-a", _silver_batch_id="silver-a", _gold_batch_id="gold-a", tag="valid",
    )
    result.update(updates)
    return result


def frame(spark, rows):
    return spark.createDataFrame([tuple(record[field.name] for field in SCHEMA) for record in rows], SCHEMA)


def test_cohort_waterfall_has_literal_expected_results_and_keeps_zero_null_and_tails(spark):
    records = [row(), row(tag="zero", trip_distance=0.0, passenger_count=None),
               row(tag="tail", fare_amount=20000.0), row(tag="distance200", trip_distance=200.0),
               row(tag="passengerzero", passenger_count=0)]
    records += [
        row(pickup_datetime_local=datetime(2024, 12, 31)), row(pickup_datetime_local=datetime(2025, 7, 1)),
        *(row(fare_amount=value) for value in (None, float("nan"), float("inf"), float("-inf"))),
        row(fare_amount=-1.0), row(fare_amount=0.0),
        *(row(trip_distance=value) for value in (None, float("nan"), float("inf"), float("-inf"), -1.0)),
        row(pickup_location_id=0), row(pickup_location_id=264), row(dropoff_location_id=265),
        row(ratecode_id=None), row(ratecode_id=2), row(trip_distance=200.01),
    ]
    source = frame(spark, records)
    assert data.cohort_summary(source) == {
        "input_rows": 24,
        "waterfall_kept": dict(zip(data.COHORT_STAGES, [22, 18, 16, 11, 8, 6, 5], strict=True)),
    }
    assert {record.tag for record in data.apply_cohort(source).select("tag").collect()} == {
        "valid", "zero", "tail", "distance200", "passengerzero",
    }


def test_actual_pickup_half_open_split_boundaries_ignore_source_month(spark):
    stamps = [datetime(2024, 12, 31, 23, 59, 59), datetime(2025, 1, 1),
              datetime(2025, 4, 30, 23, 59, 59, 999999), datetime(2025, 5, 1),
              datetime(2025, 5, 31, 23, 59, 59, 999999), datetime(2025, 6, 1),
              datetime(2025, 6, 30, 23, 59, 59, 999999), datetime(2025, 7, 1)]
    source = frame(spark, [row(tag=str(index), pickup_datetime_local=stamp, _source_month=6) for index, stamp in enumerate(stamps)])
    actual = {record.tag: record[data.SPLIT_COLUMN] for record in data.with_temporal_split(source).collect()}
    assert [actual[str(index)] for index in range(8)] == [None, "train", "train", "validation", "validation", "test", "test", None]
    combined_summary = data.cohort_summary(source, include_splits=True)
    assert combined_summary == {
        "input_rows": 8,
        "waterfall_kept": dict.fromkeys(data.COHORT_STAGES, 6),
        "split_eligible": {"train": 2, "validation": 2, "test": 2},
    }
    selected = data.with_temporal_split(data.apply_cohort(source))
    assert data.split_counts(selected) == {"train": 2, "validation": 2, "test": 2}
    data.assert_temporal_splits(selected)


def test_ntz_and_required_column_contracts_fail_before_actions(spark):
    source = frame(spark, [row()])
    wrong = source.withColumn("pickup_datetime_local", F.col("pickup_datetime_local").cast(TimestampType()))
    with pytest.raises(ValueError, match="TIMESTAMP_NTZ"):
        data.apply_cohort(wrong)
    with pytest.raises(ValueError, match="missing"):
        data.apply_cohort(source.drop("ratecode_id"))
    with pytest.raises(ValueError, match="missing"):
        data.with_identities(source.drop("total_amount"))


def test_canonical_identity_is_independent_of_lineage_but_sensitive_to_all_business_values(spark):
    source = frame(spark, [row(tag="a", passenger_count=None, trip_distance=0.0),
                           row(tag="b", passenger_count=None, trip_distance=-0.0,
                               _source_month=2, _source_file="other", _gold_batch_id="changed"),
                           row(tag="fare", passenger_count=None, trip_distance=0.0, fare_amount=11.0),
                           row(tag="passenger", passenger_count=0, trip_distance=0.0),
                           row(tag="distance", passenger_count=None, trip_distance=2.0),
                           row(tag="time", passenger_count=None, trip_distance=0.0,
                               pickup_datetime_local=datetime(2025, 1, 2, 10, 0, 0, 1))])
    keys = {record.tag: (record[data.BUSINESS_KEY_COLUMN], record[data.GROUP_KEY_COLUMN]) for record in data.with_identities(source).collect()}
    assert keys["a"] == keys["b"]
    for tag in ("fare", "passenger", "distance"):
        assert keys[tag][0] != keys["a"][0]
        assert keys[tag][1] == keys["a"][1]
    assert keys["time"][0] != keys["a"][0] and keys["time"][1] != keys["a"][1]


def test_canonical_hash_matches_independent_literal_serialization(spark):
    record = row(passenger_count=None)
    payload = {"identity_version": data.IDENTITY_VERSION + ":business"}
    for name in data.BUSINESS_COLUMNS:
        value = record[name]
        payload[name] = (None if value is None else
                         value.strftime("%Y-%m-%dT%H:%M:%S.%f") if name in data.TIMESTAMP_COLUMNS else
                         format(value, ".17e") if name in data.FLOAT_COLUMNS else str(value))
    expected = hashlib.sha256(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    actual = data.with_identities(frame(spark, [record])).select(data.BUSINESS_KEY_COLUMN).first()[0]
    assert actual == expected


def test_identity_canonicalizes_optional_nonfinite_float_tokens(spark):
    source = frame(spark, [row(tag="a", tip_amount=float("nan")), row(tag="b", tip_amount=float("nan")),
                           row(tag="positiveinf", tip_amount=float("inf")), row(tag="negativeinf", tip_amount=float("-inf"))])
    result = {record.tag: record[data.BUSINESS_KEY_COLUMN] for record in data.with_identities(source).select("tag", data.BUSINESS_KEY_COLUMN).collect()}
    assert result["a"] == result["b"]
    assert len({result["a"], result["positiveinf"], result["negativeinf"]}) == 3


def test_identity_requires_utc_without_mutating_session(spark):
    before = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "America/New_York")
        with pytest.raises(ValueError, match="UTC Spark session"):
            data.with_identities(frame(spark, [row()]))
        assert spark.conf.get("spark.sql.session.timeZone") == "America/New_York"
    finally:
        spark.conf.set("spark.sql.session.timeZone", before)


def test_cross_source_dedup_supports_lean_frames_and_deterministic_lineage(spark):
    source = frame(spark, [row(tag="late", _source_month=2, _source_file="b.parquet"),
                           row(tag="early", _source_month=1, _source_file="a.parquet"),
                           row(tag="differentfare", fare_amount=11.0)])
    keyed = data.with_temporal_split(data.with_identities(source))
    lean = keyed.select(data.BUSINESS_KEY_COLUMN, data.GROUP_KEY_COLUMN, data.SPLIT_COLUMN,
                        "pickup_datetime_local", "fare_amount", "_source_month", "_source_file", "tag")
    chosen = data.deduplicate_business(lean)
    assert {record.tag for record in chosen.select("tag").collect()} == {"early", "differentfare"}
    assert data.split_counts(chosen) == {"train": 2, "validation": 0, "test": 0}
    reverse = data.deduplicate_business(lean.orderBy(F.desc("tag")))
    assert {record.tag for record in reverse.select("tag").collect()} == {"early", "differentfare"}
    assert data.deduplicate_business(source).count() == 2


def test_exact_and_group_overlap_counts_are_separate_and_pairwise(spark):
    source = spark.createDataFrame([
        ("b1", "g1", "train"), ("b1", "g1", "validation"),
        ("b2", "g1", "test"), ("b3", "g2", "test"),
    ], [data.BUSINESS_KEY_COLUMN, data.GROUP_KEY_COLUMN, data.SPLIT_COLUMN])
    counts = data.overlap_counts(source)
    expected = dict.fromkeys(data.OVERLAP_KEYS, 0)
    expected.update(business_overlap_groups=1, business_train_validation=1,
                    group_overlap_groups=1, group_train_validation=1, group_train_test=1, group_validation_test=1)
    assert counts == expected
    with pytest.raises(ValueError, match="overlap"):
        data.assert_no_split_overlap(source, counts)


def test_split_and_overlap_guards_reject_invalid_labels_null_keys_and_false_time(spark):
    source = data.with_temporal_split(data.with_identities(frame(spark, [row()])))
    wrong = source.withColumn(data.SPLIT_COLUMN, F.lit("test"))
    with pytest.raises(ValueError, match="actual pickup"):
        data.assert_temporal_splits(wrong)
    invalid = source.withColumn(data.SPLIT_COLUMN, F.lit(None).cast("string"))
    with pytest.raises(ValueError, match="unassigned"):
        data.split_counts(invalid)
    invalid = source.withColumn(data.BUSINESS_KEY_COLUMN, F.lit(None).cast("string"))
    counts = data.overlap_counts(invalid)
    assert counts["business_invalid_groups"] == 1
    with pytest.raises(ValueError, match="invalid"):
        data.assert_no_split_overlap(invalid, counts)
    with pytest.raises(ValueError, match="invalid"):
        data.assert_no_split_overlap(source, {})


def test_valid_and_empty_profiles_have_zero_overlap_and_three_split_counts(spark):
    source = data.with_temporal_split(data.with_identities(frame(spark, [row()])))
    expected = dict.fromkeys(data.OVERLAP_KEYS, 0)
    assert data.assert_no_split_overlap(source) == expected
    empty = source.limit(0)
    assert data.split_counts(empty) == {"train": 0, "validation": 0, "test": 0}
    assert data.overlap_counts(empty) == expected


def test_sampling_is_exact_seed42_expression_grouped_and_order_independent(spark):
    source = spark.range(2000).select(F.concat(F.lit("journey-"), F.col("id")).alias(data.GROUP_KEY_COLUMN))
    source = source.unionByName(source)
    actual = data.deterministic_sample(source)
    expected = source.filter(F.pmod(F.xxhash64(F.lit(42), F.col(data.GROUP_KEY_COLUMN)), F.lit(1_000_000)) < 10_000)
    actual_keys = {record[0] for record in actual.select(data.GROUP_KEY_COLUMN).collect()}
    assert actual_keys
    assert actual_keys == {record[0] for record in expected.select(data.GROUP_KEY_COLUMN).collect()}
    assert actual_keys == {record[0] for record in data.deterministic_sample(source.orderBy(F.desc(data.GROUP_KEY_COLUMN))).collect()}
    assert {record[1] for record in actual.groupBy(data.GROUP_KEY_COLUMN).count().collect()} == {2}
    with pytest.raises(ValueError, match="fixed at 42"):
        data.deterministic_sample(source, seed=43)
    with pytest.raises(ValueError, match="missing"):
        data.deterministic_sample(source.drop(data.GROUP_KEY_COLUMN))


@pytest.fixture(scope="module")
def sampling_records():
    """Only synthetic rows: source copies plus distinct businesses per journey."""
    records = []
    for index in range(2048):
        pickup = datetime(2025, 1, 2, 10) + timedelta(seconds=index)
        original = row(
            tag=f"journey-{index:04d}-base", pickup_datetime_local=pickup,
            dropoff_datetime_local=pickup + timedelta(minutes=10),
            passenger_count=None if index % 3 else 0,
            trip_distance=0.0 if index % 2 else 1.0,
            extra=None if index % 7 else 0.0,
            tip_amount=float("nan") if index % 11 else 2.0,
            _source_file="a.parquet",
        )
        duplicate = {
            **original, "tag": f"journey-{index:04d}-copy", "_source_month": 2,
            "_source_file": "b.parquet", "_ingestion_batch_id": "bronze-b",
            "_silver_batch_id": "silver-b", "_gold_batch_id": "gold-b",
            "trip_distance": -0.0 if original["trip_distance"] == 0 else 1.0,
        }
        revised_fare = {**original, "tag": f"journey-{index:04d}-fare", "fare_amount": 11.0}
        records.extend((original, duplicate, revised_fare))
    return records


def _identity_members(source):
    columns = ("tag", "_source_file", data.BUSINESS_KEY_COLUMN, data.GROUP_KEY_COLUMN)
    return sorted(tuple(record) for record in source.select(*columns).collect())


def test_group_only_identity_needs_no_business_columns_and_preserves_literal_old_hash(spark):
    records = [row(), row(tag="null-vendor", vendor_id=None),
               row(tag="microsecond", pickup_datetime_local=datetime(2025, 1, 2, 10, 0, 0, 123456))]
    source = frame(spark, records)
    old = {record.tag: record[data.GROUP_KEY_COLUMN] for record in data.with_identities(source).select("tag", data.GROUP_KEY_COLUMN).collect()}
    lean = source.select(*data.GROUP_COLUMNS, "tag")
    actual = {record.tag: record[data.GROUP_KEY_COLUMN] for record in data.with_group_identity(lean).collect()}
    assert actual == old
    for record in records:
        payload = {"identity_version": data.IDENTITY_VERSION + ":journey"}
        for name in data.GROUP_COLUMNS:
            value = record[name]
            payload[name] = None if value is None else (
                value.strftime("%Y-%m-%dT%H:%M:%S.%f") if name in data.TIMESTAMP_COLUMNS else str(value)
            )
        expected = hashlib.sha256(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        assert actual[record["tag"]] == expected
    with pytest.raises(ValueError, match="missing"):
        data.with_group_identity(lean.drop("vendor_id"))
    wrong = lean.withColumn("dropoff_datetime_local", F.col("dropoff_datetime_local").cast(TimestampType()))
    with pytest.raises(ValueError, match="TIMESTAMP_NTZ"):
        data.with_group_identity(wrong)


def test_early_sample_matches_legacy_members_keys_and_column_order(spark, sampling_records):
    source = frame(spark, sampling_records)
    legacy = data.deterministic_sample(data.with_identities(source))
    early = data.sample_first_with_identities(source)
    expected = _identity_members(legacy)
    assert expected
    assert early.columns == legacy.columns
    assert _identity_members(early) == expected
    supplied_keys = source.withColumn(data.BUSINESS_KEY_COLUMN, F.lit("untrusted-business")) \
        .withColumn(data.GROUP_KEY_COLUMN, F.lit("untrusted-group"))
    assert _identity_members(data.sample_first_with_identities(supplied_keys)) == expected
    with pytest.raises(ValueError, match="fixed at 42"):
        data.sample_first_with_identities(source, seed=43)


def test_exact_dedup_and_group_sample_commute_with_null_zero_and_source_copies(spark, sampling_records):
    source = frame(spark, sampling_records)
    keyed = data.with_identities(source)
    sample_then_dedup = data.deduplicate_business(data.deterministic_sample(keyed))
    dedup_then_sample = data.deterministic_sample(data.deduplicate_business(keyed))
    early = data.deduplicate_business(data.sample_first_with_identities(source))
    expected = _identity_members(dedup_then_sample)
    assert expected
    assert _identity_members(sample_then_dedup) == expected
    assert _identity_members(early) == expected
    # Every selected journey retains both distinct fares and only canonical source a.
    assert all(member[1] == "a.parquet" and not member[0].endswith("-copy") for member in expected)
    assert {record[1] for record in early.groupBy(data.GROUP_KEY_COLUMN).count().collect()} == {2}


def test_early_sample_and_dedup_survive_reordering_and_repartitioning(spark, sampling_records):
    source = frame(spark, sampling_records)
    expected = _identity_members(data.deterministic_sample(data.deduplicate_business(data.with_identities(source))))
    reordered = source.orderBy(F.desc("tag")).repartition(3)
    actual = data.deduplicate_business(data.sample_first_with_identities(reordered))
    assert _identity_members(actual) == expected


def test_selected_cross_split_business_and_journey_overlap_still_rejected(spark, sampling_records):
    source = frame(spark, sampling_records)
    selected = data.sample_first_with_identities(source).withColumn(
        data.SPLIT_COLUMN,
        F.when(F.col("tag").endswith("-copy"), "validation")
        .when(F.col("tag").endswith("-fare"), "test").otherwise("train"),
    )
    before = data.overlap_counts(selected)
    assert before["business_train_validation"] > 0
    assert before["group_train_test"] > 0
    with pytest.raises(ValueError, match="overlap"):
        data.assert_no_split_overlap(selected, before)
    deduplicated = data.deduplicate_business(selected)
    after = data.overlap_counts(deduplicated)
    assert after["business_overlap_groups"] == 0
    assert after["group_train_test"] > 0
    with pytest.raises(ValueError, match="overlap"):
        data.assert_no_split_overlap(deduplicated, after)


def test_overlap_summary_plan_is_lazy_and_does_not_collect_old_data(spark, monkeypatch):
    source = spark.createDataFrame(
        [("business", "journey", "train")],
        [data.BUSINESS_KEY_COLUMN, data.GROUP_KEY_COLUMN, data.SPLIT_COLUMN],
    )

    def forbid_action(*args, **kwargs):
        raise AssertionError("Building an overlap explanation must not run a Spark action")

    for name in ("first", "collect", "count"):
        monkeypatch.setattr(type(source), name, forbid_action)
    summary = data.overlap_summary_frame(source)
    assert summary.columns == list(data.OVERLAP_KEYS)
