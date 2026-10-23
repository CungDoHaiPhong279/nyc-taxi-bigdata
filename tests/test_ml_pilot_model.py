"""Tiny Spark fixtures for pilot features/metrics; no data acquisition or tracking."""

from __future__ import annotations

from datetime import datetime
import math
import sys

import pytest
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampNTZType

from nyc_taxi.ml.pilot_model import (
    CATEGORICAL_FEATURE_COLUMNS, FEATURE_COLUMNS, GBT_PARAMETERS, LABEL_COLUMN,
    PREPARED_COLUMNS, assert_category_metadata, baseline_median,
    compare_reloaded_predictions, evaluate_predictions, fit_preprocessor,
    make_regressor, prepare_features, summarize_unseen_categories,
)


@pytest.fixture(scope="module")
def spark():
    environment = pytest.MonkeyPatch()
    environment.setenv("PYSPARK_PYTHON", sys.executable)
    session = None
    try:
        session = (
            SparkSession.builder.appName("nyc-taxi-ml-pilot-unit")
            .master("local[1]").config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "512m")
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


SCHEMA = StructType([
    StructField("trip_distance", DoubleType()),
    StructField("pickup_datetime_local", TimestampNTZType()),
    StructField("pickup_location_id", IntegerType()),
    StructField("dropoff_location_id", IntegerType()),
    StructField("fare_amount", DoubleType()),
])


def raw(spark, rows=None):
    return spark.createDataFrame(rows or [
        (0.0, datetime(2025, 1, 1, 0), 1, 2, 5.0),
        (1.0, datetime(2025, 1, 4, 6), 2, 1, 10.0),
        (2.0, datetime(2025, 1, 6, 12), 1, 2, 20.0),
        (200.0, datetime(2025, 1, 20, 18), 2, 1, 2000.0),
    ], SCHEMA)


def test_strict_projection_and_label_independent_features(spark):
    source = raw(spark).withColumns({
        "total_amount": F.lit(999999.0), "tip_amount": F.lit(123.0),
        "temperature_2m": F.lit(-99.0), "precipitation": F.lit(900.0),
        "passenger_count": F.lit(9), "_source_month": F.lit(12),
        "pickup_hour": F.lit(23), "is_holiday": F.lit(False),
        "vendor_id": F.lit(999), "ratecode_id": F.lit(5),
    })
    first = prepare_features(source)
    second = prepare_features(source.withColumn("fare_amount", F.lit(999.0)))
    assert first.columns == [*PREPARED_COLUMNS, LABEL_COLUMN]
    assert first.drop(LABEL_COLUMN).collect() == second.drop(LABEL_COLUMN).collect()
    without_labels = prepare_features(source.drop(LABEL_COLUMN), include_labels=False)
    assert without_labels.columns == list(PREPARED_COLUMNS)
    assert all("month" not in name and "weather" not in name for name in FEATURE_COLUMNS)


def test_ntz_calendar_is_not_shifted_and_existing_derived_columns_are_ignored(spark):
    source = raw(spark).withColumn("pickup_hour", F.lit(23)).withColumn("is_holiday", F.lit(False))
    expected = prepare_features(source).collect()
    old = spark.conf.get("spark.sql.session.timeZone")
    try:
        spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
        actual = prepare_features(source).collect()
    finally:
        spark.conf.set("spark.sql.session.timeZone", old)
    assert actual == expected
    assert expected[0]["is_holiday"] == 1.0  # New Year's Day, Wednesday (Mon=0 -> 2).
    assert expected[0]["pickup_hour_sin"] == pytest.approx(0.0)
    assert expected[0]["pickup_hour_cos"] == pytest.approx(1.0)
    assert expected[0]["pickup_dow_sin"] == pytest.approx(math.sin(4 * math.pi / 7))
    assert expected[1]["is_weekend"] == 1.0
    assert expected[2]["is_weekend"] == expected[2]["is_holiday"] == 0.0
    assert expected[3]["is_holiday"] == 1.0  # MLK Day.


@pytest.mark.parametrize("column,value", [
    ("trip_distance", -1.0), ("trip_distance", 201.0), ("trip_distance", float("nan")),
    ("trip_distance", float("inf")), ("pickup_location_id", 264),
    ("dropoff_location_id", 265), ("fare_amount", 0.0), ("fare_amount", float("inf")),
    ("pickup_datetime_local", datetime(2026, 1, 1)),
])
def test_input_domain_is_rejected_not_silently_filtered(spark, column, value):
    source = raw(spark).withColumn(column, F.lit(value).cast(SCHEMA[column].dataType))
    with pytest.raises(ValueError, match="contract"):
        prepare_features(source)


def test_missing_columns_and_timestamp_type_are_rejected(spark):
    with pytest.raises(ValueError, match="missing"):
        prepare_features(raw(spark).drop("pickup_datetime_local"))
    with pytest.raises(ValueError, match="TimestampNTZType"):
        prepare_features(raw(spark).withColumn("pickup_datetime_local", F.col("pickup_datetime_local").cast("timestamp")))


def test_train_only_zone_dictionaries_keep_unseen_categories_and_metadata(spark):
    train = prepare_features(raw(spark))
    preprocessor = fit_preprocessor(train)
    assert set(preprocessor.stages[0].labels) == {"1", "2"}
    validation = prepare_features(raw(spark, [(3.0, datetime(2025, 5, 1, 7), 263, 262, 15.0)]))
    transformed = preprocessor.transform(validation)
    metadata = assert_category_metadata(transformed)
    assert metadata["feature_count"] == 9
    assert metadata["categories"] == {name: 3 for name in CATEGORICAL_FEATURE_COLUMNS}
    row = transformed.first()
    assert row["pickup_zone_index"] == row["dropoff_zone_index"] == 2.0
    assert row["features"][7] == row["features"][8] == 2.0
    assert preprocessor.stages[0].labels == ["1", "2"]  # No holdout refit.
    summary = summarize_unseen_categories(preprocessor, validation)
    assert summary["count"] == 1
    assert summary["pickup_zone_index"] == {"count": 1, "rate": 1.0}
    with pytest.raises(ValueError, match="maxBins"):
        assert_category_metadata(transformed, max_bins=2)
    stripped = transformed.select(F.col("features").alias("features", metadata={}))
    with pytest.raises(ValueError, match="allowlist"):
        assert_category_metadata(stripped)


def test_baseline_is_train_median_not_validation(spark):
    train = spark.createDataFrame([(1.0,), (2.0,), (3.0,), (10000.0,), (4.0,)], [LABEL_COLUMN])
    assert baseline_median(train) == 3.0
    with pytest.raises(ValueError, match="nonempty"):
        baseline_median(train.limit(0))


def test_one_aggregate_metrics_keep_negative_predictions_and_tails(spark):
    data = spark.createDataFrame([(1.0, -1.0), (3.0, 4.0), (10001.0, 10000.0)], [LABEL_COLUMN, "prediction"])
    result = evaluate_predictions(data, 3.0)
    labels, predicted = [1.0, 3.0, 10001.0], [-1.0, 4.0, 10000.0]
    center = sum(labels) / len(labels)
    total_square = sum((value - center) ** 2 for value in labels)
    for name, values in (("baseline", [3.0] * 3), ("gbt", predicted)):
        errors = [value - prediction for value, prediction in zip(labels, values)]
        assert result[name]["mae_usd"] == pytest.approx(sum(map(abs, errors)) / 3)
        assert result[name]["rmse_usd"] == pytest.approx(math.sqrt(sum(value ** 2 for value in errors) / 3))
        assert result[name]["r2"] == pytest.approx(1.0 - sum(value ** 2 for value in errors) / total_square)
    assert result["count"] == 3
    assert result["negative_prediction_count"] == 1
    assert result["prediction_min"] == -1.0
    assert result["label_gt_1000_count"] == result["label_gt_10000_count"] == 1


@pytest.mark.parametrize("rows", [[], [(1.0, None)], [(1.0, float("nan"))], [(1.0, float("inf"))]])
def test_metrics_do_not_drop_invalid_or_empty_rows(spark, rows):
    schema = StructType([StructField(LABEL_COLUMN, DoubleType()), StructField("prediction", DoubleType())])
    with pytest.raises(ValueError, match="nonempty, entirely finite"):
        evaluate_predictions(spark.createDataFrame(rows, schema), 1.0)


def test_constant_label_r2_is_explicitly_undefined(spark):
    data = spark.createDataFrame([(5.0, 4.0), (5.0, 6.0)], [LABEL_COLUMN, "prediction"])
    result = evaluate_predictions(data, 5.0)
    assert result["baseline"]["r2"] is result["gbt"]["r2"] is None


def test_regressor_uses_predeclared_pilot_parameters(spark):
    model = make_regressor()
    for name, expected in GBT_PARAMETERS.items():
        assert model.getOrDefault(model.getParam(name)) == expected


def test_tiny_fit_save_reload_uses_same_bounded_prepared_batch(spark, tmp_path):
    # Only 12 synthetic rows and one depth-1 tree; never the real pilot/data.
    train = prepare_features(raw(spark, [
        (float(index), datetime(2025, 1, index + 1, index % 24), 1 + index % 2, 2 - index % 2, 5.0 + index)
        for index in range(12)
    ]))
    preprocessor = fit_preprocessor(train)
    fitted = make_regressor(tiny_test=True).fit(preprocessor.transform(train))
    model = PipelineModel(stages=[*preprocessor.stages, fitted])
    target = str(tmp_path / "tiny-pilot-model")
    model.write().save(target)
    reloaded = PipelineModel.load(target)
    result = compare_reloaded_predictions(model, reloaded, train.limit(3), max_rows=3)
    assert result["count"] == 3
    assert result["max_absolute_difference"] == 0.0
    with pytest.raises(ValueError, match="exceeds"):
        compare_reloaded_predictions(model, reloaded, train.limit(4), max_rows=3)
