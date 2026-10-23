"""Independent tiny Spark oracles for frozen-model holdout metrics; no fit."""

from __future__ import annotations

import math
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from nyc_taxi.ml import holdout_metrics as metrics


@pytest.fixture(scope="module")
def spark():
    environment = pytest.MonkeyPatch()
    environment.setenv("PYSPARK_PYTHON", sys.executable)
    environment.setenv("SPARK_LOCAL_IP", "127.0.0.1")
    session = None
    try:
        session = (
            SparkSession.builder.master("local[1]").appName("nyc-taxi-ml-holdout-tests")
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
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
    StructField("fare_amount", DoubleType(), True),
    StructField("prediction", DoubleType(), True),
    StructField("distance_miles", DoubleType(), True),
    StructField("pickup_borough", StringType(), True),
])


def source(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def buckets(result, kind):
    return {row["slice_label"]: row for row in result["slices"][kind]}


def test_literal_metrics_keep_outlier_negative_prediction_zero_error_and_same_rows(spark):
    # Literal absolute errors: baseline [10,0,10,9990], GBT [2,5,32,10].
    # SST=74,850,275; baseline SSE=99,800,300; GBT SSE=1,153.
    frame = source(spark, [
        (10.0, 8.0, 0.0, "Manhattan"), (20.0, 25.0, 1.0, "Manhattan"),
        (30.0, -2.0, 3.0, "Queens"), (10010.0, 10000.0, 200.0, "EWR"),
    ])
    result = metrics.summarize_holdout(frame, 20.0)
    assert result["status"] == "VALID" and result["count"] == 4
    overall = result["overall"]
    assert overall["count"] == 4 and overall["limited"] is True
    assert overall["baseline"]["mae_usd"] == 2502.5
    assert overall["baseline"]["rmse_usd"] == pytest.approx(math.sqrt(99800300 / 4))
    assert overall["baseline"]["r2"] == pytest.approx(1 - 99800300 / 74850275)
    assert overall["gbt"]["mae_usd"] == 12.25
    assert overall["gbt"]["rmse_usd"] == pytest.approx(math.sqrt(1153 / 4))
    assert overall["gbt"]["r2"] == pytest.approx(1 - 1153 / 74850275)
    assert overall["gbt"]["negative_prediction_count"] == 1
    assert [overall["gbt"][key] for key in (
        "median_absolute_error_usd", "p90_absolute_error_usd", "p95_absolute_error_usd",
    )] == [5.0, 32.0, 32.0]
    assert [overall["baseline"][key] for key in (
        "median_absolute_error_usd", "p90_absolute_error_usd", "p95_absolute_error_usd",
    )] == [10.0, 9990.0, 9990.0]
    assert result["quantiles"]["accuracy"] == 100000
    assert result["quantiles"]["relative_rank_error_bound"] == 0.00001
    assert result["quantiles"]["absolute_usd_error_bound"] is None
    assert result["quantiles"]["interpolated_even_sample_median"] is False
    assert buckets(result, "fare")["(1000,inf)"]["count"] == 1
    for rows in result["slices"].values():
        assert sum(row["count"] for row in rows) == 4


def test_slice_metrics_and_empty_bands_are_independent_small_oracles(spark):
    frame = source(spark, [
        (10.0, 8.0, 0.0, "Manhattan"), (20.0, 25.0, 1.0, "Manhattan"),
        (30.0, -2.0, 3.0, "Queens"),
    ])
    result = metrics.summarize_holdout(frame, 20.0)
    manhattan = buckets(result, "pickup_borough")["Manhattan"]
    assert manhattan["count"] == 2 and manhattan["limited"] is True
    assert manhattan["gbt"]["mae_usd"] == 3.5
    assert manhattan["gbt"]["rmse_usd"] == pytest.approx(math.sqrt(14.5))
    assert manhattan["gbt"]["r2"] == pytest.approx(0.42)
    assert manhattan["baseline"]["mae_usd"] == 5.0
    assert manhattan["baseline"]["rmse_usd"] == pytest.approx(math.sqrt(50))
    assert manhattan["baseline"]["r2"] == -1.0
    assert manhattan["gbt"]["median_absolute_error_usd"] == 2.0
    assert manhattan["baseline"]["median_absolute_error_usd"] == 0.0
    assert buckets(result, "pickup_borough")["Queens"]["gbt"]["r2"] is None
    distances = buckets(result, "distance")
    assert tuple(distances) == metrics.DISTANCE_BANDS
    assert distances["0"]["count"] == distances["(0,1]"]["count"] == distances["(1,3]"]["count"] == 1
    assert distances["(50,200]"]["count"] == 0
    assert distances["(50,200]"]["gbt"]["mae_usd"] is None
    assert distances["(50,200]"]["gbt"]["p95_absolute_error_usd"] is None
    assert tuple(buckets(result, "fare")) == metrics.FARE_BANDS


def test_all_distance_and_fare_half_open_boundaries(spark):
    distances = [0.0, 1.0, 3.0, 5.0, 10.0, 20.0, 50.0, 200.0]
    fares = [10.0, 20.0, 50.0, 100.0, 1000.0, 1001.0, 5.0, 10.0]
    result = metrics.summarize_holdout(source(spark, [
        (fare, fare, distance, "Queens") for fare, distance in zip(fares, distances, strict=True)
    ]), 10.0)
    assert [row["count"] for row in result["slices"]["distance"]] == [1] * 8
    assert [row["count"] for row in result["slices"]["fare"]] == [3, 1, 1, 1, 1, 1]
    assert result["overall"]["gbt"]["mae_usd"] == 0.0


def test_constant_label_and_negative_or_zero_predictions_are_not_clipped(spark):
    result = metrics.summarize_holdout(source(spark, [
        (5.0, -1.0, 0.0, "Queens"), (5.0, 0.0, 0.0, "Queens"), (5.0, 5.0, 0.0, "Queens"),
    ]), 5.0)
    baseline, gbt = result["overall"]["baseline"], result["overall"]["gbt"]
    assert baseline["mae_usd"] == baseline["rmse_usd"] == 0.0
    assert baseline["r2"] is gbt["r2"] is None
    assert gbt["mae_usd"] == pytest.approx(11 / 3)
    assert gbt["rmse_usd"] == pytest.approx(math.sqrt(61 / 3))
    assert gbt["negative_prediction_count"] == gbt["zero_prediction_count"] == 1
    assert gbt["median_absolute_error_usd"] == 5.0
    assert gbt["p90_absolute_error_usd"] == gbt["p95_absolute_error_usd"] == 6.0


@pytest.mark.parametrize("name, value", [
    ("fare_amount", None), ("fare_amount", float("nan")),
    ("fare_amount", float("inf")), ("fare_amount", float("-inf")),
    ("prediction", None), ("prediction", float("nan")),
    ("prediction", float("inf")), ("prediction", float("-inf")),
    ("distance_miles", None), ("distance_miles", float("nan")), ("distance_miles", float("inf")),
])
def test_any_nonfinite_input_rejects_entire_result_not_a_finite_only_subset(spark, name, value):
    invalid = {"fare_amount": 10.0, "prediction": 9.0, "distance_miles": 1.0, "pickup_borough": "Queens"}
    invalid[name] = value
    frame = source(spark, [(10.0, 9.0, 1.0, "Queens"), tuple(invalid[column] for column in metrics.REQUIRED_COLUMNS)])
    with pytest.raises(metrics.HoldoutMetricsError, match="no metrics returned") as caught:
        metrics.summarize_holdout(frame, 10.0)
    assert caught.value.counts["count"] == 2
    assert caught.value.counts["invalid_rows"] == 1
    assert not {"overall", "slices", "mae_usd"}.intersection(caught.value.counts)


def test_finite_squared_error_overflow_fails_without_clipping(spark):
    frame = source(spark, [(1e200, -1e200, 1.0, "Queens")])
    with pytest.raises(metrics.HoldoutMetricsError, match="nonfinite") as caught:
        metrics.summarize_holdout(frame, 0.0)
    assert caught.value.counts["count"] == caught.value.counts["arithmetic_nonfinite_count"] == 1


def test_nonpositive_fares_and_out_of_domain_finite_distances_remain_explicit_not_filtered(spark):
    result = metrics.summarize_holdout(source(spark, [
        (0.0, 1.0, -1.0, None), (-2.0, -1.0, 201.0, ""),
    ]), 0.0)
    assert result["count"] == 2
    assert result["overall"]["gbt"]["mae_usd"] == 1.0
    assert result["overall"]["baseline"]["zero_prediction_count"] == 2
    assert result["overall"]["diagnostics"]["label_negative_count"] == 1
    assert result["overall"]["diagnostics"]["label_zero_count"] == 1
    assert result["overall"]["diagnostics"]["distance_out_of_domain_count"] == 2
    assert buckets(result, "distance")["outside_[0,200]"]["count"] == 2
    assert buckets(result, "fare")["nonpositive"]["count"] == 2
    assert buckets(result, "pickup_borough")["(missing)"]["count"] == 2


def test_limited_evidence_threshold_is_strictly_below_100(spark):
    frame = source(spark, [(10.0, 9.0, 1.0, "Queens")] * 100)
    result = metrics.summarize_holdout(frame, 10.0)
    assert result["overall"]["limited"] is False
    assert buckets(result, "pickup_borough")["Queens"]["limited"] is False
    assert buckets(result, "distance")["0"]["limited"] is True


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_baseline_fails_before_aggregation(spark, value):
    with pytest.raises(ValueError, match="finite scalar"):
        metrics.summarize_holdout(source(spark, [(10.0, 9.0, 1.0, "Queens")]), value)


def test_empty_holdout_and_missing_columns_fail(spark):
    with pytest.raises(metrics.HoldoutMetricsError, match="empty") as caught:
        metrics.summarize_holdout(source(spark, []), 10.0)
    assert caught.value.counts == {"count": 0, "invalid_rows": 0}
    with pytest.raises(ValueError, match="missing"):
        metrics.summarize_holdout(source(spark, []).drop("pickup_borough"), 10.0)


def test_summary_uses_only_one_bounded_aggregate_collect(spark, monkeypatch):
    frame = source(spark, [(10.0, 9.0, 1.0, "Queens")])
    collected = []
    original = type(frame).collect

    def record_collect(dataframe):
        rows = original(dataframe)
        collected.append((dataframe.columns, len(rows)))
        return rows

    monkeypatch.setattr(type(frame), "collect", record_collect)
    result = metrics.summarize_holdout(frame, 10.0)
    assert result["count"] == 1
    assert len(collected) == 1
    assert collected[0][1] == 4
    assert "slice_type" in collected[0][0] and "slice_label" in collected[0][0]
    assert "fare_amount" not in collected[0][0]


def test_unbounded_borough_cardinality_fails_instead_of_collecting_trip_sized_output(spark):
    frame = source(spark, [(10.0, 9.0, 1.0, f"synthetic-{index}") for index in range(70)])
    with pytest.raises(metrics.HoldoutMetricsError, match="Too many") as caught:
        metrics.summarize_holdout(frame, 10.0)
    assert caught.value.counts == {"aggregate_rows_returned": metrics.MAX_AGGREGATE_ROWS + 1}
