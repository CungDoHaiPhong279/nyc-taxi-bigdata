"""Tiny Spark v2 tests; no source sample, holdout, production service or MLflow."""

from __future__ import annotations

import math
import sys

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from nyc_taxi.ml.pretrip_features import FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS
from nyc_taxi.ml.pretrip_training_model import (
    ENCODED_COLUMNS, GBT_PARAMETERS, LABEL_COLUMN, fit, fitted_unknown_counts,
    load_pipeline, make_pipeline, project_model_inputs, save_pipeline, validate_vector_metadata,
)


SCHEMA = StructType([*[StructField(name, DoubleType()) for name in NUMERIC_FEATURE_COLUMNS],
                     StructField("pickup_zone", StringType()), StructField("dropoff_zone", StringType()),
                     StructField(LABEL_COLUMN, DoubleType())])


def values(index, pickup=None, dropoff=None):
    hour, dow = index % 24, index % 7
    return (math.sin(2 * math.pi * hour / 24), math.cos(2 * math.pi * hour / 24),
            math.sin(2 * math.pi * dow / 7), math.cos(2 * math.pi * dow / 7),
            float(dow >= 5), float(index == 0), pickup or str(1 + index % 3),
            dropoff or str(1 + (index + 1) % 3), 5.0 + index)


@pytest.fixture(scope="module")
def spark():
    patch = pytest.MonkeyPatch()
    patch.setenv("PYSPARK_PYTHON", sys.executable)
    patch.setenv("SPARK_LOCAL_IP", "127.0.0.1")
    session = None
    try:
        session = (SparkSession.builder.master("local[2]").appName("pretrip-v2-training-fixture")
                   .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                   .config("spark.default.parallelism", "2").config("spark.driver.memory", "1536m")
                   .config("spark.driver.host", "127.0.0.1")
                   .config("spark.driver.bindAddress", "127.0.0.1")
                   .config("spark.sql.session.timeZone", "America/New_York").getOrCreate())
        session.sparkContext.setLogLevel("ERROR")
        yield session
    finally:
        try:
            if session is not None:
                session.stop()
        finally:
            patch.undo()


@pytest.fixture(scope="module")
def fitted(spark):
    train = spark.createDataFrame([values(index) for index in range(24)], SCHEMA)
    return train, fit(train, tinyfixture=True)


def test_projection_discards_all_extra_predictors_and_does_not_read_labels_when_serving(spark):
    source = spark.createDataFrame([values(1)], SCHEMA).withColumns({
        "journey_key": F.lit("not-a-feature"), "total_amount": F.lit(9999999.0),
        "trip_distance": F.lit(199.0), "actual_duration": F.lit(900.0),
        "load_id": F.lit(4), "prediction": F.lit(999.0), "features": F.array(F.lit(123.0)),
    })
    projected = project_model_inputs(source)
    assert projected.columns == [*FEATURE_COLUMNS, LABEL_COLUMN]
    assert project_model_inputs(source.drop(LABEL_COLUMN), include_label=False).columns == list(FEATURE_COLUMNS)


@pytest.mark.parametrize("column,value", [("pickup_zone", "264"), ("dropoff_zone", "01"),
                                         ("pickup_hour_sin", float("nan")), (LABEL_COLUMN, float("inf"))])
def test_invalid_model_inputs_fail_without_row_filtering(spark, column, value):
    source = spark.createDataFrame([values(0), values(1)], SCHEMA).withColumn(column, F.lit(value))
    with pytest.raises(ValueError, match="invalid or nonfinite"):
        project_model_inputs(source)


def test_numeric_zone_ids_or_missing_required_features_are_rejected(spark):
    source = spark.createDataFrame([values(0)], SCHEMA)
    with pytest.raises(ValueError, match="string categories"):
        project_model_inputs(source.withColumn("pickup_zone", F.col("pickup_zone").cast("int")))
    with pytest.raises(ValueError, match="allowlisted"):
        project_model_inputs(source.drop("is_holiday"))


def test_train_only_dictionaries_unknown_categories_are_one_hot_not_ordered_ids(spark, fitted):
    train, model = fitted
    assert model.stages[0].labels == ["1", "2", "3"]
    assert model.stages[1].labels == ["1", "2", "3"]
    validation = spark.createDataFrame([values(2, "263", "262")], SCHEMA)
    transformed = model.transform(project_model_inputs(validation))
    metadata = validate_vector_metadata(transformed)
    assert metadata["input_feature_names"] == list(FEATURE_COLUMNS)
    assert metadata["vector_dimensions"] > len(FEATURE_COLUMNS)
    row = transformed.first()
    assert row["pickup_zone_index"] == row["dropoff_zone_index"] == 3.0
    for name in ENCODED_COLUMNS:
        assert row[name].numNonzeros() == 1
        assert list(row[name].values) == [1.0]
    counts = fitted_unknown_counts(model, validation)
    assert counts["count"] == counts["either"]["count"] == 1
    assert counts["pickup_zone"] == counts["dropoff_zone"] == {"count": 1, "rate": 1.0}
    assert fitted_unknown_counts(model, train)["either"]["count"] == 0
    assert model.stages[0].labels == ["1", "2", "3"]


def test_vector_metadata_rejects_lost_or_injected_predictors(fitted):
    train, model = fitted
    transformed = model.transform(train)
    with pytest.raises(ValueError, match="allowlist"):
        validate_vector_metadata(transformed.select(F.col("features").alias("features", metadata={})))
    metadata = transformed.schema["features"].metadata
    metadata["ml_attr"]["attrs"]["numeric"][0]["name"] = "fare_amount"
    altered = transformed.select(F.col("features").alias("features", metadata=metadata))
    with pytest.raises(ValueError, match="allowlist"):
        validate_vector_metadata(altered)


def test_frozen_actual_parameters_are_not_tiny_fixture_defaults(spark):
    pipeline = make_pipeline()
    for name, expected in GBT_PARAMETERS.items():
        assert pipeline.getStages()[-1].getOrDefault(name) == expected
    assert pipeline.getStages()[2].getDropLast() is False
    assert pipeline.getStages()[3].getInputCols() == [*NUMERIC_FEATURE_COLUMNS, *ENCODED_COLUMNS]


def test_pipeline_save_reload_preserves_same_input_predictions_without_refit(spark, fitted, tmp_path):
    train, model = fitted
    target = tmp_path / "synthetic-v2-pipeline"
    save_pipeline(model, target)
    reloaded = load_pipeline(target)
    batch = spark.createDataFrame([values(1), values(2, "263", "262"), values(3)], SCHEMA)
    columns = [*FEATURE_COLUMNS, LABEL_COLUMN]
    before = model.transform(batch).select(*columns, "prediction").orderBy(*columns).collect()
    after = reloaded.transform(batch).select(*columns, "prediction").orderBy(*columns).collect()
    assert len(before) == len(after) == 3
    assert max(abs(left["prediction"] - right["prediction"]) for left, right in zip(before, after, strict=True)) <= 1e-9
    assert model.stages[0].labels == reloaded.stages[0].labels
    with pytest.raises(ValueError, match="already exists"):
        save_pipeline(model, target)
