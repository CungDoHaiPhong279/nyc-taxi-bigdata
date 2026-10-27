"""Synthetic holdout no-fit/nominal metadata checks; never open real samples."""

from __future__ import annotations

from copy import deepcopy
import math
import sys

import pytest
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, StringIndexerModel, VectorAssembler
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from nyc_taxi.ml import pretrip_holdout_model as holdout
from nyc_taxi.ml import pretrip_training_model as training
from nyc_taxi.ml.pretrip_features import NUMERIC_FEATURE_COLUMNS


SCHEMA = StructType([
    *[StructField(name, DoubleType(), nullable=False) for name in NUMERIC_FEATURE_COLUMNS],
    StructField("pickup_zone", StringType(), nullable=False),
    StructField("dropoff_zone", StringType(), nullable=False),
])


@pytest.fixture(scope="module")
def spark():
    patch = pytest.MonkeyPatch()
    patch.setenv("PYSPARK_PYTHON", sys.executable)
    patch.setenv("SPARK_LOCAL_IP", "127.0.0.1")
    session = None
    try:
        session = (SparkSession.builder.master("local[2]").appName("pretrip-holdout-no-fit-fixture")
                   .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
                   .config("spark.default.parallelism", "2").config("spark.driver.memory", "1536m")
                   .config("spark.driver.host", "127.0.0.1")
                   .config("spark.driver.bindAddress", "127.0.0.1").getOrCreate())
        session.sparkContext.setLogLevel("ERROR")
        yield session
    finally:
        try:
            if session is not None:
                session.stop()
        finally:
            patch.undo()


def frame(spark):
    # All dictionaries are supplied explicitly: no training action occurs.
    rows = []
    for hour, pickup, dropoff in ((10, "1", "2"), (15, "263", "262"), (23, "2", "1")):
        rows.append((math.sin(hour * math.pi / 12), math.cos(hour * math.pi / 12),
                     0.0, 1.0, 0.0, 0.0, pickup, dropoff))
    return spark.createDataFrame(rows, SCHEMA)


def preprocessing():
    return PipelineModel(stages=[
        StringIndexerModel.from_labels(["1", "2"], inputCol="pickup_zone",
                                      outputCol="pickup_zone_index", handleInvalid="keep"),
        StringIndexerModel.from_labels(["1", "2"], inputCol="dropoff_zone",
                                      outputCol="dropoff_zone_index", handleInvalid="keep"),
        VectorAssembler(inputCols=[*NUMERIC_FEATURE_COLUMNS, *training.INDEX_COLUMNS],
                        outputCol="features", handleInvalid="error"),
    ])


def test_frozen_preprocessing_has_exact_eight_dimensions_and_unknown_nominal_bucket(spark):
    model = preprocessing()
    labels_before = [list(stage.labels) for stage in model.stages[:2]]
    with holdout.no_fit_guard() as state:
        transformed = model.transform(frame(spark))
        metadata = holdout.validate_nominal_metadata(transformed, {
            "dictionaries": {"pickup_zone": ["1", "2"], "dropoff_zone": ["1", "2"]}})
        rows = transformed.collect()
        assert state == {"fit_calls": 0, "blocked_fit_calls": 0}
    assert metadata["vector_dimensions"] == 8
    assert metadata["categorical_nominal_dimensions"] == {"pickup_zone": 3, "dropoff_zone": 3}
    attrs = transformed.schema["features"].metadata["ml_attr"]["attrs"]
    assert [item["idx"] for item in attrs["nominal"]] == [6, 7]
    assert [item["name"] for item in attrs["numeric"]] == list(NUMERIC_FEATURE_COLUMNS)
    assert all(item["vals"] == ["1", "2", "__unknown"] for item in attrs["nominal"])
    assert rows[1]["pickup_zone_index"] == rows[1]["dropoff_zone_index"] == 2.0
    assert all(row["features"].size == 8 for row in rows)
    assert [list(stage.labels) for stage in model.stages[:2]] == labels_before


def test_nominal_same_size_but_different_dictionary_is_rejected(spark):
    transformed = preprocessing().transform(frame(spark))
    with holdout.no_fit_guard(), pytest.raises(ValueError, match="dictionary"):
        holdout.validate_nominal_metadata(transformed, {
            "dictionaries": {"pickup_zone": ["2", "1"], "dropoff_zone": ["1", "2"]}})


def test_nominal_metadata_loss_is_rejected_without_fit_or_prediction(spark):
    transformed = preprocessing().transform(frame(spark))
    metadata = deepcopy(transformed.schema["features"].metadata)
    attrs = metadata["ml_attr"]["attrs"]
    attrs["numeric"].extend(attrs.pop("nominal"))
    corrupted = transformed.select(F.col("features").alias("features", metadata=metadata))
    with holdout.no_fit_guard(), pytest.raises(ValueError, match="native vector"):
        training.validate_vector_metadata(corrupted, encoding_profile="native")


def test_preprocessing_only_serialization_reload_preserves_metadata_and_unknowns(spark, tmp_path):
    target = tmp_path / "synthetic-preprocessing-only"
    with holdout.no_fit_guard():
        model = preprocessing()
        model.write().save(str(target))
        reloaded = PipelineModel.load(str(target))
        before = model.transform(frame(spark)).select("features").collect()
        after_frame = reloaded.transform(frame(spark))
        after = after_frame.select("features").collect()
        metadata = training.validate_vector_metadata(after_frame, encoding_profile="native")
    assert len(reloaded.stages) == 3
    assert metadata["vector_dimensions"] == 8
    assert metadata["categorical_nominal_dimensions"] == {"pickup_zone": 3, "dropoff_zone": 3}
    assert before == after


@pytest.mark.parametrize("target", ["estimator", "pipeline", "training_model", "training_baseline"])
def test_no_fit_guard_blocks_all_training_entrypoints_before_any_fit(spark, target):
    batch = frame(spark)
    with holdout.no_fit_guard() as state, pytest.raises((RuntimeError, ValueError), match="fit|train"):
        if target == "estimator":
            StringIndexer(inputCol="pickup_zone", outputCol="index").fit(batch)
        elif target == "pipeline":
            Pipeline(stages=[StringIndexer(inputCol="pickup_zone", outputCol="index")]).fit(batch)
        elif target == "training_model":
            training.fit(batch, encoding_profile="native")
        else:
            training.fit_baselines([])
    assert state == {"fit_calls": 1, "blocked_fit_calls": 1}


def test_no_fit_guard_restores_training_functions_after_context(spark):
    fit_before, baselines_before = training.fit, training.fit_baselines
    with holdout.no_fit_guard():
        assert training.fit is not fit_before
        assert training.fit_baselines is not baselines_before
    assert training.fit is fit_before
    assert training.fit_baselines is baselines_before
