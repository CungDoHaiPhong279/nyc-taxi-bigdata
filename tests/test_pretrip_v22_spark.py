"""Synthetic native-category Spark tests; no pilot, holdout or live services."""

from __future__ import annotations

from copy import deepcopy
import json
import math
import sys

import pytest
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from nyc_taxi.ml.pretrip_features import FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS
from nyc_taxi.ml.pretrip_training_model import (
    INDEX_COLUMNS, LABEL_COLUMN, fit, fitted_unknown_counts, gbt_parameters,
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
        session = (SparkSession.builder.master("local[2]").appName("pretrip-v22-native-fixture")
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


@pytest.fixture(scope="module")
def fitted(spark):
    train = spark.createDataFrame([values(index) for index in range(24)], SCHEMA)
    return train, fit(train, tinyfixture=True, encoding_profile="native")


def test_native_pipeline_has_no_encoder_or_vector_indexer_and_fixed_actual_params(spark):
    stages = make_pipeline(encoding_profile="native").getStages()
    assert [type(stage).__name__ for stage in stages] == [
        "StringIndexer", "StringIndexer", "VectorAssembler", "GBTRegressor"]
    assert stages[2].getInputCols() == [*NUMERIC_FEATURE_COLUMNS, *INDEX_COLUMNS]
    for stage in stages[:2]:
        assert stage.getHandleInvalid() == "keep" and stage.getStringOrderType() == "alphabetAsc"
    assert {name: stages[-1].getOrDefault(name) for name in gbt_parameters(encoding_profile="native")} == (
        gbt_parameters(encoding_profile="native"))


def test_native_exact_eight_dimensions_and_train_only_unknown_bucket(spark, fitted):
    train, model = fitted
    assert len(model.stages) == 4
    assert all(isinstance(stage, StringIndexerModel) for stage in model.stages[:2])
    assert isinstance(model.stages[2], VectorAssembler)
    validation = spark.createDataFrame([values(2, "263", "262")], SCHEMA)
    result = model.transform(project_model_inputs(validation))
    metadata = validate_vector_metadata(result, encoding_profile="native")
    assert metadata["vector_dimensions"] == model.stages[-1].numFeatures == 8
    assert metadata["categorical_nominal_dimensions"] == {"pickup_zone": 4, "dropoff_zone": 4}
    attributes = result.schema["features"].metadata["ml_attr"]["attrs"]
    assert [item["name"] for item in attributes["numeric"]] == list(NUMERIC_FEATURE_COLUMNS)
    assert [item["idx"] for item in attributes["nominal"]] == [6, 7]
    assert all(item["vals"][-1] == "__unknown" for item in attributes["nominal"])
    row = result.first()
    assert row["features"].size == 8
    assert row["pickup_zone_index"] == row["dropoff_zone_index"] == 3.0
    assert fitted_unknown_counts(model, validation)["either"] == {"count": 1, "rate": 1.0}
    assert fitted_unknown_counts(model, train)["either"]["count"] == 0
    assert model.stages[0].labels == model.stages[1].labels == ["1", "2", "3"]


def test_native_allowlist_discards_outcomes_lineage_and_existing_vectors(spark, fitted):
    train, model = fitted
    dirty = train.withColumns({"journey_key": F.lit("not-a-feature"), "trip_distance": F.lit(42.0),
                               "fare_component": F.lit(999.0), "features": F.array(F.lit(1.0)),
                               "dropoff_datetime": F.lit("2099-01-01"), "lineage": F.lit("source")})
    projected = project_model_inputs(dirty)
    assert projected.columns == [*FEATURE_COLUMNS, LABEL_COLUMN]
    assert validate_vector_metadata(model.transform(projected), encoding_profile="native")["vector_dimensions"] == 8
    assert project_model_inputs(dirty.drop(LABEL_COLUMN), include_label=False).columns == list(FEATURE_COLUMNS)


def test_native_lost_nominal_metadata_is_rejected_before_fit(spark, fitted):
    train, model = fitted
    transformed = model.transform(train)
    metadata = deepcopy(transformed.schema["features"].metadata)
    attrs = metadata["ml_attr"]["attrs"]
    attrs["numeric"].extend(attrs.pop("nominal"))
    corrupted = transformed.select(F.col("features").alias("features", metadata=metadata))
    with pytest.raises(ValueError, match="native vector"):
        validate_vector_metadata(corrupted, encoding_profile="native")


def test_too_many_categories_stops_before_gbt_fit_without_raising_bins(spark, monkeypatch):
    train = spark.createDataFrame([values(index, str(index + 1), "1") for index in range(256)], SCHEMA)
    calls = []

    def forbidden_fit(*args, **kwargs):
        calls.append(True)
        raise AssertionError("GBTRregressor.fit must not run when unknown exceeds maxBins")

    monkeypatch.setattr(GBTRegressor, "fit", forbidden_fit)
    with pytest.raises(ValueError, match="including unknown exceed maxBins=256"):
        fit(train, tinyfixture=True, encoding_profile="native")
    assert calls == []


def test_native_serialization_metadata_reload_predictions_and_unknowns(spark, fitted, tmp_path):
    train, model = fitted
    target = tmp_path / "native-v22"
    save_pipeline(model, target)
    metadata_files = list(target.glob("stages/3_*/metadata/part-*"))
    assert len(metadata_files) == 1
    metadata = json.loads(metadata_files[0].read_text(encoding="utf-8"))
    assert metadata["paramMap"]["lossType"] == "squared"
    assert metadata["paramMap"]["maxBins"] == 256
    reloaded = load_pipeline(target)
    assert reloaded.stages[-1].getMaxBins() == 256
    assert reloaded.stages[-1].getLossType() == "squared"
    assert reloaded.stages[0].labels == model.stages[0].labels
    batch = spark.createDataFrame([values(0), values(1, "263", "262"), values(2)], SCHEMA)
    before = model.transform(batch).select(*FEATURE_COLUMNS, "prediction").orderBy(*FEATURE_COLUMNS).collect()
    after = reloaded.transform(batch).select(*FEATURE_COLUMNS, "prediction").orderBy(*FEATURE_COLUMNS).collect()
    assert max(abs(a["prediction"] - b["prediction"]) for a, b in zip(before, after, strict=True)) <= 1e-9
    assert validate_vector_metadata(reloaded.transform(batch), encoding_profile="native")["vector_dimensions"] == 8
    with pytest.raises(ValueError, match="already exists"):
        save_pipeline(model, target)


@pytest.mark.parametrize("loss", ["squared", "absolute"])
def test_old_one_hot_profiles_still_save_reload_without_encoding_changes(spark, tmp_path, loss):
    train = spark.createDataFrame([values(index) for index in range(24)], SCHEMA)
    model = fit(train, tinyfixture=True, loss_profile=loss)
    target = tmp_path / ("legacy-one-hot-" + loss)
    save_pipeline(model, target)
    reloaded = load_pipeline(target)
    assert len(reloaded.stages) == 5
    assert reloaded.stages[-1].getLossType() == loss and reloaded.stages[-1].getMaxBins() == 32
    assert validate_vector_metadata(reloaded.transform(train))["vector_dimensions"] > 8
    before = model.transform(train).select(*FEATURE_COLUMNS, "prediction").orderBy(*FEATURE_COLUMNS).collect()
    after = reloaded.transform(train).select(*FEATURE_COLUMNS, "prediction").orderBy(*FEATURE_COLUMNS).collect()
    assert [row["prediction"] for row in before] == [row["prediction"] for row in after]
