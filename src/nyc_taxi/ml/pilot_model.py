"""Bounded Spark fare-pilot features, train-only preprocessing, and evaluation.

The saved PipelineModel consumes ``prepare_features`` output, not raw Gold or a
public serving request. Feature derivation is fixed, unfitted code and must be
versioned alongside the model. No Spark session or tracking run starts on import.
"""

from __future__ import annotations

from datetime import date
import math
from typing import TYPE_CHECKING, Any

from nyc_taxi.etl.silver import FEDERAL_HOLIDAYS_2025

if TYPE_CHECKING:
    from pyspark.ml import Pipeline, PipelineModel
    from pyspark.ml.regression import GBTRegressor
    from pyspark.sql import DataFrame


INPUT_COLUMNS = (
    "trip_distance", "pickup_datetime_local", "pickup_location_id", "dropoff_location_id",
)
LABEL_COLUMN = "fare_amount"
NUMERIC_FEATURE_COLUMNS = (
    "distance_miles", "pickup_hour_sin", "pickup_hour_cos", "pickup_dow_sin",
    "pickup_dow_cos", "is_weekend", "is_holiday",
)
CATEGORICAL_INPUT_COLUMNS = ("pickup_zone", "dropoff_zone")
CATEGORICAL_FEATURE_COLUMNS = ("pickup_zone_index", "dropoff_zone_index")
FEATURE_COLUMNS = (*NUMERIC_FEATURE_COLUMNS, *CATEGORICAL_FEATURE_COLUMNS)
PREPARED_COLUMNS = (*NUMERIC_FEATURE_COLUMNS, *CATEGORICAL_INPUT_COLUMNS)
GBT_PARAMETERS = {
    "labelCol": LABEL_COLUMN,
    "featuresCol": "features",
    "predictionCol": "prediction",
    "lossType": "absolute",
    "seed": 42,
    "maxIter": 20,
    "maxDepth": 4,
    "stepSize": 0.1,
    "subsamplingRate": 0.8,
    "minInstancesPerNode": 20,
    "maxBins": 512,
    "maxMemoryInMB": 128,
}
BASELINE_RELATIVE_ERROR = 0.001


def prepare_features(dataframe: "DataFrame", *, include_labels: bool = True) -> "DataFrame":
    """Project only v1 inputs, derive calendar from NTZ, and reject bad domains.

    Extra Gold columns are deliberately discarded, not passed through to the
    assembler. Existing derived/holiday/weather/charge values are never trusted.
    Domain validation performs a bounded-result Spark action, never a full collect.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import IntegralType, NumericType, TimestampNTZType

    required = (*INPUT_COLUMNS, LABEL_COLUMN) if include_labels else INPUT_COLUMNS
    missing = sorted(set(required).difference(dataframe.columns))
    if missing:
        raise ValueError(f"Pilot input columns missing: {', '.join(missing)}")
    if not isinstance(dataframe.schema["pickup_datetime_local"].dataType, TimestampNTZType):
        raise ValueError("Pilot pickup_datetime_local must retain TimestampNTZType")
    for name in ("pickup_location_id", "dropoff_location_id"):
        if not isinstance(dataframe.schema[name].dataType, IntegralType):
            raise ValueError(f"Pilot {name} must have an integral type")
    for name in ("trip_distance", LABEL_COLUMN) if include_labels else ("trip_distance",):
        if not isinstance(dataframe.schema[name].dataType, NumericType):
            raise ValueError(f"Pilot {name} must have a numeric type")

    pickup = F.col("pickup_datetime_local")
    distance = F.col("trip_distance").cast("double")
    invalid = (
        pickup.isNull() | (F.year(pickup) != 2025)
        | distance.isNull() | F.isnan(distance) | ~distance.between(0.0, 200.0)
    )
    for name in ("pickup_location_id", "dropoff_location_id"):
        invalid = invalid | F.col(name).isNull() | ~F.col(name).between(1, 263)
    if include_labels:
        label = F.col(LABEL_COLUMN).cast("double")
        invalid = invalid | label.isNull() | F.isnan(label) | (label <= 0) | (label == float("inf"))
    if dataframe.where(invalid).limit(1).count():
        raise ValueError("Pilot input violates the positive-fare, 2025, distance [0,200], known-zone contract")

    hour = F.hour(pickup).cast("double")
    dow = F.pmod(F.dayofweek(pickup) + F.lit(5), F.lit(7)).cast("double")
    holidays = [date.fromisoformat(value) for value in sorted(FEDERAL_HOLIDAYS_2025)]
    columns = [
        distance.alias("distance_miles"),
        F.sin(hour * F.lit(2.0 * math.pi / 24.0)).alias("pickup_hour_sin"),
        F.cos(hour * F.lit(2.0 * math.pi / 24.0)).alias("pickup_hour_cos"),
        F.sin(dow * F.lit(2.0 * math.pi / 7.0)).alias("pickup_dow_sin"),
        F.cos(dow * F.lit(2.0 * math.pi / 7.0)).alias("pickup_dow_cos"),
        (dow >= 5).cast("double").alias("is_weekend"),
        F.to_date(pickup).isin(holidays).cast("double").alias("is_holiday"),
        F.col("pickup_location_id").cast("string").alias("pickup_zone"),
        F.col("dropoff_location_id").cast("string").alias("dropoff_zone"),
    ]
    if include_labels:
        columns.append(F.col(LABEL_COLUMN).cast("double").alias(LABEL_COLUMN))
    return dataframe.select(*columns)


def make_preprocessor() -> "Pipeline":
    """Create unfitted stages; category dictionaries must only see train rows."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, VectorAssembler

    stages = [
        StringIndexer(inputCol=source, outputCol=target, handleInvalid="keep", stringOrderType="alphabetAsc")
        for source, target in zip(CATEGORICAL_INPUT_COLUMNS, CATEGORICAL_FEATURE_COLUMNS, strict=True)
    ]
    stages.append(VectorAssembler(inputCols=list(FEATURE_COLUMNS), outputCol="features", handleInvalid="error"))
    return Pipeline(stages=stages)


def assert_category_metadata(dataframe: "DataFrame", *, max_bins: int = 512) -> dict[str, Any]:
    """Fail if category metadata was lost or exceeds the tree split budget."""
    if "features" not in dataframe.columns:
        raise ValueError("Assembled features column is missing")
    attributes = dataframe.schema["features"].metadata.get("ml_attr", {})
    groups = attributes.get("attrs", {})
    all_attributes = [item for values in groups.values() for item in values]
    actual_names = {item.get("name") for item in all_attributes}
    if attributes.get("num_attrs") != len(FEATURE_COLUMNS) or actual_names != set(FEATURE_COLUMNS):
        raise ValueError("Feature vector metadata does not match the strict pilot allowlist")
    nominal = {item.get("name"): item for item in groups.get("nominal", [])}
    categories = {}
    for index, name in enumerate(CATEGORICAL_FEATURE_COLUMNS, start=len(NUMERIC_FEATURE_COLUMNS)):
        attribute = nominal.get(name, {})
        values = attribute.get("vals", [])
        if attribute.get("idx") != index or not values or values[-1] != "__unknown":
            raise ValueError(f"Pilot category metadata/unknown bucket is missing: {name}")
        if len(values) > max_bins:
            raise ValueError(f"Pilot category cardinality exceeds maxBins: {name}")
        categories[name] = len(values)
    return {"feature_count": len(FEATURE_COLUMNS), "feature_names": list(FEATURE_COLUMNS), "categories": categories}


def fit_preprocessor(train_features: "DataFrame") -> "PipelineModel":
    """Fit only the supplied train sample; never concatenate validation or test."""
    model = make_preprocessor().fit(train_features)
    assert_category_metadata(model.transform(train_features))
    return model


def make_regressor(*, tiny_test: bool = False) -> "GBTRegressor":
    """Fixed P1 pilot parameters; tiny_test is solely for an offline fixture fit."""
    from pyspark.ml.regression import GBTRegressor

    parameters = dict(GBT_PARAMETERS)
    if tiny_test:
        parameters.update(maxIter=1, maxDepth=1, minInstancesPerNode=1)
    return GBTRegressor(**parameters)


def make_pipeline(*, tiny_test: bool = False) -> "Pipeline":
    """Create unfitted preprocessing + regressor; callers must fit on train only."""
    from pyspark.ml import Pipeline

    return Pipeline(stages=[*make_preprocessor().getStages(), make_regressor(tiny_test=tiny_test)])


def baseline_median(train_features: "DataFrame") -> float:
    """Train-sample median with the predeclared approximate-quantile error."""
    values = train_features.approxQuantile(LABEL_COLUMN, [0.5], BASELINE_RELATIVE_ERROR)
    if len(values) != 1 or not math.isfinite(values[0]):
        raise ValueError("A finite baseline requires a nonempty train sample")
    return float(values[0])


def evaluate_predictions(predictions: "DataFrame", baseline_value: float) -> dict[str, Any]:
    """Compare baseline/GBT on identical rows in one aggregate, without clipping.

    R2 uses stable population variance, not E[y^2]-E[y]^2. It is undefined (None)
    for fewer than two observations or a constant label, rather than hidden/forced.
    """
    from pyspark.sql import functions as F

    if not math.isfinite(baseline_value):
        raise ValueError("Baseline must be finite")
    if not {LABEL_COLUMN, "prediction"}.issubset(predictions.columns):
        raise ValueError("Evaluation requires fare_amount and prediction")
    values = predictions.select(
        F.col(LABEL_COLUMN).cast("double").alias("label"),
        F.col("prediction").cast("double").alias("prediction"),
    )
    label, predicted = F.col("label"), F.col("prediction")
    invalid = label.isNull() | predicted.isNull() | F.isnan(label) | F.isnan(predicted)
    invalid = invalid | (F.abs(label) == float("inf")) | (F.abs(predicted) == float("inf"))
    baseline_error, model_error = label - F.lit(float(baseline_value)), label - predicted
    aggregate = values.agg(
        F.count("*").alias("count"),
        F.sum(F.when(invalid, 1).otherwise(0)).alias("invalid"),
        F.var_pop(label).alias("variance"),
        F.avg(F.abs(baseline_error)).alias("baseline_mae"),
        F.avg(baseline_error * baseline_error).alias("baseline_mse"),
        F.avg(F.abs(model_error)).alias("gbt_mae"),
        F.avg(model_error * model_error).alias("gbt_mse"),
        F.sum(F.when(label > 1000, 1).otherwise(0)).alias("label_gt_1000_count"),
        F.sum(F.when(label > 10000, 1).otherwise(0)).alias("label_gt_10000_count"),
        F.sum(F.when(predicted < 0, 1).otherwise(0)).alias("negative_prediction_count"),
        F.sum(F.when(predicted == 0, 1).otherwise(0)).alias("zero_prediction_count"),
        F.min(label).alias("label_min"), F.max(label).alias("label_max"),
        F.min(predicted).alias("prediction_min"), F.max(predicted).alias("prediction_max"),
    ).first().asDict()
    if not aggregate["count"] or aggregate["invalid"]:
        raise ValueError("Evaluation requires nonempty, entirely finite label/prediction rows")
    variance = float(aggregate["variance"] or 0.0)
    result: dict[str, Any] = {"count": int(aggregate["count"])}
    for name in ("baseline", "gbt"):
        mae, mse = float(aggregate[f"{name}_mae"]), float(aggregate[f"{name}_mse"])
        if not all(math.isfinite(value) for value in (mae, mse, variance)):
            raise ValueError("Evaluation overflowed; labels and predictions were not clipped")
        result[name] = {
            "mae_usd": mae, "rmse_usd": math.sqrt(mse),
            "r2": 1.0 - mse / variance if result["count"] >= 2 and variance > 0 else None,
        }
    for key in ("label_gt_1000_count", "label_gt_10000_count", "negative_prediction_count", "zero_prediction_count"):
        result[key] = int(aggregate[key])
    for key in ("label_min", "label_max", "prediction_min", "prediction_max"):
        result[key] = float(aggregate[key])
    return result


def summarize_unseen_categories(preprocessor: "PipelineModel", prepared: "DataFrame") -> dict[str, Any]:
    """Measure unseen zones without refitting dictionaries on a holdout."""
    from pyspark.sql import functions as F

    transformed = preprocessor.transform(prepared)
    assert_category_metadata(transformed)
    expressions = [F.count("*").alias("count")]
    for stage, name in zip(preprocessor.stages[:2], CATEGORICAL_FEATURE_COLUMNS, strict=True):
        unknown_index = len(stage.labels)
        expressions.append(F.sum(F.when(F.col(name) == unknown_index, 1).otherwise(0)).alias(name))
    counts = transformed.agg(*expressions).first().asDict()
    size = int(counts["count"])
    return {
        "count": size,
        **{name: {"count": int(counts[name] or 0), "rate": float(counts[name] or 0) / size if size else None}
           for name in CATEGORICAL_FEATURE_COLUMNS},
    }


def compare_reloaded_predictions(
    original: "PipelineModel", reloaded: "PipelineModel", prepared_batch: "DataFrame",
    *, max_rows: int = 32, absolute_tolerance: float = 1e-9,
) -> dict[str, Any]:
    """Compare saved/reloaded models on the same bounded prepared batch, no fit."""
    from pyspark.sql import functions as F

    if not 1 <= max_rows <= 1000 or not math.isfinite(absolute_tolerance) or absolute_tolerance < 0:
        raise ValueError("Reload comparison requires a bounded batch and nonnegative finite tolerance")
    missing = set(PREPARED_COLUMNS).difference(prepared_batch.columns)
    if missing:
        raise ValueError("Reload comparison requires prepared feature columns")
    columns = [*PREPARED_COLUMNS]
    if LABEL_COLUMN in prepared_batch.columns:
        columns.append(LABEL_COLUMN)
    source = prepared_batch.select(*columns).limit(max_rows + 1).collect()
    if not source or len(source) > max_rows:
        raise ValueError("Reload comparison batch is empty or exceeds its explicit cap")
    batch = prepared_batch.sparkSession.createDataFrame(source, prepared_batch.select(*columns).schema)
    ordering = [F.col(name) for name in columns]
    before = original.transform(batch).select(*columns, "prediction").orderBy(*ordering).collect()
    after = reloaded.transform(batch).select(*columns, "prediction").orderBy(*ordering).collect()
    differences = []
    for left, right in zip(before, after, strict=True):
        if tuple(left[:-1]) != tuple(right[:-1]):
            raise ValueError("Reload comparison input rows changed")
        first, second = float(left["prediction"]), float(right["prediction"])
        if not math.isfinite(first) or not math.isfinite(second):
            raise ValueError("Reload comparison produced nonfinite predictions")
        differences.append(abs(first - second))
    maximum = max(differences)
    if maximum > absolute_tolerance:
        raise ValueError("Saved/reloaded predictions exceed the declared absolute tolerance")
    return {"count": len(source), "max_absolute_difference": maximum, "absolute_tolerance": absolute_tolerance}
