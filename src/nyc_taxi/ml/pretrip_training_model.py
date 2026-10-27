"""Train-only v2 pilot model and exact bounded-sample evaluation.

No Spark session, model, network request or data file is opened on import. This
module is independent of the frozen v1 helpers and production inference route.
"""

from __future__ import annotations

from collections import defaultdict
import math
from pathlib import Path
import statistics
from typing import Any, Iterable, Mapping, Sequence

from nyc_taxi.ml.pretrip_features import (
    CATEGORICAL_FEATURE_COLUMNS, FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS,
)


LABEL_COLUMN = "fare_amount"
INDEX_COLUMNS = tuple(f"{name}_index" for name in CATEGORICAL_FEATURE_COLUMNS)
ENCODED_COLUMNS = tuple(f"{name}_encoded" for name in CATEGORICAL_FEATURE_COLUMNS)
GBT_PARAMETERS = {
    "featuresCol": "features", "labelCol": LABEL_COLUMN, "predictionCol": "prediction",
    "seed": 42, "maxIter": 20, "maxDepth": 4, "stepSize": 0.1,
    "lossType": "squared", "subsamplingRate": 1.0, "minInstancesPerNode": 1,
    "maxBins": 32, "maxMemoryInMB": 128,
}
BASELINE_POLICY = {"version": "pretrip-v2-medians-1", "hour_bin_width": 6, "minimum_group_count": 5}


def project_model_inputs(dataframe: Any, include_label: bool = True) -> Any:
    """Keep only the eight contract features and optionally the separate label.

    Extra columns, including existing vectors, IDs, outcomes and lineage, are
    never forwarded to a fitted pipeline. Invalid rows cause failure, not silent
    removal. Casts do not convert zone IDs into ordinal numeric predictors.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType, StringType

    required = (*FEATURE_COLUMNS, LABEL_COLUMN) if include_label else FEATURE_COLUMNS
    if len(dataframe.columns) != len(set(dataframe.columns)) or not set(required).issubset(dataframe.columns):
        raise ValueError("V2 model input is missing or has duplicate allowlisted columns")
    for name in (*NUMERIC_FEATURE_COLUMNS, LABEL_COLUMN) if include_label else NUMERIC_FEATURE_COLUMNS:
        if not isinstance(dataframe.schema[name].dataType, NumericType):
            raise ValueError("V2 numeric feature/label schema mismatch")
    for name in CATEGORICAL_FEATURE_COLUMNS:
        if not isinstance(dataframe.schema[name].dataType, StringType):
            raise ValueError("V2 zones must be string categories, never ordinal IDs")
    columns = [F.col(name).cast("double").alias(name) for name in NUMERIC_FEATURE_COLUMNS]
    columns.extend(F.col(name) for name in CATEGORICAL_FEATURE_COLUMNS)
    if include_label:
        columns.append(F.col(LABEL_COLUMN).cast("double").alias(LABEL_COLUMN))
    projected = dataframe.select(*columns)
    invalid = F.lit(False)
    for name in (*NUMERIC_FEATURE_COLUMNS, LABEL_COLUMN) if include_label else NUMERIC_FEATURE_COLUMNS:
        value = F.col(name)
        invalid = invalid | value.isNull() | F.isnan(value) | (F.abs(value) == float("inf"))
    for name in ("is_weekend", "is_holiday"):
        invalid = invalid | ~F.col(name).isin(0.0, 1.0)
    for name in CATEGORICAL_FEATURE_COLUMNS:
        value = F.col(name)
        pattern_matches = value.rlike(r"^[1-9][0-9]{0,2}$")
        invalid = invalid | value.isNull() | ~pattern_matches
        invalid = invalid | ~F.when(pattern_matches, value).cast("int").between(1, 263)
    if include_label:
        invalid = invalid | (F.col(LABEL_COLUMN) <= 0.0)
    if projected.where(invalid).limit(1).count():
        raise ValueError("V2 model input contains invalid or nonfinite feature/label rows")
    return projected


def make_pipeline(tiny_test: bool = False) -> Any:
    """Create fresh estimators: categorical dictionaries fit the supplied train only.

    One-hot expanded zones preserve category semantics without treating their
    StringIndexer indices as geographic magnitudes. tiny_test changes one tree
    solely for offline synthetic tests, never for the actual CLI attempt.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
    from pyspark.ml.regression import GBTRegressor

    indexers = [StringIndexer(inputCol=name, outputCol=output, handleInvalid="keep",
                              stringOrderType="alphabetAsc")
                for name, output in zip(CATEGORICAL_FEATURE_COLUMNS, INDEX_COLUMNS, strict=True)]
    encoder = OneHotEncoder(inputCols=list(INDEX_COLUMNS), outputCols=list(ENCODED_COLUMNS),
                            handleInvalid="keep", dropLast=False)
    assembler = VectorAssembler(inputCols=[*NUMERIC_FEATURE_COLUMNS, *ENCODED_COLUMNS],
                                outputCol="features", handleInvalid="error")
    parameters = dict(GBT_PARAMETERS)
    if tiny_test:
        parameters.update(maxIter=1, maxDepth=1)
    return Pipeline(stages=[*indexers, encoder, assembler, GBTRegressor(**parameters)])


def validate_vector_metadata(transformed_df: Any) -> dict[str, Any]:
    """Fail if a vector contains anything but the six numeric and two OHE groups."""
    if "features" not in transformed_df.columns:
        raise ValueError("V2 allowlisted feature vector is missing")
    metadata = transformed_df.schema["features"].metadata.get("ml_attr", {})
    attributes = [attribute for group in metadata.get("attrs", {}).values() for attribute in group]
    ordered = sorted(attributes, key=lambda item: item.get("idx", -1))
    names = [item.get("name") for item in ordered]
    if (not ordered or metadata.get("num_attrs") != len(ordered)
            or [item.get("idx") for item in ordered] != list(range(len(ordered)))
            or names[:len(NUMERIC_FEATURE_COLUMNS)] != list(NUMERIC_FEATURE_COLUMNS)):
        raise ValueError("V2 feature vector metadata violates the strict allowlist")
    counts: dict[str, int] = {}
    position = len(NUMERIC_FEATURE_COLUMNS)
    binaries = {item.get("idx") for item in metadata.get("attrs", {}).get("binary", [])}
    for name in ENCODED_COLUMNS:
        start = position
        while position < len(names) and isinstance(names[position], str) and names[position].startswith(name + "_"):
            if position not in binaries:
                raise ValueError("V2 categorical feature must remain one-hot binary")
            position += 1
        counts[name] = position - start
        if counts[name] < 2:
            raise ValueError("V2 categorical feature/unknown bucket metadata is missing")
    if position != len(names):
        raise ValueError("V2 feature vector includes a non-allowlisted predictor")
    return {"input_feature_names": list(FEATURE_COLUMNS), "vector_dimensions": len(ordered),
            "categorical_one_hot_dimensions": counts}


def fit(train_df: Any, *, tinyfixture: bool = False) -> Any:
    """Fit exactly once on the supplied train frame; no validation/test parameter."""
    projected = project_model_inputs(train_df)
    model = make_pipeline(tiny_test=tinyfixture).fit(projected)
    validate_vector_metadata(model.transform(projected))
    return model


def _assert_fitted_pipeline(model: Any) -> None:
    from pyspark.ml.feature import OneHotEncoderModel, StringIndexerModel, VectorAssembler
    from pyspark.ml.regression import GBTRegressionModel

    if len(model.stages) != 5:
        raise ValueError("V2 fitted pipeline stage count mismatch")
    for stage, source, output in zip(model.stages[:2], CATEGORICAL_FEATURE_COLUMNS, INDEX_COLUMNS, strict=True):
        if (not isinstance(stage, StringIndexerModel) or stage.getInputCol() != source
                or stage.getOutputCol() != output or stage.getHandleInvalid() != "keep"):
            raise ValueError("V2 fitted categorical contract mismatch")
    encoder, assembler, regressor = model.stages[2:]
    if (not isinstance(encoder, OneHotEncoderModel) or encoder.getInputCols() != list(INDEX_COLUMNS)
            or encoder.getOutputCols() != list(ENCODED_COLUMNS) or encoder.getDropLast()
            or encoder.getHandleInvalid() != "keep"
            or not isinstance(assembler, VectorAssembler)
            or assembler.getInputCols() != [*NUMERIC_FEATURE_COLUMNS, *ENCODED_COLUMNS]
            or assembler.getOutputCol() != "features" or not isinstance(regressor, GBTRegressionModel)):
        raise ValueError("V2 fitted vector/regression contract mismatch")


def fitted_unknown_counts(model: Any, df: Any) -> dict[str, Any]:
    """Count valid zones absent from the train dictionaries without fitting again."""
    from pyspark.sql import functions as F

    _assert_fitted_pipeline(model)
    expressions = [F.count("*").alias("count")]
    unknown = []
    for stage, name in zip(model.stages[:2], CATEGORICAL_FEATURE_COLUMNS, strict=True):
        expression = F.col(name).isNull() | ~F.col(name).isin(list(stage.labels))
        unknown.append(expression)
        expressions.append(F.sum(F.when(expression, 1).otherwise(0)).alias(name))
    expressions.append(F.sum(F.when(unknown[0] | unknown[1], 1).otherwise(0)).alias("either"))
    row = df.agg(*expressions).first().asDict()
    size = int(row["count"])
    return {"count": size, **{name: {"count": int(row[name] or 0),
                                  "rate": int(row[name] or 0) / size if size else None}
                             for name in (*CATEGORICAL_FEATURE_COLUMNS, "either")}}


def _hour_bin(row: Mapping[str, Any]) -> int:
    sine, cosine = float(row["pickup_hour_sin"]), float(row["pickup_hour_cos"])
    if not math.isfinite(sine) or not math.isfinite(cosine) or abs(math.hypot(sine, cosine) - 1.0) > 1e-9:
        raise ValueError("Baseline requires the v2 integer-hour cyclic features")
    position = math.atan2(sine, cosine) * 24.0 / (2.0 * math.pi)
    hour = int(round(position)) % 24
    if abs(sine - math.sin(2.0 * math.pi * hour / 24)) > 1e-9 or abs(cosine - math.cos(2.0 * math.pi * hour / 24)) > 1e-9:
        raise ValueError("Baseline requires the v2 integer-hour cyclic features")
    return hour // BASELINE_POLICY["hour_bin_width"]


def _row_group(row: Mapping[str, Any]) -> tuple[str, str]:
    values = tuple(row[name] for name in CATEGORICAL_FEATURE_COLUMNS)
    if any(not isinstance(value, str) or not value.isascii() or not value.isdigit()
           or str(int(value)) != value or not 1 <= int(value) <= 263 for value in values):
        raise ValueError("Baseline zones must follow the string categorical contract")
    return values  # type: ignore[return-value]


def fit_baselines(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Exact train medians with the predeclared six-hour/minimum-five fallback."""
    values: list[float] = []
    pairs: dict[tuple[str, str], list[float]] = defaultdict(list)
    groups: dict[tuple[str, str, int], list[float]] = defaultdict(list)
    for row in rows:
        fare = float(row[LABEL_COLUMN])
        if not math.isfinite(fare) or fare <= 0:
            raise ValueError("Baseline train labels must be finite and positive")
        pair, hour = _row_group(row), _hour_bin(row)
        values.append(fare)
        pairs[pair].append(fare)
        groups[(*pair, hour)].append(fare)
    if not values:
        raise ValueError("Baseline requires nonempty train rows")
    minimum = BASELINE_POLICY["minimum_group_count"]
    return {**BASELINE_POLICY, "train_count": len(values), "global_median": statistics.median(values),
            "pair_groups": [{"pickup_zone": key[0], "dropoff_zone": key[1], "count": len(fares),
                             "median": statistics.median(fares)}
                            for key, fares in sorted(pairs.items()) if len(fares) >= minimum],
            "time_groups": [{"pickup_zone": key[0], "dropoff_zone": key[1], "hour_bin": key[2],
                             "count": len(fares), "median": statistics.median(fares)}
                            for key, fares in sorted(groups.items()) if len(fares) >= minimum]}


def predict_baseline(rows: Iterable[Mapping[str, Any]], artifact: Mapping[str, Any]) -> tuple[list[float], list[float]]:
    """Apply immutable train-only medians; never inspect evaluation labels."""
    if any(artifact.get(name) != value for name, value in BASELINE_POLICY.items()):
        raise ValueError("Baseline artifact policy mismatch")
    global_median = float(artifact["global_median"])
    if not math.isfinite(global_median) or global_median <= 0:
        raise ValueError("Baseline artifact median must be finite and positive")
    pairs = {(group["pickup_zone"], group["dropoff_zone"]): float(group["median"])
             for group in artifact["pair_groups"] if group["count"] >= BASELINE_POLICY["minimum_group_count"]}
    groups = {(group["pickup_zone"], group["dropoff_zone"], group["hour_bin"]): float(group["median"])
              for group in artifact["time_groups"] if group["count"] >= BASELINE_POLICY["minimum_group_count"]}
    global_predictions, hierarchical_predictions = [], []
    for row in rows:
        pair, hour = _row_group(row), _hour_bin(row)
        predicted = groups.get((*pair, hour), pairs.get(pair, global_median))
        if not math.isfinite(predicted):
            raise ValueError("Baseline artifact contains nonfinite group predictions")
        global_predictions.append(global_median)
        hierarchical_predictions.append(predicted)
    return global_predictions, hierarchical_predictions


def _quantile(sorted_values: Sequence[float], probability: float) -> float:
    index = (len(sorted_values) - 1) * probability
    lower, upper = math.floor(index), math.ceil(index)
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * (index - lower)


def metrics(labels: Sequence[float], predictions: Sequence[float]) -> dict[str, Any]:
    """Exact bounded-sample metrics on every row; no clipping or invalid-row drop.

    Undefined metrics are explicit if any label/prediction is nonfinite; counts
    remain available so a model failure cannot be hidden by filtering rows.
    Absolute-error quantiles use linear interpolation at (N-1)*p.
    """
    if len(labels) != len(predictions) or not labels:
        raise ValueError("Metrics require equally sized nonempty rows")
    actual, predicted = list(map(float, labels)), list(map(float, predictions))
    invalid_labels = sum(not math.isfinite(value) for value in actual)
    invalid_predictions = sum(not math.isfinite(value) for value in predicted)
    result = {"count": len(actual), "nonfinite_label_count": invalid_labels,
              "nonfinite_prediction_count": invalid_predictions,
              "negative_prediction_count": sum(value < 0 for value in predicted),
              "metrics_status": "nonfinite_rows" if invalid_labels or invalid_predictions else "finite_all_rows",
              "mae_usd": None, "rmse_usd": None, "r2": None,
              "p50_absolute_error_usd": None, "p90_absolute_error_usd": None, "p95_absolute_error_usd": None}
    if invalid_labels or invalid_predictions:
        return result
    errors = [abs(left - right) for left, right in zip(actual, predicted, strict=True)]
    try:
        squared_error = math.fsum(value * value for value in errors)
        mean = math.fsum(actual) / len(actual)
        centered_sum = math.fsum((value - mean) * (value - mean) for value in actual)
        mae, rmse = math.fsum(errors) / len(errors), math.sqrt(squared_error / len(errors))
    except (OverflowError, ValueError):
        result["metrics_status"] = "arithmetic_overflow"
        return result
    if not all(math.isfinite(value) for value in (squared_error, centered_sum, mae, rmse)):
        result["metrics_status"] = "arithmetic_overflow"
        return result
    ordered = sorted(errors)
    result.update(mae_usd=mae, rmse_usd=rmse,
                  r2=1 - squared_error / centered_sum if len(actual) >= 2 and centered_sum > 0 else None,
                  p50_absolute_error_usd=_quantile(ordered, 0.5),
                  p90_absolute_error_usd=_quantile(ordered, 0.9),
                  p95_absolute_error_usd=_quantile(ordered, 0.95))
    return result


def save_pipeline(model: Any, path: str | Path) -> None:
    """Save a new fitted pipeline only; never overwrite an existing attempt."""
    _assert_fitted_pipeline(model)
    target = Path(path)
    if target.exists():
        raise ValueError("V2 pipeline destination already exists")
    model.write().save(str(target))


def load_pipeline(path: str | Path) -> Any:
    """Load the explicit attempt path; no latest-model lookup or fallback."""
    from pyspark.ml import PipelineModel

    model = PipelineModel.load(str(path))
    _assert_fitted_pipeline(model)
    return model
