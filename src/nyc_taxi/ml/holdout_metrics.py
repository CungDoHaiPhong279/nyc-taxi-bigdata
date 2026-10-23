"""One-pass, aggregate-only frozen-model holdout metrics and slices.

No fitting, prediction, filtering of invalid observations, clipping or tracking
occurs here. The caller supplies one cached prediction per eligible holdout row,
including ``fare_amount``, ``prediction``, ``distance_miles`` and the static
lookup's ``pickup_borough``. Both estimators are evaluated on those same rows.
PySpark imports are lazy; importing this module does not start any service.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame


QUANTILE_ACCURACY = 100_000
QUANTILE_PROBABILITIES = (0.5, 0.9, 0.95)
MIN_SLICE_COUNT = 100
MAX_AGGREGATE_ROWS = 64
DISTANCE_BANDS = ("0", "(0,1]", "(1,3]", "(3,5]", "(5,10]", "(10,20]", "(20,50]", "(50,200]")
FARE_BANDS = ("(0,10]", "(10,20]", "(20,50]", "(50,100]", "(100,1000]", "(1000,inf)")
REQUIRED_COLUMNS = ("fare_amount", "prediction", "distance_miles", "pickup_borough")
_COUNT_FIELDS = (
    "invalid_rows", "label_nonfinite_count", "prediction_nonfinite_count",
    "distance_nonfinite_count", "arithmetic_nonfinite_count",
    "label_negative_count", "label_zero_count", "distance_out_of_domain_count",
    "missing_borough_count", "gbt_negative_prediction_count", "gbt_zero_prediction_count",
)


class HoldoutMetricsError(ValueError):
    """No metrics are returned on failure; only small public diagnostic counts."""

    def __init__(self, message: str, counts: dict[str, int]) -> None:
        super().__init__(message)
        self.counts = dict(counts)


def _baseline(value: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("Holdout baseline must be a finite scalar") from None
    if not math.isfinite(result):
        raise ValueError("Holdout baseline must be a finite scalar")
    return result


def _finite(value: "Column") -> "Column":
    from pyspark.sql import functions as F

    return value.isNotNull() & ~F.isnan(value) & (F.abs(value) != float("inf"))


def _count_if(predicate: "Column") -> "Column":
    from pyspark.sql import functions as F

    return F.coalesce(F.sum(F.when(predicate, 1).otherwise(0)), F.lit(0)).cast("long")


def _distance_band(value: "Column") -> "Column":
    from pyspark.sql import functions as F

    result = F.when(value == 0, F.lit("0"))
    for lower, upper, label in (
        (0, 1, "(0,1]"), (1, 3, "(1,3]"), (3, 5, "(3,5]"),
        (5, 10, "(5,10]"), (10, 20, "(10,20]"),
        (20, 50, "(20,50]"), (50, 200, "(50,200]"),
    ):
        result = result.when((value > lower) & (value <= upper), F.lit(label))
    # Finite unexpected values remain counted, never clipped/dropped. Nonfinite
    # inputs cause the whole result to fail after the aggregate is collected.
    return result.otherwise(F.lit("outside_[0,200]"))


def _fare_band(value: "Column") -> "Column":
    from pyspark.sql import functions as F

    result = F.when(value <= 0, F.lit("nonpositive"))
    for lower, upper, label in (
        (0, 10, "(0,10]"), (10, 20, "(10,20]"), (20, 50, "(20,50]"),
        (50, 100, "(50,100]"), (100, 1000, "(100,1000]"),
    ):
        result = result.when((value > lower) & (value <= upper), F.lit(label))
    return result.otherwise(F.lit("(1000,inf)"))


def holdout_summary_frame(predictions: "DataFrame", baseline_value: float) -> "DataFrame":
    """Construct a lazy overall + three slice-dimension aggregate in one scan.

    Exploding four fixed labels duplicates *aggregation work*, not evaluation
    membership: every input contributes once to overall and once per dimension.
    Approximate quantiles use fixed accuracy 100,000 (relative rank error 1e-5,
    not an absolute USD error guarantee). No input observation is filtered out.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType, StringType

    baseline = _baseline(baseline_value)
    missing = sorted(set(REQUIRED_COLUMNS).difference(predictions.columns))
    if missing:
        raise ValueError("Holdout metric columns missing: " + ", ".join(missing))
    for name in REQUIRED_COLUMNS[:3]:
        if not isinstance(predictions.schema[name].dataType, NumericType):
            raise ValueError(f"Holdout {name} must have a numeric type")
    if not isinstance(predictions.schema["pickup_borough"].dataType, StringType):
        raise ValueError("Holdout pickup_borough must be the static lookup string")

    source = predictions.select(
        F.col("fare_amount").cast("double").alias("label"),
        F.col("prediction").cast("double").alias("prediction"),
        F.col("distance_miles").cast("double").alias("distance"),
        F.col("pickup_borough").alias("borough"),
    )
    label, predicted, distance = F.col("label"), F.col("prediction"), F.col("distance")
    baseline_error = F.abs(label - F.lit(baseline))
    gbt_error = F.abs(label - predicted)
    source = source.withColumns({
        "baseline_error": baseline_error,
        "gbt_error": gbt_error,
        "baseline_squared_error": baseline_error * baseline_error,
        "gbt_squared_error": gbt_error * gbt_error,
    })
    numeric_valid = _finite(label) & _finite(predicted)
    arithmetic_invalid = numeric_valid & (
        ~_finite(F.col("baseline_error")) | ~_finite(F.col("gbt_error"))
        | ~_finite(F.col("baseline_squared_error")) | ~_finite(F.col("gbt_squared_error"))
    )
    invalid = ~_finite(label) | ~_finite(predicted) | ~_finite(distance) | arithmetic_invalid
    borough_missing = F.col("borough").isNull() | (F.trim(F.col("borough")) == "")
    source = source.withColumns({
        "invalid_rows": invalid,
        "label_nonfinite_count": ~_finite(label),
        "prediction_nonfinite_count": ~_finite(predicted),
        "distance_nonfinite_count": ~_finite(distance),
        "arithmetic_nonfinite_count": arithmetic_invalid,
        "label_negative_count": label < 0,
        "label_zero_count": label == 0,
        "distance_out_of_domain_count": _finite(distance) & ~distance.between(0, 200),
        "missing_borough_count": borough_missing,
        "gbt_negative_prediction_count": predicted < 0,
        "gbt_zero_prediction_count": predicted == 0,
    })
    labels = F.array(
        F.struct(F.lit("overall").alias("slice_type"), F.lit("all").alias("slice_label")),
        F.struct(F.lit("distance").alias("slice_type"), _distance_band(distance).alias("slice_label")),
        F.struct(F.lit("pickup_borough").alias("slice_type"),
                 F.when(borough_missing, "(missing)").otherwise(F.col("borough")).alias("slice_label")),
        F.struct(F.lit("fare").alias("slice_type"), _fare_band(label).alias("slice_label")),
    )
    expanded = source.select("*", F.explode(labels).alias("slice")).select("*", "slice.*").drop("slice")
    aggregates = [F.count("*").alias("count"), F.var_pop("label").alias("label_variance")]
    aggregates.extend(_count_if(F.col(name)).alias(name) for name in _COUNT_FIELDS)
    for estimator in ("baseline", "gbt"):
        aggregates.extend((
            F.avg(estimator + "_error").alias(estimator + "_mae"),
            F.avg(estimator + "_squared_error").alias(estimator + "_mse"),
            F.percentile_approx(
                estimator + "_error", list(QUANTILE_PROBABILITIES), QUANTILE_ACCURACY,
            ).alias(estimator + "_quantiles"),
        ))
    return expanded.groupBy("slice_type", "slice_label").agg(*aggregates)


def _empty_record(kind: str, label: str) -> dict[str, Any]:
    metrics = {
        "mae_usd": None, "rmse_usd": None, "r2": None,
        "median_absolute_error_usd": None, "p90_absolute_error_usd": None,
        "p95_absolute_error_usd": None, "negative_prediction_count": 0,
        "zero_prediction_count": 0,
    }
    return {
        "slice_type": kind, "slice_label": label, "count": 0, "limited": True,
        "baseline": dict(metrics), "gbt": dict(metrics),
        "diagnostics": dict.fromkeys(_COUNT_FIELDS, 0),
    }


def _decode_record(row: dict[str, Any], baseline_value: float) -> dict[str, Any]:
    count = int(row["count"])
    diagnostics = {name: int(row[name]) for name in _COUNT_FIELDS}
    variance = float(row["label_variance"] or 0.0)
    if not math.isfinite(variance) or variance < 0:
        raise HoldoutMetricsError("Holdout variance overflowed; no metrics returned", {"count": count, **diagnostics})
    result: dict[str, Any] = {
        "slice_type": row["slice_type"], "slice_label": row["slice_label"],
        "count": count, "limited": count < MIN_SLICE_COUNT, "diagnostics": diagnostics,
    }
    for estimator in ("baseline", "gbt"):
        mae, mse = float(row[estimator + "_mae"]), float(row[estimator + "_mse"])
        quantiles = [float(value) for value in row[estimator + "_quantiles"]]
        if (
            not all(math.isfinite(value) and value >= 0 for value in (mae, mse, *quantiles))
            or len(quantiles) != len(QUANTILE_PROBABILITIES)
        ):
            raise HoldoutMetricsError("Holdout arithmetic overflowed; no metrics returned", {"count": count, **diagnostics})
        r2 = 1.0 - mse / variance if count >= 2 and variance > 0 else None
        if r2 is not None and not math.isfinite(r2):
            raise HoldoutMetricsError("Holdout R2 overflowed; no metrics returned", {"count": count, **diagnostics})
        result[estimator] = {
            "mae_usd": mae, "rmse_usd": math.sqrt(mse), "r2": r2,
            "median_absolute_error_usd": quantiles[0],
            "p90_absolute_error_usd": quantiles[1], "p95_absolute_error_usd": quantiles[2],
            "negative_prediction_count": count * int(baseline_value < 0) if estimator == "baseline" else diagnostics["gbt_negative_prediction_count"],
            "zero_prediction_count": count * int(baseline_value == 0) if estimator == "baseline" else diagnostics["gbt_zero_prediction_count"],
        }
    return result


def summarize_holdout(predictions: "DataFrame", baseline_value: float) -> dict[str, Any]:
    """Execute one bounded aggregate collect, failing the whole output on bad rows.

    NULL/NaN/Inf in any metric input, or squared-error overflow, raises
    HoldoutMetricsError with diagnostic counts. No finite-only metrics escape.
    Fixed distance/fare bands are included even when empty. Unexpected finite
    labels <=0 or distance outside [0,200] are counted in explicit extra buckets
    and diagnostics, never silently removed or clipped by this metrics helper.
    """
    baseline = _baseline(baseline_value)
    rows = holdout_summary_frame(predictions, baseline).limit(MAX_AGGREGATE_ROWS + 1).collect()
    if len(rows) > MAX_AGGREGATE_ROWS:
        raise HoldoutMetricsError("Too many holdout aggregate groups; no metrics returned", {"aggregate_rows_returned": len(rows)})
    records = [row.asDict() for row in rows]
    overall = next((row for row in records if row["slice_type"] == "overall"), None)
    if overall is None or not overall["count"]:
        raise HoldoutMetricsError("Holdout is empty; no metrics returned", {"count": 0, "invalid_rows": 0})
    counts = {"count": int(overall["count"]), **{name: int(overall[name]) for name in _COUNT_FIELDS}}
    if counts["invalid_rows"]:
        raise HoldoutMetricsError("Holdout contains nonfinite input/arithmetic; no metrics returned", counts)
    result: dict[str, Any] = {
        "status": "VALID", "count": counts["count"], "invalid_rows": 0,
        "overall": _decode_record(overall, baseline),
        "slices": {},
        "quantiles": {
            "method": "Spark percentile_approx of absolute error; empirical P50/P90/P95",
            "probabilities": list(QUANTILE_PROBABILITIES), "accuracy": QUANTILE_ACCURACY,
            "relative_rank_error_bound": 1.0 / QUANTILE_ACCURACY,
            "absolute_usd_error_bound": None, "interpolated_even_sample_median": False,
        },
        "limited_evidence_below_count": MIN_SLICE_COUNT,
        "slice_membership": "Every input contributes once to each dimension; slice dimensions must not be summed together",
    }
    for kind, fixed_labels in (("distance", DISTANCE_BANDS), ("pickup_borough", ()), ("fare", FARE_BANDS)):
        observed = {row["slice_label"]: row for row in records if row["slice_type"] == kind}
        labels = [*fixed_labels, *sorted(set(observed).difference(fixed_labels))]
        result["slices"][kind] = [
            _decode_record(observed[label], baseline) if label in observed else _empty_record(kind, label)
            for label in labels
        ]
        if sum(row["count"] for row in result["slices"][kind]) != counts["count"]:
            raise HoldoutMetricsError("Holdout slice membership differs from overall; no metrics returned", counts)
    return result
