"""Offline exact-median/metrics tests; no Spark, data acquisition or real model."""

from __future__ import annotations

import json
import inspect
import math
import statistics

import pytest

from nyc_taxi.ml.pretrip_training_model import (
    BASELINE_POLICY, GBT_PARAMETERS, fit, fit_baselines, gbt_parameters,
    make_pipeline, metrics, predict_baseline,
)


def feature_row(hour=0, pickup="1", dropoff="2", fare=10.0):
    return {"pickup_hour_sin": math.sin(2 * math.pi * hour / 24),
            "pickup_hour_cos": math.cos(2 * math.pi * hour / 24),
            "pickup_dow_sin": 0.0, "pickup_dow_cos": 1.0,
            "is_weekend": 0.0, "is_holiday": 0.0,
            "pickup_zone": pickup, "dropoff_zone": dropoff, "fare_amount": fare}


def test_global_median_is_exact_including_even_count_and_extreme_tail():
    fares = [2.0, 4.0, 6.0, 1000000.0]
    artifact = fit_baselines([feature_row(fare=fare) for fare in fares])
    assert artifact["global_median"] == statistics.median(fares) == 5.0
    assert artifact["train_count"] == 4
    assert artifact["pair_groups"] == artifact["time_groups"] == []
    assert {name: artifact[name] for name in BASELINE_POLICY} == BASELINE_POLICY


def test_hierarchical_fallback_is_time_then_pair_then_global_at_exact_five():
    train = [feature_row(0, fare=value) for value in (10, 20, 30, 40, 50)]
    train += [feature_row(12, fare=value) for value in (100, 200, 300, 400)]
    train += [feature_row(6, "3", "4", value) for value in (1000, 2000, 3000, 4000)]
    artifact = fit_baselines(train)
    global_predictions, hierarchy = predict_baseline(
        [feature_row(5), feature_row(12), feature_row(6, "3", "4"), feature_row(18, "263", "262")],
        artifact,
    )
    assert hierarchy == [30.0, 50.0, artifact["global_median"], artifact["global_median"]]
    assert global_predictions == [artifact["global_median"]] * 4
    assert len(artifact["time_groups"]) == len(artifact["pair_groups"]) == 1
    assert artifact["time_groups"][0]["count"] == 5


@pytest.mark.parametrize("hour,bin_number", [(0, 0), (5, 0), (6, 1), (11, 1), (12, 2), (17, 2), (18, 3), (23, 3)])
def test_cyclic_hour_reconstruction_obeys_frozen_six_hour_boundaries(hour, bin_number):
    artifact = fit_baselines([feature_row(hour, fare=10 + index) for index in range(5)])
    assert artifact["time_groups"][0]["hour_bin"] == bin_number


def test_baseline_evaluation_never_reads_or_refits_on_labels_and_serializes():
    artifact = fit_baselines([feature_row(fare=value) for value in (1, 3, 5, 7, 9)])
    encoded = json.dumps(artifact, allow_nan=False)
    copied = json.loads(encoded)
    evaluate = feature_row(fare=999999999)
    del evaluate["fare_amount"]
    assert predict_baseline([evaluate], copied) == ([5.0], [5.0])
    assert json.dumps(artifact, allow_nan=False) == encoded


@pytest.mark.parametrize("mutation", [
    {"fare_amount": 0.0}, {"fare_amount": float("nan")}, {"fare_amount": float("inf")},
    {"pickup_zone": 1}, {"dropoff_zone": "264"}, {"pickup_zone": "01"},
    {"pickup_hour_sin": 0.4}, {"pickup_hour_cos": float("nan")},
])
def test_baseline_invalid_train_does_not_get_filtered(mutation):
    row = feature_row()
    row.update(mutation)
    with pytest.raises(ValueError):
        fit_baselines([row])


def test_baseline_empty_or_changed_policy_rejected():
    with pytest.raises(ValueError, match="nonempty"):
        fit_baselines([])
    artifact = fit_baselines([feature_row()])
    artifact["minimum_group_count"] = 4
    with pytest.raises(ValueError, match="policy"):
        predict_baseline([feature_row()], artifact)


def test_metrics_keep_negative_predictions_extreme_targets_and_exact_quantiles():
    labels, predictions = [1, 3, 10, 1000], [-1, 4, 8, 900]
    result = metrics(labels, predictions)
    errors = [2, 1, 2, 100]
    centered = sum((value - statistics.mean(labels)) ** 2 for value in labels)
    assert result["count"] == 4
    assert result["negative_prediction_count"] == 1
    assert result["nonfinite_prediction_count"] == result["nonfinite_label_count"] == 0
    assert result["mae_usd"] == statistics.mean(errors)
    assert result["rmse_usd"] == pytest.approx(math.sqrt(sum(error ** 2 for error in errors) / 4))
    assert result["r2"] == pytest.approx(1 - sum(error ** 2 for error in errors) / centered)
    assert result["p50_absolute_error_usd"] == 2.0
    assert result["p90_absolute_error_usd"] == pytest.approx(70.6)
    assert result["p95_absolute_error_usd"] == pytest.approx(85.3)
    assert result["metrics_status"] == "finite_all_rows"


@pytest.mark.parametrize("predictions", [[1, float("nan")], [1, float("inf")], [1, -float("inf")]])
def test_nonfinite_predictions_preserve_row_count_and_make_metrics_undefined(predictions):
    result = metrics([1, 2], predictions)
    assert result["count"] == 2
    assert result["nonfinite_prediction_count"] == 1
    assert result["metrics_status"] == "nonfinite_rows"
    assert all(result[name] is None for name in ("mae_usd", "rmse_usd", "r2", "p95_absolute_error_usd"))


def test_nonfinite_label_and_constant_label_r2_are_explicit():
    result = metrics([float("nan"), 3], [2, 3])
    assert result["nonfinite_label_count"] == 1
    assert result["mae_usd"] is None
    assert metrics([3, 3], [2, 4])["r2"] is None
    assert metrics([3], [3])["r2"] is None


def test_finite_overflow_is_not_reported_as_valid_metrics():
    result = metrics([1e308, 1e308], [-1e308, -1e308])
    assert result["count"] == 2
    assert result["metrics_status"] == "arithmetic_overflow"
    assert result["mae_usd"] is result["rmse_usd"] is None


@pytest.mark.parametrize("labels,predictions", [([], []), ([1], []), ([1, 2], [1])])
def test_metrics_missing_or_misaligned_rows_are_not_silently_truncated(labels, predictions):
    with pytest.raises(ValueError, match="equally sized nonempty"):
        metrics(labels, predictions)


def test_actual_regressor_policy_is_fixed_before_metrics():
    assert GBT_PARAMETERS == {
        "featuresCol": "features", "labelCol": "fare_amount", "predictionCol": "prediction",
        "seed": 42, "maxIter": 20, "maxDepth": 4, "stepSize": 0.1,
        "lossType": "squared", "subsamplingRate": 1.0, "minInstancesPerNode": 1,
        "maxBins": 32, "maxMemoryInMB": 128,
    }


def test_loss_profile_defaults_preserve_original_squared_policy():
    assert gbt_parameters() == GBT_PARAMETERS
    assert inspect.signature(make_pipeline).parameters["loss_profile"].default == "squared"
    assert inspect.signature(fit).parameters["loss_profile"].default == "squared"
    assert GBT_PARAMETERS["lossType"] == "squared"


def test_loss_profile_absolute_changes_only_effective_loss():
    squared, absolute = gbt_parameters("squared"), gbt_parameters("absolute")
    assert absolute["lossType"] == "absolute"
    assert {name for name in squared if squared[name] != absolute[name]} == {"lossType"}
    assert set(absolute) == set(squared)


def test_loss_profile_parameter_copies_cannot_mutate_original_policy():
    copy = gbt_parameters("absolute")
    copy["maxIter"] = 999
    copy["lossType"] = "unapproved"
    assert gbt_parameters("absolute")["maxIter"] == 20
    assert gbt_parameters()["lossType"] == GBT_PARAMETERS["lossType"] == "squared"


@pytest.mark.parametrize("profile", ["", "Absolute", "huber", "squared_error", None, 0])
def test_loss_profile_rejects_unapproved_values_before_spark(profile):
    with pytest.raises(ValueError, match="loss profile"):
        gbt_parameters(profile)
    with pytest.raises(ValueError, match="loss profile"):
        make_pipeline(loss_profile=profile)
    with pytest.raises(ValueError, match="loss profile"):
        fit(object(), loss_profile=profile)
