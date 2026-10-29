"""Unit tests for Gold V3 baseline fitting, prediction, and slice diagnostics."""

from __future__ import annotations

import math

from nyc_taxi.ml.gold_v3_training import (
    compute_slice_diagnostics,
    fit_v3_baselines,
    predict_v3_baseline,
)


def test_fit_and_predict_v3_baselines() -> None:
    pu_zones = ["161"] * 6 + ["236"] * 6
    do_zones = ["236"] * 6 + ["161"] * 6
    # Hour 8 -> sin(8/24 * 2pi), cos(8/24 * 2pi)
    h8_sin = math.sin(2.0 * math.pi * 8.0 / 24.0)
    h8_cos = math.cos(2.0 * math.pi * 8.0 / 24.0)
    hour_sins = [h8_sin] * 12
    hour_coss = [h8_cos] * 12
    fares = [15.0] * 6 + [25.0] * 6

    artifact = fit_v3_baselines(pu_zones, do_zones, hour_sins, hour_coss, fares)
    assert artifact["global_median"] == 20.0
    assert len(artifact["pair_groups"]) == 2
    assert len(artifact["time_groups"]) == 2

    g_preds, h_preds = predict_v3_baseline(
        ["161", "236", "99"],
        ["236", "161", "105"],
        [h8_sin, h8_sin, h8_sin],
        [h8_cos, h8_cos, h8_cos],
        artifact,
    )
    assert g_preds == [20.0, 20.0, 20.0]
    # Known groups get 15.0 and 25.0; unseen pair falls back to global median 20.0
    assert h_preds == [15.0, 25.0, 20.0]


def test_compute_slice_diagnostics() -> None:
    labels = [10.0, 20.0, 35.0, 55.0]
    preds_w = [10.5, 19.0, 34.0, 52.0]
    preds_nw = [11.0, 18.5, 33.0, 50.0]
    distances = [1.2, 3.5, 7.0, 14.0]
    hour_sins = [0.0, 0.866, 0.0, -0.866]
    hour_coss = [1.0, 0.5, -1.0, -0.5]
    precips = [0.0, 0.5, 0.0, 3.0]
    pu_ids = [161, 161, 138, 132]
    borough_lookup = {161: "Manhattan", 138: "Queens", 132: "Queens"}

    diag = compute_slice_diagnostics(
        labels,
        preds_w,
        preds_nw,
        distances,
        hour_sins,
        hour_coss,
        precips,
        pu_ids,
        borough_lookup,
    )
    assert diag["by_distance_band"]["short_lt_2km"]["count"] == 1
    assert diag["by_weather_condition"]["rain_or_precip_gt_0"]["count"] == 2
    assert diag["by_pickup_borough"]["Manhattan"]["count"] == 2
    assert diag["by_pickup_borough"]["Queens"]["count"] == 2
