"""Spark ML training, weather ablation, slice diagnostics, and 2026 drift evaluation for Gold V3.

Trains and evaluates 5 model configurations on the Gold 2025 dataset:
- B0: Hierarchical & Global Median Baselines
- M1: LinearRegression (OneHotEncoder + numeric + weather features)
- M2: RandomForestRegressor (native categorical zones + numeric + weather features)
- M3: GBTRegressor WITH ERA5 weather features (14 features total)
- M3-nw: GBTRegressor WITHOUT weather features (10 base features — weather ablation & 2026 drift)
"""

from __future__ import annotations

from collections import defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics
import time
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from nyc_taxi.ml.gold_v3_data import (
    BASE_NUMERIC_FEATURES,
    BASE_ONLY_FEATURE_COLUMNS,
    CATEGORICAL_FEATURE_COLUMNS,
    CONTRACT_VERSION,
    DEFAULT_ZONES_CSV,
    FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
    ROOT,
    SPLITS,
    WEATHER_NUMERIC_FEATURES,
)
from nyc_taxi.ml.pretrip_training_model import metrics as compute_regression_metrics


LABEL_COLUMN = "fare_amount"
INDEX_COLUMNS = ("pickup_zone_index", "dropoff_zone_index")
ENCODED_COLUMNS = ("pickup_zone_encoded", "dropoff_zone_encoded")

LR_PARAMETERS = {
    "featuresCol": "features",
    "labelCol": LABEL_COLUMN,
    "predictionCol": "prediction",
    "maxIter": 50,
    "regParam": 0.1,
    "elasticNetParam": 0.0,
}

RF_PARAMETERS = {
    "featuresCol": "features",
    "labelCol": LABEL_COLUMN,
    "predictionCol": "prediction",
    "seed": 42,
    "numTrees": 30,
    "maxDepth": 8,
    "maxBins": 264,
    "subsamplingRate": 0.8,
    "minInstancesPerNode": 5,
    "maxMemoryInMB": 256,
}

GBT_V3_PARAMETERS = {
    "featuresCol": "features",
    "labelCol": LABEL_COLUMN,
    "predictionCol": "prediction",
    "seed": 42,
    "maxIter": 30,
    "maxDepth": 5,
    "stepSize": 0.1,
    "lossType": "squared",
    "subsamplingRate": 1.0,
    "minInstancesPerNode": 5,
    "maxBins": 264,
    "maxMemoryInMB": 256,
}

BASELINE_POLICY = {
    "version": "gold-v3-medians-1",
    "hour_bin_width": 6,
    "minimum_group_count": 5,
}


def _hour_bin_from_trig(sin_val: float, cos_val: float) -> int:
    angle = math.atan2(float(sin_val), float(cos_val))
    if angle < 0.0:
        angle += 2.0 * math.pi
    fractional_hour = (angle * 24.0 / (2.0 * math.pi)) % 24.0
    hour = int(math.floor(fractional_hour + 1e-9)) % 24
    return hour // BASELINE_POLICY["hour_bin_width"]


def _hour_from_trig(sin_val: float, cos_val: float) -> int:
    angle = math.atan2(float(sin_val), float(cos_val))
    if angle < 0.0:
        angle += 2.0 * math.pi
    fractional_hour = (angle * 24.0 / (2.0 * math.pi)) % 24.0
    return int(math.floor(fractional_hour + 1e-9)) % 24


def fit_v3_baselines(
    pu_zones: Sequence[str],
    do_zones: Sequence[str],
    hour_sins: Sequence[float],
    hour_coss: Sequence[float],
    fares: Sequence[float],
) -> dict[str, Any]:
    """Fit exact global and hierarchical (PU, DO, 6h-bin) median baselines on train."""
    if not fares:
        raise ValueError("Cannot fit baseline on empty train split")
    all_fares = [float(f) for f in fares]
    pairs: dict[tuple[str, str], list[float]] = defaultdict(list)
    groups: dict[tuple[str, str, int], list[float]] = defaultdict(list)

    for pu, do, h_sin, h_cos, fare in zip(pu_zones, do_zones, hour_sins, hour_coss, all_fares, strict=True):
        h_bin = _hour_bin_from_trig(h_sin, h_cos)
        pair = (str(pu), str(do))
        pairs[pair].append(fare)
        groups[(*pair, h_bin)].append(fare)

    min_count = BASELINE_POLICY["minimum_group_count"]
    return {
        **BASELINE_POLICY,
        "train_count": len(all_fares),
        "global_median": float(statistics.median(all_fares)),
        "pair_groups": [
            {
                "pickup_zone": k[0],
                "dropoff_zone": k[1],
                "count": len(v),
                "median": float(statistics.median(v)),
            }
            for k, v in sorted(pairs.items())
            if len(v) >= min_count
        ],
        "time_groups": [
            {
                "pickup_zone": k[0],
                "dropoff_zone": k[1],
                "hour_bin": k[2],
                "count": len(v),
                "median": float(statistics.median(v)),
            }
            for k, v in sorted(groups.items())
            if len(v) >= min_count
        ],
    }


def predict_v3_baseline(
    pu_zones: Sequence[str],
    do_zones: Sequence[str],
    hour_sins: Sequence[float],
    hour_coss: Sequence[float],
    artifact: Mapping[str, Any],
) -> tuple[list[float], list[float]]:
    """Return (global_predictions, hierarchical_predictions) using fitted train medians."""
    global_med = float(artifact["global_median"])
    min_count = int(artifact.get("minimum_group_count", 5))
    pairs = {
        (g["pickup_zone"], g["dropoff_zone"]): float(g["median"])
        for g in artifact["pair_groups"]
        if g["count"] >= min_count
    }
    groups = {
        (g["pickup_zone"], g["dropoff_zone"], int(g["hour_bin"])): float(g["median"])
        for g in artifact["time_groups"]
        if g["count"] >= min_count
    }
    global_preds: list[float] = []
    hier_preds: list[float] = []
    for pu, do, h_sin, h_cos in zip(pu_zones, do_zones, hour_sins, hour_coss, strict=True):
        pair = (str(pu), str(do))
        h_bin = _hour_bin_from_trig(h_sin, h_cos)
        pred = groups.get((*pair, h_bin), pairs.get(pair, global_med))
        global_preds.append(global_med)
        hier_preds.append(pred)
    return global_preds, hier_preds


def make_linear_regression_pipeline(
    numeric_cols: Sequence[str] = NUMERIC_FEATURE_COLUMNS,
    *,
    tiny_test: bool = False,
) -> Any:
    """Create PySpark Pipeline: StringIndexer -> OneHotEncoder -> VectorAssembler -> LinearRegression."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
    from pyspark.ml.regression import LinearRegression

    indexers = [
        StringIndexer(
            inputCol=col,
            outputCol=idx_col,
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        )
        for col, idx_col in zip(CATEGORICAL_FEATURE_COLUMNS, INDEX_COLUMNS, strict=True)
    ]
    encoder = OneHotEncoder(
        inputCols=list(INDEX_COLUMNS),
        outputCols=list(ENCODED_COLUMNS),
        handleInvalid="keep",
        dropLast=False,
    )
    assembler = VectorAssembler(
        inputCols=[*numeric_cols, *ENCODED_COLUMNS],
        outputCol="features",
        handleInvalid="error",
    )
    params = dict(LR_PARAMETERS)
    if tiny_test:
        params["maxIter"] = 5
    regressor = LinearRegression(**params)
    return Pipeline(stages=[*indexers, encoder, assembler, regressor])


def make_random_forest_pipeline(
    numeric_cols: Sequence[str] = NUMERIC_FEATURE_COLUMNS,
    *,
    tiny_test: bool = False,
) -> Any:
    """Create PySpark Pipeline: StringIndexer -> VectorAssembler -> RandomForestRegressor."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    from pyspark.ml.regression import RandomForestRegressor

    indexers = [
        StringIndexer(
            inputCol=col,
            outputCol=idx_col,
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        )
        for col, idx_col in zip(CATEGORICAL_FEATURE_COLUMNS, INDEX_COLUMNS, strict=True)
    ]
    assembler = VectorAssembler(
        inputCols=[*numeric_cols, *INDEX_COLUMNS],
        outputCol="features",
        handleInvalid="error",
    )
    params = dict(RF_PARAMETERS)
    if tiny_test:
        params.update(numTrees=3, maxDepth=3, minInstancesPerNode=1)
    regressor = RandomForestRegressor(**params)
    return Pipeline(stages=[*indexers, assembler, regressor])


def make_gbt_v3_pipeline(
    numeric_cols: Sequence[str] = NUMERIC_FEATURE_COLUMNS,
    *,
    tiny_test: bool = False,
) -> Any:
    """Create PySpark Pipeline: StringIndexer -> VectorAssembler -> GBTRegressor (maxBins=264)."""
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, VectorAssembler
    from pyspark.ml.regression import GBTRegressor

    indexers = [
        StringIndexer(
            inputCol=col,
            outputCol=idx_col,
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        )
        for col, idx_col in zip(CATEGORICAL_FEATURE_COLUMNS, INDEX_COLUMNS, strict=True)
    ]
    assembler = VectorAssembler(
        inputCols=[*numeric_cols, *INDEX_COLUMNS],
        outputCol="features",
        handleInvalid="error",
    )
    params = dict(GBT_V3_PARAMETERS)
    if tiny_test:
        params.update(maxIter=2, maxDepth=2, minInstancesPerNode=1)
    regressor = GBTRegressor(**params)
    return Pipeline(stages=[*indexers, assembler, regressor])


def load_borough_lookup(zones_csv: Path = DEFAULT_ZONES_CSV) -> dict[int, str]:
    if not zones_csv.is_file():
        return {}
    with zones_csv.open(encoding="utf-8-sig", newline="") as stream:
        return {
            int(row["LocationID"]): row["Borough"]
            for row in csv.DictReader(stream)
            if row.get("LocationID", "").isdigit()
        }


def compute_slice_diagnostics(
    labels: Sequence[float],
    preds_weather: Sequence[float],
    preds_no_weather: Sequence[float],
    distances_km: Sequence[float],
    hour_sins: Sequence[float],
    hour_coss: Sequence[float],
    precipitations: Sequence[float],
    pu_ids: Sequence[int],
    borough_lookup: Mapping[int, str],
) -> dict[str, Any]:
    """Compute slice metrics by distance band, hour bucket, weather condition, and borough."""
    y = np.asarray(labels, dtype=np.float64)
    pw = np.asarray(preds_weather, dtype=np.float64)
    pnw = np.asarray(preds_no_weather, dtype=np.float64)
    dist = np.asarray(distances_km, dtype=np.float64)
    precip = np.asarray(precipitations, dtype=np.float64)
    hours = np.array([_hour_from_trig(s, c) for s, c in zip(hour_sins, hour_coss, strict=True)])
    boroughs = [borough_lookup.get(int(pid), "Unknown") for pid in pu_ids]

    def _slice_stat(mask: np.ndarray) -> dict[str, Any]:
        cnt = int(np.sum(mask))
        if cnt == 0:
            return {"count": 0, "m3_weather_mae_usd": None, "m3_no_weather_mae_usd": None}
        m_w = compute_regression_metrics(y[mask].tolist(), pw[mask].tolist())
        m_nw = compute_regression_metrics(y[mask].tolist(), pnw[mask].tolist())
        return {
            "count": cnt,
            "mean_actual_fare_usd": round(float(np.mean(y[mask])), 4),
            "m3_weather_mae_usd": round(float(m_w["mae_usd"]), 4) if m_w["mae_usd"] is not None else None,
            "m3_weather_rmse_usd": round(float(m_w["rmse_usd"]), 4) if m_w["rmse_usd"] is not None else None,
            "m3_no_weather_mae_usd": round(float(m_nw["mae_usd"]), 4) if m_nw["mae_usd"] is not None else None,
            "m3_no_weather_rmse_usd": round(float(m_nw["rmse_usd"]), 4) if m_nw["rmse_usd"] is not None else None,
        }

    distance_bands = {
        "short_lt_2km": _slice_stat(dist < 2.0),
        "medium_2_to_5km": _slice_stat((dist >= 2.0) & (dist < 5.0)),
        "long_5_to_10km": _slice_stat((dist >= 5.0) & (dist < 10.0)),
        "very_long_ge_10km": _slice_stat(dist >= 10.0),
    }

    time_buckets = {
        "night_00_05": _slice_stat((hours >= 0) & (hours <= 5)),
        "morning_peak_06_09": _slice_stat((hours >= 6) & (hours <= 9)),
        "midday_10_15": _slice_stat((hours >= 10) & (hours <= 15)),
        "evening_peak_16_19": _slice_stat((hours >= 16) & (hours <= 19)),
        "late_evening_20_23": _slice_stat((hours >= 20) & (hours <= 23)),
    }

    weather_conditions = {
        "clear_or_dry_precip_0": _slice_stat(precip <= 0.0),
        "rain_or_precip_gt_0": _slice_stat(precip > 0.0),
        "heavy_rain_ge_2mm": _slice_stat(precip >= 2.0),
    }

    borough_slices = {}
    for b_name in ("Manhattan", "Queens", "Brooklyn", "Bronx", "Staten Island"):
        b_mask = np.array([b == b_name for b in boroughs], dtype=bool)
        if np.any(b_mask):
            borough_slices[b_name] = _slice_stat(b_mask)

    return {
        "by_distance_band": distance_bands,
        "by_time_bucket": time_buckets,
        "by_weather_condition": weather_conditions,
        "by_pickup_borough": borough_slices,
    }
