"""Gold V3 Serving Predictor (supports M3 with ERA5 weather, M3-nw without weather,
and the deterministic JFK <-> Manhattan $70.00 Ratecode 2 flat-fare rule).
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import csv
from datetime import date, datetime
import json
import math
from pathlib import Path
import threading
import time
from typing import Any
from uuid import uuid4

from nyc_taxi.ml.gold_v3_data import (
    BASE_NUMERIC_FEATURES,
    BASE_ONLY_FEATURE_COLUMNS,
    CATEGORICAL_FEATURE_COLUMNS,
    CONTRACT_VERSION,
    FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
    WEATHER_NUMERIC_FEATURES,
)
from nyc_taxi.ml.gold_v3_training import predict_v3_baseline
from nyc_taxi.ml.inference import InferenceError, InputError, REQUEST_TIMEOUT_SECONDS
from nyc_taxi.ml.pretrip_v23_features import load_calendar, load_zone_centroids


JFK_LOCATION_ID = 132
EWR_LOCATION_ID = 1
JFK_MANHATTAN_FLAT_FARE_USD = 70.0


def gold_v3_identity() -> dict[str, str]:
    return {
        "model_version": "gold-v3",
        "contract_version": CONTRACT_VERSION,
    }


def derive_single_trip_features(
    *,
    pickup_location_id: int,
    dropoff_location_id: int,
    pickup_dt: datetime,
    calendar=None,
    centroids=None,
    weather: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Derive the 10 base features (and optional 4 weather features) for one trip."""
    if not (1 <= int(pickup_location_id) <= 263 and 1 <= int(dropoff_location_id) <= 263):
        raise InputError("pickup_location_id and dropoff_location_id must be in 1..263")

    cal = calendar or load_calendar()
    cen = centroids or load_zone_centroids()

    fractional_hour = float(pickup_dt.hour) + float(pickup_dt.minute) / 60.0
    hour_angle = 2.0 * math.pi * fractional_hour / 24.0
    dow = pickup_dt.weekday()
    dow_angle = 2.0 * math.pi * float(dow) / 7.0

    p_date = pickup_dt.date()
    is_hol = 1.0 if (p_date in cal.holidays or p_date.isoformat() in cal.holidays) else 0.0
    dist_km = float(cen.distance_km(int(pickup_location_id), int(dropoff_location_id)))
    same_zone = 1.0 if int(pickup_location_id) == int(dropoff_location_id) else 0.0

    row: dict[str, Any] = {
        "pickup_hour_sin": math.sin(hour_angle),
        "pickup_hour_cos": math.cos(hour_angle),
        "pickup_dow_sin": math.sin(dow_angle),
        "pickup_dow_cos": math.cos(dow_angle),
        "is_weekend": 1.0 if dow >= 5 else 0.0,
        "is_holiday": is_hol,
        "zone_centroid_distance_km": dist_km,
        "is_same_zone": same_zone,
        "pickup_zone": str(int(pickup_location_id)),
        "dropoff_zone": str(int(dropoff_location_id)),
    }
    if weather is not None:
        for w_col in WEATHER_NUMERIC_FEATURES:
            val = float(weather[w_col])
            if not math.isfinite(val):
                raise InputError(f"Non-finite weather feature: {w_col}")
            row[w_col] = val
    return row


def is_jfk_manhattan_flat_fare(
    pickup_location_id: int,
    dropoff_location_id: int,
    borough_by_id: dict[int, str],
) -> bool:
    """Return True if the route is JFK (132) <-> Manhattan (official TLC Ratecode 2)."""
    pu, do = int(pickup_location_id), int(dropoff_location_id)
    return (pu == JFK_LOCATION_ID and borough_by_id.get(do) == "Manhattan") or (
        do == JFK_LOCATION_ID and borough_by_id.get(pu) == "Manhattan"
    )


class FrozenGoldV3Predictor:
    """Bounded Spark predictor for Gold V3 (M3 weather + M3-nw no-weather + JFK flat-fare)."""

    identity = staticmethod(gold_v3_identity)

    def __init__(self, model_dir: Path, contract_dir: Path, zones_file: Path):
        self.model_dir = model_dir
        self.contract_dir = contract_dir
        self.zones_file = zones_file
        self.ready = False
        self.status = "starting"
        self.spark = None
        self.model_weather = None
        self.model_no_weather = None
        self.baselines = None
        self.calendar = None
        self.centroids = None
        self.zones: list[dict[str, Any]] = []
        self.borough_by_id: dict[int, str] = {}
        self.known_pickup: set[str] = set()
        self.known_dropoff: set[str] = set()
        self.startup_seconds: float | None = None
        self._busy = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="gold-v3-spark")
        self._closed = False
        self._load_started = False

    def load(self) -> None:
        if self._load_started:
            return
        self._load_started = True
        tick = time.perf_counter()
        try:
            self.calendar = load_calendar()
            self.centroids = load_zone_centroids()
            baselines_path = self.model_dir / "baselines.json"
            if baselines_path.is_file():
                self.baselines = json.loads(baselines_path.read_text(encoding="utf-8"))

            with self.zones_file.open(encoding="utf-8-sig", newline="") as stream:
                rows = list(csv.DictReader(stream))
            lookup = {int(r["LocationID"]): r for r in rows if r.get("LocationID", "").isdigit()}
            self.borough_by_id = {k: v["Borough"] for k, v in lookup.items()}

            from pyspark.ml import PipelineModel
            from pyspark.sql import SparkSession

            self.spark = (
                SparkSession.builder.master("local[2]")
                .appName("NYC-Taxi-Frozen-Gold-V3")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "America/New_York")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.default.parallelism", "2")
                .config("spark.sql.warehouse.dir", "file:///tmp/gold-v3-warehouse")
                .getOrCreate()
            )
            self.spark.sparkContext.setLogLevel("ERROR")

            self.model_weather = PipelineModel.load(str(self.model_dir / "pipeline_gbt_weather"))
            self.model_no_weather = PipelineModel.load(
                str(self.model_dir / "pipeline_gbt_no_weather")
            )
            self.known_pickup = set(self.model_weather.stages[0].labels)
            self.known_dropoff = set(self.model_weather.stages[1].labels)

            self.zones = [
                {
                    "location_id": k,
                    "zone": lookup[k]["Zone"],
                    "borough": lookup[k]["Borough"],
                    "supported_pickup": str(k) in self.known_pickup,
                    "supported_dropoff": str(k) in self.known_dropoff,
                }
                for k in range(1, 264)
                if k in lookup
            ]
            self.startup_seconds = round(time.perf_counter() - tick, 6)
            if not self._closed:
                self.ready = True
                self.status = "ready"
        except Exception:
            self.ready = False
            self.status = "model_load_failed"
            if self.spark is not None:
                try:
                    self.spark.stop()
                except Exception:
                    pass

    def readiness(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "status": self.status,
            "startup_seconds": self.startup_seconds,
            "supported_pickup_zones": len(self.known_pickup),
            "supported_dropoff_zones": len(self.known_dropoff),
            **gold_v3_identity(),
        }

    def zone_response(self) -> dict[str, Any]:
        return {
            "zones": self.zones,
            "pickup_location_ids": sorted(map(int, self.known_pickup)),
            "dropoff_location_ids": sorted(map(int, self.known_dropoff)),
            **gold_v3_identity(),
        }

    def _run_spark_predict(self, derived: dict[str, Any], has_weather: bool) -> dict[str, Any]:
        from pyspark.sql.types import DoubleType, StringType, StructField, StructType

        try:
            # Always evaluate M3-nw (base features)
            schema_nw = StructType(
                [
                    *[StructField(c, DoubleType(), False) for c in BASE_NUMERIC_FEATURES],
                    *[StructField(c, StringType(), False) for c in CATEGORICAL_FEATURE_COLUMNS],
                ]
            )
            df_nw = self.spark.createDataFrame(
                [tuple(derived[c] for c in BASE_ONLY_FEATURE_COLUMNS)], schema_nw
            )
            pred_nw = float(
                self.model_no_weather.transform(df_nw).select("prediction").take(1)[0]["prediction"]
            )

            pred_w = None
            if has_weather:
                schema_w = StructType(
                    [
                        *[StructField(c, DoubleType(), False) for c in NUMERIC_FEATURE_COLUMNS],
                        *[StructField(c, StringType(), False) for c in CATEGORICAL_FEATURE_COLUMNS],
                    ]
                )
                df_w = self.spark.createDataFrame(
                    [tuple(derived[c] for c in FEATURE_COLUMNS)], schema_w
                )
                pred_w = float(
                    self.model_weather.transform(df_w).select("prediction").take(1)[0]["prediction"]
                )

            return {"pred_weather": pred_w, "pred_no_weather": pred_nw}
        finally:
            self._busy.release()

    def predict(self, payload: dict[str, Any], *, timeout: float = REQUEST_TIMEOUT_SECONDS) -> dict[str, Any]:
        if not self.ready or self._closed:
            raise InferenceError("model_not_ready")

        try:
            pu_id = int(payload["pickup_location_id"])
            do_id = int(payload["dropoff_location_id"])
            raw_dt = str(payload["pickup_datetime"])
            if raw_dt.endswith("Z"):
                raw_dt = raw_dt[:-1] + "+00:00"
            pickup_dt = datetime.fromisoformat(raw_dt)
        except (KeyError, ValueError, TypeError) as exc:
            raise InputError("Invalid pickup_location_id, dropoff_location_id, or pickup_datetime") from exc

        if EWR_LOCATION_ID in (pu_id, do_id):
            raise InferenceError("special_rate_ewr_unsupported", 422)

        weather_dict = None
        if all(k in payload and payload[k] is not None for k in WEATHER_NUMERIC_FEATURES):
            weather_dict = {k: float(payload[k]) for k in WEATHER_NUMERIC_FEATURES}

        derived = derive_single_trip_features(
            pickup_location_id=pu_id,
            dropoff_location_id=do_id,
            pickup_dt=pickup_dt,
            calendar=self.calendar,
            centroids=self.centroids,
            weather=weather_dict,
        )

        baseline_h = None
        if self.baselines:
            _, h_preds = predict_v3_baseline(
                [derived["pickup_zone"]],
                [derived["dropoff_zone"]],
                [derived["pickup_hour_sin"]],
                [derived["pickup_hour_cos"]],
                self.baselines,
            )
            baseline_h = round(h_preds[0], 2)

        flat_fare = is_jfk_manhattan_flat_fare(pu_id, do_id, self.borough_by_id)
        unseen = {
            "pickup": derived["pickup_zone"] not in self.known_pickup,
            "dropoff": derived["dropoff_zone"] not in self.known_dropoff,
        }

        if not self._busy.acquire(blocking=False):
            raise InferenceError("service_busy")

        tick = time.perf_counter()
        try:
            future = self._executor.submit(
                self._run_spark_predict, derived, weather_dict is not None
            )
            preds = future.result(timeout=timeout)
        except FutureTimeout:
            raise InferenceError("prediction_timeout", 504) from None
        except Exception:
            raise InferenceError("prediction_failed") from None

        metered_usd = (
            preds["pred_weather"] if preds["pred_weather"] is not None else preds["pred_no_weather"]
        )
        final_fare = JFK_MANHATTAN_FLAT_FARE_USD if flat_fare else metered_usd
        warnings: list[str] = []
        if flat_fare:
            warnings.append(
                "Tuyến JFK (Zone 132) ↔ Manhattan áp dụng giá cước cố định (Ratecode 2 Flat Fare = $70.00 USD)."
            )
        if any(unseen.values()):
            warnings.append(
                "Zone đón/trả thuộc nhóm hiếm chưa xuất hiện trong tập train: sử dụng __unknown bucket."
            )

        return {
            "status": "supported",
            "fare_amount_usd": round(float(final_fare), 2),
            "metered_ml_estimate_usd": round(float(metered_usd), 2),
            "m3_weather_estimate_usd": (
                round(float(preds["pred_weather"]), 2)
                if preds["pred_weather"] is not None
                else None
            ),
            "m3_no_weather_estimate_usd": round(float(preds["pred_no_weather"]), 2),
            "baseline_hierarchical_usd": baseline_h,
            "flat_fare_applied": flat_fare,
            "ratecode_id": 2 if flat_fare else 1,
            "zone_centroid_distance_km": round(float(derived["zone_centroid_distance_km"]), 3),
            "unit": "USD",
            "unseen_categories": unseen,
            "warnings": warnings,
            "request_seconds": round(time.perf_counter() - tick, 6),
            **gold_v3_identity(),
        }

    def close(self) -> None:
        self._closed = True
        self.ready = False
        self._executor.shutdown(wait=False, cancel_futures=True)
        if self.spark is not None:
            try:
                self.spark.stop()
            except Exception:
                pass
