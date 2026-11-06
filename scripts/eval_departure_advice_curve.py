"""Read-only: the served M3-nw 24-hour curve for the routes eval_departure_advice.py selected.

Run in a throwaway container, with <out_dir> holding advice_routes.json:
  docker run --rm --network none --hostname localhost -m 3g --memory-swap 3g --cpus 2 \\
    -e PYSPARK_SUBMIT_ARGS="--driver-memory 2g --conf spark.ui.enabled=false pyspark-shell" \\
    -v <repo>/distribution/models/gold-v3:/model-gold-v3:ro -v <repo>/scripts:/scripts:ro -v <out_dir>:/out \\
    nyc-taxi-inference:gold-v3 python /scripts/eval_departure_advice_curve.py
"""
import json
from datetime import datetime

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from nyc_taxi.ml.gold_v3_inference import (
    BASE_NUMERIC_FEATURES,
    BASE_ONLY_FEATURE_COLUMNS,
    CATEGORICAL_FEATURE_COLUMNS,
    derive_single_trip_features,
)
from nyc_taxi.ml.pretrip_v23_features import load_calendar, load_zone_centroids

# A representative June 2025 weekday and Saturday, neither a holiday.
DATES = {0: (2025, 6, 11), 1: (2025, 6, 14)}

spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
routes = json.load(open("/out/advice_routes.json", encoding="utf-8"))
calendar, centroids = load_calendar(), load_zone_centroids()
keys, rows = [], []
for pu, do in routes:
    for weekend, day in DATES.items():
        for hour in range(24):
            features = derive_single_trip_features(
                pickup_location_id=pu, dropoff_location_id=do, pickup_dt=datetime(*day, hour, 30),
                calendar=calendar, centroids=centroids,
            )
            keys.append((pu, do, weekend, hour))
            rows.append(tuple(features[c] for c in BASE_ONLY_FEATURE_COLUMNS))
schema = StructType([
    *[StructField(c, DoubleType(), False) for c in BASE_NUMERIC_FEATURES],
    *[StructField(c, StringType(), False) for c in CATEGORICAL_FEATURE_COLUMNS],
])
frame = spark.createDataFrame(rows, schema).coalesce(1)
model = PipelineModel.load("/model-gold-v3/pipeline_gbt_no_weather")
predictions = [r["prediction"] for r in model.transform(frame).select("prediction").collect()]
with open("/out/advice_model.json", "w", encoding="utf-8") as stream:
    json.dump([[*key, round(float(p), 4)] for key, p in zip(keys, predictions)], stream)
spark.stop()
