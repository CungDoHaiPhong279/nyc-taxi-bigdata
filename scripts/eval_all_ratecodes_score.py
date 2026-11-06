"""Step 2 of 2, read-only: score the 5% June-2025 sample with the served M3-nw + JFK rule, per ratecode cohort.

Run in a throwaway container with <out_dir> holding june_sample.parquet (written by eval_all_ratecodes_sample.py):
  docker run --rm --network none --hostname localhost -m 3g --memory-swap 3g --cpus 2 \
    -e PYSPARK_SUBMIT_ARGS="--driver-memory 2g --conf spark.ui.enabled=false pyspark-shell" \
    -v <repo>/distribution/models/gold-v3:/model-gold-v3:ro -v <repo>/scripts:/scripts:ro -v <out_dir>:/out \
    nyc-taxi-inference:gold-v3 python /scripts/eval_all_ratecodes_score.py 500
"""
import csv
import math
import json
import sys

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from nyc_taxi.ml.gold_v3_inference import BASE_ONLY_FEATURE_COLUMNS, JFK_LOCATION_ID
from nyc_taxi.ml.pretrip_v23_features import load_calendar, load_zone_centroids

MODEL = "/model-gold-v3/pipeline_gbt_no_weather"
ZONES = "/model-gold-v3/zones.csv"
GOLD = "/out/june_sample.parquet"
MAX_FARE = float(sys.argv[1])

spark = (
    SparkSession.builder.master("local[2]")
    .appName("score-june-all")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.session.timeZone", "America/New_York")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

cal = load_calendar()
cen = load_zone_centroids()
holidays = sorted(str(d) for d in cal.holidays)
with open(ZONES, encoding="utf-8-sig", newline="") as f:
    manhattan = [int(r["LocationID"]) for r in csv.DictReader(f) if r["Borough"] == "Manhattan"]
dist = spark.createDataFrame(
    [(i, j, float(cen.distance_km(i, j))) for i in range(1, 264) for j in range(1, 264)],
    ["pu", "do", "zone_centroid_distance_km"],
)

raw = spark.read.parquet(GOLD)
counts = {"june_rows_all": raw.count()}
counts["fare_le_zero"] = raw.filter(F.col("fare_amount") <= 0).count()
counts["fare_gt_max"] = raw.filter(F.col("fare_amount") > MAX_FARE).count()
counts["zone_out_of_range"] = raw.filter(
    ~(F.col("pickup_location_id").between(1, 263) & F.col("dropoff_location_id").between(1, 263))
).count()

ts = F.col("pickup_datetime_local")
frac = F.hour(ts) + F.minute(ts) / 60.0
hour_angle = 2 * math.pi * frac / 24.0
dow = F.expr("weekday(pickup_datetime_local)")
dow_angle = 2 * math.pi * dow / 7.0

df = (
    raw.filter(
        (F.col("fare_amount") > 0)
        & (F.col("fare_amount") <= MAX_FARE)
        & F.col("pickup_location_id").between(1, 263)
        & F.col("dropoff_location_id").between(1, 263)
        & (F.col("dropoff_datetime_local") > ts)
    )
    .withColumn("pu", F.col("pickup_location_id").cast("int"))
    .withColumn("do", F.col("dropoff_location_id").cast("int"))
    .join(F.broadcast(dist), ["pu", "do"])
    .withColumn("pickup_hour_sin", F.sin(hour_angle))
    .withColumn("pickup_hour_cos", F.cos(hour_angle))
    .withColumn("pickup_dow_sin", F.sin(dow_angle))
    .withColumn("pickup_dow_cos", F.cos(dow_angle))
    .withColumn("is_weekend", (dow >= 5).cast("double"))
    .withColumn("is_holiday", F.date_format(ts, "yyyy-MM-dd").isin(holidays).cast("double"))
    .withColumn("is_same_zone", (F.col("pu") == F.col("do")).cast("double"))
    .withColumn("pickup_zone", F.col("pu").cast("string"))
    .withColumn("dropoff_zone", F.col("do").cast("string"))
    .withColumn(
        "flat",
        ((F.col("pu") == JFK_LOCATION_ID) & F.col("do").isin(manhattan))
        | ((F.col("do") == JFK_LOCATION_ID) & F.col("pu").isin(manhattan)),
    )
)
assert all(c in df.columns for c in BASE_ONLY_FEATURE_COLUMNS)

scored = PipelineModel.load(MODEL).transform(df)
final = (
    scored.withColumn("pred", F.when(F.col("flat"), 70.0).otherwise(F.greatest(F.col("prediction"), F.lit(0.0))))
    .withColumn("y", F.col("fare_amount").cast("double"))
    .withColumn("err", F.col("pred") - F.col("y"))
    .withColumn(
        "cohort",
        F.when(F.col("ratecode_id").isNull(), "ratecode_null")
        .when(F.col("ratecode_id") == 1, "ratecode_1")
        .when((F.col("ratecode_id") == 2) & F.col("flat"), "ratecode_2_jfk_route")
        .when(F.col("ratecode_id") == 2, "ratecode_2_other")
        .otherwise("ratecode_other"),
    )
    .select("cohort", "y", "pred", "err", "flat", "ratecode_id", "pickup_datetime_local", "dropoff_datetime_local")
    .cache()
)


def metrics(frame):
    row = frame.agg(
        F.count("*").alias("n"),
        F.avg(F.abs("err")).alias("mae"),
        F.sqrt(F.avg(F.col("err") * F.col("err"))).alias("rmse"),
        F.avg("err").alias("bias"),
        F.avg("y").alias("mean_fare"),
        F.variance("y").alias("var_y"),
        F.percentile_approx(F.abs("err"), [0.5, 0.9, 0.95], 10000).alias("q"),
    ).first()
    n = row["n"]
    if not n:
        return {"n": 0}
    mse = row["rmse"] ** 2
    return {
        "n": n,
        "mae": round(row["mae"], 4),
        "rmse": round(row["rmse"], 4),
        "r2": round(1 - mse / row["var_y"], 4) if row["var_y"] else None,
        "bias": round(row["bias"], 4),
        "mean_fare": round(row["mean_fare"], 3),
        "p50": round(row["q"][0], 3),
        "p90": round(row["q"][1], 3),
        "p95": round(row["q"][2], 3),
    }


out = {"counts": counts, "max_fare": MAX_FARE, "cohorts": {"all_eligible": metrics(final)}}
for name in [r["cohort"] for r in final.select("cohort").distinct().collect()]:
    out["cohorts"][name] = metrics(final.filter(F.col("cohort") == name))
out["cohorts"]["all_except_null_ratecode"] = metrics(final.filter(F.col("ratecode_id").isNotNull()))
out["cohorts"]["ratecode_1_excl_flat_route"] = metrics(final.filter((F.col("ratecode_id") == 1) & ~F.col("flat")))

print("RESULT " + json.dumps(out, ensure_ascii=False), flush=True)
with open("/out/june_all_metrics.json", "w", encoding="utf-8") as f:
    json.dump(out, f, indent=1)
spark.stop()
