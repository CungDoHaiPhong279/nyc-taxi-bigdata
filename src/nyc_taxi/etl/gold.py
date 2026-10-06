"""Silver-to-Gold trip enrichment without row loss or dimension fan-out.

Taxi TIMESTAMP_NTZ is treated as NYC wall-clock time for analytical alignment:
a project assumption, not an official TLC timezone guarantee.
"""

from __future__ import annotations

import logging
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from nyc_taxi.common.config import PROJECT_ROOT

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

WEATHER_VALUES = (
    "weather_timestamp_utc", "weather_timestamp_nyc", "temperature_2m",
    "precipitation", "snowfall", "wind_speed_10m", "weather_code",
)
MATCH_FLAGS = (
    "pickup_zone_matched", "dropoff_zone_matched", "calendar_matched", "weather_matched",
)


@dataclass(frozen=True)
class GoldTransformResult:
    enriched: "DataFrame"
    statistics: dict[str, Any]


def _relative_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _schema_summary(frame: "DataFrame") -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in frame.schema.fields}


def _require_columns(frame: "DataFrame", required: set[str], name: str) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{name} missing columns: {', '.join(missing)}")


def assert_no_fanout(before_count: int, after_count: int, join_name: str) -> None:
    if before_count != after_count:
        raise ValueError(
            f"{join_name} join row count changed from {before_count:,} to "
            f"{after_count:,} (loss or fan-out)"
        )


def _assert_unique_key(frame: "DataFrame", keys: tuple[str, ...], name: str) -> None:
    from pyspark.sql import functions as F

    _require_columns(frame, set(keys), name)
    if frame.filter(" OR ".join(f"`{key}` IS NULL" for key in keys)).limit(1).count():
        raise ValueError(f"{name} join key contains null")
    if frame.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{name} join key is not unique (duplicate dimension rows)")


def _hour_key(date_column: str, hour_column: str):
    from pyspark.sql import functions as F

    return F.concat(
        F.date_format(F.col(date_column), "yyyy-MM-dd"), F.lit(" "),
        F.lpad(F.col(hour_column).cast("string"), 2, "0"),
    )


def transform_gold(
    taxi_df: "DataFrame", zones_df: "DataFrame", calendar_df: "DataFrame",
    weather_df: "DataFrame",
) -> GoldTransformResult:
    """LEFT JOIN one Taxi source month to three unique Silver dimensions."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampNTZType

    _require_columns(taxi_df, {
        "pickup_datetime_local", "dropoff_datetime_local", "pickup_date", "pickup_hour",
        "pickup_location_id", "dropoff_location_id", "_source_file", "_source_dataset",
        "_ingestion_batch_id", "_source_year", "_source_month", "_silver_batch_id",
    }, "Taxi Silver")
    _require_columns(zones_df, {"location_id", "borough", "zone", "service_zone"}, "Taxi Zones Silver")
    _require_columns(calendar_df, {"date", "day_of_week", "is_weekend", "is_holiday", "holiday_name"}, "Calendar Silver")
    _require_columns(weather_df, {"weather_date_nyc", "weather_hour_nyc", *WEATHER_VALUES}, "Weather Silver")
    for field in ("pickup_datetime_local", "dropoff_datetime_local"):
        if not isinstance(taxi_df.schema[field].dataType, TimestampNTZType):
            raise ValueError(f"Taxi {field} must remain TIMESTAMP_NTZ")

    _assert_unique_key(zones_df, ("location_id",), "Taxi Zones")
    _assert_unique_key(calendar_df, ("date",), "Calendar")
    _assert_unique_key(weather_df, ("weather_date_nyc", "weather_hour_nyc"), "Weather NYC hour")
    bounds = weather_df.select(_hour_key("weather_date_nyc", "weather_hour_nyc").alias("hour_key")) \
        .agg(F.min("hour_key").alias("first_hour"), F.max("hour_key").alias("last_hour")).first()
    if bounds.first_hour is None or bounds.last_hour is None:
        raise ValueError("Weather Silver is empty")

    input_rows = before = taxi_df.count()
    current = taxi_df
    stage_counts: dict[str, dict[str, int]] = {}
    for side, taxi_key in (("pickup", "pickup_location_id"), ("dropoff", "dropoff_location_id")):
        key = f"_{side}_zone_key"
        lookup = F.broadcast(zones_df.select(
            F.col("location_id").alias(key),
            F.col("borough").alias(f"{side}_borough"),
            F.col("zone").alias(f"{side}_zone"),
            F.col("service_zone").alias(f"{side}_service_zone"),
        ))
        current = (current.join(lookup, F.col(taxi_key) == F.col(key), "left")
                   .withColumn(f"{side}_zone_matched", F.col(key).isNotNull()).drop(key))
        after = current.count()
        assert_no_fanout(before, after, f"{side} zone")
        stage_counts[f"{side}_zone"] = {"before": before, "after": after}
        before = after

    lookup = F.broadcast(calendar_df.select(
        F.col("date").alias("_calendar_key"), "day_of_week", "is_weekend",
        "is_holiday", "holiday_name",
    ))
    current = (current.join(lookup, F.col("pickup_date") == F.col("_calendar_key"), "left")
               .withColumn("calendar_matched", F.col("_calendar_key").isNotNull())
               .drop("_calendar_key"))
    after = current.count()
    assert_no_fanout(before, after, "calendar")
    stage_counts["calendar"] = {"before": before, "after": after}
    before = after

    lookup = F.broadcast(weather_df.select(
        F.col("weather_date_nyc").alias("_weather_date_key"),
        F.col("weather_hour_nyc").alias("_weather_hour_key"), *WEATHER_VALUES,
    ))
    current = (current.join(
        lookup,
        (F.col("pickup_date") == F.col("_weather_date_key"))
        & (F.col("pickup_hour") == F.col("_weather_hour_key")), "left",
    ).withColumn("weather_matched", F.col("_weather_date_key").isNotNull())
        .drop("_weather_date_key", "_weather_hour_key"))
    after = current.count()
    assert_no_fanout(before, after, "weather")
    stage_counts["weather"] = {"before": before, "after": after}

    local_hour = _hour_key("pickup_date", "pickup_hour")
    in_period = (F.year("pickup_date") == 2025) & F.month("pickup_date").between(1, 6)
    current = current.withColumn(
        "weather_unmatched_reason",
        F.when(F.col("weather_matched"), F.lit(None).cast("string"))
        .when(~in_period, "out_of_analysis_period")
        .when((local_hour < F.lit(bounds.first_hour)) | (local_hour > F.lit(bounds.last_hour)),
              "weather_edge_coverage")
        .otherwise("unexpected_missing_weather_key"),
    )

    metrics = [F.count("*").alias("output_rows")]
    for flag in MATCH_FLAGS:
        metrics.append(F.sum(F.when(F.col(flag), 1).otherwise(0)).alias(flag))
    metrics.extend((
        F.sum(F.when(F.col("is_holiday") == True, 1).otherwise(0)).alias("holiday_trip_count"),
        F.sum(F.when(F.col("precipitation") > 0, 1).otherwise(0)).alias("rainy_trip_count"),
        F.sum(F.when(F.col("precipitation") == 0, 1).otherwise(0)).alias("dry_trip_count"),
    ))
    for reason in ("out_of_analysis_period", "weather_edge_coverage", "unexpected_missing_weather_key"):
        metrics.append(F.sum(F.when(F.col("weather_unmatched_reason") == reason, 1)
                             .otherwise(0)).alias(f"reason_{reason}"))
    for field in WEATHER_VALUES:
        metrics.append(F.sum(F.when(F.col(field).isNull(), 1).otherwise(0)).alias(f"null_{field}"))
    summary = current.agg(*metrics).first().asDict()
    output_rows = int(summary["output_rows"])
    assert_no_fanout(input_rows, output_rows, "final Gold")
    joins = {
        flag.removesuffix("_matched"): {
            "matched": int(summary[flag] or 0),
            "unmatched": output_rows - int(summary[flag] or 0),
        } for flag in MATCH_FLAGS
    }
    return GoldTransformResult(current, {
        "input_rows": input_rows, "output_rows": output_rows,
        "stage_counts": stage_counts, "join_counts": joins,
        "weather_unmatched_reasons": {
            reason: int(summary[f"reason_{reason}"] or 0)
            for reason in ("out_of_analysis_period", "weather_edge_coverage", "unexpected_missing_weather_key")
        },
        "holiday_trip_count": int(summary["holiday_trip_count"] or 0),
        "weather_null_statistics": {field: int(summary[f"null_{field}"] or 0) for field in WEATHER_VALUES},
        "rainy_trip_count": int(summary["rainy_trip_count"] or 0),
        "dry_trip_count": int(summary["dry_trip_count"] or 0),
        "weather_local_coverage": {"first_hour": bounds.first_hour, "last_hour": bounds.last_hour},
    })


def _validate_silver_lineage(
    frame: "DataFrame", entry: dict[str, Any], name: str,
    source_year: int | None = None, source_month: int | None = None,
) -> None:
    """Reject stale manifest lineage before any Gold write."""
    from pyspark.sql import functions as F

    if entry.get("validation_status") != "valid" or not entry.get("silver_batch_id"):
        raise ValueError(f"{name} Silver manifest entry is not valid")
    fields = ("_source_file", "_source_dataset", "_ingestion_batch_id", "_silver_batch_id")
    _require_columns(frame, set(fields), f"{name} Silver lineage")
    metrics = [F.count("*").alias("rows")]
    for field in fields:
        metrics.extend((F.count(field).alias(f"count_{field}"),
                        F.min(field).alias(f"min_{field}"),
                        F.max(field).alias(f"max_{field}")))
    if source_year is not None:
        _require_columns(frame, {"_source_year", "_source_month"}, f"{name} source partition")
        for field in ("_source_year", "_source_month"):
            metrics.extend((F.min(field).alias(f"min_{field}"),
                            F.max(field).alias(f"max_{field}")))
    result = frame.agg(*metrics).first().asDict()
    rows = int(result["rows"])
    if rows != int(entry["silver_rows"]):
        raise ValueError(f"{name} Silver row count differs from manifest: {rows} != {entry['silver_rows']}")
    expected = {
        "_source_file": entry.get("source_raw_path"),
        "_source_dataset": entry.get("dataset"),
        "_ingestion_batch_id": entry.get("source_bronze_batch_id"),
        "_silver_batch_id": entry["silver_batch_id"],
    }
    for field, value in expected.items():
        if (result[f"count_{field}"] != rows
                or result[f"min_{field}"] != result[f"max_{field}"]
                or (value is not None and result[f"min_{field}"] != value)):
            raise ValueError(f"{name} physical Silver lineage {field} differs from manifest")
    if source_year is not None and (
        result["min__source_year"] != source_year
        or result["max__source_year"] != source_year
        or result["min__source_month"] != source_month
        or result["max__source_month"] != source_month
    ):
        raise ValueError(f"{name} source partition differs from selected year/month")


def _validate_gold_output(spark: "SparkSession", path: Path, entry: dict[str, Any]) -> None:
    """Read physical Parquet back and check count, schema, flags and lineage."""
    from pyspark.sql import functions as F

    frame = spark.read.parquet(str(path))
    if _schema_summary(frame) != entry["gold_schema"]:
        raise ValueError(f"Gold physical schema differs from manifest: {path}")
    lineage_fields = (
        "_gold_batch_id", "_silver_batch_id", "_source_file", "_source_dataset",
        "_ingestion_batch_id", "_source_year", "_source_month",
    )
    _require_columns(frame, set(MATCH_FLAGS) | set(lineage_fields) | {"_gold_processed_at"},
                     "Gold physical output")
    metrics = [F.count("*").alias("rows")]
    for flag in MATCH_FLAGS:
        metrics.extend((F.count(flag).alias(f"nonnull_{flag}"),
                        F.sum(F.when(F.col(flag), 1).otherwise(0)).alias(f"matched_{flag}")))
    for field in lineage_fields:
        metrics.extend((F.count(field).alias(f"nonnull_{field}"),
                        F.min(field).alias(f"min_{field}"),
                        F.max(field).alias(f"max_{field}")))
    metrics.append(F.count("_gold_processed_at").alias("nonnull_processed_at"))
    result = frame.agg(*metrics).first().asDict()
    rows = int(result["rows"])
    if rows != int(entry["gold_rows"]) or rows != int(entry["silver_rows"]):
        raise ValueError(f"Gold physical row count differs from Silver/manifest: {path}")
    for flag in MATCH_FLAGS:
        expected = entry["join_counts"][flag.removesuffix("_matched")]["matched"]
        if result[f"nonnull_{flag}"] != rows or int(result[f"matched_{flag}"] or 0) != expected:
            raise ValueError(f"Gold quality flag {flag} differs from manifest or contains null")
    expected_fields = {
        "_gold_batch_id": entry["gold_batch_id"],
        "_silver_batch_id": entry["source_silver_batch_id"],
        "_source_file": entry["source_lineage"]["raw_path"],
        "_source_dataset": "taxi",
        "_ingestion_batch_id": entry["source_lineage"]["bronze_batch_id"],
        "_source_year": entry["partition"]["year"],
        "_source_month": int(entry["partition"]["month"]),
    }
    for field, expected in expected_fields.items():
        if (result[f"nonnull_{field}"] != rows
                or result[f"min_{field}"] != expected
                or result[f"max_{field}"] != expected):
            raise ValueError(f"Gold physical lineage {field} differs from manifest")
    if result["nonnull_processed_at"] != rows:
        raise ValueError("Gold processed timestamp contains null")


def _analytical_summaries(frame: "DataFrame") -> dict[str, Any]:
    """Collect only small grouped summaries; never trip-level Taxi rows."""
    from pyspark.sql import functions as F

    borough = [
        {"pickup_borough": row["pickup_borough"], "trips": row["trips"]}
        for row in frame.groupBy("pickup_borough").count().withColumnRenamed("count", "trips")
        .orderBy(F.desc("trips"), "pickup_borough").collect()
    ]
    hour = [
        {"pickup_hour": row["pickup_hour"], "trips": row["trips"]}
        for row in frame.groupBy("pickup_hour").count().withColumnRenamed("count", "trips")
        .orderBy("pickup_hour").collect()
    ]
    day_type = [
        {"is_weekend": row["is_weekend"], "is_holiday": row["is_holiday"], "trips": row["trips"]}
        for row in frame.groupBy("is_weekend", "is_holiday").count()
        .withColumnRenamed("count", "trips").collect()
    ]
    rain = [
        {"weather_class": row["weather_class"], "trips": row["trips"],
         "average_fare": row["average_fare"]}
        for row in frame.withColumn(
            "weather_class",
            F.when(~F.col("weather_matched") | F.col("precipitation").isNull(), "unknown")
            .when(F.col("precipitation") > 0, "rainy").otherwise("dry"),
        ).groupBy("weather_class").agg(
            F.count("*").alias("trips"), F.avg("fare_amount").alias("average_fare")
        ).collect()
    ]
    return {"by_pickup_borough": borough, "by_pickup_hour": hour,
            "by_weekend_holiday": day_type, "by_precipitation": rain}


def _source_batches(silver_entry: dict[str, Any],
                    dimensions: dict[str, dict[str, Any]] | None) -> dict[str, str]:
    batches = {"taxi": silver_entry["silver_batch_id"]}
    if dimensions:
        for name in ("taxi-zones", "calendar", "weather"):
            batches[name] = dimensions[name]["silver_batch_id"]
    return batches


def build_gold_dataset(
    spark: "SparkSession", taxi_path: str | Path, zones_path: str | Path,
    calendar_path: str | Path, weather_path: str | Path, gold_path: str | Path,
    silver_entry: dict[str, Any], *, source_year: int, source_month: int,
    dimension_entries: dict[str, dict[str, Any]] | None = None,
    existing_entry: dict[str, Any] | None = None, force: bool = False,
) -> dict[str, Any]:
    """Build or validate one source-month Gold partition and return its manifest entry."""
    from pyspark.sql import functions as F

    started = time.monotonic()
    if source_year != 2025 or not 1 <= source_month <= 6:
        raise ValueError("Gold scope is Taxi source months 2025-01 through 2025-06")
    paths = [Path(path) for path in (taxi_path, zones_path, calendar_path, weather_path)]
    for path in paths:
        if not path.is_dir():
            raise FileNotFoundError(f"Missing Silver Parquet directory: {path}")
    target = Path(gold_path)
    if target.name != f"month={source_month:02d}" or target.parent.name != f"year={source_year}":
        raise ValueError("Gold target must be the selected Taxi source year/month partition")
    if target.exists() and not target.is_dir():
        raise ValueError(f"Gold target is not a directory: {target}")
    dataset_name = f"taxi_{source_year}-{source_month:02d}"
    if silver_entry.get("dataset_name") != dataset_name or silver_entry.get("dataset") != "taxi":
        raise ValueError("Taxi Silver manifest entry does not match selected source month")
    if dimension_entries and set(dimension_entries) != {"taxi-zones", "calendar", "weather"}:
        raise ValueError("Gold requires Taxi Zones, Calendar, and Weather Silver entries")
    current_batches = _source_batches(silver_entry, dimension_entries)
    if target.exists() and not force:
        if existing_entry is None or existing_entry.get("validation_status") != "valid":
            raise ValueError(f"Gold output exists without a valid manifest; use --force: {target}")
        if existing_entry.get("dataset_name") != dataset_name:
            raise ValueError("Existing Gold manifest dataset differs from selected month")
        if existing_entry.get("source_silver_batches") != current_batches:
            raise ValueError("Gold output is stale relative to Silver batches; use --force")
        _validate_gold_output(spark, target, existing_entry)
        logger.info("Skipping validated Gold partition %s", dataset_name)
        return {**existing_entry, "skipped": True}
    if existing_entry and not target.exists() and not force:
        raise ValueError("Gold manifest exists but physical partition is missing; use --force")

    taxi, zones, calendar, weather = (spark.read.parquet(str(path)) for path in paths)
    _validate_silver_lineage(taxi, silver_entry, "Taxi", source_year, source_month)
    if dimension_entries:
        for name, frame in (("taxi-zones", zones), ("calendar", calendar), ("weather", weather)):
            _validate_silver_lineage(frame, dimension_entries[name], name)
    result = transform_gold(taxi, zones, calendar, weather)
    if result.statistics["input_rows"] != int(silver_entry["silver_rows"]):
        raise ValueError("Taxi Silver physical row count differs from manifest")

    batch_id = uuid4().hex
    processed_at = datetime.now(timezone.utc).isoformat()
    output = (result.enriched.withColumn("_gold_batch_id", F.lit(batch_id))
              .withColumn("_gold_processed_at", F.current_timestamp()))
    source_lineage = {
        "raw_path": silver_entry.get("source_raw_path"),
        "raw_sha256": silver_entry.get("raw_sha256"),
        "bronze_path": silver_entry.get("source_bronze_path"),
        "bronze_batch_id": silver_entry.get("source_bronze_batch_id"),
        "silver_path": silver_entry.get("silver_path"),
        "silver_batch_id": silver_entry["silver_batch_id"],
        "dimension_silver_batches": {
            key: value for key, value in current_batches.items() if key != "taxi"
        },
    }
    entry: dict[str, Any] = {
        "dataset_name": dataset_name,
        "dataset": "taxi_enriched",
        "partition": {"year": source_year, "month": f"{source_month:02d}"},
        "source_silver_path": _relative_path(paths[0]),
        "gold_path": _relative_path(target),
        "silver_rows": result.statistics["input_rows"],
        "gold_rows": result.statistics["output_rows"],
        "join_counts": result.statistics["join_counts"],
        "join_stage_counts": result.statistics["stage_counts"],
        "weather_unmatched_reasons": result.statistics["weather_unmatched_reasons"],
        "weather_local_coverage": result.statistics["weather_local_coverage"],
        "weather_null_statistics": result.statistics["weather_null_statistics"],
        "holiday_trip_count": result.statistics["holiday_trip_count"],
        "rainy_trip_count": result.statistics["rainy_trip_count"],
        "dry_trip_count": result.statistics["dry_trip_count"],
        "source_silver_batch_id": silver_entry["silver_batch_id"],
        "source_silver_batches": current_batches,
        "source_lineage": source_lineage,
        "gold_batch_id": batch_id,
        "gold_schema": _schema_summary(output),
        "processed_at_utc": processed_at,
        "validation_status": "valid",
    }

    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(f".{target.name}.staging-{uuid4().hex}")
    backup = target.with_name(f".{target.name}.backup-{uuid4().hex}")
    installed = False
    backed_up = False
    try:
        output.write.mode("errorifexists").option("compression", "snappy").parquet(str(staging))
        _validate_gold_output(spark, staging, entry)
        entry["analytical_summaries"] = _analytical_summaries(spark.read.parquet(str(staging)))
        entry["output_size_bytes"] = sum(part.stat().st_size for part in staging.glob("part-*.parquet"))
        entry["elapsed_seconds"] = round(time.monotonic() - started, 2)
        if target.exists():
            if not force:
                raise ValueError("Gold output appeared during build; rerun or use --force")
            target.rename(backup)
            backed_up = True
        staging.rename(target)
        installed = True
        _validate_gold_output(spark, target, entry)
        if backed_up:
            shutil.rmtree(backup)
        logger.info("Validated Gold %s: %s rows", dataset_name, entry["gold_rows"])
        return entry
    except Exception:
        if installed and target.exists():
            shutil.rmtree(target)
        if backed_up and backup.exists():
            backup.rename(target)
        raise
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def build_gold(
    taxi_path: str | Path, weather_path: str | Path, calendar_path: str | Path,
    taxi_zones_path: str | Path, gold_path: str | Path,
) -> None:
    """Legacy convenience entry point; the manifest-aware CLI is preferred."""
    from nyc_taxi.common.spark import get_spark_session

    spark = get_spark_session()
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        result = transform_gold(
            spark.read.parquet(str(taxi_path)), spark.read.parquet(str(taxi_zones_path)),
            spark.read.parquet(str(calendar_path)), spark.read.parquet(str(weather_path)),
        )
        result.enriched.write.mode("errorifexists").option("compression", "snappy").parquet(str(gold_path))
    finally:
        spark.stop()

