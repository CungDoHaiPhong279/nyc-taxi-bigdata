"""Source-aware Bronze-to-Silver transformations.

Taxi source timestamps are TIMESTAMP_NTZ wall-clock values. TLC's published
Yellow Taxi dictionary does not specify their timezone, so we preserve that
type instead of inventing a UTC instant. Weather timestamps are UTC.
"""

from __future__ import annotations

import logging
import shutil
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from nyc_taxi.common.config import PROJECT_ROOT

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

TAXI_RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime_local",
    "tpep_dropoff_datetime": "dropoff_datetime_local",
    "RatecodeID": "ratecode_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "Airport_fee": "airport_fee",
}

# Nationwide 2025 US federal holidays; OPM's Inauguration Day is DC-area-only.
# https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/
FEDERAL_HOLIDAYS_2025 = {
    "2025-01-01": "New Year's Day",
    "2025-01-20": "Birthday of Martin Luther King, Jr.",
    "2025-02-17": "Washington's Birthday",
    "2025-05-26": "Memorial Day",
    "2025-06-19": "Juneteenth National Independence Day",
    "2025-07-04": "Independence Day",
    "2025-09-01": "Labor Day",
    "2025-10-13": "Columbus Day",
    "2025-11-11": "Veterans Day",
    "2025-11-27": "Thanksgiving Day",
    "2025-12-25": "Christmas Day",
}

TAXI_REJECT_RULES = (
    "pickup_timestamp_null",
    "dropoff_timestamp_null",
    "invalid_duration",
    "pickup_location_null",
    "dropoff_location_null",
    "trip_distance_null",
    "trip_distance_negative",
    "fare_amount_null",
    "passenger_count_negative",
)


@dataclass(frozen=True)
class TaxiTransformResult:
    accepted: "DataFrame"
    rejected: "DataFrame"
    statistics: dict[str, Any]


def _relative_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return path.name


def _schema_summary(dataframe: "DataFrame") -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in dataframe.schema.fields}


def _require_columns(dataframe: "DataFrame", required: set[str], dataset: str) -> None:
    missing = sorted(required.difference(dataframe.columns))
    if missing:
        raise ValueError(f"{dataset} Bronze columns missing: {', '.join(missing)}")


def _require_utc_session(spark: "SparkSession") -> None:
    if spark.conf.get("spark.sql.session.timeZone").upper() != "UTC":
        raise ValueError("Silver weather conversion requires a UTC Spark session timezone")


def _validate_bronze_input(
    bronze: "DataFrame",
    entry: dict[str, Any],
    source_year: int | None = None,
    source_month: int | None = None,
) -> None:
    """Verify the physical Bronze rows still match their manifest lineage."""
    from pyspark.sql import functions as F

    _require_columns(bronze, {
        "_source_file", "_source_dataset", "_ingestion_batch_id", "_ingested_at",
    }, "Bronze lineage")
    fields = ("_source_file", "_source_dataset", "_ingestion_batch_id", "_ingested_at")
    aggregates = [F.count("*").alias("rows")]
    for field in fields:
        aggregates.extend((
            F.count(field).alias(f"nonnull_{field}"),
            F.min(field).alias(f"min_{field}"),
            F.max(field).alias(f"max_{field}"),
        ))
    if entry["dataset"] == "taxi":
        _require_columns(bronze, {"_source_year", "_source_month"}, "Taxi Bronze partition")
        for field in ("_source_year", "_source_month"):
            aggregates.extend((F.min(field).alias(f"min_{field}"), F.max(field).alias(f"max_{field}")))
    summary = bronze.agg(*aggregates).first().asDict()
    rows = int(summary["rows"])
    if rows != int(entry["bronze_rows"]):
        raise ValueError("Bronze Parquet row count differs from its manifest")
    for field in fields:
        if summary[f"nonnull_{field}"] != rows or summary[f"min_{field}"] != summary[f"max_{field}"]:
            raise ValueError(f"Bronze lineage {field} is missing or inconsistent")
    expected = {
        "_source_file": entry["source_raw_path"],
        "_source_dataset": entry["dataset"],
        "_ingestion_batch_id": entry["ingestion_batch_id"],
    }
    for field, value in expected.items():
        if summary[f"min_{field}"] != value:
            raise ValueError(f"Bronze lineage {field} differs from its manifest")
    if entry["dataset"] == "taxi" and (
        summary["min__source_year"] != source_year
        or summary["max__source_year"] != source_year
        or summary["min__source_month"] != source_month
        or summary["max__source_month"] != source_month
    ):
        raise ValueError("Taxi Bronze source year/month differ from selected partition")


def transform_taxi(
    bronze: "DataFrame",
    source_year: int,
    source_month: int,
    *,
    known_zone_ids: set[int] | None = None,
) -> TaxiTransformResult:
    """Standardize one month, reject only explicit invalids, and count anomalies.

    Zero distance and nonpositive fare are preserved: those values can reflect
    legitimate corrections or special trips. Exact duplicates use every source
    business column, never ingestion metadata.
    """
    from pyspark.sql import Window, functions as F
    from pyspark.sql.types import TimestampNTZType

    if not 1 <= source_month <= 12:
        raise ValueError("source_month must be in 1..12")
    _require_columns(bronze, {
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
        "DOLocationID", "trip_distance", "fare_amount", "passenger_count",
        "_source_file", "_source_dataset", "_ingestion_batch_id", "_ingested_at",
        "_source_year", "_source_month",
    }, "taxi")
    for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        if not isinstance(bronze.schema[name].dataType, TimestampNTZType):
            raise ValueError(f"Taxi {name} must retain source TIMESTAMP_NTZ semantics")

    business_columns = [TAXI_RENAMES.get(name, name) for name in bronze.columns if not name.startswith("_")]
    renamed = bronze
    for original, standardized in TAXI_RENAMES.items():
        if original in renamed.columns:
            renamed = renamed.withColumnRenamed(original, standardized)
    standardized = (
        renamed.withColumn("pickup_date", F.to_date("pickup_datetime_local"))
        .withColumn("pickup_hour", F.hour("pickup_datetime_local"))
        .withColumn("pickup_year", F.year("pickup_datetime_local"))
        .withColumn("pickup_month", F.month("pickup_datetime_local"))
    )
    next_month = date(source_year + (source_month == 12), source_month % 12 + 1, 1)
    month_mismatch = (
        (F.col("pickup_date") < F.lit(date(source_year, source_month, 1)))
        | (F.col("pickup_date") >= F.lit(next_month))
    )
    predicates = {
        "pickup_timestamp_null": F.col("pickup_datetime_local").isNull(),
        "dropoff_timestamp_null": F.col("dropoff_datetime_local").isNull(),
        "invalid_duration": F.col("dropoff_datetime_local") <= F.col("pickup_datetime_local"),
        "pickup_location_null": F.col("pickup_location_id").isNull(),
        "dropoff_location_null": F.col("dropoff_location_id").isNull(),
        "trip_distance_null": F.col("trip_distance").isNull() | F.isnan("trip_distance"),
        "trip_distance_negative": F.col("trip_distance") < 0,
        "fare_amount_null": F.col("fare_amount").isNull() | F.isnan("fare_amount"),
        "passenger_count_negative": F.col("passenger_count") < 0,
    }
    flagged = standardized
    for name, predicate in predicates.items():
        flagged = flagged.withColumn(f"_rule_{name}", F.coalesce(predicate, F.lit(False)))
    reason = F.lit(None).cast("string")
    for name in reversed(TAXI_REJECT_RULES):
        reason = F.when(F.col(f"_rule_{name}"), F.lit(name)).otherwise(reason)
    flagged = flagged.withColumn("_reject_reason", reason)

    def sum_if(condition: Any) -> Any:
        return F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0))

    aggregates = [F.count("*").alias("input_rows")]
    aggregates += [sum_if(F.col(f"_rule_{name}")).alias(f"rule_{name}") for name in TAXI_REJECT_RULES]
    aggregates += [sum_if(F.col("_reject_reason") == name).alias(f"primary_{name}") for name in TAXI_REJECT_RULES]
    aggregates += [
        sum_if(F.col("_reject_reason").isNotNull()).alias("invalid_rows"),
        sum_if(month_mismatch).alias("pickup_outside_source_month"),
        sum_if(F.col("fare_amount") < 0).alias("fare_amount_negative_preserved"),
        sum_if(F.col("fare_amount") == 0).alias("fare_amount_zero_preserved"),
        sum_if(F.col("trip_distance") == 0).alias("trip_distance_zero_preserved"),
        sum_if(F.col("passenger_count").isNull()).alias("passenger_count_null_preserved"),
        sum_if(F.col("passenger_count") == 0).alias("passenger_count_zero_preserved"),
        F.min("trip_distance").alias("trip_distance_min"),
        F.max("trip_distance").alias("trip_distance_max"),
        F.min("fare_amount").alias("fare_amount_min"),
        F.max("fare_amount").alias("fare_amount_max"),
        F.min("pickup_datetime_local").alias("pickup_min"),
        F.max("pickup_datetime_local").alias("pickup_max"),
        F.percentile_approx("trip_distance", [0.5, 0.99], 1000).alias("trip_distance_quantiles"),
        F.percentile_approx("fare_amount", [0.5, 0.99], 1000).alias("fare_amount_quantiles"),
    ]
    if known_zone_ids is not None:
        zone_ids = sorted(known_zone_ids)
        aggregates += [
            sum_if(F.col("pickup_location_id").isNotNull() & ~F.col("pickup_location_id").isin(zone_ids)).alias("unknown_pickup_location"),
            sum_if(F.col("dropoff_location_id").isNotNull() & ~F.col("dropoff_location_id").isin(zone_ids)).alias("unknown_dropoff_location"),
        ]
    aggregate = flagged.agg(*aggregates).first().asDict(recursive=True)
    invalid_rows = int(aggregate["invalid_rows"])
    valid = flagged.filter(F.col("_reject_reason").isNull())
    duplicate_rows = valid.groupBy(*business_columns).count().agg(
        F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicates")
    ).first()["duplicates"]
    duplicate_rows = int(duplicate_rows)
    rule_columns = [f"_rule_{name}" for name in TAXI_REJECT_RULES]
    invalid = flagged.filter(F.col("_reject_reason").isNotNull()).drop(*rule_columns)
    if duplicate_rows:
        order_columns = [F.col(name).asc_nulls_last() for name in sorted(bronze.columns) if name.startswith("_")]
        numbered = valid.withColumn(
            "_silver_row_number",
            F.row_number().over(Window.partitionBy(*business_columns).orderBy(*order_columns)),
        )
        accepted = numbered.filter(F.col("_silver_row_number") == 1).drop(
            "_silver_row_number", "_reject_reason", *rule_columns
        )
        duplicate_rejected = numbered.filter(F.col("_silver_row_number") > 1).drop(
            "_silver_row_number", *rule_columns
        ).withColumn("_reject_reason", F.lit("duplicate_business_record"))
        rejected = invalid.unionByName(duplicate_rejected)
    else:
        accepted = valid.drop("_reject_reason", *rule_columns)
        rejected = invalid

    # Report preserved values from the accepted rows, not all Bronze rows.
    # A rejected trip can also have a negative fare or zero distance.
    preserved_predicates = {
        "pickup_outside_source_month": month_mismatch,
        "fare_amount_negative_preserved": F.col("fare_amount") < 0,
        "fare_amount_zero_preserved": F.col("fare_amount") == 0,
        "trip_distance_zero_preserved": F.col("trip_distance") == 0,
        "passenger_count_null_preserved": F.col("passenger_count").isNull(),
        "passenger_count_zero_preserved": F.col("passenger_count") == 0,
    }
    preserved = accepted.agg(*[
        sum_if(predicate).alias(name) for name, predicate in preserved_predicates.items()
    ]).first().asDict()

    input_rows = int(aggregate["input_rows"])
    stats: dict[str, Any] = {
        "input_rows": input_rows,
        "output_rows": input_rows - invalid_rows - duplicate_rows,
        "invalid_rows": invalid_rows,
        "duplicate_rows": duplicate_rows,
        "rejected_rows": invalid_rows + duplicate_rows,
        "rule_counts": {name: int(aggregate[f"rule_{name}"]) for name in TAXI_REJECT_RULES},
        "primary_rejection_reasons": {
            name: int(aggregate[f"primary_{name}"]) for name in TAXI_REJECT_RULES
        } | {"duplicate_business_record": duplicate_rows},
        "observed_anomalies": {
            name: int(aggregate[name]) for name in preserved_predicates
        },
        "preserved_anomalies": {
            name: int(preserved[name]) for name in preserved_predicates
        },
        "null_statistics": {
            "pickup_datetime_local": int(aggregate["rule_pickup_timestamp_null"]),
            "dropoff_datetime_local": int(aggregate["rule_dropoff_timestamp_null"]),
            "pickup_location_id": int(aggregate["rule_pickup_location_null"]),
            "dropoff_location_id": int(aggregate["rule_dropoff_location_null"]),
            "trip_distance": int(aggregate["rule_trip_distance_null"]),
            "fare_amount": int(aggregate["rule_fare_amount_null"]),
            "passenger_count": int(aggregate["passenger_count_null_preserved"]),
        },
        "profile": {
            "trip_distance_min": aggregate["trip_distance_min"],
            "trip_distance_max": aggregate["trip_distance_max"],
            "fare_amount_min": aggregate["fare_amount_min"],
            "fare_amount_max": aggregate["fare_amount_max"],
            "pickup_min": aggregate["pickup_min"].isoformat() if aggregate["pickup_min"] else None,
            "pickup_max": aggregate["pickup_max"].isoformat() if aggregate["pickup_max"] else None,
            "trip_distance_p50_p99": aggregate["trip_distance_quantiles"],
            "fare_amount_p50_p99": aggregate["fare_amount_quantiles"],
        },
    }
    if known_zone_ids is not None:
        stats["referential_quality"] = {
            "unknown_pickup_location": int(aggregate["unknown_pickup_location"]),
            "unknown_dropoff_location": int(aggregate["unknown_dropoff_location"]),
        }
    return TaxiTransformResult(accepted, rejected, stats)


def transform_weather(bronze: "DataFrame") -> "DataFrame":
    """Parse UTC hourly time and derive DST-aware New York wall time."""
    from pyspark.sql import functions as F

    _require_utc_session(bronze.sparkSession)
    _require_columns(bronze, {"time", "timezone", "utc_offset_seconds"}, "weather")
    if bronze.filter(
        F.col("timezone").isNull()
        | F.col("utc_offset_seconds").isNull()
        | ~F.col("timezone").isin("GMT", "UTC")
        | (F.col("utc_offset_seconds") != 0)
    ).limit(1).count():
        raise ValueError("Weather Bronze must contain UTC/GMT timestamps with zero UTC offset")
    result = bronze.withColumnRenamed("time", "weather_time_raw")
    result = result.withColumn(
        "weather_timestamp_utc",
        F.try_to_timestamp(F.col("weather_time_raw"), F.lit("yyyy-MM-dd'T'HH:mm")),
    )
    if result.filter(F.col("weather_timestamp_utc").isNull()).limit(1).count():
        raise ValueError("Weather UTC timestamp cannot be parsed")
    if result.groupBy("weather_timestamp_utc").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Weather UTC timestamp is not unique")
    # A local wall-clock value is NTZ; the UTC instant remains TimestampType.
    local = F.from_utc_timestamp(
        F.col("weather_timestamp_utc"), "America/New_York"
    ).cast("timestamp_ntz")
    return (
        result.withColumn("weather_timestamp_nyc", local)
        .withColumn("weather_date_nyc", F.to_date("weather_timestamp_nyc"))
        .withColumn("weather_hour_nyc", F.hour("weather_timestamp_nyc"))
        .withColumn("year", F.date_format("weather_timestamp_utc", "yyyy"))
        .withColumn("month", F.date_format("weather_timestamp_utc", "MM"))
    )


def transform_calendar(bronze: "DataFrame") -> "DataFrame":
    """Enrich the date dimension with nationwide OPM 2025 federal holidays."""
    from pyspark.sql import functions as F

    _require_columns(bronze, {
        "date", "day_of_week", "month", "year", "is_weekend",
        "is_holiday", "holiday_name",
    }, "calendar")
    result = bronze.withColumnRenamed("is_holiday", "source_is_holiday").withColumnRenamed(
        "holiday_name", "source_holiday_name"
    )
    result = (
        result.withColumn("date", F.expr("try_cast(date as date)"))
        .withColumn("day_of_week", F.expr("try_cast(day_of_week as int)"))
        .withColumn("month", F.expr("try_cast(month as int)"))
        .withColumn("year", F.expr("try_cast(year as int)"))
        .withColumn("is_weekend", F.expr("try_cast(is_weekend as boolean)"))
    )
    if result.filter(F.col("date").isNull()).limit(1).count():
        raise ValueError("Calendar date cannot be parsed")
    if result.filter(
        F.col("day_of_week").isNull()
        | F.col("month").isNull()
        | F.col("year").isNull()
        | F.col("is_weekend").isNull()
        | (F.col("day_of_week") != F.pmod(F.dayofweek("date") + 5, 7))
        | (F.col("month") != F.month("date"))
        | (F.col("year") != F.year("date"))
        | (F.col("is_weekend") != F.dayofweek("date").isin(1, 7))
    ).limit(1).count():
        raise ValueError("Calendar date attributes are null or inconsistent")
    if result.groupBy("date").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Calendar dates are not unique")
    holiday_map = F.create_map(*[
        item for day, name in FEDERAL_HOLIDAYS_2025.items()
        for item in (F.lit(day), F.lit(name))
    ])
    result = result.withColumn(
        "holiday_name", F.element_at(holiday_map, F.date_format("date", "yyyy-MM-dd"))
    )
    return result.withColumn("is_holiday", F.col("holiday_name").isNotNull())


def transform_taxi_zones(bronze: "DataFrame") -> "DataFrame":
    """Normalize the small zone dimension and require a unique valid key."""
    from pyspark.sql import functions as F

    _require_columns(bronze, {"LocationID", "Borough", "Zone", "service_zone"}, "taxi-zones")
    result = (
        bronze.withColumnRenamed("LocationID", "location_id")
        .withColumnRenamed("Borough", "borough")
        .withColumnRenamed("Zone", "zone")
        .withColumn("location_id", F.expr("try_cast(location_id as int)"))
        .withColumn("borough", F.trim("borough"))
        .withColumn("zone", F.trim("zone"))
        .withColumn("service_zone", F.trim("service_zone"))
    )
    if result.filter(F.col("location_id").isNull()).limit(1).count():
        raise ValueError("Taxi Zone location_id is null or invalid")
    if result.groupBy("location_id").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Taxi Zone location_id is not unique")
    return result


def validate_silver_output(
    spark: "SparkSession",
    dataset: str,
    path: Path,
    expected_rows: int,
    *,
    expected_schema: dict[str, str] | None = None,
    expected_silver_batch_id: str | None = None,
    expected_bronze_batch_id: str | None = None,
    expected_source_file: str | None = None,
) -> dict[str, Any]:
    """Read Parquet back and check count, Silver metadata, and key invariants."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import TimestampNTZType, TimestampType

    if not path.is_dir():
        raise ValueError(f"Silver output does not exist: {path}")
    result = spark.read.parquet(str(path))
    schema = _schema_summary(result)
    if expected_schema is not None and schema != expected_schema:
        raise ValueError(f"Silver schema differs from manifest: {path}")
    _require_columns(result, {
        "_source_file", "_source_dataset", "_ingestion_batch_id",
        "_ingested_at", "_silver_batch_id", "_silver_processed_at",
    }, dataset)
    metadata_fields = (
        "_source_file", "_source_dataset", "_ingestion_batch_id",
        "_ingested_at", "_silver_batch_id", "_silver_processed_at",
    )
    aggregates = [F.count("*").alias("rows")]
    for field in metadata_fields:
        aggregates.extend((
            F.count(field).alias(f"nonnull_{field}"),
            F.min(field).alias(f"min_{field}"),
            F.max(field).alias(f"max_{field}"),
        ))
    metadata = result.agg(*aggregates).first().asDict()
    rows = int(metadata["rows"])
    if rows != expected_rows:
        raise ValueError(f"Silver {dataset} rows {rows} != expected {expected_rows}")
    for field in metadata_fields:
        if metadata[f"nonnull_{field}"] != rows or metadata[f"min_{field}"] != metadata[f"max_{field}"]:
            raise ValueError(f"Silver {dataset} lineage metadata {field} is missing or inconsistent")
    expected_metadata = {
        "_source_file": expected_source_file,
        "_source_dataset": dataset,
        "_ingestion_batch_id": expected_bronze_batch_id,
        "_silver_batch_id": expected_silver_batch_id,
    }
    for field, expected in expected_metadata.items():
        if expected is not None and metadata[f"min_{field}"] != expected:
            raise ValueError(f"Silver {dataset} lineage metadata {field} differs from manifest")

    if dataset == "taxi":
        _require_columns(result, {
            "pickup_datetime_local", "dropoff_datetime_local", "pickup_date",
            "pickup_hour", "pickup_location_id", "dropoff_location_id",
        }, dataset)
        if not isinstance(result.schema["pickup_datetime_local"].dataType, TimestampNTZType):
            raise ValueError("Taxi local pickup lost TIMESTAMP_NTZ type")
        bad = result.filter(
            F.col("pickup_datetime_local").isNull()
            | F.col("dropoff_datetime_local").isNull()
            | (F.col("dropoff_datetime_local") <= F.col("pickup_datetime_local"))
            | F.col("pickup_location_id").isNull()
            | F.col("dropoff_location_id").isNull()
            | F.col("trip_distance").isNull()
            | (F.col("trip_distance") < 0)
            | F.col("fare_amount").isNull()
            | (F.col("passenger_count") < 0)
        ).limit(1).count()
        if bad:
            raise ValueError("Taxi Silver contains a rejected invalid row")
    elif dataset == "weather":
        _require_columns(result, {
            "weather_timestamp_utc", "weather_timestamp_nyc",
            "weather_date_nyc", "weather_hour_nyc", "year", "month",
        }, dataset)
        if not isinstance(result.schema["weather_timestamp_utc"].dataType, TimestampType):
            raise ValueError("Weather UTC time is not TimestampType")
        if not isinstance(result.schema["weather_timestamp_nyc"].dataType, TimestampNTZType):
            raise ValueError("Weather NYC local time is not TIMESTAMP_NTZ")
        if result.filter(F.col("weather_timestamp_utc").isNull()).limit(1).count():
            raise ValueError("Weather Silver has an unparsed UTC timestamp")
        if result.select("weather_timestamp_utc").distinct().count() != rows:
            raise ValueError("Weather Silver UTC timestamps are not unique")
    elif dataset == "calendar":
        _require_columns(result, {"date", "is_holiday", "holiday_name"}, dataset)
        if result.select("date").distinct().count() != rows:
            raise ValueError("Calendar Silver dates are not unique")
        if rows == 365:
            if result.agg(F.min("date").alias("start"), F.max("date").alias("end")).first() != (
                date(2025, 1, 1), date(2025, 12, 31)
            ):
                raise ValueError("Calendar Silver date coverage changed")
            holidays = result.filter(F.col("is_holiday")).count()
            if holidays != len(FEDERAL_HOLIDAYS_2025):
                raise ValueError(f"Calendar Silver holiday count {holidays} is not 11")
        if result.filter(F.col("is_holiday") & F.col("holiday_name").isNull()).limit(1).count():
            raise ValueError("Calendar holiday lacks a name")
    elif dataset == "taxi-zones":
        _require_columns(result, {"location_id", "borough", "zone", "service_zone"}, dataset)
        if result.filter(F.col("location_id").isNull()).limit(1).count():
            raise ValueError("Taxi Zone Silver location_id is null")
        if result.select("location_id").distinct().count() != rows:
            raise ValueError("Taxi Zone Silver location_id is not unique")
    else:
        raise ValueError(f"Unknown Silver dataset: {dataset}")
    return {"status": "valid", "row_count": rows, "schema": schema}


def build_silver_dataset(
    spark: "SparkSession",
    dataset: str,
    bronze_path: Path,
    output_path: Path,
    bronze_entry: dict[str, Any],
    *,
    source_year: int | None = None,
    source_month: int | None = None,
    known_zone_ids: set[int] | None = None,
    force: bool = False,
    existing_entry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one Silver output and return manifest-ready quality statistics."""
    from pyspark.sql import functions as F

    if bronze_entry.get("validation_status") != "valid":
        raise ValueError("Bronze manifest entry must be valid")
    if dataset == "taxi" and (source_year is None or source_month is None):
        raise ValueError("Taxi Silver requires source year and month")
    if dataset != "taxi" and (source_year is not None or source_month is not None):
        raise ValueError("Only Taxi has source year/month arguments")
    _require_utc_session(spark)
    source = Path(bronze_path)
    target = Path(output_path)
    name = f"taxi_{source_year:04d}-{source_month:02d}" if dataset == "taxi" else dataset
    if not source.is_dir():
        raise ValueError(f"Bronze input does not exist: {source}")
    start = time.monotonic()
    bronze = spark.read.parquet(str(source))
    _validate_bronze_input(bronze, bronze_entry, source_year, source_month)

    if target.exists() and not force:
        if (
            existing_entry is None
            or existing_entry.get("source_bronze_batch_id") != bronze_entry.get("ingestion_batch_id")
            or existing_entry.get("raw_sha256") != bronze_entry.get("raw_sha256")
        ):
            raise ValueError(f"Silver output exists without matching Bronze lineage; use --force: {target}")
        validate_silver_output(
            spark, dataset, target, int(existing_entry["silver_rows"]),
            expected_schema=existing_entry["silver_schema"],
            expected_silver_batch_id=existing_entry["silver_batch_id"],
            expected_bronze_batch_id=bronze_entry["ingestion_batch_id"],
            expected_source_file=bronze_entry["source_raw_path"],
        )
        if int(existing_entry["bronze_rows"]) != int(bronze_entry["bronze_rows"]):
            raise ValueError(f"Bronze row count changed since Silver build: {source}")
        logger.info("[SILVER] %s existing valid output kept: %s rows", name, existing_entry["silver_rows"])
        return {**existing_entry, "skipped": True}

    if dataset == "taxi":
        transformed = transform_taxi(
            bronze, source_year, source_month, known_zone_ids=known_zone_ids
        )
        output = transformed.accepted
        quality = transformed.statistics
    elif dataset == "weather":
        output = transform_weather(bronze)
        input_rows = bronze.count()
        quality = {
            "input_rows": input_rows, "output_rows": input_rows,
            "invalid_rows": 0, "duplicate_rows": 0, "rejected_rows": 0,
            "rule_counts": {}, "primary_rejection_reasons": {},
            "null_statistics": {"weather_timestamp_utc": 0},
            "preserved_anomalies": {},
        }
    elif dataset == "calendar":
        output = transform_calendar(bronze)
        input_rows = bronze.count()
        quality = {
            "input_rows": input_rows, "output_rows": input_rows,
            "invalid_rows": 0, "duplicate_rows": 0, "rejected_rows": 0,
            "rule_counts": {}, "primary_rejection_reasons": {},
            "null_statistics": {"holiday_name": input_rows - len(FEDERAL_HOLIDAYS_2025)},
            "preserved_anomalies": {"source_is_holiday_false": input_rows},
            "holiday_count": len(FEDERAL_HOLIDAYS_2025),
            "holiday_definition": "2025 nationwide US federal holidays (OPM)",
        }
    elif dataset == "taxi-zones":
        output = transform_taxi_zones(bronze)
        input_rows = bronze.count()
        string_fields = ("borough", "zone", "service_zone")
        null_counts = output.agg(*[
            F.sum(F.col(field).isNull().cast("int")).alias(field) for field in string_fields
        ]).first().asDict()
        quality = {
            "input_rows": input_rows, "output_rows": input_rows,
            "invalid_rows": 0, "duplicate_rows": 0, "rejected_rows": 0,
            "rule_counts": {}, "primary_rejection_reasons": {},
            "null_statistics": {"location_id": 0, **null_counts},
            "preserved_anomalies": {},
        }
    else:
        raise ValueError(f"Unknown Silver dataset: {dataset}")

    if quality["input_rows"] != int(bronze_entry["bronze_rows"]):
        raise ValueError(f"Bronze manifest row count differs from Spark input for {name}")
    if quality["output_rows"] + quality["rejected_rows"] != quality["input_rows"]:
        raise ValueError(f"Silver row accounting failed for {name}")
    batch_id = uuid4().hex
    processed_at = datetime.now(timezone.utc)
    if "_silver_batch_id" in output.columns or "_silver_processed_at" in output.columns:
        raise ValueError("Bronze data conflicts with Silver metadata columns")
    output = output.withColumn("_silver_batch_id", F.lit(batch_id)).withColumn(
        "_silver_processed_at", F.lit(processed_at)
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    staging = target.with_name(f"silver_staging_{batch_id}")
    backup = target.with_name(f"silver_backup_{batch_id}")
    moved_old_output = False
    try:
        writer = output.write.mode("errorifexists").option("compression", "snappy")
        if dataset == "weather":
            writer = writer.partitionBy("year", "month")
        writer.parquet(str(staging))
        validated = validate_silver_output(
            spark, dataset, staging, int(quality["output_rows"]),
            expected_silver_batch_id=batch_id,
            expected_bronze_batch_id=bronze_entry["ingestion_batch_id"],
            expected_source_file=bronze_entry["source_raw_path"],
        )
        if target.exists():
            target.rename(backup)
            moved_old_output = True
        try:
            staging.rename(target)
            validate_silver_output(
                spark, dataset, target, int(quality["output_rows"]),
                expected_schema=validated["schema"],
                expected_silver_batch_id=batch_id,
                expected_bronze_batch_id=bronze_entry["ingestion_batch_id"],
                expected_source_file=bronze_entry["source_raw_path"],
            )
        except Exception:
            if target.exists():
                shutil.rmtree(target)
            if moved_old_output:
                backup.rename(target)
                moved_old_output = False
            raise
        if moved_old_output:
            try:
                shutil.rmtree(backup)
            except OSError as exc:
                logger.warning("Could not remove prior Silver backup %s: %s", backup, exc)
        entry = {
            "dataset_name": name,
            "dataset": dataset,
            "source_raw_path": bronze_entry["source_raw_path"],
            "source_bronze_path": _relative_path(source),
            "silver_path": _relative_path(target),
            "raw_sha256": bronze_entry["raw_sha256"],
            "source_bronze_batch_id": bronze_entry["ingestion_batch_id"],
            "bronze_rows": int(quality["input_rows"]),
            "silver_rows": int(quality["output_rows"]),
            "rejected_rows": int(quality["rejected_rows"]),
            "invalid_rows": int(quality["invalid_rows"]),
            "duplicate_rows": int(quality["duplicate_rows"]),
            "quality_rules": list(TAXI_REJECT_RULES) + ["duplicate_business_record"] if dataset == "taxi" else [],
            "rule_counts": quality["rule_counts"],
            "primary_rejection_reasons": quality["primary_rejection_reasons"],
            "observed_anomalies": quality.get("observed_anomalies"),
            "preserved_anomalies": quality["preserved_anomalies"],
            "null_statistics": quality["null_statistics"],
            "profile": quality.get("profile"),
            "referential_quality": quality.get("referential_quality"),
            "holiday_count": quality.get("holiday_count"),
            "holiday_definition": quality.get("holiday_definition"),
            "bronze_schema": _schema_summary(bronze),
            "silver_schema": validated["schema"],
            "partition": {"year": source_year, "month": f"{source_month:02d}"} if dataset == "taxi" else ("UTC year/month" if dataset == "weather" else None),
            "processed_at_utc": processed_at.isoformat(),
            "silver_batch_id": batch_id,
            "validation_status": validated["status"],
            "output_size_bytes": sum(file.stat().st_size for file in target.rglob("*.parquet")),
            "elapsed_seconds": round(time.monotonic() - start, 2),
        }
        logger.info(
            "[SILVER] %s source=%s destination=%s input=%d output=%d rejected=%d "
            "invalid=%d duplicates=%d status=%s elapsed=%.2fs batch=%s",
            name, source, target, entry["bronze_rows"], entry["silver_rows"],
            entry["rejected_rows"], entry["invalid_rows"], entry["duplicate_rows"],
            entry["validation_status"], entry["elapsed_seconds"], batch_id,
        )
        return entry
    finally:
        if staging.exists():
            shutil.rmtree(staging)

