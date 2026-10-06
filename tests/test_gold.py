"""Small, offline checks for Silver-to-Gold trip enrichment."""

from __future__ import annotations

import os
import shutil
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

from nyc_taxi.etl.gold import assert_no_fanout, build_gold_dataset, transform_gold
from nyc_taxi.ingestion.manifest import load_manifest, save_manifest


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    environment = pytest.MonkeyPatch()
    environment.setenv("PYSPARK_PYTHON", sys.executable)
    session = None
    try:
        session = (
            SparkSession.builder.appName("nyc-taxi-gold-tests")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        yield session
    finally:
        try:
            if session is not None:
                session.stop()
        finally:
            environment.undo()


TAXI_SCHEMA = StructType(
    [
        StructField("vendor_id", IntegerType()),
        StructField("pickup_datetime_local", TimestampNTZType()),
        StructField("dropoff_datetime_local", TimestampNTZType()),
        StructField("passenger_count", LongType()),
        StructField("trip_distance", DoubleType()),
        StructField("ratecode_id", LongType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("pickup_location_id", IntegerType()),
        StructField("dropoff_location_id", IntegerType()),
        StructField("payment_type", LongType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("airport_fee", DoubleType()),
        StructField("cbd_congestion_fee", DoubleType()),
        StructField("pickup_date", DateType()),
        StructField("pickup_hour", IntegerType()),
        StructField("pickup_year", IntegerType()),
        StructField("pickup_month", IntegerType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_source_file", StringType()),
        StructField("_source_dataset", StringType()),
        StructField("_ingestion_batch_id", StringType()),
        StructField("_source_year", IntegerType()),
        StructField("_source_month", IntegerType()),
        StructField("_silver_batch_id", StringType()),
        StructField("_silver_processed_at", TimestampType()),
    ]
)


def _taxi_row(
    pickup: datetime,
    pickup_location_id: int,
    dropoff_location_id: int,
    **updates: object,
) -> dict[str, object]:
    row: dict[str, object] = {
        "vendor_id": 2,
        "pickup_datetime_local": pickup,
        "dropoff_datetime_local": pickup.replace(minute=10),
        "passenger_count": 1,
        "trip_distance": 1.5,
        "ratecode_id": None,
        "store_and_fwd_flag": "N",
        "pickup_location_id": pickup_location_id,
        "dropoff_location_id": dropoff_location_id,
        "payment_type": 1,
        "fare_amount": 10.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 1.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 12.5,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
        "pickup_date": pickup.date(),
        "pickup_hour": pickup.hour,
        "pickup_year": pickup.year,
        "pickup_month": pickup.month,
        "_ingested_at": datetime(2026, 10, 5, tzinfo=timezone.utc),
        "_source_file": "data/raw/taxi/yellow_tripdata_2025-01.parquet",
        "_source_dataset": "taxi",
        "_ingestion_batch_id": "bronze-batch",
        "_source_year": 2025,
        "_source_month": 1,
        "_silver_batch_id": "silver-batch",
        "_silver_processed_at": datetime(2026, 10, 5, tzinfo=timezone.utc),
    }
    row.update(updates)
    return row


def _taxi(spark: SparkSession) -> DataFrame:
    rows = [
        _taxi_row(datetime(2025, 1, 20, 10), 1, 2),
        _taxi_row(
            datetime(2025, 1, 21, 11), 99, 2,
            fare_amount=-5.0,
            trip_distance=0.0,
            passenger_count=None,
        ),
        _taxi_row(datetime(2025, 6, 30, 23), 1, 98),
        _taxi_row(datetime(2007, 1, 1, 12), 1, 2),
    ]
    return spark.createDataFrame(
        [tuple(row[field.name] for field in TAXI_SCHEMA.fields) for row in rows],
        TAXI_SCHEMA,
    )


ZONES_SCHEMA = StructType(
    [
        StructField("location_id", IntegerType()),
        StructField("borough", StringType()),
        StructField("zone", StringType()),
        StructField("service_zone", StringType()),
    ]
)


def _zones(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [(1, "Manhattan", "Midtown", "Yellow Zone"), (2, "Queens", "Airport", "Airports")],
        ZONES_SCHEMA,
    )


CALENDAR_SCHEMA = StructType(
    [
        StructField("date", DateType()),
        StructField("day_of_week", IntegerType()),
        StructField("is_weekend", BooleanType()),
        StructField("is_holiday", BooleanType()),
        StructField("holiday_name", StringType()),
    ]
)


def _calendar(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (date(2025, 1, 20), 0, False, True, "Birthday of Martin Luther King, Jr."),
            (date(2025, 1, 21), 1, False, False, None),
            (date(2025, 6, 30), 0, False, False, None),
        ],
        CALENDAR_SCHEMA,
    )


WEATHER_SCHEMA = StructType(
    [
        StructField("weather_timestamp_utc", TimestampType()),
        StructField("weather_timestamp_nyc", TimestampNTZType()),
        StructField("weather_date_nyc", DateType()),
        StructField("weather_hour_nyc", IntegerType()),
        StructField("temperature_2m", DoubleType()),
        StructField("precipitation", DoubleType()),
        StructField("snowfall", DoubleType()),
        StructField("wind_speed_10m", DoubleType()),
        StructField("weather_code", LongType()),
    ]
)


def _weather(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (
                datetime(2025, 1, 20, 15, tzinfo=timezone.utc),
                datetime(2025, 1, 20, 10),
                date(2025, 1, 20),
                10,
                2.0,
                1.0,
                0.0,
                4.0,
                3,
            ),
            (
                datetime(2025, 1, 21, 16, tzinfo=timezone.utc),
                datetime(2025, 1, 21, 11),
                date(2025, 1, 21),
                11,
                3.0,
                0.0,
                0.0,
                3.0,
                0,
            ),
        ],
        WEATHER_SCHEMA,
    )


def test_left_joins_enrich_matched_trips_and_preserve_each_unmatched_kind(
    spark: SparkSession,
) -> None:
    result = transform_gold(_taxi(spark), _zones(spark), _calendar(spark), _weather(spark))
    enriched = result.enriched
    rows = {row["pickup_datetime_local"]: row.asDict() for row in enriched.collect()}

    assert len(rows) == enriched.count() == 4
    assert result.statistics["input_rows"] == result.statistics["output_rows"] == 4
    assert result.statistics["join_counts"] == {
        "pickup_zone": {"matched": 3, "unmatched": 1},
        "dropoff_zone": {"matched": 3, "unmatched": 1},
        "calendar": {"matched": 3, "unmatched": 1},
        "weather": {"matched": 2, "unmatched": 2},
    }
    assert isinstance(enriched.schema["pickup_datetime_local"].dataType, TimestampNTZType)
    assert "location_id" not in enriched.columns
    assert {
        "pickup_borough", "pickup_zone", "pickup_service_zone",
        "dropoff_borough", "dropoff_zone", "dropoff_service_zone",
        "day_of_week", "is_weekend", "is_holiday", "holiday_name",
        "temperature_2m", "precipitation", "snowfall", "wind_speed_10m", "weather_code",
        "pickup_zone_matched", "dropoff_zone_matched", "calendar_matched", "weather_matched",
    } <= set(enriched.columns)

    matched = rows[datetime(2025, 1, 20, 10)]
    assert matched["pickup_borough"] == "Manhattan"
    assert matched["pickup_zone"] == "Midtown"
    assert matched["dropoff_borough"] == "Queens"
    assert matched["dropoff_zone"] == "Airport"
    assert matched["is_holiday"] is True
    assert matched["holiday_name"] == "Birthday of Martin Luther King, Jr."
    assert matched["precipitation"] == 1.0
    assert all(matched[flag] is True for flag in (
        "pickup_zone_matched", "dropoff_zone_matched", "calendar_matched", "weather_matched"
    ))

    pickup_missing = rows[datetime(2025, 1, 21, 11)]
    assert pickup_missing["pickup_zone_matched"] is False
    assert pickup_missing["pickup_borough"] is None
    assert pickup_missing["dropoff_zone_matched"] is True
    assert pickup_missing["calendar_matched"] is True
    assert pickup_missing["weather_matched"] is True
    assert (pickup_missing["fare_amount"], pickup_missing["trip_distance"], pickup_missing["passenger_count"]) == (-5.0, 0.0, None)

    weather_edge = rows[datetime(2025, 6, 30, 23)]
    assert weather_edge["dropoff_zone_matched"] is False
    assert weather_edge["dropoff_borough"] is None
    assert weather_edge["calendar_matched"] is True
    assert weather_edge["weather_matched"] is False
    assert weather_edge["temperature_2m"] is None

    historical = rows[datetime(2007, 1, 1, 12)]
    assert historical["calendar_matched"] is False
    assert historical["weather_matched"] is False
    assert historical["day_of_week"] is None
    assert historical["precipitation"] is None
    assert historical["_source_year"] == 2025
    assert historical["_source_month"] == 1
    assert historical["_silver_batch_id"] == "silver-batch"
    assert historical["_source_file"] == "data/raw/taxi/yellow_tripdata_2025-01.parquet"


@pytest.mark.parametrize("dimension", ["zones", "calendar", "weather"])
def test_duplicate_dimension_join_key_fails_before_fanout(
    spark: SparkSession, dimension: str
) -> None:
    zones, calendar, weather = _zones(spark), _calendar(spark), _weather(spark)
    if dimension == "zones":
        duplicate = spark.createDataFrame([(1, "Other", "Duplicate", "Yellow Zone")], ZONES_SCHEMA)
        zones = zones.unionByName(duplicate)
    elif dimension == "calendar":
        duplicate = spark.createDataFrame(
            [(date(2025, 1, 20), 0, False, True, "Duplicate")], CALENDAR_SCHEMA
        )
        calendar = calendar.unionByName(duplicate)
    else:
        duplicate = spark.createDataFrame(
            [(
                datetime(2025, 1, 20, 16, tzinfo=timezone.utc),
                datetime(2025, 1, 20, 10),
                date(2025, 1, 20), 10, 9.0, 0.0, 0.0, 1.0, 0,
            )],
            WEATHER_SCHEMA,
        )
        weather = weather.unionByName(duplicate)
    with pytest.raises(ValueError, match="(?i)duplicate|unique|fan.out"):
        transform_gold(_taxi(spark), zones, calendar, weather)


def test_explicit_fanout_guard_rejects_increased_join_count() -> None:
    assert_no_fanout(4, 4, "pickup zone")
    with pytest.raises(ValueError, match="(?i)fan.out|row count"):
        assert_no_fanout(4, 5, "pickup zone")


def test_gold_manifest_round_trip(tmp_path: Path) -> None:
    entry = {
        "dataset_name": "taxi_enriched_2025-01",
        "dataset": "taxi_enriched",
        "source_silver_path": "data/silver/taxi/year=2025/month=01",
        "gold_path": "data/gold/taxi_enriched/year=2025/month=01",
        "silver_rows": 4,
        "gold_rows": 4,
        "source_silver_batch_id": "silver-batch",
        "gold_batch_id": "gold-batch",
        "validation_status": "valid",
    }
    destination = tmp_path / "gold_manifest.json"
    save_manifest(destination, [entry])
    assert load_manifest(destination) == [entry]
    assert destination.read_text(encoding="utf-8").endswith("\n")


@pytest.mark.skipif(
    os.name == "nt"
    and shutil.which("winutils.exe") is None
    and not (
        os.environ.get("HADOOP_HOME")
        and (Path(os.environ["HADOOP_HOME"]) / "bin" / "winutils.exe").is_file()
    ),
    reason="Windows Hadoop local Parquet writes require winutils; run integration in Linux Docker",
)
def test_tiny_gold_parquet_readback_and_idempotency(
    spark: SparkSession, tmp_path: Path
) -> None:
    taxi_path = tmp_path / "silver" / "taxi" / "year=2025" / "month=01"
    zones_path = tmp_path / "silver" / "taxi_zones"
    calendar_path = tmp_path / "silver" / "calendar"
    weather_path = tmp_path / "silver" / "weather"
    gold_path = tmp_path / "gold" / "taxi_enriched" / "year=2025" / "month=01"
    for frame, path in (
        (_taxi(spark), taxi_path),
        (_zones(spark), zones_path),
        (_calendar(spark), calendar_path),
        (_weather(spark), weather_path),
    ):
        frame.write.option("compression", "snappy").parquet(str(path))

    silver_entry = {
        "dataset_name": "taxi_2025-01",
        "dataset": "taxi",
        "validation_status": "valid",
        "silver_path": "data/silver/taxi/year=2025/month=01",
        "source_bronze_path": "data/bronze/taxi/year=2025/month=01",
        "source_raw_path": "data/raw/taxi/yellow_tripdata_2025-01.parquet",
        "raw_sha256": "fixture-sha256",
        "source_bronze_batch_id": "bronze-batch",
        "silver_batch_id": "silver-batch",
        "silver_rows": 4,
        "partition": {"year": 2025, "month": "01"},
    }
    altered_silver_entry = {**silver_entry, "silver_batch_id": "not-the-on-disk-batch"}
    with pytest.raises(ValueError, match="(?i)lineage|batch"):
        build_gold_dataset(
            spark, taxi_path, zones_path, calendar_path, weather_path, gold_path,
            altered_silver_entry, source_year=2025, source_month=1,
        )
    assert not gold_path.exists()

    first = build_gold_dataset(
        spark, taxi_path, zones_path, calendar_path, weather_path, gold_path,
        silver_entry, source_year=2025, source_month=1,
    )
    assert first["validation_status"] == "valid"
    assert first["silver_rows"] == first["gold_rows"] == 4
    assert first["source_silver_batch_id"] == "silver-batch"
    assert first["gold_batch_id"]
    assert first["join_counts"] == {
        "pickup_zone": {"matched": 3, "unmatched": 1},
        "dropoff_zone": {"matched": 3, "unmatched": 1},
        "calendar": {"matched": 3, "unmatched": 1},
        "weather": {"matched": 2, "unmatched": 2},
    }
    gold = spark.read.parquet(str(gold_path))
    assert gold.count() == 4
    assert gold.select("_gold_batch_id").distinct().first()[0] == first["gold_batch_id"]
    assert gold.filter("not weather_matched").count() == 2
    assert gold.filter("not calendar_matched").count() == 1

    skipped = build_gold_dataset(
        spark, taxi_path, zones_path, calendar_path, weather_path, gold_path,
        silver_entry, source_year=2025, source_month=1, existing_entry=first,
    )
    assert skipped["skipped"] is True
    assert skipped["gold_batch_id"] == first["gold_batch_id"]
    assert spark.read.parquet(str(gold_path)).count() == 4

    manifest_path = tmp_path / "gold_manifest.json"
    save_manifest(manifest_path, [first])
    assert load_manifest(manifest_path)[0]["gold_batch_id"] == first["gold_batch_id"]

    forced = build_gold_dataset(
        spark, taxi_path, zones_path, calendar_path, weather_path, gold_path,
        silver_entry, source_year=2025, source_month=1,
        existing_entry=first, force=True,
    )
    assert forced["gold_batch_id"] != first["gold_batch_id"]
    assert spark.read.parquet(str(gold_path)).count() == 4
    assert not list(gold_path.parent.glob("gold_staging_*"))
