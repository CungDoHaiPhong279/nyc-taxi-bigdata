"""Offline, small-fixture checks for Bronze-to-Silver transformations."""

from __future__ import annotations

import os
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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

from nyc_taxi.etl.silver import (
    build_silver_dataset,
    transform_calendar,
    transform_taxi,
    transform_taxi_zones,
    transform_weather,
)
from nyc_taxi.ingestion.manifest import load_manifest, save_manifest


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Keep Spark local and deterministic without requiring external datasets."""
    environment = pytest.MonkeyPatch()
    environment.setenv("PYSPARK_PYTHON", sys.executable)
    session = None
    try:
        session = (
            SparkSession.builder.appName("nyc-taxi-silver-tests")
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
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampNTZType()),
        StructField("tpep_dropoff_datetime", TimestampNTZType()),
        StructField("passenger_count", LongType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", LongType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", LongType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("Airport_fee", DoubleType()),
        StructField("cbd_congestion_fee", DoubleType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_source_file", StringType()),
        StructField("_source_dataset", StringType()),
        StructField("_ingestion_batch_id", StringType()),
        StructField("_source_year", IntegerType()),
        StructField("_source_month", IntegerType()),
    ]
)


def _taxi_row(**updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2025, 1, 5, 10, 0),
        "tpep_dropoff_datetime": datetime(2025, 1, 5, 10, 10),
        "passenger_count": 1,
        "trip_distance": 1.5,
        "RatecodeID": None,
        "store_and_fwd_flag": "N",
        "PULocationID": 1,
        "DOLocationID": 2,
        "payment_type": 1,
        "fare_amount": 10.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 1.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 12.5,
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
        "_ingested_at": datetime(2026, 10, 5, tzinfo=timezone.utc),
        "_source_file": "data/raw/taxi/yellow_tripdata_2025-01.parquet",
        "_source_dataset": "taxi",
        "_ingestion_batch_id": "batch-a",
        "_source_year": 2025,
        "_source_month": 1,
    }
    row.update(updates)
    return row


def _taxi_dataframe(spark: SparkSession, rows: list[dict[str, object]]):
    return spark.createDataFrame(
        [tuple(row[field.name] for field in TAXI_SCHEMA.fields) for row in rows],
        TAXI_SCHEMA,
    )


def test_taxi_standardization_keeps_local_datetime_and_lineage(spark: SparkSession) -> None:
    transformed = transform_taxi(_taxi_dataframe(spark, [_taxi_row()]), 2025, 1)
    silver = transformed.accepted

    assert silver.count() == 1
    assert transformed.rejected.count() == 0
    assert transformed.statistics["input_rows"] == 1
    assert transformed.statistics["output_rows"] == 1
    assert {"vendor_id", "pickup_location_id", "dropoff_location_id", "ratecode_id"} <= set(
        silver.columns
    )
    assert {"VendorID", "PULocationID", "DOLocationID", "RatecodeID"}.isdisjoint(
        silver.columns
    )
    assert isinstance(silver.schema["pickup_datetime_local"].dataType, TimestampNTZType)
    actual = silver.select(
        "pickup_date",
        "pickup_hour",
        "pickup_year",
        "pickup_month",
        "_source_file",
        "_ingestion_batch_id",
    ).first()
    assert actual["pickup_date"] == date(2025, 1, 5)
    assert (actual["pickup_hour"], actual["pickup_year"], actual["pickup_month"]) == (10, 2025, 1)
    assert actual["_source_file"] == "data/raw/taxi/yellow_tripdata_2025-01.parquet"
    assert actual["_ingestion_batch_id"] == "batch-a"


def test_taxi_rejects_required_field_failures_but_preserves_ambiguous_values(
    spark: SparkSession,
) -> None:
    """Zero distance/passengers and nonpositive fares are not arbitrary hard filters."""
    invalid = [
        _taxi_row(tpep_pickup_datetime=None),
        _taxi_row(tpep_dropoff_datetime=None),
        _taxi_row(
            tpep_pickup_datetime=datetime(2025, 1, 5, 11),
            tpep_dropoff_datetime=datetime(2025, 1, 5, 10, 59),
        ),
        _taxi_row(PULocationID=None),
        _taxi_row(DOLocationID=None),
        _taxi_row(trip_distance=None),
        _taxi_row(trip_distance=-1.0),
        _taxi_row(fare_amount=None),
        _taxi_row(passenger_count=-1),
    ]
    preserved = [
        _taxi_row(),
        _taxi_row(
            tpep_pickup_datetime=datetime(2025, 1, 5, 19),
            tpep_dropoff_datetime=datetime(2025, 1, 5, 19, 10),
            trip_distance=0.0,
            passenger_count=0,
            fare_amount=0.0,
        ),
        _taxi_row(
            tpep_pickup_datetime=datetime(2025, 1, 5, 20),
            tpep_dropoff_datetime=datetime(2025, 1, 5, 20, 10),
            fare_amount=-5.0,
        ),
    ]

    result = transform_taxi(_taxi_dataframe(spark, invalid + preserved), 2025, 1)

    assert result.statistics["input_rows"] == 12
    assert result.statistics["output_rows"] == result.accepted.count() == 3
    assert result.statistics["rejected_rows"] == result.rejected.count() == 9
    assert result.statistics["duplicate_rows"] == 0
    assert result.rejected.filter(F.col("_reject_reason").isNull()).count() == 0
    expected_reasons = {
        "pickup_timestamp_null",
        "dropoff_timestamp_null",
        "invalid_duration",
        "pickup_location_null",
        "dropoff_location_null",
        "trip_distance_null",
        "trip_distance_negative",
        "fare_amount_null",
        "passenger_count_negative",
    }
    assert {row[0] for row in result.rejected.select("_reject_reason").collect()} == expected_reasons
    assert all(result.statistics["rule_counts"][reason] == 1 for reason in expected_reasons)
    assert {row["pickup_hour"] for row in result.accepted.select("pickup_hour").collect()} == {
        10,
        19,
        20,
    }
    assert result.accepted.filter(
        (F.col("trip_distance") == 0)
        & (F.col("passenger_count") == 0)
        & (F.col("fare_amount") == 0)
    ).count() == 1
    assert result.accepted.filter(F.col("fare_amount") < 0).count() == 1
    anomalies = result.statistics["preserved_anomalies"]
    assert anomalies["trip_distance_zero_preserved"] == 1
    assert anomalies["fare_amount_zero_preserved"] == 1
    assert anomalies["fare_amount_negative_preserved"] == 1
    assert anomalies["passenger_count_zero_preserved"] == 1


def test_taxi_deduplication_ignores_ingestion_metadata_and_is_deterministic(
    spark: SparkSession,
) -> None:
    first = _taxi_row(_ingestion_batch_id="batch-z")
    second = _taxi_row(
        _ingested_at=datetime(2026, 10, 6, tzinfo=timezone.utc),
        _ingestion_batch_id="batch-a",
    )
    forward = transform_taxi(_taxi_dataframe(spark, [first, second]), 2025, 1)
    reverse = transform_taxi(_taxi_dataframe(spark, [second, first]), 2025, 1)

    for result in (forward, reverse):
        assert result.statistics["input_rows"] == 2
        assert result.statistics["output_rows"] == result.accepted.count() == 1
        assert result.statistics["duplicate_rows"] == 1
        assert result.statistics["rejected_rows"] == result.rejected.count() == 1
        assert result.rejected.select("_reject_reason").first()[0] == "duplicate_business_record"
    assert forward.accepted.select("_ingestion_batch_id").first() == reverse.accepted.select(
        "_ingestion_batch_id"
    ).first()


def test_taxi_pickup_outside_source_month_is_reported_not_silently_dropped(
    spark: SparkSession,
) -> None:
    result = transform_taxi(
        _taxi_dataframe(
            spark,
            [
                _taxi_row(
                    tpep_pickup_datetime=datetime(2025, 2, 1, 0),
                    tpep_dropoff_datetime=datetime(2025, 2, 1, 0, 10),
                )
            ],
        ),
        2025,
        1,
    )

    assert result.accepted.count() == 1
    assert result.rejected.count() == 0
    assert result.statistics["preserved_anomalies"]["pickup_outside_source_month"] == 1
    assert result.accepted.select("pickup_month", "_source_month").first() == (2, 1)


def test_taxi_unknown_zone_ids_are_reported_without_a_gold_join(
    spark: SparkSession,
) -> None:
    result = transform_taxi(
        _taxi_dataframe(
            spark,
            [
                _taxi_row(),
                _taxi_row(
                    tpep_pickup_datetime=datetime(2025, 1, 5, 11),
                    tpep_dropoff_datetime=datetime(2025, 1, 5, 11, 10),
                    PULocationID=99,
                    DOLocationID=98,
                ),
            ],
        ),
        2025,
        1,
        known_zone_ids={1, 2},
    )

    assert result.accepted.count() == 2
    assert result.statistics["referential_quality"] == {
        "unknown_pickup_location": 1,
        "unknown_dropoff_location": 1,
    }


WEATHER_SCHEMA = StructType(
    [
        StructField("time", StringType()),
        StructField("precipitation", DoubleType()),
        StructField("snowfall", DoubleType()),
        StructField("temperature_2m", DoubleType()),
        StructField("weather_code", LongType()),
        StructField("wind_speed_10m", DoubleType()),
        StructField("timezone", StringType()),
        StructField("utc_offset_seconds", LongType()),
        StructField("_ingested_at", TimestampType()),
        StructField("_source_file", StringType()),
        StructField("_source_dataset", StringType()),
        StructField("_ingestion_batch_id", StringType()),
    ]
)


def _weather_dataframe(spark: SparkSession, times: list[str]):
    return spark.createDataFrame(
        [
            (
                time,
                0.0,
                0.0,
                -1.0,
                0,
                5.0,
                "GMT",
                0,
                datetime(2026, 10, 5, tzinfo=timezone.utc),
                "data/raw/weather/weather_nyc_2025-01_2025-06.json",
                "weather",
                "batch-a",
            )
            for time in times
        ],
        WEATHER_SCHEMA,
    )


def test_weather_utc_to_new_york_handles_both_dst_transitions(spark: SparkSession) -> None:
    times = [
        "2025-01-01T12:00",
        "2025-03-09T06:00",
        "2025-03-09T07:00",
        "2025-11-02T05:00",
        "2025-11-02T06:00",
    ]
    weather = transform_weather(_weather_dataframe(spark, times))
    rows = {
        row["weather_time_raw"]: row
        for row in weather.select(
            "weather_time_raw",
            "weather_timestamp_utc",
            "weather_timestamp_nyc",
            "weather_date_nyc",
            "weather_hour_nyc",
            F.date_format("weather_timestamp_nyc", "yyyy-MM-dd HH:mm").alias("local_clock"),
        ).collect()
    }

    assert len(rows) == len(times)
    assert isinstance(weather.schema["weather_timestamp_utc"].dataType, TimestampType)
    assert weather.select("weather_timestamp_utc").distinct().count() == len(times)
    assert rows["2025-01-01T12:00"]["local_clock"] == "2025-01-01 07:00"
    assert rows["2025-03-09T06:00"]["local_clock"] == "2025-03-09 01:00"
    assert rows["2025-03-09T07:00"]["local_clock"] == "2025-03-09 03:00"
    assert rows["2025-11-02T05:00"]["local_clock"] == "2025-11-02 01:00"
    assert rows["2025-11-02T06:00"]["local_clock"] == "2025-11-02 01:00"
    assert rows["2025-03-09T07:00"]["weather_hour_nyc"] == 3
    assert rows["2025-03-09T07:00"]["weather_date_nyc"] == date(2025, 3, 9)


def test_weather_rejects_duplicate_utc_hours(spark: SparkSession) -> None:
    with pytest.raises(ValueError, match="(?i)duplicate|unique"):
        transform_weather(_weather_dataframe(spark, ["2025-03-09T07:00"] * 2))


@pytest.mark.parametrize(
    ("field", "data_type"),
    [("timezone", "string"), ("utc_offset_seconds", "long")],
)
def test_weather_rejects_null_utc_source_metadata(
    spark: SparkSession, field: str, data_type: str
) -> None:
    source = _weather_dataframe(spark, ["2025-03-09T07:00"]).withColumn(
        field, F.lit(None).cast(data_type)
    )

    with pytest.raises(ValueError):
        transform_weather(source)


CALENDAR_SCHEMA = StructType(
    [
        StructField("date", StringType()),
        StructField("day_of_week", StringType()),
        StructField("day_name", StringType()),
        StructField("month", StringType()),
        StructField("year", StringType()),
        StructField("is_weekend", StringType()),
        StructField("is_holiday", StringType()),
        StructField("holiday_name", StringType()),
        StructField("_source_file", StringType()),
        StructField("_source_dataset", StringType()),
        StructField("_ingestion_batch_id", StringType()),
    ]
)


def _calendar_dataframe(spark: SparkSession):
    rows = []
    current = date(2025, 1, 1)
    while current.year == 2025:
        rows.append(
            (
                current.isoformat(),
                str(current.weekday()),
                current.strftime("%A"),
                str(current.month),
                str(current.year),
                str(current.weekday() >= 5),
                "False",
                None,
                "data/raw/calendar/calendar_2025.csv",
                "calendar",
                "batch-a",
            )
        )
        current += timedelta(days=1)
    return spark.createDataFrame(rows, CALENDAR_SCHEMA)


def test_calendar_enriches_2025_federal_holidays_without_losing_days(
    spark: SparkSession,
) -> None:
    calendar = transform_calendar(_calendar_dataframe(spark))
    holiday_rows = {
        row["date"]: row
        for row in calendar.filter(F.col("is_holiday")).select("date", "holiday_name").collect()
    }

    assert calendar.count() == calendar.select("date").distinct().count() == 365
    assert isinstance(calendar.schema["date"].dataType, DateType)
    assert isinstance(calendar.schema["is_holiday"].dataType, BooleanType)
    assert len(holiday_rows) == 11
    for holiday in (date(2025, 1, 1), date(2025, 1, 20), date(2025, 6, 19), date(2025, 11, 27)):
        assert holiday_rows[holiday]["holiday_name"]
    assert date(2025, 1, 2) not in holiday_rows
    assert calendar.filter(F.col("is_holiday") & F.col("holiday_name").isNull()).count() == 0


ZONE_SCHEMA = StructType(
    [
        StructField("LocationID", StringType()),
        StructField("Borough", StringType()),
        StructField("Zone", StringType()),
        StructField("service_zone", StringType()),
        StructField("_source_file", StringType()),
        StructField("_source_dataset", StringType()),
        StructField("_ingestion_batch_id", StringType()),
    ]
)


def _zones_dataframe(spark: SparkSession, identifiers: list[str | None]):
    return spark.createDataFrame(
        [
            (
                location_id,
                " Manhattan ",
                " Midtown ",
                " Yellow Zone ",
                "data/raw/taxi_zones/taxi_zone_lookup.csv",
                "taxi-zones",
                "batch-a",
            )
            for location_id in identifiers
        ],
        ZONE_SCHEMA,
    )


def test_taxi_zones_normalize_ids_and_trim_names(spark: SparkSession) -> None:
    zones = transform_taxi_zones(_zones_dataframe(spark, ["1", "2"]))
    assert zones.count() == zones.select("location_id").distinct().count() == 2
    assert isinstance(zones.schema["location_id"].dataType, IntegerType)
    assert {row["borough"] for row in zones.select("borough").collect()} == {"Manhattan"}
    assert {row["zone"] for row in zones.select("zone").collect()} == {"Midtown"}
    assert zones.filter(F.col("location_id").isNull()).count() == 0


@pytest.mark.parametrize("identifiers", [["1", "1"], ["1", None]])
def test_taxi_zones_reject_duplicate_or_null_ids(
    spark: SparkSession, identifiers: list[str | None]
) -> None:
    with pytest.raises(ValueError, match="(?i)location|duplicate|null|unique"):
        transform_taxi_zones(_zones_dataframe(spark, identifiers))


def test_silver_manifest_json_round_trip(tmp_path: Path) -> None:
    """The small trackable manifest retains lineage and quality accounting."""
    destination = tmp_path / "silver_manifest.json"
    entry = {
        "dataset_name": "taxi_2025-01",
        "dataset": "taxi",
        "bronze_path": "data/bronze/taxi/year=2025/month=01",
        "silver_path": "data/silver/taxi/year=2025/month=01",
        "input_rows": 3,
        "output_rows": 1,
        "rejected_rows": 1,
        "duplicate_rows": 1,
        "validation_status": "valid",
        "partition": {"year": 2025, "month": "01"},
    }

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
def test_tiny_taxi_silver_parquet_readback_skip_and_force(
    spark: SparkSession, tmp_path: Path
) -> None:
    source = tmp_path / "bronze" / "taxi" / "year=2025" / "month=01"
    destination = tmp_path / "silver" / "taxi" / "year=2025" / "month=01"
    _taxi_dataframe(
        spark,
        [
            _taxi_row(),
            _taxi_row(),
            _taxi_row(
                tpep_pickup_datetime=datetime(2025, 1, 5, 11),
                tpep_dropoff_datetime=datetime(2025, 1, 5, 10, 59),
            ),
        ],
    ).write.option("compression", "snappy").parquet(str(source))
    bronze_entry = {
        "dataset_name": "taxi_2025-01",
        "dataset": "taxi",
        "validation_status": "valid",
        "source_raw_path": "data/raw/taxi/yellow_tripdata_2025-01.parquet",
        "bronze_rows": 3,
        "raw_sha256": "fixture-sha256",
        "ingestion_batch_id": "batch-a",
    }

    altered_bronze_entry = {**bronze_entry, "ingestion_batch_id": "not-the-on-disk-batch"}
    with pytest.raises(ValueError, match="Bronze lineage"):
        build_silver_dataset(
            spark,
            "taxi",
            source,
            destination,
            altered_bronze_entry,
            source_year=2025,
            source_month=1,
        )
    assert not destination.exists()

    first = build_silver_dataset(
        spark, "taxi", source, destination, bronze_entry, source_year=2025, source_month=1
    )
    altered_manifest = {**first, "silver_batch_id": "not-the-on-disk-batch"}
    with pytest.raises(ValueError):
        build_silver_dataset(
            spark,
            "taxi",
            source,
            destination,
            bronze_entry,
            source_year=2025,
            source_month=1,
            existing_entry=altered_manifest,
        )
    assert spark.read.parquet(str(destination)).select("_silver_batch_id").first()[0] == first[
        "silver_batch_id"
    ]
    skipped = build_silver_dataset(
        spark,
        "taxi",
        source,
        destination,
        bronze_entry,
        source_year=2025,
        source_month=1,
        existing_entry=first,
    )
    forced = build_silver_dataset(
        spark,
        "taxi",
        source,
        destination,
        bronze_entry,
        source_year=2025,
        source_month=1,
        force=True,
        existing_entry=first,
    )

    assert first["bronze_rows"] == 3
    assert (first["silver_rows"], first["rejected_rows"]) == (1, 2)
    assert (first["invalid_rows"], first["duplicate_rows"]) == (1, 1)
    assert first["validation_status"] == "valid"
    assert first["source_bronze_batch_id"] == "batch-a"
    assert skipped["skipped"] is True
    assert skipped["silver_batch_id"] == first["silver_batch_id"]
    assert forced["silver_batch_id"] != first["silver_batch_id"]
    assert spark.read.parquet(str(destination)).count() == 1
    assert not list(destination.parent.glob("silver_staging_*"))

    manifest_path = tmp_path / "silver_manifest.json"
    save_manifest(manifest_path, [forced])
    assert load_manifest(manifest_path)[0]["silver_batch_id"] == forced["silver_batch_id"]


@pytest.mark.skipif(
    os.name == "nt"
    and shutil.which("winutils.exe") is None
    and not (
        os.environ.get("HADOOP_HOME")
        and (Path(os.environ["HADOOP_HOME"]) / "bin" / "winutils.exe").is_file()
    ),
    reason="Windows Hadoop local Parquet writes require winutils; run integration in Linux Docker",
)
def test_tiny_weather_silver_partitions_and_readback(
    spark: SparkSession, tmp_path: Path
) -> None:
    source = tmp_path / "bronze" / "weather"
    destination = tmp_path / "silver" / "weather"
    _weather_dataframe(
        spark,
        ["2025-01-01T12:00", "2025-03-09T06:00", "2025-03-09T07:00"],
    ).write.option("compression", "snappy").parquet(str(source))
    bronze_entry = {
        "dataset_name": "weather",
        "dataset": "weather",
        "validation_status": "valid",
        "source_raw_path": "data/raw/weather/weather_nyc_2025-01_2025-06.json",
        "bronze_rows": 3,
        "raw_sha256": "fixture-weather-sha256",
        "ingestion_batch_id": "batch-a",
    }

    entry = build_silver_dataset(spark, "weather", source, destination, bronze_entry)

    assert entry["validation_status"] == "valid"
    assert entry["bronze_rows"] == entry["silver_rows"] == 3
    assert entry["rejected_rows"] == entry["duplicate_rows"] == 0
    assert (destination / "year=2025" / "month=01").is_dir()
    assert (destination / "year=2025" / "month=03").is_dir()
    assert spark.read.parquet(str(destination)).count() == 3
