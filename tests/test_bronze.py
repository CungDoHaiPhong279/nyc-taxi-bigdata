"""Small, offline Spark checks for source-preserving Bronze transformations."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from nyc_taxi.etl.bronze import (
    add_ingestion_metadata,
    normalize_weather_hourly,
)
from nyc_taxi.ingestion.validation import validate_taxi_parquet


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Use one minimal local session and release its JVM after these tests."""
    environment = pytest.MonkeyPatch()
    environment.setenv("PYSPARK_PYTHON", sys.executable)
    session = None
    try:
        session = (
            SparkSession.builder.appName("nyc-taxi-bronze-tests")
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


def test_metadata_preserves_taxi_source_schema_and_records(spark: SparkSession) -> None:
    source_schema = StructType(
        [
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("cbd_congestion_fee", DoubleType(), True),
        ]
    )
    unusual_record = (datetime(2025, 1, 1, 0, 0), 0.0, -5.0, None, 2.5)
    source = spark.createDataFrame(
        [unusual_record, unusual_record, (datetime(2025, 1, 2, 1, 0), 3.5, 18.0, 1, None)],
        schema=source_schema,
    )
    ingested_at = datetime(2026, 10, 5, 12, 0, tzinfo=timezone.utc)

    bronze = add_ingestion_metadata(
        source,
        source_file="data/raw/taxi/yellow_tripdata_2025-01.parquet",
        source_dataset="taxi",
        batch_id="fixture-batch",
        ingested_at=ingested_at,
        source_year=2025,
        source_month=1,
    )

    assert {field.name: field.dataType for field in bronze.schema.fields if field.name in source.columns} == {
        field.name: field.dataType for field in source_schema.fields
    }
    assert Counter(tuple(row) for row in bronze.select(*source.columns).collect()) == Counter(
        tuple(row) for row in source.collect()
    )
    assert bronze.count() == 3
    assert isinstance(bronze.schema["_ingested_at"].dataType, TimestampType)
    assert {row["epoch"] for row in bronze.select(
        F.col("_ingested_at").cast("long").alias("epoch")
    ).distinct().collect()} == {int(ingested_at.timestamp())}
    assert isinstance(bronze.schema["_source_year"].dataType, IntegerType)
    assert isinstance(bronze.schema["_source_month"].dataType, IntegerType)
    for row in bronze.select(
        "_source_file", "_source_dataset", "_ingestion_batch_id", "_source_year", "_source_month"
    ).collect():
        assert row == (
            "data/raw/taxi/yellow_tripdata_2025-01.parquet",
            "taxi",
            "fixture-batch",
            2025,
            1,
        )


def test_metadata_rejects_source_column_collision(spark: SparkSession) -> None:
    source = spark.createDataFrame([("source value",)], ["_source_file"])

    with pytest.raises(ValueError, match="_source_file"):
        add_ingestion_metadata(
            source,
            source_file="data/raw/example.parquet",
            source_dataset="taxi",
            batch_id="fixture-batch",
            ingested_at=datetime.now(timezone.utc),
        )


def test_weather_arrays_become_rows_without_changing_source_values(
    spark: SparkSession, tmp_path: Path
) -> None:
    source = tmp_path / "weather.json"
    source.write_text(
        json.dumps(
            {
                "latitude": 40.7,
                "longitude": -74.0,
                "timezone": "GMT",
                "utc_offset_seconds": 0,
                "hourly_units": {"time": "iso8601", "temperature_2m": "°C"},
                "hourly": {
                    "time": ["2025-01-01T00:00", "2025-01-01T01:00"],
                    "temperature_2m": [-2.5, -1.0],
                    "precipitation": [None, 0.3],
                    "snowfall": [0.0, 0.1],
                    "wind_speed_10m": [12.0, 15.0],
                    "weather_code": [0, 95],
                    "relative_humidity_2m": [72, 88],
                },
            }
        ),
        encoding="utf-8",
    )

    bronze = normalize_weather_hourly(spark, source)
    rows = bronze.orderBy("time").collect()

    assert len(rows) == 2
    assert [row["time"] for row in rows] == ["2025-01-01T00:00", "2025-01-01T01:00"]
    assert [row["temperature_2m"] for row in rows] == [-2.5, -1.0]
    assert [row["precipitation"] for row in rows] == [None, 0.3]
    assert [row["snowfall"] for row in rows] == [0.0, 0.1]
    assert [row["wind_speed_10m"] for row in rows] == [12.0, 15.0]
    assert [row["weather_code"] for row in rows] == [0, 95]
    assert [row["relative_humidity_2m"] for row in rows] == [72, 88]
    assert {row["timezone"] for row in rows} == {"GMT"}
    assert {row["latitude"] for row in rows} == {40.7}


@pytest.mark.skipif(
    os.name == "nt"
    and shutil.which("winutils.exe") is None
    and not (
        os.environ.get("HADOOP_HOME")
        and (Path(os.environ["HADOOP_HOME"]) / "bin" / "winutils.exe").is_file()
    ),
    reason="Windows Hadoop local Parquet writes require winutils; this test runs in Linux Docker",
)
def test_tiny_taxi_round_trip_skips_existing_valid_partition(
    spark: SparkSession, tmp_path: Path
) -> None:
    from nyc_taxi.etl.bronze import build_bronze_dataset

    source = tmp_path / "yellow_tripdata_2025-01.parquet"
    pq.write_table(
        pa.table(
            {
                "tpep_pickup_datetime": [datetime(2025, 1, 1, 0, 0)] * 2,
                "tpep_dropoff_datetime": [datetime(2025, 1, 1, 0, 15)] * 2,
                "PULocationID": [1, 1],
                "DOLocationID": [2, 2],
                "trip_distance": [0.0, 0.0],
                "fare_amount": [-5.0, -5.0],
                "total_amount": [-4.0, -4.0],
                "cbd_congestion_fee": [2.5, 2.5],
            }
        ),
        source,
    )
    destination = tmp_path / "bronze" / "taxi" / "year=2025" / "month=01"
    raw_report = validate_taxi_parquet(source)
    checksum = hashlib.sha256(source.read_bytes()).hexdigest()
    args = (
        spark,
        "taxi",
        source,
        destination,
        raw_report,
        checksum,
    )

    first = build_bronze_dataset(*args, source_year=2025, source_month=1)
    second = build_bronze_dataset(
        *args, source_year=2025, source_month=1, existing_entry=first
    )
    forced = build_bronze_dataset(
        *args, source_year=2025, source_month=1, force=True, existing_entry=first
    )

    assert first["dataset_name"] == "taxi_2025-01"
    assert first["raw_rows"] == first["bronze_rows"] == 2
    assert first["validation_status"] == "valid"
    assert first["ingestion_batch_id"]
    assert second["skipped"] is True
    assert forced["ingestion_batch_id"] != first["ingestion_batch_id"]
    assert forced["raw_rows"] == forced["bronze_rows"] == 2
    assert spark.read.parquet(str(destination)).count() == 2
