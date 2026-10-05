"""Read-only structural checks for source files in data/raw."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from typing import Any

TAXI_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "total_amount",
}
ZONE_COLUMNS = {"LocationID", "Borough", "Zone", "service_zone"}
CALENDAR_COLUMNS = {
    "date",
    "day_of_week",
    "day_name",
    "month",
    "year",
    "is_weekend",
}
WEATHER_FIELDS = {
    "time",
    "temperature_2m",
    "precipitation",
    "snowfall",
    "wind_speed_10m",
    "weather_code",
}


def _nonempty_file(path: str | Path) -> Path:
    source = Path(path)
    if not source.is_file() or source.stat().st_size == 0:
        raise ValueError(f"Raw file is missing or empty: {source}")
    return source


def _check_columns(actual: list[str], required: set[str], source: Path) -> None:
    missing = sorted(required.difference(actual))
    if missing:
        raise ValueError(f"{source} is missing expected columns: {', '.join(missing)}")


def validate_taxi_parquet(path: str | Path) -> dict[str, Any]:
    """Inspect Parquet metadata and schema without loading trip rows."""
    import pyarrow.parquet as parquet

    source = _nonempty_file(path)
    try:
        parquet_file = parquet.ParquetFile(source)
        schema = parquet_file.schema_arrow
        columns = schema.names
        _check_columns(columns, TAXI_COLUMNS, source)
        row_count = parquet_file.metadata.num_rows
        if row_count < 1:
            raise ValueError(f"Taxi Parquet contains no rows: {source}")
        return {
            "status": "valid",
            "row_count": row_count,
            "columns": columns,
            "schema": {field.name: str(field.type) for field in schema},
        }
    except (OSError, ValueError, RuntimeError) as exc:
        raise ValueError(f"Invalid taxi Parquet {source}: {exc}") from exc


def validate_taxi_zones(path: str | Path) -> dict[str, Any]:
    """Check lookup CSV header and count records without modifying them."""
    source = _nonempty_file(path)
    with source.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        columns = reader.fieldnames or []
        _check_columns(columns, ZONE_COLUMNS, source)
        row_count = sum(1 for _ in reader)
    if row_count < 1:
        raise ValueError(f"Taxi zone CSV contains no data rows: {source}")
    return {"status": "valid", "row_count": row_count, "columns": columns}


def validate_weather(
    path: str | Path,
    *,
    expected_start: date | None = None,
    expected_end: date | None = None,
) -> dict[str, Any]:
    """Check the native Open-Meteo hourly JSON response and report its metadata."""
    source = _nonempty_file(path)
    try:
        with source.open("r", encoding="utf-8") as json_file:
            payload = json.load(json_file)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read weather JSON {source}: {exc}") from exc

    if not isinstance(payload, dict) or not isinstance(payload.get("hourly"), dict):
        raise ValueError(f"Weather JSON is missing an hourly object: {source}")
    hourly = payload["hourly"]
    columns = list(hourly)
    _check_columns(columns, WEATHER_FIELDS, source)
    timestamps = hourly["time"]
    if not isinstance(timestamps, list) or not timestamps:
        raise ValueError(f"Weather JSON has no hourly timestamps: {source}")
    if any(not isinstance(hourly[field], list) or len(hourly[field]) != len(timestamps)
           for field in WEATHER_FIELDS):
        raise ValueError(f"Weather hourly arrays have unequal lengths: {source}")
    if expected_start is not None and timestamps[0] != f"{expected_start}T00:00":
        raise ValueError(f"Weather data does not start at {expected_start}T00:00: {source}")
    if expected_end is not None and timestamps[-1] != f"{expected_end}T23:00":
        raise ValueError(f"Weather data does not end at {expected_end}T23:00: {source}")

    return {
        "status": "valid",
        "row_count": len(timestamps),
        "columns": columns,
        "schema": payload.get("hourly_units", {}),
        "timezone": payload.get("timezone"),
        "granularity": "hourly",
        "date_coverage": {"start": timestamps[0], "end": timestamps[-1]},
    }


def validate_calendar(path: str | Path) -> dict[str, Any]:
    """Check that the existing 2025 calendar covers every date exactly once."""
    source = _nonempty_file(path)
    with source.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        columns = reader.fieldnames or []
        _check_columns(columns, CALENDAR_COLUMNS, source)
        try:
            dates = [date.fromisoformat(row["date"]) for row in reader]
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Calendar has an invalid date: {source}") from exc

    if (
        len(dates) != 365
        or len(set(dates)) != 365
        or min(dates, default=None) != date(2025, 1, 1)
        or max(dates, default=None) != date(2025, 12, 31)
    ):
        raise ValueError("Calendar must contain 365 unique dates from 2025-01-01 to 2025-12-31")
    return {
        "status": "valid",
        "row_count": len(dates),
        "columns": columns,
        "date_coverage": {"start": "2025-01-01", "end": "2025-12-31"},
        "timezone": "date only",
        "granularity": "daily",
    }
