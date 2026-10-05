"""Offline checks for acquisition paths and read-only raw validation."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ingestion.manifest import build_manifest_entry, save_manifest
from nyc_taxi.ingestion.download import download_file
from nyc_taxi.ingestion.taxi import download_taxi_month, taxi_url
from nyc_taxi.ingestion.validation import (
    validate_calendar,
    validate_taxi_parquet,
    validate_taxi_zones,
    validate_weather,
)
from scripts.create_calendar import create_calendar


def _write_taxi_parquet(path: Path, *, include_fare: bool = True) -> None:
    """Create a real, tiny Parquet file with TLC's required field names."""
    columns = {
        "tpep_pickup_datetime": [datetime(2025, 1, 1, 12)],
        "tpep_dropoff_datetime": [datetime(2025, 1, 1, 12, 15)],
        "PULocationID": [1],
        "DOLocationID": [2],
        "trip_distance": [1.5],
        "total_amount": [14.0],
    }
    if include_fare:
        columns["fare_amount"] = [10.0]
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(columns), path)


def test_taxi_month_url_and_reject_invalid_month() -> None:
    assert taxi_url("2025-01") == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        "yellow_tripdata_2025-01.parquet"
    )
    assert taxi_url("2025-06").endswith("yellow_tripdata_2025-06.parquet")
    with pytest.raises(ValueError):
        taxi_url("2025-13")


def test_existing_valid_taxi_file_is_not_overwritten(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    destination = tmp_path / "yellow_tripdata_2025-01.parquet"
    _write_taxi_parquet(destination)
    before = destination.read_bytes()

    def forbid_network(*args: object, **kwargs: object) -> None:
        raise AssertionError("Existing valid file must not trigger a network request")

    monkeypatch.setattr("socket.socket.connect", forbid_network)
    download_taxi_month("2025-01", tmp_path)
    assert destination.read_bytes() == before


def test_incomplete_download_leaves_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class ShortResponse:
        status = 200
        headers = {"Content-Length": "5"}

        def __enter__(self) -> "ShortResponse":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self, _size: int) -> bytes:
            chunk, self.chunk = getattr(self, "chunk", b"abc"), b""
            return chunk

    monkeypatch.setattr("nyc_taxi.ingestion.download.urlopen", lambda *_a, **_k: ShortResponse())
    destination = tmp_path / "source.csv"

    with pytest.raises(ValueError, match="Incomplete download"):
        download_file("https://example.invalid/source.csv", destination, lambda _path: None)

    assert not destination.exists()
    assert not list(tmp_path.glob("*.part"))


def test_taxi_parquet_metadata_reports_rows_and_missing_columns(tmp_path: Path) -> None:
    valid = tmp_path / "valid.parquet"
    _write_taxi_parquet(valid)
    result = validate_taxi_parquet(valid)
    assert result["status"] == "valid"
    assert result["row_count"] == 1
    assert "fare_amount" in result["columns"]

    missing_fare = tmp_path / "missing_fare.parquet"
    _write_taxi_parquet(missing_fare, include_fare=False)
    with pytest.raises(ValueError, match="fare_amount"):
        validate_taxi_parquet(missing_fare)


def test_taxi_zone_lookup_requires_header_and_rows(tmp_path: Path) -> None:
    lookup = tmp_path / "taxi_zone_lookup.csv"
    lookup.write_text(
        "LocationID,Borough,Zone,service_zone\n1,Manhattan,Test Zone,Yellow Zone\n",
        encoding="utf-8",
    )
    result = validate_taxi_zones(lookup)
    assert result["status"] == "valid"
    assert result["row_count"] == 1
    assert "LocationID" in result["columns"]

    lookup.write_text("LocationID,Borough,Zone\n1,Manhattan,Test Zone\n", encoding="utf-8")
    with pytest.raises(ValueError, match="service_zone"):
        validate_taxi_zones(lookup)


def test_weather_native_hourly_json_reports_utc_and_rejects_mismatch(tmp_path: Path) -> None:
    weather = tmp_path / "weather.json"
    hourly = {
        "time": ["2025-01-01T00:00", "2025-01-01T01:00"],
        "temperature_2m": [0.0, -1.0],
        "precipitation": [0.0, 0.2],
        "snowfall": [0.0, 0.0],
        "wind_speed_10m": [10.0, 11.0],
        "weather_code": [0, 3],
    }
    payload = {"timezone": "GMT", "hourly": hourly, "hourly_units": {"time": "iso8601"}}
    weather.write_text(json.dumps(payload), encoding="utf-8")
    result = validate_weather(weather)
    assert result["status"] == "valid"
    assert result["row_count"] == 2
    assert result["timezone"] == "GMT"
    assert result["granularity"] == "hourly"
    assert "temperature_2m" in result["columns"]

    hourly["snowfall"] = [0.0]
    weather.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="unequal lengths"):
        validate_weather(weather)


def test_calendar_covers_2025_once_and_rejects_duplicates(tmp_path: Path) -> None:
    calendar = tmp_path / "calendar_2025.csv"
    assert create_calendar(date(2025, 1, 1), date(2025, 12, 31), calendar) == 365
    result = validate_calendar(calendar)
    assert result["status"] == "valid"
    assert result["row_count"] == 365
    assert result["date_coverage"] == {"start": "2025-01-01", "end": "2025-12-31"}

    with calendar.open("r", encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))
    rows[1]["date"] = rows[0]["date"]
    with calendar.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    with pytest.raises(ValueError, match="365 unique dates"):
        validate_calendar(calendar)


def test_manifest_serializes_verified_file_with_relative_path_and_checksum(tmp_path: Path) -> None:
    taxi = tmp_path / "data" / "raw" / "taxi" / "yellow_tripdata_2025-01.parquet"
    _write_taxi_parquet(taxi)
    entry = build_manifest_entry(
        name="yellow_taxi_2025-01",
        source="NYC TLC",
        source_url=taxi_url("2025-01"),
        path=taxi,
        format="parquet",
        validation=validate_taxi_parquet(taxi),
        date_coverage={"start": "2025-01-01", "end": "2025-01-31"},
        download_timestamp="2026-10-05T00:00:00+00:00",
        project_root=tmp_path,
    )
    assert entry["local_path"] == "data/raw/taxi/yellow_tripdata_2025-01.parquet"
    assert entry["file_size_bytes"] == taxi.stat().st_size
    assert entry["sha256"] == hashlib.sha256(taxi.read_bytes()).hexdigest()
    assert entry["row_count"] == 1
    assert entry["validation_status"] == "valid"

    manifest = tmp_path / "data" / "raw" / "raw_manifest.json"
    save_manifest(manifest, [entry])
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["datasets"] == [entry]
    assert payload["datasets"][0]["source_url"] == taxi_url("2025-01")
