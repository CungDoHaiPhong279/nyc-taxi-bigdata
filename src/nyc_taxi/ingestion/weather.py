"""Weather source ingestion scaffold."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

from nyc_taxi.ingestion.download import download_file
from nyc_taxi.ingestion.validation import validate_weather

logger = logging.getLogger(__name__)
WEATHER_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_FIELDS = "temperature_2m,precipitation,snowfall,wind_speed_10m,weather_code"


def weather_url(start_date: date, end_date: date) -> str:
    """Build a key-free ERA5 archive request for NYC hourly weather in UTC."""
    if end_date < start_date:
        raise ValueError("Weather end date must be on or after start date")
    parameters = {
        "latitude": "40.7128",
        "longitude": "-74.0060",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": WEATHER_FIELDS,
        "timezone": "UTC",
        "models": "era5",
    }
    return f"{WEATHER_ENDPOINT}?{urlencode(parameters)}"


def download_weather(
    start_date: date,
    end_date: date,
    raw_dir: str | Path,
    *,
    force: bool = False,
) -> Path:
    """Acquire native Open-Meteo JSON without renaming or transforming fields."""
    destination = Path(raw_dir) / (
        f"weather_nyc_{start_date:%Y-%m}_{end_date:%Y-%m}.json"
    )
    return download_file(
        weather_url(start_date, end_date),
        destination,
        lambda path: validate_weather(path, expected_start=start_date, expected_end=end_date),
        force=force,
    )


def ingest_weather(input_path: str | Path, bronze_path: str | Path) -> None:
    """Validate a weather source path before future ingestion."""
    source = Path(input_path)
    if not source.exists():
        raise FileNotFoundError(f"Weather input path does not exist: {source}")
    logger.info("Validated weather input path %s; target is %s", source, bronze_path)
    raise NotImplementedError("Weather ingestion is planned for the next ETL step")

