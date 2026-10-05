"""NYC taxi source ingestion scaffold."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from nyc_taxi.ingestion.download import download_file
from nyc_taxi.ingestion.validation import validate_taxi_parquet

logger = logging.getLogger(__name__)
TAXI_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def taxi_url(month: str) -> str:
    """Return the official TLC Yellow Taxi Parquet URL for a YYYY-MM month."""
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError(f"Month must have YYYY-MM format: {month}")
    return f"{TAXI_BASE_URL}/yellow_tripdata_{month}.parquet"


def download_taxi_month(month: str, raw_dir: str | Path, *, force: bool = False) -> Path:
    """Acquire one unmodified TLC monthly Parquet file into data/raw/taxi."""
    url = taxi_url(month)
    destination = Path(raw_dir) / f"yellow_tripdata_{month}.parquet"
    return download_file(url, destination, validate_taxi_parquet, force=force)


def ingest_taxi(input_path: str | Path, bronze_path: str | Path) -> None:
    """Validate a taxi source path before the future raw-to-bronze ingestion."""
    source = Path(input_path)
    if not source.exists():
        raise FileNotFoundError(f"Taxi input path does not exist: {source}")
    logger.info("Validated taxi input path %s; target is %s", source, bronze_path)
    raise NotImplementedError("Taxi ingestion is planned for the next ETL step")

