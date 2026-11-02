"""NYC taxi-zone lookup ingestion scaffold."""

from __future__ import annotations

import logging
from pathlib import Path

from nyc_taxi.ingestion.download import download_file
from nyc_taxi.ingestion.validation import validate_taxi_zones

logger = logging.getLogger(__name__)
TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download_taxi_zones(raw_dir: str | Path, *, force: bool = False) -> Path:
    """Acquire the unmodified TLC taxi-zone CSV lookup table."""
    destination = Path(raw_dir) / "taxi_zone_lookup.csv"
    return download_file(TAXI_ZONE_URL, destination, validate_taxi_zones, force=force)

