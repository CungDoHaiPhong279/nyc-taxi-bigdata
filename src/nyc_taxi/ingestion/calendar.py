"""Calendar source ingestion scaffold."""

from __future__ import annotations

import logging
from pathlib import Path

from nyc_taxi.ingestion.validation import validate_calendar

logger = logging.getLogger(__name__)


def validate_existing_calendar(path: str | Path) -> dict:
    """Validate the generated 2025 calendar without modifying its contents."""
    report = validate_calendar(path)
    logger.info("Validated %s calendar dates in %s", report["row_count"], path)
    return report


def ingest_calendar(input_path: str | Path, bronze_path: str | Path) -> None:
    """Validate a calendar CSV path before future ingestion."""
    source = Path(input_path)
    if not source.is_file():
        raise FileNotFoundError(f"Calendar input file does not exist: {source}")
    logger.info("Validated calendar input %s; target is %s", source, bronze_path)
    raise NotImplementedError("Calendar ingestion is planned for the next ETL step")

