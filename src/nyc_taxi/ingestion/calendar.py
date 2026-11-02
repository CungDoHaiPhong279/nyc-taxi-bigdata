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

