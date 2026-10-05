"""Bronze-to-silver transformation scaffold."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def build_silver(bronze_path: str | Path, silver_path: str | Path) -> None:
    """Plan deduplication, type casting, validation, and standardization."""
    source = Path(bronze_path)
    if not source.exists():
        raise FileNotFoundError(f"Bronze source path does not exist: {source}")
    logger.info("Silver scaffold validated %s -> %s", source, silver_path)
    # TODO: Remove duplicates/missing/invalid rows, cast types, normalize
    # datetimes, validate ranges, and standardize categorical values.
    raise NotImplementedError("Silver transformation is not implemented yet")

