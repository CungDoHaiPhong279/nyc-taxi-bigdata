"""Silver-to-gold analytical join scaffold."""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def build_gold(
    taxi_path: str | Path,
    weather_path: str | Path,
    calendar_path: str | Path,
    taxi_zones_path: str | Path,
    gold_path: str | Path,
) -> None:
    """Plan the taxi + weather + calendar + taxi-zone analytical join."""
    sources = [Path(path) for path in (taxi_path, weather_path, calendar_path, taxi_zones_path)]
    missing = [str(path) for path in sources if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing silver source path(s): {', '.join(missing)}")
    logger.info("Gold scaffold validated %d sources; target is %s", len(sources), gold_path)
    # TODO: Define time/location join rules and write the analytical dataset.
    raise NotImplementedError("Gold transformation is not implemented yet")

