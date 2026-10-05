"""Feature selection for taxi-fare modelling."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)

FEATURE_COLUMNS = [
    "trip_distance",
    "passenger_count",
    "pickup_hour",
    "day_of_week",
    "month",
    "is_weekend",
    "is_holiday",
    "pickup_location_id",
    "dropoff_location_id",
    "temperature",
    "precipitation",
    "snow",
    "wind_speed",
]
TARGET_COLUMN = "fare_amount"

# Post-trip fields such as total_amount and tip_amount are deliberately excluded
# because they would leak information unavailable when predicting the fare.


def build_features(dataset: "pd.DataFrame") -> tuple["pd.DataFrame", "pd.Series"]:
    """Select the approved predictors and target from an engineered gold dataset."""
    required = [*FEATURE_COLUMNS, TARGET_COLUMN]
    missing = [column for column in required if column not in dataset.columns]
    if missing:
        raise ValueError(f"Dataset is missing required columns: {', '.join(missing)}")

    logger.info("Selected %d features and target %s", len(FEATURE_COLUMNS), TARGET_COLUMN)
    return dataset[FEATURE_COLUMNS].copy(), dataset[TARGET_COLUMN].copy()

