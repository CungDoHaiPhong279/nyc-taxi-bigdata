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
    "pickup_day_of_week",
    "pickup_month",
    "is_weekend",
    "is_holiday",
    "pickup_location_id",
    "dropoff_location_id",
    "temperature_2m",
    "precipitation",
    "snowfall",
    "wind_speed_10m",
]
LEGACY_COLUMN_ALIASES = {
    "pickup_day_of_week": "day_of_week",
    "pickup_month": "month",
    "temperature_2m": "temperature",
    "snowfall": "snow",
    "wind_speed_10m": "wind_speed",
}
TARGET_COLUMN = "fare_amount"

# Post-trip fields such as total_amount and tip_amount are deliberately excluded
# because they would leak information unavailable when predicting the fare.


def build_features(dataset: "pd.DataFrame") -> tuple["pd.DataFrame", "pd.Series"]:
    """Select the approved predictors and target from an engineered gold dataset."""
    selected_columns: list[str] = []
    missing: list[str] = []
    for column in FEATURE_COLUMNS:
        if column in dataset.columns:
            selected_columns.append(column)
        elif LEGACY_COLUMN_ALIASES.get(column) in dataset.columns:
            selected_columns.append(LEGACY_COLUMN_ALIASES[column])
        else:
            missing.append(column)
    if TARGET_COLUMN not in dataset.columns:
        missing.append(TARGET_COLUMN)
    if missing:
        raise ValueError(f"Dataset is missing required columns: {', '.join(missing)}")

    logger.info("Selected %d features and target %s", len(selected_columns), TARGET_COLUMN)
    return dataset[selected_columns].copy(), dataset[TARGET_COLUMN].copy()

