"""Unit tests for Gold 2025 ML V3 data and feature preparation."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ml.gold_v3_data import (
    CONTRACT_VERSION,
    FEATURE_COLUMNS,
    load_gold_v3_contract,
    prepare_gold_v3_samples,
)


def _write_synthetic_gold_parquet(path: Path) -> None:
    rows = [
        # Train row 1: normal standard-metered trip in Jan 2025
        {
            "vendor_id": 2,
            "pickup_datetime_local": datetime(2025, 1, 15, 8, 30, 0),
            "dropoff_datetime_local": datetime(2025, 1, 15, 8, 50, 0),
            "passenger_count": None,  # Must NOT be filtered out!
            "trip_distance": 3.2,
            "ratecode_id": 1,
            "pickup_location_id": 161,
            "dropoff_location_id": 236,
            "fare_amount": 18.5,
            "temperature_2m": 2.5,
            "precipitation": 0.0,
            "snowfall": 0.0,
            "wind_speed_10m": 12.0,
            "weather_code": 1,
            "weather_matched": True,
        },
        # Train row 2: rare pickup zone 99 & dropoff zone 105 in Feb 2025
        {
            "vendor_id": 1,
            "pickup_datetime_local": datetime(2025, 2, 10, 14, 15, 0),
            "dropoff_datetime_local": datetime(2025, 2, 10, 14, 35, 0),
            "passenger_count": 1,
            "trip_distance": 4.0,
            "ratecode_id": 1,
            "pickup_location_id": 99,
            "dropoff_location_id": 105,
            "fare_amount": 22.0,
            "temperature_2m": 5.0,
            "precipitation": 1.2,
            "snowfall": 0.0,
            "wind_speed_10m": 15.0,
            "weather_code": 61,
            "weather_matched": True,
        },
        # Train row 3: JFK (132) <-> Manhattan (161) flat fare Ratecode 2 ($70)
        {
            "vendor_id": 2,
            "pickup_datetime_local": datetime(2025, 3, 5, 10, 0, 0),
            "dropoff_datetime_local": datetime(2025, 3, 5, 10, 55, 0),
            "passenger_count": 2,
            "trip_distance": 17.5,
            "ratecode_id": 2,
            "pickup_location_id": 132,
            "dropoff_location_id": 161,
            "fare_amount": 70.0,
            "temperature_2m": 8.0,
            "precipitation": 0.0,
            "snowfall": 0.0,
            "wind_speed_10m": 10.0,
            "weather_code": 0,
            "weather_matched": True,
        },
        # Train row 4: corrupt meter reading (> $500) -> must be rejected
        {
            "vendor_id": 2,
            "pickup_datetime_local": datetime(2025, 4, 1, 12, 0, 0),
            "dropoff_datetime_local": datetime(2025, 4, 1, 12, 20, 0),
            "passenger_count": 1,
            "trip_distance": 2.1,
            "ratecode_id": 1,
            "pickup_location_id": 161,
            "dropoff_location_id": 162,
            "fare_amount": 863372.12,
            "temperature_2m": 12.0,
            "precipitation": 0.0,
            "snowfall": 0.0,
            "wind_speed_10m": 8.0,
            "weather_code": 0,
            "weather_matched": True,
        },
        # Validation row: May 2025
        {
            "vendor_id": 2,
            "pickup_datetime_local": datetime(2025, 5, 12, 18, 45, 0),
            "dropoff_datetime_local": datetime(2025, 5, 12, 19, 5, 0),
            "passenger_count": 1,
            "trip_distance": 2.8,
            "ratecode_id": 1,
            "pickup_location_id": 236,
            "dropoff_location_id": 161,
            "fare_amount": 16.8,
            "temperature_2m": 19.5,
            "precipitation": 0.0,
            "snowfall": 0.0,
            "wind_speed_10m": 9.5,
            "weather_code": 2,
            "weather_matched": True,
        },
        # Test row: June 2025
        {
            "vendor_id": 1,
            "pickup_datetime_local": datetime(2025, 6, 20, 9, 0, 0),
            "dropoff_datetime_local": datetime(2025, 6, 20, 9, 25, 0),
            "passenger_count": None,
            "trip_distance": 5.1,
            "ratecode_id": 1,
            "pickup_location_id": 138,
            "dropoff_location_id": 161,
            "fare_amount": 28.7,
            "temperature_2m": 26.0,
            "precipitation": 0.4,
            "snowfall": 0.0,
            "wind_speed_10m": 11.0,
            "weather_code": 51,
            "weather_matched": True,
        },
    ]
    schema = pa.schema(
        [
            ("vendor_id", pa.int32()),
            ("pickup_datetime_local", pa.timestamp("us")),
            ("dropoff_datetime_local", pa.timestamp("us")),
            ("passenger_count", pa.int64()),
            ("trip_distance", pa.float64()),
            ("ratecode_id", pa.int64()),
            ("pickup_location_id", pa.int32()),
            ("dropoff_location_id", pa.int32()),
            ("fare_amount", pa.float64()),
            ("temperature_2m", pa.float64()),
            ("precipitation", pa.float64()),
            ("snowfall", pa.float64()),
            ("wind_speed_10m", pa.float64()),
            ("weather_code", pa.int64()),
            ("weather_matched", pa.bool_()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


def test_gold_v3_contract_loads() -> None:
    contract = load_gold_v3_contract()
    assert contract["contract_version"] == CONTRACT_VERSION
    assert tuple(contract["numeric_features"] + contract["categorical_features"]) == FEATURE_COLUMNS
    assert contract["flat_fare_policy"]["flat_fare_usd"] == 70.0


def test_prepare_gold_v3_samples_synthetic(tmp_path: Path) -> None:
    src_pq = tmp_path / "synthetic_gold.parquet"
    _write_synthetic_gold_parquet(src_pq)
    out_dir = tmp_path / "out"

    report = prepare_gold_v3_samples(
        {"2025-01": [src_pq]},
        out_dir,
        sampling_fraction=1.0,
        min_train_zone_support=5,
    )

    assert report["status"] == "PREPARED"
    # Null passenger_count rows kept; corrupt fare > 500 rejected; Ratecode 2 tallied
    assert report["counts"]["selected_rows"] == {"train": 2, "validation": 1, "test": 1}
    assert report["counts"]["rejected"]["nonpositive_or_extreme_fare"] == 1
    assert report["jfk_manhattan_flat_fare_verification"]["trip_count"] == 1
    assert report["jfk_manhattan_flat_fare_verification"]["fare_70_count"] == 1

    # Verify Parquet schema and zero missing active zones in train
    assert report["zone_coverage"]["train"]["missing_from_sample_pickup"] == []
    assert report["zone_coverage"]["train"]["missing_from_sample_dropoff"] == []

    train_feats = pq.read_table(out_dir / "train_features.parquet")
    assert set(FEATURE_COLUMNS).issubset(train_feats.column_names)
    assert "trip_distance" not in train_feats.column_names
    assert train_feats.num_rows == 2
