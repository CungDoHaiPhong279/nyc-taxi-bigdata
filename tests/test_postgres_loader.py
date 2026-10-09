"""Offline invariants for the Gold-to-PostgreSQL bulk loader."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.database.loader import (
    FACT_COLUMNS,
    iter_parquet_rows,
    map_fact_row,
    serialize_copy_row,
    should_skip_month,
)


ROOT = Path(__file__).resolve().parents[1]


def _gold_row(**updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "_source_year": 2025,
        "_source_month": 1,
        "vendor_id": 2,
        "pickup_datetime_local": datetime(2025, 1, 20, 10),
        "dropoff_datetime_local": datetime(2025, 1, 20, 10, 15),
        "pickup_date": date(2025, 1, 20),
        "pickup_hour": 10,
        "pickup_location_id": 1,
        "dropoff_location_id": 2,
        "passenger_count": None,
        "trip_distance": 1.5,
        "ratecode_id": None,
        "store_and_fwd_flag": "N",
        "payment_type": 1,
        "fare_amount": 10.0,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 1.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 12.5,
        "congestion_surcharge": 0.0,
        "airport_fee": 0.0,
        "cbd_congestion_fee": 0.0,
        "pickup_zone_matched": True,
        "dropoff_zone_matched": True,
        "calendar_matched": True,
        "weather_matched": True,
        "weather_unmatched_reason": None,
        "weather_timestamp_utc": datetime(2025, 1, 20, 15, tzinfo=timezone.utc),
        "_source_file": "data/raw/taxi/yellow_tripdata_2025-01.parquet",
        "_ingestion_batch_id": "bronze-fixture",
        "_silver_batch_id": "silver-fixture",
        "_gold_batch_id": "gold-fixture",
    }
    row.update(updates)
    return row


def _mapped(row: dict[str, object], weather_ids: dict[datetime, int]) -> dict[str, object]:
    values = map_fact_row(row, weather_ids)
    assert len(values) == len(FACT_COLUMNS)
    return dict(zip(FACT_COLUMNS, values, strict=True))


def test_copy_text_escapes_delimiters_and_preserves_sql_null() -> None:
    assert serialize_copy_row((None, "a\tb", "one\ntwo", "a\rb", "a\\b", 7)) == (
        "\\N\ta\\tb\tone\\ntwo\ta\\rb\ta\\\\b\t7\n"
    )
    assert serialize_copy_row((True, False, r"\N")) == "t\tf\t\\\\N\n"
    with pytest.raises(ValueError, match="NUL"):
        serialize_copy_row(("bad\x00text",))


def test_fact_mapping_uses_nullable_foreign_keys_and_numeric_payment_code() -> None:
    timestamp_utc = datetime(2025, 1, 20, 15, tzinfo=timezone.utc)
    mapped = _mapped(_gold_row(), {timestamp_utc: 42})
    assert mapped["source_year"] == 2025
    assert mapped["source_month"] == 1
    assert mapped["pickup_date"] == date(2025, 1, 20)
    assert mapped["calendar_date_id"] == date(2025, 1, 20)
    assert mapped["weather_id"] == 42
    assert mapped["payment_type"] == 1
    assert isinstance(mapped["payment_type"], int)
    assert mapped["passenger_count"] is None
    assert mapped["_gold_batch_id"] == "gold-fixture"

    unmatched = _mapped(
        _gold_row(
            calendar_matched=False,
            weather_matched=False,
            weather_timestamp_utc=None,
            weather_unmatched_reason="weather_edge_coverage",
            payment_type=None,
        ),
        {},
    )
    assert unmatched["pickup_date"] == date(2025, 1, 20)
    assert unmatched["calendar_date_id"] is None
    assert unmatched["weather_id"] is None
    assert unmatched["payment_type"] is None
    assert unmatched["weather_unmatched_reason"] == "weather_edge_coverage"


def test_matched_weather_without_dimension_key_fails() -> None:
    with pytest.raises(ValueError, match="(?i)weather|missing|mapping"):
        map_fact_row(_gold_row(), {})


def test_month_skip_requires_matching_gold_batch_and_row_count() -> None:
    log = {
        "status": "loaded",
        "gold_batch_id": "gold-fixture",
        "source_row_count": 3,
        "loaded_row_count": 3,
    }
    assert should_skip_month(log, "gold-fixture", 3, 3)
    assert not should_skip_month(None, "gold-fixture", 3, 3)
    assert not should_skip_month(log, "new-gold-batch", 3, 3)
    assert not should_skip_month(log, "gold-fixture", 3, 2)
    assert not should_skip_month(log, "gold-fixture", 3, 3, force=True)
    assert not should_skip_month({**log, "status": "failed"}, "gold-fixture", 3, 3)
    assert not should_skip_month({**log, "source_row_count": 2}, "gold-fixture", 3, 3)


def test_parquet_iterator_uses_small_batches_and_projected_columns(tmp_path: Path) -> None:
    directory = tmp_path / "gold"
    directory.mkdir()
    pq.write_table(
        pa.table({"source_month": [1, 1, 1], "payment_type": [1, None, 2], "unused": [9, 9, 9]}),
        directory / "part-000.parquet",
    )
    rows = list(iter_parquet_rows(directory, ["source_month", "payment_type"], batch_size=2))
    assert rows == [
        {"source_month": 1, "payment_type": 1},
        {"source_month": 1, "payment_type": None},
        {"source_month": 1, "payment_type": 2},
    ]


def test_sql_star_schema_matches_gold_without_fake_dimension_rows() -> None:
    ddl = (ROOT / "sql" / "02_create_tables.sql").read_text(encoding="utf-8")
    indexes = (ROOT / "sql" / "postload" / "create_indexes.sql").read_text(encoding="utf-8")
    for name in ("dim_location", "dim_calendar", "dim_weather", "fact_trip", "etl_load_log"):
        assert re.search(rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+analytics\.{name}\b", ddl, re.I)
    for column, datatype in (
        ("source_year", "SMALLINT|INTEGER"),
        ("source_month", "SMALLINT|INTEGER"),
        ("payment_type", "BIGINT|INTEGER"),
    ):
        assert re.search(rf"^\s*{column}\s+(?:{datatype})\b", ddl, re.I | re.M)
    for nullable_fk in ("calendar_date_id", "weather_id"):
        declaration = re.search(rf"^\s*{nullable_fk}\s+[^,\n]+", ddl, re.I | re.M)
        assert declaration is not None
        assert "NOT NULL" not in declaration.group().upper()
    assert re.search(r"\bweather_timestamp_utc\b", ddl)
    assert re.search(r"\bUNIQUE\s*\(\s*weather_timestamp_utc\s*\)", ddl, re.I) or re.search(
        r"^\s*weather_timestamp_utc\b[^\n]*\bUNIQUE\b", ddl, re.I | re.M
    )
    assert not re.search(r"\bUNIQUE\s*\(\s*weather_date_nyc\s*,\s*weather_hour_nyc\s*\)", ddl, re.I)
    assert re.search(r"\btrip_id\b[^\n]*(?:BIGSERIAL|IDENTITY)", ddl, re.I)
    assert re.search(r"\bsource_year\s*,\s*source_month\b", ddl + indexes, re.I)
    for column, table in (
        ("pickup_location_id", "dim_location"),
        ("dropoff_location_id", "dim_location"),
        ("calendar_date_id", "dim_calendar"),
        ("weather_id", "dim_weather"),
    ):
        assert re.search(rf"^\s*{column}\b[^\n]*REFERENCES\s+analytics\.{table}\b", ddl, re.I | re.M)
    for column in (
        "pickup_datetime_local", "pickup_location_id", "dropoff_location_id",
        "calendar_date_id", "weather_id", "pickup_hour",
    ):
        assert re.search(rf"ON\s+analytics\.fact_trip\s*\(\s*{column}\s*\)", indexes, re.I)
    assert re.search(r"ON\s+analytics\.dim_weather\s*\(\s*weather_date_nyc\s*,\s*weather_hour_nyc\s*\)", indexes, re.I)
    assert not re.search(r"^\s*DROP\s+(?:DATABASE|SCHEMA|TABLE)\b", ddl, re.I | re.M)
