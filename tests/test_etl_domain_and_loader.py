"""Tests for 2025 US holiday enrichment, UTC weather coverage, NaN-safe manifest serialization, and vectorized Arrow COPY."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import pyarrow as pa
import pytest

from nyc_taxi.ingestion.manifest import save_manifest
import scripts.create_calendar as create_calendar
import scripts.download_raw_data as download_raw_data


def test_calendar_marks_2025_us_holidays():
    rows = {r["date"]: r for r in create_calendar.build_calendar_rows(date(2025, 1, 1), date(2025, 12, 31))}
    assert rows["2025-01-01"]["is_holiday"] is True
    assert rows["2025-01-01"]["holiday_name"] == "New Year's Day"
    assert rows["2025-07-04"]["is_holiday"] is True
    assert rows["2025-07-04"]["holiday_name"] == "Independence Day"
    assert rows["2025-12-25"]["is_holiday"] is True
    assert rows["2025-03-12"]["is_holiday"] is False
    assert rows["2025-03-12"]["holiday_name"] == ""


def test_weather_download_end_date_covers_june_30_nyc_evening():
    # 2025-06-30 23:00 EDT is 2025-07-01 03:00 UTC
    assert download_raw_data.WEATHER_END >= date(2025, 7, 1)


def test_save_manifest_rejects_nan_values(tmp_path: Path):
    with pytest.raises(ValueError):
        save_manifest(tmp_path / "bad.json", [{"dataset_name": "bad", "metric": float("nan")}])


def test_write_gold_batch_tsv_vectorized():
    from nyc_taxi.database.loader import write_gold_batch_tsv

    batch = pa.RecordBatch.from_pydict(
        {"trip_key": ["k1", "k2"], "fare_amount": [12.5, None], "is_weekend": [True, False]}
    )
    payload = write_gold_batch_tsv(batch, ["trip_key", "fare_amount", "is_weekend"]).decode("utf-8")
    lines = payload.strip().splitlines()
    assert lines[0] == "k1\t12.5\tt"
    assert lines[1] == "k2\t\\N\tf"
