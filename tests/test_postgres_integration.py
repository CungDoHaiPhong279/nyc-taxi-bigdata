"""Opt-in PostgreSQL fixture; never touches the configured production DB."""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from nyc_taxi.database.loader import FACT_COLUMNS, apply_schema, map_fact_row, serialize_copy_row
from tests.test_postgres_loader import _gold_row


ROOT = Path(__file__).resolve().parents[1]
ENABLED = os.environ.get("NYC_TAXI_RUN_POSTGRES_INTEGRATION") == "1"
TEST_URL = os.environ.get("NYC_TAXI_TEST_POSTGRES_URL")


@pytest.fixture
def isolated_connection():
    """Allow writes only in an empty, explicitly named test database."""
    psycopg = pytest.importorskip("psycopg")
    assert TEST_URL is not None
    connection = psycopg.connect(TEST_URL, connect_timeout=5)
    try:
        database = connection.info.dbname or ""
        if not re.search(r"(?:^test_|_test$|_test_)", database, re.I):
            pytest.fail("Refusing integration test: database name must contain _test")

        with connection.cursor() as cursor:
            cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'analytics')")
            if cursor.fetchone()[0]:
                pytest.fail("Refusing integration test: analytics schema must not already exist in test DB")
        yield connection
    finally:
        connection.rollback()
        connection.close()


@pytest.mark.skipif(
    not ENABLED or not TEST_URL,
    reason="Set NYC_TAXI_RUN_POSTGRES_INTEGRATION=1 and NYC_TAXI_TEST_POSTGRES_URL for an isolated test DB",
)
def test_small_copy_fixture_and_nullable_dimension_foreign_keys(isolated_connection) -> None:
    """Run DDL and tiny COPY inside a transaction that is always rolled back."""
    connection = isolated_connection
    with connection.cursor() as cursor:
        cursor.execute((ROOT / "sql" / "01_create_schema.sql").read_text(encoding="utf-8"))
        cursor.execute((ROOT / "sql" / "02_create_tables.sql").read_text(encoding="utf-8"))
        cursor.execute(
            "INSERT INTO analytics.dim_location (location_id, borough, zone, service_zone) "
            "VALUES (1, 'Manhattan', 'Midtown', 'Yellow Zone'), "
            "(2, 'Queens', 'Airport', 'Airports')"
        )
        cursor.execute(
            "INSERT INTO analytics.dim_calendar "
            "(date_id, day_of_week, day_name, month, year, is_weekend, is_holiday, holiday_name) "
            "VALUES (%s, 0, 'Monday', 1, 2025, false, true, 'Fixture holiday')",
            (date(2025, 1, 20),),
        )
        weather_utc = datetime(2025, 1, 20, 15, tzinfo=timezone.utc)
        cursor.execute(
            "INSERT INTO analytics.dim_weather "
            "(weather_timestamp_utc, weather_timestamp_nyc, weather_date_nyc, weather_hour_nyc) "
            "VALUES (%s, %s, %s, 10) RETURNING weather_id",
            (weather_utc, datetime(2025, 1, 20, 10), date(2025, 1, 20)),
        )
        weather_id = cursor.fetchone()[0]

        matched = _gold_row()
        unmatched = _gold_row(
            pickup_datetime_local=datetime(2007, 1, 1, 12),
            dropoff_datetime_local=datetime(2007, 1, 1, 12, 15),
            pickup_date=date(2007, 1, 1),
            pickup_hour=12,
            calendar_matched=False,
            weather_matched=False,
            weather_timestamp_utc=None,
            weather_unmatched_reason="out_of_analysis_period",
            payment_type=None,
        )
        copy_sql = f"COPY analytics.fact_trip ({', '.join(FACT_COLUMNS)}) FROM STDIN"
        with cursor.copy(copy_sql) as writer:
            for row in (matched, unmatched):
                writer.write(serialize_copy_row(map_fact_row(row, {weather_utc: weather_id})))

        cursor.execute(
            "SELECT count(*), count(*) FILTER (WHERE calendar_date_id IS NULL), "
            "count(*) FILTER (WHERE weather_id IS NULL) FROM analytics.fact_trip"
        )
        assert cursor.fetchone() == (2, 1, 1)
        cursor.execute(
            "SELECT payment_type, pg_typeof(payment_type)::text FROM analytics.fact_trip "
            "WHERE calendar_matched"
        )
        assert cursor.fetchone() == (1, "bigint")


@pytest.mark.skipif(
    not ENABLED or not TEST_URL,
    reason="Set NYC_TAXI_RUN_POSTGRES_INTEGRATION=1 and NYC_TAXI_TEST_POSTGRES_URL for an isolated test DB",
)
def test_guarded_legacy_migration_accepts_empty_and_refuses_nonempty(isolated_connection) -> None:
    """Opt-in migration must never discard even one legacy data row."""
    connection = isolated_connection
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA analytics")
        cursor.execute(
            "CREATE TABLE analytics.dim_weather (weather_id BIGSERIAL PRIMARY KEY, "
            "observation_datetime TIMESTAMP NOT NULL)"
        )
        cursor.execute("CREATE TABLE analytics.fact_trip (trip_id BIGINT PRIMARY KEY)")
    apply_schema(connection, recreate=True)
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = 'analytics' AND table_name = 'dim_weather' "
            "AND column_name = 'weather_timestamp_utc')"
        )
        assert cursor.fetchone()[0] is True
        cursor.execute("SELECT count(*) FROM analytics.fact_trip")
        assert cursor.fetchone()[0] == 0

    connection.rollback()
    with connection.cursor() as cursor:
        cursor.execute("CREATE SCHEMA analytics")
        cursor.execute("CREATE TABLE analytics.fact_trip (trip_id BIGINT PRIMARY KEY)")
        cursor.execute("INSERT INTO analytics.fact_trip (trip_id) VALUES (123)")
    with pytest.raises(Exception, match="(?i)refusing.*not empty"):
        apply_schema(connection, recreate=True)
    with connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM analytics.fact_trip")
        assert cursor.fetchone()[0] == 1
