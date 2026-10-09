"""Bounded-memory Gold/Silver to PostgreSQL analytical mart loader.

Taxi local timestamps stay wall-clock timestamps. Weather identity is the UTC
instant, not a NYC local hour (which can repeat during a fall DST transition).
"""

from __future__ import annotations

import json
import os
import tempfile
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from nyc_taxi.common.config import PROJECT_ROOT

SCHEMA_VERSION = 2
SQL_DIR = PROJECT_ROOT / "sql"
EXPECTED_DIMENSION_COUNTS = {"location": 265, "calendar": 365, "weather": 4344}
MONTHS = tuple(f"2025-{month:02d}" for month in range(1, 7))

LOCATION_COLUMNS = ("location_id", "borough", "zone", "service_zone")
CALENDAR_COLUMNS = (
    "date_id", "day_of_week", "day_name", "month", "year", "is_weekend",
    "is_holiday", "holiday_name",
)
WEATHER_COLUMNS = (
    "weather_timestamp_utc", "weather_timestamp_nyc", "weather_date_nyc",
    "weather_hour_nyc", "temperature_2m", "precipitation", "snowfall",
    "wind_speed_10m", "weather_code",
)
FACT_COLUMNS = (
    "source_year", "source_month", "vendor_id", "pickup_datetime_local",
    "dropoff_datetime_local", "pickup_date", "pickup_hour",
    "pickup_location_id", "dropoff_location_id", "calendar_date_id", "weather_id",
    "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
    "pickup_zone_matched", "dropoff_zone_matched", "calendar_matched",
    "weather_matched", "weather_unmatched_reason", "_source_file",
    "_ingestion_batch_id", "_silver_batch_id", "_gold_batch_id",
)

_FACT_SOURCE_COLUMNS = (
    # Explicit projection: PyArrow reads only columns required by PostgreSQL.
    "_source_year", "_source_month", "vendor_id", "pickup_datetime_local",
    "dropoff_datetime_local", "pickup_date", "pickup_hour",
    "pickup_location_id", "dropoff_location_id", "weather_timestamp_utc",
    "passenger_count", "trip_distance", "ratecode_id", "store_and_fwd_flag",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
    "pickup_zone_matched", "dropoff_zone_matched", "calendar_matched",
    "weather_matched", "weather_unmatched_reason", "_source_file",
    "_ingestion_batch_id", "_silver_batch_id", "_gold_batch_id",
)


def normalize_utc(value: datetime) -> datetime:
    """Interpret Spark's timezone-less Parquet UTC value as a UTC instant."""
    if not isinstance(value, datetime):
        raise ValueError(f"Expected UTC datetime, got {type(value).__name__}")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def serialize_copy_row(values: Sequence[object]) -> str:
    """Serialize one row for PostgreSQL COPY TEXT, including its newline.

    PostgreSQL's default null token is the *unescaped* ``\\N``. Actual text
    backslashes are doubled first, so a literal ``\\N`` is never mistaken for
    NULL. No whole-file CSV or DataFrame is materialized.
    """
    fields: list[str] = []
    for value in values:
        if value is None:
            fields.append(r"\N")
            continue
        if isinstance(value, bool):
            text = "t" if value else "f"
        elif isinstance(value, datetime):
            text = value.isoformat(sep=" ")
        elif isinstance(value, date):
            text = value.isoformat()
        else:
            text = str(value)
        if "\x00" in text:
            raise ValueError("PostgreSQL COPY cannot load NUL characters")
        fields.append(
            text.replace("\\", "\\\\").replace("\t", "\\t")
            .replace("\n", "\\n").replace("\r", "\\r")
        )
    return "\t".join(fields) + "\n"


def iter_parquet_rows(
    path: str | Path, columns: Sequence[str], batch_size: int = 10_000,
) -> Iterator[dict[str, Any]]:
    """Stream only selected Parquet columns, at most one Arrow batch at a time."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    source = Path(path)
    if not source.is_dir():
        raise FileNotFoundError(f"Parquet directory does not exist: {source}")
    import pyarrow.dataset as ds

    dataset = ds.dataset(str(source), format="parquet")
    missing = set(columns).difference(dataset.schema.names)
    if missing:
        raise ValueError(f"Parquet columns missing from {source}: {sorted(missing)}")
    scanner = dataset.scanner(columns=list(columns), batch_size=batch_size)
    for batch in scanner.to_batches():
        yield from batch.to_pylist()


def map_fact_row(
    row: Mapping[str, Any], weather_ids: Mapping[datetime, int],
) -> tuple[Any, ...]:
    """Map one Gold trip to the ordered fact COPY columns, without row loss."""
    if not row["pickup_zone_matched"] or not row["dropoff_zone_matched"]:
        raise ValueError("Gold trip has an unmatched location; cannot satisfy location FK")
    calendar_matched = row["calendar_matched"]
    weather_matched = row["weather_matched"]
    if not isinstance(calendar_matched, bool) or not isinstance(weather_matched, bool):
        raise ValueError("Gold match flags must be non-null booleans")
    weather_id: int | None = None
    if weather_matched:
        stamp = row["weather_timestamp_utc"]
        if stamp is None:
            raise ValueError("Weather-matched Gold trip has no UTC weather timestamp")
        weather_key = normalize_utc(stamp)
        weather_id = weather_ids.get(weather_key)
        if weather_id is None:
            raise ValueError(f"Weather UTC timestamp absent from dim_weather: {weather_key}")
    elif row["weather_timestamp_utc"] is not None:
        raise ValueError("Weather-unmatched Gold trip unexpectedly has a weather timestamp")

    mapped = {
        "source_year": row["_source_year"],
        "source_month": row["_source_month"],
        "calendar_date_id": row["pickup_date"] if calendar_matched else None,
        "weather_id": weather_id,
        "payment_type": None if row["payment_type"] is None else int(row["payment_type"]),
    }
    return tuple(mapped[column] if column in mapped else row[column] for column in FACT_COLUMNS)


def should_skip_month(
    log: Mapping[str, Any] | None, gold_batch_id: str, expected_rows: int,
    actual_rows: int, *, force: bool = False,
) -> bool:
    """Skip only a complete, physically matching load of the same Gold batch."""
    return bool(
        not force and log and log.get("status") == "loaded"
        and log.get("gold_batch_id") == gold_batch_id
        and int(log.get("source_row_count", -1)) == expected_rows
        and int(log.get("loaded_row_count", -1)) == expected_rows
        and actual_rows == expected_rows
    )


def _copy_rows(
    connection: Any, table: str, columns: Sequence[str],
    rows: Iterable[Sequence[object]], *, batch_size: int,
) -> int:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    statement = f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT text)"
    count = 0
    chunk: list[str] = []
    with connection.cursor() as cursor, cursor.copy(statement) as writer:
        for values in rows:
            chunk.append(serialize_copy_row(values))
            count += 1
            if len(chunk) >= batch_size:
                writer.write("".join(chunk))
                chunk.clear()
        if chunk:
            writer.write("".join(chunk))
    return count


def _sql_file(connection: Any, filename: str) -> None:
    statement = (SQL_DIR / filename).read_text(encoding="utf-8")
    with connection.cursor() as cursor:
        cursor.execute(statement)


def _assert_schema_columns(connection: Any) -> None:
    expected = {
        "dim_location": {"location_id", *LOCATION_COLUMNS},
        "dim_calendar": set(CALENDAR_COLUMNS),
        "dim_weather": {"weather_id", *WEATHER_COLUMNS},
        "fact_trip": {"trip_id", *FACT_COLUMNS},
        "etl_load_log": {
            "dataset", "source_year", "source_month", "gold_batch_id",
            "source_row_count", "loaded_row_count", "started_at", "completed_at", "status",
        },
    }
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE table_schema = 'analytics'"
        )
        actual: dict[str, set[str]] = {}
        for table, column in cursor.fetchall():
            actual.setdefault(table, set()).add(column)
    for table, columns in expected.items():
        missing = columns.difference(actual.get(table, set()))
        if missing:
            raise ValueError(f"analytics.{table} is incompatible; missing columns: {sorted(missing)}")


def apply_schema(connection: Any, *, recreate: bool = False) -> None:
    """Install v2 DDL; explicit recreate only removes known, *empty* legacy tables."""
    with connection.transaction():
        if recreate:
            _sql_file(connection, "migrations/recreate_empty_analytics.sql")
        _sql_file(connection, "01_create_schema.sql")
        _sql_file(connection, "02_create_tables.sql")
        _assert_schema_columns(connection)


def validate_schema(connection: Any) -> None:
    """Read-only compatibility check for standalone ``--validate``."""
    _assert_schema_columns(connection)


def create_indexes(connection: Any) -> list[str]:
    """Build deferred indexes and the non-materialized convenience view."""
    with connection.transaction():
        _sql_file(connection, "postload/create_indexes.sql")
        _sql_file(connection, "postload/create_views.sql")
    return list_indexes(connection)


def list_indexes(connection: Any) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'analytics' ORDER BY indexname"
        )
        return [row[0] for row in cursor.fetchall()]


def _dimension_source_rows(
    path: Path, source_columns: Sequence[str], target_columns: Sequence[str],
    *, batch_size: int,
) -> Iterator[tuple[Any, ...]]:
    for row in iter_parquet_rows(path, source_columns, batch_size):
        mapped = dict(row)
        if "date_id" in target_columns:
            mapped["date_id"] = row["date"]
        if "weather_timestamp_utc" in target_columns:
            mapped["weather_timestamp_utc"] = normalize_utc(row["weather_timestamp_utc"])
        yield tuple(mapped[column] for column in target_columns)


def _stage_dimension(
    connection: Any, *, name: str, path: Path, source_columns: Sequence[str],
    target_columns: Sequence[str], key_columns: Sequence[str], expected_rows: int,
    batch_size: int,
) -> None:
    table = f"analytics.dim_{name}"
    stage = f"pg_temp._stage_dim_{name}"
    with connection.cursor() as cursor:
        # CTAS copies only selected data types. LIKE would copy NOT NULL from
        # weather_id but not its identity default, making COPY fail.
        cursor.execute(
            f"CREATE TEMP TABLE _stage_dim_{name} ON COMMIT DROP AS "
            f"SELECT {', '.join(target_columns)} FROM {table} WHERE FALSE"
        )
    copied = _copy_rows(
        connection, stage, target_columns,
        _dimension_source_rows(path, source_columns, target_columns, batch_size=batch_size),
        batch_size=batch_size,
    )
    if copied != expected_rows:
        raise ValueError(f"dim_{name} Silver rows {copied} != expected {expected_rows}")
    key_sql = ", ".join(key_columns)
    with connection.cursor() as cursor:
        cursor.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT ({key_sql})) FROM {stage}"
        )
        rows, distinct_keys = cursor.fetchone()
        if rows != expected_rows or distinct_keys != expected_rows:
            raise ValueError(f"dim_{name} Silver key is null or not unique")
        for column in key_columns:
            cursor.execute(f"SELECT EXISTS(SELECT 1 FROM {stage} WHERE {column} IS NULL)")
            if cursor.fetchone()[0]:
                raise ValueError(f"dim_{name} Silver key {column} contains null")
        if name == "calendar":
            cursor.execute(f"SELECT COUNT(*) FROM {stage} WHERE is_holiday")
            if cursor.fetchone()[0] != 11:
                raise ValueError("Silver Calendar must contain 11 federal holidays")
        update_columns = [column for column in target_columns if column not in key_columns]
        update_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
        cursor.execute(
            f"INSERT INTO {table} ({', '.join(target_columns)}) "
            f"SELECT {', '.join(target_columns)} FROM {stage} WHERE TRUE "
            f"ON CONFLICT ({key_sql}) DO UPDATE SET {update_sql}"
        )


def load_dimensions(
    connection: Any, silver_paths: Mapping[str, str | Path], *, batch_size: int = 10_000,
) -> dict[str, Any]:
    """Atomically stage/upsert all three Silver dimensions, then validate."""
    started = time.monotonic()
    required = {"taxi-zones", "calendar", "weather"}
    if set(silver_paths) != required:
        raise ValueError(f"silver_paths must contain exactly {sorted(required)}")
    with connection.transaction():
        _stage_dimension(
            connection, name="location", path=Path(silver_paths["taxi-zones"]),
            source_columns=LOCATION_COLUMNS, target_columns=LOCATION_COLUMNS,
            key_columns=("location_id",), expected_rows=265, batch_size=batch_size,
        )
        _stage_dimension(
            connection, name="calendar", path=Path(silver_paths["calendar"]),
            source_columns=("date", *CALENDAR_COLUMNS[1:]),
            target_columns=CALENDAR_COLUMNS, key_columns=("date_id",),
            expected_rows=365, batch_size=batch_size,
        )
        _stage_dimension(
            connection, name="weather", path=Path(silver_paths["weather"]),
            source_columns=WEATHER_COLUMNS, target_columns=WEATHER_COLUMNS,
            key_columns=("weather_timestamp_utc",), expected_rows=4344,
            batch_size=batch_size,
        )
        counts = validate_dimension_counts(connection)
    return {"counts": counts, "elapsed_seconds": round(time.monotonic() - started, 2)}


def validate_dimension_counts(connection: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    with connection.cursor() as cursor:
        for name, expected in EXPECTED_DIMENSION_COUNTS.items():
            cursor.execute(f"SELECT COUNT(*) FROM analytics.dim_{name}")
            actual = cursor.fetchone()[0]
            if actual != expected:
                raise ValueError(f"analytics.dim_{name} has {actual} rows; expected {expected}")
            counts[name] = actual
        cursor.execute("SELECT COUNT(*) FROM analytics.dim_calendar WHERE is_holiday")
        if cursor.fetchone()[0] != 11:
            raise ValueError("analytics.dim_calendar must contain 11 holidays")
    return counts


def get_weather_ids(connection: Any) -> dict[datetime, int]:
    """Fetch only the small canonical weather key-to-surrogate mapping."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT weather_timestamp_utc, weather_id FROM analytics.dim_weather"
        )
        mapping = {normalize_utc(stamp): weather_id for stamp, weather_id in cursor}
    if len(mapping) != EXPECTED_DIMENSION_COUNTS["weather"]:
        raise ValueError("dim_weather UTC keys are missing or duplicated")
    return mapping


def _gold_entry(entry: Mapping[str, Any]) -> tuple[int, int, int, Path, str]:
    if entry.get("dataset") != "taxi_enriched" or entry.get("validation_status") != "valid":
        raise ValueError("Gold manifest entry is not valid")
    partition = entry["partition"]
    year, month = int(partition["year"]), int(partition["month"])
    if f"{year:04d}-{month:02d}" not in MONTHS or entry.get("dataset_name") != f"taxi_{year:04d}-{month:02d}":
        raise ValueError("Gold manifest entry is outside Jan-Jun 2025")
    expected = int(entry["gold_rows"])
    if expected <= 0 or expected != int(entry["silver_rows"]):
        raise ValueError("Gold manifest rows do not match Taxi Silver")
    batch = entry["gold_batch_id"]
    if not isinstance(batch, str) or not batch:
        raise ValueError("Gold batch ID is missing")
    path = PROJECT_ROOT / str(entry["gold_path"])
    expected_path = PROJECT_ROOT / "data" / "gold" / "taxi_enriched" / f"year={year}" / f"month={month:02d}"
    if path.resolve() != expected_path.resolve() or not path.is_dir():
        raise ValueError(f"Gold physical partition path is invalid: {path}")
    return year, month, expected, path, batch


def load_month(
    connection: Any, entry: Mapping[str, Any], weather_ids: Mapping[datetime, int],
    *, force: bool = False, batch_size: int = 10_000,
) -> dict[str, Any]:
    """DELETE + COPY + log one month in a single rollback-safe transaction."""
    year, month, expected, path, batch = _gold_entry(entry)
    started_at = datetime.now(timezone.utc)
    started = time.monotonic()
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(%s, %s)", (year, month))
            cursor.execute(
                "SELECT COUNT(*) FROM analytics.fact_trip WHERE source_year = %s AND source_month = %s",
                (year, month),
            )
            existing_rows = cursor.fetchone()[0]
            cursor.execute(
                "SELECT gold_batch_id, source_row_count, loaded_row_count, status "
                "FROM analytics.etl_load_log WHERE dataset = 'fact_trip' "
                "AND source_year = %s AND source_month = %s FOR UPDATE",
                (year, month),
            )
            record = cursor.fetchone()
            previous = dict(zip(
                ("gold_batch_id", "source_row_count", "loaded_row_count", "status"), record
            )) if record else None
            if should_skip_month(previous, batch, expected, existing_rows, force=force):
                return {
                    "month": f"{year:04d}-{month:02d}", "status": "skipped",
                    "gold_rows": expected, "postgres_rows": existing_rows,
                    "gold_batch_id": batch, "elapsed_seconds": round(time.monotonic() - started, 2),
                }
            if previous is None and existing_rows and not force:
                raise ValueError(
                    f"Unlogged fact rows exist for {year:04d}-{month:02d}; "
                    "inspect them before using --force"
                )
            cursor.execute(
                "DELETE FROM analytics.fact_trip WHERE source_year = %s AND source_month = %s",
                (year, month),
            )

        def mapped_rows() -> Iterator[tuple[Any, ...]]:
            for row in iter_parquet_rows(path, _FACT_SOURCE_COLUMNS, batch_size):
                if row["_source_year"] != year or row["_source_month"] != month:
                    raise ValueError("Gold row source partition differs from manifest")
                if row["_gold_batch_id"] != batch:
                    raise ValueError("Gold physical batch ID differs from manifest")
                if row["_silver_batch_id"] != entry["source_silver_batch_id"]:
                    raise ValueError("Gold physical Silver batch ID differs from manifest")
                yield map_fact_row(row, weather_ids)

        copied = _copy_rows(
            connection, "analytics.fact_trip", FACT_COLUMNS, mapped_rows(),
            batch_size=batch_size,
        )
        if copied != expected:
            raise ValueError(f"Gold Parquet yielded {copied} rows; manifest expected {expected}")
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE calendar_date_id IS NULL), "
                "COUNT(*) FILTER (WHERE weather_id IS NULL) "
                "FROM analytics.fact_trip WHERE source_year = %s AND source_month = %s",
                (year, month),
            )
            actual, calendar_null, weather_null = cursor.fetchone()
            expected_calendar_null = entry["join_counts"]["calendar"]["unmatched"]
            expected_weather_null = entry["join_counts"]["weather"]["unmatched"]
            if (actual != expected or calendar_null != expected_calendar_null
                    or weather_null != expected_weather_null):
                raise ValueError(
                    f"PostgreSQL Gold quality mismatch for {year:04d}-{month:02d}: "
                    f"rows={actual}, calendar_null={calendar_null}, weather_null={weather_null}"
                )
            completed_at = datetime.now(timezone.utc)
            cursor.execute(
                "INSERT INTO analytics.etl_load_log "
                "(dataset, source_year, source_month, gold_batch_id, source_row_count, "
                "loaded_row_count, started_at, completed_at, status) "
                "VALUES ('fact_trip', %s, %s, %s, %s, %s, %s, %s, 'loaded') "
                "ON CONFLICT (dataset, source_year, source_month) DO UPDATE SET "
                "gold_batch_id = EXCLUDED.gold_batch_id, "
                "source_row_count = EXCLUDED.source_row_count, "
                "loaded_row_count = EXCLUDED.loaded_row_count, "
                "started_at = EXCLUDED.started_at, "
                "completed_at = EXCLUDED.completed_at, status = EXCLUDED.status",
                (year, month, batch, expected, actual, started_at, completed_at),
            )
    return {
        "month": f"{year:04d}-{month:02d}", "status": "loaded",
        "gold_rows": expected, "postgres_rows": actual,
        "calendar_null": calendar_null, "weather_null": weather_null,
        "gold_batch_id": batch, "elapsed_seconds": round(time.monotonic() - started, 2),
    }


def _fetch_dicts(connection: Any, query: str, parameters: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(query, parameters)
        names = [column.name for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]


def collect_validation_report(
    connection: Any, gold_entries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return only aggregate SQL results and Gold lineage, never trip rows."""
    dimensions = {}
    for name in EXPECTED_DIMENSION_COUNTS:
        dimensions[name] = _fetch_dicts(
            connection, f"SELECT COUNT(*) AS rows FROM analytics.dim_{name}"
        )[0]["rows"]
    holiday_count = _fetch_dicts(
        connection, "SELECT COUNT(*) AS rows FROM analytics.dim_calendar WHERE is_holiday"
    )[0]["rows"]
    summary = _fetch_dicts(
        connection,
        "SELECT COUNT(*) AS total_trips, "
        "COUNT(*) FILTER (WHERE calendar_date_id IS NULL) AS calendar_null_fk, "
        "COUNT(*) FILTER (WHERE weather_id IS NULL) AS weather_null_fk, "
        "COUNT(*) FILTER (WHERE pickup_location_id IS NULL) AS pickup_location_null_fk, "
        "COUNT(*) FILTER (WHERE dropoff_location_id IS NULL) AS dropoff_location_null_fk "
        "FROM analytics.fact_trip",
    )[0]
    per_month = _fetch_dicts(
        connection,
        "SELECT source_year, source_month, COUNT(*) AS rows FROM analytics.fact_trip "
        "GROUP BY source_year, source_month ORDER BY source_year, source_month",
    )
    logs = _fetch_dicts(
        connection,
        "SELECT dataset, source_year, source_month, gold_batch_id, "
        "source_row_count, loaded_row_count, started_at, completed_at, status, "
        "EXTRACT(EPOCH FROM completed_at - started_at) AS elapsed_seconds "
        "FROM analytics.etl_load_log ORDER BY source_year, source_month",
    )
    missing_pickup = _fetch_dicts(
        connection,
        "SELECT COUNT(*) AS rows FROM analytics.fact_trip f LEFT JOIN analytics.dim_location d "
        "ON f.pickup_location_id = d.location_id WHERE d.location_id IS NULL",
    )[0]["rows"]
    missing_dropoff = _fetch_dicts(
        connection,
        "SELECT COUNT(*) AS rows FROM analytics.fact_trip f LEFT JOIN analytics.dim_location d "
        "ON f.dropoff_location_id = d.location_id WHERE d.location_id IS NULL",
    )[0]["rows"]
    expected = {entry["dataset_name"]: entry for entry in gold_entries}
    by_month = {name.removeprefix("taxi_"): entry for name, entry in expected.items()}
    month_counts = {
        f"{row['source_year']:04d}-{row['source_month']:02d}": row["rows"]
        for row in per_month
    }
    loaded_months = set(month_counts)
    log_by_month = {
        f"{row['source_year']:04d}-{row['source_month']:02d}": row
        for row in logs if row["dataset"] == "fact_trip"
    }
    complete = set(by_month) == set(MONTHS) and loaded_months == set(MONTHS)
    month_lineage_valid = loaded_months.issubset(by_month) and all(
        month_counts[month] == int(by_month[month]["gold_rows"])
        and month in log_by_month
        and log_by_month[month]["status"] == "loaded"
        and log_by_month[month]["gold_batch_id"] == by_month[month]["gold_batch_id"]
        and log_by_month[month]["source_row_count"] == int(by_month[month]["gold_rows"])
        and log_by_month[month]["loaded_row_count"] == month_counts[month]
        and log_by_month[month]["started_at"] is not None
        and log_by_month[month]["completed_at"] is not None
        for month in loaded_months
    )
    expected_calendar_null = sum(
        by_month[month]["join_counts"]["calendar"]["unmatched"]
        for month in loaded_months if month in by_month
    )
    expected_weather_null = sum(
        by_month[month]["join_counts"]["weather"]["unmatched"]
        for month in loaded_months if month in by_month
    )
    consistent = (
        month_lineage_valid and dimensions == EXPECTED_DIMENSION_COUNTS
        and holiday_count == 11
        and summary["calendar_null_fk"] == expected_calendar_null
        and summary["weather_null_fk"] == expected_weather_null
        and missing_pickup == 0 and missing_dropoff == 0
        and summary["total_trips"] == sum(month_counts.values())
        and not (set(log_by_month) - loaded_months)
    )
    sanity = {
        "by_pickup_borough": _fetch_dicts(
            connection,
            "SELECT d.borough, COUNT(*) AS trips FROM analytics.fact_trip f "
            "JOIN analytics.dim_location d ON d.location_id = f.pickup_location_id "
            "GROUP BY d.borough ORDER BY trips DESC",
        ),
        "by_pickup_hour": _fetch_dicts(
            connection,
            "SELECT pickup_hour, COUNT(*) AS trips FROM analytics.fact_trip "
            "GROUP BY pickup_hour ORDER BY pickup_hour",
        ),
        "by_holiday": _fetch_dicts(
            connection,
            "SELECT c.is_holiday, COUNT(*) AS trips FROM analytics.fact_trip f "
            "LEFT JOIN analytics.dim_calendar c ON c.date_id = f.calendar_date_id "
            "GROUP BY c.is_holiday ORDER BY c.is_holiday NULLS LAST",
        ),
        "by_weather_match": _fetch_dicts(
            connection,
            "SELECT weather_matched, COUNT(*) AS trips FROM analytics.fact_trip "
            "GROUP BY weather_matched ORDER BY weather_matched",
        ),
        "by_rain": _fetch_dicts(
            connection,
            "SELECT CASE WHEN w.weather_id IS NULL OR w.precipitation IS NULL "
            "THEN 'unknown' WHEN w.precipitation > 0 THEN 'rainy' ELSE 'dry' END "
            "AS weather_class, COUNT(*) AS trips FROM analytics.fact_trip f "
            "LEFT JOIN analytics.dim_weather w ON w.weather_id = f.weather_id "
            "GROUP BY 1 ORDER BY 1",
        ),
    }
    size = _fetch_dicts(
        connection,
        "SELECT pg_total_relation_size('analytics.fact_trip') AS fact_bytes, "
        "pg_total_relation_size('analytics.dim_weather') AS weather_bytes",
    )[0]
    view_present = _fetch_dicts(
        connection,
        "SELECT to_regclass('analytics.vw_trip_enriched') IS NOT NULL AS present",
    )[0]["present"]
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "validation_status": "valid" if consistent and complete else
                             "partial" if consistent else "invalid",
        "dimension_counts": dimensions,
        "calendar_holiday_count": holiday_count,
        "fact_summary": {**summary, "pickup_location_missing_fk": missing_pickup,
                         "dropoff_location_missing_fk": missing_dropoff},
        "per_month_counts": month_counts,
        "load_log": [
            {**row,
             "started_at": row["started_at"].isoformat() if row["started_at"] else None,
             "completed_at": row["completed_at"].isoformat() if row["completed_at"] else None,
             "elapsed_seconds": float(row["elapsed_seconds"] or 0)}
            for row in logs
        ],
        "gold_batch_lineage": {
            entry["dataset_name"]: entry["gold_batch_id"] for entry in gold_entries
        },
        "indexes": list_indexes(connection),
        "view_trip_enriched_present": view_present,
        "table_size_bytes": size,
        "sanity_queries": sanity,
    }


def save_validation_report(path: str | Path, report: Mapping[str, Any]) -> None:
    """Atomically write the compact, secret-free JSON report."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as output:
            json.dump(report, output, indent=2, ensure_ascii=False)
            output.write("\n")
        os.replace(temporary, destination)
    finally:
        Path(temporary).unlink(missing_ok=True)
