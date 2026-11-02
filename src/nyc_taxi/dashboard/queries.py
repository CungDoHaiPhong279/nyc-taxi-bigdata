"""Bounded PostgreSQL aggregates shared by the dashboard and its tests."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Iterable, Mapping

from sqlalchemy import bindparam, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.elements import TextClause

from nyc_taxi.database.postgres import get_engine

CONNECT_TIMEOUT_SECONDS = 5
STATEMENT_TIMEOUT_MS = 180_000
LOCK_TIMEOUT_MS = 5_000


class DashboardQueryError(RuntimeError):
    """Safe, user-facing error without connection strings or server details."""


@dataclass(frozen=True)
class DashboardFilters:
    """Empty selections mean all; canonical tuples also form stable cache keys."""

    months: tuple[str, ...] = ()
    boroughs: tuple[str, ...] = ()

    @classmethod
    def from_selection(
        cls, months: Iterable[str] = (), boroughs: Iterable[str] = ()
    ) -> DashboardFilters:
        selected_months = tuple(sorted(set(months)))
        if any(
            re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", month) is None
            or int(month[:4]) == 0
            for month in selected_months
        ):
            raise ValueError("Source months must use YYYY-MM.")
        return cls(selected_months, tuple(sorted(set(boroughs))))


def build_aggregate_query(filters: DashboardFilters) -> tuple[TextClause, dict[str, Any]]:
    """Reduce facts before the dimension join and shared KPI/chart rollups."""
    predicates: list[str] = []
    parameters: dict[str, Any] = {}
    if filters.months:
        month_predicates = []
        for index, month in enumerate(filters.months):
            month_predicates.append(
                f"(f.source_year = :year_{index} AND f.source_month = :month_{index})"
            )
            parameters[f"year_{index}"] = int(month[:4])
            parameters[f"month_{index}"] = int(month[5:])
        predicates.append("(" + " OR ".join(month_predicates) + ")")
    month_where = "WHERE " + " AND ".join(predicates) if predicates else ""
    borough_where = ""
    if filters.boroughs:
        borough_where = "WHERE pu.borough IN :boroughs"
        parameters["boroughs"] = filters.boroughs

    # Ordinary GROUP BY permits partial/final parallel aggregation of the fact.
    # Join only the small location/hour/month buckets, not every trip. Borough
    # filtering is safe after this reduction because location_id is a bucket key.
    # Keep independent non-NULL counts: averaging bucket averages is incorrect.
    # GROUPING masks remain total=15, source month=3, pickup hour=13, borough=14.
    query = text(
        f"""
        WITH buckets AS (
            SELECT
                f.source_year, f.source_month, f.pickup_hour, f.pickup_location_id,
                COUNT(*) AS trip_count,
                SUM(f.fare_amount) AS fare_sum,
                COUNT(f.fare_amount) AS fare_count,
                SUM(f.trip_distance) AS distance_sum,
                COUNT(f.trip_distance) AS distance_count
            FROM analytics.fact_trip AS f
            {month_where}
            GROUP BY f.source_year, f.source_month, f.pickup_hour, f.pickup_location_id
        )
        SELECT
            CASE GROUPING(b.source_year, b.source_month, b.pickup_hour, pu.borough)
                WHEN 15 THEN 'total'
                WHEN 3 THEN 'month'
                WHEN 13 THEN 'hour'
                WHEN 14 THEN 'borough'
            END AS group_kind,
            b.source_year, b.source_month, b.pickup_hour,
            pu.borough AS pickup_borough,
            COALESCE(SUM(b.trip_count), 0) AS total_trips,
            SUM(b.fare_sum) / NULLIF(SUM(b.fare_count), 0)::double precision AS average_fare,
            SUM(b.distance_sum) / NULLIF(SUM(b.distance_count), 0)::double precision AS average_distance,
            COALESCE(SUM(b.fare_sum), 0) AS total_fare
        FROM buckets AS b
        JOIN analytics.dim_location AS pu ON pu.location_id = b.pickup_location_id
        {borough_where}
        GROUP BY GROUPING SETS (
            (), (b.source_year, b.source_month), (b.pickup_hour), (pu.borough)
        )
        """
    )
    if filters.boroughs:
        query = query.bindparams(bindparam("boroughs", expanding=True))
    return query, parameters


def summarize_rows(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Split the small aggregate result without fetching individual trips."""
    result: dict[str, Any] = {
        "total_trips": 0,
        "average_fare": None,
        "average_distance": None,
        "total_fare": 0.0,
        "by_month": [],
        "by_hour": [],
        "by_borough": [],
    }
    for row in rows:
        kind = row["group_kind"]
        trips = int(row["total_trips"])
        if kind == "total":
            result.update(
                total_trips=trips,
                average_fare=row["average_fare"],
                average_distance=row["average_distance"],
                total_fare=float(row["total_fare"]),
            )
        elif kind == "month":
            result["by_month"].append(
                {"source_month": f"{row['source_year']:04d}-{row['source_month']:02d}", "trips": trips}
            )
        elif kind == "hour":
            result["by_hour"].append({"pickup_hour": row["pickup_hour"], "trips": trips})
        elif kind == "borough":
            result["by_borough"].append({"pickup_borough": row["pickup_borough"], "trips": trips})
    result["by_month"].sort(key=lambda row: row["source_month"])
    result["by_hour"].sort(key=lambda row: row["pickup_hour"])
    result["by_borough"].sort(key=lambda row: row["pickup_borough"])
    return result


def fetch_dashboard(
    filters: DashboardFilters, *, config: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Read a consistent snapshot; timeout settings apply only to this transaction."""
    query, parameters = build_aggregate_query(filters)
    engine = None
    try:
        engine = get_engine(config, connect_timeout_seconds=CONNECT_TIMEOUT_SECONDS)
        with engine.connect() as connection:
            with connection.begin():
                connection.exec_driver_sql(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                connection.execute(
                    text("SELECT set_config('statement_timeout', :timeout, true)"),
                    {"timeout": str(STATEMENT_TIMEOUT_MS)},
                )
                connection.execute(
                    text("SELECT set_config('lock_timeout', :timeout, true)"),
                    {"timeout": str(LOCK_TIMEOUT_MS)},
                )
                started = perf_counter()
                rows = connection.execute(query, parameters).mappings().all()
                elapsed = perf_counter() - started
        result = summarize_rows(rows)
        result["query_seconds"] = elapsed
        result["queried_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        return result
    except (SQLAlchemyError, OSError, ValueError, KeyError) as exc:
        if getattr(getattr(exc, "orig", None), "sqlstate", None) == "57014":
            raise DashboardQueryError(
                "Truy vấn vượt giới hạn 180 giây. Hãy chọn ít tháng hoặc borough hơn rồi thử lại."
            ) from None
        raise DashboardQueryError(
            "Không thể đọc PostgreSQL. Hãy kiểm tra service, cấu hình kết nối và quyền SELECT rồi thử lại."
        ) from None
    finally:
        if engine is not None:
            engine.dispose()



def build_multisource_query(filters: DashboardFilters) -> tuple[TextClause, dict[str, Any]]:
    """Build a bounded aggregate query joining fact_trip with dim_weather and dim_calendar."""
    predicates: list[str] = []
    parameters: dict[str, Any] = {}
    if filters.months:
        month_predicates = []
        for index, month in enumerate(filters.months):
            month_predicates.append(
                f"(f.source_year = :year_{index} AND f.source_month = :month_{index})"
            )
            parameters[f"year_{index}"] = int(month[:4])
            parameters[f"month_{index}"] = int(month[5:])
        predicates.append("(" + " OR ".join(month_predicates) + ")")
    if filters.boroughs:
        predicates.append("pu.borough IN :boroughs")
        parameters["boroughs"] = filters.boroughs
    where_clause = "WHERE " + " AND ".join(predicates) if predicates else ""

    query = text(
        f"""
        WITH filtered AS (
            SELECT
                f.fare_amount,
                f.trip_distance * 1.60934 AS distance_km,
                CASE
                    WHEN COALESCE(w.precipitation, 0) >= 2.0 OR COALESCE(w.snowfall, 0) > 0
                        THEN '3. Mưa lớn / Có tuyết (>= 2 mm/h)'
                    WHEN COALESCE(w.precipitation, 0) > 0
                        THEN '2. Mưa nhỏ & Mưa vừa (< 2 mm/h)'
                    ELSE '1. Trời quang / Khô ráo (0 mm/h)'
                END AS weather_bucket,
                CASE
                    WHEN COALESCE(c.is_holiday, false)
                        THEN '3. Ngày nghỉ lễ Liên bang (Holiday)'
                    WHEN COALESCE(c.is_weekend, false)
                        THEN '2. Cuối tuần (Thứ 7 & Chủ Nhật)'
                    ELSE '1. Ngày làm việc (Thứ 2 – Thứ 6)'
                END AS calendar_bucket
            FROM analytics.fact_trip AS f
            JOIN analytics.dim_location AS pu ON pu.location_id = f.pickup_location_id
            LEFT JOIN analytics.dim_weather AS w ON w.weather_id = f.weather_id
            LEFT JOIN analytics.dim_calendar AS c ON c.date_id = f.pickup_date_id
            {where_clause}
        )
        SELECT
            CASE GROUPING(weather_bucket, calendar_bucket)
                WHEN 1 THEN 'weather'
                WHEN 2 THEN 'calendar'
            END AS section_kind,
            COALESCE(weather_bucket, calendar_bucket) AS bucket_label,
            COUNT(*) AS trip_count,
            AVG(fare_amount)::double precision AS avg_fare_usd,
            AVG(distance_km)::double precision AS avg_distance_km
        FROM filtered
        GROUP BY GROUPING SETS ((weather_bucket), (calendar_bucket))
        ORDER BY section_kind, bucket_label
        """
    )
    if filters.boroughs:
        query = query.bindparams(bindparam("boroughs", expanding=True))
    return query, parameters


def fetch_multisource_summary(
    filters: DashboardFilters, *, config: dict[str, Any] | None = None
) -> dict[str, list[dict[str, Any]]]:
    """Query dynamic Weather & Calendar breakdown for Section 4 of the historical dashboard."""
    query, parameters = build_multisource_query(filters)
    engine = None
    try:
        engine = get_engine(config, connect_timeout_seconds=CONNECT_TIMEOUT_SECONDS)
        with engine.connect() as connection:
            with connection.begin():
                connection.exec_driver_sql(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
                )
                connection.execute(
                    text("SELECT set_config('statement_timeout', :timeout, true)"),
                    {"timeout": str(STATEMENT_TIMEOUT_MS)},
                )
                connection.execute(
                    text("SELECT set_config('lock_timeout', :timeout, true)"),
                    {"timeout": str(LOCK_TIMEOUT_MS)},
                )
                rows = connection.execute(query, parameters).mappings().all()

        weather_rows = [r for r in rows if r["section_kind"] == "weather"]
        calendar_rows = [r for r in rows if r["section_kind"] == "calendar"]
        w_total = sum(int(r["trip_count"]) for r in weather_rows) or 1
        c_total = sum(int(r["trip_count"]) for r in calendar_rows) or 1

        return {
            "weather": [
                {
                    "Điều kiện Thời tiết (ERA5)": str(r["bucket_label"]),
                    "Tỷ trọng chuyến (%)": f"{100.0 * int(r['trip_count']) / w_total:.1f}%",
                    "Giá cước TB (USD)": round(float(r["avg_fare_usd"] or 0.0), 2),
                    "Khoảng cách TB (km)": round(float(r["avg_distance_km"] or 0.0), 2),
                }
                for r in weather_rows
            ],
            "calendar": [
                {
                    "Nhóm Ngày (US Calendar)": str(r["bucket_label"]),
                    "Tỷ trọng chuyến (%)": f"{100.0 * int(r['trip_count']) / c_total:.1f}%",
                    "Giá cước TB (USD)": round(float(r["avg_fare_usd"] or 0.0), 2),
                    "Khoảng cách TB (km)": round(float(r["avg_distance_km"] or 0.0), 2),
                }
                for r in calendar_rows
            ],
        }
    except (SQLAlchemyError, OSError, ValueError, KeyError) as exc:
        raise DashboardQueryError("Không thể đọc bảng đa nguồn từ PostgreSQL.") from exc
    finally:
        if engine is not None:
            engine.dispose()
