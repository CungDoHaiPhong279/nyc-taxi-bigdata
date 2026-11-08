"""Offline tests for filters, bounded SQL, transaction safety and safe errors."""

import math
import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy import text

from nyc_taxi.dashboard import queries


def test_filter_keys_are_canonical_and_preserve_year_month_pairs():
    filters = queries.DashboardFilters.from_selection(
        ["2026-02", "2025-01", "2025-01"], ["Queens", "Manhattan", "Queens"]
    )
    assert filters.months == ("2025-01", "2026-02")
    assert filters.boroughs == ("Manhattan", "Queens")
    query, parameters = queries.build_aggregate_query(filters)
    assert parameters == {
        "year_0": 2025, "month_0": 1, "year_1": 2026, "month_1": 2,
        "boroughs": ("Manhattan", "Queens"),
    }
    sql = str(query)
    assert "f.source_year = :year_0 AND f.source_month = :month_0" in sql
    assert "f.source_year = :year_1 AND f.source_month = :month_1" in sql


@pytest.mark.parametrize("month", ["2025-00", "2025-13", "2025-1", "0000-01", "2025-01'; SELECT 1"])
def test_invalid_months_are_rejected(month):
    with pytest.raises(ValueError, match="YYYY-MM"):
        queries.DashboardFilters.from_selection([month])


def test_all_groupings_share_bound_filters_and_averages_skip_invalid_records():
    borough = "Queens'); DROP TABLE analytics.fact_trip; --"
    query, parameters = queries.build_aggregate_query(
        queries.DashboardFilters.from_selection(["2025-01"], [borough])
    )
    sql = str(query)
    assert borough not in sql
    assert parameters["boroughs"] == (borough,)
    assert "analytics.fact_trip" not in sql
    assert sql.count("FROM analytics.mv_kpi_buckets AS f") == 1
    assert sql.replace("FILTER (WHERE ", "").count("WHERE ") == 2  # month before buckets; borough after small join
    assert "GROUP BY GROUPING SETS" in sql
    assert "SELECT *" not in sql
    assert "etl_load_log" not in sql
    assert "AVG(" not in sql  # never average bucket averages
    assert "SUM(b.valid_fare_sum) / NULLIF(SUM(b.valid_fare_count), 0)" in sql
    assert "SUM(b.valid_distance_sum) / NULLIF(SUM(b.valid_distance_count), 0)" in sql
    assert "COALESCE(SUM(b.fare_sum), 0) AS total_fare" in sql  # totals keep every row
    all_query, all_parameters = queries.build_aggregate_query(queries.DashboardFilters())
    assert "WHERE " not in str(all_query).replace("FILTER (WHERE ", "")
    assert all_parameters == {}


def _kpi_view_sql():
    summary = (Path(__file__).resolve().parents[1] / "sql" / "postload" / "create_aggregates.sql").read_text(encoding="utf-8")
    start = summary.index("CREATE MATERIALIZED VIEW analytics.mv_kpi_buckets AS")
    return summary[start:summary.index(";", start)]


def test_kpi_view_keeps_every_row_and_filters_only_averages():
    block = _kpi_view_sql()
    assert "FROM analytics.fact_trip AS f" in block
    assert "WHERE" not in block.replace("FILTER (WHERE ", "")
    assert "HAVING" not in block and "JOIN" not in block
    assert "SUM(f.fare_amount) AS fare_sum" in block
    assert f"FILTER (WHERE {queries.VALID_FARE_SQL}) AS valid_fare_sum" in block
    assert f"FILTER (WHERE {queries.VALID_DISTANCE_SQL}) AS valid_distance_sum" in block
    assert "GROUP BY f.source_year, f.source_month, f.pickup_hour, f.pickup_location_id" in block


def test_fact_is_reduced_before_join_and_borough_filter():
    query, _ = queries.build_aggregate_query(
        queries.DashboardFilters.from_selection(["2025-01"], ["Manhattan"])
    )
    sql = query.text
    assert sql.count("JOIN analytics.dim_location") == 1
    bucket_end = sql.index("FROM buckets AS b")
    assert "JOIN" not in sql[:bucket_end]
    assert "f.pickup_location_id" in sql[:bucket_end]
    assert sql.index("f.source_year = :year_0") < bucket_end
    assert sql.index("WHERE pu.borough IN :boroughs") > bucket_end


def test_small_result_is_split_sorted_and_keeps_zero_negative_averages():
    result = queries.summarize_rows([
        {"group_kind": "hour", "pickup_hour": 9, "total_trips": 1},
        {"group_kind": "hour", "pickup_hour": 8, "total_trips": 1},
        {"group_kind": "month", "source_year": 2025, "source_month": 1, "total_trips": 2},
        {"group_kind": "borough", "pickup_borough": "Queens", "total_trips": 2},
        {"group_kind": "total", "total_trips": 2, "average_fare": -2.5,
         "average_distance": 0.0, "total_fare": -5.0},
    ])
    assert result["average_fare"] == -2.5
    assert result["average_distance"] == 0.0
    assert result["total_fare"] == -5.0
    assert [row["pickup_hour"] for row in result["by_hour"]] == [8, 9]
    for chart in ("by_month", "by_hour", "by_borough"):
        assert sum(row["trips"] for row in result[chart]) == result["total_trips"]


def fake_engine(monkeypatch):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    factory = MagicMock(return_value=engine)
    monkeypatch.setattr(queries, "get_engine", factory)
    return engine, connection, factory


def test_readonly_timeout_and_disposal_are_enforced_before_aggregate(monkeypatch):
    engine, connection, factory = fake_engine(monkeypatch)
    connection.execute.return_value.mappings.return_value.all.return_value = [
        {"group_kind": "total", "total_trips": 0, "average_fare": None,
         "average_distance": None, "total_fare": 0}
    ]
    result = queries.fetch_dashboard(queries.DashboardFilters())
    factory.assert_called_once_with(None, connect_timeout_seconds=5)
    connection.exec_driver_sql.assert_called_once_with(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    )
    calls = connection.execute.call_args_list
    assert "statement_timeout" in str(calls[0].args[0])
    assert calls[0].args[1] == {"timeout": "180000"}
    assert "lock_timeout" in str(calls[1].args[0])
    assert calls[1].args[1] == {"timeout": "5000"}
    assert "FROM analytics.mv_kpi_buckets" in str(calls[2].args[0])
    assert len(calls) == 3
    assert result["total_trips"] == 0 and result["average_fare"] is None
    assert result["by_month"] == []
    assert result["query_seconds"] >= 0
    engine.dispose.assert_called_once()


def test_database_failure_is_sanitized_and_connection_is_closed(monkeypatch):
    engine, connection, _ = fake_engine(monkeypatch)
    connection.execute.side_effect = OperationalError(
        "sensitive SQL", None, Exception("postgresql://user:secret@private-host/db")
    )
    with pytest.raises(queries.DashboardQueryError) as caught:
        queries.fetch_dashboard(queries.DashboardFilters())
    assert "Không thể đọc PostgreSQL" in str(caught.value)
    assert "secret" not in str(caught.value) and "private-host" not in str(caught.value)
    connection.begin.return_value.__exit__.assert_called_once()
    engine.connect.return_value.__exit__.assert_called_once()
    engine.dispose.assert_called_once()


def test_statement_timeout_has_actionable_safe_message(monkeypatch):
    class QueryCanceled(Exception):
        sqlstate = "57014"

    engine, connection, _ = fake_engine(monkeypatch)
    connection.execute.side_effect = OperationalError("", None, QueryCanceled("private"))
    with pytest.raises(queries.DashboardQueryError, match="180 giây"):
        queries.fetch_dashboard(queries.DashboardFilters())
    engine.dispose.assert_called_once()


def test_missing_kpi_summary_tells_how_to_rebuild(monkeypatch):
    class UndefinedTable(Exception):
        sqlstate = "42P01"

    engine, connection, _ = fake_engine(monkeypatch)
    connection.execute.side_effect = ProgrammingError("", None, UndefinedTable("private"))
    with pytest.raises(queries.DashboardQueryError, match="--create-indexes"):
        queries.fetch_dashboard(queries.DashboardFilters())
    engine.dispose.assert_called_once()


@pytest.mark.skipif(
    os.environ.get("NYC_TAXI_DASHBOARD_SQL_TEST") != "1",
    reason="Opt-in PostgreSQL READ ONLY test using only VALUES fixtures",
)
def test_postgres_readonly_weighted_aggregates_against_row_reference():
    """Execute the real query over VALUES, without creating or reading fact tables."""
    trips = [
        (2025, 1, 8, 1, 10.0, 1.0),
        (2025, 1, 8, 1, 0.0, None),
        (2025, 1, 8, 1, None, 5.0),
        (2025, 1, 9, 2, -4.0, None),
        (2025, 2, 9, 1, None, 9.0),
        (2025, 2, 8, 3, 20.0, None),
        (2025, 3, 10, 4, None, None),
        (2026, 2, 8, 2, 100.0, 1.0),
        (2025, 1, 8, 1, 600.0, 386088.0),
    ]
    borough_by_location = {1: "Manhattan", 2: "Queens", 3: "Bronx", 4: "Staten Island"}
    kpi_select = _kpi_view_sql().split(" AS", 1)[1].replace("analytics.fact_trip", "fact_fixture")
    fixtures = f"""WITH fact_fixture (
        source_year, source_month, pickup_hour, pickup_location_id, fare_amount, trip_distance
    ) AS (VALUES
        (2025,1,8,1,10.0::double precision,1.0::double precision),
        (2025,1,8,1,0.0,NULL), (2025,1,8,1,NULL,5.0),
        (2025,1,9,2,-4.0,NULL), (2025,2,9,1,NULL,9.0),
        (2025,2,8,3,20.0,NULL), (2025,3,10,4,NULL,NULL),
        (2026,2,8,2,100.0,1.0), (2025,1,8,1,600.0,386088.0)
    ), location_fixture (location_id, borough) AS (VALUES
        (1,'Manhattan'), (2,'Queens'), (3,'Bronx'), (4,'Staten Island')
    ), kpi_fixture AS ({kpi_select}), """
    cases = [
        queries.DashboardFilters(),
        queries.DashboardFilters(("2025-01",), ()),
        queries.DashboardFilters(("2025-01",), ("Manhattan",)),
        queries.DashboardFilters(("2025-02",), ("Manhattan",)),
        queries.DashboardFilters(("2025-03",), ("Staten Island",)),
        queries.DashboardFilters(("2000-01",), ()),
        queries.DashboardFilters(("2025-01", "2026-02"), ("Queens",)),
    ]
    engine = queries.get_engine(connect_timeout_seconds=5)
    try:
        with engine.connect() as connection:
            with connection.begin():
                connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                connection.exec_driver_sql("SET LOCAL statement_timeout='10s'")
                assert connection.exec_driver_sql("SHOW transaction_read_only").scalar_one() == "on"
                for filters in cases:
                    query, params = queries.build_aggregate_query(filters)
                    sql = query.text.lstrip()
                    assert sql.startswith("WITH buckets AS")
                    fixture_sql = fixtures + sql.removeprefix("WITH ")
                    fixture_sql = fixture_sql.replace("analytics.mv_kpi_buckets", "kpi_fixture")
                    fixture_sql = fixture_sql.replace("analytics.dim_location", "location_fixture")
                    fixture_query = text(fixture_sql)
                    if filters.boroughs:
                        from sqlalchemy import bindparam
                        fixture_query = fixture_query.bindparams(bindparam("boroughs", expanding=True))
                    actual = queries.summarize_rows(
                        connection.execute(fixture_query, params).mappings().all()
                    )
                    selected = [row for row in trips if
                                (not filters.months or f"{row[0]:04d}-{row[1]:02d}" in filters.months)
                                and (not filters.boroughs or borough_by_location[row[3]] in filters.boroughs)]
                    fares = [row[4] for row in selected if row[4] is not None]
                    valid_fares = [fare for fare in fares if 0 < fare <= 500]
                    distances = [row[5] for row in selected if row[5] is not None and 0 < row[5] <= 100]
                    assert actual["total_trips"] == len(selected)
                    expected_fare = math.fsum(valid_fares) / len(valid_fares) if valid_fares else None
                    expected_distance = math.fsum(distances) / len(distances) if distances else None
                    for key, expected in [("average_fare", expected_fare),
                                          ("average_distance", expected_distance)]:
                        if expected is None:
                            assert actual[key] is None
                        else:
                            assert math.isclose(actual[key], expected, rel_tol=1e-12, abs_tol=1e-12)
                    assert math.isclose(actual["total_fare"], math.fsum(fares), abs_tol=1e-12)
                    for name, field, key in [
                        ("by_month", "source_month", lambda row: f"{row[0]:04d}-{row[1]:02d}"),
                        ("by_hour", "pickup_hour", lambda row: row[2]),
                        ("by_borough", "pickup_borough", lambda row: borough_by_location[row[3]]),
                    ]:
                        expected_bins = {key(row): sum(key(item) == key(row) for item in selected)
                                         for row in selected}
                        assert {row[field]: row["trips"] for row in actual[name]} == expected_bins
    finally:
        engine.dispose()



def test_route_history_summary_covers_only_the_validated_months_and_cohort():
    summary = (Path(__file__).resolve().parents[1] / "sql" / "postload" / "create_aggregates.sql").read_text(encoding="utf-8")
    block = summary[summary.index("CREATE MATERIALIZED VIEW analytics.mv_route_hour_fare"):]
    assert "f.source_year = 2025 AND f.source_month <= 5" in block  # June stays the holdout
    assert "f.ratecode_id = 1" in block
    assert "NOT c.is_holiday" in block
    assert "HAVING COUNT(*) >= 30" in block


def test_multisource_query_uses_calendar_date_id_and_reduces_fact_first():
    query, params = queries.build_multisource_query(
        queries.DashboardFilters.from_selection(["2025-01"], ["Manhattan"])
    )
    sql = query.text
    assert "FROM analytics.mv_multisource_summary AS f" in sql
    assert "analytics.fact_trip" not in sql  # the full join took over 180 seconds
    assert "f.source_month = :month_0" in sql and "pu.borough IN" in sql
    assert params["year_0"] == 2025
    assert params["month_0"] == 1
    assert params["boroughs"] == ("Manhattan",)
    summary = (Path(__file__).resolve().parents[1] / "sql" / "postload" / "create_aggregates.sql").read_text(encoding="utf-8")
    assert f"FILTER (WHERE {queries.VALID_FARE_SQL}) AS fare_sum" in summary
    assert f"FILTER (WHERE {queries.VALID_DISTANCE_SQL}) * 1.60934" in summary
    assert "COALESCE(f.calendar_date_id, f.pickup_date)" in summary
