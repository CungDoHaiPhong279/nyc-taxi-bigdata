"""Streamlit AppTest checks using a tiny in-memory fixture, never the real DB."""

from pathlib import Path
from statistics import mean
from unittest.mock import Mock

import pytest
import pyarrow.ipc as ipc

st = pytest.importorskip("streamlit")
from streamlit.testing.v1 import AppTest

from nyc_taxi.dashboard import queries

APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
TRIPS = [
    # Unequal group sizes, independent NULLs, and zero/negative fares.
    ("2025-01", "Manhattan", 8, 10.0, 1.0),
    ("2025-01", "Manhattan", 8, 0.0, None),
    ("2025-01", "Manhattan", 8, None, 5.0),
    ("2025-01", "Queens", 9, -4.0, None),
    ("2025-02", "Manhattan", 9, None, 9.0),
    ("2025-02", "Bronx", 8, 20.0, None),
]


def fixture_snapshot(filters):
    rows = [r for r in TRIPS if (not filters.months or r[0] in filters.months)
            and (not filters.boroughs or r[1] in filters.boroughs)]
    fares = [r[3] for r in rows if r[3] is not None]
    distances = [r[4] for r in rows if r[4] is not None]
    result = {
        "total_trips": len(rows),
        "average_fare": mean(fares) if fares else None,
        "average_distance": mean(distances) if distances else None,
        "total_fare": sum(fares),
        "query_seconds": 0.01, "queried_at": "2026-10-20T00:00:00+00:00",
    }
    for name, field, index in [("by_month", "source_month", 0), ("by_hour", "pickup_hour", 2),
                               ("by_borough", "pickup_borough", 1)]:
        result[name] = [{field: value, "trips": sum(r[index] == value for r in rows)}
                        for value in sorted({r[index] for r in rows})]
    return result


def assert_chart_data(app, filters=queries.DashboardFilters()):
    expected = fixture_snapshot(filters)
    charts = app.get("vega_lite_chart")
    assert len(charts) == 3
    for chart, name, field in zip(
        charts,
        ("by_month", "by_hour", "by_borough"),
        ("source_month", "pickup_hour", "pickup_borough"),
    ):
        table = ipc.open_stream(chart.proto.datasets[0].data.data).read_all()
        actual_bins = {row[field]: row["trips"] for row in table.to_pylist()}
        expected_bins = {row[field]: row["trips"] for row in expected[name]}
        assert actual_bins == expected_bins
        assert sum(actual_bins.values()) == expected["total_trips"]


@pytest.fixture(autouse=True)
def clear_cache():
    st.cache_data.clear()
    yield
    st.cache_data.clear()


@pytest.fixture
def reader(monkeypatch):
    reader = Mock(side_effect=fixture_snapshot)
    monkeypatch.setattr(queries, "fetch_dashboard", reader)
    return reader


def test_ui_form_filters_and_cache_include_both_dimensions(reader):
    app = AppTest.from_file(str(APP)).run(timeout=10)
    assert not app.exception
    assert app.title[0].value == "NYC Taxi Analytics"
    assert app.metric[0].value == "6"
    assert app.metric[1].value == "$6.50"
    assert app.metric[2].value == "5.00 mi"
    assert app.metric[3].value == "$26.00"
    assert_chart_data(app)
    assert reader.call_count == 1  # baseline also supplies filter options

    # Form values are sent together with submit, just as in the browser.
    app.multiselect(key="selected_months").set_value(["2025-01"])
    assert app.metric[0].value == "6"  # not applied until submit
    assert reader.call_count == 1
    app.button[0].click().run(timeout=10)
    assert app.metric[0].value == "4"
    assert app.metric[1].value == "$2.00"
    assert app.metric[2].value == "3.00 mi"
    assert app.metric[3].value == "$6.00"
    assert_chart_data(app, queries.DashboardFilters(("2025-01",), ()))
    assert reader.call_args.args[0] == queries.DashboardFilters(("2025-01",), ())

    app.multiselect(key="selected_boroughs").set_value(["Queens"])
    app.button[0].click().run(timeout=10)
    assert app.metric[0].value == "1"
    assert app.metric[1].value == "$-4.00"
    assert app.metric[2].value == "\u2014"
    assert app.metric[3].value == "$-4.00"
    assert reader.call_args.args[0] == queries.DashboardFilters(("2025-01",), ("Queens",))
    assert_chart_data(app, queries.DashboardFilters(("2025-01",), ("Queens",)))
    assert reader.call_count == 3

    app.multiselect(key="selected_boroughs").set_value([])
    app.button[0].click().run(timeout=10)
    assert app.metric[0].value == "4"
    assert reader.call_count == 3  # cached month-only result
    assert not app.exception


def test_null_fare_keeps_trip_distance_and_chart_bins(reader):
    app = AppTest.from_file(str(APP)).run(timeout=10)
    app.multiselect(key="selected_months").set_value(["2025-02"])
    app.multiselect(key="selected_boroughs").set_value(["Manhattan"])
    app.button[0].click().run(timeout=10)
    assert not app.exception
    assert app.metric[0].value == "1"
    assert app.metric[1].value == "\u2014"
    assert app.metric[2].value == "9.00 mi"
    assert app.metric[3].value == "$0.00"
    assert_chart_data(app, queries.DashboardFilters(("2025-02",), ("Manhattan",)))


def test_empty_results_have_zero_kpis_and_no_charts(reader):
    app = AppTest.from_file(str(APP)).run(timeout=10)
    app.multiselect(key="selected_months").set_value(["2025-01"])
    app.multiselect(key="selected_boroughs").set_value(["Bronx"])
    app.button[0].click().run(timeout=10)
    assert not app.exception
    assert app.metric[0].value == "0"
    assert app.metric[1].value == "—"
    assert app.metric[2].value == "—"
    assert "Không có chuyến đi" in app.info[0].value
    assert len(app.get("vega_lite_chart")) == 0


def test_database_error_is_visible_and_retry_recovers(monkeypatch):
    reader = Mock(side_effect=[
        queries.DashboardQueryError("Không thể đọc PostgreSQL. Hãy thử lại."),
        fixture_snapshot(queries.DashboardFilters()),
    ])
    monkeypatch.setattr(queries, "fetch_dashboard", reader)
    app = AppTest.from_file(str(APP)).run(timeout=10)
    assert not app.exception
    assert "Không thể đọc PostgreSQL" in app.error[0].value
    assert len(app.metric) == 0
    app.button(key="retry_connection").click().run(timeout=10)
    assert not app.exception and not app.error
    assert app.metric[0].value == "6"
    assert_chart_data(app)


def test_failed_filter_query_does_not_show_stale_results(reader):
    app = AppTest.from_file(str(APP)).run(timeout=10)
    reader.side_effect = queries.DashboardQueryError("Không thể đọc PostgreSQL. Hãy thử lại.")
    app.multiselect(key="selected_months").set_value(["2025-02"])
    app.button[0].click().run(timeout=10)
    assert not app.exception
    assert app.error
    assert len(app.metric) == 0
    assert len(app.get("vega_lite_chart")) == 0
