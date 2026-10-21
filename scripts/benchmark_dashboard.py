"""Measure three real dashboard cache misses without writing files or data.

Run from the repository/image root. This script is deliberately ASCII-only so
PowerShell can pipe it into an existing dashboard image before or after a build.
AppTest wall time includes the Python test harness, not browser rendering.
PostgreSQL and operating-system caches are neither cleared nor controlled.
"""

from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal
from pathlib import Path
from time import perf_counter

import pyarrow.ipc as ipc
import streamlit as st
from sqlalchemy import event
from streamlit.testing.v1 import AppTest

from nyc_taxi.dashboard import queries


def main() -> None:
    statement_counts: Counter[str] = Counter()
    fetches: list[tuple[queries.DashboardFilters, dict]] = []
    original_engine = queries.get_engine
    original_fetch = queries.fetch_dashboard

    def count_statement(connection, cursor, statement, parameters, context, executemany):
        # Record only safe categories, never SQL, parameters, or connection data.
        normalized = statement.casefold()
        if "analytics.fact_trip" in normalized:
            category = "fact_aggregate"
        elif "set transaction" in normalized:
            category = "transaction_setup"
        elif "statement_timeout" in normalized:
            category = "statement_timeout"
        elif "lock_timeout" in normalized:
            category = "lock_timeout"
        else:
            category = "connection_or_other"
        statement_counts[category] += 1

    def instrumented_engine(*args, **kwargs):
        engine = original_engine(*args, **kwargs)
        event.listen(engine, "before_cursor_execute", count_statement)
        return engine

    def instrumented_fetch(filters, **kwargs):
        payload = original_fetch(filters, **kwargs)
        fetches.append((filters, payload))
        return payload

    def assert_rendered(app, payload):
        assert not app.exception, "AppTest reported an application exception"
        assert not app.error, "Dashboard reported a database error"
        rendered = [metric.value for metric in app.metric]
        expected = [
            f"{payload['total_trips']:,}",
            "\u2014" if payload["average_fare"] is None
            else f"${payload['average_fare']:,.2f}",
            "\u2014" if payload["average_distance"] is None
            else f"{payload['average_distance']:,.2f} mi",
            f"${payload['total_fare']:,.2f}",
        ]
        assert rendered == expected, "Rendered KPI strings differ from SQL payload"
        charts = app.get("vega_lite_chart")
        assert len(charts) == 3, "Expected all three charts"
        for chart, name, field in zip(
            charts,
            ("by_month", "by_hour", "by_borough"),
            ("source_month", "pickup_hour", "pickup_borough"),
        ):
            table = ipc.open_stream(chart.proto.datasets[0].data.data).read_all()
            actual = [
                {field: row[field], "trips": row["trips"]}
                for row in table.to_pylist()
            ]
            expected_rows = [
                {field: row[field], "trips": row["trips"]}
                for row in payload[name]
            ]
            sorter = lambda row: str(row[field])
            assert sorted(actual, key=sorter) == sorted(expected_rows, key=sorter), (
                f"Rendered {name} bins differ from SQL payload"
            )
            assert sum(row["trips"] for row in actual) == payload["total_trips"]
        return rendered

    def json_number(value):
        if isinstance(value, Decimal):
            return float(value)
        raise TypeError(f"Unsupported JSON value: {type(value).__name__}")

    queries.get_engine = instrumented_engine
    queries.fetch_dashboard = instrumented_fetch
    st.cache_data.clear()
    app = AppTest.from_file(str(Path("dashboard/app.py").resolve()), default_timeout=240)
    cases = (
        ("all", (), (), 23_886_265),
        ("january", ("2025-01",), (), 3_473_175),
        ("january_manhattan", ("2025-01",), ("Manhattan",), 3_087_471),
    )
    payloads = {}
    try:
        for index, (name, months, boroughs, expected_trips) in enumerate(cases):
            if index:
                app.multiselect(key="selected_months").set_value(list(months))
                app.multiselect(key="selected_boroughs").set_value(list(boroughs))
            count_before = statement_counts.copy()
            fetch_before = len(fetches)
            started = perf_counter()
            if index:
                app.button[0].click().run(timeout=240)
            else:
                app.run(timeout=240)
            ui_seconds = perf_counter() - started
            assert len(fetches) == fetch_before + 1, "Expected exactly one result-cache miss"
            filters, payload = fetches[-1]
            assert filters == queries.DashboardFilters(months, boroughs)
            assert payload["total_trips"] == expected_trips, "Trip checkpoint differs"
            categories = statement_counts - count_before
            assert categories["fact_aggregate"] == 1, "Expected exactly one fact aggregate"
            rendered = assert_rendered(app, payload)
            payloads[name] = payload
            result = {
                "case": name,
                "payload": payload,
                "sql_seconds": payload["query_seconds"],
                "ui_seconds": ui_seconds,
                "sql_counters": {
                    "total": sum(categories.values()),
                    "aggregate": categories["fact_aggregate"],
                    "fetch_calls": len(fetches) - fetch_before,
                },
                "statement_categories": dict(sorted(categories.items())),
                "rendered_kpis": rendered,
            }
            print("P41_RESULT " + json.dumps(result, default=json_number, ensure_ascii=True), flush=True)

        # Revisit an already measured key within the same session and finite TTL.
        app.multiselect(key="selected_months").set_value(["2025-01"])
        app.multiselect(key="selected_boroughs").set_value([])
        count_before = statement_counts.copy()
        fetch_before = len(fetches)
        started = perf_counter()
        app.button[0].click().run(timeout=240)
        elapsed = perf_counter() - started
        assert len(fetches) == fetch_before, "January revisit missed application cache"
        assert statement_counts == count_before, "Cache hit issued SQL"
        assert_rendered(app, payloads["january"])
        print("P41_CACHE_HIT " + json.dumps({
            "case": "january", "ui_seconds": elapsed, "new_fetch_calls": 0,
            "new_sql_statements": 0,
        }), flush=True)
    finally:
        queries.get_engine = original_engine
        queries.fetch_dashboard = original_fetch


if __name__ == "__main__":
    main()
