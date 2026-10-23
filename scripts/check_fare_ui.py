"""Real API + packaged Streamlit AppTest parity, not a browser render test.

Runs only in the optional web test image. No PostgreSQL access or model fitting.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import time


def main(argv=None):
    from streamlit.testing.v1 import AppTest
    from nyc_taxi.dashboard import queries

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    args = parser.parse_args(argv)
    raw = args.fixture.read_bytes()
    fixture = json.loads(raw)
    reference = json.loads(args.reference.read_text(encoding="utf-8"))
    if reference["fixture_sha256"] != hashlib.sha256(raw).hexdigest():
        raise ValueError("UI verification fixture differs from reference")
    expected = {row["id"]: row["fare_amount_usd"] for row in reference["predictions"]}
    query_calls = []

    def forbid_sql(*a, **kw):
        query_calls.append(True)
        raise AssertionError("Estimate page must not query PostgreSQL")

    queries.fetch_dashboard = forbid_sql
    cases = [fixture["cases"][index] for index in (0, 1, 4)]
    outcomes = []
    for case in cases:
        app = AppTest.from_file("/app/dashboard/app.py", default_timeout=60)
        app.session_state["dashboard_page"] = "Ước tính cước"
        app.run()
        if app.exception or app.error:
            raise ValueError("Packaged estimate page did not open")
        values = case["inputs"]
        pickup = datetime.fromisoformat(values["pickup_datetime"])
        app.number_input(key="fare_distance").set_value(values["distance_miles"])
        app.date_input(key="fare_pickup_date").set_value(pickup.date())
        app.time_input(key="fare_pickup_time").set_value(pickup.time())
        app.selectbox(key="fare_pickup_zone").set_value(values["pickup_location_id"])
        app.selectbox(key="fare_dropoff_zone").set_value(values["dropoff_location_id"])
        app.checkbox(key="fare_standard_rate").set_value(True)
        tick = time.perf_counter()
        app.button[0].click().run()
        elapsed = time.perf_counter() - tick
        if app.exception or app.error:
            raise ValueError("Packaged UI prediction failed")
        stored = app.session_state["fare_result"]
        if stored["request"] != values:
            raise ValueError("Zone/datetime UI mapping changed the submitted inputs")
        result = stored["result"]
        difference = abs(result["fare_amount_usd"] - expected[case["id"]])
        if difference > 1e-9 or result["unseen_categories"] != case["expected_unseen"]:
            raise ValueError("UI/service/original-model prediction parity failed")
        if app.metric[0].value != f"${expected[case['id']]:,.2f}":
            raise ValueError("Displayed USD differs from the raw model result")
        outcomes.append({"id": case["id"], "ui_fare_usd": result["fare_amount_usd"],
                         "displayed": app.metric[0].value, "absolute_difference": difference,
                         "submit_apptest_seconds": elapsed, "warnings": len(app.warning)})
    # A real refused connection, without stopping the real model service.
    os.environ["FARE_INFERENCE_URL"] = "http://127.0.0.1:9"
    unavailable = AppTest.from_file("/app/dashboard/app.py", default_timeout=15)
    unavailable.session_state["dashboard_page"] = "Ước tính cước"
    unavailable.run()
    if unavailable.exception or not unavailable.error or len(unavailable.metric):
        raise ValueError("Unavailable service must show an error, never a fallback fare")
    if query_calls:
        raise ValueError("Estimate page queried PostgreSQL")
    print("ML_P4_UI_CHECK " + json.dumps({"passed": True, "cases": outcomes,
          "fixture_sha256": hashlib.sha256(raw).hexdigest(), "sql_calls": 0,
          "real_service_unavailable_error": True, "browser_render_verified": False}, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
