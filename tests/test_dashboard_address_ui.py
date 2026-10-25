"""Streamlit AppTest for the opt-in flow; injected API, geometry and predictor."""

from copy import deepcopy
from unittest.mock import Mock

import pytest


@pytest.fixture
def ui(monkeypatch):
    # Missing Streamlit must be reported as a dependency gap, never a silent skip.
    from streamlit.testing.v1 import AppTest
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.address_component import new_address_state
    from nyc_taxi.dashboard.route_fare import RouteFareCoordinator
    from nyc_taxi.dashboard.prediction import MODEL_SHA256, TRAINING_RUN_ID, EVALUATION_RUN_ID

    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "1")
    monkeypatch.delenv("FARE_ADDRESS_ALLOW_NETWORK", raising=False)
    monkeypatch.setattr(address_page, "address_picker", lambda *_args, **_kw: None)
    router, geometry, predictor = Mock(), Mock(), Mock()
    router.directions.return_value = {"distance_meters": 1234.56789}
    geometry.resolve.side_effect = lambda lon, lat: {
        "location_id": 161 if lon < -73.97 else 237, "borough": "Manhattan", "zone": "Synthetic zone",
    }
    predictor.predict.return_value = {
        "fare_amount_usd": 12.34, "unit": "USD", "request_seconds": .1,
        "model_sha256": MODEL_SHA256, "training_run_id": TRAINING_RUN_ID,
        "evaluation_run_id": EVALUATION_RUN_ID,
        "unseen_categories": {"pickup": False, "dropoff": False}, "warnings": [],
    }
    coordinator = RouteFareCoordinator(router=router, geometry=geometry, predictor=predictor)
    monkeypatch.setattr(address_page, "make_coordinator", lambda: coordinator)

    def main():
        from nyc_taxi.dashboard.fare_page import render_page
        render_page()

    app = AppTest.from_function(main)
    for slot, lon in (("pickup", -73.98), ("dropoff", -73.95)):
        state = new_address_state()
        state["selected"] = {"label": "Synthetic " + slot, "longitude": lon, "latitude": 40.75}
        app.session_state["address_" + slot] = state
    yield app, router, predictor


def test_opt_in_ui_submits_once_and_keeps_submitted_snapshot_through_rerun(ui):
    app, router, predictor = ui
    app.run(timeout=10)
    assert not app.exception and len(app.number_input) == 0
    router.directions.assert_not_called()
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception and not app.error
    assert app.metric[0].value == "$12.34"
    assert "1.235 km" in " ".join(row.value for row in app.caption)
    snapshot = deepcopy(app.session_state["address_fare_result"])
    app.session_state["address_pickup"]["selected"]["label"] = "Edited draft"
    app.run(timeout=10)
    assert not app.exception and app.session_state["address_fare_result"] == snapshot
    assert "Edited draft" not in " ".join(row.value for row in app.caption)
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()
    assert predictor.predict.call_args.kwargs["distance_miles"] == 1234.56789 / 1609.344


def test_opt_in_ui_missing_selection_and_nonstandard_never_routes(ui):
    app, router, predictor = ui
    app.run(timeout=10)
    app.button[0].click().run(timeout=10)
    assert app.error and not app.metric
    app.session_state["address_pickup"]["selected"] = None
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert app.error and not app.metric
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_opt_in_ui_failed_second_submit_clears_previous_quote(ui):
    from nyc_taxi.dashboard.mapbox_client import MapboxError
    app, router, predictor = ui
    app.run(timeout=10)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert app.metric[0].value == "$12.34"
    router.directions.side_effect = MapboxError("SYNTHETIC_SECRET")
    app.button[0].click().run(timeout=10)
    assert not app.exception and app.error and not app.metric
    assert "SYNTHETIC_SECRET" not in app.error[0].value
    assert predictor.predict.call_count == 1


def test_offline_transport_is_disabled_even_with_token_present(monkeypatch):
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.address_component import AddressSelectionError
    monkeypatch.delenv("FARE_ADDRESS_ALLOW_NETWORK", raising=False)
    monkeypatch.setenv("MAPBOX_ACCESS_TOKEN", "synthetic-token-never-used")
    api = Mock(side_effect=AssertionError("No live client may be constructed"))
    monkeypatch.setattr(address_page, "MapboxClient", api)
    with pytest.raises(AddressSelectionError, match="offline"):
        address_page.make_search_client()
    api.assert_not_called()


def test_feature_flag_defaults_off_and_requires_explicit_one(monkeypatch):
    from nyc_taxi.dashboard.address_page import address_flow_enabled
    monkeypatch.delenv("FARE_ADDRESS_ENABLED", raising=False)
    assert address_flow_enabled() is False
    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "true")
    assert address_flow_enabled() is False
    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "1")
    assert address_flow_enabled() is True
