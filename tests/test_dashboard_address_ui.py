"""Streamlit AppTest for the opt-in flow; injected API, geometry and predictor."""

from copy import deepcopy
from datetime import date
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
    router.directions.return_value = {"distance_meters": 1234.56789, "geometry": {
        "type": "LineString", "coordinates": [[-73.98, 40.75], [-73.95, 40.75]],
    }}
    monkeypatch.setattr(address_page, "route_map", Mock())
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
    from nyc_taxi.dashboard import address_page
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
    rendered_args = deepcopy(address_page.route_map.call_args.args)
    rendered_kwargs = deepcopy(address_page.route_map.call_args.kwargs)
    assert rendered_args[0] == snapshot["route_geometry"]
    assert rendered_kwargs["submission_id"] == snapshot["submission_id"]
    for slot in ("pickup", "dropoff"):
        assert rendered_kwargs[slot] == [snapshot[slot]["longitude"], snapshot[slot]["latitude"]]
    assert rendered_kwargs["key"] == "address_submitted_route_map"
    assert rendered_kwargs["online"] is False and rendered_kwargs["access_token"] == ""
    app.session_state["address_pickup"]["selected"]["label"] = "Edited draft"
    app.session_state["address_pickup"]["selected"]["longitude"] = -73.90
    app.run(timeout=10)
    assert not app.exception and app.session_state["address_fare_result"] == snapshot
    assert "Edited draft" not in " ".join(row.value for row in app.caption)
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()
    assert address_page.route_map.call_args.args == rendered_args
    assert address_page.route_map.call_args.kwargs == rendered_kwargs
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
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.mapbox_client import MapboxError
    app, router, predictor = ui
    app.run(timeout=10)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert app.metric[0].value == "$12.34"
    rendered_count = address_page.route_map.call_count
    router.directions.side_effect = MapboxError("SYNTHETIC_SECRET")
    app.button[0].click().run(timeout=10)
    assert not app.exception and app.error and not app.metric
    assert "SYNTHETIC_SECRET" not in app.error[0].value
    assert predictor.predict.call_count == 1
    assert address_page.route_map.call_count == rendered_count


def test_online_map_uses_runtime_public_token_and_saved_route_not_draft(ui, monkeypatch):
    from nyc_taxi.dashboard import address_page
    app, router, predictor = ui
    monkeypatch.setenv("FARE_ADDRESS_ALLOW_NETWORK", "1")
    monkeypatch.setenv("MAPBOX_ACCESS_TOKEN", "pk.synthetic-offline-only")
    app.run(timeout=10)
    address_page.route_map.assert_not_called()
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception
    saved = deepcopy(app.session_state["address_fare_result"])
    rendered_args = deepcopy(address_page.route_map.call_args.args)
    rendered_kwargs = deepcopy(address_page.route_map.call_args.kwargs)
    assert rendered_args == (saved["route_geometry"],)
    assert rendered_kwargs["online"] is True
    assert rendered_kwargs["access_token"] == "pk.synthetic-offline-only"
    assert rendered_kwargs["submission_id"] == saved["submission_id"]
    for slot in ("pickup", "dropoff"):
        assert rendered_kwargs[slot] == [saved[slot]["longitude"], saved[slot]["latitude"]]
    app.session_state["address_dropoff"]["selected"]["longitude"] = -73.85
    app.date_input[0].set_value(date(2025, 1, 1)).run(timeout=10)
    assert not app.exception and app.session_state["address_fare_result"] == saved
    assert address_page.route_map.call_args.args == rendered_args
    assert address_page.route_map.call_args.kwargs == rendered_kwargs
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()


def test_address_event_rerun_does_not_repeat_search_http(ui, monkeypatch):
    from uuid import uuid4
    from nyc_taxi.dashboard import address_page
    app, router, predictor = ui
    search = Mock()
    search.suggest.return_value = [{"mapbox_id": "synthetic-a", "label": "Synthetic A"}]
    monkeypatch.setattr(address_page, "make_search_client", lambda: search)
    state = app.session_state["address_pickup"]
    event = {"event_id": str(uuid4()), "session_token": state["session_token"],
             "kind": "suggest", "query": "Synthetic A"}
    monkeypatch.setattr(address_page, "address_picker",
                        lambda _state, *, label, key: event if key == "address_pickup_picker" else None)
    app.run(timeout=10)
    assert not app.exception
    app.run(timeout=10)
    assert not app.exception
    search.suggest.assert_called_once_with("Synthetic A", event["session_token"])
    search.retrieve.assert_not_called()
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


@pytest.mark.parametrize("online", [False, True])
def test_real_map_component_bridge_keeps_identity_and_submitted_args_on_rerun(ui, monkeypatch, online):
    import json
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.route_map import route_map
    app, router, predictor = ui
    monkeypatch.setattr(address_page, "route_map", route_map)
    monkeypatch.setenv("FARE_ADDRESS_ALLOW_NETWORK", "1" if online else "0")
    monkeypatch.setenv("MAPBOX_ACCESS_TOKEN", "pk.synthetic-offline-only")
    app.run(timeout=10)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception
    component = app.get("component_instance")[0].proto
    saved_id, saved_args = component.id, json.loads(component.json_args)
    assert saved_args["online"] is online
    if online:
        stored = app.session_state["address_fare_result"]
        assert saved_args["geometry"] == stored["route_geometry"]
        assert saved_args["submission_id"] == stored["submission_id"]
        assert saved_args["pickup"] == [stored["pickup"]["longitude"], stored["pickup"]["latitude"]]
        assert saved_args["dropoff"] == [stored["dropoff"]["longitude"], stored["dropoff"]["latitude"]]
    else:
        assert "geometry" not in saved_args and "access_token" not in saved_args
    app.session_state["address_pickup"]["selected"]["label"] = "Edited draft"
    app.session_state["address_pickup"]["selected"]["longitude"] = -73.8
    app.run(timeout=10)
    assert not app.exception
    rerun = app.get("component_instance")[0].proto
    assert rerun.id == saved_id and json.loads(rerun.json_args) == saved_args
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()


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
