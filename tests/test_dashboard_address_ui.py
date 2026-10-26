"""Streamlit AppTest for the opt-in flow; injected API, geometry and predictor."""

from copy import deepcopy
from datetime import date, time
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
    monkeypatch.setattr(address_page, "get_geometry", lambda: geometry)

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
    assert "1.235 km" in " ".join(row.value for row in app.markdown)
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
    assert "Edited draft" not in " ".join(row.value for row in app.text)
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()
    assert address_page.route_map.call_args.args == rendered_args
    assert address_page.route_map.call_args.kwargs == rendered_kwargs
    assert predictor.predict.call_args.kwargs["distance_miles"] == 1234.56789 / 1609.344


@pytest.mark.parametrize(
    ("pickup_date", "pickup_time", "expected"),
    [
        (date(2025, 1, 15), time(10, 0), "2025-01-15T10:00:00"),
        (date(2025, 2, 20), time(14, 30), "2025-02-20T14:30:00"),
    ],
)
def test_submitted_date_time_reaches_coordinator_and_predictor_without_default_fallback(
    ui, monkeypatch, pickup_date, pickup_time, expected,
):
    from nyc_taxi.dashboard import address_page

    app, router, predictor = ui
    submit = Mock(wraps=address_page.execute_submission)
    monkeypatch.setattr(address_page, "execute_submission", submit)
    app.run(timeout=10)
    app.date_input[0].set_value(pickup_date)
    app.selectbox(key="address_pickup_hour").set_value(pickup_time.hour)
    app.selectbox(key="address_pickup_minute").set_value(pickup_time.minute)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)

    assert not app.exception and not app.error
    assert app.date_input[0].value == pickup_date
    assert len(app.time_input) == 0
    assert app.selectbox(key="address_pickup_hour").value == pickup_time.hour
    assert app.selectbox(key="address_pickup_minute").value == pickup_time.minute
    submit.assert_called_once()
    assert submit.call_args.args[2]["pickup_datetime"] == expected
    predictor.predict.assert_called_once()
    assert predictor.predict.call_args.kwargs["pickup_datetime"] == expected
    saved = deepcopy(app.session_state["address_fare_result"])
    assert saved["pickup_datetime"] == expected
    assert saved["request"]["pickup_datetime"] == expected
    assert expected + " NYC" in " ".join(item.value for item in app.caption)
    assert (f"{pickup_date:%d/%m/%Y} {pickup_time:%H:%M} — giờ New York"
            in " ".join(item.value for item in app.markdown))

    # Editing the draft and rerunning must not resubmit or relabel the quote.
    app.date_input[0].set_value(date(2025, 3, 1)).run(timeout=10)
    assert not app.exception and app.session_state["address_fare_result"] == saved
    submit.assert_called_once()
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()


def test_opt_in_ui_missing_selection_and_nonstandard_never_routes(ui):
    app, router, predictor = ui
    app.run(timeout=10)
    app.button[0].click().run(timeout=10)
    assert app.error and not app.metric
    app.session_state["address_pickup"]["selected"] = None
    app.run(timeout=10)
    assert app.button[0].disabled and not app.metric
    assert app.session_state["address_dropoff"]["selected"] is not None
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
             "kind": "suggest", "query": "Synthetic A", "version": 1}
    monkeypatch.setattr(address_page, "address_picker",
                        lambda _state, *, label, key, online: event if key == "address_pickup_picker" else None)
    app.run(timeout=10)
    assert not app.exception
    app.run(timeout=10)
    assert not app.exception
    search.suggest.assert_called_once_with("Synthetic A", event["session_token"])
    search.retrieve.assert_not_called()
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_edit_and_clear_without_http_keep_submitted_trip_snapshot(ui, monkeypatch):
    from uuid import uuid4
    from nyc_taxi.dashboard import address_page
    app, router, predictor = ui
    app.run(timeout=10)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    saved = deepcopy(app.session_state["address_fare_result"])
    search_factory = Mock(side_effect=AssertionError("Typing must not construct an HTTP client"))
    monkeypatch.setattr(address_page, "make_search_client", search_factory)
    state = app.session_state["address_pickup"]
    action = {"event_id": str(uuid4()), "session_token": state["session_token"],
              "kind": "edit", "query": "m", "version": 1}
    monkeypatch.setattr(address_page, "address_picker",
                        lambda _state, *, label, key, online: action if key == "address_pickup_picker" else None)
    app.run(timeout=10)
    assert not app.exception
    assert app.session_state["address_pickup"]["selected"] is None
    assert app.session_state["address_fare_result"] == saved
    action = {**action, "event_id": str(uuid4()), "query": "", "version": 2}
    app.run(timeout=10)
    assert not app.exception
    assert app.session_state["address_pickup"]["suggestions"] == []
    assert app.session_state["address_fare_result"] == saved
    search_factory.assert_not_called()
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()


def test_autocomplete_retrieve_selects_once_without_confirmation_or_routing(ui, monkeypatch):
    from uuid import uuid4
    from nyc_taxi.dashboard import address_page
    app, router, predictor = ui
    api = Mock()
    api.suggest.return_value = [{"mapbox_id": "synthetic-a", "label": "Synthetic A"}]
    api.retrieve.return_value = {"label": "Synthetic A", "longitude": -73.98, "latitude": 40.75}
    monkeypatch.setattr(address_page, "make_search_client", lambda: api)
    state = app.session_state["address_pickup"]
    token = state["session_token"]
    action = {"event_id": str(uuid4()), "session_token": token, "kind": "suggest", "query": "m", "version": 1}
    monkeypatch.setattr(address_page, "address_picker",
                        lambda _state, *, label, key, online: action if key == "address_pickup_picker" else None)
    app.run(timeout=10)
    action = {**action, "event_id": str(uuid4()), "kind": "retrieve", "mapbox_id": "synthetic-a"}
    app.run(timeout=10)
    app.run(timeout=10)
    assert not app.exception
    selected = app.session_state["address_pickup"]["selected"]
    assert selected == api.retrieve.return_value
    assert app.session_state["address_pickup"]["session_token"] != token
    api.suggest.assert_called_once_with("m", token)
    api.retrieve.assert_called_once_with("synthetic-a", token)
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_failed_client_construction_has_current_query_error_without_retry(ui, monkeypatch):
    from uuid import uuid4
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.address_component import AddressSelectionError
    app, router, predictor = ui
    api_factory = Mock(side_effect=AddressSelectionError("SYNTHETIC_PRIVATE_CONFIG"))
    monkeypatch.setattr(address_page, "make_search_client", api_factory)
    state = app.session_state["address_pickup"]
    action = {"event_id": str(uuid4()), "session_token": state["session_token"],
              "kind": "suggest", "query": "m", "version": 1}
    monkeypatch.setattr(address_page, "address_picker",
                        lambda _state, *, label, key, online: action if key == "address_pickup_picker" else None)
    app.run(timeout=10)
    app.run(timeout=10)
    assert not app.exception
    state = app.session_state["address_pickup"]
    assert state["query"] == "m" and state["version"] == 1 and state["status"] == "error"
    assert state["selected"] is None and "SYNTHETIC_PRIVATE_CONFIG" not in state["error"]
    api_factory.assert_called_once()
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


def test_support_badges_are_local_cached_and_invalidated_per_field(ui, monkeypatch):
    from nyc_taxi.dashboard import address_page

    app, router, predictor = ui
    geometry = address_page.get_geometry()
    search_factory = Mock(side_effect=AssertionError("Support must not use Search"))
    monkeypatch.setattr(address_page, "make_search_client", search_factory)
    app.run(timeout=10)
    assert not app.exception and len(app.success) == 2
    assert all("hợp lệ trong Taxi Zones" in row.value for row in app.success)
    assert geometry.resolve.call_count == 2 and not app.button[0].disabled
    app.run(timeout=10)
    assert geometry.resolve.call_count == 2

    app.session_state["address_pickup"]["selected"]["longitude"] = -73.96
    app.run(timeout=10)
    assert geometry.resolve.call_count == 3
    other = deepcopy(app.session_state["address_dropoff"]["selected"])
    app.session_state["address_pickup"]["selected"] = None
    app.run(timeout=10)
    assert not app.exception and app.button[0].disabled
    assert "_zone_support" not in app.session_state["address_pickup"]
    assert app.session_state["address_dropoff"]["selected"] == other
    search_factory.assert_not_called()
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


@pytest.mark.parametrize(
    ("code", "expected"),
    [("outside_zones", "ngoài vùng Taxi Zones"), ("boundary", "trên ranh giới"),
     ("overlap", "chồng lấn")],
)
def test_unsupported_pickup_explains_reason_at_correct_field_without_network(ui, code, expected):
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.taxi_zones import GeometryError

    app, router, predictor = ui
    geometry = address_page.get_geometry()

    def resolve(lon, _lat):
        if lon < -73.97:
            raise GeometryError("SYNTHETIC_PRIVATE_DETAIL", code=code)
        return {"location_id": 237, "borough": "Manhattan", "zone": "Synthetic zone"}

    geometry.resolve.side_effect = resolve
    app.run(timeout=10)
    assert not app.exception and app.button[0].disabled
    assert any("Địa chỉ đón:" in row.value and expected in row.value for row in app.warning)
    assert any("Địa chỉ đến:" in row.value for row in app.success)
    assert "SYNTHETIC_PRIVATE_DETAIL" not in " ".join(row.value for row in app.warning)
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_outside_nyc_pickup_is_blocked_without_rejecting_dropoff(ui):
    app, router, predictor = ui
    app.session_state["address_pickup"]["selected"]["longitude"] = -75
    app.run(timeout=10)
    assert not app.exception and app.button[0].disabled
    assert any("Địa chỉ đón:" in row.value and "ngoài vùng NYC" in row.value for row in app.warning)
    assert any("Địa chỉ đến:" in row.value for row in app.success)
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_special_rate_pair_is_blocked_before_directions_or_prediction(ui):
    from nyc_taxi.dashboard import address_page

    app, router, predictor = ui
    address_page.get_geometry().resolve.side_effect = lambda lon, _lat: {
        "location_id": 132 if lon < -73.97 else 237,
        "borough": "Queens" if lon < -73.97 else "Manhattan", "zone": "Synthetic zone",
    }
    app.run(timeout=10)
    assert not app.exception and app.button[0].disabled
    assert any("cước cố định" in row.value for row in app.warning)
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_missing_geometry_is_not_reported_as_address_outside_and_can_recover(ui, monkeypatch):
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.taxi_zones import GeometryError

    app, router, predictor = ui
    geometry = address_page.get_geometry()
    monkeypatch.setattr(address_page, "get_geometry", Mock(side_effect=GeometryError("SYNTHETIC_SECRET_PATH")))
    app.run(timeout=10)
    assert not app.exception and app.button[0].disabled
    assert all("chưa phải kết luận địa chỉ nằm ngoài vùng" in row.value for row in app.warning)
    assert "SYNTHETIC_SECRET_PATH" not in " ".join(row.value for row in app.warning)
    assert "_zone_support" not in app.session_state["address_pickup"]
    monkeypatch.setattr(address_page, "get_geometry", lambda: geometry)
    app.run(timeout=10)
    assert not app.exception and not app.button[0].disabled and len(app.success) == 2
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_hour_and_minute_controls_keep_all_minutes_and_values_on_rerun(ui):
    app, router, predictor = ui
    app.run(timeout=10)
    assert len(app.time_input) == 0 and len(app.selectbox) == 2
    assert app.selectbox(key="address_pickup_hour").options == [f"{value:02d}" for value in range(24)]
    assert app.selectbox(key="address_pickup_minute").options == [f"{value:02d}" for value in range(60)]
    app.date_input[0].set_value(date(2025, 1, 1))
    app.selectbox(key="address_pickup_hour").set_value(0)
    app.selectbox(key="address_pickup_minute").set_value(7)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception and not app.error
    app.run(timeout=10)
    assert app.date_input[0].value == date(2025, 1, 1)
    assert app.selectbox(key="address_pickup_hour").value == 0
    assert app.selectbox(key="address_pickup_minute").value == 7
    assert app.session_state["address_fare_result"]["pickup_datetime"] == "2025-01-01T00:07:00"
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()


def test_stale_notice_only_appears_after_inputs_change_not_identical_rerun(ui):
    from nyc_taxi.dashboard.address_page import DIRTY_INPUTS_MESSAGE

    app, router, predictor = ui
    app.run(timeout=10)
    assert not any(row.value == DIRTY_INPUTS_MESSAGE for row in app.info)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    saved = deepcopy(app.session_state["address_fare_result"])
    app.run(timeout=10)
    assert not app.exception and not any(row.value == DIRTY_INPUTS_MESSAGE for row in app.info)
    assert app.session_state["address_fare_result"] == saved
    assert not any("sửa biểu mẫu chưa" in row.value for row in app.markdown)
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()


@pytest.mark.parametrize("field", ["date", "hour", "minute", "standard", "pickup", "dropoff", "clear"])
def test_draft_changes_show_notice_without_relabeling_or_repredicting(ui, field):
    from nyc_taxi.dashboard.address_page import DIRTY_INPUTS_MESSAGE

    app, router, predictor = ui
    app.run(timeout=10)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    saved = deepcopy(app.session_state["address_fare_result"])
    if field == "date":
        app.date_input[0].set_value(date(2025, 1, 15))
    elif field == "hour":
        app.selectbox(key="address_pickup_hour").set_value(10)
    elif field == "minute":
        app.selectbox(key="address_pickup_minute").set_value(7)
    elif field == "standard":
        app.checkbox[0].set_value(False)
    elif field == "clear":
        app.session_state["address_pickup"]["selected"] = None
    else:
        app.session_state["address_" + field]["selected"]["label"] = "Edited synthetic draft"
    app.run(timeout=10)
    assert not app.exception
    assert sum(row.value == DIRTY_INPUTS_MESSAGE for row in app.info) == 1
    assert app.metric[0].value == "$12.34" and app.session_state["address_fare_result"] == saved
    assert "15/06/2025 12:00 — giờ New York" in " ".join(row.value for row in app.markdown)
    assert "Edited synthetic draft" not in " ".join(row.value for row in app.expander[0].text)
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()


def test_reverting_draft_hides_notice_and_new_explicit_submit_replaces_snapshot(ui):
    from nyc_taxi.dashboard.address_page import DIRTY_INPUTS_MESSAGE

    app, router, predictor = ui
    app.run(timeout=10)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    old = deepcopy(app.session_state["address_fare_result"])
    app.selectbox(key="address_pickup_minute").set_value(7).run(timeout=10)
    assert any(row.value == DIRTY_INPUTS_MESSAGE for row in app.info)
    app.selectbox(key="address_pickup_minute").set_value(0).run(timeout=10)
    assert not any(row.value == DIRTY_INPUTS_MESSAGE for row in app.info)
    assert app.session_state["address_fare_result"] == old
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()
    app.selectbox(key="address_pickup_minute").set_value(7)
    app.button[0].click().run(timeout=10)
    new = app.session_state["address_fare_result"]
    assert not app.exception and new["submission_id"] != old["submission_id"]
    assert new["pickup_datetime"] == "2025-06-15T12:07:00"
    assert not any(row.value == DIRTY_INPUTS_MESSAGE for row in app.info)
    assert router.directions.call_count == predictor.predict.call_count == 2


def test_repeated_addresses_and_technical_metadata_are_only_in_expander(ui):
    from nyc_taxi.dashboard.prediction import MODEL_SHA256

    app, _router, _predictor = ui
    app.run(timeout=10)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception
    assert len(app.text) == len(app.expander[0].text) == 1
    assert app.expander[0].text[0].value == "Synthetic pickup → Synthetic dropoff"
    assert MODEL_SHA256 not in " ".join(row.value for row in app.markdown)
    assert not app.warning  # No repeated generic fare/units banners.
    assert sum("chưa gồm tip, tolls và phụ phí" in row.value for row in app.markdown) == 1


@pytest.mark.parametrize("case", ["zero_distance", "long_distance", "unseen", "negative_fare", "high_fare"])
def test_relevant_case_warnings_remain_visible_without_clipping(ui, case):
    app, router, predictor = ui
    expected = {"zero_distance": "Quãng đường bằng 0", "long_distance": "Chuyến xa",
                "unseen": "chưa có trong dữ liệu huấn luyện", "negative_fare": "cước không dương",
                "high_fare": "Cước cao"}[case]
    if case == "zero_distance":
        router.directions.return_value["distance_meters"] = 0
    elif case == "long_distance":
        router.directions.return_value["distance_meters"] = 51 * 1609.344
    elif case == "unseen":
        predictor.predict.return_value["unseen_categories"]["pickup"] = True
    elif case == "negative_fare":
        predictor.predict.return_value["fare_amount_usd"] = -1.25
    else:
        predictor.predict.return_value["fare_amount_usd"] = 1001.25
    app.run(timeout=10)
    app.checkbox[0].set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception and any(expected in row.value for row in app.warning)
    assert app.session_state["address_fare_result"]["prediction"]["fare_amount_usd"] == predictor.predict.return_value["fare_amount_usd"]
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()
