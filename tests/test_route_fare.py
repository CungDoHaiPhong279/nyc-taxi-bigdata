"""Entirely synthetic routing/model tests: no provider, geometry file or Spark."""

from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import datetime, timezone
from unittest.mock import Mock
from uuid import uuid4

import pytest

from nyc_taxi.dashboard.mapbox_client import MapboxError
from nyc_taxi.dashboard.prediction import (
    EVALUATION_RUN_ID, MODEL_SHA256, TRAINING_RUN_ID, PredictionError,
)
from nyc_taxi.dashboard.route_fare import (
    MAX_ROUTE_GEOMETRY_POINTS, MAX_SESSION_SUBMISSIONS, METERS_PER_MILE,
    RouteFareCoordinator, RouteFareError, execute_submission,
)
from nyc_taxi.dashboard.taxi_zones import GeometryError


PAYLOAD = {
    "pickup": {"label": "Synthetic pickup A", "longitude": -73.98, "latitude": 40.75},
    "dropoff": {"label": "Synthetic dropoff B", "longitude": -73.95, "latitude": 40.76},
    "pickup_datetime": "2025-01-15T08:30:00", "standard_rate": True,
}
RESULT = {
    "fare_amount_usd": 12.34, "unit": "USD", "request_seconds": 0.1,
    "training_run_id": TRAINING_RUN_ID, "evaluation_run_id": EVALUATION_RUN_ID,
    "model_sha256": MODEL_SHA256, "warnings": [],
    "unseen_categories": {"pickup": False, "dropoff": False},
}
ROUTE_GEOMETRY = {
    "type": "LineString", "coordinates": [[-73.98, 40.75], [-73.96, 40.755], [-73.95, 40.76]],
}


def coordinator(*, distance=1234.56789, zones=None):
    router = Mock()
    router.directions.return_value = {"distance_meters": distance, "geometry": deepcopy(ROUTE_GEOMETRY)}
    geometry = Mock()
    geometry.resolve.side_effect = zones if zones is not None else [
        {"location_id": 161, "borough": "Manhattan", "zone": "Synthetic A"},
        {"location_id": 237, "borough": "Manhattan", "zone": "Synthetic B"},
    ]
    predictor = Mock()
    predictor.predict.return_value = deepcopy(RESULT)
    return RouteFareCoordinator(router=router, geometry=geometry, predictor=predictor)


def zone(location_id, borough="Queens"):
    return {"location_id": location_id, "borough": borough, "zone": "Synthetic zone"}


def test_exact_route_units_mapping_and_frozen_identity_snapshot():
    worker = coordinator()
    payload = deepcopy(PAYLOAD)
    result = worker.estimate(payload)
    worker.router.directions.assert_called_once_with((-73.98, 40.75), (-73.95, 40.76))
    assert worker.geometry.resolve.call_args_list[0].args == (-73.98, 40.75)
    assert worker.geometry.resolve.call_args_list[1].args == (-73.95, 40.76)
    worker.predictor.predict.assert_called_once_with(
        distance_miles=1234.56789 / 1609.344, pickup_datetime="2025-01-15T08:30:00",
        pickup_location_id=161, dropoff_location_id=237,
    )
    assert result.distance_km == 1234.56789 / 1000
    assert result.distance_meters == 1234.56789
    assert result.prediction["model_sha256"] == MODEL_SHA256
    assert set(result.request) == {"distance_miles", "pickup_datetime", "pickup_location_id", "dropoff_location_id"}
    payload["pickup"]["label"] = "Edited, not submitted"
    worker.predictor.predict.return_value["warnings"].append("Edited response")
    assert result.pickup["label"] == "Synthetic pickup A"
    assert result.prediction["warnings"] == ()
    assert result.route_geometry["type"] == "LineString"
    assert result.route_geometry["coordinates"] == tuple(tuple(point) for point in ROUTE_GEOMETRY["coordinates"])
    worker.router.directions.return_value["geometry"]["coordinates"][0][0] = 0
    assert result.route_geometry["coordinates"][0][0] == -73.98
    with pytest.raises(TypeError):
        result.route_geometry["coordinates"][0][0] = 0
    with pytest.raises(TypeError):
        result.request["distance_miles"] = 3
    with pytest.raises(FrozenInstanceError):
        result.distance_km = 3
    displayed = result.to_dict()
    displayed["pickup"]["label"] = "Edited display"
    displayed["route_geometry"]["coordinates"][0][0] = 0
    assert result.pickup["label"] == "Synthetic pickup A"
    assert result.route_geometry["coordinates"][0][0] == -73.98


def test_injected_http_geometry_and_existing_predict_contract_together(monkeypatch, caplog):
    """One complete synthetic path, using real adapters without any open socket."""
    import json
    import socket
    from urllib.parse import parse_qs, urlsplit
    from nyc_taxi.dashboard.address_component import new_address_state, process_address_event
    from nyc_taxi.dashboard.mapbox_client import MapboxClient
    from nyc_taxi.dashboard.taxi_zones import TaxiZoneGeometry
    from nyc_taxi.dashboard import prediction as serving

    monkeypatch.setattr(socket, "create_connection", Mock(side_effect=AssertionError("Offline test forbids sockets")))
    coordinates = {"Synthetic pickup A": [-73.98, 40.75], "Synthetic dropoff B": [-73.95, 40.76]}
    selected_queries = {}
    calls = []

    def provider_http(url, **_kwargs):
        parsed = urlsplit(url)
        params = parse_qs(parsed.query)
        calls.append((parsed.path, params))
        if parsed.path.endswith("/suggest"):
            name = params["q"][0]
            selected_queries[params["session_token"][0]] = name
            return {"suggestions": [{"name": name, "mapbox_id": "synthetic-id"}]}
        if "/retrieve/" in parsed.path:
            name = selected_queries[params["session_token"][0]]
            return {"features": [{"geometry": {"type": "Point", "coordinates": coordinates[name]},
                                  "properties": {"name": name}}]}
        assert "/directions/v5/mapbox/driving/" in parsed.path
        return {"code": "Ok", "routes": [{"distance": 1234.56789, "geometry": deepcopy(ROUTE_GEOMETRY)}]}

    api = MapboxClient("synthetic-not-a-real-token", http=provider_http)
    selections = []
    for name in coordinates:
        state = new_address_state()
        for kind, fields in (("suggest", {"query": name}), ("retrieve", {"mapbox_id": "synthetic-id"})):
            process_address_event(state, {"event_id": str(uuid4()), "session_token": state["session_token"],
                                          "kind": kind, **fields}, api)
        process_address_event(state, {"event_id": str(uuid4()), "session_token": state["session_token"],
                                      "kind": "select", "selection": deepcopy(state["retrieved"])}, api)
        selections.append(state["selected"])

    def polygon(location, west, east):
        return {"type": "Feature", "properties": {"LocationID": location, "Borough": "Manhattan", "Zone": "Synthetic"},
                "geometry": {"type": "Polygon", "coordinates": [[[west, 40.70], [east, 40.70],
                              [east, 40.80], [west, 40.80], [west, 40.70]]]}}

    geometry = TaxiZoneGeometry({"type": "FeatureCollection", "features": [polygon(161, -74.0, -73.97), polygon(237, -73.96, -73.94)]})

    class Response:
        status = 200
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def read(self, limit): return json.dumps(RESULT).encode()[:limit]

    inference_http = Mock(return_value=Response())
    monkeypatch.setattr(serving, "urlopen", inference_http)
    worker = RouteFareCoordinator(router=api, geometry=geometry, predictor=serving.InferenceClient())
    payload = {**PAYLOAD, "pickup": selections[0], "dropoff": selections[1]}
    session, identifier = {}, str(uuid4())
    outcome = execute_submission(session, identifier, payload, worker)
    assert outcome.success
    assert execute_submission(session, identifier, payload, worker) is outcome
    sent = json.loads(inference_http.call_args.args[0].data)
    assert sent == {"distance_miles": 1234.56789 / 1609.344, "pickup_datetime": PAYLOAD["pickup_datetime"],
                    "pickup_location_id": 161, "dropoff_location_id": 237}
    inference_http.assert_called_once()
    assert len(calls) == 5 and len(selected_queries) == 2  # two searches/retrieves, one route
    assert outcome.estimate.prediction["model_sha256"] == MODEL_SHA256
    assert outcome.estimate.to_dict()["route_geometry"] == ROUTE_GEOMETRY
    assert outcome.estimate.submission_id == identifier
    assert caplog.text == ""


@pytest.mark.parametrize("distance", [0, 200 * METERS_PER_MILE])
def test_distance_boundaries_not_rounded_or_clipped(distance):
    worker = coordinator(distance=distance)
    worker.predictor.predict.return_value["fare_amount_usd"] = -2.5
    result = worker.estimate(PAYLOAD)
    assert result.request["distance_miles"] == distance / METERS_PER_MILE
    assert result.prediction["fare_amount_usd"] == -2.5


@pytest.mark.parametrize("distance", [True, "1000", None, -0.1, 200 * METERS_PER_MILE + .01, float("nan"), float("inf"), 10 ** 400])
def test_bad_router_distance_never_predicts(distance):
    worker = coordinator(distance=distance)
    with pytest.raises(RouteFareError):
        worker.estimate(PAYLOAD)
    worker.predictor.predict.assert_not_called()


@pytest.mark.parametrize("geometry", [
    None, {}, {"type": "Polygon", "coordinates": []},
    {"type": "LineString", "coordinates": []},
    {"type": "LineString", "coordinates": [[-73.98, 40.75]]},
    {"type": "LineString", "coordinates": [[-73.98, 40.75]] * (MAX_ROUTE_GEOMETRY_POINTS + 1)},
    {"type": "LineString", "coordinates": [[-73.98, 40.75, 10], [-73.95, 40.76]]},
    {"type": "LineString", "coordinates": [[True, 40.75], [-73.95, 40.76]]},
    {"type": "LineString", "coordinates": [[float("nan"), 40.75], [-73.95, 40.76]]},
    {"type": "LineString", "coordinates": [[float("inf"), 40.75], [-73.95, 40.76]]},
    {"type": "LineString", "coordinates": [[-181, 40.75], [-73.95, 40.76]]},
    {"type": "LineString", "coordinates": [[-73.98, 91], [-73.95, 40.76]]},
    {"type": "LineString", "coordinates": [[10 ** 400, 40.75], [-73.95, 40.76]]},
])
def test_bad_router_geometry_never_predicts_or_exposes_provider_payload(geometry, caplog):
    worker = coordinator()
    worker.router.directions.return_value["geometry"] = geometry
    with pytest.raises(RouteFareError, match="Hình dạng tuyến đường"):
        worker.estimate(PAYLOAD)
    worker.router.directions.assert_called_once()
    worker.predictor.predict.assert_not_called()
    assert caplog.text == ""


def test_geometry_point_budget_accepts_boundary_without_simplifying():
    worker = coordinator()
    points = [[-73.98, 40.75]] * MAX_ROUTE_GEOMETRY_POINTS
    worker.router.directions.return_value["geometry"] = {"type": "LineString", "coordinates": points}
    estimate = worker.estimate(PAYLOAD)
    assert len(estimate.route_geometry["coordinates"]) == MAX_ROUTE_GEOMETRY_POINTS
    assert estimate.to_dict()["route_geometry"]["coordinates"] == points
    worker.predictor.predict.assert_called_once()


@pytest.mark.parametrize("when", [
    "2024-12-31T23:59:59", "2025-07-01T00:00:00", "2025-01-01", "2025-02-30T10:00:00",
    "2025-06-30T23:59:59+00:00", datetime(2025, 1, 1, tzinfo=timezone.utc), None,
])
def test_invalid_or_out_of_period_time_stops_before_remote_calls(when):
    worker = coordinator()
    with pytest.raises(RouteFareError):
        worker.estimate({**PAYLOAD, "pickup_datetime": when})
    worker.router.directions.assert_not_called()
    worker.predictor.predict.assert_not_called()


@pytest.mark.parametrize("when", ["2025-01-01T00:00:00", "2025-06-30T23:59:59.999999", datetime(2025, 3, 9, 3, 30)])
def test_local_time_is_preserved_without_utc_conversion(when):
    worker = coordinator()
    result = worker.estimate({**PAYLOAD, "pickup_datetime": when})
    expected = when.isoformat() if isinstance(when, datetime) else when
    assert result.request["pickup_datetime"] == expected


@pytest.mark.parametrize("coordinates", [
    (-180, 0), (181, 40.75), (-73.98, 91), (True, 40.75), (-73.98, float("nan")),
    (float("inf"), 40.75), ("-73.98", 40.75), (10 ** 400, 40.75), (None, 40.75),
])
def test_invalid_or_unsupported_coordinates_never_route(coordinates):
    worker = coordinator()
    payload = deepcopy(PAYLOAD)
    payload["pickup"].update(longitude=coordinates[0], latitude=coordinates[1])
    with pytest.raises(RouteFareError):
        worker.estimate(payload)
    worker.geometry.resolve.assert_not_called()
    worker.router.directions.assert_not_called()


@pytest.mark.parametrize("standard", [False, None, 1, "standard", "negotiated", "group", "upfront"])
def test_standard_rate_requires_explicit_confirmation(standard):
    worker = coordinator()
    with pytest.raises(RouteFareError, match="standard-rate"):
        worker.estimate({**PAYLOAD, "standard_rate": standard})
    worker.router.directions.assert_not_called()
    worker.predictor.predict.assert_not_called()


@pytest.mark.parametrize("zones", [
    [zone(1, "EWR"), zone(237, "Manhattan")],
    [zone(237, "Manhattan"), zone(1, "EWR")],
    [zone(132), zone(237, "Manhattan")],
    [zone(161, "Manhattan"), zone(132)],
    [zone(264, "Unknown"), zone(161, "Manhattan")],
    [zone(161, "Outside NYC"), zone(132)],
])
def test_special_fare_and_unsupported_zone_gate_precedes_routing(zones):
    worker = coordinator(zones=zones)
    with pytest.raises(RouteFareError):
        worker.estimate(PAYLOAD)
    worker.router.directions.assert_not_called()
    worker.predictor.predict.assert_not_called()


@pytest.mark.parametrize("zones", [[zone(132), zone(100, "Brooklyn")], [zone(138), zone(161, "Manhattan")]])
def test_allowed_airport_routes_keep_fee_warning(zones):
    worker = coordinator(zones=zones)
    result = worker.estimate(PAYLOAD)
    assert any("sân bay" in warning for warning in result.warnings)
    worker.predictor.predict.assert_called_once()


@pytest.mark.parametrize("stage,error", [
    ("geometry", GeometryError("SYNTHETIC_SECRET_OR_ADDRESS")),
    ("router", MapboxError("SYNTHETIC_SECRET_OR_ADDRESS")),
    ("predictor", PredictionError("SYNTHETIC_SECRET_OR_ADDRESS")),
])
def test_provider_failures_are_safe_and_never_fabricate_price(stage, error, caplog):
    worker = coordinator()
    target = {"geometry": worker.geometry.resolve, "router": worker.router.directions, "predictor": worker.predictor.predict}[stage]
    target.side_effect = error
    with pytest.raises(RouteFareError) as caught:
        worker.estimate(PAYLOAD)
    assert "SYNTHETIC_SECRET_OR_ADDRESS" not in str(caught.value)
    assert "SYNTHETIC_SECRET_OR_ADDRESS" not in caplog.text
    if stage in {"geometry", "router"}:
        worker.predictor.predict.assert_not_called()


def test_session_rerun_never_repeats_routing_or_prediction_and_rejects_edited_payload():
    worker = coordinator()
    state, event = {}, str(uuid4())
    first = execute_submission(state, event, deepcopy(PAYLOAD), worker)
    second = execute_submission(state, event, deepcopy(PAYLOAD), worker)
    assert first.success and second is first
    assert first.estimate.submission_id == event
    assert first.estimate.to_dict()["route_geometry"] == ROUTE_GEOMETRY
    worker.router.directions.assert_called_once()
    worker.predictor.predict.assert_called_once()
    changed = deepcopy(PAYLOAD)
    changed["pickup"]["label"] = "Edited input"
    with pytest.raises(RouteFareError):
        execute_submission(state, event, changed, worker)
    assert first.estimate.pickup["label"] == "Synthetic pickup A"
    assert first.estimate.to_dict()["route_geometry"] == ROUTE_GEOMETRY
    worker.router.directions.assert_called_once()


@pytest.mark.parametrize("unexpected", [False, True])
def test_failed_session_submission_is_not_retried_on_rerun(unexpected, caplog):
    worker = coordinator()
    state, event = {}, str(uuid4())
    worker.router.directions.side_effect = RuntimeError("SYNTHETIC_SECRET") if unexpected else MapboxError("SYNTHETIC_SECRET")
    first = execute_submission(state, event, PAYLOAD, worker)
    second = execute_submission(state, event, PAYLOAD, worker)
    assert second is first and not first.success and first.estimate is None
    assert "SYNTHETIC_SECRET" not in first.error and "SYNTHETIC_SECRET" not in caplog.text
    worker.router.directions.assert_called_once()
    worker.predictor.predict.assert_not_called()


def test_new_uuid_is_deliberate_retry_and_does_not_share_between_sessions():
    worker = coordinator(zones=[zone(161, "Manhattan"), zone(237, "Manhattan")] * 3)
    state, event = {}, str(uuid4())
    assert execute_submission(state, event, PAYLOAD, worker).success
    assert execute_submission(state, str(uuid4()), PAYLOAD, worker).success
    assert execute_submission({}, event, PAYLOAD, worker).success
    assert worker.router.directions.call_count == worker.predictor.predict.call_count == 3


def test_session_limit_keeps_old_events_without_allowing_replay():
    worker = coordinator(zones=[zone(161, "Manhattan"), zone(237, "Manhattan")] * MAX_SESSION_SUBMISSIONS)
    state, first_event = {}, str(uuid4())
    first = execute_submission(state, first_event, PAYLOAD, worker)
    for _ in range(MAX_SESSION_SUBMISSIONS - 1):
        assert execute_submission(state, str(uuid4()), PAYLOAD, worker).success
    with pytest.raises(RouteFareError, match="giới hạn"):
        execute_submission(state, str(uuid4()), PAYLOAD, worker)
    assert execute_submission(state, first_event, PAYLOAD, worker) is first
    assert execute_submission(state, first_event.upper(), PAYLOAD, worker) is first
    assert worker.router.directions.call_count == MAX_SESSION_SUBMISSIONS
    assert worker.predictor.predict.call_count == MAX_SESSION_SUBMISSIONS


@pytest.mark.parametrize("event", [None, "not-uuid", 123])
def test_bad_submission_identifier_never_runs(event):
    worker = coordinator()
    with pytest.raises(RouteFareError):
        execute_submission({}, event, PAYLOAD, worker)
    worker.router.directions.assert_not_called()
