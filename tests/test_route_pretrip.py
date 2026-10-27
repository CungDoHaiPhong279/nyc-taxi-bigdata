"""Synthetic V2 serving client/routing guards; no Spark, network or real rows."""

from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
import json
from unittest.mock import Mock
from urllib.error import HTTPError, URLError
from uuid import uuid4

import pytest

from nyc_taxi.dashboard.prediction import PredictionError, selected_model_version
from nyc_taxi.dashboard.pretrip_prediction import PretripInferenceClient, UnsupportedEstimate
from nyc_taxi.dashboard.route_fare import PretripRouteFareCoordinator, RouteFareError, execute_submission
from nyc_taxi.ml.pretrip_features import create_submission
from nyc_taxi.ml.pretrip_serving_contract import identity


NOW = datetime(2026, 10, 27, 14, tzinfo=timezone.utc)


def request():
    return create_submission(pickup_location_id=161, dropoff_location_id=237, now=NOW)


def result(snapshot):
    return {**identity(), "status": "supported", "fare_amount_usd": 12.34, "unit": "USD",
            "request_seconds": .1, "unseen_categories": {"pickup": False, "dropoff": False}, "warnings": [],
            "submission": deepcopy(snapshot),
            **{key: snapshot[key] for key in ("submission_id", "pickup_datetime", "submitted_at", "mode")}}


def metadata():
    return {**identity(), "pickup_location_ids": [161], "dropoff_location_ids": [237], "zones": [
        {"location_id": 161, "zone": "Synthetic A", "borough": "Manhattan", "supported_pickup": True, "supported_dropoff": False},
        {"location_id": 237, "zone": "Synthetic B", "borough": "Manhattan", "supported_pickup": False, "supported_dropoff": True},
    ]}


def coordinator():
    geometry, router, predictor = Mock(), Mock(), Mock()
    geometry.resolve.side_effect = lambda longitude, _latitude: {
        "location_id": 161 if longitude < -73.97 else 237, "borough": "Manhattan", "zone": "Synthetic zone",
    }
    router.directions.return_value = {"distance_meters": 1234.56789,
        "geometry": {"type": "LineString", "coordinates": [[-73.98, 40.75], [-73.95, 40.76]]}}
    predictor.zones.return_value = metadata()
    snapshot = request()
    predictor.predict.return_value = result(snapshot)
    payload = {"pickup": {"label": "Synthetic A", "longitude": -73.98, "latitude": 40.75},
               "dropoff": {"label": "Synthetic B", "longitude": -73.95, "latitude": 40.76},
               "standard_rate": True, "pretrip_request": snapshot,
               "time_selection": {"mode": "now", "scheduled_local": None, "fold": None}}
    return PretripRouteFareCoordinator(router=router, geometry=geometry, predictor=predictor), payload


def test_route_distance_is_display_only_and_uuid_result_reused():
    worker, payload = coordinator()
    state, submission_id = {}, payload["pretrip_request"]["submission_id"]
    first = execute_submission(state, submission_id, payload, worker)
    second = execute_submission(state, submission_id, deepcopy(payload), worker)
    assert first is second and first.success
    stored = first.estimate.to_dict()
    assert stored["model_version"] == "v2.2" and stored["submission_id"] == submission_id
    assert stored["distance_km"] == pytest.approx(1.23456789, abs=1e-12)
    assert "distance_miles" not in stored["request"]
    assert stored["request"] == payload["pretrip_request"]
    worker.predictor.predict.assert_called_once_with(**payload["pretrip_request"])
    worker.router.directions.assert_called_once()
    payload["pickup"]["label"] = "Edited draft"
    assert stored["pickup"]["label"] == "Synthetic A"


@pytest.mark.parametrize("role", ["pickup", "dropoff"])
def test_role_specific_unseen_is_unsupported_before_route_or_model(role):
    worker, payload = coordinator()
    worker.predictor.zones.return_value[role + "_location_ids"] = [237 if role == "pickup" else 161]
    with pytest.raises(RouteFareError, match="Chưa hỗ trợ") as caught:
        worker.estimate(payload)
    assert caught.value.code == "unsupported_categories"
    worker.router.directions.assert_not_called()
    worker.predictor.predict.assert_not_called()


@pytest.mark.parametrize("case", ["wrong_zone", "distance_in_request", "unconfirmed", "special_rate"])
def test_final_pretrip_guards_stop_before_directions(case):
    worker, payload = coordinator()
    if case == "wrong_zone":
        payload["pretrip_request"]["pickup_location_id"] = 237
    elif case == "distance_in_request":
        payload["pretrip_request"]["distance_miles"] = 1
    elif case == "unconfirmed":
        payload["standard_rate"] = False
    else:
        worker.geometry.resolve.side_effect = lambda longitude, _latitude: {
            "location_id": 132 if longitude < -73.97 else 237,
            "borough": "Queens" if longitude < -73.97 else "Manhattan", "zone": "Synthetic",
        }
    with pytest.raises(RouteFareError):
        worker.estimate(payload)
    worker.router.directions.assert_not_called()
    worker.predictor.predict.assert_not_called()


def test_server_unseen_error_is_preserved_without_baseline():
    worker, payload = coordinator()
    worker.predictor.predict.side_effect = UnsupportedEstimate({"pickup": False, "dropoff": True})
    outcome = execute_submission({}, payload["pretrip_request"]["submission_id"], payload, worker)
    assert not outcome.success and "vùng đến" in outcome.error and outcome.estimate is None
    worker.predictor.predict.assert_called_once()


def test_v1_default_and_invalid_version_fail_closed(monkeypatch):
    monkeypatch.delenv("FARE_MODEL_VERSION", raising=False)
    assert selected_model_version() == "v1"
    monkeypatch.setenv("FARE_MODEL_VERSION", "v2.2")
    assert selected_model_version() == "v2.2"
    monkeypatch.setenv("FARE_MODEL_VERSION", "latest")
    with pytest.raises(PredictionError):
        selected_model_version()


def test_client_requires_exact_identity_and_full_submission(monkeypatch):
    snapshot, client = request(), PretripInferenceClient("http://synthetic-inference:8000")
    transport = Mock(return_value=result(snapshot))
    monkeypatch.setattr(client, "_request", transport)
    assert client.predict(**snapshot)["fare_amount_usd"] == 12.34
    transport.assert_called_once_with("/predict", snapshot)
    broken = result(snapshot)
    broken["submission"]["fold"] = 1
    transport.return_value = broken
    with pytest.raises(PredictionError):
        client.predict(**snapshot)
    broken = result(snapshot)
    broken["model_sha256"] = "0" * 64
    transport.return_value = broken
    with pytest.raises(PredictionError, match="không khớp"):
        client.predict(**snapshot)


@pytest.mark.parametrize("case", ["nonfinite", "unseen", "missing_status", "wrong_unit", "wrong_request", "warnings"])
def test_client_rejects_unsupported_or_malformed_price(monkeypatch, case):
    snapshot, client = request(), PretripInferenceClient("http://synthetic-inference:8000")
    response = result(snapshot)
    if case == "nonfinite": response["fare_amount_usd"] = float("inf")
    elif case == "unseen": response["unseen_categories"]["pickup"] = True
    elif case == "missing_status": response.pop("status")
    elif case == "wrong_unit": response["unit"] = "EUR"
    elif case == "wrong_request": response["submission_id"] = str(uuid4())
    else: response["warnings"] = "bad"
    monkeypatch.setattr(client, "_request", Mock(return_value=response))
    with pytest.raises(PredictionError):
        client.predict(**snapshot)


def test_client_rejects_distance_or_wrong_pin_before_http(monkeypatch):
    client, snapshot = PretripInferenceClient("http://synthetic-inference:8000"), request()
    transport = Mock(side_effect=AssertionError("No network"))
    monkeypatch.setattr(client, "_request", transport)
    snapshot["distance_miles"] = 1
    with pytest.raises(PredictionError): client.predict(**snapshot)
    snapshot = request()
    snapshot["calendar_sha256"] = "0" * 64
    with pytest.raises(PredictionError): client.predict(**snapshot)
    transport.assert_not_called()


@pytest.mark.parametrize("case", ["identity", "role", "invalid_dictionary"])
def test_zones_fail_closed_if_dictionaries_or_roles_drift(monkeypatch, case):
    client, data = PretripInferenceClient("http://synthetic-inference:8000"), metadata()
    if case == "identity": data["tzdb_sha256"] = "0" * 64
    elif case == "role": data["zones"][0]["supported_pickup"] = False
    else: data["pickup_location_ids"] = [[161]]
    monkeypatch.setattr(client, "_request", Mock(return_value=data))
    with pytest.raises(PredictionError): client.zones()


def test_safe_unsupported_http_422_has_no_retry_or_price(monkeypatch):
    from nyc_taxi.dashboard import pretrip_prediction
    body = {**identity(), "status": "unsupported", "error": "unsupported_categories",
            "unseen_categories": {"pickup": True, "dropoff": False}}
    transport = Mock(side_effect=HTTPError("http://synthetic-inference/predict", 422, "ignored", {}, BytesIO(json.dumps(body).encode())))
    monkeypatch.setattr(pretrip_prediction, "urlopen", transport)
    with pytest.raises(UnsupportedEstimate, match="vùng đón"):
        PretripInferenceClient("http://synthetic-inference:8000").predict(**request())
    transport.assert_called_once()


def test_transport_error_is_safe_and_not_retried(monkeypatch):
    from nyc_taxi.dashboard import pretrip_prediction
    transport = Mock(side_effect=URLError("SYNTHETIC_SECRET_DETAIL"))
    monkeypatch.setattr(pretrip_prediction, "urlopen", transport)
    with pytest.raises(PredictionError) as caught:
        PretripInferenceClient("http://synthetic-inference:8000").predict(**request())
    assert "SYNTHETIC_SECRET_DETAIL" not in str(caught.value)
    transport.assert_called_once()
