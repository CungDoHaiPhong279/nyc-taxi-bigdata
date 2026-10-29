"""Offline Streamlit V2 tests, with no tokens, database, model or API."""

from copy import deepcopy
from datetime import date, datetime, timezone
from unittest.mock import Mock

import pytest


NOW = datetime(2026, 10, 27, 14, 5, 7, tzinfo=timezone.utc)


@pytest.fixture
def ui(monkeypatch):
    from streamlit.testing.v1 import AppTest
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.address_component import new_address_state
    from nyc_taxi.dashboard.route_fare import PretripRouteFareCoordinator
    from nyc_taxi.ml.pretrip_serving_contract import identity

    monkeypatch.setenv("FARE_MODEL_VERSION", "v2.2")
    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "1")
    monkeypatch.delenv("FARE_ADDRESS_ALLOW_NETWORK", raising=False)
    monkeypatch.setattr(address_page, "address_picker", lambda *_a, **_kw: None)
    clock = Mock(return_value=NOW)
    monkeypatch.setattr(address_page, "submit_clock", clock)
    metadata = {**identity(), "pickup_location_ids": [161], "dropoff_location_ids": [237], "zones": []}
    monkeypatch.setattr(address_page, "load_pretrip_metadata", lambda _url: deepcopy(metadata))
    router, geometry, predictor, map_renderer = Mock(), Mock(), Mock(), Mock()
    router.directions.return_value = {"distance_meters": 1234.56789,
        "geometry": {"type": "LineString", "coordinates": [[-73.98, 40.75], [-73.95, 40.76]]}}
    geometry.resolve.side_effect = lambda lon, _lat: {
        "location_id": 161 if lon < -73.97 else 237, "borough": "Manhattan", "zone": "Synthetic zone"}
    predictor.zones.return_value = metadata

    def predict(**snapshot):
        return {**identity(), "status": "supported", "fare_amount_usd": 12.34, "unit": "USD",
                "request_seconds": .1, "unseen_categories": {"pickup": False, "dropoff": False},
                "warnings": [], "submission": deepcopy(snapshot),
                **{key: snapshot[key] for key in ("submission_id", "pickup_datetime", "submitted_at", "mode")}}

    predictor.predict.side_effect = predict
    coordinator = PretripRouteFareCoordinator(router=router, geometry=geometry, predictor=predictor)
    monkeypatch.setattr(address_page, "make_coordinator", lambda: coordinator)
    monkeypatch.setattr(address_page, "get_geometry", lambda: geometry)
    monkeypatch.setattr(address_page, "route_map", map_renderer)

    def main():
        from nyc_taxi.dashboard.fare_page import render_page
        render_page()

    app = AppTest.from_function(main)
    for slot, lon in (("pickup", -73.98), ("dropoff", -73.95)):
        state = new_address_state()
        state["selected"] = {"label": "Synthetic " + slot, "longitude": lon, "latitude": 40.75}
        app.session_state["address_" + slot] = state
    yield app, router, predictor, clock, map_renderer, metadata


def submit(app):
    app.checkbox[0].set_value(True)
    app.button(key="address_estimate_submit").click().run(timeout=10)
    assert not app.exception


def test_default_now_freezes_current_new_york_only_on_submit_and_reruns_reuse(ui):
    app, router, predictor, clock, map_renderer, _ = ui
    app.run(timeout=10)
    assert not app.exception and app.radio[0].value == "Đi ngay"
    assert not app.date_input and not app.number_input
    clock.assert_not_called()
    predictor.predict.assert_not_called()
    submit(app)
    assert not app.error and app.metric[0].value == "$12.34"
    snapshot = deepcopy(app.session_state["address_fare_result"])
    request = predictor.predict.call_args.kwargs
    assert request["pickup_datetime"] == "2026-10-27T10:05:07-04:00"
    assert request["submitted_at"] == "2026-10-27T14:05:07+00:00" and request["mode"] == "now"
    assert request["submission_id"] == snapshot["submission_id"]
    assert "distance_miles" not in request and len(request) == 12
    clock.assert_called_once()
    first_args = deepcopy(map_renderer.call_args.args), deepcopy(map_renderer.call_args.kwargs)
    clock.return_value = datetime(2026, 10, 27, 15, tzinfo=timezone.utc)
    app.run(timeout=10)
    assert not app.exception and not app.error
    assert app.session_state["address_fare_result"] == snapshot
    assert (map_renderer.call_args.args, map_renderer.call_args.kwargs) == first_args
    assert map_renderer.call_args.kwargs["submission_id"] == snapshot["submission_id"]
    assert not any("Thông tin đã thay đổi" in row.value for row in app.info)
    clock.assert_called_once()
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()


def test_scheduled_uses_actual_year_local_offset_and_not_2025_mapping(ui):
    app, _router, predictor, _clock, _map, _metadata = ui
    app.run(timeout=10)
    app.radio[0].set_value("Hẹn giờ").run(timeout=10)
    app.date_input[0].set_value(date(2026, 10, 28))
    app.selectbox(key="address_pickup_hour").set_value(9)
    app.selectbox(key="address_pickup_minute").set_value(17)
    submit(app)
    assert not app.error
    request = predictor.predict.call_args.kwargs
    assert request["pickup_datetime"] == "2026-10-28T09:17:00-04:00" and request["mode"] == "scheduled"


def test_editing_mode_after_result_keeps_submitted_labels_map_uuid_and_price(ui):
    app, router, predictor, clock, map_renderer, _ = ui
    app.run(timeout=10)
    submit(app)
    saved = deepcopy(app.session_state["address_fare_result"])
    map_args = deepcopy(map_renderer.call_args.args), deepcopy(map_renderer.call_args.kwargs)
    app.radio[0].set_value("Hẹn giờ").run(timeout=10)
    assert not app.exception
    assert app.session_state["address_fare_result"] == saved and app.metric[0].value == "$12.34"
    assert (map_renderer.call_args.args, map_renderer.call_args.kwargs) == map_args
    assert map_renderer.call_args.kwargs["submission_id"] == saved["submission_id"]
    assert sum("Thông tin đã thay đổi" in row.value for row in app.info) == 1
    assert "27/10/2026 10:05" in " ".join(row.value for row in app.markdown)
    predictor.predict.assert_called_once()
    router.directions.assert_called_once()
    clock.assert_called_once()


@pytest.mark.parametrize("role", ["pickup", "dropoff"])
def test_per_role_unknown_prevents_submission_and_does_not_reject_other_role(ui, role):
    app, router, predictor, clock, _map, metadata = ui
    metadata[role + "_location_ids"] = [237 if role == "pickup" else 161]
    app.run(timeout=10)
    assert not app.exception and app.button(key="address_estimate_submit").disabled
    assert any("Chưa hỗ trợ ước tính" in row.value for row in app.warning)
    assert len(app.success) == 1
    predictor.predict.assert_not_called()
    router.directions.assert_not_called()
    clock.assert_not_called()


def test_bad_identity_metadata_error_has_no_price_or_model_fallback(ui, monkeypatch):
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.prediction import PredictionError
    app, router, predictor, _clock, _map, _metadata = ui
    monkeypatch.setattr(address_page, "load_pretrip_metadata", Mock(side_effect=PredictionError("Candidate hash mismatch")))
    app.run(timeout=10)
    assert not app.exception and app.error and app.button(key="address_estimate_submit").disabled
    assert not app.metric
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_time_outside_horizon_is_rejected_without_routing_or_prediction(ui):
    app, router, predictor, _clock, _map, _metadata = ui
    app.run(timeout=10)
    app.radio[0].set_value("Hẹn giờ").run(timeout=10)
    app.date_input[0].set_value(date(2026, 11, 10))
    submit(app)
    assert app.error and not app.metric
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


@pytest.mark.parametrize("instant,local,fold,valid", [
    (datetime(2026, 10, 30, 14, tzinfo=timezone.utc), "2026-11-01T01:30:00", None, False),
    (datetime(2026, 10, 30, 14, tzinfo=timezone.utc), "2026-11-01T01:30:00", 1, True),
    (datetime(2026, 3, 5, 14, tzinfo=timezone.utc), "2026-03-08T02:30:00", None, False),
])
def test_submit_builder_uses_shared_dst_guards(monkeypatch, instant, local, fold, valid):
    from nyc_taxi.dashboard import address_page
    from uuid import uuid4
    monkeypatch.setattr(address_page, "submit_clock", lambda: instant)
    kwargs = {"pickup_id": 161, "dropoff_id": 237, "submission_id": str(uuid4()),
              "time_selection": {"mode": "scheduled", "scheduled_local": local, "fold": fold}}
    if not valid:
        with pytest.raises(ValueError):
            address_page.build_pretrip_submission(**kwargs)
    else:
        request = address_page.build_pretrip_submission(**kwargs)
        assert request["pickup_datetime"] == "2026-11-01T01:30:00-05:00" and request["fold"] == 1


def test_pretrip_copy_explains_distance_is_not_predictor_without_accuracy_guarantee(ui):
    app, _router, _predictor, _clock, _map, _metadata = ui
    app.run(timeout=10)
    submit(app)
    assert not app.exception
    text = " ".join(row.value for row in app.markdown)
    assert "chỉ tham khảo, không phải đầu vào mô hình" in text
    assert "chưa gồm tip, tolls và phụ phí" in text
    assert "±" not in text and "tháng 1–6/2025" not in text


def test_invalid_version_does_not_open_v1_or_call_service(ui, monkeypatch):
    app, router, predictor, clock, _map, _metadata = ui
    monkeypatch.setenv("FARE_MODEL_VERSION", "latest")
    app.run(timeout=10)
    assert not app.exception and app.error and not app.metric and not app.button
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()
    clock.assert_not_called()


def test_session_metadata_is_not_requested_again_on_result_rerun_or_draft_edit(ui, monkeypatch):
    from nyc_taxi.dashboard import address_page
    app, router, predictor, clock, _map, metadata = ui
    loader = Mock(return_value=deepcopy(metadata))
    monkeypatch.setattr(address_page, "load_pretrip_metadata", loader)
    app.run(timeout=10)
    loader.assert_called_once()
    submit(app)
    saved = deepcopy(app.session_state["address_fare_result"])
    # Simulate an expired/cleared outer TTL cache: calling the initial loader
    # now would fail. Verified session metadata must still prevent that call.
    loader.side_effect = AssertionError("Unexpected repeated metadata request")
    app.run(timeout=10)
    app.radio[0].set_value("Hẹn giờ").run(timeout=10)
    app.session_state["address_pickup"]["selected"]["label"] = "Edited synthetic draft"
    app.run(timeout=10)
    assert not app.exception and not app.error
    assert app.session_state["address_fare_result"] == saved
    loader.assert_called_once()
    router.directions.assert_called_once()
    predictor.predict.assert_called_once()
    clock.assert_called_once()


def test_session_metadata_invalidates_on_url_and_full_identity_change(ui, monkeypatch):
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.ml import pretrip_serving_contract
    app, router, predictor, _clock, _map, metadata = ui
    original_identity = pretrip_serving_contract.identity
    loader = Mock(side_effect=lambda _url: {**deepcopy(metadata), **pretrip_serving_contract.identity()})
    monkeypatch.setattr(address_page, "load_pretrip_metadata", loader)
    app.run(timeout=10)
    assert not app.exception and loader.call_count == 1
    monkeypatch.setenv("FARE_INFERENCE_URL", "http://changed-synthetic-inference:8000")
    app.run(timeout=10)
    assert not app.exception and not app.error and loader.call_count == 2
    assert loader.call_args.args == ("http://changed-synthetic-inference:8000",)
    monkeypatch.setattr(pretrip_serving_contract, "identity", lambda: {**original_identity(), "model_sha256": "f" * 64})
    app.run(timeout=10)
    assert not app.exception and not app.error and loader.call_count == 3
    assert app.session_state["_pretrip_verified_public_metadata"]["metadata"]["model_sha256"] == "f" * 64
    app.run(timeout=10)
    assert loader.call_count == 3
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_failed_metadata_load_is_not_cached_and_drift_does_not_use_old_metadata(ui, monkeypatch):
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.prediction import PredictionError
    app, router, predictor, _clock, _map, metadata = ui
    loader = Mock(side_effect=PredictionError("Synthetic unavailable metadata"))
    monkeypatch.setattr(address_page, "load_pretrip_metadata", loader)
    app.run(timeout=10)
    assert not app.exception and app.error and loader.call_count == 1
    assert "_pretrip_verified_public_metadata" not in app.session_state
    loader.side_effect = None
    loader.return_value = deepcopy(metadata)
    app.run(timeout=10)
    assert not app.exception and not app.error and loader.call_count == 2
    assert not app.button(key="address_estimate_submit").disabled
    monkeypatch.setenv("FARE_INFERENCE_URL", "http://unavailable-synthetic-inference:8000")
    loader.side_effect = PredictionError("Synthetic unavailable replacement")
    app.run(timeout=10)
    assert not app.exception and app.error and loader.call_count == 3
    assert "_pretrip_verified_public_metadata" not in app.session_state
    assert app.button(key="address_estimate_submit").disabled
    router.directions.assert_not_called()
    predictor.predict.assert_not_called()


def test_format_pretrip_time_error_distinguishes_past_horizon_dst_and_calendar():
    from nyc_taxi.dashboard.address_page import format_pretrip_time_error
    past_msg = format_pretrip_time_error(
        ValueError("Scheduled pickup must be within submit tolerance and seven-day horizon"),
        {"mode": "scheduled", "scheduled_local": "2026-10-27T08:00:00"},
        now=NOW,
    )
    assert "đã qua" in past_msg
    future_msg = format_pretrip_time_error(
        ValueError("Scheduled pickup must be within submit tolerance and seven-day horizon"),
        {"mode": "scheduled", "scheduled_local": "2026-11-10T12:00:00"},
        now=NOW,
    )
    assert "tối đa 7 ngày" in future_msg
    gap_msg = format_pretrip_time_error(ValueError("Pickup local time does not exist at the NYC DST transition"))
    assert "DST mùa xuân" in gap_msg
    fold_msg = format_pretrip_time_error(ValueError("Ambiguous NYC pickup time requires explicit fold 0 or 1"))
    assert "Lần thứ nhất/Lần thứ hai" in fold_msg
