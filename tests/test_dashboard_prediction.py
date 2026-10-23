"""Offline HTTP-contract and Streamlit tests; never call Spark, a DB, or a server."""

from copy import deepcopy
from datetime import date, datetime, timezone
from io import BytesIO
import json
from pathlib import Path
from unittest.mock import Mock
from urllib.error import HTTPError, URLError

import pytest

from nyc_taxi.dashboard import prediction


APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
IDENTITY = {
    "training_run_id": prediction.TRAINING_RUN_ID,
    "evaluation_run_id": prediction.EVALUATION_RUN_ID,
    "model_sha256": prediction.MODEL_SHA256,
}
READY = {"ready": True, **IDENTITY}
ZONES = {**IDENTITY, "zones": [
    {"location_id": 2, "zone": "Fixture B", "borough": "Queens"},
    {"location_id": 1, "zone": "Fixture A", "borough": "Manhattan"},
]}
RESULT = {
    **IDENTITY, "fare_amount_usd": 12.34, "unit": "USD", "request_seconds": 0.125,
    "unseen_categories": {"pickup": False, "dropoff": False}, "warnings": [],
}
INPUT = {
    "distance_miles": 1.25, "pickup_datetime": "2025-06-15T12:00:00",
    "pickup_location_id": 1, "dropoff_location_id": 2,
}


class Response:
    status = 200

    def __init__(self, payload=None, *, raw=None):
        self.raw = json.dumps(payload).encode("utf-8") if raw is None else raw
        self.read_limit = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self, limit):
        self.read_limit = limit
        return self.raw[:limit]


def mock_http(monkeypatch, payload):
    response = Response(payload)
    opener = Mock(return_value=response)
    monkeypatch.setattr(prediction, "urlopen", opener)
    return opener, response


def test_client_exact_post_contract_and_bounded_timeout(monkeypatch):
    opener, response = mock_http(monkeypatch, {**RESULT, "ignored_extra": "not returned"})
    result = prediction.InferenceClient("http://fixture.invalid:8000/").predict(**INPUT)
    request = opener.call_args.args[0]
    assert request.full_url == "http://fixture.invalid:8000/predict"
    assert request.get_method() == "POST"
    assert request.get_header("Content-type") == "application/json"
    assert json.loads(request.data) == INPUT
    assert opener.call_args.kwargs == {"timeout": 45}
    assert response.read_limit == prediction.MAX_RESPONSE_BYTES + 1
    assert result == RESULT


def test_client_readiness_and_named_zones_are_small_sorted(monkeypatch):
    opener = Mock(side_effect=[Response(READY), Response(ZONES)])
    monkeypatch.setattr(prediction, "urlopen", opener)
    client = prediction.InferenceClient()
    assert client.ready() == READY
    assert client.zones() == {**IDENTITY, "zones": sorted(ZONES["zones"], key=lambda row: row["location_id"])}
    assert [call.kwargs["timeout"] for call in opener.call_args_list] == [5, 5]
    assert [call.args[0].get_method() for call in opener.call_args_list] == ["GET", "GET"]


@pytest.mark.parametrize("method,payload", [("ready", READY), ("zones", ZONES), ("predict", RESULT)])
@pytest.mark.parametrize("field,bad", [
    ("training_run_id", "0" * 32), ("evaluation_run_id", "0" * 32),
    ("model_sha256", "0" * 64),
])
def test_client_rejects_wrong_frozen_identity(monkeypatch, method, payload, field, bad):
    mock_http(monkeypatch, {**payload, field: bad})
    client = prediction.InferenceClient()
    with pytest.raises(prediction.PredictionError):
        getattr(client, method)(**(INPUT if method == "predict" else {}))


@pytest.mark.parametrize("field,bad", [
    ("distance_miles", -0.001), ("distance_miles", 200.001), ("distance_miles", True),
    ("distance_miles", float("nan")), ("distance_miles", float("inf")),
    ("distance_miles", 10 ** 400), ("distance_miles", "1.25"),
    ("pickup_location_id", True), ("pickup_location_id", 0), ("dropoff_location_id", 264),
    ("dropoff_location_id", 2.0), ("pickup_datetime", "2024-06-15T12:00:00"),
    ("pickup_datetime", "2025-06-15T12:00:00+00:00"), ("pickup_datetime", "2025-02-30T12:00:00"),
    ("pickup_datetime", "2025-06-15"), ("pickup_datetime", datetime(2025, 1, 1, tzinfo=timezone.utc)),
])
def test_invalid_input_never_sends_http(monkeypatch, field, bad):
    opener = Mock(side_effect=AssertionError("No HTTP for invalid input"))
    monkeypatch.setattr(prediction, "urlopen", opener)
    with pytest.raises(prediction.PredictionError):
        prediction.InferenceClient().predict(**{**INPUT, field: bad})
    opener.assert_not_called()


@pytest.mark.parametrize("distance", [0, 0.0, 200])
def test_exact_distance_edges_and_july_are_not_replaced(distance):
    values = {**INPUT, "distance_miles": distance, "pickup_datetime": datetime(2025, 7, 1, 12, 30)}
    result = prediction.build_prediction_request(**values)
    assert result["distance_miles"] == distance
    assert result["pickup_datetime"] == "2025-07-01T12:30:00"
    assert set(result) == set(INPUT)


@pytest.mark.parametrize("fare", [-3.25, 0, 1200.5])
def test_finite_predictions_are_never_clipped_or_replaced(monkeypatch, fare):
    mock_http(monkeypatch, {**RESULT, "fare_amount_usd": fare})
    assert prediction.InferenceClient().predict(**INPUT)["fare_amount_usd"] == fare


@pytest.mark.parametrize("field,bad", [
    ("fare_amount_usd", float("nan")), ("fare_amount_usd", float("inf")),
    ("fare_amount_usd", 10 ** 400), ("fare_amount_usd", True),
    ("request_seconds", -1), ("request_seconds", float("inf")),
    ("unit", "EUR"), ("unseen_categories", {"pickup": 1, "dropoff": False}),
    ("warnings", ["bad\nmessage"]), ("warnings", "not an array"),
])
def test_invalid_model_response_has_no_quote(monkeypatch, field, bad):
    mock_http(monkeypatch, {**RESULT, field: bad})
    with pytest.raises(prediction.PredictionError):
        prediction.InferenceClient().predict(**INPUT)


@pytest.mark.parametrize("zones", [[], [ZONES["zones"][0]] * 2,
    [{"location_id": 264, "zone": "Bad", "borough": "Bad"}],
    [{"location_id": 1, "zone": "", "borough": "Bad"}],
])
def test_invalid_zone_metadata_fails_closed(monkeypatch, zones):
    mock_http(monkeypatch, {**IDENTITY, "zones": zones})
    with pytest.raises(prediction.PredictionError):
        prediction.InferenceClient().zones()


@pytest.mark.parametrize("raw", [b"not-json", b"[]", b"\xff", b"x" * (prediction.MAX_RESPONSE_BYTES + 1)],
                         ids=["not-json", "not-object", "invalid-utf8", "oversized"])
def test_invalid_or_oversized_response_is_rejected(monkeypatch, raw):
    monkeypatch.setattr(prediction, "urlopen", Mock(return_value=Response(raw=raw)))
    with pytest.raises(prediction.PredictionError):
        prediction.InferenceClient().ready()


@pytest.mark.parametrize("failure", [
    URLError("SYNTHETIC_SECRET_IN_URL"), TimeoutError("SYNTHETIC_SECRET_IN_TIMEOUT"),
    HTTPError("http://SYNTHETIC_SECRET.invalid", 503, "SYNTHETIC_SECRET", {}, BytesIO(b"SYNTHETIC_SECRET")),
    HTTPError("http://SYNTHETIC_SECRET.invalid", 422, "SYNTHETIC_SECRET", {}, BytesIO(b"SYNTHETIC_SECRET")),
])
def test_transport_errors_never_expose_upstream_details(monkeypatch, failure):
    monkeypatch.setattr(prediction, "urlopen", Mock(side_effect=failure))
    with pytest.raises(prediction.PredictionError) as caught:
        prediction.InferenceClient().predict(**INPUT)
    assert "SYNTHETIC_SECRET" not in str(caught.value)


@pytest.mark.parametrize("url", [
    "file:///tmp/model", "http://user:SYNTHETIC_SECRET@fixture.invalid", "http://fixture.invalid/?token=SYNTHETIC_SECRET",
    "http://fixture.invalid/path", "http://fixture.invalid:99999", "http://fixture.invalid\n",
])
def test_invalid_service_url_is_safe(url):
    with pytest.raises(prediction.PredictionError) as caught:
        prediction.InferenceClient(url)
    assert "SYNTHETIC_SECRET" not in str(caught.value)


@pytest.fixture
def ui(monkeypatch):
    # Direct imports: a missing web dependency fails these tests, never silently skips.
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    from nyc_taxi.dashboard import fare_page, queries

    st.cache_data.clear()
    ready = Mock(return_value=deepcopy(READY))
    zones = Mock(return_value={**IDENTITY, "zones": sorted(deepcopy(ZONES["zones"]), key=lambda row: row["location_id"])})
    predict = Mock(return_value=deepcopy(RESULT))
    database = Mock(side_effect=AssertionError("Estimate page must not query PostgreSQL"))
    monkeypatch.setattr(prediction.InferenceClient, "ready", ready)
    monkeypatch.setattr(prediction.InferenceClient, "zones", zones)
    monkeypatch.setattr(prediction.InferenceClient, "predict", predict)
    monkeypatch.setattr(queries, "fetch_dashboard", database)

    def create():
        app = AppTest.from_file(str(APP))
        app.session_state["dashboard_page"] = "Ước tính cước"
        return app.run(timeout=10)

    yield create, ready, zones, predict, database
    fare_page.load_model_metadata.clear()
    st.cache_data.clear()


def test_ui_form_does_not_predict_or_query_database_before_submit(ui):
    create, ready, zones, predict, database = ui
    app = create()
    assert not app.exception and not app.error
    assert app.title[0].value == "Ước tính cước"
    assert len(app.metric) == 0
    assert any("mile" in item.value for item in app.info)
    assert "1 — Manhattan — Fixture A" in app.selectbox(key="fare_pickup_zone").options
    ready.assert_called_once()
    zones.assert_called_once()
    predict.assert_not_called()
    database.assert_not_called()


def test_ui_submit_calls_service_once_and_shows_real_price_and_lineage(ui):
    create, _ready, _zones, predict, database = ui
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.number_input(key="fare_distance").set_value(1.25)
    app.button[0].click().run(timeout=10)
    assert not app.exception and not app.error
    assert app.metric[0].value == "$12.34"
    predict.assert_called_once_with(**INPUT)
    assert IDENTITY["model_sha256"] in app.code[0].value
    assert IDENTITY["evaluation_run_id"] in app.code[0].value
    database.assert_not_called()


def test_ui_requires_standard_metered_confirmation(ui):
    create, _ready, _zones, predict, _database = ui
    app = create()
    app.button[0].click().run(timeout=10)
    assert not app.exception and app.error
    assert len(app.metric) == 0
    predict.assert_not_called()


def test_ui_service_unavailable_has_no_fake_quote(ui):
    create, ready, _zones, predict, _database = ui
    ready.side_effect = prediction.PredictionError("Dịch vụ chưa sẵn sàng.")
    app = create()
    assert not app.exception and "chưa sẵn sàng" in app.error[0].value
    assert len(app.metric) == 0 and len(app.number_input) == 0
    predict.assert_not_called()


def test_ui_failed_second_submission_clears_old_price(ui):
    create, _ready, _zones, predict, _database = ui
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.button[0].click().run(timeout=10)
    assert app.metric[0].value == "$12.34"
    predict.side_effect = prediction.PredictionError("Không thể ước tính; không có giá thay thế.")
    app.button[0].click().run(timeout=10)
    assert not app.exception and app.error
    assert len(app.metric) == 0
    assert "fare_result" not in app.session_state


def test_ui_zero_negative_and_unseen_warnings_preserve_raw_prediction(ui):
    create, _ready, _zones, predict, _database = ui
    predict.return_value = {**RESULT, "fare_amount_usd": -3.25,
                            "unseen_categories": {"pickup": True, "dropoff": False}}
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.number_input(key="fare_distance").set_value(0.0)
    app.button[0].click().run(timeout=10)
    assert not app.exception and app.metric[0].value == "$-3.25"
    warnings = " ".join(item.value for item in app.warning)
    assert "bằng 0" in warnings and "không tự clip" in warnings
    assert "Vùng này chưa có trong dữ liệu huấn luyện; kết quả có thể kém chính xác hơn." in warnings
    assert predict.call_args.kwargs["distance_miles"] == 0.0


def test_ui_july_is_allowed_but_explicitly_warned(ui):
    create, _ready, _zones, predict, _database = ui
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.date_input(key="fare_pickup_date").set_value(date(2025, 7, 1))
    app.button[0].click().run(timeout=10)
    assert not app.exception and app.metric[0].value == "$12.34"
    assert any("Jul–Dec/2025 chưa được đánh giá" in item.value for item in app.warning)
    assert predict.call_args.kwargs["pickup_datetime"] == "2025-07-01T12:00:00"


def test_ui_mismatched_evaluation_metadata_fails_closed(ui):
    create, _ready, zones, predict, _database = ui
    zones.return_value = {**ZONES, "evaluation_run_id": "0" * 32}
    app = create()
    assert not app.exception and app.error
    assert len(app.number_input) == 0 and len(app.metric) == 0
    predict.assert_not_called()


def test_ui_mismatched_prediction_identity_has_no_price(ui):
    create, _ready, _zones, predict, _database = ui
    predict.return_value = {**RESULT, "evaluation_run_id": "0" * 32}
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception and app.error and len(app.metric) == 0


@pytest.mark.parametrize("technical_warning", [
    "Category PU/DO chưa thấy trong train: dùng unknown bucket đã fit, không fit lại.",
    "PU unknown_bucket đã fit.",
    "Vùng này chưa có trong dữ liệu huấn luyện; kết quả có thể kém chính xác hơn.",
])
def test_ui_unseen_warning_is_friendly_once_and_keeps_other_warnings(ui, technical_warning):
    create, _ready, _zones, predict, _database = ui
    other_warning = "Category metadata requires operator review; do not suppress unrelated warnings."
    predict.return_value = {
        **RESULT, "unseen_categories": {"pickup": True, "dropoff": True},
        "warnings": [technical_warning, technical_warning, other_warning],
    }
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception
    warnings = [item.value for item in app.warning]
    friendly = "Vùng này chưa có trong dữ liệu huấn luyện; kết quả có thể kém chính xác hơn."
    assert warnings.count(friendly) == 1
    assert warnings == [friendly, other_warning]
    assert "unknown bucket" not in " ".join(warnings)


def test_ui_without_unseen_flag_does_not_hide_service_warning(ui):
    create, _ready, _zones, predict, _database = ui
    warning = "Category metadata requires review."
    predict.return_value = {**RESULT, "warnings": [warning]}
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception
    assert [item.value for item in app.warning] == [warning]


def test_ui_model_identity_and_api_timing_only_inside_collapsed_expander(ui):
    create, _ready, _zones, _predict, _database = ui
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.button[0].click().run(timeout=10)
    assert not app.exception
    expander = app.expander[0]
    assert expander.proto.expanded is False
    assert IDENTITY["training_run_id"] in expander.code[0].value
    assert IDENTITY["evaluation_run_id"] in expander.code[0].value
    assert IDENTITY["model_sha256"] in expander.code[0].value
    assert "API" in expander.caption[0].value
    # Element identity proves every occurrence belongs to this expander subtree.
    inside_text = {id(item) for kind in ("caption", "code", "markdown") for item in expander.get(kind)}
    for kind in ("caption", "code", "markdown"):
        for item in app.get(kind):
            if any(value in item.value for value in IDENTITY.values()) or "API" in item.value:
                assert id(item) in inside_text


def test_ui_pending_form_changes_do_not_relabel_or_recompute_submitted_quote(ui):
    create, _ready, _zones, predict, database = ui
    app = create()
    app.checkbox(key="fare_standard_rate").set_value(True)
    app.number_input(key="fare_distance").set_value(1.25)
    app.button[0].click().run(timeout=10)
    submitted = deepcopy(app.session_state["fare_result"])
    assert app.metric[0].value == "$12.34"
    original_caption = next(item.value for item in app.caption if "Điểm đón:" in item.value)
    assert "1.25 mile" in original_caption and "Manhattan — Fixture A" in original_caption

    app.number_input(key="fare_distance").set_value(20.0)
    app.selectbox(key="fare_pickup_zone").select(2)
    app.selectbox(key="fare_dropoff_zone").select(1)
    app.date_input(key="fare_pickup_date").set_value(date(2025, 7, 1))
    app.run(timeout=10)  # changing inputs is not form submission
    assert not app.exception and app.metric[0].value == "$12.34"
    assert app.session_state["fare_result"] == submitted
    assert next(item.value for item in app.caption if "Điểm đón:" in item.value) == original_caption
    assert "yêu cầu gần nhất đã gửi" in original_caption
    predict.assert_called_once_with(**INPUT)
    database.assert_not_called()
