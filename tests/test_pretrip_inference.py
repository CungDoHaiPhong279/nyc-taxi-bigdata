"""Offline V2 serving transport/guards; no model, network or Spark process."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from nyc_taxi.ml import inference as v1
from nyc_taxi.ml import inference_server as transport
from nyc_taxi.ml import pretrip_features as features
from nyc_taxi.ml import pretrip_inference as serving
from nyc_taxi.ml import pretrip_serving_contract as contract


NOW = datetime(2026, 10, 27, 12, 30, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def pinned_timezone(monkeypatch):
    monkeypatch.setattr(features, "timezone_identity", lambda: {
        "timezone": "America/New_York", "tzif_sha256": contract.TZDB_SHA256})


def payload(**kwargs):
    return features.create_submission(pickup_location_id=161, dropoff_location_id=237,
                                      now=NOW, **kwargs)


@pytest.fixture
def predictor():
    value = serving.FrozenPretripPredictor(Path("candidate"), Path("config"), Path("zones.csv"), clock=lambda: NOW)
    value._executor.shutdown(wait=False)
    value._executor = Mock()
    value.calendar = features.load_calendar()
    value._known_pickup, value._known_dropoff = {"161"}, {"237"}
    value.spark = SimpleNamespace(sparkContext=Mock(applicationId="local-mock-only"), stop=Mock())
    yield value
    value.close()


def test_pure_identity_has_independent_full_pins_and_version_not_latest():
    actual = contract.identity()
    assert actual["model_version"] == "v2.2"
    assert actual["model_sha256"] == serving.frozen.MODEL_TREE_SHA256
    assert actual["artifact_manifest_sha256"] == serving.frozen.ARTIFACT_MANIFEST_SHA256
    assert actual["dictionary_sha256"] == serving.frozen.DICTIONARY_SHA256
    assert actual["calendar_version"] == features.load_calendar().version
    assert all(len(actual[name]) == 64 for name in actual if name.endswith("sha256"))


def test_now_and_train_features_share_current_nyc_clock_without_2025_mapping():
    request = payload()
    actual = serving.validate_request(request, now=NOW)
    local = datetime.fromisoformat(request["pickup_datetime"])
    assert local.year == 2026 and local.hour == 8
    assert actual == features.build_training_features(local.replace(tzinfo=None), 161, 237)
    assert tuple(actual) == features.FEATURE_COLUMNS and "distance_miles" not in actual


@pytest.mark.parametrize("seconds", [-6, 61, 86400])
def test_server_clock_rejects_spoofed_stale_future_submissions(seconds):
    with pytest.raises(v1.InputError, match="Invalid V2"):
        serving.validate_request(payload(), now=NOW + timedelta(seconds=seconds))


@pytest.mark.parametrize("change", [
    {"distance_miles": 3.0}, {"fare_amount": 20.0}, {"calendar_sha256": "0" * 64},
    {"tzdb_sha256": "0" * 64}, {"pickup_location_id": 264}, {"dropoff_location_id": True},
    {"timezone": "UTC"}, {"contract_version": "v1"},
])
def test_strict_contract_leakage_zones_and_identity_rejected(change):
    with pytest.raises(v1.InputError):
        serving.validate_request({**payload(), **change}, now=NOW)


def test_dst_fall_schedule_uses_explicit_fold_not_invented_utc_or_2025():
    first = features.create_submission(pickup_location_id=161, dropoff_location_id=237,
              mode="scheduled", scheduled_local=datetime(2026, 11, 1, 1, 30), fold=0, now=NOW)
    second = features.create_submission(pickup_location_id=161, dropoff_location_id=237,
              mode="scheduled", scheduled_local=datetime(2026, 11, 1, 1, 30), fold=1, now=NOW)
    assert first["pickup_datetime"].endswith("-04:00")
    assert second["pickup_datetime"].endswith("-05:00")
    assert serving.validate_request(first, now=NOW) == serving.validate_request(second, now=NOW)
    invalid = {**second, "fold": 0}
    with pytest.raises(v1.InputError):
        serving.validate_request(invalid, now=NOW)


def test_schedule_rejects_spring_gap_and_horizon_before_model_call():
    march = datetime(2026, 3, 7, 12, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="does not exist"):
        features.create_submission(pickup_location_id=161, dropoff_location_id=237,
           mode="scheduled", scheduled_local=datetime(2026, 3, 8, 2, 30), now=march)
    with pytest.raises(ValueError, match="seven-day"):
        features.create_submission(pickup_location_id=161, dropoff_location_id=237,
           mode="scheduled", scheduled_local=datetime(2026, 11, 4, 12), now=NOW)


@pytest.mark.parametrize("pickup,dropoff,expected", [
    (161, 237, {"pickup": False, "dropoff": False}),
    (237, 237, {"pickup": True, "dropoff": False}),
    (161, 161, {"pickup": False, "dropoff": True}),
    (237, 161, {"pickup": True, "dropoff": True}),
])
def test_role_dictionaries_block_unseen_before_spark_or_executor(predictor, pickup, dropoff, expected):
    predictor.ready = True
    request = features.create_submission(pickup_location_id=pickup, dropoff_location_id=dropoff, now=NOW)
    if any(expected.values()):
        with pytest.raises(serving.UnsupportedCategories) as caught:
            predictor.predict(request)
        assert caught.value.status == 422
        assert caught.value.public_payload == {"status": "unsupported", "unseen_categories": expected}
        predictor._executor.submit.assert_not_called()
    else:
        predictor._executor.submit.return_value.result.return_value = {"fare_amount_usd": 21.1, **contract.identity()}
        assert predictor.predict(request)["fare_amount_usd"] == 21.1
        assert predictor._executor.submit.call_count == 1
        predictor._busy.release()
    predictor.spark.sparkContext.setJobGroup.assert_not_called()


def test_missing_model_and_busy_fail_without_fallback_or_refit(predictor):
    with pytest.raises(v1.InferenceError, match="model_not_ready"):
        predictor.predict(payload())
    predictor.ready = True
    assert predictor._busy.acquire(False)
    try:
        with pytest.raises(v1.InferenceError, match="service_busy"):
            predictor.predict(payload())
    finally:
        predictor._busy.release()
    predictor._executor.submit.assert_not_called()


@pytest.mark.parametrize("pickup,dropoff", [(1, 237), (161, 1), (132, 237), (161, 132)])
def test_direct_api_special_airport_gate_blocks_before_executor(predictor, pickup, dropoff):
    predictor.ready = True
    predictor._known_pickup = {"1", "132", "161", "237"}
    predictor._known_dropoff = set(predictor._known_pickup)
    predictor.zones = [{"location_id": 1, "borough": "EWR"}, {"location_id": 132, "borough": "Queens"},
                       {"location_id": 161, "borough": "Manhattan"}, {"location_id": 237, "borough": "Manhattan"}]
    request = features.create_submission(pickup_location_id=pickup, dropoff_location_id=dropoff, now=NOW)
    with pytest.raises(serving.SpecialRateUnsupported) as caught:
        predictor.predict(request)
    assert caught.value.status == 422 and caught.value.code == "special_rate_no_estimate"
    assert caught.value.public_payload == {"status": "unsupported", "reason": "special_rate_route"}
    assert predictor.ready
    predictor._executor.submit.assert_not_called()
    predictor.spark.sparkContext.setJobGroup.assert_not_called()


def test_airport_guard_does_not_generalize_all_queens_or_lga_routes():
    lookup = [{"location_id": 132, "borough": "Queens"}, {"location_id": 138, "borough": "Queens"},
              {"location_id": 7, "borough": "Queens"}]
    assert serving.guard_special_rate({"pickup_location_id": 132, "dropoff_location_id": 7}, lookup) is None
    assert serving.guard_special_rate({"pickup_location_id": 138, "dropoff_location_id": 7}, lookup) is None


@pytest.mark.parametrize("error,code,status", [
    (serving.FutureTimeout(), "prediction_timeout", 504),
    (RuntimeError("private_internal_error"), "prediction_failed", 503),
])
def test_failure_timeout_disable_service_without_details_or_fallback(predictor, monkeypatch, error, code, status):
    predictor.ready = True
    predictor._executor.submit.return_value.result.side_effect = error
    monkeypatch.setattr(serving.threading, "Thread", lambda *, target, **kwargs: SimpleNamespace(start=target))
    with pytest.raises(v1.InferenceError) as caught:
        predictor.predict(payload(), timeout=0.1)
    assert caught.value.code == code and caught.value.status == status
    assert not predictor.ready and predictor.status == code
    assert "private_internal_error" not in str(caught.value)
    predictor._busy.release()


def test_failed_artifact_audit_happens_before_spark_import_once(predictor, monkeypatch, capsys):
    audit = Mock(side_effect=ValueError("private_path"))
    monkeypatch.setattr(serving, "validate_artifacts", audit)
    predictor.load()
    assert not predictor.ready and predictor.status == "model_load_failed"
    assert "private_path" not in capsys.readouterr().out
    with pytest.raises(RuntimeError, match="once per process"):
        predictor.load()
    audit.assert_called_once()


def test_tampered_manifest_is_rejected_before_calendar_lookup_or_spark(tmp_path):
    candidate = tmp_path / "untrusted-candidate"
    candidate.mkdir()
    (candidate / "artifact_manifest.json").write_text('{"fixture":"tampered"}', encoding="utf-8")
    with pytest.raises(ValueError, match="manifest_identity_mismatch"):
        serving.validate_artifacts(candidate, tmp_path / "missing-config", tmp_path / "missing-zones")


@pytest.mark.parametrize("field", ["model_tree_sha256", "artifact_manifest_sha256"])
def test_audited_candidate_cannot_select_another_serving_identity(tmp_path, monkeypatch, field):
    identities = {"model_tree_sha256": contract.MODEL_SHA256,
                  "artifact_manifest_sha256": contract.ARTIFACT_MANIFEST_SHA256}
    identities[field] = "0" * 64
    monkeypatch.setattr(serving.frozen, "audit_candidate", lambda root: {"identities": identities})
    with pytest.raises(ValueError, match="serving_candidate_identity_drift"):
        serving.validate_artifacts(tmp_path, tmp_path, tmp_path / "not-opened.csv")


def test_runtime_contract_tamper_rejected_even_with_valid_candidate_audit(tmp_path, monkeypatch):
    monkeypatch.setattr(serving.frozen, "audit_candidate", lambda root: {"identities": {
        "model_tree_sha256": contract.MODEL_SHA256,
        "artifact_manifest_sha256": contract.ARTIFACT_MANIFEST_SHA256},
        "lineage": {"feature_builder_sha256": contract.FEATURE_BUILDER_SHA256}})
    (tmp_path / "pretrip_v2_contract.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="runtime_contract_drift"):
        serving.validate_artifacts(tmp_path, tmp_path, tmp_path / "not-opened.csv")


def test_feature_builder_drift_rejected_before_runtime_files(tmp_path, monkeypatch):
    monkeypatch.setattr(serving.frozen, "audit_candidate", lambda root: {"identities": {
        "model_tree_sha256": contract.MODEL_SHA256,
        "artifact_manifest_sha256": contract.ARTIFACT_MANIFEST_SHA256},
        "lineage": {"feature_builder_sha256": "0" * 64}})
    with pytest.raises(ValueError, match="feature_builder_drift"):
        serving.validate_artifacts(tmp_path, tmp_path, tmp_path / "not-opened.csv")


def test_successful_mocked_load_one_session_model_and_role_support(predictor, monkeypatch):
    from contextlib import contextmanager
    audit = {"dictionaries": {"pickup_zone": ["161"], "dropoff_zone": ["237"]}}
    monkeypatch.setattr(serving, "validate_artifacts", lambda *args: {
        "audit": audit, "calendar": features.load_calendar(), "zones": [{"location_id": 161, "borough": "Manhattan"}]})
    @contextmanager
    def no_fit():
        yield {"fit_calls": 0, "blocked_fit_calls": 0}
    monkeypatch.setattr(serving.frozen, "no_fit_guard", no_fit)
    fitted = object()
    loader = Mock(return_value=fitted)
    monkeypatch.setattr(serving.frozen, "load_candidate", loader)
    builder = Mock()
    for method in ("master", "appName", "config"):
        getattr(builder, method).return_value = builder
    builder.getOrCreate.return_value = predictor.spark
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = SimpleNamespace(builder=builder)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    predictor.load()
    assert predictor.ready and predictor.model is fitted
    assert predictor.readiness()["fit_calls"] == 0
    assert predictor.readiness()["model_version"] == "v2.2"
    assert predictor.zone_response()["pickup_location_ids"] == [161]
    assert predictor.zone_response()["dropoff_location_ids"] == [237]
    loader.assert_called_once_with(Path("candidate"), audit, predictor.spark)
    with pytest.raises(RuntimeError, match="once per process"):
        predictor.load()
    builder.getOrCreate.assert_called_once()


def http_request(backend, *, method="POST", path="/predict", request=None):
    raw = json.dumps(request or payload()).encode()
    handler = object.__new__(transport.handler_for(backend))
    handler.path = path
    handler.headers = {"Content-Type": "application/json", "Content-Length": str(len(raw))}
    handler.rfile, handler.wfile = BytesIO(raw), BytesIO()
    captured = {}
    handler.send_response = lambda status: captured.update(status=status)
    handler.send_header = lambda *args: None
    handler.end_headers = lambda: None
    getattr(handler, "do_" + method)()
    return captured["status"], json.loads(handler.wfile.getvalue())


def test_http_unseen_reports_unsupported_no_fare_and_correct_identity(predictor):
    predictor.ready = True
    status, response = http_request(predictor, request={**payload(), "pickup_location_id": 237})
    assert status == 422 and response["status"] == "unsupported"
    assert response["error"] == "unsupported_categories" and "fare_amount_usd" not in response
    assert all(response[name] == value for name, value in contract.identity().items())
    predictor._executor.submit.assert_not_called()


def test_http_v2_zones_and_readiness_identity_are_not_v1(predictor):
    predictor.ready = True
    for path in ("/ready", "/zones"):
        status, response = http_request(predictor, method="GET", path=path)
        assert status == 200 and response["model_version"] == "v2.2"
        assert "training_run_id" not in response
    assert response["pickup_location_ids"] == [161] and response["dropoff_location_ids"] == [237]


def test_factory_keeps_v1_default_compatible_and_selects_one_predictor(monkeypatch):
    old = Mock()
    monkeypatch.setattr(transport, "FrozenPredictor", old)
    args = (Path("m"), Path("c"), Path("z"))
    assert transport.make_predictor("v1", *args) is old.return_value
    instance = transport.make_predictor("v2.2", *args)
    try:
        assert isinstance(instance, serving.FrozenPretripPredictor)
        old.assert_called_once_with(*args)
    finally:
        instance.close()
    with pytest.raises(ValueError, match="Unsupported"):
        transport.make_predictor("latest", *args)
