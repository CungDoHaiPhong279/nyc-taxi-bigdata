"""Offline serving contract tests; no Spark, model files, Docker or services."""

from __future__ import annotations

from datetime import datetime
import ast
from copy import deepcopy
import csv
from io import BytesIO
import json
import math
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from nyc_taxi.ml import inference as api
from nyc_taxi.ml import inference_server as transport
from nyc_taxi.ml import pilot_model


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PARITY_PATH = PROJECT_ROOT / "config/ml_serving_parity.json"


def test_parity_inputs_are_small_public_invented_requests_not_golden_predictions():
    fixture = json.loads(PARITY_PATH.read_text(encoding="utf-8"))
    assert fixture["schema_version"] == 1
    assert set(fixture) == {"schema_version", "cases"}
    assert len(fixture["cases"]) == 8
    assert len({case["id"] for case in fixture["cases"]}) == 8
    for case in fixture["cases"]:
        assert set(case) == {"id", "inputs", "expected_unseen"}
        inputs = case["inputs"]
        assert set(inputs) == {"distance_miles", "pickup_datetime", "pickup_location_id", "dropoff_location_id"}
        assert type(inputs["distance_miles"]) in {int, float}
        assert math.isfinite(inputs["distance_miles"]) and 0 <= inputs["distance_miles"] <= 200
        pickup = datetime.fromisoformat(inputs["pickup_datetime"])
        assert pickup.tzinfo is None and pickup.year == 2025
        for field in ("pickup_location_id", "dropoff_location_id"):
            assert type(inputs[field]) is int and 1 <= inputs[field] <= 263
        assert set(case["expected_unseen"]) == {"pickup", "dropoff"}
        assert all(type(value) is bool for value in case["expected_unseen"].values())


def test_parity_fixture_covers_unknown_buckets_zero_distance_and_temporal_warnings():
    cases = {case["id"]: case for case in json.loads(PARITY_PATH.read_text(encoding="utf-8"))["cases"]}
    assert cases["unseen-pickup-zone"]["inputs"]["pickup_location_id"] == 1
    assert cases["unseen-pickup-zone"]["expected_unseen"] == {"pickup": True, "dropoff": False}
    assert cases["unseen-dropoff-zone"]["inputs"]["dropoff_location_id"] == 5
    assert cases["unseen-dropoff-zone"]["expected_unseen"] == {"pickup": False, "dropoff": True}
    assert cases["unseen-both-zones"]["expected_unseen"] == {"pickup": True, "dropoff": True}
    assert cases["zero-distance-juneteenth"]["inputs"]["distance_miles"] == 0
    assert cases["long-distance-100-miles"]["inputs"]["distance_miles"] == 100
    boundary = cases["upper-distance-december-extrapolation"]["inputs"]
    assert boundary["distance_miles"] == 200 and boundary["pickup_datetime"].startswith("2025-12-31")
    assert datetime.fromisoformat(cases["microsecond-local-wall-clock"]["inputs"]["pickup_datetime"]).microsecond == 123456


def valid_payload():
    return {"distance_miles": 2.5, "pickup_datetime": "2025-01-15T08:30:00",
            "pickup_location_id": 161, "dropoff_location_id": 237}


def test_all_public_parity_cases_use_the_real_request_validator_without_timezone_conversion():
    cases = json.loads(PARITY_PATH.read_text(encoding="utf-8"))["cases"]
    for case in cases:
        validated = api.validate_inputs(case["inputs"])
        assert validated["pickup_datetime"] == datetime.fromisoformat(case["inputs"]["pickup_datetime"])
        assert validated["pickup_datetime"].tzinfo is None
        assert type(validated["distance_miles"]) is float
        assert validated["pickup_location_id"] == case["inputs"]["pickup_location_id"]


@pytest.mark.parametrize("field,value", [
    ("distance_miles", -0.1), ("distance_miles", 200.1), ("distance_miles", True),
    ("distance_miles", "2.5"), ("distance_miles", None), ("distance_miles", float("nan")),
    ("distance_miles", float("inf")), ("distance_miles", float("-inf")),
    ("distance_miles", 10 ** 1000),
    ("pickup_location_id", 0), ("pickup_location_id", 264), ("pickup_location_id", 1.0),
    ("pickup_location_id", True), ("pickup_location_id", "161"),
    ("dropoff_location_id", 0), ("dropoff_location_id", 265), ("dropoff_location_id", False),
    ("pickup_datetime", "2024-12-31T23:59:59"), ("pickup_datetime", "2026-01-01T00:00:00"),
    ("pickup_datetime", "2025-01-01T00:00:00Z"), ("pickup_datetime", "2025-01-01T00:00:00-05:00"),
    ("pickup_datetime", "2025-02-29T12:00:00"), ("pickup_datetime", "2025-01-01"),
    ("pickup_datetime", "2025-01-01T25:00:00"), ("pickup_datetime", "2025-01-01T00:00:00.1234567"),
    ("pickup_datetime", datetime(2025, 1, 1)),
])
def test_invalid_request_domains_are_public_validation_errors(field, value):
    payload = valid_payload()
    payload[field] = value
    with pytest.raises(api.InputError):
        api.validate_inputs(payload)


@pytest.mark.parametrize("payload", [None, [], {}, {**valid_payload(), "fare_amount": 12.1},
                                     {key: value for key, value in valid_payload().items() if key != "distance_miles"}])
def test_only_exact_public_input_fields_are_accepted(payload):
    with pytest.raises(api.InputError):
        api.validate_inputs(payload)


def test_boundary_distance_and_warning_conditions_preserve_raw_value():
    payload = valid_payload()
    payload.update(distance_miles=0, pickup_datetime="2025-06-19T00:00")
    validated = api.validate_inputs(payload)
    assert validated["distance_miles"] == 0.0
    assert len(api.input_warnings(validated)) == 1
    payload.update(distance_miles=200, pickup_datetime="2025-12-31T23:59:59")
    validated = api.validate_inputs(payload)
    assert validated["distance_miles"] == 200.0
    assert len(api.input_warnings(validated)) == 2
    assert api.input_warnings(api.validate_inputs(valid_payload())) == []


@pytest.fixture
def artifact_bundle(tmp_path, monkeypatch):
    model_dir, contract_dir, app = tmp_path / "model", tmp_path / "contract", tmp_path / "app"
    model_dir.mkdir()
    contract_dir.mkdir()
    feature_file = contract_dir / "feature_contract.json"
    lock_file = contract_dir / "candidate_lock.json"
    zones_file = contract_dir / "zones.csv"
    contract = {"input": list(pilot_model.INPUT_COLUMNS), "prepared": list(pilot_model.PREPARED_COLUMNS),
                "vector": list(pilot_model.FEATURE_COLUMNS), "month_year_predictors": False,
                "contract": "public offline frozen contract"}
    feature_file.write_text(json.dumps(contract), encoding="utf-8")
    zones = [{"LocationID": str(index), "Borough": "Queens", "Zone": "Public fixture zone", "service_zone": "Yellow Zone"}
             for index in range(1, 266)]
    with zones_file.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(zones[0]))
        writer.writeheader()
        writer.writerows(zones)
    hashes = {}
    for name in ("src/nyc_taxi/ml/pilot_model.py", "src/nyc_taxi/etl/silver.py"):
        path = app / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("public frozen code fixture", encoding="utf-8")
        hashes[name] = api.digest(path)
    tree = {"sha256": api.MODEL_SHA256, "files": [
        {"path": f"weights/{number:02d}", "bytes": number, "sha256": f"{number:064x}"} for number in range(40)]}
    lock = {"training_run_id": api.TRAINING_RUN_ID, "model_tree": deepcopy(tree),
            "parameters": dict(pilot_model.GBT_PARAMETERS), "features": contract,
            "feature_contract_sha256": api.digest(feature_file), "zone_lookup_sha256": api.digest(zones_file),
            "core_file_sha256": hashes}
    lock_file.write_text(json.dumps(lock), encoding="utf-8")
    real_digest = api.digest
    monkeypatch.setattr(api, "digest", lambda path: api.CANDIDATE_LOCK_SHA256 if path == lock_file else real_digest(path))
    monkeypatch.setattr(api, "tree_identity", lambda path: deepcopy(tree))
    return SimpleNamespace(model=model_dir, contract=contract_dir, app=app, feature_file=feature_file,
                           lock_file=lock_file, zones_file=zones_file, tree=tree, lock=lock,
                           real_digest=real_digest, zones=zones)


def check_bundle(bundle):
    return api.validate_artifacts(bundle.model, bundle.contract, bundle.zones_file, app=bundle.app)


def save_bundle_lock(bundle):
    bundle.lock_file.write_text(json.dumps(bundle.lock), encoding="utf-8")


def test_full_frozen_artifact_contract_is_verified_before_spark_import(artifact_bundle):
    result = check_bundle(artifact_bundle)
    assert result["model_sha256"] == api.MODEL_SHA256
    assert len(result["zones"]) == 263
    assert result["zones"][0] == {"location_id": 1, "zone": "Public fixture zone", "borough": "Queens"}
    assert result["lock"] == artifact_bundle.lock


@pytest.mark.parametrize("change", ["short_hash", "wrong_hash", "wrong_training_run", "model_tree", "file_count", "parameters",
                                    "feature_bytes", "zone_bytes", "core_bytes", "missing_core_hash", "feature_order"])
def test_artifact_identity_or_derivation_drift_fails_closed(artifact_bundle, change):
    bundle = artifact_bundle
    if change == "short_hash":
        bundle.tree["sha256"] = api.MODEL_SHA256[:16]
    elif change == "wrong_hash":
        bundle.tree["sha256"] = "0" * 64
    elif change == "wrong_training_run":
        bundle.lock["training_run_id"] = "0" * 32
    elif change == "model_tree":
        bundle.tree["files"][0]["bytes"] += 1
    elif change == "file_count":
        bundle.tree["files"].pop()
        bundle.lock["model_tree"] = deepcopy(bundle.tree)
    elif change == "parameters":
        bundle.lock["parameters"]["maxIter"] += 1
    elif change == "feature_bytes":
        bundle.feature_file.write_text("changed public feature file", encoding="utf-8")
    elif change == "zone_bytes":
        bundle.zones_file.write_text("changed public zone file", encoding="utf-8")
    elif change == "core_bytes":
        (bundle.app / "src/nyc_taxi/etl/silver.py").write_text("changed holiday code", encoding="utf-8")
    elif change == "missing_core_hash":
        del bundle.lock["core_file_sha256"]["src/nyc_taxi/etl/silver.py"]
    else:
        contract = deepcopy(bundle.lock["features"])
        contract["vector"].reverse()
        bundle.feature_file.write_text(json.dumps(contract), encoding="utf-8")
        bundle.lock["features"] = contract
        bundle.lock["feature_contract_sha256"] = bundle.real_digest(bundle.feature_file)
    save_bundle_lock(bundle)
    with pytest.raises(ValueError):
        check_bundle(bundle)


def test_pinned_candidate_lock_hash_must_match_in_full_before_model_tree_read(artifact_bundle, monkeypatch):
    monkeypatch.setattr(api, "digest", lambda path: api.CANDIDATE_LOCK_SHA256[:12])
    tree = Mock(side_effect=AssertionError("Must not inspect/load model after invalid lock"))
    monkeypatch.setattr(api, "tree_identity", tree)
    with pytest.raises(ValueError, match="candidate-lock hash"):
        check_bundle(artifact_bundle)
    tree.assert_not_called()


@pytest.mark.parametrize("change", ["duplicate", "missing", "blank_name"])
def test_verified_lookup_still_requires_unique_complete_named_ids(artifact_bundle, change):
    bundle = artifact_bundle
    if change == "duplicate":
        bundle.zones[-1]["LocationID"] = "1"
    elif change == "missing":
        bundle.zones.pop()
    else:
        bundle.zones[0]["Zone"] = ""
    with bundle.zones_file.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=("LocationID", "Borough", "Zone", "service_zone"))
        writer.writeheader()
        writer.writerows(bundle.zones)
    bundle.lock["zone_lookup_sha256"] = bundle.real_digest(bundle.zones_file)
    save_bundle_lock(bundle)
    with pytest.raises(ValueError):
        check_bundle(bundle)


@pytest.fixture
def predictor():
    value = api.FrozenPredictor(Path("model"), Path("contracts"), Path("zones.csv"))
    value._executor.shutdown(wait=False)
    value._executor = Mock()
    value.spark = SimpleNamespace(sparkContext=Mock(), stop=Mock())
    yield value
    value.close()


def test_predict_does_not_submit_until_ready_or_while_busy(predictor):
    with pytest.raises(api.InferenceError, match="model_not_ready"):
        predictor.predict(valid_payload())
    predictor._executor.submit.assert_not_called()
    predictor.ready = True
    assert predictor._busy.acquire(blocking=False)
    try:
        with pytest.raises(api.InferenceError, match="service_busy"):
            predictor.predict(valid_payload())
        predictor._executor.submit.assert_not_called()
    finally:
        predictor._busy.release()


def test_predict_submits_only_canonical_input_and_does_not_reload_model(predictor):
    predictor.ready = True
    future = predictor._executor.submit.return_value
    future.result.return_value = {"fare_amount_usd": 12.3456789, "unit": "USD", **api.identity()}
    result = predictor.predict(valid_payload(), timeout=7)
    function, inputs, group = predictor._executor.submit.call_args.args
    assert function == predictor._predict
    assert isinstance(inputs["pickup_datetime"], datetime) and type(inputs["distance_miles"]) is float
    assert group.startswith("fare-")
    future.result.assert_called_once_with(timeout=7)
    assert result["fare_amount_usd"] == 12.3456789 and result["request_seconds"] >= 0
    predictor._busy.release()  # Worker mock does not run the real finally block.


def test_timeout_cancels_own_job_and_disables_readiness_without_fallback(predictor, monkeypatch):
    monkeypatch.setattr(api.threading, "Thread", lambda *, target, **kwargs: SimpleNamespace(start=target))
    predictor.ready = True
    predictor._executor.submit.return_value.result.side_effect = api.FutureTimeout()
    with pytest.raises(api.InferenceError) as caught:
        predictor.predict(valid_payload(), timeout=0.01)
    assert caught.value.code == "prediction_timeout" and caught.value.status == 504
    assert predictor.ready is False and predictor.status == "prediction_timeout"
    group = predictor._executor.submit.call_args.args[2]
    predictor.spark.sparkContext.cancelJobGroup.assert_called_once_with(group)
    with pytest.raises(api.InferenceError, match="model_not_ready"):
        predictor.predict(valid_payload())
    assert predictor._executor.submit.call_count == 1
    predictor._busy.release()


def test_prediction_failure_hides_private_exception_and_disables_readiness(predictor):
    predictor.ready = True
    predictor._executor.submit.return_value.result.side_effect = RuntimeError("private_failure_sentinel")
    with pytest.raises(api.InferenceError, match="prediction_failed") as caught:
        predictor.predict(valid_payload())
    assert "private_failure_sentinel" not in str(caught.value)
    assert predictor.ready is False and predictor.status == "prediction_failed"
    predictor._busy.release()


def test_submit_failure_releases_busy_without_printing_internal_details(predictor):
    predictor.ready = True
    predictor._executor.submit.side_effect = RuntimeError("private_submit_sentinel")
    with pytest.raises(api.InferenceError, match="service_unavailable") as caught:
        predictor.predict(valid_payload())
    assert not predictor._busy.locked() and "private_submit_sentinel" not in str(caught.value)


def test_failed_startup_never_imports_spark_or_retries_or_leaks_paths(predictor, monkeypatch, capsys):
    monkeypatch.setattr(api, "validate_artifacts", Mock(side_effect=ValueError("private_path_sentinel")))
    predictor.load()
    assert not predictor.ready and predictor.status == "model_load_failed"
    assert "private_path_sentinel" not in capsys.readouterr().out
    with pytest.raises(RuntimeError, match="once per process"):
        predictor.load()
    api.validate_artifacts.assert_called_once()
    predictor._executor.submit.assert_not_called()


@pytest.fixture
def fake_loaded_runtime(predictor, monkeypatch):
    spark = SimpleNamespace(sparkContext=Mock(applicationId="local-offline-fixture"), stop=Mock())
    builder = Mock()
    for name in ("master", "appName", "config"):
        getattr(builder, name).return_value = builder
    builder.getOrCreate.return_value = spark
    pyspark, sql, mlflow, ml_spark = (ModuleType(name) for name in ("pyspark", "pyspark.sql", "mlflow", "mlflow.spark"))
    sql.SparkSession = SimpleNamespace(builder=builder)
    pyspark.sql, mlflow.spark = sql, ml_spark
    stages = []
    for source, target in zip(pilot_model.CATEGORICAL_INPUT_COLUMNS, pilot_model.CATEGORICAL_FEATURE_COLUMNS, strict=True):
        stage = type("StringIndexerModel", (), {})()
        stage.getInputCol, stage.getOutputCol, stage.getHandleInvalid = Mock(return_value=source), Mock(return_value=target), Mock(return_value="keep")
        stage.labels = ["10", "161", "237"]
        stages.append(stage)
    assembler = type("VectorAssembler", (), {})()
    assembler.getInputCols = Mock(return_value=list(pilot_model.FEATURE_COLUMNS))
    regressor = type("GBTRegressionModel", (), {})()
    regressor.getOrDefault = Mock(side_effect=pilot_model.GBT_PARAMETERS.__getitem__)
    fitted = SimpleNamespace(stages=[*stages, assembler, regressor])
    ml_spark.load_model = Mock(return_value=fitted)
    for name, module in (("pyspark", pyspark), ("pyspark.sql", sql), ("mlflow", mlflow), ("mlflow.spark", ml_spark)):
        monkeypatch.setitem(sys.modules, name, module)
    artifacts = Mock(return_value={"zones": [{"location_id": 10, "zone": "Public fixture zone", "borough": "Queens"}]})
    monkeypatch.setattr(api, "validate_artifacts", artifacts)
    return SimpleNamespace(spark=spark, builder=builder, fitted=fitted, load_model=ml_spark.load_model, artifacts=artifacts)


def test_one_successful_mocked_load_reuses_one_session_and_model_without_refit(predictor, fake_loaded_runtime):
    runtime = fake_loaded_runtime
    predictor.load()
    assert predictor.ready is True and predictor.status == "ready"
    assert predictor.model is runtime.fitted and predictor.spark is runtime.spark
    assert predictor._known_pickup == predictor._known_dropoff == {"10", "161", "237"}
    assert predictor.readiness()["model_load_count"] == 1
    assert predictor.readiness()["spark_application_id"] == "local-offline-fixture"
    with pytest.raises(RuntimeError, match="once per process"):
        predictor.load()
    runtime.builder.getOrCreate.assert_called_once()
    runtime.load_model.assert_called_once_with("model", dfs_tmpdir="/tmp/fare-model-load")
    runtime.artifacts.assert_called_once()
    predictor._executor.submit.assert_not_called()


@pytest.mark.parametrize("change", ["stage_class", "input_column", "unknown_handling", "vector_order", "gbt_parameter"])
def test_mocked_load_rejects_changed_fitted_stage_contract(predictor, fake_loaded_runtime, change):
    runtime = fake_loaded_runtime
    if change == "stage_class":
        runtime.fitted.stages[0] = SimpleNamespace()
    elif change == "input_column":
        runtime.fitted.stages[0].getInputCol.return_value = "other_zone"
    elif change == "unknown_handling":
        runtime.fitted.stages[0].getHandleInvalid.return_value = "skip"
    elif change == "vector_order":
        runtime.fitted.stages[2].getInputCols.return_value = list(reversed(pilot_model.FEATURE_COLUMNS))
    else:
        runtime.fitted.stages[3].getOrDefault.side_effect = lambda name: 999 if name == "maxIter" else pilot_model.GBT_PARAMETERS[name]
    predictor.load()
    assert predictor.ready is False and predictor.status == "model_load_failed"
    assert predictor.readiness()["spark_application_id"] is None
    runtime.spark.stop.assert_called_once()
    predictor._executor.submit.assert_not_called()


class FakeBackend:
    ready = True
    zones = [{"location_id": 1, "zone": "Public zone", "borough": "Queens"}]

    def __init__(self):
        self.error = None
        self.calls = []
        self.result = {"fare_amount_usd": 12.3456789, "unit": "USD", "warnings": [],
                       "unseen_categories": {"pickup": False, "dropoff": False}, **api.identity()}

    def readiness(self):
        return {"ready": self.ready, "status": "ready" if self.ready else "starting", **api.identity()}

    def predict(self, payload):
        api.validate_inputs(payload)
        self.calls.append(payload)
        if self.error:
            raise self.error
        return self.result


def http_request(backend, *, method="POST", path="/predict", body=None, headers=None):
    raw = json.dumps(valid_payload()).encode() if body is None else body
    handler = object.__new__(transport.handler_for(backend))
    handler.path = path
    handler.headers = {"Content-Type": "application/json", "Content-Length": str(len(raw)), **(headers or {})}
    handler.rfile, handler.wfile = BytesIO(raw), BytesIO()
    captured = {"headers": {}}
    handler.send_response = lambda status: captured.update(status=status)
    handler.send_header = lambda name, value: captured["headers"].update({name: value})
    handler.end_headers = lambda: None
    getattr(handler, "do_" + method)()
    captured["raw"] = handler.wfile.getvalue()
    captured["body"] = json.loads(captured["raw"])
    return captured


def test_http_success_preserves_unrounded_prediction_and_pinned_identity():
    backend = FakeBackend()
    result = http_request(backend)
    assert result["status"] == 200 and result["body"] == backend.result
    assert len(backend.calls) == 1
    assert result["headers"]["Cache-Control"] == "no-store"
    assert int(result["headers"]["Content-Length"]) == len(result["raw"])


@pytest.mark.parametrize("ready,path,status", [(False, "/health", 200), (False, "/ready", 503),
                                                (True, "/ready", 200), (False, "/zones", 503),
                                                (True, "/zones", 200), (True, "/not-found", 404)])
def test_http_readiness_and_zone_requests_do_not_invoke_prediction(ready, path, status):
    backend = FakeBackend()
    backend.ready = ready
    result = http_request(backend, method="GET", path=path)
    assert result["status"] == status and backend.calls == []
    if path == "/zones" and ready:
        assert result["body"]["zones"] == backend.zones


@pytest.mark.parametrize("body,headers", [
    (b"{}", {"Content-Type": "text/plain"}), (b"{}", {"Content-Length": "0"}),
    (b"{}", {"Content-Length": "invalid"}), (b"{}", {"Content-Length": str(transport.MAX_BODY_BYTES + 1)}),
    (b"{}", {"Content-Length": "100"}), (b"{}", {"Transfer-Encoding": "chunked"}),
    (b"{invalid", {}), (b"[]", {}), (b"\xff", {}),
    (b'{"distance_miles":NaN}', {}), (b'{"distance_miles":Infinity}', {}),
    (json.dumps({**valid_payload(), "distance_miles": 10 ** 1000}).encode(), {}),
    (json.dumps({**valid_payload(), "fare_amount": "private_request_sentinel"}).encode(), {}),
])
def test_http_invalid_or_unbounded_requests_fail_without_calling_model_or_echoing_input(body, headers):
    backend = FakeBackend()
    result = http_request(backend, body=body, headers=headers)
    assert result["status"] == 400 and result["body"]["error"] == "invalid_inputs"
    assert backend.calls == []
    assert b"private_request_sentinel" not in result["raw"]


@pytest.mark.parametrize("error,status,code", [(api.InferenceError("model_not_ready"), 503, "model_not_ready"),
                                              (api.InferenceError("prediction_timeout", 504), 504, "prediction_timeout"),
                                              (RuntimeError("private_server_sentinel"), 503, "service_unavailable")])
def test_http_public_errors_do_not_leak_internal_failures(error, status, code):
    backend = FakeBackend()
    backend.error = error
    result = http_request(backend)
    assert result["status"] == status and result["body"]["error"] == code
    assert b"private_server_sentinel" not in result["raw"]


def test_http_refuses_nonfinite_backend_response_instead_of_sending_nan():
    backend = FakeBackend()
    backend.result["fare_amount_usd"] = float("nan")
    result = http_request(backend)
    assert result["status"] >= 400
    assert b"NaN" not in result["raw"] and b"Infinity" not in result["raw"]


def test_serving_sources_have_no_fit_training_or_tracking_write_calls():
    forbidden = {"fit", "fit_preprocessor", "baseline_median", "approxQuantile", "make_pipeline",
                 "make_preprocessor", "make_regressor", "run_training", "train_models",
                 "set_tracking_uri", "create_run", "log_artifact", "log_artifacts", "log_metric",
                 "log_param", "set_terminated", "eval", "exec", "__import__"}
    for source in (api.__file__, transport.__file__):
        parsed = ast.parse(Path(source).read_text(encoding="utf-8"))
        calls = {node.func.attr if isinstance(node.func, ast.Attribute) else node.func.id
                 for node in ast.walk(parsed) if isinstance(node, ast.Call)
                 and isinstance(node.func, (ast.Attribute, ast.Name))}
        assert not calls & forbidden
