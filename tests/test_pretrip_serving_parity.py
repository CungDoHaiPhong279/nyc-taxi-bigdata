"""Opt-in frozen-candidate serving parity on synthetic inputs, no real rows.

Run only in the bounded worker with three explicit read-only mounts. This is
not a CI artifact download or a training/holdout test. No model fitting occurs.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading
import time

import pytest

from nyc_taxi.ml import pretrip_features as features
from nyc_taxi.ml import pretrip_holdout_model as frozen
from nyc_taxi.ml import pretrip_inference as serving
from nyc_taxi.ml.inference_server import handler_for
from nyc_taxi.dashboard.pretrip_prediction import PretripInferenceClient


def test_frozen_pipeline_and_serving_parity_on_eight_synthetic_submissions():
    artifact_path = os.environ.get("PRETRIP_SERVING_ARTIFACTS")
    if not artifact_path:
        pytest.skip("Explicit frozen-candidate worker mounts required; never discover runtime artifacts")
    clock = datetime(2026, 10, 27, 12, 30, tzinfo=timezone.utc)
    tick = time.perf_counter()
    predictor = serving.FrozenPretripPredictor(Path(artifact_path),
        Path(os.environ["PRETRIP_SERVING_CONTRACT_DIR"]), Path(os.environ["PRETRIP_SERVING_ZONES"]), clock=lambda: clock)
    try:
        predictor.load()
        assert predictor.ready and predictor.readiness()["fit_calls"] == 0
        jvm_runtime = predictor.spark.sparkContext._jvm.java.lang.Runtime.getRuntime()
        actual_heap_bytes = int(jvm_runtime.maxMemory())
        actual_processors = int(jvm_runtime.availableProcessors())
        assert actual_heap_bytes == 1536 * 1024 * 1024
        assert actual_processors == 2
        scratch_space = os.statvfs("/tmp")
        scratch_ceiling_bytes = scratch_space.f_blocks * scratch_space.f_frsize
        assert scratch_ceiling_bytes <= 512 * 1024 * 1024
        pickup = [int(value) for value in predictor.audit["dictionaries"]["pickup_zone"] if int(value) not in (1, 132)]
        dropoff = [int(value) for value in predictor.audit["dictionaries"]["dropoff_zone"] if int(value) not in (1, 132)]
        requests = []
        for index in range(8):
            kwargs = {} if index == 0 else {"mode": "scheduled", "scheduled_local":
                datetime(2026, 10, 27, 9, 30) + timedelta(hours=index * 17)}
            requests.append(features.create_submission(pickup_location_id=pickup[index],
                dropoff_location_id=dropoff[-index - 1], now=clock, **kwargs))
        # Independent training-side derivation on invented wall-clock inputs;
        # the reference uses the identical, already loaded frozen pipeline.
        expected_features = [features.build_training_features(
            datetime.fromisoformat(item["pickup_datetime"]).replace(tzinfo=None),
            item["pickup_location_id"], item["dropoff_location_id"], calendar=predictor.calendar)
            for item in requests]
        with frozen.no_fit_guard() as state:
            transformed = predictor.model.transform(serving.feature_frame(predictor.spark, expected_features))
            metadata = frozen.validate_nominal_metadata(transformed, predictor.audit)
            expected = [float(row["prediction"]) for row in transformed.select("prediction").collect()]
            assert state["fit_calls"] == 0
        actual = [predictor.predict(item) for item in requests]
        assert len(actual) == len(expected) == 8
        maximum_difference = max(abs(row["fare_amount_usd"] - expected_value)
                                 for row, expected_value in zip(actual, expected, strict=True))
        assert maximum_difference <= 1e-9
        assert all(row["submission"] == request for row, request in zip(actual, requests, strict=True))
        assert metadata["vector_dimensions"] == 8
        assert metadata["categorical_nominal_dimensions"] == {"pickup_zone": 118, "dropoff_zone": 225}
        assert predictor.readiness()["model_load_count"] == 1 and predictor.fit_calls == 0
        unknown_pickup = next(value for value in range(1, 264) if str(value) not in predictor._known_pickup)
        unknown_dropoff = next(value for value in range(1, 264) if str(value) not in predictor._known_dropoff)
        for field, value in (("pickup_location_id", unknown_pickup), ("dropoff_location_id", unknown_dropoff)):
            request = {**requests[0], field: value}
            with pytest.raises(serving.UnsupportedCategories):
                predictor.predict(request)
        opposite_only = predictor._known_dropoff - predictor._known_pickup
        assert opposite_only
        # Being known to DO does not make a zone supported as PU.
        with pytest.raises(serving.UnsupportedCategories) as caught:
            predictor.predict({**requests[0], "pickup_location_id": int(sorted(opposite_only)[0])})
        assert caught.value.public_payload["unseen_categories"] == {"pickup": True, "dropoff": False}
        assert predictor.fit_calls == 0
        # Exercise the actual dashboard client and HTTP handler against this
        # same loaded predictor. Network-none still permits private loopback;
        # no production service, second model or external request is involved.
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(predictor))
        server.daemon_threads = True
        server_thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.1), daemon=True)
        server_thread.start()
        try:
            client = PretripInferenceClient(f"http://127.0.0.1:{server.server_address[1]}")
            ready = client.ready()
            zones = client.zones()
            assert ready["ready"] is True and ready["fit_calls"] == 0
            assert ready["model_sha256"] == predictor.identity()["model_sha256"]
            assert set(zones["pickup_location_ids"]) == set(map(int, predictor._known_pickup))
            assert set(zones["dropoff_location_ids"]) == set(map(int, predictor._known_dropoff))
            assert all(row["supported_pickup"] == (str(row["location_id"]) in predictor._known_pickup)
                       and row["supported_dropoff"] == (str(row["location_id"]) in predictor._known_dropoff)
                       for row in zones["zones"])
            http_prediction = client.predict(**requests[0])
            http_difference = abs(http_prediction["fare_amount_usd"] - expected[0])
            assert http_difference <= 1e-9
            assert http_prediction["submission_id"] == requests[0]["submission_id"]
            assert predictor.readiness()["model_load_count"] == 1 and predictor.fit_calls == 0
        finally:
            server.shutdown()
            server.server_close()
            server_thread.join(timeout=5)
            assert not server_thread.is_alive()
        from nyc_taxi.ml.pretrip_training import telemetry
        resource_usage = telemetry()
        assert resource_usage.get("memory.max") == str(3 * 1024 * 1024 * 1024)
        assert resource_usage.get("memory.swap.max") == "0"
        assert resource_usage.get("pids.max") == "512"
        quota, period = map(int, resource_usage["cpu.max"].split())
        assert quota / period == 2
        print(json.dumps({"identity": predictor.identity(),
                          "synthetic_cases": len(actual), "max_absolute_prediction_difference": maximum_difference,
                          "http_roundtrip_cases": 1, "http_max_absolute_prediction_difference": http_difference,
                          "fit_calls": predictor.fit_calls, "model_load_count": 1,
                          "vector_dimensions": 8, "nominal_categories": {"pickup_zone": 118, "dropoff_zone": 225},
                          "elapsed_seconds": time.perf_counter() - tick,
                          "jvm_max_heap_bytes": actual_heap_bytes, "jvm_available_processors": actual_processors,
                          "scratch_reserved_ceiling_bytes": scratch_ceiling_bytes, "telemetry": resource_usage}))
    finally:
        predictor.close()
