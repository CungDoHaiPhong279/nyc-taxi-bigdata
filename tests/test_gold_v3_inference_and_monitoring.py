"""Unit tests for Gold V3 inference helpers, JFK flat-fare rule, and monitoring metrics."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

from nyc_taxi.ml.gold_v3_inference import (
    JFK_MANHATTAN_FLAT_FARE_USD,
    derive_single_trip_features,
    gold_v3_identity,
    is_jfk_manhattan_flat_fare,
)
from nyc_taxi.monitoring.metrics import (
    fetch_etl_monitoring_metrics,
    fetch_quality_and_reject_metrics,
)


def test_gold_v3_identity_and_jfk_flat_fare_rule() -> None:
    ident = gold_v3_identity()
    assert ident["model_version"] == "gold-v3"

    boroughs = {132: "Queens", 161: "Manhattan", 138: "Queens", 68: "Manhattan"}
    assert is_jfk_manhattan_flat_fare(132, 161, boroughs) is True
    assert is_jfk_manhattan_flat_fare(68, 132, boroughs) is True
    assert is_jfk_manhattan_flat_fare(132, 138, boroughs) is False
    assert is_jfk_manhattan_flat_fare(161, 68, boroughs) is False
    assert JFK_MANHATTAN_FLAT_FARE_USD == 70.0


def test_served_bundle_matches_its_manifest_and_tampering_is_refused(tmp_path) -> None:
    import json
    import shutil
    from pathlib import Path

    import pytest

    from nyc_taxi.ml.gold_v3_inference import verify_bundle

    bundle = Path(__file__).resolve().parents[1] / "distribution" / "models" / "gold-v3"
    verify_bundle(bundle)

    (tmp_path / "zones.csv").write_text("LocationID\n1\n", encoding="utf-8")
    shutil.copy(bundle / "artifact_manifest.json", tmp_path / "artifact_manifest.json")
    manifest = json.loads((tmp_path / "artifact_manifest.json").read_text(encoding="utf-8"))
    manifest["files"] = [e for e in manifest["files"] if e["path"] == "zones.csv"]
    (tmp_path / "artifact_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="zones.csv"):
        verify_bundle(tmp_path)


def test_timed_out_prediction_recovers_when_spark_returns() -> None:
    import threading
    from pathlib import Path

    import pytest

    from nyc_taxi.ml.gold_v3_inference import FrozenGoldV3Predictor
    from nyc_taxi.ml.inference import InferenceError
    from nyc_taxi.ml.pretrip_v23_features import load_calendar, load_zone_centroids

    predictor = FrozenGoldV3Predictor(Path("unused"), Path("unused"), Path("unused"))
    predictor.calendar, predictor.centroids = load_calendar(), load_zone_centroids()
    predictor.ready, predictor.status = True, "ready"
    spark_returns = threading.Event()

    def slow_spark(*_args):
        try:
            spark_returns.wait(5)
            raise RuntimeError("job cancelled")
        finally:
            predictor._busy.release()

    predictor._run_spark_predict = slow_spark
    payload = {"pickup_location_id": 161, "dropoff_location_id": 236, "pickup_datetime": "2025-06-11T12:30:00"}
    with pytest.raises(InferenceError) as caught:
        predictor.predict(payload, timeout=0.05)
    assert caught.value.code == "prediction_timeout"
    assert predictor.ready is False  # fail closed while the job may still be running

    spark_returns.set()
    predictor._executor.shutdown(wait=True)
    assert predictor.ready is True and predictor.status == "ready"


def test_derive_single_trip_features_with_and_without_weather() -> None:
    dt = datetime(2025, 7, 4, 9, 30, 0)  # July 4th holiday
    base_only = derive_single_trip_features(
        pickup_location_id=161,
        dropoff_location_id=236,
        pickup_dt=dt,
        weather=None,
    )
    assert base_only["is_holiday"] == 1.0
    assert base_only["is_same_zone"] == 0.0
    assert base_only["zone_centroid_distance_km"] > 0.0
    assert "temperature_2m" not in base_only

    with_w = derive_single_trip_features(
        pickup_location_id=161,
        dropoff_location_id=161,
        pickup_dt=dt,
        weather={
            "temperature_2m": 24.5,
            "precipitation": 1.2,
            "snowfall": 0.0,
            "wind_speed_10m": 8.0,
        },
    )
    assert with_w["is_same_zone"] == 1.0
    assert with_w["zone_centroid_distance_km"] == 0.0
    assert with_w["temperature_2m"] == 24.5
    assert with_w["precipitation"] == 1.2


def test_fetch_etl_monitoring_metrics_mock_engine() -> None:
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn

    row1 = MagicMock()
    row1._mapping = {
        "dataset": "taxi_2025-01",
        "source_year": 2025,
        "source_month": 1,
        "gold_batch_id": "b1",
        "source_row_count": 3000000,
        "loaded_row_count": 3000000,
        "row_delta": 0,
        "duration_seconds": 42.5,
        "status": "loaded",
        "completed_at": datetime(2025, 7, 1, 12, 0, 0),
    }
    mock_conn.execute.return_value = [row1]

    res = fetch_etl_monitoring_metrics(engine=mock_engine)
    assert res["metric_1_job_status"]["all_healthy"] is True
    assert res["metric_1_job_status"]["success_rate_pct"] == 100.0
    assert res["metric_3_row_reconciliation"]["zero_row_loss"] is True
    assert res["metric_3_row_reconciliation"]["row_delta"] == 0


def test_fetch_quality_and_reject_metrics_real_manifests() -> None:
    res = fetch_quality_and_reject_metrics()
    m4 = res["metric_4_quality_and_rejects"]
    assert m4["silver_cleaning"]["silver_valid_rows"] == 23886265
    assert m4["ml_gold_v3_cohort"]["gold_input_rows"] == 23886265
    assert m4["ml_gold_v3_cohort"]["train_active_pickup_zones"] == 258
    assert m4["ml_gold_v3_cohort"]["train_active_dropoff_zones"] == 259
