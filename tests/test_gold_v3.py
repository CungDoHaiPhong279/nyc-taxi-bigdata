"""Unit tests for Gold V3 inference client, Mapbox route coordinator, and 5-metric pipeline monitoring."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from nyc_taxi.dashboard.prediction import selected_model_version
from nyc_taxi.dashboard.pretrip_prediction import GOLD_V3_IDENTITY
from nyc_taxi.dashboard.route_fare import GoldV3RouteFareCoordinator
from nyc_taxi.ml.gold_v3_inference import is_jfk_manhattan_flat_fare
from nyc_taxi.ml.pretrip_v23_features import load_zone_centroids
from nyc_taxi.monitoring.metrics import fetch_quality_and_reject_metrics


def test_selected_model_version_supports_gold_v3(monkeypatch):
    monkeypatch.setenv("FARE_MODEL_VERSION", "gold-v3")
    assert selected_model_version() == "gold-v3"


def test_jfk_manhattan_flat_fare_rule_and_centroids():
    centroids = load_zone_centroids()
    dist_km = centroids.distance_km(132, 161)
    assert dist_km > 15.0
    boroughs = {132: "Queens", 161: "Manhattan", 236: "Manhattan"}
    assert is_jfk_manhattan_flat_fare(132, 161, boroughs) is True
    assert is_jfk_manhattan_flat_fare(161, 132, boroughs) is True
    assert is_jfk_manhattan_flat_fare(161, 236, boroughs) is False


def test_gold_v3_route_fare_coordinator_supports_jfk_manhattan_and_standard_routes():
    router = MagicMock()
    router.directions.return_value = {
        "distance_meters": 26500.0,
        "geometry": {
            "type": "LineString",
            "coordinates": [[-73.7781, 40.6413], [-73.9855, 40.7580]],
        },
    }
    geometry = MagicMock()
    geometry.resolve.side_effect = [
        {"location_id": 132, "borough": "Queens", "zone": "JFK Airport"},
        {"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center"},
    ]
    predictor = MagicMock()
    predictor.predict.return_value = {
        **GOLD_V3_IDENTITY,
        "status": "supported",
        "unit": "USD",
        "fare_amount_usd": 70.0,
        "baseline_fare_usd": 70.0,
        "zone_centroid_distance_km": 20.69,
        "is_same_zone": 0,
        "model_variant": "Rule-JFK-Manhattan-Flat-70",
        "ratecode_id": 2,
        "flat_fare_applied": True,
        "request_seconds": 0.01,
        "unseen_categories": {"pickup": False, "dropoff": False},
        "warnings": [
            "Tuyến JFK (Zone 132) ↔ Manhattan áp dụng giá cước cố định theo quy định NYC TLC (Ratecode 2 Flat Fare = $70.00 USD)."
        ],
    }

    coordinator = GoldV3RouteFareCoordinator(router=router, geometry=geometry, predictor=predictor)
    payload = {
        "pickup": {"label": "JFK Airport, Queens, NY", "longitude": -73.7781, "latitude": 40.6413},
        "dropoff": {"label": "Times Square, Manhattan, NY", "longitude": -73.9855, "latitude": 40.7580},
        "standard_rate": True,
        "time_selection": {"mode": "now", "scheduled_local": None, "fold": None},
        "pretrip_request": {
            "submission_id": "11111111-2222-3333-4444-555555555555",
            "pickup_location_id": 132,
            "dropoff_location_id": 161,
            "pickup_datetime": "2026-10-29T12:00:00-04:00",
            "submitted_at": "2026-10-29T16:00:00Z",
            "mode": "now",
        },
    }
    estimate = coordinator.estimate(payload)
    out = estimate.to_dict()
    assert out["model_version"] == "gold-v3"
    assert out["prediction"]["fare_amount_usd"] == 70.0
    assert out["prediction"]["flat_fare_applied"] is True
    assert out["distance_km"] == pytest.approx(26.5)
    assert any("Ratecode 2" in w for w in out["warnings"])


def test_quality_and_reject_metrics_reads_verified_counts_and_container_fallback(tmp_path):
    # Test container fallback when /app/data is absent
    missing_silver = tmp_path / "missing_silver.json"
    missing_prep = tmp_path / "missing_prep.json"
    res = fetch_quality_and_reject_metrics(
        silver_manifest_path=missing_silver,
        ml_prep_report_path=missing_prep,
    )
    sc = res["metric_4_quality_and_rejects"]["silver_cleaning"]
    assert sc["raw_taxi_rows"] == 24083384
    assert sc["silver_valid_rows"] == 23886265
    assert sc["silver_rejected_rows"] == 197119
    assert sc["silver_reject_rate_pct"] == 0.82


def test_airflow_dag_module_parses_without_airflow_runtime():
    """Parse the DAG statically so the offline CI profile does not need Airflow installed."""
    import ast
    from pathlib import Path

    dag_path = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "nyc_taxi_elt_ml_dag.py"
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    constants = {
        target.id: node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert constants["DAG_ID"] == "nyc_taxi_medallion_elt_and_ml_v3"
    assert constants["SCHEDULE_INTERVAL"] == "@monthly"


def test_gold_v3_streamlit_page_renders_mapbox_address_flow_first(monkeypatch):
    from pathlib import Path
    import streamlit as st
    from streamlit.testing.v1 import AppTest
    from nyc_taxi.dashboard import address_page, gold_v3_page

    st.cache_data.clear()
    monkeypatch.setenv("FARE_MODEL_VERSION", "gold-v3")
    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "1")
    monkeypatch.setenv("FARE_ADDRESS_ALLOW_NETWORK", "1")
    monkeypatch.setenv("MAPBOX_ACCESS_TOKEN", "pk.synthetic_test_token")

    fake_zones = {
        **GOLD_V3_IDENTITY,
        "pickup_location_ids": [132, 161, 236],
        "dropoff_location_ids": [132, 161, 236],
        "zones": [
            {"location_id": 132, "borough": "Queens", "zone": "JFK Airport", "supported_pickup": True, "supported_dropoff": True},
            {"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center", "supported_pickup": True, "supported_dropoff": True},
            {"location_id": 236, "borough": "Manhattan", "zone": "Upper East Side North", "supported_pickup": True, "supported_dropoff": True},
        ],
    }
    monkeypatch.setattr(address_page, "get_gold_v3_metadata", lambda _url: fake_zones)
    monkeypatch.setattr(gold_v3_page, "_load_gold_v3_zones", lambda _url: fake_zones)
    monkeypatch.setattr(gold_v3_page, "_query_model_metrics", lambda: [])
    monkeypatch.setattr(gold_v3_page, "_query_prediction_summary", lambda: {"by_hour": [], "by_borough": [], "by_weather": []})
    monkeypatch.setattr(
        gold_v3_page,
        "_cached_monitoring_snapshot",
        lambda _url: {
            "etl": {
                "metric_1_job_status": {"loaded_jobs": 6, "total_jobs": 6, "success_rate_pct": 100.0},
                "metric_2_processing_time": {"total_load_seconds": 5184.2, "avg_job_seconds": 864.0},
                "metric_3_row_reconciliation": {"total_loaded_rows": 23886265, "row_delta": 0},
                "jobs": [],
            },
            "qual": fetch_quality_and_reject_metrics(),
            "srv": {
                "metric_5_model_and_serving": {
                    "serving_health": {"ready": True, "supported_pickup_zones": 258, "container_memory_mb": 688.0, "container_memory_limit_mb": 3072.0},
                    "dashboard_container_resources": {"memory_current_mb": 145.0, "memory_limit_mb": 1024.0},
                }
            },
        },
    )

    app_path = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"
    app = AppTest.from_file(str(app_path))
    app.session_state["dashboard_page"] = "Ước tính cước"
    app.run(timeout=15)

    assert not app.exception
    assert any("Ước tính cước từ địa chỉ" in t.value for t in app.title)
    assert any("1. Chọn điểm đón và điểm đến" in s.value for s in app.subheader)
    assert any("2. Chọn thời gian đón" in s.value for s in app.subheader)
    assert any("0.82%" in m.value for m in app.metric)

