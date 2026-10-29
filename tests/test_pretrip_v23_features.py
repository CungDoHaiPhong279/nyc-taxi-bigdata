"""Unit tests for V2.3 pre-trip contract, zone centroid Haversine distance, and minute-resolution cyclic time."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path

import pytest

from nyc_taxi.ml import pretrip_features as v22_features
from nyc_taxi.ml import pretrip_serving_contract as v22_contract
from nyc_taxi.ml.pretrip_v23_features import (
    CATEGORICAL_FEATURE_COLUMNS,
    CONTRACT_VERSION,
    FEATURE_COLUMNS,
    GEOMETRY_SHA256,
    MAX_BINS,
     MODEL_STATUS_ALLOWLIST,
    NUMERIC_FEATURE_COLUMNS,
    build_serving_features,
    build_training_features,
    create_submission,
    haversine_distance_km,
    load_calendar,
    load_contract,
    load_zone_centroids,
)


def request(local: str = "2026-06-19T10:30:00-04:00", **changes):
    result = create_submission(
        pickup_location_id=161,
        dropoff_location_id=236,
        now=datetime(2026, 6, 19, 14, 30, tzinfo=timezone.utc),
    )
    result["pickup_datetime"] = local
    result["fold"] = None
    parsed = datetime.fromisoformat(local)
    if parsed.tzinfo is not None:
        result["submitted_at"] = parsed.astimezone(timezone.utc).isoformat()
    result.update(changes)
    return result


def test_v22_pinned_files_remain_byte_identical():
    root = Path(__file__).resolve().parents[1]
    v22_builder_sha = hashlib.sha256((root / "src/nyc_taxi/ml/pretrip_features.py").read_bytes()).hexdigest()
    v22_contract_sha = hashlib.sha256((root / "config/pretrip_v2_contract.json").read_bytes()).hexdigest()
    assert v22_builder_sha == v22_contract.FEATURE_BUILDER_SHA256
    assert v22_contract_sha == "a1ddbe55f0128c20d8189d9f37b3b6172e90f20be71ae7a99300e21fb306add5"
    assert len(v22_features.FEATURE_COLUMNS) == 8


def test_v23_contract_schema_max_bins_and_status_evolution(tmp_path):
    contract = load_contract()
    assert contract["contract_version"] == CONTRACT_VERSION
    assert contract["max_bins"] == MAX_BINS == 264
    assert tuple(contract["numeric_features"]) == NUMERIC_FEATURE_COLUMNS
    assert tuple(contract["categorical_features"]) == CATEGORICAL_FEATURE_COLUMNS
    assert len(FEATURE_COLUMNS) == 10
    assert contract["geometry"]["sha256"] == GEOMETRY_SHA256
    assert contract["cohort_policy"]["sampling_fraction"] == 0.25
    assert set(contract["required_request_fields"]) == set(request())

    for valid_status in MODEL_STATUS_ALLOWLIST:
        modified = dict(contract, model_status=valid_status)
        path = tmp_path / f"contract_{valid_status}.json"
        path.write_text(json.dumps(modified), encoding="utf-8")
        assert load_contract(path)["model_status"] == valid_status

    invalid = dict(contract, max_bins=256)
    bad_path = tmp_path / "bad_bins.json"
    bad_path.write_text(json.dumps(invalid), encoding="utf-8")
    with pytest.raises(ValueError, match="v2.3 feature contract"):
        load_contract(bad_path)


def test_zone_centroids_cover_all_263_zones_and_compute_haversine_distance(tmp_path):
    centroids = load_zone_centroids()
    assert centroids.sha256 == GEOMETRY_SHA256
    assert set(centroids.centroids) == set(range(1, 264))

    lon_161, lat_161 = centroids.centroid(161)
    lon_236, lat_236 = centroids.centroid(236)
    assert -74.05 < lon_161 < -73.90 and 40.70 < lat_161 < 40.85
    assert -74.05 < lon_236 < -73.90 and 40.70 < lat_236 < 40.85

    d_midtown_ues = centroids.distance_km(161, 236)
    d_reverse = centroids.distance_km(236, 161)
    d_midtown_jfk = centroids.distance_km(161, 132)
    assert d_midtown_ues == pytest.approx(d_reverse, rel=1e-12)
    assert 2.5 < d_midtown_ues < 3.6
    assert 18.0 < d_midtown_jfk < 23.0
    assert centroids.distance_km(161, 161) == 0.0
    assert haversine_distance_km(lon_161, lat_161, lon_161, lat_161) == 0.0

    tampered = tmp_path / "tampered.geojson"
    tampered.write_text('{"type": "FeatureCollection", "features": []}', encoding="utf-8")
    with pytest.raises(ValueError, match="checksum"):
        load_zone_centroids(tampered)


def test_minute_resolution_cyclic_time_distinguishes_within_hour_minutes():
    on_the_hour = build_training_features(datetime(2026, 6, 19, 10, 0), 161, 236)
    half_past = build_training_features(datetime(2026, 6, 19, 10, 30), 161, 236)
    expected_angle = 2.0 * math.pi * (10.5 / 24.0)
    assert half_past["pickup_hour_sin"] == pytest.approx(math.sin(expected_angle))
    assert half_past["pickup_hour_cos"] == pytest.approx(math.cos(expected_angle))
    assert on_the_hour["pickup_hour_sin"] != pytest.approx(half_past["pickup_hour_sin"])
    assert on_the_hour["pickup_hour_cos"] != pytest.approx(half_past["pickup_hour_cos"])


def test_same_zone_and_inter_zone_features_and_train_serve_parity():
    inter = build_serving_features(request("2026-06-19T10:30:00-04:00", pickup_location_id=161, dropoff_location_id=236))
    expected_inter = build_training_features(datetime(2026, 6, 19, 10, 30), 161, 236)
    assert inter == expected_inter
    assert tuple(inter) == FEATURE_COLUMNS
    assert inter["is_same_zone"] == 0.0
    assert inter["zone_centroid_distance_km"] > 2.5

    intra = build_serving_features(request("2026-06-19T10:30:00-04:00", pickup_location_id=161, dropoff_location_id=161))
    assert intra["is_same_zone"] == 1.0
    assert intra["zone_centroid_distance_km"] == 0.0
    assert intra["pickup_zone"] == "161" and intra["dropoff_zone"] == "161"


@pytest.mark.parametrize("missing", [
    "pickup_location_id", "dropoff_location_id", "pickup_datetime", "timezone", "submission_id",
    "contract_version", "calendar_version", "calendar_sha256", "geometry_sha256", "tzdb_sha256",
    "submitted_at", "mode", "fold",
])
def test_v23_serving_requires_all_identity_and_geometry_pins(missing):
    partial = request()
    del partial[missing]
    with pytest.raises(ValueError, match="allowlist"):
        build_serving_features(partial)


def test_v23_serving_rejects_geometry_or_calendar_identity_drift():
    with pytest.raises(ValueError, match="identity mismatch"):
        build_serving_features(request(geometry_sha256="0" * 64))
    with pytest.raises(ValueError, match="identity mismatch"):
        build_serving_features(request(contract_version="pretrip-v2-zone-time-1"))
    assert len(load_calendar().holidays) == 34
