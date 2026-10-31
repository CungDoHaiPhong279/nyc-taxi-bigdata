"""Unit tests for the AI Smart Fare Advisor, Automatic NYC Weather, Active TLC Surcharges,
Nearest Better Time Window Advisor, and Natural Language Quick-Book Parser.
"""

from __future__ import annotations

from datetime import date, datetime
import json
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

from nyc_taxi.dashboard.ai_fare_advisor import (
    TLC_AIRPORT_ACCESS_FEE_USD,
    TLC_MANDATORY_BASE_TAX_USD,
    TLC_MANHATTAN_CONGESTION_SURCHARGE_USD,
    TLC_OVERNIGHT_SURCHARGE_USD,
    TLC_RUSH_HOUR_JFK_FLAT_USD,
    TLC_RUSH_HOUR_METered_USD,
    build_fare_explanation_and_advice,
    compute_active_tlc_surcharges,
    compute_mapbox_distance_adjustment,
    fetch_nyc_hourly_weather,
    parse_natural_language_booking,
)
from nyc_taxi.ml.gold_v3_inference import derive_24h_horizon_features


NYC_TZ = ZoneInfo("America/New_York")


def test_active_tlc_surcharges_only_returns_applicable_fees():
    # Case 1: Queens -> Brooklyn at 10:30 AM on a Tuesday (no rush hour, no night, no Manhattan, no airport)
    dt_morning = datetime(2025, 6, 10, 10, 30, tzinfo=NYC_TZ)
    res_quiet = compute_active_tlc_surcharges(
        pickup_location_id=7,
        dropoff_location_id=33,
        pickup_borough="Queens",
        dropoff_borough="Brooklyn",
        pickup_dt=dt_morning,
        flat_fare_applied=False,
    )
    codes_quiet = [item["code"] for item in res_quiet["active_surcharges"]]
    assert codes_quiet == ["mta_and_improvement"]
    assert res_quiet["total_surcharges_usd"] == pytest.approx(TLC_MANDATORY_BASE_TAX_USD)
    assert res_quiet["has_rush_hour_surcharge"] is False
    assert res_quiet["has_overnight_surcharge"] is False

    # Case 2: Weekday Rush Hour (17:30) in Manhattan -> includes Rush Hour ($2.50) + Manhattan Congestion ($2.50)
    dt_rush = datetime(2025, 6, 10, 17, 30, tzinfo=NYC_TZ)
    res_rush = compute_active_tlc_surcharges(
        pickup_location_id=161,
        dropoff_location_id=236,
        pickup_borough="Manhattan",
        dropoff_borough="Manhattan",
        pickup_dt=dt_rush,
        flat_fare_applied=False,
    )
    codes_rush = [item["code"] for item in res_rush["active_surcharges"]]
    assert "rush_hour_surcharge" in codes_rush
    assert "congestion_surcharge" in codes_rush
    assert "overnight_surcharge" not in codes_rush
    assert "airport_fee" not in codes_rush
    assert res_rush["total_surcharges_usd"] == pytest.approx(
        TLC_MANDATORY_BASE_TAX_USD + TLC_RUSH_HOUR_METered_USD + TLC_MANHATTAN_CONGESTION_SURCHARGE_USD
    )

    # Case 3: US Holiday (July 4th, Friday at 17:30) -> Rush Hour surcharge must NOT apply on legal holiday
    dt_holiday = datetime(2025, 7, 4, 17, 30, tzinfo=NYC_TZ)
    res_hol = compute_active_tlc_surcharges(
        pickup_location_id=161,
        dropoff_location_id=236,
        pickup_borough="Manhattan",
        dropoff_borough="Manhattan",
        pickup_dt=dt_holiday,
        flat_fare_applied=False,
    )
    assert res_hol["has_rush_hour_surcharge"] is False

    # Case 4: Overnight (22:15) from LGA Airport (138) to Manhattan (161)
    dt_night = datetime(2025, 6, 10, 22, 15, tzinfo=NYC_TZ)
    res_night = compute_active_tlc_surcharges(
        pickup_location_id=138,
        dropoff_location_id=161,
        pickup_borough="Queens",
        dropoff_borough="Manhattan",
        pickup_dt=dt_night,
        flat_fare_applied=False,
    )
    codes_night = [item["code"] for item in res_night["active_surcharges"]]
    assert "overnight_surcharge" in codes_night
    assert "congestion_surcharge" in codes_night
    assert "airport_fee" in codes_night
    assert res_night["total_surcharges_usd"] == pytest.approx(
        TLC_MANDATORY_BASE_TAX_USD
        + TLC_OVERNIGHT_SURCHARGE_USD
        + TLC_MANHATTAN_CONGESTION_SURCHARGE_USD
        + TLC_AIRPORT_ACCESS_FEE_USD
    )

    # Case 5: JFK <-> Manhattan Flat Fare during weekday rush hour -> $5.00 rush hour surcharge, no overnight surcharge
    res_jfk_rush = compute_active_tlc_surcharges(
        pickup_location_id=132,
        dropoff_location_id=161,
        pickup_borough="Queens",
        dropoff_borough="Manhattan",
        pickup_dt=dt_rush,
        flat_fare_applied=True,
    )
    rush_item = next(i for i in res_jfk_rush["active_surcharges"] if i["code"] == "rush_hour_surcharge")
    assert rush_item["amount_usd"] == pytest.approx(TLC_RUSH_HOUR_JFK_FLAT_USD)


def test_fetch_nyc_hourly_weather_live_and_offline_fallback():
    dt = datetime(2026, 11, 2, 6, 0, tzinfo=NYC_TZ)

    # Offline fallback works deterministically without network
    offline_w = fetch_nyc_hourly_weather(dt, allow_network=False)
    assert offline_w["source"] == "seasonal-fallback"
    assert isinstance(offline_w["temperature_2m"], float)
    assert offline_w["precipitation"] == 0.0
    assert "condition_vi" in offline_w

    # Mocked Open-Meteo response extracts exact hour index (hour 6)
    fake_payload = {
        "hourly": {
            "time": [f"2026-11-02T{h:02d}:00" for h in range(24)],
            "temperature_2m": [10.0 + h * 0.5 for h in range(24)],
            "precipitation": [2.4 if h == 6 else 0.0 for h in range(24)],
            "snowfall": [0.0 for _ in range(24)],
            "wind_speed_10m": [18.5 for _ in range(24)],
            "weather_code": [61 if h == 6 else 1 for h in range(24)],
        }
    }
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(fake_payload).encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp

    with patch("nyc_taxi.dashboard.ai_fare_advisor.urlopen", return_value=mock_resp):
        live_w = fetch_nyc_hourly_weather(dt, allow_network=True)
    assert live_w["source"] == "open-meteo-live"
    assert live_w["temperature_2m"] == pytest.approx(13.0)
    assert live_w["precipitation"] == pytest.approx(2.4)
    assert live_w["wind_speed_10m"] == pytest.approx(18.5)
    assert "Mưa" in live_w["condition_vi"]


def test_mapbox_distance_adjustment_refines_same_zone_and_detour():
    # Same-zone trip with short 0.4 km distance vs long 3.8 km distance
    short_adj = compute_mapbox_distance_adjustment(
        metered_ml_usd=10.50,
        mapbox_distance_km=0.40,
        zone_centroid_distance_km=0.0,
        is_same_zone=True,
        flat_fare_applied=False,
    )
    long_adj = compute_mapbox_distance_adjustment(
        metered_ml_usd=10.50,
        mapbox_distance_km=3.80,
        zone_centroid_distance_km=0.0,
        is_same_zone=True,
        flat_fare_applied=False,
    )
    assert short_adj["adjusted_metered_usd"] < 10.50
    assert long_adj["adjusted_metered_usd"] > 10.50


def test_build_fare_explanation_and_nearest_better_time_advisor():
    dt_rush = datetime(2025, 6, 10, 17, 30, tzinfo=NYC_TZ)  # Tuesday 17:30 Rush Hour
    hourly_curve = [
        {
            "hour": h,
            "time_label": f"{h:02d}:30",
            "fare_amount_usd": 20.0 if 16 <= h <= 19 else 16.5,
            "metered_ml_usd": 20.0 if 16 <= h <= 19 else 16.5,
            "m3_weather_usd": 20.0 if 16 <= h <= 19 else 16.5,
            "m3_no_weather_usd": 19.0 if 16 <= h <= 19 else 15.8,
            "baseline_usd": 17.0,
        }
        for h in range(24)
    ]
    prediction = {
        "fare_amount_usd": 20.0,
        "metered_ml_estimate_usd": 20.0,
        "m3_weather_estimate_usd": 20.0,
        "m3_no_weather_estimate_usd": 19.0,
        "baseline_fare_usd": 17.0,
        "zone_centroid_distance_km": 3.5,
        "is_same_zone": 0,
        "flat_fare_applied": False,
        "hourly_curve": hourly_curve,
    }
    advice = build_fare_explanation_and_advice(
        prediction=prediction,
        pickup_zone={"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center"},
        dropoff_zone={"location_id": 236, "borough": "Manhattan", "zone": "Upper East Side North"},
        pickup_dt=dt_rush,
        mapbox_distance_km=4.5,
        weather_info={"temperature_2m": 22.0, "precipitation": 1.2, "wind_speed_10m": 14.0, "condition_vi": "Mưa rào nhẹ", "source": "open-meteo-live"},
        mode="scheduled",
    )

    # At 17:30 in Manhattan: surcharges = $1.50 (MTA+Imp) + $2.50 (Rush Hour) + $2.50 (Manhattan) = $6.50
    assert advice["total_surcharges_usd"] == pytest.approx(6.50)
    assert advice["total_estimated_fare_usd"] == pytest.approx(advice["metered_fare_usd"] + 6.50)
    assert advice["xai_components"]["weather_delta_usd"] == pytest.approx(1.0)

    # Nearest better option should recommend 15:30 (-2h) or 20:30 (+3h) where Rush Hour ends and metered fare drops!
    nearest = advice["nearest_better_option"]
    assert nearest["has_better_nearby"] is True
    assert nearest["hour"] in (15, 20)
    assert nearest["savings_usd"] >= 4.0
    assert "Phụ phí Giờ cao điểm" in nearest["message_vi"]


def test_parse_natural_language_booking_vietnamese_and_english():
    now_ref = datetime(2026, 10, 31, 14, 0, tzinfo=NYC_TZ)

    res1 = parse_natural_language_booking(
        "Đặt xe từ Times Square đi sân bay JFK lúc 6h sáng mai",
        now_nyc=now_ref,
    )
    assert res1["ok"] is True
    assert res1["pickup"]["location_id"] == 230
    assert res1["dropoff"]["location_id"] == 132
    assert res1["mode"] == "scheduled"
    assert res1["pickup_date"] == date(2026, 11, 1)
    assert res1["pickup_hour"] == 6
    assert res1["pickup_minute"] == 0

    res2 = parse_natural_language_booking(
        "Đi từ Công viên Trung tâm về Phố Wall lúc 5:30 chiều nay",
        now_nyc=now_ref,
    )
    assert res2["ok"] is True
    assert res2["pickup"]["location_id"] == 236
    assert res2["dropoff"]["location_id"] == 87
    assert res2["pickup_hour"] == 17
    assert res2["pickup_minute"] == 30


def test_derive_24h_horizon_features_returns_24_valid_rows():
    dt = datetime(2025, 6, 15, 17, 30, tzinfo=NYC_TZ)
    rows = derive_24h_horizon_features(
        pickup_location_id=161,
        dropoff_location_id=236,
        pickup_dt=dt,
        weather={
            "temperature_2m": 24.0,
            "precipitation": 0.5,
            "snowfall": 0.0,
            "wind_speed_10m": 10.0,
        },
    )
    assert len(rows) == 24
    assert all(r["temperature_2m"] == 24.0 for r in rows)
    assert rows[0]["pickup_hour_sin"] != rows[12]["pickup_hour_sin"]


def test_gold_v3_address_page_auto_weather_and_smart_fare_advisor_receipt(monkeypatch):
    from unittest.mock import Mock
    from streamlit.testing.v1 import AppTest
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.address_component import new_address_state
    from nyc_taxi.dashboard.pretrip_prediction import GOLD_V3_IDENTITY
    from nyc_taxi.dashboard.route_fare import GoldV3RouteFareCoordinator

    monkeypatch.setenv("FARE_MODEL_VERSION", "gold-v3")
    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "1")
    monkeypatch.delenv("FARE_ADDRESS_ALLOW_NETWORK", raising=False)
    monkeypatch.setattr(address_page, "address_picker", lambda *_a, **_kw: None)
    monkeypatch.setattr(address_page, "interactive_booking_map", lambda **_kw: None)

    fake_meta = {
        **GOLD_V3_IDENTITY,
        "pickup_location_ids": [161, 236],
        "dropoff_location_ids": [161, 236],
        "zones": [
            {"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center", "supported_pickup": True, "supported_dropoff": True},
            {"location_id": 236, "borough": "Manhattan", "zone": "Upper East Side North", "supported_pickup": True, "supported_dropoff": True},
        ],
    }
    monkeypatch.setattr(address_page, "get_gold_v3_metadata", lambda _url: fake_meta)

    router, geometry, predictor = Mock(), Mock(), Mock()
    router.directions.return_value = {
        "distance_meters": 3800.0,
        "geometry": {"type": "LineString", "coordinates": [[-73.98, 40.75], [-73.95, 40.78]]},
    }
    geometry.resolve.side_effect = lambda lon, _lat: (
        {"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center"}
        if lon < -73.97
        else {"location_id": 236, "borough": "Manhattan", "zone": "Upper East Side North"}
    )
    geometry.zone_feature.return_value = None

    def fake_predict(**req):
        # Verify automatic weather was populated without manual user input
        assert "temperature_2m" in req
        assert "precipitation" in req
        return {
            **GOLD_V3_IDENTITY,
            "status": "supported",
            "unit": "USD",
            "fare_amount_usd": 18.50,
            "metered_ml_estimate_usd": 18.50,
            "m3_weather_estimate_usd": 18.50,
            "m3_no_weather_estimate_usd": 17.80,
            "baseline_fare_usd": 16.00,
            "zone_centroid_distance_km": 3.1,
            "is_same_zone": 0,
            "model_variant": "M3-w (GBT Full Weather + Centroid)",
            "ratecode_id": 1,
            "flat_fare_applied": False,
            "request_seconds": 0.02,
            "unseen_categories": {"pickup": False, "dropoff": False},
            "warnings": [],
        }

    predictor.predict.side_effect = fake_predict
    coordinator = GoldV3RouteFareCoordinator(router=router, geometry=geometry, predictor=predictor, metadata=fake_meta)
    monkeypatch.setattr(address_page, "make_coordinator", lambda: coordinator)
    monkeypatch.setattr(address_page, "get_geometry", lambda: geometry)

    def main():
        from nyc_taxi.dashboard.address_page import render_page
        render_page()

    app = AppTest.from_function(main)
    for slot, lon in (("pickup", -73.98), ("dropoff", -73.95)):
        state = new_address_state()
        state["selected"] = {"label": "Synthetic " + slot, "longitude": lon, "latitude": 40.75}
        app.session_state["address_" + slot] = state

    app.run(timeout=15)
    assert not app.exception
    app.button(key="address_estimate_submit").click().run(timeout=15)
    assert not app.exception and not app.error
    predictor.predict.assert_called_once()
    all_md = " ".join(m.value for m in app.markdown)
    assert "Bảng kê Chi tiết Giá cước & Phụ phí Chính thức NYC TLC" in all_md
    assert "Phụ phí Tắc đường Khu vực Manhattan" in all_md

