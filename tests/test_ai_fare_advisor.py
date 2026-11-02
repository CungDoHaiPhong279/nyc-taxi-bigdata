"""Unit and integration tests for the AI Smart Fare Advisor (`ai_fare_advisor.py`).

Covers:
1. Official NYC TLC Active Surcharge Engine (only surfaces applicable fees > 0 USD,
   verified against `analytics.fact_trip`: Manhattan <96th St, CBD <60th St, LGA +5.00 USD,
   and Airport Access Fee 1.75 USD on pickup only).
2. Zero unescaped `$` characters in Vietnamese labels/reasons/messages (prevents Streamlit KaTeX `undefined`).
3. Automatic NYC 24h Hourly Weather Lookup (live Open-Meteo parsing + offline seasonal fallback + TTL cache).
4. Mapbox Real Driving Distance Refinement (intra-zone & detour adjustment).
5. Counterfactual XAI & Nearest-Time-Window Savings Advisor (never suggests past hours in `now` or same-day mode).
6. Natural Language Ride Booking Parser (Vietnamese & English, standard & reversed preposition order).
7. 24h Horizon Feature Derivation (`derive_24h_horizon_features` with per-hour 24h weather).
8. End-to-end Streamlit UI rendering & 1-click suggested hour application.
"""

from __future__ import annotations

from datetime import date, datetime
import json
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from nyc_taxi.dashboard.ai_fare_advisor import (
    MANHATTAN_CBD_60TH_ZONE_IDS,
    MANHATTAN_CONGESTION_96TH_ZONE_IDS,
    TLC_AIRPORT_ACCESS_FEE_USD,
    TLC_CBD_CONGESTION_FEE_USD,
    TLC_LGA_AIRPORT_SURCHARGE_USD,
    TLC_MANHATTAN_CONGESTION_SURCHARGE_USD,
    TLC_MANDATORY_BASE_TAX_USD,
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


def test_active_tlc_surcharges_matches_fact_trip_empirical_rules():
    # Case 1: Outer-borough daytime weekday trip (Brooklyn 61 -> Brooklyn 66 at 10:30 AM)
    dt_morning = datetime(2025, 6, 10, 10, 30, tzinfo=NYC_TZ)  # Tuesday
    res_quiet = compute_active_tlc_surcharges(
        pickup_location_id=61,
        dropoff_location_id=66,
        pickup_borough="Brooklyn",
        dropoff_borough="Brooklyn",
        pickup_dt=dt_morning,
        flat_fare_applied=False,
    )
    codes_quiet = [item["code"] for item in res_quiet["active_surcharges"]]
    assert codes_quiet == ["mta_and_improvement"]
    assert res_quiet["total_surcharges_usd"] == pytest.approx(TLC_MANDATORY_BASE_TAX_USD)

    # Case 2: Upper Manhattan North of 96th St (Central Harlem 41 -> East Harlem 74 at 10:30 AM)
    # Neither congestion_surcharge (2.50) nor cbd_congestion_fee (0.75) applies north of 96th St!
    assert 41 not in MANHATTAN_CONGESTION_96TH_ZONE_IDS
    assert 74 not in MANHATTAN_CBD_60TH_ZONE_IDS
    res_harlem = compute_active_tlc_surcharges(
        pickup_location_id=41,
        dropoff_location_id=74,
        pickup_borough="Manhattan",
        dropoff_borough="Manhattan",
        pickup_dt=dt_morning,
        flat_fare_applied=False,
    )
    assert [i["code"] for i in res_harlem["active_surcharges"]] == ["mta_and_improvement"]
    assert res_harlem["total_surcharges_usd"] == pytest.approx(1.50)

    # Case 3: Upper East Side (236 -> 237, between 60th and 96th St) at 10:30 AM
    # Pays NY State Congestion (2.50) but NOT CBD <60th St fee (0.75)
    res_ues = compute_active_tlc_surcharges(
        pickup_location_id=236,
        dropoff_location_id=237,
        pickup_borough="Manhattan",
        dropoff_borough="Manhattan",
        pickup_dt=dt_morning,
        flat_fare_applied=False,
    )
    assert [i["code"] for i in res_ues["active_surcharges"]] == [
        "mta_and_improvement",
        "congestion_surcharge",
    ]
    assert res_ues["total_surcharges_usd"] == pytest.approx(4.00)

    # Case 4: Weekday Rush Hour (17:30) Midtown CBD (161 -> 236)
    # Includes MTA+Imp (1.50) + Rush Hour (2.50) + Manhattan <96th (2.50) + CBD <60th (0.75) = 7.25 USD
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
    assert codes_rush == [
        "mta_and_improvement",
        "rush_hour_surcharge",
        "congestion_surcharge",
        "cbd_congestion_fee",
    ]
    assert res_rush["total_surcharges_usd"] == pytest.approx(
        TLC_MANDATORY_BASE_TAX_USD
        + TLC_RUSH_HOUR_METered_USD
        + TLC_MANHATTAN_CONGESTION_SURCHARGE_USD
        + TLC_CBD_CONGESTION_FEE_USD
    )

    # Case 5: US Holiday (July 4th, Friday at 17:30) -> Rush Hour surcharge must NOT apply
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

    # Case 6: Times Square (230) -> JFK Airport (132) at 06:00 AM (Ratecode 2 Flat Fare = 70.00)
    # Dropoff at JFK does NOT pay 1.75 airport pickup fee! Total surcharges = 1.50 + 2.50 + 0.75 = 4.75 -> Total 74.75
    dt_6am = datetime(2025, 3, 12, 6, 0, tzinfo=NYC_TZ)
    res_to_jfk = compute_active_tlc_surcharges(
        pickup_location_id=230,
        dropoff_location_id=132,
        pickup_borough="Manhattan",
        dropoff_borough="Queens",
        pickup_dt=dt_6am,
        flat_fare_applied=True,
    )
    codes_to_jfk = [i["code"] for i in res_to_jfk["active_surcharges"]]
    assert "airport_fee" not in codes_to_jfk
    assert res_to_jfk["total_surcharges_usd"] == pytest.approx(4.75)

    # Case 7: Pickup at JFK Airport (132) -> Times Square (230) at 17:30 Rush Hour (Ratecode 2 Flat Fare = 70.00)
    # Includes 1.50 + 5.00 (JFK Rush Hour) + 2.50 + 0.75 + 1.75 (Airport pickup) = 11.50 -> Total 81.50
    res_from_jfk_rush = compute_active_tlc_surcharges(
        pickup_location_id=132,
        dropoff_location_id=230,
        pickup_borough="Queens",
        dropoff_borough="Manhattan",
        pickup_dt=dt_rush,
        flat_fare_applied=True,
    )
    codes_from_jfk = [i["code"] for i in res_from_jfk_rush["active_surcharges"]]
    assert "airport_fee" in codes_from_jfk
    assert res_from_jfk_rush["total_surcharges_usd"] == pytest.approx(
        1.50 + TLC_RUSH_HOUR_JFK_FLAT_USD + 2.50 + 0.75 + TLC_AIRPORT_ACCESS_FEE_USD
    )

    # Case 8: Overnight (22:15) from LGA Airport (138) to Midtown (161)
    # Includes 1.50 + 1.00 (Overnight) + 5.00 (LGA Surcharge) + 2.50 (Manhattan) + 0.75 (CBD) + 1.75 (Airport pickup) = 12.50
    dt_night = datetime(2025, 6, 10, 22, 15, tzinfo=NYC_TZ)
    res_lga = compute_active_tlc_surcharges(
        pickup_location_id=138,
        dropoff_location_id=161,
        pickup_borough="Queens",
        dropoff_borough="Manhattan",
        pickup_dt=dt_night,
        flat_fare_applied=False,
    )
    codes_lga = [item["code"] for item in res_lga["active_surcharges"]]
    assert "lga_airport_surcharge" in codes_lga
    assert "airport_fee" in codes_lga
    assert "overnight_surcharge" in codes_lga
    assert res_lga["total_surcharges_usd"] == pytest.approx(
        TLC_MANDATORY_BASE_TAX_USD
        + TLC_OVERNIGHT_SURCHARGE_USD
        + TLC_LGA_AIRPORT_SURCHARGE_USD
        + TLC_MANHATTAN_CONGESTION_SURCHARGE_USD
        + TLC_CBD_CONGESTION_FEE_USD
        + TLC_AIRPORT_ACCESS_FEE_USD
    )
    # Ensure zero raw '$' signs in any surcharge label or reason (prevents Streamlit KaTeX 'undefined')
    for item in res_lga["active_surcharges"]:
        assert "$" not in item["label_vi"]
        assert "$" not in item["reason_vi"]


def test_fetch_nyc_hourly_weather_live_24h_and_offline_fallback(monkeypatch):
    dt = datetime(2025, 6, 15, 18, 0, tzinfo=NYC_TZ)

    # 1. Offline mode returns deterministic seasonal fallback + 24h array
    offline_w = fetch_nyc_hourly_weather(dt, allow_network=False)
    assert offline_w["source"] == "seasonal-fallback"
    assert isinstance(offline_w["temperature_2m"], float)
    assert len(offline_w["hourly_24h"]) == 24
    assert len(offline_w["hourly_24h_compact"]) == 24

    # 2. Mocked live Open-Meteo response
    fake_payload = {
        "hourly": {
            "time": [f"2025-06-15T{h:02d}:00" for h in range(24)],
            "temperature_2m": [20.0 + h * 0.2 for h in range(24)],
            "precipitation": [4.5 if h == 18 else 0.0 for h in range(24)],
            "snowfall": [0.0 for _ in range(24)],
            "wind_speed_10m": [15.0 for _ in range(24)],
            "weather_code": [65 if h == 18 else 1 for h in range(24)],
        }
    }
    mock_resp = MagicMock()
    mock_resp.read.return_value = json.dumps(fake_payload).encode("utf-8")
    mock_resp.__enter__.return_value = mock_resp
    monkeypatch.setattr("nyc_taxi.dashboard.ai_fare_advisor.urlopen", lambda *a, **kw: mock_resp)

    live_w = fetch_nyc_hourly_weather(dt, allow_network=True)
    assert live_w["source"] == "open-meteo-live"
    assert live_w["precipitation"] == pytest.approx(4.5)
    assert "Mưa lớn" in live_w["condition_vi"]
    assert live_w["hourly_24h"][18]["precipitation"] == pytest.approx(4.5)
    assert live_w["hourly_24h"][10]["precipitation"] == pytest.approx(0.0)


def test_mapbox_distance_adjustment_refines_same_zone_and_detour():
    short_adj = compute_mapbox_distance_adjustment(
        metered_ml_usd=10.50,
        mapbox_distance_km=0.55,
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
    assert "$" not in (long_adj["explanation_vi"] or "")


def test_build_fare_explanation_and_nearest_better_time_advisor_never_suggests_past():
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

    # 1. When mode == 'now' at 17:30, advisor MUST ONLY suggest future hours (e.g. 20:30, +3h), never 15:30 (-2h in past)!
    advice_now = build_fare_explanation_and_advice(
        prediction=prediction,
        pickup_zone={"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center"},
        dropoff_zone={"location_id": 236, "borough": "Manhattan", "zone": "Upper East Side North"},
        pickup_dt=dt_rush,
        mapbox_distance_km=4.5,
        weather_info={"temperature_2m": 22.0, "precipitation": 1.2, "wind_speed_10m": 14.0, "condition_vi": "Mưa rào nhẹ", "source": "open-meteo-live"},
        mode="now",
        now_nyc=dt_rush,
    )
    assert advice_now["total_surcharges_usd"] == pytest.approx(7.25)
    nearest_now = advice_now["nearest_better_option"]
    assert nearest_now["has_better_nearby"] is True
    assert nearest_now["hour"] == 20
    assert nearest_now["offset_hours"] > 0
    assert "$" not in nearest_now["message_vi"]

    # 2. When mode == 'scheduled' for tomorrow (when 15:30 is in the future), advisor prefers 15:30 (-2h, closer than +3h)
    now_yesterday = datetime(2025, 6, 9, 12, 0, tzinfo=NYC_TZ)
    advice_sched = build_fare_explanation_and_advice(
        prediction=prediction,
        pickup_zone={"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center"},
        dropoff_zone={"location_id": 236, "borough": "Manhattan", "zone": "Upper East Side North"},
        pickup_dt=dt_rush,
        mapbox_distance_km=4.5,
        mode="scheduled",
        now_nyc=now_yesterday,
    )
    assert advice_sched["nearest_better_option"]["hour"] == 15


def test_parse_natural_language_booking_standard_and_reversed_order():
    now_ref = datetime(2026, 10, 31, 14, 0, tzinfo=NYC_TZ)

    # Standard order: "Từ A đi B"
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

    # Reversed order: "Đi B từ A"
    res_rev = parse_natural_language_booking(
        "Đi sân bay JFK từ Times Square lúc 7h rưỡi tối mai",
        now_nyc=now_ref,
    )
    assert res_rev["ok"] is True
    assert res_rev["pickup"]["location_id"] == 230
    assert res_rev["dropoff"]["location_id"] == 132
    assert res_rev["pickup_hour"] == 19
    assert res_rev["pickup_minute"] == 30

    # English AM/PM expression: "6pm"
    res_en = parse_natural_language_booking(
        "From Wall Street to LaGuardia at 6pm tomorrow",
        now_nyc=now_ref,
    )
    assert res_en["ok"] is True
    assert res_en["pickup"]["location_id"] == 87
    assert res_en["dropoff"]["location_id"] == 138
    assert res_en["pickup_hour"] == 18


def test_derive_24h_horizon_features_supports_24h_hourly_weather_array():
    dt = datetime(2025, 6, 15, 17, 30, tzinfo=NYC_TZ)
    compact_24h = [[15.0 + h * 0.5, 2.5 if h == 17 else 0.0, 0.0, 12.0] for h in range(24)]
    rows = derive_24h_horizon_features(
        pickup_location_id=161,
        dropoff_location_id=236,
        pickup_dt=dt,
        weather={
            "temperature_2m": 23.5,
            "precipitation": 2.5,
            "snowfall": 0.0,
            "wind_speed_10m": 12.0,
        },
        hourly_weather_24h=compact_24h,
    )
    assert len(rows) == 24
    assert rows[0]["temperature_2m"] == pytest.approx(15.0)
    assert rows[17]["precipitation"] == pytest.approx(2.5)
    assert rows[10]["precipitation"] == pytest.approx(0.0)


def test_gold_v3_address_page_auto_weather_and_smart_fare_advisor_receipt(monkeypatch):
    from streamlit.testing.v1 import AppTest
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.dashboard.address_component import new_address_state
    from nyc_taxi.dashboard.pretrip_prediction import GOLD_V3_IDENTITY
    from nyc_taxi.dashboard.route_fare import GoldV3RouteFareCoordinator

    monkeypatch.setenv("FARE_MODEL_VERSION", "gold-v3")
    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "1")
    monkeypatch.delenv("FARE_ADDRESS_ALLOW_NETWORK", raising=False)
    monkeypatch.setattr(address_page, "address_picker", lambda *_args, **_kw: None)
    monkeypatch.setattr(address_page, "interactive_booking_map", lambda **_kw: None)

    fake_zones = {
        **GOLD_V3_IDENTITY,
        "pickup_location_ids": [161, 236],
        "dropoff_location_ids": [161, 236],
        "zones": [
            {"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center", "supported_pickup": True, "supported_dropoff": True},
            {"location_id": 236, "borough": "Manhattan", "zone": "Upper East Side North", "supported_pickup": True, "supported_dropoff": True},
        ],
    }
    monkeypatch.setattr(address_page, "get_gold_v3_metadata", lambda _url: fake_zones)

    router = MagicMock()
    router.directions.return_value = {
        "distance_meters": 4500.0,
        "geometry": {"type": "LineString", "coordinates": [[-73.9787, 40.7587], [-73.9566, 40.7794]]},
    }
    geometry = MagicMock()
    geometry.resolve.side_effect = lambda lon, _lat: (
        {"location_id": 161, "borough": "Manhattan", "zone": "Midtown Center"}
        if lon < -73.965
        else {"location_id": 236, "borough": "Manhattan", "zone": "Upper East Side North"}
    )
    geometry.zone_feature.return_value = None

    predictor = MagicMock()
    predictor.predict.return_value = {
        **GOLD_V3_IDENTITY,
        "status": "supported",
        "unit": "USD",
        "fare_amount_usd": 18.50,
        "metered_ml_estimate_usd": 18.50,
        "m3_weather_estimate_usd": 18.50,
        "m3_no_weather_estimate_usd": 17.80,
        "baseline_fare_usd": 16.00,
        "zone_centroid_distance_km": 3.45,
        "is_same_zone": 0,
        "model_variant": "M3-w (GBT Full Weather + Centroid)",
        "ratecode_id": 1,
        "flat_fare_applied": False,
        "hourly_curve": [
            {
                "hour": h,
                "time_label": f"{h:02d}:00",
                "fare_amount_usd": 18.50 if 16 <= h <= 19 else 15.00,
                "metered_ml_usd": 18.50 if 16 <= h <= 19 else 15.00,
                "m3_weather_usd": 18.50 if 16 <= h <= 19 else 15.00,
                "m3_no_weather_usd": 17.80 if 16 <= h <= 19 else 14.50,
                "baseline_usd": 16.00,
            }
            for h in range(24)
        ],
        "request_seconds": 0.02,
        "unseen_categories": {"pickup": False, "dropoff": False},
        "warnings": [],
    }
    coordinator = GoldV3RouteFareCoordinator(
        router=router,
        geometry=geometry,
        predictor=predictor,
        metadata=fake_zones,
    )
    monkeypatch.setattr(address_page, "make_coordinator", lambda: coordinator)
    monkeypatch.setattr(address_page, "get_geometry", lambda: geometry)

    def main():
        from nyc_taxi.dashboard.address_page import render_page
        render_page()

    app = AppTest.from_function(main)
    for slot, lon in (("pickup", -73.9787), ("dropoff", -73.9566)):
        st_slot = new_address_state()
        st_slot["selected"] = {"label": f"Pin {slot}", "longitude": lon, "latitude": 40.76}
        app.session_state["address_" + slot] = st_slot

    app.run(timeout=15)
    assert not app.exception
    app.button(key="address_estimate_submit").click().run(timeout=15)
    assert not app.exception

    # Verify automatic weather fields + 24h weather array were sent to predictor without checking the manual weather box
    called_kwargs = predictor.predict.call_args.kwargs
    assert "temperature_2m" in called_kwargs
    assert "precipitation" in called_kwargs
    assert "hourly_weather_24h" in called_kwargs
    assert len(called_kwargs["hourly_weather_24h"]) == 24

    # Verify the Smart Fare Advisor receipt rendered with zero KaTeX '$' corruption
    md_joined = " ".join(m.value for m in app.markdown)
    assert "Bảng kê Chi tiết Giá cước & Phụ phí Chính thức NYC TLC" in md_joined
    assert "Phụ phí Tắc đường Bang NY" in md_joined
    assert "undefined" not in md_joined



def _real_tlc_zones():
    from nyc_taxi.dashboard.geometry_assets import load_geometry_bundle

    geom = load_geometry_bundle()
    rows = []
    for lid in geom.location_ids:
        feature = geom.zone_feature(lid) or {}
        props = feature.get("properties", {}) if isinstance(feature, dict) else {}
        rows.append({"location_id": lid, "zone": props.get("zone", ""), "borough": props.get("borough", "")})
    return rows


def test_parse_natural_language_booking_matches_real_tlc_zone_names_without_crash():
    """Regression: 'ZoneCentroidLookup' object has no attribute 'coords' when a TLC zone name matched."""
    now_ref = datetime(2026, 11, 1, 12, 0, tzinfo=NYC_TZ)
    zones = _real_tlc_zones()
    assert any(z["zone"] == "Greenpoint" for z in zones)

    # Two real TLC zone names -> both resolved to WGS84 centroids inside NYC
    res = parse_natural_language_booking("Đặt xe từ Astoria đi Greenpoint lúc 9h sáng mai", now_nyc=now_ref, zones=zones)
    assert res["ok"] is True
    assert res["pickup"]["zone"] == "Astoria"
    assert res["dropoff"]["zone"] == "Greenpoint"
    for loc in (res["pickup"], res["dropoff"]):
        assert -74.35 <= loc["longitude"] <= -73.65
        assert 40.45 <= loc["latitude"] <= 40.95

    # Unknown place + 'đi Greenpoint' -> Greenpoint is the DROPOFF, clear partial-match message, no exception
    partial = parse_natural_language_booking("đặt xe từ queens college đi greenpoint", now_nyc=now_ref, zones=zones)
    assert partial["ok"] is False
    assert partial["pickup"] is None
    assert partial["dropoff"]["zone"] == "Greenpoint"
    assert "Điểm đến" in partial["error"] and "$" not in partial["error"]


def test_nlp_vietnamese_toi_pronoun_or_preposition_does_not_force_pm():
    now_ref = datetime(2026, 11, 2, 10, 0, tzinfo=NYC_TZ)
    res = parse_natural_language_booking(
        "Đặt xe cho tôi từ Times Square tới JFK lúc 6h sáng mai",
        now_nyc=now_ref,
    )
    assert res["ok"] is True
    assert res["pickup_hour"] == 6, f"Expected 6 AM, got {res['pickup_hour']}"


def test_nlp_prefix_zone_names_do_not_double_match_single_mention():
    now_ref = datetime(2026, 11, 2, 10, 0, tzinfo=NYC_TZ)
    zones = [
        {"location_id": 12, "zone": "Battery Park", "borough": "Manhattan"},
        {"location_id": 13, "zone": "Battery Park City", "borough": "Manhattan"},
        {"location_id": 132, "zone": "JFK Airport", "borough": "Queens"},
    ]
    res = parse_natural_language_booking(
        "Từ Battery Park City đi JFK lúc 9h sáng mai",
        now_nyc=now_ref,
        zones=zones,
    )
    assert res["ok"] is True
    assert res["pickup"]["location_id"] == 13
    assert res["dropoff"]["location_id"] == 132


def test_readme_b0_global_metrics_and_cli_flags_are_accurate():
    from pathlib import Path

    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")
    assert "python scripts/build_bronze.py --all" in readme
    assert "python scripts/load_postgres.py --execute" in readme
    assert "| **1. Trung vị toàn cục (`B0_global`)** | Thống kê cơ sở đơn giản | Không | `7,90` | `13,33` | `-13,22%` | `4,20` |" in readme
