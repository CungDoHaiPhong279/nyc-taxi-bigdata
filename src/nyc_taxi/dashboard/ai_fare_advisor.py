"""AI Smart Fare Advisor for NYC Yellow Taxi Ride Booking.

Provides:
1. Automatic NYC 24-Hour Weather Lookup (Open-Meteo Live Forecast + Historical Archive
   with deterministic seasonal fallback & TTL caching so M3-w is always active).
2. Official NYC TLC Active Surcharge Engine (only surfaces surcharges that actually apply
   to the specific ride, verified against 23.88M trips in `analytics.fact_trip` & `nyc.gov`).
3. Mapbox Real Driving Distance Refinement (smoothly adjusts intra-zone & detour routes).
4. Counterfactual Explainable AI (XAI) & Proactive Nearest-Time-Window Savings Advisor.
5. Natural Language Ride Booking Parser (Vietnamese & English AI Quick-Book with role detection).
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
import json
import math
import re
import time as _time
import unicodedata
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from nyc_taxi.dashboard.interactive_map import NYC_POPULAR_PRESETS
from nyc_taxi.ml.pretrip_v23_features import load_calendar, load_zone_centroids


NYC_TZ = ZoneInfo("America/New_York")
NYC_LAT = 40.7128
NYC_LON = -74.0060

FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_HOURLY_FIELDS = "temperature_2m,precipitation,snowfall,wind_speed_10m,weather_code"

# Official NYC TLC Regulated Surcharges & Fees (https://www.nyc.gov/site/tlc/passengers/taxi-fare.page)
# Empirically verified against 23,886,265 real 2025 trips in `analytics.fact_trip`.
TLC_INITIAL_CHARGE_USD = 3.00
TLC_MILEAGE_UNIT_USD = 0.70  # per 1/5 mile (0.2 mile) or per 60s in slow traffic
TLC_PER_MILE_RATE_USD = 3.50  # $0.70 * 5 = $3.50 per mile
TLC_MTA_STATE_TAX_USD = 0.50
TLC_IMPROVEMENT_SURCHARGE_USD = 1.00
TLC_MANDATORY_BASE_TAX_USD = TLC_MTA_STATE_TAX_USD + TLC_IMPROVEMENT_SURCHARGE_USD  # 1.50 USD
TLC_RUSH_HOUR_METered_USD = 2.50  # 16:00 - 20:00 Mon-Fri (excluding legal holidays)
TLC_RUSH_HOUR_JFK_FLAT_USD = 5.00  # 16:00 - 20:00 Mon-Fri on JFK Ratecode 2 flat fare
TLC_OVERNIGHT_SURCHARGE_USD = 1.00  # 20:00 - 06:00 daily (metered trips)
TLC_MANHATTAN_CONGESTION_SURCHARGE_USD = 2.50  # Trips entering/leaving/within Manhattan south of 96th St
TLC_CBD_CONGESTION_FEE_USD = 0.75  # MTA Congestion Relief Zone toll south of 60th St (effective Jan 5, 2025)
TLC_AIRPORT_ACCESS_FEE_USD = 1.75  # PANYNJ airport pickup access fee at JFK (132) & LGA (138)
TLC_LGA_AIRPORT_SURCHARGE_USD = 5.00  # Mandatory LaGuardia Airport surcharge (pickup or dropoff at Zone 138)

# Empirically verified Manhattan Taxi Zones south of 96th Street (`congestion_surcharge = 2.50` in `analytics.fact_trip`).
# Excludes Upper Manhattan north of 96th St (Harlem, Morningside Heights, Manhattan Valley, Washington Heights, Inwood,
# Marble Hill, Randalls/Roosevelt Island: Zones 24, 41, 42, 74, 75, 116, 120, 127, 128, 151, 152, 153, 166, 194, 202, 243, 244).
MANHATTAN_CONGESTION_96TH_ZONE_IDS: frozenset[int] = frozenset(
    {
        4, 12, 13, 43, 45, 48, 50, 68, 79, 87, 88, 90, 100, 103, 104, 105,
        107, 113, 114, 125, 137, 140, 141, 142, 143, 144, 148, 158, 161, 162,
        163, 164, 170, 186, 209, 211, 224, 229, 230, 231, 232, 233, 234, 236,
        237, 238, 239, 246, 249, 261, 262, 263,
    }
)

# Empirically verified Manhattan CBD Taxi Zones south of 60th Street (`cbd_congestion_fee = 0.75` in `analytics.fact_trip`).
# Upper East Side (140, 141, 236, 237, 262, 263), Upper West Side / Lincoln Sq (142, 143, 238, 239) and Central Park (43)
# lie between 60th St and 96th St, so they pay the 2.50 USD NY State Congestion Surcharge but 0.00 USD CBD fee when not crossing <60th St.
MANHATTAN_CBD_60TH_ZONE_IDS: frozenset[int] = frozenset(
    {
        4, 12, 13, 45, 48, 50, 68, 79, 87, 88, 90, 100, 107, 113, 114, 125,
        137, 144, 148, 158, 161, 162, 163, 164, 170, 186, 209, 211, 224, 229,
        230, 231, 232, 233, 234, 246, 249, 261,
    }
)

# Typical NYC monthly baseline weather (°C, wind km/h) used as fast offline fallback
_SEASONAL_NYC_TEMP_C = {
    1: 1.5,
    2: 3.0,
    3: 7.5,
    4: 13.0,
    5: 18.5,
    6: 23.5,
    7: 26.5,
    8: 25.5,
    9: 21.5,
    10: 15.5,
    11: 9.5,
    12: 4.0,
}

# Bounded TTL cache for daily 24h Open-Meteo responses: key -> (monotonic_ts, list_of_24_hour_dicts, source)
_DAILY_WEATHER_CACHE: dict[str, tuple[float, list[dict[str, Any]], str]] = {}
_WEATHER_CACHE_TTL_SECONDS = 600.0


def describe_wmo_weather_vi(
    weather_code: int | None,
    precipitation: float,
    snowfall: float,
    temperature_2m: float,
) -> str:
    """Translate WMO weather code and precipitation into natural Vietnamese."""
    if snowfall > 0.1 or (weather_code in (71, 73, 75, 77, 85, 86)):
        return f"Tuyết rơi ({snowfall:.1f} cm/h)"
    if precipitation >= 4.0 or weather_code in (65, 82, 95, 96, 99):
        return f"Mưa lớn ({precipitation:.1f} mm/h)"
    if precipitation >= 0.8 or weather_code in (61, 63, 80, 81):
        return f"Mưa rào nhẹ ({precipitation:.1f} mm/h)"
    if precipitation > 0.0 or weather_code in (51, 53, 55):
        return f"Mưa phùn ({precipitation:.1f} mm/h)"
    if weather_code in (45, 48):
        return "Sương mù nhẹ"
    if weather_code in (2, 3):
        return "Nhiều mây, khô ráo"
    if weather_code == 1:
        return "Ít mây, tạnh ráo"
    if temperature_2m <= 2.0:
        return "Trời quang, rét đậm"
    return "Trời quang đãng, khô ráo"


def _seasonal_hour_point(month: int, hour: int) -> dict[str, Any]:
    base_temp = _SEASONAL_NYC_TEMP_C.get(month, 16.0)
    # Diurnal temperature wave: coolest at 05:00, warmest at 15:00
    hour_offset = -3.0 * math.cos(2.0 * math.pi * (hour - 5) / 24.0)
    temp_c = round(base_temp + hour_offset, 1)
    wind_kmh = round(11.0 + 3.0 * math.sin(2.0 * math.pi * hour / 24.0), 1)
    return {
        "hour": int(hour),
        "temperature_2m": temp_c,
        "precipitation": 0.0,
        "snowfall": 0.0,
        "wind_speed_10m": wind_kmh,
        "weather_code": 1,
        "condition_vi": describe_wmo_weather_vi(1, 0.0, 0.0, temp_c),
    }


def _seasonal_fallback_weather(target_dt: datetime) -> dict[str, Any]:
    """Deterministic NYC 24-hour weather profile when offline or outside forecast horizon."""
    hourly_24h = [_seasonal_hour_point(target_dt.month, h) for h in range(24)]
    chosen = dict(hourly_24h[int(target_dt.hour) % 24])
    chosen["source"] = "seasonal-fallback"
    chosen["hourly_24h"] = hourly_24h
    chosen["hourly_24h_compact"] = [
        [
            float(pt["temperature_2m"]),
            float(pt["precipitation"]),
            float(pt["snowfall"]),
            float(pt["wind_speed_10m"]),
        ]
        for pt in hourly_24h
    ]
    return chosen


def fetch_nyc_hourly_weather(
    target_dt: datetime,
    *,
    allow_network: bool = True,
    timeout: float = 2.5,
) -> dict[str, Any]:
    """Automatically fetch accurate NYC weather for the target pickup date & all 24 hours.

    Uses Open-Meteo Forecast API for current/upcoming dates (and recent past) or
    Archive ERA5 API for historical dates, falling back cleanly if offline.
    """
    if target_dt.tzinfo is not None:
        local_dt = target_dt.astimezone(NYC_TZ)
    else:
        local_dt = target_dt.replace(tzinfo=NYC_TZ)

    target_date_str = local_dt.date().isoformat()
    target_hour = int(local_dt.hour) % 24

    if not allow_network:
        return _seasonal_fallback_weather(local_dt)

    now_mono = _time.monotonic()
    cached = _DAILY_WEATHER_CACHE.get(target_date_str)
    if cached is not None and (now_mono - cached[0]) < _WEATHER_CACHE_TTL_SECONDS:
        hourly_24h, src = cached[1], cached[2]
        chosen = dict(hourly_24h[target_hour])
        chosen["source"] = src
        chosen["hourly_24h"] = hourly_24h
        chosen["hourly_24h_compact"] = [
            [
                float(pt["temperature_2m"]),
                float(pt["precipitation"]),
                float(pt["snowfall"]),
                float(pt["wind_speed_10m"]),
            ]
            for pt in hourly_24h
        ]
        return chosen

    today_nyc = datetime.now(NYC_TZ).date()
    days_diff = (local_dt.date() - today_nyc).days

    try:
        if -7 <= days_diff <= 15:
            params = {
                "latitude": f"{NYC_LAT}",
                "longitude": f"{NYC_LON}",
                "hourly": WEATHER_HOURLY_FIELDS,
                "timezone": "America/New_York",
                "start_date": target_date_str,
                "end_date": target_date_str,
            }
            url = f"{FORECAST_API_URL}?{urlencode(params)}"
        elif days_diff < -7:
            params = {
                "latitude": f"{NYC_LAT}",
                "longitude": f"{NYC_LON}",
                "hourly": WEATHER_HOURLY_FIELDS,
                "timezone": "America/New_York",
                "start_date": target_date_str,
                "end_date": target_date_str,
            }
            url = f"{ARCHIVE_API_URL}?{urlencode(params)}"
        else:
            return _seasonal_fallback_weather(local_dt)

        req = Request(url, headers={"Accept": "application/json", "User-Agent": "NYCTaxiFareAdvisor/1.0"})
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        if not times:
            return _seasonal_fallback_weather(local_dt)

        def _val_at(key: str, idx: int, default: float = 0.0) -> float:
            arr = hourly.get(key) or []
            if idx < len(arr) and arr[idx] is not None:
                return round(float(arr[idx]), 2)
            return default

        w_code_arr = hourly.get("weather_code") or []
        hourly_24h: list[dict[str, Any]] = []
        for h in range(24):
            idx = min(h, len(times) - 1)
            fallback_pt = _seasonal_hour_point(local_dt.month, h)
            t_c = _val_at("temperature_2m", idx, fallback_pt["temperature_2m"])
            pr = max(0.0, _val_at("precipitation", idx, 0.0))
            sn = max(0.0, _val_at("snowfall", idx, 0.0))
            wi = max(0.0, _val_at("wind_speed_10m", idx, fallback_pt["wind_speed_10m"]))
            wc = int(w_code_arr[idx]) if (idx < len(w_code_arr) and w_code_arr[idx] is not None) else 0
            hourly_24h.append(
                {
                    "hour": h,
                    "temperature_2m": t_c,
                    "precipitation": pr,
                    "snowfall": sn,
                    "wind_speed_10m": wi,
                    "weather_code": wc,
                    "condition_vi": describe_wmo_weather_vi(wc, pr, sn, t_c),
                }
            )

        if len(_DAILY_WEATHER_CACHE) >= 32:
            _DAILY_WEATHER_CACHE.clear()
        _DAILY_WEATHER_CACHE[target_date_str] = (now_mono, hourly_24h, "open-meteo-live")

        chosen = dict(hourly_24h[target_hour])
        chosen["source"] = "open-meteo-live"
        chosen["hourly_24h"] = hourly_24h
        chosen["hourly_24h_compact"] = [
            [
                float(pt["temperature_2m"]),
                float(pt["precipitation"]),
                float(pt["snowfall"]),
                float(pt["wind_speed_10m"]),
            ]
            for pt in hourly_24h
        ]
        return chosen
    except Exception:
        return _seasonal_fallback_weather(local_dt)


def compute_mapbox_distance_adjustment(
    *,
    metered_ml_usd: float,
    mapbox_distance_km: float | None,
    zone_centroid_distance_km: float,
    is_same_zone: bool,
    flat_fare_applied: bool,
) -> dict[str, Any]:
    """Refine zone-level ML metered estimate with actual Mapbox street driving distance."""
    if flat_fare_applied or mapbox_distance_km is None or not math.isfinite(mapbox_distance_km) or mapbox_distance_km <= 0:
        return {
            "adjusted_metered_usd": round(float(metered_ml_usd), 2),
            "distance_delta_usd": 0.0,
            "reference_road_km": round(float(zone_centroid_distance_km * 1.28), 2),
            "explanation_vi": None,
        }

    if is_same_zone or zone_centroid_distance_km < 0.05:
        # Intra-zone trips have a typical average road distance of ~1.35 km in TLC training data
        reference_road_km = 1.35
        diff_km = float(mapbox_distance_km) - reference_road_km
        raw_delta_usd = (diff_km * 0.621371) * TLC_PER_MILE_RATE_USD * 0.75
    else:
        # Typical Manhattan/NYC street routing factor vs Haversine centroid distance is ~1.28x
        reference_road_km = float(zone_centroid_distance_km) * 1.28
        diff_km = float(mapbox_distance_km) - reference_road_km
        if abs(diff_km) <= 0.45:
            raw_delta_usd = 0.0
        else:
            raw_delta_usd = (diff_km * 0.621371) * TLC_PER_MILE_RATE_USD * 0.55

    # Bound adjustment so it refines rather than distorts the GBT prediction
    max_up = float(metered_ml_usd) * 0.45 + 6.0
    max_down = float(metered_ml_usd) * 0.35
    clamped_delta = max(-max_down, min(max_up, raw_delta_usd))
    adjusted = max(TLC_INITIAL_CHARGE_USD + TLC_MILEAGE_UNIT_USD, float(metered_ml_usd) + clamped_delta)
    final_delta = round(adjusted - float(metered_ml_usd), 2)

    explanation = None
    if abs(final_delta) >= 0.35:
        if final_delta > 0:
            explanation = (
                f"Lộ trình đường bộ thực tế trên Mapbox ({mapbox_distance_km:.2f} km) dài hơn mức trung bình "
                f"tâm vùng ({reference_road_km:.2f} km), cộng thêm +{final_delta:.2f} USD theo biểu phí đồng hồ TLC."
            )
        else:
            explanation = (
                f"Điểm đón/trả nằm gần nhau trên lộ trình Mapbox ({mapbox_distance_km:.2f} km so với trung bình vùng "
                f"{reference_road_km:.2f} km), giảm {abs(final_delta):.2f} USD cước đồng hồ."
            )

    return {
        "adjusted_metered_usd": round(adjusted, 2),
        "distance_delta_usd": final_delta,
        "reference_road_km": round(reference_road_km, 2),
        "explanation_vi": explanation,
    }


def compute_active_tlc_surcharges(
    *,
    pickup_location_id: int,
    dropoff_location_id: int,
    pickup_borough: str,
    dropoff_borough: str,
    pickup_dt: datetime,
    flat_fare_applied: bool = False,
    include_cbd_fee: bool = True,
    calendar=None,
) -> dict[str, Any]:
    """Return ONLY the official NYC TLC surcharges that actively apply to this specific trip.

    Never returns 0.00 USD dummy rows; every item in `active_surcharges` is a real fee > 0 USD
    mandated by NYC TLC (`https://www.nyc.gov/site/tlc/passengers/taxi-fare.page`) and verified
    against `analytics.fact_trip`.
    All Vietnamese labels and descriptions avoid raw `$` characters to prevent Streamlit KaTeX glitches.
    """
    cal = calendar or load_calendar()
    p_date = pickup_dt.date()
    is_hol = bool(p_date in cal.holidays or p_date.isoformat() in cal.holidays)
    is_weekday = pickup_dt.weekday() < 5
    hour = int(pickup_dt.hour)
    pu_id = int(pickup_location_id)
    do_id = int(dropoff_location_id)

    active: list[dict[str, Any]] = []

    # 1. Mandatory MTA State Tax (0.50 USD) + TLC Improvement Surcharge (1.00 USD) = 1.50 USD
    active.append(
        {
            "code": "mta_and_improvement",
            "label_vi": "Thuế Bang MTA (0.50 USD) & Phí cải tiến TLC (1.00 USD)",
            "column_name": "mta_tax + improvement_surcharge",
            "amount_usd": round(TLC_MANDATORY_BASE_TAX_USD, 2),
            "reason_vi": "Khoản thuế và phí bắt buộc cố định trên mọi chuyến NYC Yellow Taxi.",
            "is_time_sensitive": False,
        }
    )

    # 2. Rush Hour Surcharge (16:00 - 19:59 Mon-Fri, excluding legal holidays)
    if is_weekday and not is_hol and 16 <= hour < 20:
        rush_amt = TLC_RUSH_HOUR_JFK_FLAT_USD if flat_fare_applied else TLC_RUSH_HOUR_METered_USD
        active.append(
            {
                "code": "rush_hour_surcharge",
                "label_vi": "Phụ phí Giờ cao điểm (16:00–20:00 ngày thường)",
                "column_name": "extra",
                "amount_usd": round(rush_amt, 2),
                "reason_vi": (
                    "Quy định NYC TLC áp dụng từ 16:00 đến 20:00 các ngày Thứ Hai – Thứ Sáu "
                    f"({'+5.00 USD cho tuyến cố định sân bay JFK' if flat_fare_applied else '+2.50 USD cho chuyến tính đồng hồ'})."
                ),
                "is_time_sensitive": True,
            }
        )

    # 3. Overnight Surcharge (20:00 - 05:59 daily, metered trips only)
    if not flat_fare_applied and (hour >= 20 or hour < 6):
        active.append(
            {
                "code": "overnight_surcharge",
                "label_vi": "Phụ phí Ban đêm (20:00–06:00 sáng)",
                "column_name": "extra",
                "amount_usd": round(TLC_OVERNIGHT_SURCHARGE_USD, 2),
                "reason_vi": "Quy định NYC TLC áp dụng cho các chuyến xe tính đồng hồ khởi hành từ 20:00 tối đến 06:00 sáng.",
                "is_time_sensitive": True,
            }
        )

    # 4. LaGuardia Airport Surcharge (+5.00 USD in `extra` for any pickup or dropoff at Zone 138)
    if not flat_fare_applied and 138 in (pu_id, do_id):
        active.append(
            {
                "code": "lga_airport_surcharge",
                "label_vi": "Phụ phí Tuyến Sân bay LaGuardia (LGA — Zone 138)",
                "column_name": "extra",
                "amount_usd": round(TLC_LGA_AIRPORT_SURCHARGE_USD, 2),
                "reason_vi": "Quy định NYC TLC cộng thêm 5.00 USD (cột extra) cho mọi chuyến taxi đón hoặc trả khách tại Sân bay LaGuardia.",
                "is_time_sensitive": False,
            }
        )

    # 5. NY State Manhattan Congestion Surcharge (2.50 USD — South of 96th Street)
    in_manhattan_congestion_zone = (
        pu_id in MANHATTAN_CONGESTION_96TH_ZONE_IDS or do_id in MANHATTAN_CONGESTION_96TH_ZONE_IDS
    )
    if in_manhattan_congestion_zone:
        active.append(
            {
                "code": "congestion_surcharge",
                "label_vi": "Phụ phí Tắc đường Bang NY (Manhattan phía nam Đường 96)",
                "column_name": "congestion_surcharge",
                "amount_usd": round(TLC_MANHATTAN_CONGESTION_SURCHARGE_USD, 2),
                "reason_vi": "Phụ phí Bang New York áp dụng cho chuyến xe đón hoặc trả tại khu vực Manhattan từ Đường 96 trở xuống.",
                "is_time_sensitive": False,
            }
        )

    # 6. MTA CBD Congestion Relief Zone Toll (0.75 USD — Manhattan South of 60th Street, effective Jan 5, 2025)
    in_cbd_60th_zone = (
        pu_id in MANHATTAN_CBD_60TH_ZONE_IDS or do_id in MANHATTAN_CBD_60TH_ZONE_IDS
    )
    if include_cbd_fee and in_cbd_60th_zone and p_date >= date(2025, 1, 5):
        active.append(
            {
                "code": "cbd_congestion_fee",
                "label_vi": "Phí Giảm ùn tắc Trung tâm Manhattan CBD (Nam Đường 60)",
                "column_name": "cbd_congestion_fee",
                "amount_usd": round(TLC_CBD_CONGESTION_FEE_USD, 2),
                "reason_vi": "Phí khu vực trung tâm thương mại Manhattan (phía nam Đường 60) áp dụng từ năm 2025 trên mọi chuyến Yellow Taxi.",
                "is_time_sensitive": False,
            }
        )

    # 7. Airport Access Fee (1.75 USD — charged ONLY on PICK-UP at JFK Zone 132 or LaGuardia Zone 138)
    if pu_id in (132, 138):
        airport_name = "Sân bay JFK" if pu_id == 132 else "Sân bay LaGuardia (LGA)"
        active.append(
            {
                "code": "airport_fee",
                "label_vi": f"Phí Đón khách tại {airport_name} (PANYNJ)",
                "column_name": "airport_fee",
                "amount_usd": round(TLC_AIRPORT_ACCESS_FEE_USD, 2),
                "reason_vi": f"Phí cảng hàng không PANYNJ bắt buộc khi đón khách chiều từ {airport_name} đi ra (chiều trả khách vào sân bay không mất phí này).",
                "is_time_sensitive": False,
            }
        )

    total_surcharges_usd = round(sum(item["amount_usd"] for item in active), 2)
    return {
        "active_surcharges": active,
        "total_surcharges_usd": total_surcharges_usd,
        "has_rush_hour_surcharge": any(i["code"] == "rush_hour_surcharge" for i in active),
        "has_overnight_surcharge": any(i["code"] == "overnight_surcharge" for i in active),
    }


def build_fare_explanation_and_advice(
    *,
    prediction: Mapping[str, Any],
    pickup_zone: Mapping[str, Any],
    dropoff_zone: Mapping[str, Any],
    pickup_dt: datetime,
    mapbox_distance_km: float | None = None,
    weather_info: Mapping[str, Any] | None = None,
    mode: str = "now",
    now_nyc: datetime | None = None,
) -> dict[str, Any]:
    """Build complete Total Fare receipt, Counterfactual XAI breakdown, and Nearest Better Time advice."""
    pu_id = int(pickup_zone["location_id"])
    do_id = int(dropoff_zone["location_id"])
    pu_borough = str(pickup_zone.get("borough", ""))
    do_borough = str(dropoff_zone.get("borough", ""))
    flat_fare = bool(prediction.get("flat_fare_applied", False))
    centroid_km = float(prediction.get("zone_centroid_distance_km", 0.0))
    is_same_zone = bool(prediction.get("is_same_zone", 0) or pu_id == do_id)

    raw_fare_usd = float(prediction.get("fare_amount_usd", 0.0))
    m3_w_usd = prediction.get("m3_weather_estimate_usd")
    m3_nw_usd = float(prediction.get("m3_no_weather_estimate_usd", raw_fare_usd))
    metered_ml_usd = float(prediction.get("metered_ml_estimate_usd", m3_w_usd if m3_w_usd is not None else m3_nw_usd))

    dist_adj = compute_mapbox_distance_adjustment(
        metered_ml_usd=metered_ml_usd,
        mapbox_distance_km=mapbox_distance_km,
        zone_centroid_distance_km=centroid_km,
        is_same_zone=is_same_zone,
        flat_fare_applied=flat_fare,
    )
    adjusted_metered_usd = 70.0 if flat_fare else dist_adj["adjusted_metered_usd"]
    distance_delta_usd = 0.0 if flat_fare else dist_adj["distance_delta_usd"]

    cal = load_calendar()
    surcharge_info = compute_active_tlc_surcharges(
        pickup_location_id=pu_id,
        dropoff_location_id=do_id,
        pickup_borough=pu_borough,
        dropoff_borough=do_borough,
        pickup_dt=pickup_dt,
        flat_fare_applied=flat_fare,
        calendar=cal,
    )
    total_surcharges_usd = surcharge_info["total_surcharges_usd"]
    total_estimated_fare_usd = round(adjusted_metered_usd + total_surcharges_usd, 2)

    # Weather impact delta (M3-w vs M3-nw)
    weather_delta_usd = (
        round(float(m3_w_usd) - float(m3_nw_usd), 2)
        if (m3_w_usd is not None and not flat_fare)
        else 0.0
    )

    # Build 24h total fare curve (combining ML hourly curve + distance adjustment + hourly TLC surcharges)
    raw_curve = prediction.get("hourly_curve")
    hourly_total_curve: list[dict[str, Any]] = []
    base_date_naive = pickup_dt.replace(tzinfo=None)

    if isinstance(raw_curve, list) and len(raw_curve) == 24:
        for pt in raw_curve:
            h = int(pt["hour"])
            dt_h = base_date_naive.replace(hour=h)
            s_h = compute_active_tlc_surcharges(
                pickup_location_id=pu_id,
                dropoff_location_id=do_id,
                pickup_borough=pu_borough,
                dropoff_borough=do_borough,
                pickup_dt=dt_h,
                flat_fare_applied=flat_fare,
                calendar=cal,
            )
            metered_h = 70.0 if flat_fare else max(
                TLC_INITIAL_CHARGE_USD + TLC_MILEAGE_UNIT_USD,
                round(float(pt.get("metered_ml_usd", pt["fare_amount_usd"])) + distance_delta_usd, 2),
            )
            total_h = round(metered_h + s_h["total_surcharges_usd"], 2)
            hourly_total_curve.append(
                {
                    "hour": h,
                    "time_label": f"{h:02d}:{pickup_dt.minute:02d}",
                    "metered_fare_usd": metered_h,
                    "surcharges_usd": s_h["total_surcharges_usd"],
                    "total_fare_usd": total_h,
                    "has_rush_hour": s_h["has_rush_hour_surcharge"],
                    "has_overnight": s_h["has_overnight_surcharge"],
                    "is_selected_hour": h == int(pickup_dt.hour),
                }
            )
    else:
        # Fallback curve if predictor mock in unit tests didn't supply hourly_curve
        for h in range(24):
            dt_h = base_date_naive.replace(hour=h)
            s_h = compute_active_tlc_surcharges(
                pickup_location_id=pu_id,
                dropoff_location_id=do_id,
                pickup_borough=pu_borough,
                dropoff_borough=do_borough,
                pickup_dt=dt_h,
                flat_fare_applied=flat_fare,
                calendar=cal,
            )
            traffic_mod = 0.0
            if not flat_fare and h != int(pickup_dt.hour):
                if 16 <= h <= 19:
                    traffic_mod = 1.6
                elif 7 <= h <= 9:
                    traffic_mod = 1.0
                elif 0 <= h <= 5:
                    traffic_mod = -1.4
            metered_h = 70.0 if flat_fare else round(max(3.70, adjusted_metered_usd + traffic_mod), 2)
            total_h = round(metered_h + s_h["total_surcharges_usd"], 2)
            hourly_total_curve.append(
                {
                    "hour": h,
                    "time_label": f"{h:02d}:{pickup_dt.minute:02d}",
                    "metered_fare_usd": metered_h,
                    "surcharges_usd": s_h["total_surcharges_usd"],
                    "total_fare_usd": total_h,
                    "has_rush_hour": s_h["has_rush_hour_surcharge"],
                    "has_overnight": s_h["has_overnight_surcharge"],
                    "is_selected_hour": h == int(pickup_dt.hour),
                }
            )

    current_hour = int(pickup_dt.hour)
    offpeak_metered_min = min(pt["metered_fare_usd"] for pt in hourly_total_curve)
    time_of_day_traffic_delta_usd = (
        0.0 if flat_fare else round(max(0.0, adjusted_metered_usd - weather_delta_usd - offpeak_metered_min), 2)
    )
    base_offpeak_route_usd = (
        70.0 if flat_fare else round(adjusted_metered_usd - weather_delta_usd - time_of_day_traffic_delta_usd, 2)
    )

    nearest_better = _find_nearest_better_hour(
        hourly_total_curve=hourly_total_curve,
        current_hour=current_hour,
        current_total_usd=total_estimated_fare_usd,
        current_has_rush=surcharge_info["has_rush_hour_surcharge"],
        flat_fare_applied=flat_fare,
        mode=mode,
        pickup_dt=pickup_dt,
        now_nyc=now_nyc,
    )

    return {
        "metered_fare_usd": adjusted_metered_usd,
        "metered_ml_reference_usd": round(metered_ml_usd, 2),
        "raw_ml_fare_usd": round(raw_fare_usd, 2),
        "distance_delta_usd": distance_delta_usd,
        "distance_explanation_vi": dist_adj["explanation_vi"],
        "active_surcharges": surcharge_info["active_surcharges"],
        "total_surcharges_usd": total_surcharges_usd,
        "total_estimated_fare_usd": total_estimated_fare_usd,
        "has_rush_hour_surcharge": surcharge_info["has_rush_hour_surcharge"],
        "has_overnight_surcharge": surcharge_info["has_overnight_surcharge"],
        "flat_fare_applied": flat_fare,
        "xai_components": {
            "base_route_offpeak_usd": base_offpeak_route_usd,
            "time_of_day_traffic_delta_usd": time_of_day_traffic_delta_usd,
            "weather_delta_usd": weather_delta_usd,
            "tlc_surcharges_usd": total_surcharges_usd,
        },
        "weather_info": dict(weather_info) if isinstance(weather_info, Mapping) else None,
        "hourly_total_curve": hourly_total_curve,
        "nearest_better_option": nearest_better,
    }


def _find_nearest_better_hour(
    *,
    hourly_total_curve: list[dict[str, Any]],
    current_hour: int,
    current_total_usd: float,
    current_has_rush: bool,
    flat_fare_applied: bool = False,
    mode: str = "now",
    pickup_dt: datetime | None = None,
    now_nyc: datetime | None = None,
) -> dict[str, Any]:
    """Find the closest practical departure hour (±1 to ±4 hours) that saves meaningful money.

    Never suggests a departure time in the past:
    - When `mode == 'now'`, only considers forward shifts (+1..+4 hours ahead).
    - When `mode == 'scheduled'` and `now_nyc` is provided (or pickup_dt is on today's NYC date),
      filters out any candidate hour that has already passed in New York.
    """
    by_hour = {pt["hour"]: pt for pt in hourly_total_curve}
    local_pickup = (
        pickup_dt.astimezone(NYC_TZ)
        if (pickup_dt is not None and pickup_dt.tzinfo is not None)
        else (pickup_dt.replace(tzinfo=NYC_TZ) if pickup_dt is not None else None)
    )
    ref_now = (
        now_nyc.astimezone(NYC_TZ)
        if (now_nyc is not None and now_nyc.tzinfo is not None)
        else (now_nyc.replace(tzinfo=NYC_TZ) if now_nyc is not None else datetime.now(NYC_TZ))
    )

    candidates: list[dict[str, Any]] = []
    offsets = (1, 2, 3, 4) if mode == "now" else (1, -1, 2, -2, 3, -3, 4, -4)

    for offset in offsets:
        h = current_hour + offset
        if not (0 <= h <= 23):
            continue
        # If pickup_dt is on today's NYC date (or earlier), never suggest an hour in the past
        if local_pickup is not None and offset < 0:
            candidate_dt = local_pickup.replace(hour=h)
            if local_pickup.date() == ref_now.date() and candidate_dt < ref_now + timedelta(minutes=2):
                continue

        pt = by_hour[h]
        savings = round(current_total_usd - pt["total_fare_usd"], 2)
        if savings >= 0.40:
            score = savings / (abs(offset) * 0.75 + 0.5)
            reasons: list[str] = []
            if current_has_rush and not pt["has_rush_hour"]:
                rush_str = "5.00 USD" if flat_fare_applied else "2.50 USD"
                reasons.append(f"né được Phụ phí Giờ cao điểm ({rush_str})")
            if pt["metered_fare_usd"] < by_hour[current_hour]["metered_fare_usd"] - 0.30:
                reasons.append("lưu lượng giao thông thông thoáng hơn")
            reason_text = " và ".join(reasons) if reasons else "biểu phí khung giờ thấp hơn"
            shift_text = (
                f"lùi {offset} tiếng" if offset > 0 else f"đi sớm hơn {abs(offset)} tiếng"
            )
            candidates.append(
                {
                    "hour": h,
                    "time_label": pt["time_label"],
                    "offset_hours": offset,
                    "shift_text_vi": shift_text,
                    "new_total_fare_usd": pt["total_fare_usd"],
                    "new_metered_fare_usd": pt["metered_fare_usd"],
                    "new_surcharges_usd": pt["surcharges_usd"],
                    "savings_usd": savings,
                    "savings_pct": round((savings / current_total_usd) * 100.0, 1) if current_total_usd > 0 else 0.0,
                    "reason_vi": reason_text,
                    "score": score,
                }
            )

    if candidates:
        best = max(candidates, key=lambda c: c["score"])
        return {
            "has_better_nearby": True,
            **best,
            "message_vi": (
                f"💡 **Đề xuất giờ đi gần nhất tiết kiệm hơn**: Nếu bạn khởi hành lúc **{best['time_label']}** "
                f"({best['shift_text_vi']}), tổng giá cước giảm từ **{current_total_usd:.2f} USD** xuống còn "
                f"**{best['new_total_fare_usd']:.2f} USD** (tiết kiệm **{best['savings_usd']:.2f} USD / {best['savings_pct']:.1f}%** "
                f"nhờ {best['reason_vi']})."
            ),
        }

    return {
        "has_better_nearby": False,
        "hour": current_hour,
        "time_label": by_hour[current_hour]["time_label"],
        "savings_usd": 0.0,
        "message_vi": (
            f"✅ **Khung giờ tối ưu**: Thời điểm bạn chọn (**{by_hour[current_hour]['time_label']}**) "
            "đang có mức tổng cước tốt nhất trong phạm vi các giờ lân cận."
        ),
    }


def _strip_accents(text: str) -> str:
    norm = unicodedata.normalize("NFD", text.lower().replace("đ", "d"))
    return "".join(ch for ch in norm if unicodedata.category(ch) != "Mn")


# Rich Vietnamese & English alias table mapped to NYC_POPULAR_PRESETS & key Taxi Zones
_LOCATION_ALIASES: list[tuple[tuple[str, ...], dict[str, Any]]] = [
    (
        ("times square", "quang truong thoi dai", "time square", "times sq", "theatre district"),
        next(p for p in NYC_POPULAR_PRESETS if p["id"] == "times_square"),
    ),
    (
        ("jfk", "san bay jfk", "john f kennedy", "kennedy airport"),
        next(p for p in NYC_POPULAR_PRESETS if p["id"] == "jfk_airport"),
    ),
    (
        ("laguardia", "san bay laguardia", "lga", "san bay lga"),
        next(p for p in NYC_POPULAR_PRESETS if p["id"] == "laguardia"),
    ),
    (
        ("midtown", "rockefeller", "midtown center"),
        next(p for p in NYC_POPULAR_PRESETS if p["id"] == "midtown_center"),
    ),
    (
        ("penn station", "ga penn", "madison square garden", "nha ga penn"),
        next(p for p in NYC_POPULAR_PRESETS if p["id"] == "penn_station"),
    ),
    (
        ("wall street", "pho wall", "financial district", "world trade center", "wtc", "one world"),
        next(p for p in NYC_POPULAR_PRESETS if p["id"] == "wall_street"),
    ),
    (
        ("central park", "cong vien trung tam", "upper east", "upper west", "bao tang met"),
        next(p for p in NYC_POPULAR_PRESETS if p["id"] == "upper_east"),
    ),
    (
        ("brooklyn", "downtown brooklyn", "cau brooklyn", "brooklyn bridge", "dumbo"),
        next(p for p in NYC_POPULAR_PRESETS if p["id"] == "brooklyn_dumbo"),
    ),
]


def _detect_location_role(clean_text: str, pos: int) -> str | None:
    """Inspect the preposition immediately preceding a matched location at `pos`."""
    prefix = clean_text[max(0, pos - 24) : pos]
    if re.search(r"\b(?:tu|don o|don tai|xuat phat tu|khoi hanh tu|from)\s+$", prefix):
        return "pickup"
    if re.search(r"\b(?:di|den|toi|ve|ra|sang|to)\s+$", prefix):
        return "dropoff"
    return None


def parse_natural_language_booking(
    prompt: str,
    *,
    now_nyc: datetime | None = None,
    zones: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Parse a natural Vietnamese or English ride-booking sentence into Pickup, Dropoff, Date & Time.

    Supports both standard order ('Từ Times Square đi JFK') and reversed order ('Đi JFK từ Times Square').
    Note: The user never needs to specify weather; weather is automatically fetched for the parsed time!
    """
    raw = (prompt or "").strip()
    if not raw:
        return {"ok": False, "error": "Vui lòng nhập câu lệnh đặt xe (ví dụ: 'Từ Times Square đi sân bay JFK lúc 6h sáng mai')."}

    ref_now = now_nyc.astimezone(NYC_TZ) if (now_nyc and now_nyc.tzinfo) else (now_nyc or datetime.now(NYC_TZ)).replace(tzinfo=NYC_TZ)
    clean = _strip_accents(raw)

    # 1. Extract all location matches ordered by their first character position in the sentence
    matched_locs: list[tuple[int, dict[str, Any], str | None]] = []
    used_ids: set[str] = set()

    for aliases, preset in _LOCATION_ALIASES:
        best_pos = None
        for alias in aliases:
            pos = clean.find(alias)
            if pos != -1 and (best_pos is None or pos < best_pos):
                best_pos = pos
        if best_pos is not None and preset["id"] not in used_ids:
            used_ids.add(preset["id"])
            role = _detect_location_role(clean, best_pos)
            matched_locs.append((best_pos, dict(preset), role))

    # Also match against the 263 TLC zones if fewer than 2 hubs matched
    if len(matched_locs) < 2 and zones:
        centroids = load_zone_centroids()
        for z in zones:
            z_name_clean = _strip_accents(str(z.get("zone", "")))
            if len(z_name_clean) < 4:
                continue
            pos = clean.find(z_name_clean)
            if pos != -1:
                lid = int(z["location_id"])
                lat, lon = centroids.coords.get(lid, (40.7580, -73.9855))
                key_id = f"zone_{lid}"
                if key_id not in used_ids:
                    used_ids.add(key_id)
                    role = _detect_location_role(clean, pos)
                    matched_locs.append(
                        (
                            pos,
                            {
                                "id": key_id,
                                "short_name": f"{z['zone']} (Zone {lid})",
                                "label": f"{z['zone']}, {z['borough']} (Zone {lid})",
                                "location_id": lid,
                                "borough": z.get("borough", ""),
                                "zone": z.get("zone", ""),
                                "longitude": float(lon),
                                "latitude": float(lat),
                            },
                            role,
                        )
                    )

    matched_locs.sort(key=lambda item: item[0])
    pickup_loc = None
    dropoff_loc = None

    if len(matched_locs) >= 2:
        first_loc, first_role = matched_locs[0][1], matched_locs[0][2]
        second_loc, second_role = matched_locs[1][1], matched_locs[1][2]
        # If sentence explicitly says "Đi B từ A" (first has dropoff preposition or second has pickup preposition)
        if first_role == "dropoff" and second_role == "pickup":
            pickup_loc, dropoff_loc = second_loc, first_loc
        elif second_role == "pickup" and first_role != "pickup":
            pickup_loc, dropoff_loc = second_loc, first_loc
        else:
            pickup_loc, dropoff_loc = first_loc, second_loc
    elif len(matched_locs) == 1:
        pickup_loc = matched_locs[0][1]

    # 2. Extract Date & Time
    mode = "now"
    target_date = ref_now.date()
    target_hour = ref_now.hour
    target_minute = ref_now.minute

    if any(k in clean for k in ("ngay mai", "sang mai", "trua mai", "chieu mai", "toi mai", "dem mai", "tomorrow")):
        target_date = ref_now.date() + timedelta(days=1)
        mode = "scheduled"
    elif any(k in clean for k in ("ngay kia", "day after tomorrow")):
        target_date = ref_now.date() + timedelta(days=2)
        mode = "scheduled"
    elif any(k in clean for k in ("hom nay", "sang nay", "trua nay", "chieu nay", "toi nay", "dem nay", "today", "tonight")):
        target_date = ref_now.date()
        mode = "scheduled"

    # Match explicit time patterns: "18h30", "6h", "6:30", "17 giờ 15", "7h rưỡi", "6pm", "6:30 pm", "7 am"
    time_match = re.search(
        r"\b(\d{1,2})\s*(?:(?:h|gio|:)\s*(\d{1,2}|ruoi)?|(am|pm))\b",
        clean,
    )
    if time_match:
        h_val = int(time_match.group(1))
        min_token = time_match.group(2)
        ampm_token = time_match.group(3)
        if min_token == "ruoi" or "ruoi" in clean[time_match.start() : min(len(clean), time_match.end() + 8)]:
            m_val = 30
        elif min_token and min_token.isdigit():
            m_val = int(min_token)
        else:
            m_val = 0

        is_pm = ampm_token == "pm" or any(pm_w in clean for pm_w in ("chieu", "toi", "pm"))
        is_am = ampm_token == "am" or any(am_w in clean for am_w in ("dem", "khuya", "am"))
        if h_val < 12 and is_pm:
            h_val += 12
        elif h_val == 12 and is_am:
            h_val = 0
        if 0 <= h_val <= 23 and 0 <= m_val <= 59:
            target_hour = h_val
            target_minute = m_val
            mode = "scheduled"
            candidate_dt = datetime.combine(target_date, time(target_hour, target_minute), tzinfo=NYC_TZ)
            if candidate_dt < ref_now - timedelta(minutes=2):
                target_date = ref_now.date() + timedelta(days=1)
    elif "nua dem" in clean or "midnight" in clean:
        target_hour, target_minute = 0, 0
        mode = "scheduled"
        if datetime.combine(target_date, time(0, 0), tzinfo=NYC_TZ) < ref_now:
            target_date = ref_now.date() + timedelta(days=1)
    elif "giua trua" in clean or "noon" in clean:
        target_hour, target_minute = 12, 0
        mode = "scheduled"

    if any(k in clean for k in ("di ngay", "bay gio", "right now", "now")) and not time_match:
        mode = "now"
        target_date = ref_now.date()
        target_hour = ref_now.hour
        target_minute = ref_now.minute

    if pickup_loc is None or dropoff_loc is None:
        return {
            "ok": False,
            "pickup": pickup_loc,
            "dropoff": dropoff_loc,
            "error": (
                "Chưa nhận diện đủ cả Điểm đón và Điểm đến trong câu lệnh. "
                "Hãy nêu rõ 2 địa điểm (ví dụ: 'Đặt xe từ Times Square đi sân bay JFK lúc 6h sáng mai')."
            ),
        }

    scheduled_dt = datetime.combine(target_date, time(target_hour, target_minute), tzinfo=NYC_TZ)
    return {
        "ok": True,
        "pickup": pickup_loc,
        "dropoff": dropoff_loc,
        "mode": mode,
        "pickup_date": target_date,
        "pickup_hour": target_hour,
        "pickup_minute": target_minute,
        "pickup_datetime_nyc": scheduled_dt,
        "summary_vi": (
            f"Đã điền tự động & ước tính cước: **{pickup_loc['short_name']}** → **{dropoff_loc['short_name']}** · "
            + ("Khởi hành **Đi ngay**" if mode == "now" else f"Hẹn giờ **{target_hour:02d}:{target_minute:02d} ngày {target_date:%d/%m/%Y}**")
        ),
    }


def render_smart_fare_advisor_panel(advice: Mapping[str, Any], *, on_apply_suggested_hour=None) -> None:
    """Render the transparent TLC Total Fare breakdown, active surcharges, XAI, and Nearest Better Time advisor."""
    import streamlit as st

    nearest = advice.get("nearest_better_option") or {}
    if nearest.get("has_better_nearby"):
        st.info(nearest["message_vi"])
        if callable(on_apply_suggested_hour):
            if st.button(
                f"⏰ Đổi sang giờ gợi ý & Tính lại ngay ({nearest['time_label']} — chỉ {nearest['new_total_fare_usd']:.2f} USD)",
                key="ai_advisor_apply_nearest_hour_btn",
            ):
                on_apply_suggested_hour(int(nearest["hour"]))
    elif nearest.get("message_vi"):
        st.success(nearest["message_vi"])

    with st.container(border=True):
        st.markdown("#### 🧾 Bảng kê Chi tiết Giá cước & Phụ phí Chính thức NYC TLC")
        st.caption(
            "Chỉ hiển thị các khoản phụ phí thực tế phát sinh của chuyến xe này theo quy định chính thức của Ủy ban Taxi New York "
            "([nyc.gov/taxi](https://www.nyc.gov/site/tlc/passengers/taxi-fare.page)) và đối soát trực tiếp với kho dữ liệu `analytics.fact_trip`."
        )

        w_info = advice.get("weather_info")
        if isinstance(w_info, Mapping):
            src_label = (
                "Dự báo thời tiết thực tế Open-Meteo tại NYC"
                if w_info.get("source") == "open-meteo-live"
                else (
                    "Thông số thời tiết giả lập thủ công"
                    if w_info.get("source") == "manual-override"
                    else "Hồ sơ khí tượng theo mùa tại NYC"
                )
            )
            st.markdown(
                f"🌦️ **Thời tiết áp dụng (`M3-w`)**: **{w_info.get('condition_vi', 'Khô ráo')}** · "
                f"Nhiệt độ `{w_info.get('temperature_2m', 18.0):.1f}°C` · "
                f"Lượng mưa `{w_info.get('precipitation', 0.0):.1f} mm/h` · "
                f"Tốc độ gió `{w_info.get('wind_speed_10m', 10.0):.1f} km/h` *({src_label})*"
            )

        rows_md: list[str] = []
        if advice.get("flat_fare_applied"):
            ml_ref = advice.get("metered_ml_reference_usd")
            ref_note = (
                f" *(thay cho cước bấm đồng hồ theo km/kẹt xe; nếu tính đồng hồ ML thông thường ước tính khoảng `{ml_ref:.2f} USD`)*"
                if isinstance(ml_ref, (int, float)) and ml_ref > 0
                else " *(mức cước gốc cố định theo luật, không nhảy tiền theo km hay kẹt xe)*"
            )
            rows_md.append(
                f"- **Cước xe cố định tuyến Sân bay JFK ↔ Manhattan (`Ratecode 2` — cột `fare_amount`)**: **`{advice['metered_fare_usd']:.2f} USD`**{ref_note}"
            )
        else:
            rows_md.append(
                f"- **Cước đồng hồ dự báo (`fare_amount` — Mô hình Spark ML `M3-w`)**: **`{advice['metered_fare_usd']:.2f} USD`**"
            )
            if advice.get("distance_explanation_vi"):
                rows_md.append(f"  - *↳ {advice['distance_explanation_vi']}*")

        for item in advice.get("active_surcharges", []):
            badge = "🔥 **Đang áp dụng**" if item.get("is_time_sensitive") else "⚖️ Theo luật TLC"
            rows_md.append(
                f"- **+ {item['label_vi']}** ({badge}): **`+{item['amount_usd']:.2f} USD`** — *{item['reason_vi']}*"
            )

        rows_md.append(
            f"- 🎯 **TỔNG THANH TOÁN DỰ KIẾN (`Total Fare` gồm Cước xe + Phụ phí TLC)**: "
            f"**`{advice['total_estimated_fare_usd']:.2f} USD`** "
            f"*(chưa bao gồm tiền tip tự nguyện và phí cầu đường nếu có)*"
        )
        st.markdown("\n".join(rows_md))

        xai = advice.get("xai_components") or {}
        if not advice.get("flat_fare_applied") and xai:
            w_delta = float(xai.get("weather_delta_usd", 0.0))
            t_delta = float(xai.get("time_of_day_traffic_delta_usd", 0.0))
            base_off = float(xai.get("base_route_offpeak_usd", advice["metered_fare_usd"]))
            w_sign = f"+{w_delta:.2f}" if w_delta >= 0 else f"-{abs(w_delta):.2f}"
            st.caption(
                f"🧠 **Giải thích AI (Counterfactual XAI)**: Cước nền lúc đường thông thoáng `{base_off:.2f} USD` "
                f"· Mật độ giao thông theo khung giờ `+{t_delta:.2f} USD` "
                f"· Tác động thời tiết ERA5 `{w_sign} USD` "
                f"· Tổng phụ phí luật TLC `+{advice['total_surcharges_usd']:.2f} USD`."
            )

        curve = advice.get("hourly_total_curve") or []
        if len(curve) == 24:
            with st.expander("📊 Xem Biểu đồ Biến động Tổng Giá Cước & Khung Giờ Tối Ưu Trong 24 Giờ", expanded=False):
                chart_rows = []
                best_near_h = nearest.get("hour") if nearest.get("has_better_nearby") else None
                rush_label = "3. Khung giờ cao điểm (+5.00 USD)" if advice.get("flat_fare_applied") else "3. Khung giờ cao điểm (+2.50 USD)"
                for pt in curve:
                    h = int(pt["hour"])
                    if pt.get("is_selected_hour"):
                        role = "1. Giờ bạn đang chọn"
                    elif best_near_h is not None and h == int(best_near_h):
                        role = "2. Giờ gợi ý gần nhất tiết kiệm"
                    elif pt.get("has_rush_hour"):
                        role = rush_label
                    else:
                        role = "4. Khung giờ thường"
                    chart_rows.append(
                        {
                            "Giờ khởi hành": pt["time_label"],
                            "Thứ tự giờ": h,
                            "Tổng giá cước (USD)": pt["total_fare_usd"],
                            "Cước xe gốc / ML (USD)": pt["metered_fare_usd"],
                            "Phụ phí TLC (USD)": pt["surcharges_usd"],
                            "Phân loại": role,
                        }
                    )
                st.vega_lite_chart(
                    chart_rows,
                    {
                        "height": 240,
                        "mark": {"type": "bar", "cornerRadiusTopLeft": 4, "cornerRadiusTopRight": 4},
                        "encoding": {
                            "x": {
                                "field": "Giờ khởi hành",
                                "type": "ordinal",
                                "sort": {"field": "Thứ tự giờ", "order": "ascending"},
                                "title": "Khung giờ khởi hành tại New York (24h)",
                            },
                            "y": {
                                "field": "Tổng giá cước (USD)",
                                "type": "quantitative",
                                "title": "Tổng thanh toán gồm phụ phí (USD)",
                            },
                            "color": {
                                "field": "Phân loại",
                                "type": "nominal",
                                "scale": {
                                    "domain": [
                                        "1. Giờ bạn đang chọn",
                                        "2. Giờ gợi ý gần nhất tiết kiệm",
                                        rush_label,
                                        "4. Khung giờ thường",
                                    ],
                                    "range": ["#f59e0b", "#10b981", "#ef4444", "#64748b"],
                                },
                                "legend": {"orient": "top", "title": None},
                            },
                            "tooltip": [
                                {"field": "Giờ khởi hành", "title": "Giờ đi"},
                                {"field": "Tổng giá cước (USD)", "title": "Tổng thanh toán (USD)"},
                                {"field": "Cước xe gốc / ML (USD)", "title": "Cước xe gốc / ML (USD)"},
                                {"field": "Phụ phí TLC (USD)", "title": "Phụ phí TLC (USD)"},
                                {"field": "Phân loại", "title": "Trạng thái"},
                            ],
                        },
                    },
                    use_container_width=True,
                )
