"""Interactive ride-booking Mapbox component bridge for Gold V3.

Provides a 2-way Uber/Grab-style interactive map experience:
- Click or drag markers A (Pickup) and B (Dropoff) directly on the NYC map
- Quick preset buttons for iconic NYC hubs (Times Square, JFK Airport, LaGuardia, Wall Street, Central Park, Brooklyn Bridge)
- Swap A <-> B and Clear pins
- Renders Taxi Zone polygon boundaries for selected Pickup & Dropoff zones
- Renders actual Mapbox driving route geometry + live HUD trip summary card
"""

from __future__ import annotations

from copy import deepcopy
from math import isfinite
from pathlib import Path
import re
from typing import Any
from uuid import UUID


_PUBLIC_TOKEN = re.compile(r"pk\.[A-Za-z0-9._-]+\Z")

NYC_POPULAR_PRESETS: tuple[dict[str, Any], ...] = (
    {
        "id": "times_square",
        "short_name": "🌆 Times Square",
        "label": "Times Square, Manhattan, NY (Zone 230)",
        "longitude": -73.9855,
        "latitude": 40.7580,
        "location_id": 230,
        "borough": "Manhattan",
        "zone": "Times Sq/Theatre District",
    },
    {
        "id": "jfk_airport",
        "short_name": "✈️ Sân bay JFK",
        "label": "JFK International Airport, Queens, NY (Zone 132)",
        "longitude": -73.7781,
        "latitude": 40.6413,
        "location_id": 132,
        "borough": "Queens",
        "zone": "JFK Airport",
    },
    {
        "id": "laguardia",
        "short_name": "🛫 Sân bay LaGuardia",
        "label": "LaGuardia Airport (LGA), Queens, NY (Zone 138)",
        "longitude": -73.8740,
        "latitude": 40.7769,
        "location_id": 138,
        "borough": "Queens",
        "zone": "LaGuardia Airport",
    },
    {
        "id": "midtown_center",
        "short_name": "🏙️ Midtown / Rockefeller",
        "label": "Rockefeller Center, Midtown Manhattan, NY (Zone 161)",
        "longitude": -73.9787,
        "latitude": 40.7587,
        "location_id": 161,
        "borough": "Manhattan",
        "zone": "Midtown Center",
    },
    {
        "id": "wall_street",
        "short_name": "🏦 Phố Wall (Financial Dist)",
        "label": "Wall Street, Financial District, Manhattan, NY (Zone 87)",
        "longitude": -74.0089,
        "latitude": 40.7069,
        "location_id": 87,
        "borough": "Manhattan",
        "zone": "Financial District North",
    },
    {
        "id": "penn_station",
        "short_name": "🚆 Ga Penn Station",
        "label": "Moynihan Train Hall at Penn Station, Manhattan, NY (Zone 186)",
        "longitude": -73.9936,
        "latitude": 40.7506,
        "location_id": 186,
        "borough": "Manhattan",
        "zone": "Penn Station/Madison Sq West",
    },
    {
        "id": "upper_east",
        "short_name": "🏛️ Bảo tàng Met / Upper East",
        "label": "The Met / Upper East Side North, Manhattan, NY (Zone 236)",
        "longitude": -73.9566,
        "latitude": 40.7794,
        "location_id": 236,
        "borough": "Manhattan",
        "zone": "Upper East Side North",
    },
    {
        "id": "brooklyn_dumbo",
        "short_name": "🌉 Cầu Brooklyn / DUMBO",
        "label": "DUMBO / Vinegar Hill, Brooklyn, NY (Zone 66)",
        "longitude": -73.9893,
        "latitude": 40.7033,
        "location_id": 66,
        "borough": "Brooklyn",
        "zone": "DUMBO/Vinegar Hill",
    },
)


def _valid_point(point: object) -> bool:
    if not isinstance(point, (list, tuple)) or len(point) != 2:
        return False
    try:
        return (
            all(not isinstance(value, bool) and isinstance(value, (int, float)) and isfinite(value) for value in point)
            and -180 <= point[0] <= 180
            and -90 <= point[1] <= 90
        )
    except (OverflowError, ValueError, TypeError):
        return False


def _sanitize_marker_info(raw: object) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    lon, lat = raw.get("longitude"), raw.get("latitude")
    if not _valid_point([lon, lat]):
        return None
    label = str(raw.get("label") or "").strip()[:220]
    zone = str(raw.get("zone") or "").strip()[:120]
    borough = str(raw.get("borough") or "").strip()[:80]
    loc_id = raw.get("location_id")
    loc_int = int(loc_id) if type(loc_id) is int and 1 <= loc_id <= 263 else None
    return {
        "longitude": float(lon),
        "latitude": float(lat),
        "label": label,
        "zone": zone,
        "borough": borough,
        "location_id": loc_int,
    }


def interactive_map_arguments(
    *,
    access_token: str,
    online: bool,
    pickup: dict[str, Any] | None = None,
    dropoff: dict[str, Any] | None = None,
    pickup_zone_feature: dict[str, Any] | None = None,
    dropoff_zone_feature: dict[str, Any] | None = None,
    route_geometry: dict[str, Any] | None = None,
    trip_summary: dict[str, Any] | None = None,
    active_target: str = "pickup",
) -> dict[str, Any]:
    """Validate and construct safe props for the interactive Mapbox ride-booking component."""
    if online is not True or not isinstance(access_token, str) or not _PUBLIC_TOKEN.fullmatch(access_token):
        return {"online": False}

    clean_pickup = _sanitize_marker_info(pickup)
    clean_dropoff = _sanitize_marker_info(dropoff)

    clean_route = None
    if isinstance(route_geometry, dict) and route_geometry.get("type") == "LineString":
        coords = route_geometry.get("coordinates")
        if isinstance(coords, list) and 2 <= len(coords) <= 20_000 and all(_valid_point(p) for p in coords):
            clean_route = {"type": "LineString", "coordinates": deepcopy(coords)}

    clean_summary = None
    if isinstance(trip_summary, dict):
        clean_summary = {
            "fare_usd": float(trip_summary.get("fare_usd", 0.0)),
            "distance_km": float(trip_summary.get("distance_km", 0.0)),
            "centroid_km": float(trip_summary.get("centroid_km", 0.0)),
            "flat_fare_applied": bool(trip_summary.get("flat_fare_applied", False)),
            "pickup_zone_text": str(trip_summary.get("pickup_zone_text", ""))[:140],
            "dropoff_zone_text": str(trip_summary.get("dropoff_zone_text", ""))[:140],
        }

    return {
        "online": True,
        "access_token": access_token,
        "pickup": clean_pickup,
        "dropoff": clean_dropoff,
        "pickup_zone_feature": deepcopy(pickup_zone_feature) if isinstance(pickup_zone_feature, dict) else None,
        "dropoff_zone_feature": deepcopy(dropoff_zone_feature) if isinstance(dropoff_zone_feature, dict) else None,
        "route_geometry": clean_route,
        "trip_summary": clean_summary,
        "active_target": "dropoff" if active_target == "dropoff" else "pickup",
    }


def parse_interactive_map_event(event: object) -> dict[str, Any] | None:
    """Validate incoming event from the interactive Mapbox frontend."""
    if not isinstance(event, dict):
        return None
    event_id = event.get("event_id")
    try:
        if not isinstance(event_id, str) or str(UUID(event_id)) != event_id:
            return None
    except (ValueError, AttributeError):
        return None

    action = event.get("action")
    if action == "pick_point":
        target = event.get("target")
        lon, lat = event.get("longitude"), event.get("latitude")
        if target not in {"pickup", "dropoff"} or not _valid_point([lon, lat]):
            return None
        return {
            "event_id": event_id,
            "action": "pick_point",
            "target": target,
            "longitude": round(float(lon), 6),
            "latitude": round(float(lat), 6),
        }
    if action in {"swap_points", "clear_points"}:
        return {"event_id": event_id, "action": action}
    return None


def interactive_booking_map(
    *,
    access_token: str,
    online: bool,
    pickup: dict[str, Any] | None = None,
    dropoff: dict[str, Any] | None = None,
    pickup_zone_feature: dict[str, Any] | None = None,
    dropoff_zone_feature: dict[str, Any] | None = None,
    route_geometry: dict[str, Any] | None = None,
    trip_summary: dict[str, Any] | None = None,
    active_target: str = "pickup",
    key: str = "nyc_interactive_booking_map",
) -> dict[str, Any] | None:
    """Render the interactive 2-way Uber/Grab-style Mapbox ride-booking map."""
    import streamlit.components.v1 as components

    component = components.declare_component(
        "nyc_interactive_booking_map",
        path=str(Path(__file__).with_name("interactive_map_frontend")),
    )
    raw_event = component(
        **interactive_map_arguments(
            access_token=access_token,
            online=online,
            pickup=pickup,
            dropoff=dropoff,
            pickup_zone_feature=pickup_zone_feature,
            dropoff_zone_feature=dropoff_zone_feature,
            route_geometry=route_geometry,
            trip_summary=trip_summary,
            active_target=active_target,
        ),
        default=None,
        key=key,
    )
    return parse_interactive_map_event(raw_event)

