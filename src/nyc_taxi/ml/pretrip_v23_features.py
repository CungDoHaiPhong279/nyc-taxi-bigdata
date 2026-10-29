"""Unfitted, stdlib-only V2.3 zone/centroid/time features shared by train and serve.

V2.3 extends the V2.2 pre-trip architecture without modifying any pinned V2.2 files:
- Minute-resolution cyclic NYC local time-of-day: (hour + minute / 60.0) / 24.0.
- Checksum-pinned Taxi Zone area-weighted polygon centroid Haversine distance
  (`zone_centroid_distance_km`) and intra-zone indicator (`is_same_zone`).
- Native categorical support up to `MAX_BINS = 264` (all 263 TLC zones + 1 unknown bucket).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID, uuid4

from nyc_taxi.ml.pretrip_features import (
    HolidayCalendar,
    MAX_SCHEDULE_HORIZON_SECONDS,
    PAST_CLOCK_TOLERANCE_SECONDS,
    TIMEZONE_NAME,
    _aware_pickup,
    _local_candidates,
    _localize,
    _nyc,
    _parse_datetime,
    _zone,
    timezone_identity,
)


CONTRACT_VERSION = "pretrip-v23-zone-centroid-time-1"
MAX_BINS = 264
EARTH_RADIUS_KM = 6371.0088
GEOMETRY_SHA256 = "6513c39044b8dee18c00af0dd6f97919d681b1e85efbd2d6980786c4cfbb8c9c"
MODEL_STATUS_ALLOWLIST = (
    "not_trained_or_deployed",
    "trained_candidate",
    "evaluated_holdout",
    "deployed_frozen",
)
NUMERIC_FEATURE_COLUMNS = (
    "pickup_hour_sin",
    "pickup_hour_cos",
    "pickup_dow_sin",
    "pickup_dow_cos",
    "is_weekend",
    "is_holiday",
    "zone_centroid_distance_km",
    "is_same_zone",
)
CATEGORICAL_FEATURE_COLUMNS = ("pickup_zone", "dropoff_zone")
FEATURE_COLUMNS = (*NUMERIC_FEATURE_COLUMNS, *CATEGORICAL_FEATURE_COLUMNS)
_ROOT = Path(__file__).resolve().parents[3]
_CONFIG = _ROOT / "config"
_ASSETS = _ROOT / "assets" / "taxi_zones"
_REQUIRED_REQUEST = {
    "pickup_location_id",
    "dropoff_location_id",
    "pickup_datetime",
    "timezone",
    "submission_id",
    "contract_version",
    "calendar_version",
    "calendar_sha256",
    "geometry_sha256",
    "tzdb_sha256",
    "submitted_at",
    "mode",
    "fold",
}
_COHORT_POLICY = {
    "train": ["2026-06-01", "2026-07-01"],
    "validation": ["2026-07-01", "2026-08-01"],
    "holdout": ["2026-08-01", "2026-09-01"],
    "sampling_fraction": 0.25,
    "seed": 42,
    "caps": {"train": 750000, "validation": 250000, "holdout": 250000},
}


def _cohort_policy_matches(policy: Any) -> bool:
    if not isinstance(policy, dict) or any(policy.get(name) != value for name, value in _COHORT_POLICY.items()):
        return False
    return type(policy["seed"]) is int and all(type(value) is int for value in policy["caps"].values())


def load_contract(path: str | Path | None = None) -> dict[str, Any]:
    """Load the explicit V2.3 feature and cohort contract; never fallback to V1/V2.2."""
    contract = json.loads(Path(path or _CONFIG / "pretrip_v23_contract.json").read_text(encoding="utf-8"))
    geometry = contract.get("geometry", {})
    if (
        contract.get("contract_version") != CONTRACT_VERSION
        or contract.get("timezone") != TIMEZONE_NAME
        or tuple(contract.get("numeric_features", [])) != NUMERIC_FEATURE_COLUMNS
        or tuple(contract.get("categorical_features", [])) != CATEGORICAL_FEATURE_COLUMNS
        or type(contract.get("max_bins")) is not int
        or contract.get("max_bins") != MAX_BINS
        or contract.get("model_status") not in MODEL_STATUS_ALLOWLIST
        or set(contract.get("required_request_fields", [])) != _REQUIRED_REQUEST
        or contract.get("time_policy", {}).get("scheduled_max_horizon_seconds") != MAX_SCHEDULE_HORIZON_SECONDS
        or contract.get("time_policy", {}).get("past_clock_tolerance_seconds") != PAST_CLOCK_TOLERANCE_SECONDS
        or not isinstance(geometry, dict)
        or geometry.get("sha256") != GEOMETRY_SHA256
        or type(geometry.get("feature_count")) is not int
        or geometry.get("feature_count") != 263
        or geometry.get("earth_radius_km") != EARTH_RADIUS_KM
        or not _cohort_policy_matches(contract.get("cohort_policy"))
    ):
        raise ValueError("Unsupported pretrip v2.3 feature contract")
    return contract


def load_calendar(path: str | Path | None = None, *, contract_path: str | Path | None = None) -> HolidayCalendar:
    """Verify raw calendar bytes against the V2.3 contract before parsing."""
    contract = load_contract(contract_path)
    raw = Path(path or _CONFIG / "pretrip_v2_calendar.json").read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != contract["calendar"]["sha256"]:
        raise ValueError("Pretrip v2.3 calendar checksum does not match pinned contract")
    data = json.loads(raw)
    if (
        data.get("calendar_version") != contract["calendar"]["version"]
        or data.get("timezone") != TIMEZONE_NAME
    ):
        raise ValueError("Pretrip v2.3 calendar identity does not match pinned contract")
    dates = [date.fromisoformat(row["date"]) for row in data["holidays"]]
    start, end = date.fromisoformat(data["coverage_start"]), date.fromisoformat(data["coverage_end"])
    if start > end or len(dates) != len(set(dates)) or any(not start <= day <= end for day in dates):
        raise ValueError("Invalid pretrip v2.3 calendar coverage or duplicate dates")
    return HolidayCalendar(data["calendar_version"], digest, start, end, frozenset(dates))


def _ring_area_and_centroid(ring: Any) -> tuple[float, float, float]:
    """Compute planar shoelace signed area and centroid relative to ring[0]."""
    if not isinstance(ring, list) or len(ring) < 4 or ring[0] != ring[-1]:
        raise ValueError("Invalid Taxi Zone polygon ring for centroid derivation")
    x0, y0 = float(ring[0][0]), float(ring[0][1])
    area2 = 0.0
    cx6 = 0.0
    cy6 = 0.0
    for a, b in zip(ring, ring[1:]):
        ax, ay = float(a[0]) - x0, float(a[1]) - y0
        bx, by = float(b[0]) - x0, float(b[1]) - y0
        cross = ax * by - bx * ay
        area2 += cross
        cx6 += (ax + bx) * cross
        cy6 += (ay + by) * cross
    if abs(area2) <= 1e-20:
        raise ValueError("Degenerate Taxi Zone polygon ring has zero area")
    return 0.5 * area2, x0 + cx6 / (3.0 * area2), y0 + cy6 / (3.0 * area2)


def haversine_distance_km(
    lon1: float,
    lat1: float,
    lon2: float,
    lat2: float,
    *,
    earth_radius_km: float = EARTH_RADIUS_KM,
) -> float:
    """Great-circle Haversine distance in kilometers between two WGS84 points."""
    if lon1 == lon2 and lat1 == lat2:
        return 0.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    return 2.0 * earth_radius_km * math.asin(min(1.0, math.sqrt(a)))


@dataclass(frozen=True)
class ZoneCentroidLookup:
    """Checksum-verified WGS84 area-weighted centroids for TLC LocationIDs 1..263."""

    sha256: str
    centroids: Mapping[int, tuple[float, float]]

    def centroid(self, location_id: Any) -> tuple[float, float]:
        zone_id = int(_zone(location_id))
        return self.centroids[zone_id]

    def distance_km(self, pickup_location_id: Any, dropoff_location_id: Any) -> float:
        pickup_id = int(_zone(pickup_location_id))
        dropoff_id = int(_zone(dropoff_location_id))
        if pickup_id == dropoff_id:
            return 0.0
        lon1, lat1 = self.centroids[pickup_id]
        lon2, lat2 = self.centroids[dropoff_id]
        return haversine_distance_km(lon1, lat1, lon2, lat2)


@lru_cache(maxsize=4)
def _cached_zone_centroids(resolved_geojson_path: str, expected_sha256: str) -> ZoneCentroidLookup:
    raw = Path(resolved_geojson_path).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if digest != expected_sha256:
        raise ValueError("Taxi Zone GeoJSON checksum does not match pinned V2.3 contract")
    document = json.loads(raw)
    features = document.get("features") if isinstance(document, dict) else None
    if not isinstance(features, list) or len(features) != 263:
        raise ValueError("Taxi Zone GeoJSON must contain exactly 263 features")
    centroids: dict[int, tuple[float, float]] = {}
    for feature in features:
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        loc_id = int(_zone(int(props["LocationID"])))
        if loc_id in centroids:
            raise ValueError("Duplicate LocationID in Taxi Zone GeoJSON")
        gtype = geom.get("type")
        polys = [geom["coordinates"]] if gtype == "Polygon" else geom.get("coordinates") if gtype == "MultiPolygon" else None
        if not isinstance(polys, list) or not polys:
            raise ValueError("Unsupported Taxi Zone geometry type")
        total_weight = 0.0
        weighted_x = 0.0
        weighted_y = 0.0
        for poly in polys:
            outer_area, outer_cx, outer_cy = _ring_area_and_centroid(poly[0])
            poly_weight = abs(outer_area)
            px = outer_cx * poly_weight
            py = outer_cy * poly_weight
            for hole in poly[1:]:
                hole_area, hole_cx, hole_cy = _ring_area_and_centroid(hole)
                hw = abs(hole_area)
                poly_weight -= hw
                px -= hole_cx * hw
                py -= hole_cy * hw
            if poly_weight <= 0.0:
                raise ValueError("Taxi Zone polygon net area must be positive")
            total_weight += poly_weight
            weighted_x += px
            weighted_y += py
        lon, lat = weighted_x / total_weight, weighted_y / total_weight
        if not (-74.35 <= lon <= -73.65 and 40.45 <= lat <= 40.95):
            raise ValueError("Derived Taxi Zone centroid lies outside NYC WGS84 bounds")
        centroids[loc_id] = (lon, lat)
    if set(centroids) != set(range(1, 264)):
        raise ValueError("Taxi Zone centroids must cover every LocationID 1..263")
    return ZoneCentroidLookup(sha256=digest, centroids=centroids)


def load_zone_centroids(
    path: str | Path | None = None,
    *,
    contract_path: str | Path | None = None,
) -> ZoneCentroidLookup:
    """Verify raw Taxi Zone GeoJSON bytes against the V2.3 contract and return centroids."""
    contract = load_contract(contract_path)
    expected_sha256 = contract["geometry"]["sha256"]
    target = Path(path or _ASSETS / "taxi_zones.geojson").resolve()
    return _cached_zone_centroids(str(target), expected_sha256)


def _features(
    local: datetime,
    pickup_id: Any,
    dropoff_id: Any,
    calendar: HolidayCalendar,
    centroids: ZoneCentroidLookup,
) -> dict[str, Any]:
    pickup_zone, dropoff_zone = _zone(pickup_id), _zone(dropoff_id)
    holiday = calendar.is_holiday(local.date())
    fractional_hour = local.hour + local.minute / 60.0
    hour_angle = 2.0 * math.pi * fractional_hour / 24.0
    dow_angle = 2.0 * math.pi * local.weekday() / 7.0
    distance_km = centroids.distance_km(pickup_id, dropoff_id)
    same_zone = 1.0 if pickup_zone == dropoff_zone else 0.0
    return {
        "pickup_hour_sin": math.sin(hour_angle),
        "pickup_hour_cos": math.cos(hour_angle),
        "pickup_dow_sin": math.sin(dow_angle),
        "pickup_dow_cos": math.cos(dow_angle),
        "is_weekend": float(local.weekday() >= 5),
        "is_holiday": float(holiday),
        "zone_centroid_distance_km": float(distance_km),
        "is_same_zone": same_zone,
        "pickup_zone": pickup_zone,
        "dropoff_zone": dropoff_zone,
    }


def build_training_features(
    pickup_datetime: datetime,
    pickup_location_id: int,
    dropoff_location_id: int,
    *,
    calendar: HolidayCalendar | None = None,
    centroids: ZoneCentroidLookup | None = None,
) -> dict[str, Any]:
    """Derive V2.3 pre-trip features from historical NYC wall time and TLC Zone IDs."""
    if not _local_candidates(pickup_datetime):
        raise ValueError("Historical pickup wall time does not exist in NYC")
    return _features(
        pickup_datetime,
        pickup_location_id,
        dropoff_location_id,
        calendar or load_calendar(),
        centroids or load_zone_centroids(),
    )


def build_serving_features(
    request: Mapping[str, Any],
    *,
    calendar: HolidayCalendar | None = None,
    centroids: ZoneCentroidLookup | None = None,
) -> dict[str, Any]:
    """Validate an immutable V2.3 submission, then call the exact training derivation."""
    if not isinstance(request, Mapping):
        raise ValueError("Pretrip v2.3 serving request must be an object")
    if set(request) != _REQUIRED_REQUEST:
        raise ValueError("Pretrip v2.3 request fields violate the strict feature/metadata allowlist")
    if request["timezone"] != TIMEZONE_NAME:
        raise ValueError("Pretrip v2.3 request timezone must be America/New_York")
    try:
        UUID(str(request["submission_id"]))
    except (ValueError, TypeError, AttributeError) as error:
        raise ValueError("Pretrip v2.3 submission_id must be a UUID") from error
    cal = calendar or load_calendar()
    geom = centroids or load_zone_centroids()
    identities = {
        "contract_version": CONTRACT_VERSION,
        "calendar_version": cal.version,
        "calendar_sha256": cal.sha256,
        "geometry_sha256": geom.sha256,
        "tzdb_sha256": timezone_identity()["tzif_sha256"],
    }
    if any(request[name] != expected for name, expected in identities.items()):
        raise ValueError("Pretrip v2.3 submission artifact/calendar/geometry/tzdb identity mismatch")
    local = _aware_pickup(request["pickup_datetime"], request.get("fold"))
    if request["mode"] not in ("now", "scheduled"):
        raise ValueError("Pretrip v2.3 mode must be now or scheduled")
    submitted = _parse_datetime(request["submitted_at"], "submitted_at")
    if submitted.tzinfo is None or submitted.utcoffset() != timedelta(0):
        raise ValueError("submitted_at must be an aware UTC instant")
    elapsed = local.astimezone(timezone.utc) - submitted
    if request["mode"] == "now" and elapsed != timedelta(0):
        raise ValueError("Now pickup instant must equal the frozen submit instant")
    if (
        elapsed < -timedelta(seconds=PAST_CLOCK_TOLERANCE_SECONDS)
        or elapsed > timedelta(seconds=MAX_SCHEDULE_HORIZON_SECONDS)
    ):
        raise ValueError("Scheduled pickup must be within submit tolerance and seven-day horizon")
    return build_training_features(
        local.replace(tzinfo=None),
        request["pickup_location_id"],
        request["dropoff_location_id"],
        calendar=cal,
        centroids=geom,
    )


def create_submission(
    *,
    pickup_location_id: int,
    dropoff_location_id: int,
    mode: str = "now",
    scheduled_local: datetime | str | None = None,
    fold: int | None = None,
    now: datetime | None = None,
    submission_id: str | None = None,
    calendar: HolidayCalendar | None = None,
    centroids: ZoneCentroidLookup | None = None,
) -> dict[str, Any]:
    """Freeze the NYC pickup clock and V2.3 contract/calendar/geometry/tzdb identities at submit."""
    current = now if now is not None else datetime.now(timezone.utc)
    if not isinstance(current, datetime) or current.tzinfo is None or current.utcoffset() != timedelta(0):
        raise ValueError("The injected submit clock must be an aware UTC datetime")
    if mode == "now":
        if scheduled_local is not None or fold is not None:
            raise ValueError("Now mode cannot include a scheduled time or explicit fold")
        pickup = current.astimezone(_nyc())
    elif mode == "scheduled":
        if scheduled_local is None:
            raise ValueError("Scheduled mode requires a NYC pickup datetime")
        parsed = _parse_datetime(scheduled_local, "scheduled_local")
        pickup = _localize(parsed, fold) if parsed.tzinfo is None else _aware_pickup(parsed, fold)
    else:
        raise ValueError("Pretrip v2.3 mode must be now or scheduled")
    cal = calendar or load_calendar()
    geom = centroids or load_zone_centroids()
    snapshot = {
        "contract_version": CONTRACT_VERSION,
        "calendar_version": cal.version,
        "calendar_sha256": cal.sha256,
        "geometry_sha256": geom.sha256,
        "tzdb_sha256": timezone_identity()["tzif_sha256"],
        "submission_id": str(uuid4() if submission_id is None else submission_id),
        "timezone": TIMEZONE_NAME,
        "mode": mode,
        "submitted_at": current.astimezone(timezone.utc).isoformat(),
        "pickup_datetime": pickup.isoformat(),
        "fold": pickup.fold,
        "pickup_location_id": pickup_location_id,
        "dropoff_location_id": dropoff_location_id,
    }
    build_serving_features(snapshot, calendar=cal, centroids=geom)
    return snapshot
