"""Offline point-in-polygon lookup for pinned, local Taxi Zone GeoJSON.

Coordinates use GeoJSON's longitude/latitude WGS84 order. There is no nearest
zone fallback: outside, boundary and overlapping matches fail closed. No network
access or optional GIS dependency is required.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any


MAX_GEOMETRY_BYTES = 32 * 1024 * 1024
BOUNDARY_TOLERANCE = 1e-10  # Degrees, deliberately conservative around edges.
_WGS84_NAMES = {
    "EPSG:4326", "urn:ogc:def:crs:EPSG::4326", "urn:ogc:def:crs:OGC:1.3:CRS84",
    "urn:ogc:def:crs:OGC::CRS84", "OGC:CRS84",
}


class GeometryError(RuntimeError):
    """Safe error without user coordinates, file contents or filesystem paths."""

    def __init__(self, message: str, *, code: str = "invalid_geometry") -> None:
        super().__init__(message)
        self.code = code


def validate_coordinates(longitude: Any, latitude: Any) -> tuple[float, float]:
    """Validate numeric WGS84 coordinates; booleans are not numbers here."""
    try:
        valid = (
            type(longitude) in {int, float} and type(latitude) in {int, float}
            and math.isfinite(longitude) and math.isfinite(latitude)
            and -180 <= longitude <= 180 and -90 <= latitude <= 90
        )
    except OverflowError:
        valid = False
    if not valid:
        raise GeometryError("Invalid WGS84 longitude/latitude coordinates.")
    return float(longitude), float(latitude)


def _name(value: Any) -> str:
    if (
        not isinstance(value, str) or not value.strip() or len(value) > 200
        or any(ord(character) < 32 for character in value)
    ):
        raise GeometryError("Taxi Zone metadata is invalid.")
    return value.strip()


def _ring(raw: Any) -> tuple[tuple[float, float], ...]:
    if not isinstance(raw, list) or len(raw) < 4:
        raise GeometryError("Taxi Zone polygon ring is invalid.")
    points = []
    for position in raw:
        if not isinstance(position, list) or len(position) != 2:
            raise GeometryError("Taxi Zone polygons require 2D WGS84 coordinates.")
        points.append(validate_coordinates(*position))
    if points[0] != points[-1] or len(set(points[:-1])) < 3:
        raise GeometryError("Taxi Zone polygon ring must be closed and nondegenerate.")
    if max(point[0] for point in points) - min(point[0] for point in points) > 180:
        raise GeometryError("Antimeridian-crossing Taxi Zone geometry is unsupported.")
    # Work relative to the first point to avoid cancellation at NYC longitudes.
    origin_x, origin_y = points[0]
    area = sum(
        (a[0] - origin_x) * (b[1] - origin_y)
        - (b[0] - origin_x) * (a[1] - origin_y)
        for a, b in zip(points, points[1:])
    )
    if abs(area) <= BOUNDARY_TOLERANCE ** 2:
        raise GeometryError("Taxi Zone polygon ring has no usable area.")
    return tuple(points)


def _ring_relation(point: tuple[float, float], ring: tuple) -> int:
    """Return -1 outside, 0 boundary, 1 inside, including concave rings."""
    x, y = point
    inside = False
    for (ax, ay), (bx, by) in zip(ring, ring[1:]):
        dx, dy = bx - ax, by - ay
        length = math.hypot(dx, dy)
        if (
            min(ax, bx) - BOUNDARY_TOLERANCE <= x <= max(ax, bx) + BOUNDARY_TOLERANCE
            and min(ay, by) - BOUNDARY_TOLERANCE <= y <= max(ay, by) + BOUNDARY_TOLERANCE
            and abs(dx * (y - ay) - dy * (x - ax)) <= BOUNDARY_TOLERANCE * max(length, BOUNDARY_TOLERANCE)
        ):
            return 0
        if (ay > y) != (by > y) and x < ax + (y - ay) * dx / dy:
            inside = not inside
    return 1 if inside else -1


def _polygon_relation(point: tuple[float, float], rings: tuple) -> int:
    outer = _ring_relation(point, rings[0])
    if outer != 1:
        return outer
    for hole in rings[1:]:
        relation = _ring_relation(point, hole)
        if relation == 0:
            return 0
        if relation == 1:
            return -1
    return 1


class TaxiZoneGeometry:
    """Injected FeatureCollection; production callers load a checksum-pinned file."""

    def __init__(self, geojson: dict[str, Any]) -> None:
        if not isinstance(geojson, dict) or geojson.get("type") != "FeatureCollection":
            raise GeometryError("Taxi Zone geometry must be a GeoJSON FeatureCollection.")
        # RFC 7946 omits crs and defines WGS84. Accept an explicit equivalent
        # declaration from older exporters, but never silently reproject NYC feet.
        crs = geojson.get("crs")
        if crs is not None and (
            not isinstance(crs, dict) or crs.get("type") != "name"
            or not isinstance(crs.get("properties"), dict)
            or not isinstance(crs["properties"].get("name"), str)
            or crs["properties"].get("name") not in _WGS84_NAMES
        ):
            raise GeometryError("Taxi Zone geometry must use WGS84 longitude/latitude.")
        features = geojson.get("features")
        if not isinstance(features, list) or not features:
            raise GeometryError("Taxi Zone geometry contains no features.")
        self._zones = []
        seen_ids = set()
        for feature in features:
            if not isinstance(feature, dict) or feature.get("type") != "Feature":
                raise GeometryError("Taxi Zone GeoJSON feature is invalid.")
            properties, geometry = feature.get("properties"), feature.get("geometry")
            if not isinstance(properties, dict) or not isinstance(geometry, dict):
                raise GeometryError("Taxi Zone feature requires geometry and metadata.")
            location_id = properties.get("LocationID", properties.get("location_id", properties.get("locationid")))
            # Official SHP-to-GeoJSON conversions sometimes encode IDs as strings.
            if isinstance(location_id, str) and re.fullmatch(r"[0-9]{1,3}", location_id):
                location_id = int(location_id)
            if type(location_id) is not int or not 1 <= location_id <= 263 or location_id in seen_ids:
                raise GeometryError("Taxi Zone LocationID must be unique and within 1..263.")
            seen_ids.add(location_id)
            metadata = {
                "location_id": location_id,
                "borough": _name(properties.get("Borough", properties.get("borough"))),
                "zone": _name(properties.get("Zone", properties.get("zone"))),
            }
            raw_polygons = geometry.get("coordinates")
            if geometry.get("type") == "Polygon":
                raw_polygons = [raw_polygons]
            elif geometry.get("type") != "MultiPolygon":
                raise GeometryError("Taxi Zones support Polygon or MultiPolygon geometry only.")
            if not isinstance(raw_polygons, list) or not raw_polygons:
                raise GeometryError("Taxi Zone geometry has no polygons.")
            polygons = []
            for raw_polygon in raw_polygons:
                if not isinstance(raw_polygon, list) or not raw_polygon:
                    raise GeometryError("Taxi Zone polygon requires an outer ring.")
                rings = tuple(_ring(raw) for raw in raw_polygon)
                outer = rings[0]
                bounds = (
                    min(point[0] for point in outer), min(point[1] for point in outer),
                    max(point[0] for point in outer), max(point[1] for point in outer),
                )
                polygons.append((bounds, rings))
            self._zones.append((metadata, tuple(polygons)))

    @property
    def location_ids(self) -> tuple[int, ...]:
        """Public geometry inventory; 264/265 are non-geographic sentinels."""
        return tuple(sorted(metadata["location_id"] for metadata, _polygons in self._zones))

    def zone_feature(self, location_id: int) -> dict[str, Any] | None:
        """Return a fresh GeoJSON Feature for a single LocationID (for map polygon overlay)."""
        if type(location_id) is not int or not 1 <= location_id <= 263:
            return None
        for metadata, polygons in self._zones:
            if metadata["location_id"] == location_id:
                multi_coords = [
                    [[[float(lon), float(lat)] for lon, lat in ring] for ring in rings]
                    for _bounds, rings in polygons
                ]
                return {
                    "type": "Feature",
                    "properties": dict(metadata),
                    "geometry": {
                        "type": "Polygon" if len(multi_coords) == 1 else "MultiPolygon",
                        "coordinates": multi_coords[0] if len(multi_coords) == 1 else multi_coords,
                    },
                }
        return None

    @classmethod
    def load(cls, path: str | Path, expected_sha256: str) -> TaxiZoneGeometry:
        """Load local bytes only; never fetch missing assets or skip verification."""
        if not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
            raise GeometryError("A full lowercase SHA256 is required for Taxi Zone geometry.")
        try:
            with Path(path).open("rb") as stream:
                content = stream.read(MAX_GEOMETRY_BYTES + 1)
        except (OSError, TypeError, ValueError):
            raise GeometryError("Pinned Taxi Zone geometry is unavailable; prepare the local asset first.") from None
        if len(content) > MAX_GEOMETRY_BYTES:
            raise GeometryError("Taxi Zone geometry exceeds the supported file size.")
        if hashlib.sha256(content).hexdigest() != expected_sha256:
            raise GeometryError("Taxi Zone geometry checksum does not match the pinned asset.")
        try:
            document = json.loads(content)
        except (ValueError, UnicodeDecodeError, RecursionError):
            raise GeometryError("Taxi Zone geometry is not valid GeoJSON.") from None
        return cls(document)

    def resolve(self, longitude: Any, latitude: Any) -> dict[str, Any]:
        point = validate_coordinates(longitude, latitude)
        x, y = point
        matches = []
        for metadata, polygons in self._zones:
            for (west, south, east, north), rings in polygons:
                if not (
                    west - BOUNDARY_TOLERANCE <= x <= east + BOUNDARY_TOLERANCE
                    and south - BOUNDARY_TOLERANCE <= y <= north + BOUNDARY_TOLERANCE
                ):
                    continue
                relation = _polygon_relation(point, rings)
                if relation == 0:
                    raise GeometryError("Location lies on an ambiguous Taxi Zone boundary.", code="boundary")
                if relation == 1:
                    matches.append(metadata)
        if len(matches) > 1:
            raise GeometryError("Location matches overlapping Taxi Zones; choose an unambiguous point.", code="overlap")
        if not matches:
            raise GeometryError("Location is outside the supported Taxi Zones.", code="outside_zones")
        return dict(matches[0])
