"""Synthetic polygons only: no downloaded geometry or real address responses."""

import hashlib
import json
import math

import pytest

from nyc_taxi.dashboard.taxi_zones import GeometryError, TaxiZoneGeometry


def rectangle(west=-74.0, south=40.7, east=-73.9, north=40.8):
    return [[west, south], [east, south], [east, north], [west, north], [west, south]]


def feature(location_id=161, *, coordinates=None, geometry_type="Polygon", borough="Manhattan"):
    return {
        "type": "Feature",
        "properties": {"LocationID": location_id, "Borough": borough, "zone": "Synthetic zone"},
        "geometry": {"type": geometry_type, "coordinates": coordinates or [rectangle()]},
    }


def collection(*features):
    return {"type": "FeatureCollection", "features": list(features)}


def test_maps_polygon_with_metadata_without_nearest_fallback():
    zones = TaxiZoneGeometry(collection(feature()))
    assert zones.resolve(-73.95, 40.75) == {
        "location_id": 161, "borough": "Manhattan", "zone": "Synthetic zone",
    }
    with pytest.raises(GeometryError, match="outside") as caught:
        zones.resolve(-74.5, 40.75)
    assert caught.value.code == "outside_zones"


def test_hole_excludes_interior_and_rejects_boundary():
    zones = TaxiZoneGeometry(collection(feature(coordinates=[rectangle(), rectangle(-73.98, 40.72, -73.96, 40.74)])))
    assert zones.resolve(-73.94, 40.75)["location_id"] == 161
    with pytest.raises(GeometryError, match="outside") as outside:
        zones.resolve(-73.97, 40.73)
    assert outside.value.code == "outside_zones"
    with pytest.raises(GeometryError, match="boundary") as boundary:
        zones.resolve(-73.98, 40.73)
    assert boundary.value.code == "boundary"


def test_multipolygon_disjoint_parts():
    zones = TaxiZoneGeometry(collection(feature(
        geometry_type="MultiPolygon",
        coordinates=[[rectangle()], [rectangle(-73.8, 40.7, -73.7, 40.8)]],
    )))
    assert zones.resolve(-73.95, 40.75)["location_id"] == 161
    assert zones.resolve(-73.75, 40.75)["location_id"] == 161


def test_concave_polygon_does_not_use_bounding_box_as_membership():
    ring = [[-74, 40.7], [-73.9, 40.7], [-73.9, 40.72], [-73.98, 40.72], [-73.98, 40.8], [-74, 40.8], [-74, 40.7]]
    zones = TaxiZoneGeometry(collection(feature(coordinates=[ring])))
    assert zones.resolve(-73.99, 40.75)["location_id"] == 161
    with pytest.raises(GeometryError, match="outside"):
        zones.resolve(-73.95, 40.75)


@pytest.mark.parametrize("point", [(-74, 40.75), (-74, 40.7), (-73.95, 40.8)])
def test_outer_edges_and_vertices_fail_closed(point):
    with pytest.raises(GeometryError, match="boundary") as caught:
        TaxiZoneGeometry(collection(feature())).resolve(*point)
    assert caught.value.code == "boundary"


def test_overlapping_features_fail_closed():
    zones = TaxiZoneGeometry(collection(feature(), feature(237)))
    with pytest.raises(GeometryError, match="overlapping") as caught:
        zones.resolve(-73.95, 40.75)
    assert caught.value.code == "overlap"


@pytest.mark.parametrize("point", [(True, 40.75), (-73.95, False), (math.inf, 40.75), (-73.95, math.nan), (181, 40.75), (-73.95, -91), ("-73.95", 40.75)])
def test_invalid_coordinates_do_not_resolve(point):
    with pytest.raises(GeometryError, match="coordinates") as caught:
        TaxiZoneGeometry(collection(feature())).resolve(*point)
    assert caught.value.code == "invalid_geometry"


@pytest.mark.parametrize("location_id", [True, 0, 264, -1, "unknown"])
def test_invalid_zone_ids_rejected(location_id):
    with pytest.raises(GeometryError, match="LocationID"):
        TaxiZoneGeometry(collection(feature(location_id)))


def test_legacy_wgs84_and_official_string_id_supported():
    data = collection(feature("161"))
    data["crs"] = {"type": "name", "properties": {"name": "EPSG:4326"}}
    assert TaxiZoneGeometry(data).resolve(-73.95, 40.75)["location_id"] == 161


@pytest.mark.parametrize("crs", [{"type": "name", "properties": {"name": "EPSG:2263"}}, {"type": "name", "properties": {"name": []}}, "EPSG:4326", {}])
def test_unexpected_crs_rejected(crs):
    data = collection(feature())
    data["crs"] = crs
    with pytest.raises(GeometryError, match="WGS84"):
        TaxiZoneGeometry(data)


def test_degenerate_or_unclosed_ring_rejected():
    with pytest.raises(GeometryError):
        TaxiZoneGeometry(collection(feature(coordinates=[[[-74, 40.7], [-73.9, 40.7], [-73.9, 40.8], [-74, 40.8]]])))
    with pytest.raises(GeometryError):
        TaxiZoneGeometry(collection(feature(coordinates=[[[-74, 40.7], [-73.95, 40.7], [-73.9, 40.7], [-74, 40.7]]])))


def test_pinned_local_loading_and_hash_failure(tmp_path):
    path = tmp_path / "synthetic-zones.json"
    content = json.dumps(collection(feature())).encode()
    path.write_bytes(content)
    zones = TaxiZoneGeometry.load(path, hashlib.sha256(content).hexdigest())
    assert zones.resolve(-73.95, 40.75)["location_id"] == 161
    with pytest.raises(GeometryError, match="checksum"):
        TaxiZoneGeometry.load(path, "0" * 64)


def test_missing_asset_and_invalid_hash_fail_safely(tmp_path):
    with pytest.raises(GeometryError, match="unavailable"):
        TaxiZoneGeometry.load(tmp_path / "absent.json", "0" * 64)
    with pytest.raises(GeometryError, match="SHA256"):
        TaxiZoneGeometry.load(tmp_path / "absent.json", "short")


def test_rejects_invalid_json_with_matching_checksum(tmp_path):
    path = tmp_path / "bad.json"
    path.write_bytes(b"not json")
    with pytest.raises(GeometryError, match="GeoJSON"):
        TaxiZoneGeometry.load(path, hashlib.sha256(b"not json").hexdigest())
