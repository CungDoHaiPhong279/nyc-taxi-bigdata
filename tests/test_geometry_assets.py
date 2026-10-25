"""Public geometry pin/readiness checks; tiny generated polygons, no network/GIS."""

from hashlib import sha256
import json

import pytest

from nyc_taxi.dashboard.geometry_assets import load_geometry_bundle
from nyc_taxi.dashboard.taxi_zones import GeometryError


@pytest.fixture
def bundle(tmp_path):
    features = []
    for location in range(1, 264):
        west = -74 + location * .0002
        east = west + .0001
        features.append({"type": "Feature", "properties": {
            "LocationID": location, "Borough": "Manhattan", "Zone": "Synthetic zone"},
            "geometry": {"type": "Polygon", "coordinates": [[[west, 40.7], [east, 40.7],
                          [east, 40.8], [west, 40.8], [west, 40.7]]]}})
    raw = json.dumps({"type": "FeatureCollection", "features": features}).encode()
    path = tmp_path / "geometry.geojson"
    path.write_bytes(raw)
    manifest = {"schema_version": 1, "status": "validated", "feature_count": 263,
                "geojson_sha256": sha256(raw).hexdigest()}
    pin = tmp_path / "manifest.json"
    pin.write_text(json.dumps(manifest), encoding="utf-8")
    return path, pin, manifest


def test_validated_bundle_loads_all_ids_and_can_resolve(bundle):
    path, pin, manifest = bundle
    geometry = load_geometry_bundle(path, pin, manifest["geojson_sha256"])
    assert geometry.location_ids == tuple(range(1, 264))
    assert geometry.resolve(-74 + 2 * .0002 + .00005, 40.75)["location_id"] == 2


@pytest.mark.parametrize("field,value", [("status", "pending_topology"), ("schema_version", 2),
                                       ("feature_count", 262), ("geojson_sha256", "short")])
def test_unverified_or_incomplete_manifest_cannot_enable_geometry(bundle, field, value):
    path, pin, manifest = bundle
    manifest[field] = value
    pin.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(GeometryError):
        load_geometry_bundle(path, pin)


def test_env_pin_cannot_bypass_pending_topology_or_replace_the_manifest_pin(bundle):
    path, pin, manifest = bundle
    with pytest.raises(GeometryError):
        load_geometry_bundle(path, pin, "0" * 64)
    manifest["status"] = "pending_topology"
    pin.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(GeometryError):
        load_geometry_bundle(path, pin, manifest["geojson_sha256"])


def test_asset_hash_tampering_and_missing_manifest_fail_closed(bundle):
    path, pin, _ = bundle
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(GeometryError):
        load_geometry_bundle(path, pin)
    with pytest.raises(GeometryError):
        load_geometry_bundle(path, pin.with_name("missing.json"))


def test_correct_hash_does_not_allow_missing_location_ids(bundle):
    path, pin, manifest = bundle
    document = json.loads(path.read_bytes())
    document["features"].pop()
    raw = json.dumps(document).encode()
    path.write_bytes(raw)
    manifest["geojson_sha256"] = sha256(raw).hexdigest()
    pin.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(GeometryError):
        load_geometry_bundle(path, pin)


def test_preparation_preserves_geometry_and_rejects_lookup_mismatch(bundle):
    from scripts.prepare_taxi_zone_geometry import GeometryPreparationError, normalize_geojson
    path, _pin, _manifest = bundle
    document = json.loads(path.read_bytes())
    # A real hole topology case independent of production data (which has none).
    first = document["features"][0]["geometry"]["coordinates"]
    west = first[0][0][0]
    first.append([[west + .00002, 40.72], [west + .00002, 40.73],
                  [west + .00004, 40.73], [west + .00004, 40.72], [west + .00002, 40.72]])
    rows = [{"LocationID": str(item["properties"]["LocationID"]),
             "Borough": item["properties"]["Borough"], "Zone": item["properties"]["Zone"]}
            for item in document["features"]]
    normalized, counts = normalize_geojson(document, rows)
    assert normalized["features"][0]["geometry"] == document["features"][0]["geometry"]
    assert counts["polygon_count"] == 263 and counts["hole_count"] == 1
    rows[0]["Borough"] = "Synthetic mismatch"
    with pytest.raises(GeometryPreparationError, match="Lookup mismatch"):
        normalize_geojson(document, rows)


def test_preparation_refuses_existing_outputs_and_invalid_topology_report(bundle, tmp_path):
    from scripts.prepare_taxi_zone_geometry import GeometryPreparationError, prepare_geometry, finalize_geometry
    path, pin, manifest = bundle
    with pytest.raises(GeometryPreparationError, match="overwrite"):
        prepare_geometry(path, pin.parent, tmp_path / "not-needed.csv")
    manifest.update(status="pending_topology", polygon_count=263, hole_count=0)
    pin.write_text(json.dumps(manifest), encoding="utf-8")
    # Finalizer expects canonical output names, separate from the prior fixture.
    output = tmp_path / "pending"
    output.mkdir()
    (output / "taxi_zones.geojson").write_bytes(path.read_bytes())
    (output / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    report = tmp_path / "bad-report.json"
    report.write_text(json.dumps({"schema_version": 1, "geojson_sha256": manifest["geojson_sha256"],
                                 "feature_count": 263, "polygon_count": 263, "hole_count": 0,
                                 "invalid": [{"location_id": 1, "reason": "Synthetic invalid"}]}), encoding="utf-8")
    with pytest.raises(GeometryPreparationError, match="Topology report"):
        finalize_geometry(output, report)
    assert json.loads((output / "manifest.json").read_bytes())["status"] == "pending_topology"
    assert not (output / "topology_validation.json").exists()


def test_download_budget_never_reads_beyond_cap_or_retries(tmp_path, monkeypatch):
    from unittest.mock import MagicMock, Mock
    from scripts import prepare_taxi_zone_geometry as prep
    response = MagicMock()
    response.__enter__.return_value = response
    response.status = 200
    response.headers = {}
    response.read.side_effect = lambda limit: b"x" * limit
    opener = Mock()
    opener.open.return_value = response
    monkeypatch.setattr(prep, "MAX_SOURCE_BYTES", 32)
    monkeypatch.setattr(prep.urllib.request, "build_opener", Mock(return_value=opener))
    target = tmp_path / "source.zip"
    with pytest.raises(prep.GeometryPreparationError):
        prep.download_source(target)
    response.read.assert_called_once_with(32)
    opener.open.assert_called_once()
    assert not target.exists()
