"""Load only a local, topology-validated, checksum-pinned public geometry bundle."""

from __future__ import annotations

import json
from pathlib import Path
import re

from nyc_taxi.dashboard.taxi_zones import GeometryError, TaxiZoneGeometry


ASSET_DIRECTORY = Path(__file__).resolve().parents[3] / "assets" / "taxi_zones"


def load_geometry_bundle(
    geojson_path: str | Path | None = None,
    manifest_path: str | Path | None = None,
    expected_sha256: str | None = None,
) -> TaxiZoneGeometry:
    """An env override may tighten the pin; it cannot bypass pending topology."""
    path = Path(geojson_path) if geojson_path else ASSET_DIRECTORY / "taxi_zones.geojson"
    manifest_file = Path(manifest_path) if manifest_path else ASSET_DIRECTORY / "manifest.json"
    try:
        with manifest_file.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError
        manifest = json.loads(raw)
        digest = manifest.get("geojson_sha256")
        if (
            manifest.get("schema_version") != 1 or manifest.get("status") != "validated"
            or type(manifest.get("feature_count")) is not int or manifest["feature_count"] != 263
            or not isinstance(digest, str) or not re.fullmatch("[0-9a-f]{64}", digest)
            or (expected_sha256 and expected_sha256 != digest)
        ):
            raise ValueError
    except (OSError, ValueError, TypeError, AttributeError):
        raise GeometryError("Pinned geometry manifest is missing, invalid or awaiting topology verification.") from None
    geometry = TaxiZoneGeometry.load(path, digest)
    if geometry.location_ids != tuple(range(1, 264)):
        raise GeometryError("Pinned geometry must contain every Taxi Zone LocationID 1..263 exactly once.")
    return geometry
