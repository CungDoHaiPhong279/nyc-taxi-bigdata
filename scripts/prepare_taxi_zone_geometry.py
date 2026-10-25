"""Prepare a pinned Taxi Zone asset without changing source boundaries.

Only the explicit --download option makes a bounded, tokenless request to the
official NYC Open Data endpoint. Topology validation is a separate local step;
this generator never repairs, simplifies, rounds or drops geometry.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import sys
from typing import Any
import urllib.error
import urllib.request
import zipfile


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi.dashboard.taxi_zones import GeometryError, TaxiZoneGeometry


GEOJSON_SOURCE_URL = (
    "https://data.cityofnewyork.us/api/v3/views/8meu-9t5y/query.geojson"
    "?accessType=DOWNLOAD"
)
SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
SOURCE_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
MAX_SOURCE_BYTES = 20 * 1024 * 1024
DOWNLOAD_TIMEOUT_SECONDS = 20
GEOJSON_NAME = "taxi_zones.geojson"
MANIFEST_NAME = "manifest.json"


class GeometryPreparationError(ValueError):
    """Preparation failed without exposing coordinate or source payload data."""


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _canonical(document: Any) -> bytes:
    return (json.dumps(document, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def _read_bounded(path: Path) -> bytes:
    try:
        with path.open("rb") as stream:
            content = stream.read(MAX_SOURCE_BYTES + 1)
    except OSError:
        raise GeometryPreparationError("A required local input is unavailable.") from None
    if not content or len(content) > MAX_SOURCE_BYTES:
        raise GeometryPreparationError("Input is empty or exceeds the 20 MiB limit.")
    return content


def _json_document(content: bytes) -> Any:
    try:
        return json.loads(content)
    except (ValueError, UnicodeDecodeError, RecursionError):
        raise GeometryPreparationError("Input is not valid JSON.") from None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def download_source(source_file: Path) -> None:
    """One request, no retries/redirects; preserve raw official bytes separately."""
    if source_file.exists():
        raise GeometryPreparationError("Raw source already exists; use it without --download.")
    if source_file.suffix.lower() != ".zip":
        raise GeometryPreparationError("Official download requires a .zip source filename.")
    request = urllib.request.Request(SOURCE_URL, headers={"Accept": "application/zip"})
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        with opener.open(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            if response.status != 200:
                raise GeometryPreparationError("Official source did not return HTTP 200.")
            declared_length = response.headers.get("Content-Length")
            if declared_length is not None and int(declared_length) > MAX_SOURCE_BYTES:
                raise GeometryPreparationError("Official source exceeds the 20 MiB download limit.")
            content = response.read(MAX_SOURCE_BYTES)
    except GeometryPreparationError:
        raise
    except (OSError, ValueError, urllib.error.URLError):
        raise GeometryPreparationError("Official source download failed; no retry was attempted.") from None
    if not content or len(content) >= MAX_SOURCE_BYTES:
        raise GeometryPreparationError("Official source is empty or exceeds the 20 MiB download limit.")
    if not zipfile.is_zipfile(io.BytesIO(content)):
        raise GeometryPreparationError("Official source is not a ZIP archive.")
    source_file.parent.mkdir(parents=True, exist_ok=True)
    with source_file.open("xb") as stream:
        stream.write(content)


def read_lookup(lookup_path: Path) -> list[dict[str, str]]:
    content = _read_bounded(lookup_path)
    try:
        return list(csv.DictReader(content.decode("utf-8-sig").splitlines()))
    except (ValueError, UnicodeDecodeError, csv.Error):
        raise GeometryPreparationError("Lookup CSV is invalid.") from None


def _geometry_counts(geometry: dict[str, Any]) -> tuple[int, int]:
    kind = geometry.get("type")
    coordinates = geometry.get("coordinates")
    if kind not in {"Polygon", "MultiPolygon"} or not isinstance(coordinates, (list, tuple)):
        raise GeometryPreparationError("Shapefile conversion requires Polygon/MultiPolygon geometry.")
    polygons = [coordinates] if kind == "Polygon" else coordinates
    return sum(len(polygon) for polygon in polygons), sum(
        len(ring) for polygon in polygons for ring in polygon
    )


def shapefile_zip_document(content: bytes) -> tuple[dict[str, Any], dict[str, Any]]:
    """Project the official SHP in memory; never extract files or repair rings."""
    try:
        import shapefile
        import pyproj
    except ImportError:
        raise GeometryPreparationError(
            "ZIP preparation requires the optional pinned requirements-geometry-tools.txt tools."
        ) from None
    if shapefile.__version__ != "2.3.1" or pyproj.__version__ != "3.7.2":
        raise GeometryPreparationError("Geometry preparation tool versions do not match the pinned requirements.")
    allowed = {f"taxi_zones/taxi_zones.{suffix}" for suffix in ("shp", "shx", "dbf", "prj", "cpg")}
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            members = [item for item in archive.infolist() if not item.is_dir()]
            if (
                {item.filename for item in members} != allowed or len(members) != len(allowed)
                or any(item.flag_bits & 1 for item in members)
                or sum(item.file_size for item in members) > MAX_SOURCE_BYTES
            ):
                raise GeometryPreparationError("Shapefile ZIP members or expanded size do not match the expected official archive.")
            parts = {Path(item.filename).suffix: archive.read(item) for item in members}
        prj = parts[".prj"].decode("utf-8")
        encoding = parts[".cpg"].decode("ascii").strip()
        if encoding.upper() not in {"UTF-8", "UTF8", "65001"}:
            raise GeometryPreparationError("Official shapefile character encoding is not the expected UTF-8.")
        pyproj.network.set_network_enabled(False)
        source_crs = pyproj.CRS.from_wkt(prj)
        if source_crs.to_epsg() != 2263:
            raise GeometryPreparationError("Official shapefile CRS must resolve to EPSG:2263.")
        transformer = pyproj.Transformer.from_crs(
            source_crs, pyproj.CRS.from_epsg(4326), always_xy=True,
            allow_ballpark=False, only_best=True,
        )
        reader = shapefile.Reader(
            shp=io.BytesIO(parts[".shp"]), shx=io.BytesIO(parts[".shx"]),
            dbf=io.BytesIO(parts[".dbf"]), encoding="utf-8",
        )
        features = []
        source_ring_count = source_vertex_count = 0
        try:
            if reader.shapeType != shapefile.POLYGON:
                raise GeometryPreparationError("Official shapefile must contain 2D polygons.")
            for item in reader.iterShapeRecords():
                if item.shape.shapeType != shapefile.POLYGON:
                    raise GeometryPreparationError("Official shapefile contains an unsupported shape.")
                geometry = item.shape.__geo_interface__
                rings, vertices = _geometry_counts(geometry)
                if rings != len(item.shape.parts) or vertices != len(item.shape.points):
                    raise GeometryPreparationError("Shapefile conversion changed the number of source rings or vertices.")
                source_ring_count += rings
                source_vertex_count += vertices

                def project(position):
                    if len(position) != 2:
                        raise GeometryPreparationError("Official shapefile position must be two-dimensional.")
                    longitude, latitude = transformer.transform(*position, errcheck=True)
                    return [longitude, latitude]

                coordinates = geometry["coordinates"]
                if geometry["type"] == "Polygon":
                    projected = [[project(point) for point in ring] for ring in coordinates]
                else:
                    projected = [
                        [[project(point) for point in ring] for ring in polygon]
                        for polygon in coordinates
                    ]
                projected_geometry = {"type": geometry["type"], "coordinates": projected}
                if _geometry_counts(projected_geometry) != (rings, vertices):
                    raise GeometryPreparationError("Projection changed source rings or vertices.")
                record = item.record.as_dict()
                features.append({
                    "type": "Feature", "properties": {
                        "LocationID": record.get("LocationID"),
                        "Borough": record.get("borough"), "Zone": record.get("zone"),
                    }, "geometry": projected_geometry,
                })
        finally:
            reader.close()
        metadata = {
            "input_format": "ESRI Shapefile ZIP", "input_crs": "EPSG:2263",
            "input_prj_sha256": _sha256(parts[".prj"]), "input_crs_wkt": prj,
            "output_crs": "OGC:CRS84",
            "always_xy": True, "network_enabled": False, "allow_ballpark": False,
            "only_best": True, "transformer": transformer.description,
            "transformer_accuracy_m": transformer.accuracy,
            "pyshp_version": shapefile.__version__, "pyproj_version": pyproj.__version__,
            "proj_version": pyproj.proj_version_str,
            "source_ring_count": source_ring_count, "source_vertex_count": source_vertex_count,
            "repaired_geometry": False, "simplified_geometry": False,
        }
        return {"type": "FeatureCollection", "features": features}, metadata
    except GeometryPreparationError:
        raise
    except Exception:
        # Exceptions from GIS libraries may contain geometry or projected data.
        raise GeometryPreparationError("Official shapefile decoding/projection failed without modifying geometry.") from None


def normalize_geojson(
    document: Any, lookup_rows: list[dict[str, str]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Normalize metadata only, retaining every coordinate, polygon and hole."""
    if not isinstance(document, dict) or document.get("type") != "FeatureCollection":
        raise GeometryPreparationError("Source must be a GeoJSON FeatureCollection.")
    features = document.get("features")
    if not isinstance(features, list) or len(features) != 263:
        raise GeometryPreparationError("Source must contain exactly 263 Taxi Zone features.")
    try:
        lookup = {int(row["LocationID"]): row for row in lookup_rows}
    except (KeyError, TypeError, ValueError):
        raise GeometryPreparationError("Lookup IDs are invalid.") from None
    if len(lookup) != len(lookup_rows) or not set(range(1, 264)).issubset(lookup):
        raise GeometryPreparationError("Lookup must contain each Taxi Zone ID 1..263 exactly once.")
    normalized_features = []
    seen = set()
    polygon_count = hole_count = 0
    geometry_types: dict[str, int] = {}
    for feature in features:
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise GeometryPreparationError("Source contains an invalid feature.")
        properties = feature.get("properties")
        geometry = feature.get("geometry")
        if not isinstance(properties, dict) or not isinstance(geometry, dict):
            raise GeometryPreparationError("Source feature requires metadata and geometry.")
        raw_id = properties.get("LocationID", properties.get("locationid"))
        if type(raw_id) is bool or not (
            type(raw_id) is int or isinstance(raw_id, str) and raw_id.isdigit()
        ):
            raise GeometryPreparationError("Source LocationID is invalid.")
        location_id = int(raw_id)
        if location_id not in range(1, 264) or location_id in seen:
            raise GeometryPreparationError("Source IDs must be unique within 1..263.")
        seen.add(location_id)
        borough = properties.get("Borough", properties.get("borough"))
        zone = properties.get("Zone", properties.get("zone"))
        for field, actual in (("Borough", borough), ("Zone", zone)):
            if lookup[location_id].get(field) != actual:
                raise GeometryPreparationError(f"Lookup mismatch: LocationID {location_id}, field {field}.")
        kind = geometry.get("type")
        coordinates = geometry.get("coordinates")
        if kind not in {"Polygon", "MultiPolygon"} or not isinstance(coordinates, list):
            raise GeometryPreparationError("Source requires Polygon/MultiPolygon geometry.")
        polygons = [coordinates] if kind == "Polygon" else coordinates
        geometry_types[kind] = geometry_types.get(kind, 0) + 1
        for polygon in polygons:
            if not isinstance(polygon, list) or not polygon:
                raise GeometryPreparationError("Source polygon has no rings.")
            polygon_count += 1
            hole_count += len(polygon) - 1
            for ring in polygon:
                if not isinstance(ring, list):
                    raise GeometryPreparationError("Source polygon ring is invalid.")
                for point in ring:
                    if (
                        not isinstance(point, list) or len(point) != 2
                        or any(type(value) not in {int, float} or not math.isfinite(value) for value in point)
                        or not -74.5 <= point[0] <= -73.5 or not 40.4 <= point[1] <= 41.1
                    ):
                        raise GeometryPreparationError("Source is outside the expected NYC/EWR WGS84 bounds.")
        normalized_features.append({
            "type": "Feature", "properties": {
                "LocationID": location_id, "Borough": borough, "Zone": zone,
            }, "geometry": geometry,
        })
    normalized_features.sort(key=lambda item: item["properties"]["LocationID"])
    normalized = {"type": "FeatureCollection", "features": normalized_features}
    if "crs" in document:
        normalized["crs"] = document["crs"]
    try:
        TaxiZoneGeometry(normalized)
    except GeometryError as exc:
        raise GeometryPreparationError(str(exc)) from None
    return normalized, {
        "feature_count": len(normalized_features), "polygon_count": polygon_count,
        "hole_count": hole_count, "geometry_types": geometry_types,
    }


def prepare_geometry(source_file: Path, output_dir: Path, lookup_path: Path) -> dict[str, Any]:
    """Create an asset pending independent topology validation, never overwrite."""
    targets = [output_dir / name for name in (GEOJSON_NAME, MANIFEST_NAME, "PROVENANCE.md")]
    if any(path.exists() for path in targets):
        raise GeometryPreparationError("Prepared output already exists; refusing to overwrite.")
    source_content = _read_bounded(source_file)
    if zipfile.is_zipfile(io.BytesIO(source_content)):
        document, projection = shapefile_zip_document(source_content)
        source_url = SOURCE_URL
    else:
        document = _json_document(source_content)
        projection = {"input_format": "GeoJSON", "input_crs": "OGC:CRS84", "output_crs": "OGC:CRS84"}
        source_url = GEOJSON_SOURCE_URL
    normalized, counts = normalize_geojson(document, read_lookup(lookup_path))
    geojson_content = _canonical(normalized)
    manifest = {
        "schema_version": 1, "status": "pending_topology", **counts,
        "geojson_sha256": _sha256(geojson_content), "geojson_bytes": len(geojson_content),
        "source_url": source_url, "source_page": SOURCE_PAGE,
        "source_sha256": _sha256(source_content), "source_bytes": len(source_content),
        "lookup_sha256": _sha256(_read_bounded(lookup_path)), "crs": "OGC:CRS84",
        "projection": projection,
        "normalization": "Metadata keys and feature order; CRS projection only when required. No vertex/ring removal, repair, simplification or rounding.",
    }
    provenance = f"""# Taxi Zone geometry provenance

Official publisher: NYC Open Data / NYC Taxi and Limousine Commission.
Dataset page: {SOURCE_PAGE}
Source endpoint: {source_url}
Source SHA256: `{manifest['source_sha256']}` ({len(source_content)} bytes).
Asset SHA256: `{manifest['geojson_sha256']}` ({len(geojson_content)} bytes).

The source archive CRS is identified from its .prj and must be EPSG:2263
(NAD83 / New York Long Island, US survey feet). Pinned optional preparation
tools pyshp 2.3.1 and pyproj 3.7.2 transform it to WGS84 longitude/latitude with
always_xy=True, allow_ballpark=False, only_best=True and PROJ network disabled.
The manifest records source WKT/hash, tool/PROJ versions, operation and stated
transformation accuracy. No remote grid is downloaded. Every source vertex and
ring is retained; Polygon/MultiPolygon parts and holes are counted before and
after conversion. Floating-point CRS projection is the only coordinate change.
GeoJSON input already in WGS84 skips this projection. No geometry is repaired,
simplified, rounded, dropped or replaced. Metadata keys and feature ordering
are normalized. The local lookup CSV
must match all 263 geographic LocationIDs and names. Lookup IDs 264/265 have no
geographic polygon. No geometry is repaired, simplified, rounded or omitted.

Usage terms remain those of the publisher; this file asserts no new license.
See the dataset page and https://opendata.cityofnewyork.us/overview/#termsofuse.

Reproduce from the repository root into a NEW private output directory
(one request, maximum 20 MiB, timeout 20 s). Do not overwrite the tracked asset:

```powershell
$env:PYTHONPATH = (Resolve-Path .sandbox/address-2b/geometry-tools).Path
python scripts/prepare_taxi_zone_geometry.py --download --source-file .sandbox/address-2b/source.zip --output .sandbox/address-2b/reproduced
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/verify_taxi_zone_geometry.ps1 -GeoJsonPath .sandbox/address-2b/reproduced/taxi_zones.geojson | Out-File -Encoding utf8 .sandbox/address-2b/topology.json
if ($LASTEXITCODE -ne 0) {{ throw 'Topology verification failed.' }}
python scripts/prepare_taxi_zone_geometry.py --finalize --output .sandbox/address-2b/reproduced --topology-report .sandbox/address-2b/topology.json
```

If the raw source exists, omit `--download`; generation refuses to overwrite
existing outputs. The downloaded source is retained outside the tracked asset.
Install optional preparation tools separately from the dashboard runtime using
requirements-geometry-tools.txt into the isolated geometry-tools directory
(`python -m pip install --no-cache-dir --target .sandbox/address-2b/geometry-tools -r requirements-geometry-tools.txt`).
The tools directory is needed only for Shapefile reprojection. Inspect the new
asset hash against the pinned manifest before considering any tracked update.
Topology must be independently validated before enabling serving; the dashboard
requires no pyshp, pyproj or SQL spatial dependency.
"""
    output_dir.mkdir(parents=True, exist_ok=True)
    for path, content in zip(targets, (geojson_content, _canonical(manifest), provenance.encode("utf-8"))):
        with path.open("xb") as stream:
            stream.write(content)
    return manifest


def finalize_geometry(output_dir: Path, topology_report_path: Path) -> dict[str, Any]:
    """Pin an independently generated, matching topology report without repair."""
    manifest_path = output_dir / MANIFEST_NAME
    manifest = _json_document(_read_bounded(manifest_path))
    geometry_content = _read_bounded(output_dir / GEOJSON_NAME)
    report_content = _read_bounded(topology_report_path)
    report = _json_document(report_content)
    validator = report.get("validator") if isinstance(report, dict) else None
    if (
        not isinstance(manifest, dict) or manifest.get("status") != "pending_topology"
        or manifest.get("schema_version") != 1
        or manifest.get("geojson_sha256") != _sha256(geometry_content)
        or not isinstance(report, dict) or report.get("schema_version") != 1
        or report.get("geojson_sha256") != manifest.get("geojson_sha256")
        or report.get("feature_count") != 263 or report.get("invalid") != []
        or any(report.get(key) != manifest.get(key) for key in ("polygon_count", "hole_count"))
        or not isinstance(validator, dict) or validator.get("repaired_geometry") is not False
        or not isinstance(validator.get("name"), str)
        or not isinstance(validator.get("assembly_version"), str)
        or not isinstance(validator.get("assembly_sha256"), str)
        or len(validator["assembly_sha256"]) != 64
    ):
        raise GeometryPreparationError("Topology report is incomplete, invalid or does not match the prepared asset.")
    topology_path = output_dir / "topology_validation.json"
    if topology_path.exists():
        raise GeometryPreparationError("Pinned topology report already exists; refusing to overwrite.")
    # Copy the report unchanged; no coordinate or per-user data is generated.
    with topology_path.open("xb") as stream:
        stream.write(report_content)
    manifest["status"] = "validated"
    manifest["topology"] = {
        "report_file": topology_path.name, "report_sha256": _sha256(report_content),
        "invalid_feature_count": 0, "validator": validator,
    }
    manifest_path.write_bytes(_canonical(manifest))
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-file", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--lookup", type=Path, default=PROJECT_ROOT / "data/raw/taxi_zones/taxi_zone_lookup.csv")
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--finalize", action="store_true")
    parser.add_argument("--topology-report", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.finalize:
            if args.source_file or args.download or not args.topology_report:
                parser.error("--finalize requires --topology-report and excludes --source-file/--download")
            manifest = finalize_geometry(args.output, args.topology_report)
        else:
            if not args.source_file or args.topology_report:
                parser.error("preparation requires --source-file and excludes --topology-report")
            if args.download:
                download_source(args.source_file)
            manifest = prepare_geometry(args.source_file, args.output, args.lookup)
    except (GeometryPreparationError, OSError) as exc:
        print(str(exc) if isinstance(exc, GeometryPreparationError) else "Local geometry output could not be written.", file=sys.stderr)
        return 1
    print(json.dumps({key: manifest[key] for key in ("status", "feature_count", "polygon_count", "hole_count", "geojson_sha256", "geojson_bytes")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
