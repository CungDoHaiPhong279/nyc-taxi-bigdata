# Taxi Zone geometry provenance

Official publisher: NYC Open Data / NYC Taxi and Limousine Commission.
Dataset page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Source endpoint: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip
Source SHA256: `f6d711917bb4340f8f644d5366c51665489eb2d426dd1a4a55677721ae5adf17` (1022574 bytes).
Asset SHA256: `6513c39044b8dee18c00af0dd6f97919d681b1e85efbd2d6980786c4cfbb8c9c` (3868898 bytes).

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
if ($LASTEXITCODE -ne 0) { throw 'Topology verification failed.' }
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

## Verification captured on 2026-10-25

All 263 projected features passed in-process OGC polygon topology validation by
Microsoft.SqlServer.Types 15.0.0.0. The pinned report covers Polygon/MultiPolygon
parts and holes within each feature; it does not certify that different zones
never overlap. The resolver refuses ambiguous overlapping matches and boundaries.
This source has 240 Polygon and 23 MultiPolygon features, 354 polygon parts,
98,192 source/projected vertices and zero holes. IDs/names/boroughs match the
existing 265-row TLC lookup (264/265 are non-geographic sentinels).

The PROJ operation declares 4 m accuracy; this is operation metadata, not an
empirical point/zone accuracy result. No coordinate rounding or topology repair
was performed. The ZIP and isolated projection tools remain in the ignored
`.sandbox/address-2b/20261025-geometry01/` directory, separate from original data.
