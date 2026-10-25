"""Offline image smoke: verify pinned geometry and shipped web assets, not rendering."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import re
import sys

from nyc_taxi.dashboard.geometry_assets import load_geometry_bundle


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GEOMETRY_SHA256 = "6513c39044b8dee18c00af0dd6f97919d681b1e85efbd2d6980786c4cfbb8c9c"
MAPBOX_GL_VERSION = "3.15.0"


class PackagingError(RuntimeError):
    """A public asset was omitted or does not match the reviewed package."""


def check_package(project_root: Path = PROJECT_ROOT) -> None:
    """Never reads runtime configuration, performs HTTP or initializes inference."""
    geometry_directory = project_root / "assets" / "taxi_zones"
    geometry_path = geometry_directory / "taxi_zones.geojson"
    try:
        digest = sha256(geometry_path.read_bytes()).hexdigest()
    except OSError:
        raise PackagingError("Pinned Taxi Zone geometry is missing.") from None
    if digest != GEOMETRY_SHA256:
        raise PackagingError("Taxi Zone geometry does not match the reviewed SHA256.")
    geometry = load_geometry_bundle(
        geometry_path, geometry_directory / "manifest.json", GEOMETRY_SHA256,
    )
    if geometry.location_ids != tuple(range(1, 264)):
        raise PackagingError("Taxi Zone geometry must contain exactly LocationIDs 1..263.")

    dashboard = project_root / "src" / "nyc_taxi" / "dashboard"
    required = (
        dashboard / "address_component.py",
        dashboard / "address_frontend" / "index.html",
        dashboard / "route_map.py",
        dashboard / "route_map_frontend" / "index.html",
    )
    for path in required:
        if not path.is_file() or not path.stat().st_size:
            raise PackagingError(f"Packaged dashboard asset is missing: {path.relative_to(project_root)}")
    frontend = required[-1].read_text(encoding="utf-8")
    version_pattern = rf'const\s+GL_VERSION\s*=\s*"{re.escape(MAPBOX_GL_VERSION)}"\s*;'
    required_markers = (
        '"https://api.mapbox.com/mapbox-gl-js/v"', '"mapbox-gl.js"', '"mapbox-gl.css"',
    )
    if not re.search(version_pattern, frontend) or any(marker not in frontend for marker in required_markers):
        raise PackagingError("Route-map frontend is missing its reviewed Mapbox GL asset pin.")


def main() -> int:
    try:
        check_package()
    except Exception:
        # Never expose paths/configuration supplied by the deployment or raw data.
        print("FAIL: dashboard geometry/frontend package smoke.", file=sys.stderr)
        return 1
    print("PASS: pinned geometry (263 IDs) and address/route-map frontend assets packaged; no network/render check.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
