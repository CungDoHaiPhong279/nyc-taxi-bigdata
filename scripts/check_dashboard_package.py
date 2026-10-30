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
PRETRIP_PINS = {
    "config/pretrip_v2_contract.json": "a1ddbe55f0128c20d8189d9f37b3b6172e90f20be71ae7a99300e21fb306add5",
    "config/pretrip_v2_calendar.json": "00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac",
    "src/nyc_taxi/ml/pretrip_features.py": "0f2cd9b8b3d84ddbd7f9b0aa49cbcc3acdd8d7ed75432281d6f62501f9fa091a",
}


class PackagingError(RuntimeError):
    """A public asset was omitted or does not match the reviewed package."""


def check_pretrip_package(project_root: Path) -> None:
    """Verify only public stdlib contract assets, never load a model or Spark."""
    for relative, expected in PRETRIP_PINS.items():
        try:
            actual = sha256((project_root / relative).read_bytes()).hexdigest()
        except OSError:
            raise PackagingError("A pinned V2 public contract asset is missing.") from None
        if actual != expected:
            raise PackagingError("V2 public contract/helper SHA256 mismatch.")
    for relative in ("src/nyc_taxi/ml/__init__.py", "src/nyc_taxi/ml/pretrip_serving_contract.py",
                     "src/nyc_taxi/dashboard/pretrip_prediction.py"):
        path = project_root / relative
        if not path.is_file() or not path.stat().st_size:
            raise PackagingError("A V2 web contract/client module is missing.")


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
        dashboard / "interactive_map.py",
        dashboard / "interactive_map_frontend" / "index.html",
    )
    for path in required:
        if not path.is_file() or not path.stat().st_size:
            raise PackagingError(f"Packaged dashboard asset is missing: {path.relative_to(project_root)}")
    frontend = (dashboard / "route_map_frontend" / "index.html").read_text(encoding="utf-8")
    version_pattern = rf'const\s+GL_VERSION\s*=\s*"{re.escape(MAPBOX_GL_VERSION)}"\s*;'
    required_markers = (
        '"https://api.mapbox.com/mapbox-gl-js/v"', '"mapbox-gl.js"', '"mapbox-gl.css"',
    )
    if not re.search(version_pattern, frontend) or any(marker not in frontend for marker in required_markers):
        raise PackagingError("Route-map frontend is missing its reviewed Mapbox GL asset pin.")
    check_pretrip_package(project_root)


def main() -> int:
    try:
        check_package()
    except Exception:
        # Never expose paths/configuration supplied by the deployment or raw data.
        print("FAIL: dashboard geometry/frontend package smoke.", file=sys.stderr)
        return 1
    print("PASS: geometry (263 IDs), frontend and pinned V2 web contracts packaged; no network/model/render check.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
