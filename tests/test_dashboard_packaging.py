"""Image asset smoke failures with synthetic files; no HTTP/Streamlit/model."""

from hashlib import sha256
from types import SimpleNamespace

import pytest

from scripts import check_dashboard_package as smoke


@pytest.fixture
def packaged(tmp_path, monkeypatch):
    for relative in smoke.PRETRIP_PINS:
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((smoke.PROJECT_ROOT / relative).read_bytes())
    for relative in ("src/nyc_taxi/ml/__init__.py", "src/nyc_taxi/ml/pretrip_serving_contract.py",
                     "src/nyc_taxi/dashboard/pretrip_prediction.py"):
        target = tmp_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("# synthetic stdlib module\n", encoding="utf-8")
    directory = tmp_path / "assets" / "taxi_zones"
    directory.mkdir(parents=True)
    geometry = directory / "taxi_zones.geojson"
    geometry.write_bytes(b"synthetic public geometry")
    monkeypatch.setattr(smoke, "GEOMETRY_SHA256", sha256(geometry.read_bytes()).hexdigest())
    calls = []

    def load(path, manifest, expected):
        calls.append((path, manifest, expected))
        return SimpleNamespace(location_ids=tuple(range(1, 264)))

    monkeypatch.setattr(smoke, "load_geometry_bundle", load)
    dashboard = tmp_path / "src" / "nyc_taxi" / "dashboard"
    for relative in ("address_component.py", "address_frontend/index.html", "route_map.py"):
        path = dashboard / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("synthetic asset", encoding="utf-8")
    frontend = dashboard / "route_map_frontend" / "index.html"
    frontend.parent.mkdir()
    frontend.write_text('const GL_VERSION = "3.15.0";\n'
                        '"https://api.mapbox.com/mapbox-gl-js/v"; "mapbox-gl.js"; "mapbox-gl.css";',
                        encoding="utf-8")
    return tmp_path, geometry, frontend, calls


def test_smoke_checks_reviewed_pin_and_exact_geometry_ids(packaged):
    root, geometry, _frontend, calls = packaged
    smoke.check_package(root)
    assert calls == [(geometry, geometry.with_name("manifest.json"), smoke.GEOMETRY_SHA256)]


def test_smoke_rejects_missing_or_altered_geometry_before_loading(packaged):
    root, geometry, _frontend, calls = packaged
    geometry.write_bytes(b"altered")
    with pytest.raises(smoke.PackagingError, match="SHA256"):
        smoke.check_package(root)
    geometry.unlink()
    with pytest.raises(smoke.PackagingError, match="missing"):
        smoke.check_package(root)
    assert calls == []


def test_smoke_rejects_missing_ids_even_if_loader_returns(packaged, monkeypatch):
    root, _geometry, _frontend, _calls = packaged
    monkeypatch.setattr(smoke, "load_geometry_bundle", lambda *_: SimpleNamespace(location_ids=(1, 2)))
    with pytest.raises(smoke.PackagingError, match="1..263"):
        smoke.check_package(root)


@pytest.mark.parametrize("relative", ["address_component.py", "address_frontend/index.html",
                                      "route_map.py", "route_map_frontend/index.html"])
def test_smoke_rejects_omitted_frontend_assets(packaged, relative):
    root, _geometry, _frontend, _calls = packaged
    (root / "src" / "nyc_taxi" / "dashboard" / relative).unlink()
    with pytest.raises(smoke.PackagingError, match="missing"):
        smoke.check_package(root)


def test_smoke_rejects_unpinned_gl_version(packaged):
    root, _geometry, frontend, _calls = packaged
    frontend.write_text(frontend.read_text(encoding="utf-8").replace("3.15.0", "latest"), encoding="utf-8")
    with pytest.raises(smoke.PackagingError, match="pin"):
        smoke.check_package(root)


@pytest.mark.parametrize("relative", tuple(smoke.PRETRIP_PINS))
def test_smoke_rejects_changed_pretrip_public_contract_or_helper(packaged, relative):
    root, _, _, _ = packaged
    (root / relative).write_bytes(b"changed public asset")
    with pytest.raises(smoke.PackagingError, match="SHA256"):
        smoke.check_package(root)


def test_smoke_rejects_missing_pretrip_client_without_loading_model(packaged):
    root, _, _, _ = packaged
    (root / "src/nyc_taxi/dashboard/pretrip_prediction.py").unlink()
    with pytest.raises(smoke.PackagingError, match="module is missing"):
        smoke.check_package(root)
