"""Execute entry routing with stdlib stubs; these checks are not Streamlit AppTest."""

import ast
import builtins
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from nyc_taxi.dashboard.prediction import PredictionError, selected_model_version


APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


def entry_function():
    """Compile the actual entry function, without importing unavailable web deps."""
    module = ast.parse(APP.read_text(encoding="utf-8"))
    function = next(node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == "main")
    calls = []
    render = Mock()
    streamlit = Mock()
    streamlit.sidebar.radio.return_value = "Ước tính cước"

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        calls.append((name, tuple(fromlist)))
        if name == "nyc_taxi.dashboard.prediction":
            return SimpleNamespace(PredictionError=PredictionError, selected_model_version=selected_model_version)
        if name == "nyc_taxi.dashboard.fare_page":
            return SimpleNamespace(render_page=render)
        if name == "nyc_taxi.dashboard" and "queries" in fromlist:
            return SimpleNamespace(queries=SimpleNamespace())
        raise AssertionError("Unexpected import from entry routing")

    namespace = {"os": os, "st": streamlit, "__builtins__": {**vars(builtins), "__import__": guarded_import}}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(APP), "exec"), namespace)
    return namespace["main"], streamlit, render, calls


def configure(monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DASHBOARD_MODE", "pretrip")
    monkeypatch.setenv("FARE_MODEL_VERSION", "v2.2")
    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "1")
    monkeypatch.setenv("FARE_ADDRESS_ALLOW_NETWORK", "1")
    monkeypatch.setenv("MAPBOX_ACCESS_TOKEN", "synthetic-runtime-placeholder")


def assert_no_db_import(calls):
    assert not any(name == "nyc_taxi.dashboard" and "queries" in values for name, values in calls)


def test_pretrip_entry_routes_directly_without_analytics_import(monkeypatch):
    configure(monkeypatch)
    main, st, render, calls = entry_function()
    main()
    render.assert_called_once()
    st.sidebar.radio.assert_not_called()
    assert_no_db_import(calls)


@pytest.mark.parametrize("missing", ["FARE_ADDRESS_ENABLED", "FARE_ADDRESS_ALLOW_NETWORK", "MAPBOX_ACCESS_TOKEN"])
def test_pretrip_config_guidance_prevents_api_backed_page_and_db_import(monkeypatch, missing):
    configure(monkeypatch)
    monkeypatch.delenv(missing)
    main, st, render, calls = entry_function()
    main()
    render.assert_not_called()
    st.info.assert_called_once()
    assert "MAPBOX_ACCESS_TOKEN" in st.info.call_args.args[0]
    assert_no_db_import(calls)


@pytest.mark.parametrize("version", ["v1", "invalid"])
def test_pretrip_version_guard_never_falls_back(monkeypatch, version):
    configure(monkeypatch)
    monkeypatch.setenv("FARE_MODEL_VERSION", version)
    main, st, render, calls = entry_function()
    main()
    st.error.assert_called_once()
    render.assert_not_called()
    assert_no_db_import(calls)


def test_invalid_mode_is_closed_before_analytics_import(monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DASHBOARD_MODE", "typo")
    main, st, render, calls = entry_function()
    main()
    st.error.assert_called_once()
    render.assert_not_called()
    assert_no_db_import(calls)


@pytest.mark.parametrize("mode", [None, "full"])
def test_existing_full_navigation_remains_available(monkeypatch, mode):
    if mode is None:
        monkeypatch.delenv("NYC_TAXI_DASHBOARD_MODE", raising=False)
    else:
        monkeypatch.setenv("NYC_TAXI_DASHBOARD_MODE", mode)
    main, st, render, calls = entry_function()
    main()
    st.sidebar.radio.assert_called_once_with("Trang", ("Phân tích lịch sử", "Ước tính cước"), key="dashboard_page")
    render.assert_called_once()
    assert any(name == "nyc_taxi.dashboard" and "queries" in values for name, values in calls)
