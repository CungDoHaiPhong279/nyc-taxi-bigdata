"""Offline AppTest for standalone pretrip; never needs DB, model or API."""

from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

from nyc_taxi.dashboard import queries


APP = Path(__file__).resolve().parents[1] / "dashboard" / "app.py"


@pytest.fixture(autouse=True)
def isolated_runtime(monkeypatch):
    st.cache_data.clear()
    st.cache_resource.clear()
    for name in ("NYC_TAXI_DASHBOARD_MODE", "FARE_MODEL_VERSION", "FARE_ADDRESS_ENABLED",
                 "FARE_ADDRESS_ALLOW_NETWORK", "MAPBOX_ACCESS_TOKEN"):
        monkeypatch.delenv(name, raising=False)
    yield
    st.cache_data.clear()
    st.cache_resource.clear()


@pytest.fixture
def no_database(monkeypatch):
    from nyc_taxi.database import postgres

    read = Mock(side_effect=AssertionError("pretrip must not query analytics"))
    engine = Mock(side_effect=AssertionError("pretrip must not request a DB engine"))
    settings = Mock(side_effect=AssertionError("pretrip must not request DB settings"))
    monkeypatch.setattr(queries, "fetch_dashboard", read)
    monkeypatch.setattr(queries, "get_engine", engine)
    monkeypatch.setattr(postgres, "get_engine", engine)
    monkeypatch.setattr(postgres, "load_config", settings)
    return read, engine, settings


def configure_pretrip(monkeypatch):
    monkeypatch.setenv("NYC_TAXI_DASHBOARD_MODE", "pretrip")
    monkeypatch.setenv("FARE_MODEL_VERSION", "v2.2")
    monkeypatch.setenv("FARE_ADDRESS_ENABLED", "1")
    monkeypatch.setenv("FARE_ADDRESS_ALLOW_NETWORK", "1")
    monkeypatch.setenv("MAPBOX_ACCESS_TOKEN", "synthetic-runtime-placeholder")


def test_pretrip_opens_real_fare_form_without_analytics_and_rerun_reuses_metadata(monkeypatch, no_database):
    from nyc_taxi.dashboard import address_page
    from nyc_taxi.ml.pretrip_serving_contract import identity

    configure_pretrip(monkeypatch)
    metadata = {**identity(), "pickup_location_ids": [161], "dropoff_location_ids": [237], "zones": []}
    reader = Mock(side_effect=lambda _url: deepcopy(metadata))
    search = Mock(side_effect=AssertionError("typing was not submitted"))
    coordinator = Mock(side_effect=AssertionError("estimate was not submitted"))
    monkeypatch.setattr(address_page, "load_pretrip_metadata", reader)
    monkeypatch.setattr(address_page, "make_search_client", search)
    monkeypatch.setattr(address_page, "make_coordinator", coordinator)
    monkeypatch.setattr(address_page, "address_picker", lambda *_args, **_kw: None)
    app = AppTest.from_file(str(APP)).run(timeout=10)
    assert not app.exception and not app.error
    assert app.title[0].value == "Ước tính cước từ địa chỉ"
    assert app.radio[0].value == "Đi ngay"
    assert not app.sidebar.radio and not app.multiselect and not app.metric
    app.run(timeout=10)
    assert not app.exception and not app.error
    reader.assert_called_once()
    search.assert_not_called()
    coordinator.assert_not_called()
    for call in no_database:
        call.assert_not_called()


@pytest.mark.parametrize("missing", ["FARE_ADDRESS_ENABLED", "FARE_ADDRESS_ALLOW_NETWORK", "MAPBOX_ACCESS_TOKEN"])
def test_unconfigured_pretrip_explains_runtime_config_without_any_api(monkeypatch, no_database, missing):
    from nyc_taxi.dashboard import fare_page, address_page

    configure_pretrip(monkeypatch)
    monkeypatch.delenv(missing)
    render = Mock(side_effect=AssertionError("unconfigured mode must not start API-backed page"))
    metadata = Mock(side_effect=AssertionError("unconfigured mode must not call inference API"))
    monkeypatch.setattr(fare_page, "render_page", render)
    monkeypatch.setattr(address_page, "get_pretrip_metadata", metadata)
    app = AppTest.from_file(str(APP)).run(timeout=10)
    assert not app.exception and not app.error
    guide = " ".join(item.value for item in app.info)
    assert "MAPBOX_ACCESS_TOKEN" in guide and "FARE_ADDRESS_ALLOW_NETWORK=1" in guide
    assert "không gọi API" in guide
    assert not app.metric and not app.sidebar.radio
    render.assert_not_called()
    metadata.assert_not_called()
    for call in no_database:
        call.assert_not_called()


@pytest.mark.parametrize("version", ["v1", "invalid"])
def test_pretrip_rejects_wrong_model_without_v1_fallback(monkeypatch, no_database, version):
    from nyc_taxi.dashboard import fare_page

    configure_pretrip(monkeypatch)
    monkeypatch.setenv("FARE_MODEL_VERSION", version)
    render = Mock()
    monkeypatch.setattr(fare_page, "render_page", render)
    app = AppTest.from_file(str(APP)).run(timeout=10)
    assert not app.exception and app.error and not app.metric
    render.assert_not_called()
    for call in no_database:
        call.assert_not_called()


def test_unknown_dashboard_mode_fails_closed_without_database(monkeypatch, no_database):
    monkeypatch.setenv("NYC_TAXI_DASHBOARD_MODE", "typo")
    app = AppTest.from_file(str(APP)).run(timeout=10)
    assert not app.exception and app.error
    assert "full hoặc pretrip" in app.error[0].value
    for call in no_database:
        call.assert_not_called()


@pytest.mark.parametrize("mode", [None, "full"])
def test_default_and_explicit_full_preserve_analytics_navigation(monkeypatch, mode):
    if mode:
        monkeypatch.setenv("NYC_TAXI_DASHBOARD_MODE", mode)
    snapshot = {
        "total_trips": 1, "average_fare": 12.0, "average_distance": 2.0, "total_fare": 12.0,
        "query_seconds": .01, "queried_at": "2026-01-01T00:00:00+00:00",
        "by_month": [{"source_month": "2025-01", "trips": 1}],
        "by_hour": [{"pickup_hour": 10, "trips": 1}],
        "by_borough": [{"pickup_borough": "Manhattan", "trips": 1}],
    }
    read = Mock(return_value=deepcopy(snapshot))
    monkeypatch.setattr(queries, "fetch_dashboard", read)
    app = AppTest.from_file(str(APP)).run(timeout=10)
    assert not app.exception and not app.error
    assert app.title[0].value == "NYC Taxi Analytics"
    assert app.sidebar.radio[0].options == ["Phân tích lịch sử", "Ước tính cước"]
    assert app.metric[0].value == "1" and len(app.get("vega_lite_chart")) == 3
    read.assert_called_once()
