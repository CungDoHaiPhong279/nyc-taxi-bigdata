"""Ensure every project module can be imported without starting services."""

import importlib

import pytest

MODULES = [
    "nyc_taxi",
    "nyc_taxi.common.config",
    "nyc_taxi.common.logging_config",
    "nyc_taxi.common.spark",
    "nyc_taxi.ingestion.taxi",
    "nyc_taxi.ingestion.weather",
    "nyc_taxi.ingestion.calendar",
    "nyc_taxi.ingestion.taxi_zones",
    "nyc_taxi.etl.bronze",
    "nyc_taxi.etl.silver",
    "nyc_taxi.etl.gold",
    "nyc_taxi.features.build_features",
    "nyc_taxi.database.postgres",
    "nyc_taxi.ml.train",
    "nyc_taxi.ml.evaluate",
    "nyc_taxi.ml.predict",
    "nyc_taxi.ml.pilot_model",
    "nyc_taxi.ml.inference",
    "nyc_taxi.ml.pretrip_features",
    "nyc_taxi.ml.pretrip_serving_contract",
    "nyc_taxi.ml.pretrip_training_model",
    "nyc_taxi.ml.pretrip_holdout_model",
    "nyc_taxi.ml.pretrip_inference",
    "nyc_taxi.dashboard.queries",
    "nyc_taxi.dashboard.prediction",
    "nyc_taxi.dashboard.pretrip_prediction",
    "nyc_taxi.dashboard.route_fare",
]


@pytest.mark.parametrize("module_name", MODULES)
def test_module_imports(module_name: str) -> None:
    assert importlib.import_module(module_name) is not None

