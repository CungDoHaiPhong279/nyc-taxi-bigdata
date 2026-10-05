"""Regression model and MLflow training scaffold."""

from __future__ import annotations

import logging
from typing import Any

from nyc_taxi.common.config import load_config

logger = logging.getLogger(__name__)


def get_candidate_models(random_seed: int = 42) -> dict[str, Any]:
    """Create the three planned scikit-learn regression estimators."""
    from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
    from sklearn.linear_model import LinearRegression

    return {
        "linear_regression": LinearRegression(),
        "random_forest": RandomForestRegressor(random_state=random_seed),
        "gradient_boosting": GradientBoostingRegressor(random_state=random_seed),
    }


def configure_mlflow() -> None:
    """Point MLflow at the tracking server declared in project configuration."""
    import mlflow

    config = load_config()["mlflow"]
    mlflow.set_tracking_uri(config["tracking_uri"])
    mlflow.set_experiment(config["experiment_name"])
    logger.info("Configured MLflow tracking for experiment %s", config["experiment_name"])


def train_models(features: Any, target: Any) -> dict[str, Any]:
    """Reserved entry point for splitting, fitting, and logging model runs."""
    if len(features) != len(target):
        raise ValueError("Features and target must contain the same number of rows")
    # TODO: Split by configured ratio, train candidate models, evaluate them, and
    # log parameters/metrics/artifacts to MLflow.
    raise NotImplementedError("Model training is intentionally deferred")

