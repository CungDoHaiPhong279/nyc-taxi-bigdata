"""Regression evaluation helpers."""

from __future__ import annotations

from typing import Any


def evaluate_regression(y_true: Any, y_pred: Any) -> dict[str, float]:
    """Calculate MAE, RMSE, and R-squared for regression predictions."""
    try:
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        return {
            "mae": float(mean_absolute_error(y_true, y_pred)),
            "rmse": float(mean_squared_error(y_true, y_pred) ** 0.5),
            "r2": float(r2_score(y_true, y_pred)),
        }
    except ModuleNotFoundError:
        from nyc_taxi.ml.pretrip_training_model import metrics

        computed = metrics([float(v) for v in y_true], [float(v) for v in y_pred])
        return {
            "mae": float(computed["mae_usd"]),
            "rmse": float(computed["rmse_usd"]),
            "r2": float(computed["r2"]) if computed["r2"] is not None else 0.0,
        }

