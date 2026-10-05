"""Prediction entry point for a future trained fare model."""

from __future__ import annotations

from typing import Any


def predict_fares(model: Any, features: Any) -> Any:
    """Run predictions using an already fitted estimator."""
    if not hasattr(model, "predict"):
        raise TypeError("Model must provide a predict() method")
    return model.predict(features)

