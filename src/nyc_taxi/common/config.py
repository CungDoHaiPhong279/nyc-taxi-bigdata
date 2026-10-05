"""Load project configuration and environment variables."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


def find_project_root(start: Path | None = None) -> Path:
    """Find the nearest parent containing ``config/config.yaml``."""
    candidate = (start or Path(__file__)).resolve()
    if candidate.is_file():
        candidate = candidate.parent

    for directory in (candidate, *candidate.parents):
        if (directory / "config" / "config.yaml").is_file():
            return directory

    raise FileNotFoundError("Could not find project root containing config/config.yaml")


PROJECT_ROOT = find_project_root()


def load_config(
    config_path: str | Path | None = None,
    env_path: str | Path | None = None,
) -> dict[str, Any]:
    """Read YAML configuration and overlay supported values from ``.env``."""
    yaml_path = Path(config_path) if config_path else PROJECT_ROOT / "config" / "config.yaml"
    dotenv_path = Path(env_path) if env_path else PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=dotenv_path, override=False)

    if not yaml_path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {yaml_path}")

    with yaml_path.open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    if not isinstance(config, dict):
        raise ValueError("config.yaml must contain a YAML mapping at its root")

    database = config.setdefault("database", {})
    env_database_keys = {
        "POSTGRES_HOST": "host",
        "POSTGRES_PORT": "port",
        "POSTGRES_DB": "name",
        "POSTGRES_USER": "user",
        "POSTGRES_PASSWORD": "password",
    }
    for env_name, config_name in env_database_keys.items():
        if env_name in os.environ:
            value: Any = os.environ[env_name]
            if config_name == "port":
                value = int(value)
            database[config_name] = value

    mlflow = config.setdefault("mlflow", {})
    if "MLFLOW_TRACKING_URI" in os.environ:
        mlflow["tracking_uri"] = os.environ["MLFLOW_TRACKING_URI"]

    return config


def get_config_value(config: dict[str, Any], dotted_key: str) -> Any:
    """Return a nested value using a key such as ``machine_learning.target``."""
    value: Any = config
    for key in dotted_key.split("."):
        if not isinstance(value, dict) or key not in value:
            raise KeyError(f"Configuration key not found: {dotted_key}")
        value = value[key]
    return value


def resolve_project_path(path_value: str | Path) -> Path:
    """Resolve a configured path relative to the project root."""
    path = Path(path_value)
    return path if path.is_absolute() else PROJECT_ROOT / path

