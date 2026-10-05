"""Small SQLAlchemy helpers for the project PostgreSQL database."""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote_plus

from nyc_taxi.common.config import load_config

logger = logging.getLogger(__name__)


def build_database_url(config: dict[str, Any] | None = None) -> str:
    """Build a SQLAlchemy PostgreSQL URL from project configuration."""
    database = (config or load_config())["database"]
    user = quote_plus(str(database["user"]))
    password = quote_plus(str(database["password"]))
    host = database["host"]
    port = database["port"]
    name = database["name"]
    driver = database.get("driver", "postgresql+psycopg")
    return f"{driver}://{user}:{password}@{host}:{port}/{name}"


def get_engine(
    config: dict[str, Any] | None = None,
    *,
    connect_timeout_seconds: int | None = None,
):
    """Create a SQLAlchemy engine without opening a connection immediately."""
    from sqlalchemy import create_engine

    engine_options: dict[str, Any] = {"pool_pre_ping": True}
    if connect_timeout_seconds is not None:
        engine_options["connect_args"] = {
            "connect_timeout": connect_timeout_seconds,
        }

    return create_engine(
        build_database_url(config),
        **engine_options,
    )


def check_database_connection(
    config: dict[str, Any] | None = None,
    *,
    connect_timeout_seconds: int | None = None,
) -> tuple[bool, str]:
    """Attempt ``SELECT 1`` and return status without raising to callers."""
    try:
        from sqlalchemy import text

        with get_engine(
            config,
            connect_timeout_seconds=connect_timeout_seconds,
        ).connect() as connection:
            connection.execute(text("SELECT 1"))
        return True, "PostgreSQL connection succeeded"
    except Exception as exc:  # Environment check must remain non-fatal.
        logger.warning("PostgreSQL is unavailable: %s", exc)
        return False, str(exc)

