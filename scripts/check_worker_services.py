"""Read-only PostgreSQL and MLflow connectivity check for the Compose worker."""

from __future__ import annotations

import sys
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.config import load_config
from nyc_taxi.database.postgres import get_engine


def main() -> int:
    """Check both services without writing data or printing connection secrets."""
    try:
        config = load_config()
        engine = get_engine(config, connect_timeout_seconds=5)
        try:
            with engine.connect() as connection:
                connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                result = connection.exec_driver_sql(
                    "SELECT 1, current_database()"
                ).one()
        finally:
            engine.dispose()
        if result[0] != 1:
            raise RuntimeError("SELECT 1 returned an unexpected result")
        print(f"PostgreSQL SELECT 1 OK; database={result[1]}")
    except Exception as exc:
        print(f"PostgreSQL check failed ({type(exc).__name__})", file=sys.stderr)
        return 1

    try:
        parsed = urlsplit(str(config["mlflow"]["tracking_uri"]))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Invalid MLflow tracking URI")
        health_url = urlunsplit((parsed.scheme, parsed.netloc, "/health", "", ""))
        with urlopen(health_url, timeout=5) as response:
            if response.status != 200:
                raise RuntimeError("MLflow health endpoint returned a non-200 status")
        print("MLflow /health OK (HTTP 200)")
    except Exception as exc:
        print(f"MLflow check failed ({type(exc).__name__})", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
