"""Check the local Python, project, Spark, and optional database environment."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.config import load_config, resolve_project_path
from nyc_taxi.database.postgres import check_database_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("environment-check")


def check_spark() -> None:
    """Create and always stop a local Spark session."""
    spark = None
    try:
        from nyc_taxi.common.spark import get_spark_session

        spark = get_spark_session()
        logger.info("Spark session created: version %s", spark.version)
    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")


def main() -> int:
    """Run required checks; an unavailable PostgreSQL instance is only a warning."""
    errors: list[str] = []
    logger.info("Python version: %s", sys.version.split()[0])
    if sys.version_info[:2] != (3, 11):
        errors.append("This project expects Python 3.11")

    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
    except Exception as exc:
        logger.error("Configuration check failed: %s", exc)
        return 1

    required_paths = ["raw", "bronze", "silver", "gold", "models", "logs"]
    for path_key in required_paths:
        path = resolve_project_path(config["paths"][path_key])
        if path.is_dir():
            logger.info("Directory exists: %s", path)
        else:
            errors.append(f"Required directory is missing: {path}")

    try:
        check_spark()
    except Exception as exc:
        errors.append(f"PySpark/Spark check failed: {exc}")

    database_ok, database_message = check_database_connection(
        config,
        connect_timeout_seconds=3,
    )
    if database_ok:
        logger.info(database_message)
    else:
        logger.warning("PostgreSQL check skipped/failed (non-fatal): %s", database_message)

    for error in errors:
        logger.error(error)
    if errors:
        return 1

    logger.info("All required environment checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

