"""Build and validate source-preserving Bronze Parquet datasets."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.config import load_config, resolve_project_path
from nyc_taxi.common.spark import get_spark_session
from nyc_taxi.etl.bronze import build_bronze_dataset
from nyc_taxi.ingestion.manifest import load_manifest, save_manifest, sha256_file
from nyc_taxi.ingestion.taxi import taxi_url
from nyc_taxi.ingestion.validation import (
    validate_calendar,
    validate_taxi_parquet,
    validate_taxi_zones,
    validate_weather,
)

logger = logging.getLogger("bronze-build")


def month_range(start: str, end: str) -> list[str]:
    """Return inclusive YYYY-MM labels, rejecting reversed or malformed ranges."""
    taxi_url(start)
    taxi_url(end)
    first_year, first_month = map(int, start.split("-"))
    last_year, last_month = map(int, end.split("-"))
    first = first_year * 12 + first_month - 1
    last = last_year * 12 + last_month - 1
    if last < first:
        raise ValueError("End month must be on or after start month")
    return [f"{index // 12:04d}-{index % 12 + 1:02d}" for index in range(first, last + 1)]


def parse_args() -> argparse.Namespace:
    """Parse a dataset selection and optional exact-partition overwrite."""
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--dataset", choices=["taxi", "weather", "taxi-zones", "calendar"])
    selection.add_argument("--all", action="store_true")
    parser.add_argument("--month", help="One Taxi month, e.g. 2025-01")
    parser.add_argument("--start-month", help="First Taxi month in an inclusive range")
    parser.add_argument("--end-month", help="Last Taxi month in an inclusive range")
    parser.add_argument("--force", action="store_true", help="Replace only the selected Bronze output(s)")
    args = parser.parse_args()
    month_options = [args.month, args.start_month, args.end_month]
    if args.all and any(month_options):
        parser.error("--all cannot be combined with Taxi month options")
    if args.dataset == "taxi":
        if args.month and (args.start_month or args.end_month):
            parser.error("Choose --month or --start-month/--end-month")
        if not args.month and not (args.start_month and args.end_month):
            parser.error("Taxi requires --month or both --start-month and --end-month")
    elif any(month_options):
        parser.error("Taxi month options require --dataset taxi")
    return args


def _tasks(args: argparse.Namespace, raw_dir: Path, bronze_dir: Path) -> list[dict[str, Any]]:
    """Describe requested one-source-at-a-time work in execution order."""
    tasks: list[dict[str, Any]] = []
    if args.all or args.dataset == "taxi":
        months = month_range(
            "2025-01" if args.all else args.month or args.start_month,
            "2025-06" if args.all else args.month or args.end_month,
        )
        for month in months:
            year, number = map(int, month.split("-"))
            tasks.append({
                "dataset": "taxi",
                "dataset_name": f"taxi_{month}",
                "raw_name": f"yellow_taxi_{month}",
                "raw_path": raw_dir / "taxi" / f"yellow_tripdata_{month}.parquet",
                "output_path": bronze_dir / "taxi" / f"year={year:04d}" / f"month={number:02d}",
                "validator": validate_taxi_parquet,
                "source_year": year,
                "source_month": number,
            })
    if args.all or args.dataset == "weather":
        tasks.append({
            "dataset": "weather",
            "dataset_name": "weather",
            "raw_name": "weather_nyc_2025-01_2025-06",
            "raw_path": raw_dir / "weather" / "weather_nyc_2025-01_2025-06.json",
            "output_path": bronze_dir / "weather",
            "validator": lambda path: validate_weather(
                path, expected_start=date(2025, 1, 1), expected_end=date(2025, 6, 30)
            ),
        })
    if args.all or args.dataset == "taxi-zones":
        tasks.append({
            "dataset": "taxi-zones",
            "dataset_name": "taxi-zones",
            "raw_name": "taxi_zone_lookup",
            "raw_path": raw_dir / "taxi_zones" / "taxi_zone_lookup.csv",
            "output_path": bronze_dir / "taxi_zones",
            "validator": validate_taxi_zones,
        })
    if args.all or args.dataset == "calendar":
        tasks.append({
            "dataset": "calendar",
            "dataset_name": "calendar",
            "raw_name": "calendar_2025",
            "raw_path": raw_dir / "calendar" / "calendar_2025.csv",
            "output_path": bronze_dir / "calendar",
            "validator": validate_calendar,
        })
    return tasks


def main() -> int:
    """Validate Raw lineage, build selected Bronze outputs, and stop Spark."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    try:
        config = load_config()
        raw_dir = resolve_project_path(config["paths"]["raw"])
        bronze_dir = resolve_project_path(config["paths"]["bronze"])
        raw_entries = {
            entry["dataset_name"]: entry for entry in load_manifest(raw_dir / "raw_manifest.json")
        }
        manifest_path = bronze_dir / "bronze_manifest.json"
        bronze_entries = {
            entry["dataset_name"]: entry for entry in load_manifest(manifest_path)
        }
        tasks = _tasks(args, raw_dir, bronze_dir)
        for task in tasks:
            raw_entry = raw_entries.get(task["raw_name"])
            if raw_entry is None or raw_entry.get("validation_status") != "valid":
                raise ValueError(f"Valid raw manifest entry missing: {task['raw_name']}")
            raw_report = task["validator"](task["raw_path"])
            if raw_report["row_count"] != raw_entry["row_count"]:
                raise ValueError(f"Raw row count differs from manifest: {task['raw_name']}")
            actual_sha256 = sha256_file(task["raw_path"])
            if actual_sha256 != raw_entry["sha256"]:
                raise ValueError(f"Raw SHA256 differs from manifest: {task['raw_name']}")
            task["raw_report"] = raw_report
            task["raw_sha256"] = actual_sha256
    except (KeyError, OSError, ValueError) as exc:
        logger.error("Cannot prepare Bronze ingestion: %s", exc)
        return 1

    # Keep Spark's Python workers on the same interpreter as this Python 3.11 driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = None
    try:
        spark = get_spark_session()
        for task in tasks:
            entry = build_bronze_dataset(
                spark,
                task["dataset"],
                task["raw_path"],
                task["output_path"],
                task["raw_report"],
                task["raw_sha256"],
                source_year=task.get("source_year"),
                source_month=task.get("source_month"),
                force=args.force,
                existing_entry=bronze_entries.get(task["dataset_name"]),
            )
            entry.pop("skipped", None)
            bronze_entries[entry["dataset_name"]] = entry
            save_manifest(manifest_path, list(bronze_entries.values()))
        logger.info("Bronze manifest: %s (%d validated datasets)", manifest_path, len(bronze_entries))
        return 0
    except Exception as exc:
        logger.exception("Bronze ingestion failed: %s", exc)
        return 1
    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    raise SystemExit(main())
