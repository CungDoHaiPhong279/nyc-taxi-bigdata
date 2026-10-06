"""Build validated Silver Parquet outputs from the Bronze manifest."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.config import load_config, resolve_project_path
from nyc_taxi.common.spark import get_spark_session
from nyc_taxi.etl.silver import build_silver_dataset
from nyc_taxi.ingestion.manifest import load_manifest, save_manifest
from nyc_taxi.ingestion.taxi import taxi_url

logger = logging.getLogger("silver-build")


def month_range(start: str, end: str) -> list[str]:
    """Return inclusive YYYY-MM labels."""
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
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--dataset", choices=["taxi", "weather", "taxi-zones", "calendar"])
    selection.add_argument("--all", action="store_true")
    parser.add_argument("--month", help="One Taxi month, e.g. 2025-01")
    parser.add_argument("--start-month", help="First Taxi month in a range")
    parser.add_argument("--end-month", help="Last Taxi month in a range")
    parser.add_argument("--force", action="store_true", help="Replace selected Silver output(s)")
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


def _tasks(args: argparse.Namespace, bronze_dir: Path, silver_dir: Path) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    if args.all or args.dataset == "taxi":
        months = month_range(
            "2025-01" if args.all else args.month or args.start_month,
            "2025-06" if args.all else args.month or args.end_month,
        )
        for month in months:
            year, number = map(int, month.split("-"))
            tasks.append({
                "dataset": "taxi", "dataset_name": f"taxi_{month}",
                "bronze_path": bronze_dir / "taxi" / f"year={year:04d}" / f"month={number:02d}",
                "output_path": silver_dir / "taxi" / f"year={year:04d}" / f"month={number:02d}",
                "source_year": year, "source_month": number,
            })
    if args.all or args.dataset == "weather":
        tasks.append({
            "dataset": "weather", "dataset_name": "weather",
            "bronze_path": bronze_dir / "weather", "output_path": silver_dir / "weather",
        })
    if args.all or args.dataset == "taxi-zones":
        tasks.append({
            "dataset": "taxi-zones", "dataset_name": "taxi-zones",
            "bronze_path": bronze_dir / "taxi_zones", "output_path": silver_dir / "taxi_zones",
        })
    if args.all or args.dataset == "calendar":
        tasks.append({
            "dataset": "calendar", "dataset_name": "calendar",
            "bronze_path": bronze_dir / "calendar", "output_path": silver_dir / "calendar",
        })
    return tasks


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    try:
        config = load_config()
        bronze_dir = resolve_project_path(config["paths"]["bronze"])
        silver_dir = resolve_project_path(config["paths"]["silver"])
        bronze_entries = {
            entry["dataset_name"]: entry
            for entry in load_manifest(bronze_dir / "bronze_manifest.json")
        }
        manifest_path = silver_dir / "silver_manifest.json"
        silver_entries = {
            entry["dataset_name"]: entry for entry in load_manifest(manifest_path)
        }
        tasks = _tasks(args, bronze_dir, silver_dir)
        for task in tasks:
            bronze_entry = bronze_entries.get(task["dataset_name"])
            if bronze_entry is None or bronze_entry.get("validation_status") != "valid":
                raise ValueError(f"Valid Bronze manifest entry missing: {task['dataset_name']}")
            if not task["bronze_path"].is_dir():
                raise ValueError(f"Bronze Parquet directory missing: {task['bronze_path']}")
            task["bronze_entry"] = bronze_entry
    except (KeyError, OSError, ValueError) as exc:
        logger.error("Cannot prepare Silver build: %s", exc)
        return 1

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = None
    try:
        spark = get_spark_session()
        # Only this Silver CLI uses UTC: weather source timestamps are GMT/UTC.
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        known_zone_ids = None
        if any(task["dataset"] == "taxi" for task in tasks):
            zones_entry = bronze_entries.get("taxi-zones")
            if zones_entry is None or zones_entry.get("validation_status") != "valid":
                raise ValueError("Taxi Zone Bronze manifest entry is required for referential checks")
            zones_path = resolve_project_path(zones_entry["bronze_path"])
            known_zone_ids = {
                int(row["LocationID"])
                for row in spark.read.parquet(str(zones_path)).select("LocationID").collect()
            }
            if len(known_zone_ids) != int(zones_entry["bronze_rows"]):
                raise ValueError("Taxi Zone Bronze keys are not unique")

        for task in tasks:
            entry = build_silver_dataset(
                spark, task["dataset"], task["bronze_path"], task["output_path"],
                task["bronze_entry"], source_year=task.get("source_year"),
                source_month=task.get("source_month"), known_zone_ids=known_zone_ids,
                force=args.force, existing_entry=silver_entries.get(task["dataset_name"]),
            )
            entry.pop("skipped", None)
            silver_entries[entry["dataset_name"]] = entry
            save_manifest(manifest_path, list(silver_entries.values()))
        logger.info("Silver manifest: %s (%d valid datasets)", manifest_path, len(silver_entries))
        return 0
    except Exception as exc:
        logger.exception("Silver build failed: %s", exc)
        return 1
    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    raise SystemExit(main())
