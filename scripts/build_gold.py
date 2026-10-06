"""Build month-partitioned Gold Taxi trips from validated Silver datasets."""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.config import load_config, resolve_project_path
from nyc_taxi.common.spark import get_spark_session
from nyc_taxi.etl.gold import build_gold_dataset
from nyc_taxi.ingestion.manifest import load_manifest, save_manifest

logger = logging.getLogger("gold-build")
MONTHS = tuple(f"2025-{month:02d}" for month in range(1, 7))
DIMENSIONS = ("taxi-zones", "calendar", "weather")


def month_range(start: str, end: str) -> list[str]:
    """Return the inclusive range, restricted to the available Jan–Jun 2025 data."""
    if not re.fullmatch(r"2025-0[1-6]", start) or not re.fullmatch(r"2025-0[1-6]", end):
        raise ValueError("Gold Taxi months must be between 2025-01 and 2025-06")
    first, last = MONTHS.index(start), MONTHS.index(end)
    if last < first:
        raise ValueError("End month must be on or after start month")
    return list(MONTHS[first : last + 1])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", help="One Taxi source month, e.g. 2025-01")
    parser.add_argument("--start-month", help="First Taxi source month in a range")
    parser.add_argument("--end-month", help="Last Taxi source month in a range")
    parser.add_argument("--all", action="store_true", help="Build all Jan–Jun 2025 months")
    parser.add_argument("--force", action="store_true", help="Replace selected Gold partition(s)")
    args = parser.parse_args(argv)
    if args.all:
        if args.month or args.start_month or args.end_month:
            parser.error("--all cannot be combined with month options")
        return args
    if args.month:
        if args.start_month or args.end_month:
            parser.error("Choose --month or --start-month/--end-month")
        labels = (args.month, args.month)
    elif args.start_month and args.end_month:
        labels = (args.start_month, args.end_month)
    else:
        parser.error("Choose --month, --start-month with --end-month, or --all")
    try:
        month_range(*labels)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def _silver_entries(silver_dir: Path) -> dict[str, dict[str, Any]]:
    """Require all nine valid Silver outputs at their expected paths."""
    entries = load_manifest(silver_dir / "silver_manifest.json")
    expected = {
        **{
            f"taxi_{month}": silver_dir / "taxi" / "year=2025" / f"month={month[-2:]}"
            for month in MONTHS
        },
        "taxi-zones": silver_dir / "taxi_zones",
        "calendar": silver_dir / "calendar",
        "weather": silver_dir / "weather",
    }
    if len(entries) != len(expected):
        raise ValueError(f"Silver manifest must contain exactly {len(expected)} entries")
    by_name: dict[str, dict[str, Any]] = {}
    for entry in entries:
        name = entry.get("dataset_name")
        if not isinstance(name, str) or name in by_name:
            raise ValueError(f"Invalid or duplicate Silver dataset name: {name!r}")
        by_name[name] = entry
    if set(by_name) != set(expected):
        raise ValueError("Silver manifest does not contain the six Taxi months and three dimensions")
    for name, expected_path in expected.items():
        entry = by_name[name]
        expected_dataset = "taxi" if name.startswith("taxi_") else name
        if entry.get("dataset") != expected_dataset or entry.get("validation_status") != "valid":
            raise ValueError(f"Silver manifest entry is not valid: {name}")
        actual_path = entry.get("silver_path")
        if not isinstance(actual_path, str) or resolve_project_path(actual_path).resolve() != expected_path.resolve():
            raise ValueError(f"Silver path mismatch for {name}: {actual_path!r}")
        if not expected_path.is_dir():
            raise ValueError(f"Silver Parquet directory missing: {expected_path}")
        if not entry.get("silver_batch_id") or int(entry.get("silver_rows", -1)) <= 0:
            raise ValueError(f"Silver lineage or row count missing: {name}")
    return by_name


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    try:
        config = load_config()
        silver_dir = resolve_project_path(config["paths"]["silver"])
        gold_dir = resolve_project_path(config["paths"]["gold"])
        silver_entries = _silver_entries(silver_dir)
        manifest_path = gold_dir / "gold_manifest.json"
        current_entries = load_manifest(manifest_path)
        if len({entry.get("dataset_name") for entry in current_entries}) != len(current_entries):
            raise ValueError("Gold manifest contains duplicate dataset names")
        gold_entries = {entry["dataset_name"]: entry for entry in current_entries}
        months = list(MONTHS) if args.all else month_range(
            args.month or args.start_month, args.month or args.end_month
        )
    except (KeyError, OSError, TypeError, ValueError) as exc:
        logger.error("Cannot prepare Gold build: %s", exc)
        return 1

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = None
    try:
        spark = get_spark_session()
        # Weather timestamps are UTC; leave this setting local to the Gold CLI.
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        dimension_entries = {name: silver_entries[name] for name in DIMENSIONS}
        for month in months:
            year, number = map(int, month.split("-"))
            dataset_name = f"taxi_{month}"
            entry = build_gold_dataset(
                spark,
                resolve_project_path(silver_entries[dataset_name]["silver_path"]),
                resolve_project_path(dimension_entries["taxi-zones"]["silver_path"]),
                resolve_project_path(dimension_entries["calendar"]["silver_path"]),
                resolve_project_path(dimension_entries["weather"]["silver_path"]),
                gold_dir / "taxi_enriched" / f"year={year:04d}" / f"month={number:02d}",
                silver_entries[dataset_name],
                source_year=year,
                source_month=number,
                dimension_entries=dimension_entries,
                existing_entry=gold_entries.get(dataset_name),
                force=args.force,
            )
            entry.pop("skipped", None)
            gold_entries[dataset_name] = entry
            save_manifest(manifest_path, list(gold_entries.values()))
            logger.info("[GOLD] %s: %s rows; status=%s", dataset_name,
                        entry.get("gold_rows"), entry.get("validation_status"))
        logger.info("Gold manifest: %s (%d entries)", manifest_path, len(gold_entries))
        return 0
    except Exception as exc:
        logger.exception("Gold build failed: %s", exc)
        return 1
    finally:
        if spark is not None:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    raise SystemExit(main())
