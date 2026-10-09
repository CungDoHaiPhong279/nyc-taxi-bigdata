"""STEP 6: stream Silver dimensions and Gold Taxi months into PostgreSQL.

This CLI implements the user's latest STEP 6 request. Schema recreation is
opt-in and guarded by SQL that refuses to remove populated legacy tables.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.config import load_config, resolve_project_path
from nyc_taxi.database.loader import (
    MONTHS, apply_schema, collect_validation_report, create_indexes,
    get_weather_ids, load_dimensions, load_month, save_validation_report,
    validate_dimension_counts, validate_schema,
)
from nyc_taxi.ingestion.manifest import load_manifest

logger = logging.getLogger("postgres-load")


def month_range(first: str, last: str) -> list[str]:
    if not re.fullmatch(r"2025-0[1-6]", first) or not re.fullmatch(r"2025-0[1-6]", last):
        raise ValueError("Taxi months must be between 2025-01 and 2025-06")
    start, end = MONTHS.index(first), MONTHS.index(last)
    if end < start:
        raise ValueError("End month must be on or after start month")
    return list(MONTHS[start : end + 1])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dimensions", action="store_true", help="Load all Silver dimensions")
    parser.add_argument("--month", help="One Gold source month, e.g. 2025-01")
    parser.add_argument("--start-month", help="First Gold source month")
    parser.add_argument("--end-month", help="Last Gold source month (inclusive)")
    parser.add_argument("--all", action="store_true", help="Load dimensions and all six Gold months")
    parser.add_argument("--force", action="store_true", help="Reload selected fact month(s)")
    parser.add_argument("--batch-size", type=int, default=10_000,
                        help="Arrow/COPY rows per bounded batch (default: 10000)")
    parser.add_argument("--validate", action="store_true",
                        help="Run aggregate checks and write reports/postgres_load_report.json")
    parser.add_argument("--create-indexes", action="store_true",
                        help="Create deferred analytical indexes after fact load")
    parser.add_argument("--recreate-schema", action="store_true",
                        help="Explicitly replace only known empty legacy analytics objects")
    args = parser.parse_args(argv)
    if args.batch_size <= 0 or args.batch_size > 100_000:
        parser.error("--batch-size must be between 1 and 100000")
    if args.all and (args.month or args.start_month or args.end_month):
        parser.error("--all cannot be combined with month selectors")
    if args.month and (args.start_month or args.end_month):
        parser.error("Choose --month or --start-month/--end-month")
    if bool(args.start_month) != bool(args.end_month):
        parser.error("--start-month and --end-month must be provided together")
    try:
        if args.month:
            month_range(args.month, args.month)
        elif args.start_month:
            month_range(args.start_month, args.end_month)
    except ValueError as exc:
        parser.error(str(exc))
    if args.recreate_schema and not (args.dimensions or args.all):
        parser.error("--recreate-schema requires --dimensions or --all")
    if args.force and not (args.all or args.month or args.start_month):
        parser.error("--force requires a fact month selector or --all")
    if not any((args.dimensions, args.month, args.start_month, args.all,
                args.validate, args.create_indexes)):
        parser.error("Select a load, --validate, or --create-indexes")
    return args


def _manifest_entries(path: Path, expected_names: set[str]) -> dict[str, dict[str, Any]]:
    entries = load_manifest(path)
    by_name: dict[str, dict[str, Any]] = {}
    for entry in entries:
        name = entry.get("dataset_name")
        if not isinstance(name, str) or name in by_name:
            raise ValueError(f"Duplicate or invalid manifest dataset name: {name!r}")
        by_name[name] = entry
    if set(by_name) != expected_names:
        raise ValueError(f"Manifest {path} does not contain expected datasets")
    if any(entry.get("validation_status") != "valid" for entry in entries):
        raise ValueError(f"Manifest {path} has an invalid dataset")
    return by_name


def _silver_paths(config: dict[str, Any]) -> dict[str, Path]:
    silver_dir = resolve_project_path(config["paths"]["silver"])
    manifest = _manifest_entries(
        silver_dir / "silver_manifest.json",
        {*(f"taxi_{month}" for month in MONTHS), "taxi-zones", "calendar", "weather"},
    )
    paths: dict[str, Path] = {}
    for name, expected_count in (("taxi-zones", 265), ("calendar", 365), ("weather", 4344)):
        entry = manifest[name]
        if int(entry.get("silver_rows", -1)) != expected_count:
            raise ValueError(f"Silver {name} row count differs from expected {expected_count}")
        path = resolve_project_path(entry["silver_path"])
        expected = silver_dir / {"taxi-zones": "taxi_zones", "calendar": "calendar", "weather": "weather"}[name]
        if path.resolve() != expected.resolve() or not path.is_dir():
            raise ValueError(f"Invalid Silver path for {name}: {path}")
        paths[name] = path
    return paths


def _gold_entries(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    gold_dir = resolve_project_path(config["paths"]["gold"])
    entries = _manifest_entries(
        gold_dir / "gold_manifest.json", {f"taxi_{month}" for month in MONTHS}
    )
    for month in MONTHS:
        entry = entries[f"taxi_{month}"]
        expected_path = gold_dir / "taxi_enriched" / "year=2025" / f"month={month[-2:]}"
        if (resolve_project_path(entry["gold_path"]).resolve() != expected_path.resolve()
                or not expected_path.is_dir()):
            raise ValueError(f"Invalid Gold partition path for {month}")
        if int(entry["gold_rows"]) != int(entry["silver_rows"]):
            raise ValueError(f"Gold/Silver row mismatch in {month}")
    return entries


def _connect(config: dict[str, Any]):
    import psycopg

    database = config["database"]
    if database.get("schema", "analytics") != "analytics":
        raise ValueError("STEP 6 SQL targets the analytics schema only")
    return psycopg.connect(
        host=database["host"], port=int(database["port"]),
        dbname=database["name"], user=database["user"],
        password=database["password"], connect_timeout=5, autocommit=True,
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args(argv)
    try:
        config = load_config()
        gold = _gold_entries(config)
        silver_paths = _silver_paths(config) if (args.dimensions or args.all) else None
        months = (list(MONTHS) if args.all else
                  month_range(args.month, args.month) if args.month else
                  month_range(args.start_month, args.end_month) if args.start_month else [])
        with _connect(config) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            logger.info("PostgreSQL connection succeeded")
            if args.dimensions or months or args.create_indexes:
                if args.recreate_schema:
                    logger.warning(
                        "Explicit guarded schema recreation requested; only known empty legacy objects may be removed"
                    )
                apply_schema(connection, recreate=args.recreate_schema)
            if silver_paths is not None:
                result = load_dimensions(connection, silver_paths, batch_size=args.batch_size)
                logger.info("Dimensions loaded: %s (%.2f s)", result["counts"], result["elapsed_seconds"])
            if months:
                validate_dimension_counts(connection)
                weather_ids = get_weather_ids(connection)
                for month in months:
                    result = load_month(
                        connection, gold[f"taxi_{month}"], weather_ids,
                        force=args.force, batch_size=args.batch_size,
                    )
                    logger.info(
                        "%s: %s; Gold=%s PostgreSQL=%s; %.2f s",
                        month, result["status"], result["gold_rows"],
                        result["postgres_rows"], result["elapsed_seconds"],
                    )
            if args.create_indexes:
                indexes = create_indexes(connection)
                logger.info("Analytical indexes present: %s", ", ".join(indexes))
                logger.info("Non-materialized analytics.vw_trip_enriched view is present")
            if args.validate or args.all:
                validate_schema(connection)
                report = collect_validation_report(connection, list(gold.values()))
                report_path = PROJECT_ROOT / "reports" / "postgres_load_report.json"
                save_validation_report(report_path, report)
                logger.info("Validation report: %s; status=%s", report_path, report["validation_status"])
                if report["validation_status"] != "valid":
                    return 1
        return 0
    except Exception as exc:
        logger.error("PostgreSQL load failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
