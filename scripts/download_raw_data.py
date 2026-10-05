"""Acquire and validate the project source datasets without transforming them."""

from __future__ import annotations

import argparse
import calendar
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.config import load_config, resolve_project_path
from nyc_taxi.ingestion.calendar import validate_existing_calendar
from nyc_taxi.ingestion.manifest import build_manifest_entry, load_manifest, save_manifest
from nyc_taxi.ingestion.taxi import download_taxi_month, taxi_url
from nyc_taxi.ingestion.taxi_zones import TAXI_ZONE_URL, download_taxi_zones
from nyc_taxi.ingestion.validation import validate_taxi_parquet, validate_taxi_zones, validate_weather
from nyc_taxi.ingestion.weather import download_weather, weather_url

logger = logging.getLogger("raw-data-download")
WEATHER_START = date(2025, 1, 1)
WEATHER_END = date(2025, 6, 30)


def month_range(start: str, end: str) -> list[str]:
    """Return inclusive YYYY-MM month labels in chronological order."""
    taxi_url(start)
    taxi_url(end)
    first_year, first_month = map(int, start.split("-"))
    last_year, last_month = map(int, end.split("-"))
    first = first_year * 12 + first_month - 1
    last = last_year * 12 + last_month - 1
    if last < first:
        raise ValueError("Taxi end month must be on or after start month")
    return [f"{index // 12:04d}-{index % 12 + 1:02d}" for index in range(first, last + 1)]


def parse_args() -> argparse.Namespace:
    """Parse a small set of download controls."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--taxi-start-month", default="2025-01")
    parser.add_argument("--taxi-end-month", default="2025-06")
    parser.add_argument("--skip-taxi", action="store_true")
    parser.add_argument("--skip-weather", action="store_true")
    parser.add_argument("--skip-zones", action="store_true")
    parser.add_argument("--force", action="store_true", help="Replace existing validated source files")
    return parser.parse_args()


def main() -> int:
    """Download requested sources and write a manifest of validated local files."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    args = parse_args()
    try:
        months = month_range(args.taxi_start_month, args.taxi_end_month)
        raw_dir = resolve_project_path(load_config()["paths"]["raw"])
        manifest_path = raw_dir / "raw_manifest.json"
        entries = {entry["dataset_name"]: entry for entry in load_manifest(manifest_path)}
    except (KeyError, OSError, ValueError) as exc:
        logger.error("Cannot prepare raw data acquisition: %s", exc)
        return 1

    failures = 0

    def acquire(
        name: str,
        source: str,
        source_url: str | None,
        path: Path,
        file_format: str,
        downloader: Callable[[], Path] | None,
        validator: Callable[[Path], dict],
        date_coverage: dict[str, str] | None = None,
    ) -> None:
        nonlocal failures
        existed_before = path.is_file()
        previous_timestamp = entries.get(name, {}).get("download_timestamp")
        try:
            if downloader is not None:
                downloader()
            report = validator(path)
            timestamp = None
            if downloader is not None:
                if args.force or not existed_before:
                    timestamp = datetime.now(timezone.utc).isoformat()
                else:
                    timestamp = previous_timestamp
            entry = build_manifest_entry(
                name,
                source,
                source_url,
                path,
                file_format,
                report,
                date_coverage,
                timestamp,
            )
            entries[name] = entry
            save_manifest(manifest_path, list(entries.values()))
            logger.info("Validated %s: %s rows, %s bytes", name, entry["row_count"], entry["file_size_bytes"])
        except Exception as exc:
            failures += 1
            entries.pop(name, None)
            save_manifest(manifest_path, list(entries.values()))
            logger.error("%s failed: %s", name, exc)

    if not args.skip_taxi:
        for month in months:
            name = f"yellow_taxi_{month}"
            destination = raw_dir / "taxi" / f"yellow_tripdata_{month}.parquet"
            acquire(
                name,
                "NYC Taxi & Limousine Commission",
                taxi_url(month),
                destination,
                "parquet",
                lambda month=month: download_taxi_month(month, raw_dir / "taxi", force=args.force),
                validate_taxi_parquet,
                {
                    "start": f"{month}-01",
                    "end": f"{month}-{calendar.monthrange(*map(int, month.split('-')))[1]:02d}",
                    "basis": "TLC publication month; actual trip dates are not scanned",
                },
            )

    if not args.skip_zones:
        destination = raw_dir / "taxi_zones" / "taxi_zone_lookup.csv"
        acquire(
            "taxi_zone_lookup",
            "NYC Taxi & Limousine Commission",
            TAXI_ZONE_URL,
            destination,
            "csv",
            lambda: download_taxi_zones(raw_dir / "taxi_zones", force=args.force),
            validate_taxi_zones,
        )

    if not args.skip_weather:
        destination = raw_dir / "weather" / "weather_nyc_2025-01_2025-06.json"
        acquire(
            "weather_nyc_2025-01_2025-06",
            "Open-Meteo Historical Weather API / ERA5",
            weather_url(WEATHER_START, WEATHER_END),
            destination,
            "json",
            lambda: download_weather(WEATHER_START, WEATHER_END, raw_dir / "weather", force=args.force),
            lambda path: validate_weather(path, expected_start=WEATHER_START, expected_end=WEATHER_END),
        )

    calendar_path = raw_dir / "calendar" / "calendar_2025.csv"
    acquire(
        "calendar_2025",
        "Generated locally by scripts/create_calendar.py",
        None,
        calendar_path,
        "csv",
        None,
        validate_existing_calendar,
    )

    logger.info("Raw manifest: %s (%d validated datasets)", manifest_path, len(entries))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
