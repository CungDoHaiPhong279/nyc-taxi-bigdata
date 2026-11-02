"""Generate the small 2025 calendar source dataset enriched with authoritative US Federal Holidays."""

from __future__ import annotations

import argparse
import csv
from datetime import date, timedelta
import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "raw" / "calendar" / "calendar_2025.csv"
HOLIDAY_CONFIG_PATH = PROJECT_ROOT / "config" / "pretrip_v2_calendar.json"


def _load_holiday_lookup() -> dict[str, str]:
    if not HOLIDAY_CONFIG_PATH.is_file():
        return {}
    payload = json.loads(HOLIDAY_CONFIG_PATH.read_text(encoding="utf-8"))
    return {
        str(entry["date"]): str(entry["name"])
        for entry in payload.get("holidays", [])
        if "date" in entry and "name" in entry
    }


def build_calendar_rows(
    start_date: date = date(2025, 1, 1),
    end_date: date = date(2025, 12, 31),
) -> list[dict[str, Any]]:
    """Build inclusive calendar rows enriched with US federal observed holidays."""
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    holidays = _load_holiday_lookup()
    rows: list[dict[str, Any]] = []
    current = start_date
    while current <= end_date:
        iso_date = current.isoformat()
        holiday_name = holidays.get(iso_date, "")
        rows.append(
            {
                "date": iso_date,
                "day_of_week": current.weekday(),
                "day_name": current.strftime("%A"),
                "month": current.month,
                "year": current.year,
                "is_weekend": current.weekday() >= 5,
                "is_holiday": bool(holiday_name),
                "holiday_name": holiday_name,
            }
        )
        current += timedelta(days=1)
    return rows


def create_calendar(start_date: date, end_date: date, output_path: Path) -> int:
    """Write an inclusive calendar range and return the number of rows."""
    rows = build_calendar_rows(start_date, end_date)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "date",
        "day_of_week",
        "day_name",
        "month",
        "year",
        "is_weekend",
        "is_holiday",
        "holiday_name",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def parse_args() -> argparse.Namespace:
    """Parse optional ISO date range and output path arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default="2025-01-01", help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2025-12-31", help="Inclusive end date (YYYY-MM-DD)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def main() -> None:
    """Generate the requested calendar CSV."""
    args = parse_args()
    count = create_calendar(date.fromisoformat(args.start), date.fromisoformat(args.end), args.output)
    print(f"Created {count} calendar rows at {args.output}")


if __name__ == "__main__":
    main()
