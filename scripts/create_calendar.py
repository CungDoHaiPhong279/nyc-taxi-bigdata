"""Generate the small 2025 calendar source dataset."""

from __future__ import annotations

import argparse
import csv
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "raw" / "calendar" / "calendar_2025.csv"


def create_calendar(start_date: date, end_date: date, output_path: Path) -> int:
    """Write an inclusive calendar range and return the number of rows."""
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

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

    row_count = 0
    current = start_date
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        while current <= end_date:
            writer.writerow(
                {
                    "date": current.isoformat(),
                    "day_of_week": current.weekday(),
                    "day_name": current.strftime("%A"),
                    "month": current.month,
                    "year": current.year,
                    "is_weekend": current.weekday() >= 5,
                    # TODO: Enrich with an authoritative US holiday calendar.
                    "is_holiday": False,
                    "holiday_name": "",
                }
            )
            row_count += 1
            current += timedelta(days=1)

    return row_count


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

