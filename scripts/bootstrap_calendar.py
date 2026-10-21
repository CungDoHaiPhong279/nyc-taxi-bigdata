"""Create the bootstrap calendar only when missing; keep valid sources unchanged."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from nyc_taxi.common.config import load_config, resolve_project_path
from nyc_taxi.ingestion.calendar import validate_existing_calendar


def ensure_calendar(path: Path) -> dict[str, Any]:
    """Reuse the existing generator without overwriting an existing source."""
    path = Path(path)
    if path.exists() or path.is_symlink():
        if not path.is_file():
            raise ValueError("Calendar source exists but is not a regular file")
        report = validate_existing_calendar(path)
        print("Calendar: existing valid 2025 source kept unchanged.")
        return report

    subprocess.run(
        [
            sys.executable,
            "-B",
            str(PROJECT_ROOT / "scripts" / "create_calendar.py"),
            "--start", "2025-01-01",
            "--end", "2025-12-31",
            "--output", str(path),
        ],
        check=True,
    )
    report = validate_existing_calendar(path)
    print("Calendar: missing 2025 source created and validated.")
    return report


def main() -> int:
    """Resolve the configured raw directory and fail safely on invalid sources."""
    try:
        config = load_config()
        path = resolve_project_path(config["paths"]["raw"]) / "calendar" / "calendar_2025.csv"
        ensure_calendar(path)
        return 0
    except Exception:
        # Avoid exposing arbitrary configuration/error contents in bootstrap output.
        print(
            "Calendar bootstrap failed. Check the public path configuration and calendar "
            "source; existing files were not replaced. Resolve the issue before retrying.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
