"""PLAN-first pretrip v2 sample preparation; no downloads, training or services."""

from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi.ml.pretrip_pilot import main


if __name__ == "__main__":
    raise SystemExit(main())
