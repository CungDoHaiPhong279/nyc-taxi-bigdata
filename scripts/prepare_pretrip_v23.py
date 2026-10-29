"""Prepare the Pre-trip V2.3 Big Data sample (10% hash + train rare-zone support)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi.ml.pretrip_v23_data import DEFAULT_CAPS, prepare_v23_samples
from nyc_taxi.ml.pretrip_v23_features import CONTRACT_VERSION, FEATURE_COLUMNS, load_contract


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sources-dir",
        type=Path,
        default=PROJECT_ROOT / ".sandbox" / "pretrip-v2" / "sources",
        help="Directory containing yellow_tripdata_2026-{06,07,08}.parquet",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / ".sandbox" / "pretrip-v2" / "pilot-2026-06-08-v23-01",
        help="New output directory under .sandbox/pretrip-v2/",
    )
    parser.add_argument("--execute", action="store_true", help="Execute preparation instead of printing plan")
    args = parser.parse_args(argv)

    contract = load_contract()
    sources = {
        month: args.sources_dir / f"yellow_tripdata_{month}.parquet"
        for month in ("2026-06", "2026-07", "2026-08")
    }
    if not args.execute:
        plan = {
            "mode": "PLAN",
            "contract_version": CONTRACT_VERSION,
            "feature_columns": list(FEATURE_COLUMNS),
            "sampling_fraction": contract["cohort_policy"]["sampling_fraction"],
            "min_train_zone_support": contract["cohort_policy"]["min_train_zone_support"],
            "caps": DEFAULT_CAPS,
            "output_dir": str(args.output_dir),
            "sources_present": {m: p.is_file() for m, p in sources.items()},
        }
        print(json.dumps(plan, indent=2))
        return 0

    report = prepare_v23_samples(sources, args.output_dir)
    summary = {
        "status": report["status"],
        "elapsed_seconds": report["elapsed_seconds"],
        "input_rows": report["counts"]["input_rows"],
        "eligible_by_split": report["counts"]["eligible_by_split"],
        "selected_rows": report["counts"]["selected_rows"],
        "duplicate_rows_removed": report["counts"]["duplicate_rows_removed"],
        "ambiguous_coincident_rows_excluded": report["counts"]["ambiguous_coincident_rows_excluded"],
        "rare_zone_stratified_rows_added": report["counts"]["rare_zone_stratified_rows_added"],
        "zone_coverage": {
            s: {
                "unique_pickup_zones": report["zone_coverage"][s]["unique_pickup_zones"],
                "unique_dropoff_zones": report["zone_coverage"][s]["unique_dropoff_zones"],
            }
            for s in ("train", "validation", "test")
        },
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
