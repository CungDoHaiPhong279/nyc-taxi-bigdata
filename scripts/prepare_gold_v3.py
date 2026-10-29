"""Prepare the Gold 2025 ML V3 dataset (Train/Validation/Test Parquet splits)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from nyc_taxi.ml.gold_v3_data import (
    CONTRACT_VERSION,
    DEFAULT_CAPS,
    DEFAULT_GOLD_ROOT,
    FEATURE_COLUMNS,
    discover_gold_2025_files,
    load_gold_v3_contract,
    prepare_gold_v3_samples,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--gold-root",
        type=Path,
        default=DEFAULT_GOLD_ROOT,
        help="Root directory of Gold taxi_enriched partitions",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "ml" / "gold-v3",
        help="Target directory for prepared Parquet splits",
    )
    parser.add_argument(
        "--sampling-fraction",
        type=float,
        default=0.03,
        help="Deterministic SplitMix64 base sampling fraction (default: 0.03)",
    )
    parser.add_argument(
        "--min-train-zone-support",
        type=int,
        default=30,
        help="Minimum train support per active pickup and dropoff zone (default: 30)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Execute preparation instead of printing the plan",
    )
    args = parser.parse_args(argv)

    contract = load_gold_v3_contract()
    files_by_month = discover_gold_2025_files(args.gold_root)

    if not args.execute:
        plan = {
            "mode": "PLAN",
            "contract_version": CONTRACT_VERSION,
            "feature_columns": list(FEATURE_COLUMNS),
            "sampling_fraction": args.sampling_fraction,
            "min_train_zone_support": args.min_train_zone_support,
            "caps": dict(DEFAULT_CAPS),
            "output_dir": str(args.output_dir),
            "months_discovered": {k: len(v) for k, v in files_by_month.items()},
        }
        print(json.dumps(plan, indent=2))
        return 0

    report = prepare_gold_v3_samples(
        files_by_month,
        args.output_dir,
        gold_root=args.gold_root,
        sampling_fraction=args.sampling_fraction,
        min_train_zone_support=args.min_train_zone_support,
        seed=int(contract["cohort_policy"]["seed"]),
    )
    summary = {
        "status": report["status"],
        "elapsed_seconds": report["elapsed_seconds"],
        "input_rows": report["counts"]["input_rows"],
        "eligible_by_split": report["counts"]["eligible_by_split"],
        "selected_rows": report["counts"]["selected_rows"],
        "rare_zone_stratified_rows_added": report["counts"]["rare_zone_stratified_rows_added"],
        "jfk_manhattan_flat_fare_verification": report["jfk_manhattan_flat_fare_verification"],
        "zone_coverage": report["zone_coverage"],
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
