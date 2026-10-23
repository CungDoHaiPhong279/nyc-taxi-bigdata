"""ML P1: bounded, read-only Gold survey. No training, ETL or database calls.

Run by stdin in the verified worker; stdout contains only progress/aggregates.
Exactly one projected pass over each Parquet file; no pandas/full-data collect.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import resource
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

COLUMNS = (
    "fare_amount", "trip_distance", "ratecode_id", "passenger_count",
    "pickup_datetime_local", "pickup_location_id", "dropoff_location_id",
    "pickup_hour", "pickup_month", "day_of_week", "is_weekend", "is_holiday",
    "calendar_matched", "weather_matched", "_source_year", "_source_month",
)
STAGES = (
    "pickup_in_2025_Jan_Jun", "finite_fare", "positive_fare",
    "finite_nonnegative_distance", "named_location_ids_1_263",
    "standard_rate_1", "distance_at_most_200_miles",
)
SPLITS = ("train_Jan_Apr", "validation_May", "test_June")


def new_state():
    return {
        "rows": 0, "nulls": Counter(), "flags": Counter(),
        "ratecodes": Counter(), "passengers": Counter(), "pickup_months": Counter(),
        "distance_bins": Counter(), "fare_bins": Counter(), "waterfall_kept": Counter(),
        "split_all": Counter(), "split_before_distance_cap": Counter(),
        "split_eligible": Counter(), "rate1_target": Counter(), "eligible_flags": Counter(),
        "ranges": {},
    }


def count_labels(target, array):
    labels, counts = np.unique(array, return_counts=True)
    target.update({str(label): int(count) for label, count in zip(labels, counts)})


def update(state, batch, source_year, source_month):
    n = batch.num_rows
    state["rows"] += n
    arrays = {name: batch.column(batch.schema.get_field_index(name)) for name in COLUMNS}
    for name, column in arrays.items():
        state["nulls"][name] += column.null_count

    def numeric(name):
        return np.asarray(arrays[name].to_numpy(zero_copy_only=False), dtype=np.float64)

    fare, distance, rate, passenger = [numeric(name) for name in COLUMNS[:4]]
    pu, do = numeric("pickup_location_id"), numeric("dropoff_location_id")
    pickup = arrays["pickup_datetime_local"].to_numpy(zero_copy_only=False).astype("datetime64[us]")
    pickup_month = pickup.astype("datetime64[M]")
    valid_time = ~np.isnat(pickup)
    period = valid_time & (pickup >= np.datetime64("2025-01-01")) & (pickup < np.datetime64("2025-07-01"))
    split_masks = (
        period & (pickup < np.datetime64("2025-05-01")),
        period & (pickup >= np.datetime64("2025-05-01")) & (pickup < np.datetime64("2025-06-01")),
        period & (pickup >= np.datetime64("2025-06-01")),
    )
    predicates = (
        period, np.isfinite(fare), fare > 0,
        np.isfinite(distance) & (distance >= 0),
        np.isfinite(pu) & np.isfinite(do) & (pu >= 1) & (pu <= 263) & (do >= 1) & (do <= 263),
        rate == 1, distance <= 200,
    )
    keep = np.ones(n, dtype=bool)
    for name, predicate in zip(STAGES, predicates):
        keep &= predicate
        state["waterfall_kept"][name] += int(keep.sum())
        if name == "standard_rate_1":
            for split, mask in zip(SPLITS, split_masks):
                state["split_before_distance_cap"][split] += int((mask & keep).sum())
    for split, mask in zip(SPLITS, split_masks):
        state["split_all"][split] += int(mask.sum())
        state["split_eligible"][split] += int((mask & keep).sum())

    actual_month = np.where(valid_time, pickup_month.astype(str), "NULL")
    count_labels(state["pickup_months"], actual_month)
    count_labels(state["ratecodes"], np.where(np.isfinite(rate), rate, -999))
    count_labels(state["passengers"], np.where(np.isfinite(passenger), passenger, -999))
    source_label = np.datetime64(f"{source_year}-{source_month:02d}", "M")
    flags = {
        "fare_nan": np.isnan(fare), "fare_infinite": np.isinf(fare),
        "fare_negative": fare < 0, "fare_zero": fare == 0,
        "fare_above_1000": fare > 1000, "fare_above_10000": fare > 10000,
        "distance_nan": np.isnan(distance), "distance_infinite": np.isinf(distance),
        "distance_negative": distance < 0, "distance_zero": distance == 0,
        "distance_above_200": distance > 200, "distance_above_1000": distance > 1000,
        "passenger_zero": passenger == 0, "passenger_above_6": passenger > 6,
        "pickup_outside_source_month": valid_time & (pickup_month != source_label),
        "pickup_outside_analysis_period": ~period,
        "source_lineage_mismatch": (numeric("_source_year") != source_year) | (numeric("_source_month") != source_month),
        "pickup_hour_inconsistent": valid_time & (numeric("pickup_hour") != pickup.astype("datetime64[h]").astype(np.int64) % 24),
        "pickup_month_inconsistent": valid_time & (numeric("pickup_month") != pickup_month.astype(np.int64) % 12 + 1),
        "weekday_inconsistent": period & (numeric("day_of_week") != (pickup.astype("datetime64[D]").astype(np.int64) + 3) % 7),
    }
    state["flags"].update({name: int(mask.sum()) for name, mask in flags.items()})
    state["eligible_flags"].update({name: int((mask & keep).sum()) for name, mask in flags.items()})
    state["eligible_flags"]["passenger_null"] += int((np.isnan(passenger) & keep).sum())
    for label, mask in (("negative", fare < 0), ("zero", fare == 0), ("positive", fare > 0)):
        state["rate1_target"][label] += int(((rate == 1) & mask).sum())
    for prefix, values, cuts in (("fare", fare, (0, 10, 50, 100, 1000)), ("distance", distance, (0, 1, 5, 20, 200))):
        target = state[prefix + "_bins"]
        target["negative"] += int((values < 0).sum())
        target["zero"] += int((values == 0).sum())
        for lower, upper in zip(cuts, cuts[1:]):
            target[f"({lower},{upper}]"] += int(((values > lower) & (values <= upper)).sum())
        target[f">{cuts[-1]}"] += int((values > cuts[-1]).sum())
        finite = values[np.isfinite(values)]
        if finite.size:
            lo, hi = float(finite.min()), float(finite.max())
            old = state["ranges"].get(prefix, [lo, hi])
            state["ranges"][prefix] = [min(old[0], lo), max(old[1], hi)]
    if valid_time.any():
        lo, hi = str(pickup[valid_time].min()), str(pickup[valid_time].max())
        old = state["ranges"].get("pickup", [lo, hi])
        state["ranges"]["pickup"] = [min(old[0], lo), max(old[1], hi)]


def self_test():
    # Independent literal oracle: 11 rows, mutually exclusive failure stages.
    n = 11
    values = {name: [1] * n for name in COLUMNS}
    values.update({
        "fare_amount": [10, -2, 0, None, 10, 10, 10, 10, 2000, 10, 10],
        "trip_distance": [1, 1, 1, 1, -1, 1, 1, 201, 0, 1, 1],
        "pickup_location_id": [1, 1, 1, 1, 1, 264, 1, 1, 1, 1, 1],
        "ratecode_id": [1, 1, 1, 1, 1, 1, 2, 1, 1, 1, 1],
        "passenger_count": [None] * n,
        "pickup_datetime_local": [datetime(2025, 1, 1)] * 9 + [datetime(2024, 12, 31), datetime(2025, 6, 1)],
        "pickup_hour": [0] * n, "pickup_month": [1] * 9 + [12, 6],
        "day_of_week": [2] * 9 + [1, 6], "_source_year": [2025] * n,
    })
    columns = []
    for name in COLUMNS:
        dtype = pa.timestamp("us") if name == "pickup_datetime_local" else pa.float64()
        columns.append(pa.array(values[name], type=dtype))
    state = new_state()
    update(state, pa.RecordBatch.from_arrays(columns, names=COLUMNS), 2025, 1)
    assert state["rows"] == 11
    assert state["waterfall_kept"] == dict(zip(STAGES, [10, 9, 7, 6, 5, 4, 3]))
    assert state["split_eligible"] == {SPLITS[0]: 2, SPLITS[1]: 0, SPLITS[2]: 1}
    assert state["eligible_flags"]["distance_zero"] == 1  # zero distance retained
    assert state["eligible_flags"]["fare_above_1000"] == 1  # no arbitrary target cap
    assert state["eligible_flags"]["passenger_null"] == 3  # no NULL-row deletion
    assert state["flags"]["pickup_outside_source_month"] == 2
    assert state["nulls"]["fare_amount"] == 1
    print("ML P1 profile self-test passed: literal waterfall/splits/NULL/tail checks; no data read.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-root", type=Path, default=Path("/gold"))
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    started = time.perf_counter()
    manifest_path = args.gold_root / "gold_manifest.json"
    manifest_bytes = manifest_path.read_bytes()
    entries = sorted(json.loads(manifest_bytes)["datasets"], key=lambda e: e["dataset_name"])
    assert [e["dataset_name"] for e in entries] == [f"taxi_2025-{m:02d}" for m in range(1, 7)]
    total, parts, file_inventory = new_state(), [], []
    for entry in entries:
        assert entry["validation_status"] == "valid"
        partition_start = time.perf_counter()
        directory = args.gold_root / entry["gold_path"].removeprefix("data/gold/")
        files = sorted(directory.glob("*.parquet"))
        assert files, "Missing Gold Parquet files"
        part = new_state()
        year, month = int(entry["partition"]["year"]), int(entry["partition"]["month"])
        metadata_rows = 0
        for file in files:
            before = file.stat()
            reader = pq.ParquetFile(file)
            metadata_rows += reader.metadata.num_rows
            field = reader.schema_arrow.field("pickup_datetime_local")
            assert pa.types.is_timestamp(field.type) and field.type.tz is None, "Expected preserved TIMESTAMP_NTZ"
            for batch in reader.iter_batches(batch_size=65_536, columns=list(COLUMNS), use_threads=False):
                update(part, batch, year, month)
                # Merge aggregates once per partition below, not another data pass.
            after = file.stat()
            assert (before.st_size, before.st_mtime_ns) == (after.st_size, after.st_mtime_ns)
            file_inventory.append({"path": file.relative_to(args.gold_root).as_posix(), "bytes": before.st_size, "mtime_ns": before.st_mtime_ns, "rows": reader.metadata.num_rows})
        assert part["rows"] == metadata_rows == entry["gold_rows"], "Actual/metadata/manifest count mismatch"
        for key, value in part.items():
            if key == "rows":
                total[key] += value
            elif key == "ranges":
                for name, (lo, hi) in value.items():
                    old = total[key].get(name, [lo, hi])
                    total[key][name] = [min(old[0], lo), max(old[1], hi)]
            else:
                total[key].update(value)
        parts.append({"source_month": f"{year}-{month:02d}", "gold_batch_id": entry["gold_batch_id"], "seconds": round(time.perf_counter() - partition_start, 3), **part})
        print(f"PROFILE {year}-{month:02d}: rows={part['rows']}; seconds={parts[-1]['seconds']}", flush=True)
    assert manifest_path.read_bytes() == manifest_bytes, "Manifest changed during survey"
    result = {
        "survey_version": 1, "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "method": "One projected Arrow streaming pass, 65536 rows/batch, no pandas; no sampling for counts",
        "columns": COLUMNS, "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "seconds": round(time.perf_counter() - started, 3),
        "peak_rss_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2),
        "total": total, "partitions": parts, "file_inventory": file_inventory,
    }
    print("ML_P1_RESULT " + json.dumps(result, allow_nan=False, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
