"""Fast, vectorized Big Data preparation for Pre-trip V2.3 (10% hash + rare-zone stratification).

Reads the local TLC 2026-06, 2026-07, and 2026-08 Parquet sources read-only,
applies the 6-stage standard-metered cohort waterfall, performs deterministic
64-bit SplitMix64 journey hash sampling (10%) plus train-only rare-zone support
(`min_train_zone_support = 30`) so 100% of active June zones are represented in
the training dictionary, verifies zero cross-split overlap and exact parity with
`pretrip_v23_features.build_training_features`, and writes aligned Parquet
feature/label tables to a new attempt directory under `.sandbox/pretrip-v2/`.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from functools import reduce
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from nyc_taxi.ml import pretrip_v23_features
from nyc_taxi.ml.pretrip_data import (
    COHORT_STAGES,
    PreparationError,
    READ_COLUMNS,
    SOURCE_MONTHS,
    SPLITS,
    _assert_source_snapshot,
    _file_sha256,
    _source_snapshot,
)
from nyc_taxi.ml.pretrip_v23_features import (
    CONTRACT_VERSION,
    FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
    HolidayCalendar,
    ZoneCentroidLookup,
    build_training_features,
    load_calendar,
    load_contract,
    load_zone_centroids,
    timezone_identity,
)


IDENTITY_VERSION = "pretrip-v23-splitmix64-journey-1"
DEFAULT_CAPS = {"train": 350_000, "validation": 300_000, "test": 300_000}


def splitmix64(values: np.ndarray) -> np.ndarray:
    """Deterministic 64-bit unsigned SplitMix64 finalizer."""
    x = values.astype(np.uint64, copy=True)
    x = (x ^ (x >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    x = (x ^ (x >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    return x ^ (x >> np.uint64(31))


def compute_journey_hashes(
    vendor: np.ndarray,
    pickup_us: np.ndarray,
    dropoff_us: np.ndarray,
    pu_id: np.ndarray,
    do_id: np.ndarray,
    *,
    seed: int = 42,
) -> np.ndarray:
    """Compute one deterministic 64-bit hash per completed-trip identity."""
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise PreparationError("sample seed must be an integer")
    h = splitmix64(np.uint64(seed) ^ vendor.astype(np.uint64))
    h = splitmix64(h ^ pickup_us.astype(np.uint64))
    h = splitmix64(h ^ dropoff_us.astype(np.uint64))
    packed_zones = (pu_id.astype(np.uint64) << np.uint64(16)) | do_id.astype(np.uint64)
    return splitmix64(h ^ packed_zones)


def _build_distance_matrix(centroids: ZoneCentroidLookup) -> np.ndarray:
    matrix = np.zeros((264, 264), dtype=np.float64)
    for i in range(1, 264):
        for j in range(1, 264):
            matrix[i, j] = centroids.distance_km(i, j)
    return matrix


def _build_holiday_lookup(calendar: HolidayCalendar) -> tuple[int, np.ndarray]:
    start_ord = calendar.coverage_start.toordinal()
    end_ord = calendar.coverage_end.toordinal()
    flags = np.zeros(end_ord - start_ord + 1, dtype=np.float64)
    for day in calendar.holidays:
        flags[day.toordinal() - start_ord] = 1.0
    return start_ord, flags


def prepare_v23_samples(
    source_paths: Mapping[str, Path | str],
    output_dir: Path | str,
    *,
    seed: int = 42,
    sample_modulus: int = 1_000_000,
    sample_buckets: int = 100_000,
    min_train_zone_support: int = 30,
    conflict_policy: str = "exclude_ambiguous",
    caps: Mapping[str, int] | None = None,
    calendar: HolidayCalendar | None = None,
    centroids: ZoneCentroidLookup | None = None,
) -> dict[str, Any]:
    """Prepare the V2.3 Big Data sample across June, July, and August 2026."""
    t_start = time.perf_counter()
    if conflict_policy not in ("exclude_ambiguous", "stop"):
        raise PreparationError("conflict_policy must be exclude_ambiguous or stop")
    if set(source_paths) != set(SOURCE_MONTHS):
        raise PreparationError("exactly June, July and August 2026 sources are required")
    if (
        isinstance(sample_modulus, bool)
        or not isinstance(sample_modulus, int)
        or sample_modulus <= 0
        or isinstance(sample_buckets, bool)
        or not isinstance(sample_buckets, int)
        or not 0 < sample_buckets <= sample_modulus
    ):
        raise PreparationError("invalid hash-sampling bounds")
    if isinstance(min_train_zone_support, bool) or not isinstance(min_train_zone_support, int) or min_train_zone_support < 0:
        raise PreparationError("min_train_zone_support must be a non-negative integer")
    selected_caps = dict(DEFAULT_CAPS if caps is None else caps)
    if set(selected_caps) != set(SPLITS) or any(
        isinstance(v, bool) or not isinstance(v, int) or v <= 0 for v in selected_caps.values()
    ):
        raise PreparationError("each split requires a positive integer cap")

    contract = load_contract()
    selected_calendar = calendar or load_calendar()
    selected_centroids = centroids or load_zone_centroids()
    builder_path = Path(pretrip_v23_features.__file__).resolve()
    contract_path = builder_path.parents[3] / "config" / "pretrip_v23_contract.json"

    inputs = {month: Path(source_paths[month]) for month in SOURCE_MONTHS}
    for path in inputs.values():
        if path.is_symlink() or not path.is_file():
            raise PreparationError("each source must be an existing regular Parquet file")
    if len({path.resolve() for path in inputs.values()}) != len(SOURCE_MONTHS):
        raise PreparationError("source files must be distinct")
    source_snapshots = {month: _source_snapshot(path) for month, path in inputs.items()}

    destination = Path(output_dir).absolute()
    if destination.exists() or destination.is_symlink():
        raise PreparationError("output directory already exists; choose a new attempt directory")
    destination.mkdir(parents=True, exist_ok=False)
    checkpoint_path = destination / "preparation_checkpoint.json"

    counts: dict[str, Any] = {
        "input_rows": 0,
        "waterfall_kept": dict.fromkeys(COHORT_STAGES, 0),
        "excluded_first_reason": dict.fromkeys(COHORT_STAGES, 0),
        "eligible_by_split": dict.fromkeys(SPLITS, 0),
        "uniform_sampled_rows_before_stratification": dict.fromkeys(SPLITS, 0),
        "rare_zone_stratified_rows_added": {"train": 0, "validation": 0, "test": 0},
        "sampled_rows_before_dedup": dict.fromkeys(SPLITS, 0),
        "duplicate_rows_removed": dict.fromkeys(SPLITS, 0),
        "ambiguous_coincident_rows_excluded": dict.fromkeys(SPLITS, 0),
        "selected_rows": dict.fromkeys(SPLITS, 0),
        "source_input_rows": dict.fromkeys(SOURCE_MONTHS, 0),
    }

    report: dict[str, Any] = {
        "status": "INCOMPLETE",
        "identity_version": IDENTITY_VERSION,
        "feature_contract_version": CONTRACT_VERSION,
        "max_bins": contract["max_bins"],
        "calendar_version": selected_calendar.version,
        "calendar_sha256": selected_calendar.sha256,
        "geometry_sha256": selected_centroids.sha256,
        "feature_contract_sha256": _file_sha256(contract_path),
        "feature_builder_sha256": _file_sha256(builder_path),
        "timezone_identity": timezone_identity(),
        "inputs": {},
        "source_snapshot_verified": False,
        "read_columns": list(READ_COLUMNS),
        "feature_columns": list(FEATURE_COLUMNS),
        "sampling": {
            "algorithm": "splitmix64(seed, VendorID, pickup_us, dropoff_us, PULocationID, DOLocationID) % modulus < buckets + train rare-zone support",
            "seed": seed,
            "modulus": sample_modulus,
            "buckets": sample_buckets,
            "fraction": sample_buckets / sample_modulus,
            "min_train_zone_support": min_train_zone_support,
            "placement": "after cohort and before deduplication; rare-zone support applied on train only",
        },
        "split_policy": {
            "timezone": "America/New_York",
            "raw_timestamp_policy": "naive TLC wall time",
            "train": "2026-06-01 <= actual pickup < 2026-07-01",
            "validation": "2026-07-01 <= actual pickup < 2026-08-01",
            "test": "2026-08-01 <= actual pickup < 2026-09-01",
        },
        "caps": selected_caps,
        "counts": counts,
        "zone_coverage": {},
        "outputs": {},
    }

    try:
        checkpoint_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        eligible_tables: list[pa.Table] = []
        w_start = datetime(2026, 6, 1)
        w_val = datetime(2026, 7, 1)
        w_test = datetime(2026, 8, 1)
        w_end = datetime(2026, 9, 1)

        for month, path in inputs.items():
            _assert_source_snapshot(path, source_snapshots[month])
            source_digest = _file_sha256(path)
            _assert_source_snapshot(path, source_snapshots[month])
            report["inputs"][month] = {
                "bytes": source_snapshots[month][0],
                "sha256": source_digest,
                "mtime_ns": source_snapshots[month][1],
            }
            pf = pq.ParquetFile(path)
            missing = sorted(set(READ_COLUMNS) - set(pf.schema_arrow.names))
            if missing:
                raise PreparationError("source Parquet is missing required columns")
            for name in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
                ft = pf.schema_arrow.field(name).type
                if not pa.types.is_timestamp(ft) or ft.tz is not None:
                    raise PreparationError("source timestamps must be timezone-naive TLC wall time")

            table = pq.read_table(path, columns=list(READ_COLUMNS))
            n_rows = table.num_rows
            counts["input_rows"] += n_rows
            counts["source_input_rows"][month] = n_rows

            # Stage 1: valid_local_pickup (non-null naive timestamp)
            s1 = pc.is_valid(table["tpep_pickup_datetime"])
            n1 = int(pc.sum(s1).as_py() or 0)
            counts["excluded_first_reason"]["valid_local_pickup"] += n_rows - n1
            counts["waterfall_kept"]["valid_local_pickup"] += n1

            # Stage 2: pickup_in_pilot_window
            s2 = reduce(
                pc.and_kleene,
                [
                    s1,
                    pc.greater_equal(table["tpep_pickup_datetime"], w_start),
                    pc.less(table["tpep_pickup_datetime"], w_end),
                ],
            )
            n2 = int(pc.sum(s2).as_py() or 0)
            counts["excluded_first_reason"]["pickup_in_pilot_window"] += n1 - n2
            counts["waterfall_kept"]["pickup_in_pilot_window"] += n2

            # Stage 3: finite_positive_fare
            fare_col = table["fare_amount"]
            s3 = reduce(
                pc.and_kleene,
                [
                    s2,
                    pc.is_valid(fare_col),
                    pc.is_finite(fare_col),
                    pc.greater(fare_col, 0.0),
                ],
            )
            n3 = int(pc.sum(s3).as_py() or 0)
            counts["excluded_first_reason"]["finite_positive_fare"] += n2 - n3
            counts["waterfall_kept"]["finite_positive_fare"] += n3

            # Stage 4: standard_final_ratecode_1
            rc_col = table["RatecodeID"]
            s4 = reduce(pc.and_kleene, [s3, pc.is_valid(rc_col), pc.equal(rc_col, 1)])
            n4 = int(pc.sum(s4).as_py() or 0)
            counts["excluded_first_reason"]["standard_final_ratecode_1"] += n3 - n4
            counts["waterfall_kept"]["standard_final_ratecode_1"] += n4

            # Stage 5: named_zones_1_263
            pu_col, do_col = table["PULocationID"], table["DOLocationID"]
            s5 = reduce(
                pc.and_kleene,
                [
                    s4,
                    pc.is_valid(pu_col),
                    pc.is_valid(do_col),
                    pc.greater_equal(pu_col, 1),
                    pc.less_equal(pu_col, 263),
                    pc.greater_equal(do_col, 1),
                    pc.less_equal(do_col, 263),
                ],
            )
            n5 = int(pc.sum(s5).as_py() or 0)
            counts["excluded_first_reason"]["named_zones_1_263"] += n4 - n5
            counts["waterfall_kept"]["named_zones_1_263"] += n5

            # Stage 6: valid_completed_journey_identity
            vid_col, drop_col = table["VendorID"], table["tpep_dropoff_datetime"]
            s6 = reduce(
                pc.and_kleene,
                [
                    s5,
                    pc.is_valid(vid_col),
                    pc.greater(vid_col, 0),
                    pc.is_valid(drop_col),
                    pc.greater_equal(drop_col, table["tpep_pickup_datetime"]),
                ],
            )
            n6 = int(pc.sum(s6).as_py() or 0)
            counts["excluded_first_reason"]["valid_completed_journey_identity"] += n5 - n6
            counts["waterfall_kept"]["valid_completed_journey_identity"] += n6

            eligible_tables.append(table.filter(s6))
            _assert_source_snapshot(path, source_snapshots[month])

        elig = pa.concat_tables(eligible_tables)
        if elig.num_rows == 0:
            raise PreparationError("no eligible rows found across input sources")

        is_train = pc.less(elig["tpep_pickup_datetime"], w_val).to_numpy()
        is_val = reduce(
            pc.and_kleene,
            [
                pc.greater_equal(elig["tpep_pickup_datetime"], w_val),
                pc.less(elig["tpep_pickup_datetime"], w_test),
            ],
        ).to_numpy()
        is_test = pc.greater_equal(elig["tpep_pickup_datetime"], w_test).to_numpy()

        counts["eligible_by_split"]["train"] = int(is_train.sum())
        counts["eligible_by_split"]["validation"] = int(is_val.sum())
        counts["eligible_by_split"]["test"] = int(is_test.sum())

        vendor = elig["VendorID"].to_numpy().astype(np.int64)
        pu_us = pc.cast(elig["tpep_pickup_datetime"], pa.int64()).to_numpy().astype(np.int64)
        do_us = pc.cast(elig["tpep_dropoff_datetime"], pa.int64()).to_numpy().astype(np.int64)
        pu_id = elig["PULocationID"].to_numpy().astype(np.int64)
        do_id = elig["DOLocationID"].to_numpy().astype(np.int64)

        hashes = compute_journey_hashes(vendor, pu_us, do_us, pu_id, do_id, seed=seed)
        uniform_mask = (hashes % np.uint64(sample_modulus)) < np.uint64(sample_buckets)

        counts["uniform_sampled_rows_before_stratification"]["train"] = int((is_train & uniform_mask).sum())
        counts["uniform_sampled_rows_before_stratification"]["validation"] = int((is_val & uniform_mask).sum())
        counts["uniform_sampled_rows_before_stratification"]["test"] = int((is_test & uniform_mask).sum())

        sampled_mask = uniform_mask.copy()
        if min_train_zone_support > 0:
            train_indices = np.flatnonzero(is_train)
            for zone_arr in (pu_id, do_id):
                sampled_train_zones = zone_arr[is_train & sampled_mask]
                vals, z_counts = np.unique(sampled_train_zones, return_counts=True)
                current_support = dict(zip(vals.tolist(), z_counts.tolist()))
                for z in np.unique(zone_arr[is_train]):
                    if current_support.get(int(z), 0) < min_train_zone_support:
                        z_rows = train_indices[zone_arr[train_indices] == z]
                        order = np.argsort(hashes[z_rows], kind="stable")[:min_train_zone_support]
                        sampled_mask[z_rows[order]] = True

        counts["rare_zone_stratified_rows_added"]["train"] = int(
            (is_train & sampled_mask).sum() - counts["uniform_sampled_rows_before_stratification"]["train"]
        )

        split_masks = {"train": is_train, "validation": is_val, "test": is_test}
        for s_name, s_mask in split_masks.items():
            counts["sampled_rows_before_dedup"][s_name] = int((s_mask & sampled_mask).sum())

        # Filter down to sampled rows and perform exact journey deduplication + conflict/overlap checks
        sampled_indices = np.flatnonzero(sampled_mask)
        sampled_table = elig.take(pa.array(sampled_indices))
        s_vendor = vendor[sampled_indices]
        s_pu_us = pu_us[sampled_indices]
        s_do_us = do_us[sampled_indices]
        s_pu_id = pu_id[sampled_indices]
        s_do_id = do_id[sampled_indices]
        s_hashes = hashes[sampled_indices]
        s_fares = sampled_table["fare_amount"].to_numpy().astype(np.float64)
        s_pickups = sampled_table["tpep_pickup_datetime"].to_pylist()
        s_split_codes = np.where(is_train[sampled_indices], 0, np.where(is_val[sampled_indices], 1, 2))
        split_names = ("train", "validation", "test")

        retained_by_identity: dict[tuple[int, int, int, int, int], dict[str, Any]] = {}
        ambiguous_identities: set[tuple[int, int, int, int, int]] = set()
        for idx in range(len(sampled_indices)):
            ident = (
                int(s_vendor[idx]),
                int(s_pu_us[idx]),
                int(s_do_us[idx]),
                int(s_pu_id[idx]),
                int(s_do_id[idx]),
            )
            split_name = split_names[int(s_split_codes[idx])]
            fare_val = float(s_fares[idx])
            if ident in ambiguous_identities:
                counts["ambiguous_coincident_rows_excluded"][split_name] += 1
                continue
            prev = retained_by_identity.get(ident)
            if prev is not None:
                if prev["split"] != split_name:
                    raise PreparationError("retained journey overlaps temporal splits")
                if prev["fare_amount"] != fare_val:
                    if conflict_policy == "stop":
                        raise PreparationError("conflicting eligible fares for a retained journey")
                    ambiguous_identities.add(ident)
                    del retained_by_identity[ident]
                    counts["selected_rows"][split_name] -= 1
                    counts["ambiguous_coincident_rows_excluded"][split_name] += 2
                    continue
                counts["duplicate_rows_removed"][split_name] += 1
                continue
            key_str = f"{int(s_hashes[idx]):016x}:{ident[0]}:{ident[1]}:{ident[2]}:{ident[3]}:{ident[4]}"
            retained_by_identity[ident] = {
                "journey_key": key_str,
                "split": split_name,
                "fare_amount": fare_val,
                "pickup_datetime_local": s_pickups[idx],
                "pu_id": ident[3],
                "do_id": ident[4],
            }
            counts["selected_rows"][split_name] += 1
            if counts["selected_rows"][split_name] > selected_caps[split_name]:
                raise PreparationError("retained split exceeds its predeclared cap")

        if any(counts["selected_rows"][s] == 0 for s in SPLITS):
            raise PreparationError("every temporal split must retain at least one sampled journey")

        dist_matrix = _build_distance_matrix(selected_centroids)
        cal_start_ord, cal_flags = _build_holiday_lookup(selected_calendar)

        for split_name in SPLITS:
            rows = [r for r in retained_by_identity.values() if r["split"] == split_name]
            rows.sort(key=lambda r: r["journey_key"])
            n_split = len(rows)

            pickups = [r["pickup_datetime_local"] for r in rows]
            pu_arr = np.array([r["pu_id"] for r in rows], dtype=np.int64)
            do_arr = np.array([r["do_id"] for r in rows], dtype=np.int64)
            fares_arr = np.array([r["fare_amount"] for r in rows], dtype=np.float64)
            keys_list = [r["journey_key"] for r in rows]

            hours = np.array([dt.hour for dt in pickups], dtype=np.float64)
            minutes = np.array([dt.minute for dt in pickups], dtype=np.float64)
            dows = np.array([dt.weekday() for dt in pickups], dtype=np.float64)
            ordinals = np.array([dt.date().toordinal() for dt in pickups], dtype=np.int64)

            if np.any(ordinals < cal_start_ord) or np.any(ordinals >= cal_start_ord + len(cal_flags)):
                raise PreparationError("pickup date lies outside pinned calendar coverage")

            hour_angles = 2.0 * math.pi * (hours + minutes / 60.0) / 24.0
            dow_angles = 2.0 * math.pi * dows / 7.0

            f_hour_sin = np.sin(hour_angles)
            f_hour_cos = np.cos(hour_angles)
            f_dow_sin = np.sin(dow_angles)
            f_dow_cos = np.cos(dow_angles)
            f_weekend = (dows >= 5.0).astype(np.float64)
            f_holiday = cal_flags[ordinals - cal_start_ord]
            f_dist_km = dist_matrix[pu_arr, do_arr]
            f_same_zone = (pu_arr == do_arr).astype(np.float64)
            f_pu_str = [str(int(x)) for x in pu_arr]
            f_do_str = [str(int(x)) for x in do_arr]

            # Parity verification on up to 256 deterministic sample rows per split
            check_step = max(1, n_split // 256)
            for check_idx in range(0, n_split, check_step):
                ref = build_training_features(
                    pickups[check_idx],
                    int(pu_arr[check_idx]),
                    int(do_arr[check_idx]),
                    calendar=selected_calendar,
                    centroids=selected_centroids,
                )
                vec_vals = (
                    float(f_hour_sin[check_idx]),
                    float(f_hour_cos[check_idx]),
                    float(f_dow_sin[check_idx]),
                    float(f_dow_cos[check_idx]),
                    float(f_weekend[check_idx]),
                    float(f_holiday[check_idx]),
                    float(f_dist_km[check_idx]),
                    float(f_same_zone[check_idx]),
                )
                for col_name, got_val in zip(NUMERIC_FEATURE_COLUMNS, vec_vals, strict=True):
                    if abs(got_val - ref[col_name]) > 1e-12:
                        raise PreparationError(f"vectorized feature parity mismatch on {col_name}")
                if f_pu_str[check_idx] != ref["pickup_zone"] or f_do_str[check_idx] != ref["dropoff_zone"]:
                    raise PreparationError("vectorized categorical zone mismatch")

            feature_table = pa.table(
                {
                    "pickup_hour_sin": pa.array(f_hour_sin, type=pa.float64()),
                    "pickup_hour_cos": pa.array(f_hour_cos, type=pa.float64()),
                    "pickup_dow_sin": pa.array(f_dow_sin, type=pa.float64()),
                    "pickup_dow_cos": pa.array(f_dow_cos, type=pa.float64()),
                    "is_weekend": pa.array(f_weekend, type=pa.float64()),
                    "is_holiday": pa.array(f_holiday, type=pa.float64()),
                    "zone_centroid_distance_km": pa.array(f_dist_km, type=pa.float64()),
                    "is_same_zone": pa.array(f_same_zone, type=pa.float64()),
                    "pickup_zone": pa.array(f_pu_str, type=pa.string()),
                    "dropoff_zone": pa.array(f_do_str, type=pa.string()),
                }
            )
            label_table = pa.table(
                {
                    "journey_key": pa.array(keys_list, type=pa.string()),
                    "pickup_datetime_local": pa.array(pickups, type=pa.timestamp("us")),
                    "fare_amount": pa.array(fares_arr, type=pa.float64()),
                }
            )

            features_path = destination / f"{split_name}_features.parquet"
            labels_path = destination / f"{split_name}_labels.parquet"
            pq.write_table(feature_table, features_path, compression="snappy")
            pq.write_table(label_table, labels_path, compression="snappy")

            unique_pu = sorted(int(x) for x in np.unique(pu_arr))
            unique_do = sorted(int(x) for x in np.unique(do_arr))
            report["zone_coverage"][split_name] = {
                "unique_pickup_zones": len(unique_pu),
                "unique_dropoff_zones": len(unique_do),
                "pickup_location_ids": unique_pu,
                "dropoff_location_ids": unique_do,
            }
            report["outputs"][split_name] = {
                "features": features_path.name,
                "labels": labels_path.name,
                "rows": n_split,
                "features_bytes": features_path.stat().st_size,
                "labels_bytes": labels_path.stat().st_size,
                "features_sha256": _file_sha256(features_path),
                "labels_sha256": _file_sha256(labels_path),
                "journey_set_sha256": hashlib.sha256("\n".join(keys_list).encode("ascii")).hexdigest(),
                "row_alignment": "identical row ordering; labels journey_key ascending",
            }

        for month, path in inputs.items():
            _assert_source_snapshot(path, source_snapshots[month])
        report["overlap_counts"] = {"train_validation": 0, "train_test": 0, "validation_test": 0}
        report["source_snapshot_verified"] = True
        report["elapsed_seconds"] = round(time.perf_counter() - t_start, 3)
        report["status"] = "PREPARED_NOT_TRAINED"

        report_json = json.dumps(report, indent=2)
        (destination / "preparation_report.json").write_text(report_json, encoding="utf-8")
        checkpoint_path.write_text(report_json, encoding="utf-8")
        return report
    except Exception as error:
        report["status"] = "FAILED"
        report["error_type"] = type(error).__name__
        checkpoint_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        raise
