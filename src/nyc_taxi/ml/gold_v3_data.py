"""Gold 2025 ML V3 data preparation (Medallion Gold -> Train/Validation/Test).

Streams the official 2025 Gold dataset (`data/gold/taxi_enriched/year=2025/month=01..06`,
23.88M rows integrating NYC Yellow Taxi + ERA5 Weather + Holiday Calendar + Taxi Zones),
applies deterministic SplitMix64 hash sampling plus rare-zone minimum support
stratification on BOTH pickup and dropoff zones in train, and writes bounded
Parquet feature/label tables for Spark ML training and evaluation.
"""

from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Mapping

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from nyc_taxi.ml.pretrip_v23_data import splitmix64
from nyc_taxi.ml.pretrip_v23_features import (
    HolidayCalendar,
    ZoneCentroidLookup,
    load_calendar,
    load_zone_centroids,
)


SPRING_DST_2025_START_US = int((datetime(2025, 3, 9, 2, 0, 0) - datetime(1970, 1, 1)).total_seconds() * 1_000_000)
SPRING_DST_2025_END_US = int((datetime(2025, 3, 9, 3, 0, 0) - datetime(1970, 1, 1)).total_seconds() * 1_000_000)


def _nonexistent_spring_mask(pu_us: np.ndarray) -> np.ndarray:
    """Return True for timestamps falling in the nonexistent 2025-03-09 02:00..02:59:59 NYC hour."""
    return (pu_us >= SPRING_DST_2025_START_US) & (pu_us < SPRING_DST_2025_END_US)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONTRACT_PATH = ROOT / "config" / "gold_ml_v3_contract.json"
DEFAULT_GOLD_ROOT = ROOT / "data" / "gold" / "taxi_enriched"
DEFAULT_ZONES_CSV = ROOT / "data" / "raw" / "taxi_zones" / "taxi_zone_lookup.csv"

CONTRACT_VERSION = "gold-ml-v3-2025-weather-centroid-1"

BASE_NUMERIC_FEATURES = (
    "pickup_hour_sin",
    "pickup_hour_cos",
    "pickup_dow_sin",
    "pickup_dow_cos",
    "is_weekend",
    "is_holiday",
    "zone_centroid_distance_km",
    "is_same_zone",
)
WEATHER_NUMERIC_FEATURES = (
    "temperature_2m",
    "precipitation",
    "snowfall",
    "wind_speed_10m",
)
NUMERIC_FEATURE_COLUMNS = (*BASE_NUMERIC_FEATURES, *WEATHER_NUMERIC_FEATURES)
CATEGORICAL_FEATURE_COLUMNS = ("pickup_zone", "dropoff_zone")
FEATURE_COLUMNS = (*NUMERIC_FEATURE_COLUMNS, *CATEGORICAL_FEATURE_COLUMNS)
BASE_ONLY_FEATURE_COLUMNS = (*BASE_NUMERIC_FEATURES, *CATEGORICAL_FEATURE_COLUMNS)

SPLITS = ("train", "validation", "test")
SPLIT_BOUNDARIES_US = {
    "train": (
        int((datetime(2025, 1, 1) - datetime(1970, 1, 1)).total_seconds() * 1_000_000),
        int((datetime(2025, 5, 1) - datetime(1970, 1, 1)).total_seconds() * 1_000_000),
    ),
    "validation": (
        int((datetime(2025, 5, 1) - datetime(1970, 1, 1)).total_seconds() * 1_000_000),
        int((datetime(2025, 6, 1) - datetime(1970, 1, 1)).total_seconds() * 1_000_000),
    ),
    "test": (
        int((datetime(2025, 6, 1) - datetime(1970, 1, 1)).total_seconds() * 1_000_000),
        int((datetime(2025, 7, 1) - datetime(1970, 1, 1)).total_seconds() * 1_000_000),
    ),
}
DEFAULT_CAPS = {
    "train": 350_000,
    "validation": 120_000,
    "test": 120_000,
}

GOLD_REQUIRED_COLUMNS = (
    "vendor_id",
    "pickup_datetime_local",
    "dropoff_datetime_local",
    "trip_distance",
    "ratecode_id",
    "pickup_location_id",
    "dropoff_location_id",
    "fare_amount",
    "temperature_2m",
    "precipitation",
    "snowfall",
    "wind_speed_10m",
    "weather_code",
    "weather_matched",
)


def load_gold_v3_contract(path: Path = DEFAULT_CONTRACT_PATH) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("contract_version") != CONTRACT_VERSION:
        raise ValueError(f"Unsupported Gold ML V3 contract version: {raw.get('contract_version')}")
    return raw


def discover_gold_2025_files(gold_root: Path = DEFAULT_GOLD_ROOT) -> dict[str, list[Path]]:
    """Return sorted Parquet files for each 2025 Gold month (01..06)."""
    files_by_month: dict[str, list[Path]] = {}
    for month in range(1, 7):
        key = f"2025-{month:02d}"
        month_dir = gold_root / "year=2025" / f"month={month:02d}"
        if not month_dir.is_dir():
            raise FileNotFoundError(f"Missing Gold partition directory: {month_dir}")
        found = sorted(month_dir.glob("*.parquet"))
        if not found:
            raise FileNotFoundError(f"No parquet files found in Gold partition: {month_dir}")
        files_by_month[key] = found
    return files_by_month


def load_manhattan_zone_ids(zones_csv: Path = DEFAULT_ZONES_CSV) -> set[int]:
    """Load LocationIDs belonging to Manhattan for JFK flat-fare verification."""
    import csv

    if not zones_csv.is_file():
        return set()
    with zones_csv.open(encoding="utf-8-sig", newline="") as stream:
        return {
            int(row["LocationID"])
            for row in csv.DictReader(stream)
            if row.get("Borough") == "Manhattan" and row.get("LocationID", "").isdigit()
        }


def compute_gold_hashes(
    vendor_id: np.ndarray,
    pu_us: np.ndarray,
    do_us: np.ndarray,
    pu_id: np.ndarray,
    do_id: np.ndarray,
    ratecode: np.ndarray,
    fare_bits: np.ndarray,
    dist_bits: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute 64-bit group_hash and journey_hash without filter-sensitive columns."""
    h = np.full(len(pu_us), np.uint64(0x243F6A8885A308D3), dtype=np.uint64)
    for arr in (
        vendor_id.view(np.uint64),
        pu_us.view(np.uint64),
        do_us.view(np.uint64),
        pu_id.view(np.uint64),
        do_id.view(np.uint64),
        ratecode.view(np.uint64),
    ):
        h = splitmix64(h ^ arr)
    group_hash = h.copy()
    journey_hash = splitmix64(splitmix64(h ^ fare_bits) ^ dist_bits)
    return group_hash, journey_hash


def _derive_batch_features(
    pu_us: np.ndarray,
    pu_id: np.ndarray,
    do_id: np.ndarray,
    temp_2m: np.ndarray,
    precip: np.ndarray,
    snow: np.ndarray,
    wind_10m: np.ndarray,
    calendar: HolidayCalendar,
    centroids: ZoneCentroidLookup,
    holiday_ordinals: np.ndarray,
    dist_matrix: np.ndarray,
) -> dict[str, np.ndarray]:
    seconds = pu_us // 1_000_000
    days_since_epoch = seconds // 86400
    sec_of_day = seconds % 86400
    hour = sec_of_day // 3600
    minute = (sec_of_day % 3600) // 60

    fractional_hour = hour.astype(np.float64) + minute.astype(np.float64) / 60.0
    hour_angle = 2.0 * np.pi * fractional_hour / 24.0
    hour_sin = np.sin(hour_angle)
    hour_cos = np.cos(hour_angle)

    # 1970-01-01 was Thursday (Monday=0 -> +3)
    dow = (days_since_epoch + 3) % 7
    dow_angle = 2.0 * np.pi * dow.astype(np.float64) / 7.0
    dow_sin = np.sin(dow_angle)
    dow_cos = np.cos(dow_angle)
    is_weekend = np.where(dow >= 5, 1.0, 0.0)

    epoch_ordinal = date(1970, 1, 1).toordinal()
    ordinals = days_since_epoch + epoch_ordinal
    is_holiday = np.isin(ordinals, holiday_ordinals).astype(np.float64)

    dist_km = dist_matrix[pu_id, do_id]
    is_same_zone = np.where(pu_id == do_id, 1.0, 0.0)

    return {
        "pickup_hour_sin": hour_sin,
        "pickup_hour_cos": hour_cos,
        "pickup_dow_sin": dow_sin,
        "pickup_dow_cos": dow_cos,
        "is_weekend": is_weekend,
        "is_holiday": is_holiday,
        "zone_centroid_distance_km": dist_km,
        "is_same_zone": is_same_zone,
        "temperature_2m": temp_2m.astype(np.float64),
        "precipitation": precip.astype(np.float64),
        "snowfall": snow.astype(np.float64),
        "wind_speed_10m": wind_10m.astype(np.float64),
        "pickup_zone": np.char.mod("%d", pu_id),
        "dropoff_zone": np.char.mod("%d", do_id),
    }


def _format_local_iso(pu_us: np.ndarray) -> list[str]:
    """Convert microseconds since 1970-01-01 local NTZ to ISO-8601 strings."""
    base = np.datetime64("1970-01-01T00:00:00", "us")
    dt_arr = base + pu_us.astype("timedelta64[us]")
    return np.datetime_as_string(dt_arr, unit="s").tolist()


def prepare_gold_v3_samples(
    files_by_month: Mapping[str, list[Path]] | None = None,
    output_dir: Path = ROOT / "data" / "ml" / "gold-v3",
    *,
    gold_root: Path = DEFAULT_GOLD_ROOT,
    sampling_fraction: float = 0.03,
    min_train_zone_support: int = 30,
    seed: int = 42,
    caps: Mapping[str, int] = DEFAULT_CAPS,
    fare_max_inclusive: float = 500.0,
    batch_size: int = 262_144,
) -> dict[str, Any]:
    """Execute Gold 2025 V3 sample preparation and write Parquet splits + report."""
    started = time.monotonic()
    contract = load_gold_v3_contract()
    calendar = load_calendar()
    centroids = load_zone_centroids()
    manhattan_ids = load_manhattan_zone_ids()

    if files_by_month is None:
        files_by_month = discover_gold_2025_files(gold_root)

    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"Output directory must be empty or new: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    holiday_ordinals = np.array(
        sorted(
            d.toordinal() if isinstance(d, date) else date.fromisoformat(d).toordinal()
            for d in calendar.holidays
        ),
        dtype=np.int64,
    )
    dist_matrix = np.zeros((264, 264), dtype=np.float64)
    for i in range(1, 264):
        for j in range(1, 264):
            dist_matrix[i, j] = centroids.distance_km(i, j)

    threshold = int(round(sampling_fraction * 10_000))
    seed_u64 = np.uint64(seed)

    counts = {
        "input_rows": 0,
        "by_month_input": {},
        "rejected": {
            "missing_or_nonpositive_duration": 0,
            "out_of_study_window_or_nonexistent_local": 0,
            "non_standard_ratecode": 0,
            "invalid_location_id": 0,
            "nonpositive_or_extreme_fare": 0,
            "unmatched_or_nonfinite_weather": 0,
        },
        "jfk_manhattan_ratecode2": {
            "trip_count": 0,
            "fare_70_count": 0,
            "fare_sum": 0.0,
        },
        "eligible_by_split": {s: 0 for s in SPLITS},
        "base_sampled_by_split": {s: 0 for s in SPLITS},
        "rare_zone_stratified_rows_added": 0,
        "duplicate_rows_removed": {s: 0 for s in SPLITS},
        "ambiguous_coincident_rows_excluded": {s: 0 for s in SPLITS},
        "selected_rows": {s: 0 for s in SPLITS},
    }

    full_cohort_pu_zones: dict[str, set[int]] = {s: set() for s in SPLITS}
    full_cohort_do_zones: dict[str, set[int]] = {s: set() for s in SPLITS}

    buffers: dict[str, list[dict[str, np.ndarray]]] = {s: [] for s in SPLITS}
    rare_pu_reservoir: dict[int, list[tuple[int, int, dict[str, Any]]]] = {
        z: [] for z in range(1, 264)
    }
    rare_do_reservoir: dict[int, list[tuple[int, int, dict[str, Any]]]] = {
        z: [] for z in range(1, 264)
    }

    for month_key, month_files in sorted(files_by_month.items()):
        month_rows = 0
        for pq_path in month_files:
            pf = pq.ParquetFile(pq_path)
            for batch in pf.iter_batches(batch_size=batch_size, columns=list(GOLD_REQUIRED_COLUMNS)):
                n = batch.num_rows
                counts["input_rows"] += n
                month_rows += n

                pu_col = batch.column("pickup_datetime_local")
                do_col = batch.column("dropoff_datetime_local")
                rate_col = batch.column("ratecode_id")
                pu_id_col = batch.column("pickup_location_id")
                do_id_col = batch.column("dropoff_location_id")
                fare_col = batch.column("fare_amount")
                dist_col = batch.column("trip_distance")
                vendor_col = batch.column("vendor_id")
                temp_col = batch.column("temperature_2m")
                precip_col = batch.column("precipitation")
                snow_col = batch.column("snowfall")
                wind_col = batch.column("wind_speed_10m")
                wcode_col = batch.column("weather_code")
                wmatch_col = batch.column("weather_matched")

                pu_us = pc.cast(pu_col, pa.int64()).to_numpy(zero_copy_only=False)
                do_us = pc.cast(do_col, pa.int64()).to_numpy(zero_copy_only=False)
                dt_valid = (~pu_col.is_null().to_numpy(zero_copy_only=False)) & (
                    ~do_col.is_null().to_numpy(zero_copy_only=False)
                ) & (do_us > pu_us)
                counts["rejected"]["missing_or_nonpositive_duration"] += int(np.sum(~dt_valid))

                in_window = (
                    dt_valid
                    & (pu_us >= SPLIT_BOUNDARIES_US["train"][0])
                    & (pu_us < SPLIT_BOUNDARIES_US["test"][1])
                    & (~_nonexistent_spring_mask(pu_us))
                )
                counts["rejected"]["out_of_study_window_or_nonexistent_local"] += int(
                    np.sum(dt_valid & ~in_window)
                )

                rate = rate_col.fill_null(-1).to_numpy(zero_copy_only=False).astype(np.int64)
                pu_id = pu_id_col.fill_null(-1).to_numpy(zero_copy_only=False).astype(np.int64)
                do_id = do_id_col.fill_null(-1).to_numpy(zero_copy_only=False).astype(np.int64)
                fare = fare_col.fill_null(-1.0).to_numpy(zero_copy_only=False).astype(np.float64)

                # Tally JFK (132) <-> Manhattan Ratecode 2 flat-fare trips
                if manhattan_ids:
                    jfk_man = (
                        in_window
                        & (rate == 2)
                        & (fare > 0.0)
                        & (
                            ((pu_id == 132) & np.isin(do_id, list(manhattan_ids)))
                            | ((do_id == 132) & np.isin(pu_id, list(manhattan_ids)))
                        )
                    )
                    if np.any(jfk_man):
                        jfk_fares = fare[jfk_man]
                        counts["jfk_manhattan_ratecode2"]["trip_count"] += int(len(jfk_fares))
                        counts["jfk_manhattan_ratecode2"]["fare_70_count"] += int(
                            np.sum(np.isclose(jfk_fares, 70.0))
                        )
                        counts["jfk_manhattan_ratecode2"]["fare_sum"] += float(np.sum(jfk_fares))

                rate_ok = in_window & (rate == 1)
                counts["rejected"]["non_standard_ratecode"] += int(np.sum(in_window & ~rate_ok))

                loc_ok = (
                    rate_ok
                    & (~pu_id_col.is_null().to_numpy(zero_copy_only=False))
                    & (~do_id_col.is_null().to_numpy(zero_copy_only=False))
                    & (pu_id >= 1)
                    & (pu_id <= 263)
                    & (do_id >= 1)
                    & (do_id <= 263)
                )
                counts["rejected"]["invalid_location_id"] += int(np.sum(rate_ok & ~loc_ok))

                fare_ok = (
                    loc_ok
                    & (~fare_col.is_null().to_numpy(zero_copy_only=False))
                    & np.isfinite(fare)
                    & (fare > 0.0)
                    & (fare <= fare_max_inclusive)
                )
                counts["rejected"]["nonpositive_or_extreme_fare"] += int(np.sum(loc_ok & ~fare_ok))

                wmatch = wmatch_col.fill_null(False).to_numpy(zero_copy_only=False).astype(bool)
                temp_2m = temp_col.fill_null(np.nan).to_numpy(zero_copy_only=False).astype(np.float64)
                precip = precip_col.fill_null(np.nan).to_numpy(zero_copy_only=False).astype(np.float64)
                snow = snow_col.fill_null(np.nan).to_numpy(zero_copy_only=False).astype(np.float64)
                wind_10m = wind_col.fill_null(np.nan).to_numpy(zero_copy_only=False).astype(np.float64)
                wcode = wcode_col.fill_null(-1).to_numpy(zero_copy_only=False).astype(np.int64)

                weather_ok = (
                    fare_ok
                    & wmatch
                    & np.isfinite(temp_2m)
                    & np.isfinite(precip)
                    & np.isfinite(snow)
                    & np.isfinite(wind_10m)
                )
                counts["rejected"]["unmatched_or_nonfinite_weather"] += int(
                    np.sum(fare_ok & ~weather_ok)
                )

                if not np.any(weather_ok):
                    continue

                idx = np.nonzero(weather_ok)[0]
                v_pu_us = pu_us[idx]
                v_do_us = do_us[idx]
                v_pu_id = pu_id[idx]
                v_do_id = do_id[idx]
                v_rate = rate[idx]
                v_fare = fare[idx]
                v_temp = temp_2m[idx]
                v_precip = precip[idx]
                v_snow = snow[idx]
                v_wind = wind_10m[idx]
                v_wcode = wcode[idx]
                v_vendor = vendor_col.fill_null(-1).to_numpy(zero_copy_only=False).astype(np.int64)[idx]
                v_dist = dist_col.fill_null(0.0).to_numpy(zero_copy_only=False).astype(np.float64)[idx]

                g_hash, j_hash = compute_gold_hashes(
                    v_vendor,
                    v_pu_us,
                    v_do_us,
                    v_pu_id,
                    v_do_id,
                    v_rate,
                    v_fare.view(np.uint64),
                    v_dist.view(np.uint64),
                )
                sample_val = splitmix64(j_hash ^ seed_u64) % np.uint64(10_000)
                is_sampled = sample_val < np.uint64(threshold)

                for split_name, (start_us, end_us) in SPLIT_BOUNDARIES_US.items():
                    in_split = (v_pu_us >= start_us) & (v_pu_us < end_us)
                    split_count = int(np.sum(in_split))
                    if split_count == 0:
                        continue
                    counts["eligible_by_split"][split_name] += split_count
                    full_cohort_pu_zones[split_name].update(
                        np.unique(v_pu_id[in_split]).astype(int).tolist()
                    )
                    full_cohort_do_zones[split_name].update(
                        np.unique(v_do_id[in_split]).astype(int).tolist()
                    )

                    sel = in_split & is_sampled
                    if np.any(sel):
                        s_idx = np.nonzero(sel)[0]
                        counts["base_sampled_by_split"][split_name] += len(s_idx)
                        buffers[split_name].append(
                            {
                                "pu_us": v_pu_us[s_idx],
                                "pu_id": v_pu_id[s_idx],
                                "do_id": v_do_id[s_idx],
                                "fare": v_fare[s_idx],
                                "temp_2m": v_temp[s_idx],
                                "precip": v_precip[s_idx],
                                "snow": v_snow[s_idx],
                                "wind_10m": v_wind[s_idx],
                                "wcode": v_wcode[s_idx],
                                "g_hash": g_hash[s_idx],
                                "j_hash": j_hash[s_idx],
                                "is_rare_fill": np.zeros(len(s_idx), dtype=bool),
                            }
                        )

                    if split_name == "train" and min_train_zone_support > 0:
                        unsel = in_split & (~is_sampled)
                        if np.any(unsel):
                            u_idx = np.nonzero(unsel)[0]
                            u_pu = v_pu_id[u_idx]
                            u_do = v_do_id[u_idx]
                            u_sval = sample_val[u_idx]
                            u_jhash = j_hash[u_idx]

                            # Reservoir for pickup zones
                            for z in np.unique(u_pu):
                                z_int = int(z)
                                res = rare_pu_reservoir[z_int]
                                z_Positions = np.nonzero(u_pu == z)[0]
                                if len(z_Positions) > min_train_zone_support:
                                    order = np.argsort(u_sval[z_Positions])[:min_train_zone_support]
                                    z_Positions = z_Positions[order]
                                for pos in z_Positions:
                                    orig = u_idx[pos]
                                    cand = (
                                        int(u_sval[pos]),
                                        int(u_jhash[pos]),
                                        {
                                            "pu_us": int(v_pu_us[orig]),
                                            "pu_id": int(v_pu_id[orig]),
                                            "do_id": int(v_do_id[orig]),
                                            "fare": float(v_fare[orig]),
                                            "temp_2m": float(v_temp[orig]),
                                            "precip": float(v_precip[orig]),
                                            "snow": float(v_snow[orig]),
                                            "wind_10m": float(v_wind[orig]),
                                            "wcode": int(v_wcode[orig]),
                                            "g_hash": np.uint64(g_hash[orig]),
                                            "j_hash": np.uint64(j_hash[orig]),
                                        },
                                    )
                                    res.append(cand)
                                if len(res) > min_train_zone_support * 2:
                                    res.sort(key=lambda item: (item[0], item[1]))
                                    del res[min_train_zone_support:]

                            # Reservoir for dropoff zones
                            for z in np.unique(u_do):
                                z_int = int(z)
                                res = rare_do_reservoir[z_int]
                                z_Positions = np.nonzero(u_do == z)[0]
                                if len(z_Positions) > min_train_zone_support:
                                    order = np.argsort(u_sval[z_Positions])[:min_train_zone_support]
                                    z_Positions = z_Positions[order]
                                for pos in z_Positions:
                                    orig = u_idx[pos]
                                    cand = (
                                        int(u_sval[pos]),
                                        int(u_jhash[pos]),
                                        {
                                            "pu_us": int(v_pu_us[orig]),
                                            "pu_id": int(v_pu_id[orig]),
                                            "do_id": int(v_do_id[orig]),
                                            "fare": float(v_fare[orig]),
                                            "temp_2m": float(v_temp[orig]),
                                            "precip": float(v_precip[orig]),
                                            "snow": float(v_snow[orig]),
                                            "wind_10m": float(v_wind[orig]),
                                            "wcode": int(v_wcode[orig]),
                                            "g_hash": np.uint64(g_hash[orig]),
                                            "j_hash": np.uint64(j_hash[orig]),
                                        },
                                    )
                                    res.append(cand)
                                if len(res) > min_train_zone_support * 2:
                                    res.sort(key=lambda item: (item[0], item[1]))
                                    del res[min_train_zone_support:]

        counts["by_month_input"][month_key] = month_rows

    # Top up rare pickup AND dropoff zones in train
    if min_train_zone_support > 0 and buffers["train"]:
        train_pu_all = np.concatenate([b["pu_id"] for b in buffers["train"]])
        train_do_all = np.concatenate([b["do_id"] for b in buffers["train"]])
        seen_jhashes: set[int] = set(
            int(x) for x in np.concatenate([b["j_hash"] for b in buffers["train"]])
        )
        pu_counts = np.bincount(train_pu_all, minlength=264)
        do_counts = np.bincount(train_do_all, minlength=264)

        extra_rows: list[dict[str, Any]] = []
        for z in range(1, 264):
            needed = min_train_zone_support - int(pu_counts[z])
            if needed > 0 and rare_pu_reservoir[z]:
                res = sorted(rare_pu_reservoir[z], key=lambda item: (item[0], item[1]))
                added = 0
                for _, jh, row_dict in res:
                    if jh not in seen_jhashes:
                        seen_jhashes.add(jh)
                        extra_rows.append(row_dict)
                        pu_counts[row_dict["pu_id"]] += 1
                        do_counts[row_dict["do_id"]] += 1
                        added += 1
                        if added >= needed:
                            break

        for z in range(1, 264):
            needed = min_train_zone_support - int(do_counts[z])
            if needed > 0 and rare_do_reservoir[z]:
                res = sorted(rare_do_reservoir[z], key=lambda item: (item[0], item[1]))
                added = 0
                for _, jh, row_dict in res:
                    if jh not in seen_jhashes:
                        seen_jhashes.add(jh)
                        extra_rows.append(row_dict)
                        pu_counts[row_dict["pu_id"]] += 1
                        do_counts[row_dict["do_id"]] += 1
                        added += 1
                        if added >= needed:
                            break

        if extra_rows:
            counts["rare_zone_stratified_rows_added"] = len(extra_rows)
            buffers["train"].append(
                {
                    "pu_us": np.array([r["pu_us"] for r in extra_rows], dtype=np.int64),
                    "pu_id": np.array([r["pu_id"] for r in extra_rows], dtype=np.int64),
                    "do_id": np.array([r["do_id"] for r in extra_rows], dtype=np.int64),
                    "fare": np.array([r["fare"] for r in extra_rows], dtype=np.float64),
                    "temp_2m": np.array([r["temp_2m"] for r in extra_rows], dtype=np.float64),
                    "precip": np.array([r["precip"] for r in extra_rows], dtype=np.float64),
                    "snow": np.array([r["snow"] for r in extra_rows], dtype=np.float64),
                    "wind_10m": np.array([r["wind_10m"] for r in extra_rows], dtype=np.float64),
                    "wcode": np.array([r["wcode"] for r in extra_rows], dtype=np.int64),
                    "g_hash": np.array([r["g_hash"] for r in extra_rows], dtype=np.uint64),
                    "j_hash": np.array([r["j_hash"] for r in extra_rows], dtype=np.uint64),
                    "is_rare_fill": np.ones(len(extra_rows), dtype=bool),
                }
            )

    output_files: dict[str, Any] = {}
    zone_coverage: dict[str, Any] = {}

    for split_name in SPLITS:
        if not buffers[split_name]:
            raise ValueError(f"Split {split_name} has zero sampled rows")
        pu_us = np.concatenate([b["pu_us"] for b in buffers[split_name]])
        pu_id = np.concatenate([b["pu_id"] for b in buffers[split_name]])
        do_id = np.concatenate([b["do_id"] for b in buffers[split_name]])
        fare = np.concatenate([b["fare"] for b in buffers[split_name]])
        temp_2m = np.concatenate([b["temp_2m"] for b in buffers[split_name]])
        precip = np.concatenate([b["precip"] for b in buffers[split_name]])
        snow = np.concatenate([b["snow"] for b in buffers[split_name]])
        wind_10m = np.concatenate([b["wind_10m"] for b in buffers[split_name]])
        wcode = np.concatenate([b["wcode"] for b in buffers[split_name]])
        g_hash = np.concatenate([b["g_hash"] for b in buffers[split_name]])
        j_hash = np.concatenate([b["j_hash"] for b in buffers[split_name]])
        is_rare_fill = np.concatenate([b["is_rare_fill"] for b in buffers[split_name]])

        # 1. Exact duplicate removal by journey_hash
        _, unique_idx = np.unique(j_hash, return_index=True)
        unique_idx.sort()
        counts["duplicate_rows_removed"][split_name] = int(len(j_hash) - len(unique_idx))
        pu_us, pu_id, do_id, fare = (
            pu_us[unique_idx],
            pu_id[unique_idx],
            do_id[unique_idx],
            fare[unique_idx],
        )
        temp_2m, precip, snow, wind_10m, wcode = (
            temp_2m[unique_idx],
            precip[unique_idx],
            snow[unique_idx],
            wind_10m[unique_idx],
            wcode[unique_idx],
        )
        g_hash, j_hash, is_rare_fill = (
            g_hash[unique_idx],
            j_hash[unique_idx],
            is_rare_fill[unique_idx],
        )

        # 2. Exclude ambiguous coincident group keys
        g_vals, g_counts = np.unique(g_hash, return_counts=True)
        ambig_groups = g_vals[g_counts > 1]
        if len(ambig_groups) > 0:
            keep_mask = ~np.isin(g_hash, ambig_groups)
            counts["ambiguous_coincident_rows_excluded"][split_name] = int(np.sum(~keep_mask))
            pu_us, pu_id, do_id, fare = (
                pu_us[keep_mask],
                pu_id[keep_mask],
                do_id[keep_mask],
                fare[keep_mask],
            )
            temp_2m, precip, snow, wind_10m, wcode = (
                temp_2m[keep_mask],
                precip[keep_mask],
                snow[keep_mask],
                wind_10m[keep_mask],
                wcode[keep_mask],
            )
            g_hash, j_hash, is_rare_fill = (
                g_hash[keep_mask],
                j_hash[keep_mask],
                is_rare_fill[keep_mask],
            )

        # 3. Apply cap while preserving rare-zone support rows first
        cap = int(caps[split_name])
        if len(j_hash) > cap:
            # Sort so rare-fill rows come first, then by deterministic hash
            priority = np.where(is_rare_fill, 0, 1)
            order = np.lexsort((j_hash, priority))[:cap]
            pu_us, pu_id, do_id, fare = (
                pu_us[order],
                pu_id[order],
                do_id[order],
                fare[order],
            )
            temp_2m, precip, snow, wind_10m, wcode = (
                temp_2m[order],
                precip[order],
                snow[order],
                wind_10m[order],
                wcode[order],
            )
            g_hash, j_hash = g_hash[order], j_hash[order]

        # 4. Final deterministic sort by (pu_us, j_hash)
        final_order = np.lexsort((j_hash, pu_us))
        pu_us, pu_id, do_id, fare = (
            pu_us[final_order],
            pu_id[final_order],
            do_id[final_order],
            fare[final_order],
        )
        temp_2m, precip, snow, wind_10m, wcode = (
            temp_2m[final_order],
            precip[final_order],
            snow[final_order],
            wind_10m[final_order],
            wcode[final_order],
        )
        g_hash, j_hash = g_hash[final_order], j_hash[final_order]

        counts["selected_rows"][split_name] = int(len(j_hash))

        feats = _derive_batch_features(
            pu_us,
            pu_id,
            do_id,
            temp_2m,
            precip,
            snow,
            wind_10m,
            calendar,
            centroids,
            holiday_ordinals,
            dist_matrix,
        )
        row_ids = np.arange(len(j_hash), dtype=np.int64)

        feat_table = pa.table(
            {
                "row_id": row_ids,
                **{col: feats[col] for col in FEATURE_COLUMNS},
            }
        )
        label_table = pa.table(
            {
                "row_id": row_ids,
                "fare_amount": fare,
                "pickup_datetime": _format_local_iso(pu_us),
                "pickup_location_id": pu_id.astype(np.int32),
                "dropoff_location_id": do_id.astype(np.int32),
                "weather_code": wcode,
                "journey_key": np.char.mod("%016x", j_hash),
                "group_key": np.char.mod("%016x", g_hash),
            }
        )

        feat_path = output_dir / f"{split_name}_features.parquet"
        label_path = output_dir / f"{split_name}_labels.parquet"
        pq.write_table(feat_table, feat_path, compression="snappy")
        pq.write_table(label_table, label_path, compression="snappy")

        unique_pu = sorted(np.unique(pu_id).astype(int).tolist())
        unique_do = sorted(np.unique(do_id).astype(int).tolist())
        zone_coverage[split_name] = {
            "sample_unique_pickup_zones": len(unique_pu),
            "sample_unique_dropoff_zones": len(unique_do),
            "full_cohort_unique_pickup_zones": len(full_cohort_pu_zones[split_name]),
            "full_cohort_unique_dropoff_zones": len(full_cohort_do_zones[split_name]),
            "missing_from_sample_pickup": sorted(full_cohort_pu_zones[split_name] - set(unique_pu)),
            "missing_from_sample_dropoff": sorted(full_cohort_do_zones[split_name] - set(unique_do)),
        }
        output_files[split_name] = {
            "features": {
                "path": feat_path.name,
                "rows": int(len(j_hash)),
                "bytes": feat_path.stat().st_size,
                "sha256": hashlib.sha256(feat_path.read_bytes()).hexdigest(),
            },
            "labels": {
                "path": label_path.name,
                "rows": int(len(j_hash)),
                "bytes": label_path.stat().st_size,
                "sha256": hashlib.sha256(label_path.read_bytes()).hexdigest(),
            },
        }

    jfk_stats = counts["jfk_manhattan_ratecode2"]
    jfk_trip_count = jfk_stats["trip_count"]
    jfk_summary = {
        "trip_count": jfk_trip_count,
        "fare_70_count": jfk_stats["fare_70_count"],
        "fare_70_share": round(jfk_stats["fare_70_count"] / jfk_trip_count, 6)
        if jfk_trip_count
        else None,
        "mean_fare_usd": round(jfk_stats["fare_sum"] / jfk_trip_count, 2)
        if jfk_trip_count
        else None,
        "flat_fare_rule_usd": 70.0,
    }

    report = {
        "status": "PREPARED",
        "contract_version": CONTRACT_VERSION,
        "calendar_version": calendar.version,
        "calendar_sha256": calendar.sha256,
        "geometry_sha256": centroids.sha256,
        "feature_columns": list(FEATURE_COLUMNS),
        "base_numeric_features": list(BASE_NUMERIC_FEATURES),
        "weather_numeric_features": list(WEATHER_NUMERIC_FEATURES),
        "categorical_features": list(CATEGORICAL_FEATURE_COLUMNS),
        "sampling_policy": {
            "sampling_fraction": sampling_fraction,
            "min_train_zone_support": min_train_zone_support,
            "seed": seed,
            "caps": dict(caps),
            "fare_max_inclusive": fare_max_inclusive,
        },
        "counts": counts,
        "jfk_manhattan_flat_fare_verification": jfk_summary,
        "zone_coverage": zone_coverage,
        "output_files": output_files,
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }

    report_path = output_dir / "preparation_report.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report
