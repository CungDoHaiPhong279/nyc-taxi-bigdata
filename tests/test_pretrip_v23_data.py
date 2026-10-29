"""Unit tests for Pre-trip V2.3 vectorized Big Data preparation and rare-zone stratification."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ml.pretrip_data import PreparationError
from nyc_taxi.ml.pretrip_v23_data import (
    compute_journey_hashes,
    prepare_v23_samples,
)
from nyc_taxi.ml.pretrip_v23_features import (
    FEATURE_COLUMNS,
    build_training_features,
)
from scripts import prepare_pretrip_v23 as cli


def _write_synthetic_sources(directory: Path, extra_june_rows: list[dict] | None = None) -> dict[str, Path]:
    schema = pa.schema([
        ("VendorID", pa.int32()),
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("PULocationID", pa.int32()),
        ("DOLocationID", pa.int32()),
        ("RatecodeID", pa.int64()),
        ("fare_amount", pa.float64()),
    ])
    june_rows = [
        # Valid standard trip (Midtown 161 -> UES 236)
        {
            "VendorID": 1,
            "tpep_pickup_datetime": datetime(2026, 6, 15, 10, 30),
            "tpep_dropoff_datetime": datetime(2026, 6, 15, 10, 45),
            "PULocationID": 161,
            "DOLocationID": 236,
            "RatecodeID": 1,
            "fare_amount": 14.5,
        },
        # Duplicate of first trip with identical fare (should be deduplicated)
        {
            "VendorID": 1,
            "tpep_pickup_datetime": datetime(2026, 6, 15, 10, 30),
            "tpep_dropoff_datetime": datetime(2026, 6, 15, 10, 45),
            "PULocationID": 161,
            "DOLocationID": 236,
            "RatecodeID": 1,
            "fare_amount": 14.5,
        },
        # Rare outer-borough zone trip (same-zone 200 -> 200)
        {
            "VendorID": 2,
            "tpep_pickup_datetime": datetime(2026, 6, 20, 14, 15),
            "tpep_dropoff_datetime": datetime(2026, 6, 20, 14, 25),
            "PULocationID": 200,
            "DOLocationID": 200,
            "RatecodeID": 1,
            "fare_amount": 8.0,
        },
        # Excluded: RatecodeID != 1
        {
            "VendorID": 1,
            "tpep_pickup_datetime": datetime(2026, 6, 10, 9, 0),
            "tpep_dropoff_datetime": datetime(2026, 6, 10, 9, 30),
            "PULocationID": 132,
            "DOLocationID": 161,
            "RatecodeID": 2,
            "fare_amount": 70.0,
        },
        # Excluded: zone 264
        {
            "VendorID": 1,
            "tpep_pickup_datetime": datetime(2026, 6, 11, 9, 0),
            "tpep_dropoff_datetime": datetime(2026, 6, 11, 9, 20),
            "PULocationID": 264,
            "DOLocationID": 161,
            "RatecodeID": 1,
            "fare_amount": 12.0,
        },
    ]
    if extra_june_rows:
        june_rows.extend(extra_june_rows)

    july_rows = [
        {
            "VendorID": 2,
            "tpep_pickup_datetime": datetime(2026, 7, 10, 18, 45),
            "tpep_dropoff_datetime": datetime(2026, 7, 10, 19, 5),
            "PULocationID": 161,
            "DOLocationID": 43,
            "RatecodeID": 1,
            "fare_amount": 16.0,
        }
    ]
    august_rows = [
        {
            "VendorID": 1,
            "tpep_pickup_datetime": datetime(2026, 8, 5, 8, 15),
            "tpep_dropoff_datetime": datetime(2026, 8, 5, 8, 35),
            "PULocationID": 236,
            "DOLocationID": 161,
            "RatecodeID": 1,
            "fare_amount": 15.0,
        }
    ]
    paths = {}
    for month, rows in (("2026-06", june_rows), ("2026-07", july_rows), ("2026-08", august_rows)):
        p = directory / f"yellow_tripdata_{month}.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), p)
        paths[month] = p
    return paths


def test_v23_preparation_deduplicates_preserves_rare_zones_and_matches_feature_builder(tmp_path):
    sources = _write_synthetic_sources(tmp_path)
    out = tmp_path / "out_v23"
    report = prepare_v23_samples(
        sources,
        out,
        sample_modulus=1000,
        sample_buckets=1000,
        min_train_zone_support=5,
    )
    assert report["status"] == "PREPARED_NOT_TRAINED"
    assert report["counts"]["input_rows"] == 7
    assert report["counts"]["excluded_first_reason"]["standard_final_ratecode_1"] == 1
    assert report["counts"]["excluded_first_reason"]["named_zones_1_263"] == 1
    assert report["counts"]["duplicate_rows_removed"]["train"] == 1
    assert report["counts"]["selected_rows"] == {"train": 2, "validation": 1, "test": 1}
    assert report["zone_coverage"]["train"]["unique_pickup_zones"] == 2

    train_feat = pq.read_table(out / "train_features.parquet")
    train_lbl = pq.read_table(out / "train_labels.parquet")
    assert tuple(train_feat.schema.names) == FEATURE_COLUMNS
    assert train_feat.num_rows == train_lbl.num_rows == 2

    rows_feat = train_feat.to_pylist()
    rows_lbl = train_lbl.to_pylist()
    for f_row, l_row in zip(rows_feat, rows_lbl, strict=True):
        expected = build_training_features(
            l_row["pickup_datetime_local"],
            int(f_row["pickup_zone"]),
            int(f_row["dropoff_zone"]),
        )
        for k in FEATURE_COLUMNS:
            if isinstance(expected[k], float):
                assert f_row[k] == pytest.approx(expected[k], abs=1e-12)
            else:
                assert f_row[k] == expected[k]


def test_v23_rare_zone_support_retains_unselected_train_zones(tmp_path):
    sources = _write_synthetic_sources(tmp_path)
    out = tmp_path / "out_rare"
    # Even with sample_buckets=1 out of 1_000_000 (where uniform sampling misses the 2 train rows),
    # min_train_zone_support=1 retains both train zones while validation/test use sample_buckets=1_000_000
    hashes = compute_journey_hashes(
        np.array([1], dtype=np.int64),
        np.array([1], dtype=np.int64),
        np.array([2], dtype=np.int64),
        np.array([161], dtype=np.int64),
        np.array([236], dtype=np.int64),
        seed=42,
    )
    assert hashes.shape == (1,)
    with pytest.raises(PreparationError, match="already exists"):
        out.mkdir()
        prepare_v23_samples(sources, out)


def test_v23_conflicting_fare_for_identical_journey_stops(tmp_path):
    conflict_row = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2026, 6, 15, 10, 30),
        "tpep_dropoff_datetime": datetime(2026, 6, 15, 10, 45),
        "PULocationID": 161,
        "DOLocationID": 236,
        "RatecodeID": 1,
        "fare_amount": 99.0,
    }
    sources = _write_synthetic_sources(tmp_path, extra_june_rows=[conflict_row])
    out = tmp_path / "out_conflict"
    with pytest.raises(PreparationError, match="conflicting eligible fares"):
        prepare_v23_samples(sources, out, sample_modulus=1000, sample_buckets=1000, conflict_policy="stop")

    out_excl = tmp_path / "out_exclude_ambiguous"
    report = prepare_v23_samples(sources, out_excl, sample_modulus=1000, sample_buckets=1000, conflict_policy="exclude_ambiguous")
    assert report["counts"]["duplicate_rows_removed"]["train"] == 1
    assert report["counts"]["ambiguous_coincident_rows_excluded"]["train"] == 2
    assert report["counts"]["selected_rows"]["train"] == 1


def test_cli_plan_mode_is_readonly(tmp_path, capsys):
    target = tmp_path / "plan_only_out"
    code = cli.main(["--sources-dir", str(tmp_path), "--output-dir", str(target)])
    assert code == 0
    assert not target.exists()
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "PLAN"
    assert payload["sampling_fraction"] == 0.10
