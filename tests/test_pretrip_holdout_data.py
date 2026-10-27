"""Synthetic August-only fixtures; June/July and raw paths are access traps."""

from copy import deepcopy
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ml import pretrip_features as features
from nyc_taxi.ml import pretrip_holdout_data as holdout
from nyc_taxi.ml import pretrip_training_data as common


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sampled_key(index):
    counter = index * 100_000
    while True:
        key = hashlib.sha256(f"holdout-synthetic-{counter}".encode("ascii")).hexdigest()
        if int.from_bytes(hashlib.sha256(f"42:{key}".encode("ascii")).digest(), "big") % 1_000_000 < 10_000:
            return key
        counter += 1


def write_metadata(sample, *, refresh_output=True, resign=True):
    directory, _, report, candidate = sample
    if refresh_output:
        labels = pq.read_table(directory / "test_labels.parquet").to_pylist()
        report["outputs"]["test"] = {
            "features": "test_features.parquet", "labels": "test_labels.parquet",
            "rows": 4,
            "features_sha256": digest(directory / "test_features.parquet"),
            "labels_sha256": digest(directory / "test_labels.parquet"),
            "journey_set_sha256": hashlib.sha256("\n".join(
                row["journey_key"] for row in labels
            ).encode("ascii")).hexdigest(),
            "row_alignment": "identical row ordering; labels journey_key ascending",
        }
    for filename in ("preparation_report.json", "preparation_checkpoint.json"):
        (directory / filename).write_text(json.dumps(report), encoding="utf-8")
    if resign:
        candidate["preparation_report_sha256"] = digest(directory / "preparation_report.json")
        candidate["preparation_checkpoint_sha256"] = digest(directory / "preparation_checkpoint.json")


@pytest.fixture
def sample(tmp_path):
    directory, config = tmp_path / "holdout", tmp_path / "config"
    directory.mkdir()
    config.mkdir()
    for name in ("pretrip_v2_contract.json", "pretrip_v2_calendar.json"):
        (config / name).write_bytes((common.DEFAULT_CONFIG / name).read_bytes())
    identities, calendar = common._identities(config)
    pairs = []
    for index in range(4):
        pickup = datetime(2026, 8, 5, 9) + timedelta(hours=index)
        pairs.append((features.build_training_features(pickup, 164, 43, calendar=calendar), {
            "journey_key": sampled_key(index), "pickup_datetime_local": pickup,
            "fare_amount": float(index + 10),
        }))
    pairs.sort(key=lambda pair: pair[1]["journey_key"])
    for kind, rows in (("features", [pair[0] for pair in pairs]), ("labels", [pair[1] for pair in pairs])):
        pq.write_table(pa.Table.from_pylist(rows), directory / f"test_{kind}.parquet")
    for name in ("train_features.parquet", "train_labels.parquet", "validation_features.parquet",
                 "validation_labels.parquet", "yellow_tripdata_2026-08.parquet"):
        (directory / name).write_bytes(b"NEVER OPEN IN HOLDOUT EVALUATION")
    sampling = {"seed": 42, "modulus": 1_000_000, "buckets": 10_000, "fraction": 0.01,
                "placement": "after cohort and before deduplication",
                "algorithm": "sha256(seed + ':' + journey_key) modulo modulus < buckets"}
    prior_hashes = {
        split: {"features_sha256": str(index) * 64, "labels_sha256": str(index + 1) * 64,
                "journey_set_sha256": str(index + 2) * 64}
        for split, index in (("train", 1), ("validation", 4))
    }
    prior_counts = {"train": 6, "validation": 5}
    source_lineage = {"2026-08": {"bytes": 123, "sha256": "f" * 64}}
    report = {
        **identities,
        "status": "PREPARED_NOT_TRAINED", "source_snapshot_verified": True,
        "identity_version": "pretrip-v2-journey-sha256-1",
        "feature_columns": list(features.FEATURE_COLUMNS), "sampling": sampling,
        "caps": {"train": 100_000, "validation": 50_000, "test": 50_000},
        "split_policy": {
            "timezone": "America/New_York", "raw_timestamp_policy": "naive TLC wall time",
            "train": "2026-06-01 <= actual pickup < 2026-07-01",
            "validation": "2026-07-01 <= actual pickup < 2026-08-01",
            "test": "2026-08-01 <= actual pickup < 2026-09-01",
        },
        "counts": {"selected_rows": {**prior_counts, "test": 4}},
        "inputs": source_lineage,
        "overlap_counts": {"train_test": 0, "train_validation": 0, "validation_test": 0},
        "overlap_scope": holdout.OVERLAP_SCOPE,
        "outputs": {
            split: {**prior_hashes[split], "features": f"{split}_features.parquet",
                    "labels": f"{split}_labels.parquet", "rows": prior_counts[split]}
            for split in ("train", "validation")
        },
    }
    candidate = {
        **identities, "feature_columns": list(features.FEATURE_COLUMNS),
        "sampling": sampling, "preparation_input_lineage": source_lineage,
        "holdout_data_accessed": False, "raw_sources_accessed": False,
        "journey_overlap_train_validation": 0, "sample_counts": prior_counts,
        "sample_hashes": prior_hashes,
        "sample_identity_sha256": hashlib.sha256(json.dumps(
            prior_hashes, sort_keys=True, separators=(",", ":"),
        ).encode("ascii")).hexdigest(),
    }
    fixture = directory, config, report, candidate
    write_metadata(fixture)
    return fixture


def verify(sample):
    return holdout.verify_holdout_metadata(
        sample[0], candidate_lineage=sample[3], config_dir=sample[1], expected_count=4,
    )


def load(sample, **kwargs):
    return holdout.load_holdout_samples(
        sample[0], candidate_lineage=sample[3], config_dir=sample[1], expected_count=4, **kwargs,
    )


def mutate(sample, kind, transform):
    path = sample[0] / f"test_{kind}.parquet"
    rows = pq.read_table(path).to_pylist()
    result = transform(rows)
    pq.write_table(pa.Table.from_pylist(rows if result is None else result), path)
    write_metadata(sample)


def test_metadata_verification_does_not_stat_open_or_decode_any_parquet(sample, monkeypatch):
    original_open, original_stat = Path.open, Path.stat

    def guard_open(path, *args, **kwargs):
        assert path.suffix != ".parquet"
        return original_open(path, *args, **kwargs)

    def guard_stat(path, *args, **kwargs):
        assert path.suffix != ".parquet"
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guard_open)
    monkeypatch.setattr(Path, "stat", guard_stat)
    monkeypatch.setattr(pq, "ParquetFile", lambda *a, **kw: pytest.fail("metadata must not decode Parquet"))
    metadata = verify(sample)
    assert metadata["expected_rows"] == 4
    assert metadata["holdout_parquet_opened"] is False
    assert json.loads(json.dumps(metadata)) == metadata


def test_load_opens_only_fixed_august_pair_never_train_validation_raw_or_discovers(sample, monkeypatch):
    original_open, original_stat = Path.open, Path.stat
    forbidden = {"train_features.parquet", "train_labels.parquet", "validation_features.parquet",
                 "validation_labels.parquet", "yellow_tripdata_2026-08.parquet"}

    def guard_open(path, *args, **kwargs):
        assert path.name not in forbidden
        return original_open(path, *args, **kwargs)

    def guard_stat(path, *args, **kwargs):
        assert path.name not in forbidden
        return original_stat(path, *args, **kwargs)

    def no_discovery(*args, **kwargs):
        pytest.fail("holdout loader must not discover any file")

    monkeypatch.setattr(Path, "open", guard_open)
    monkeypatch.setattr(Path, "stat", guard_stat)
    monkeypatch.setattr(Path, "glob", no_discovery)
    monkeypatch.setattr(Path, "rglob", no_discovery)
    result = load(sample, verified_metadata=verify(sample))
    assert set(result) == {"test", "lineage"}
    assert len(result["test"]) == 4
    lineage = result["lineage"]
    assert lineage["sample_counts"] == {"test": 4}
    assert lineage["sample_identity_sha256"] != sample[3]["sample_identity_sha256"]
    assert set(lineage["sample_hashes"]) == {"test"}
    assert lineage["holdout_data_accessed"] is True
    assert lineage["raw_sources_accessed"] is False
    assert lineage["train_validation_samples_accessed"] is False
    assert "reused pinned" in lineage["overlap_scope"]
    assert lineage["preparation_overlap_proof"]["overlap_counts"]["train_test"] == 0


def test_default_expected_rows_are_pinned_not_silently_inferred_from_metadata(sample):
    with pytest.raises(holdout.HoldoutDataError, match="output_manifest"):
        holdout.verify_holdout_metadata(sample[0], candidate_lineage=sample[3], config_dir=sample[1])


@pytest.mark.parametrize("value", [0, 50_001, True, 4.0, None])
def test_rejects_invalid_synthetic_count_override_before_access(sample, value):
    with pytest.raises(holdout.HoldoutDataError, match="expected_count"):
        holdout.verify_holdout_metadata(sample[0], candidate_lineage=sample[3], expected_count=value)


@pytest.mark.parametrize("name", ["preparation_report.json", "preparation_checkpoint.json"])
def test_preparation_json_tamper_is_blocked_before_any_holdout_open(sample, monkeypatch, name):
    with (sample[0] / name).open("ab") as stream:
        stream.write(b" ")
    monkeypatch.setattr(pq, "ParquetFile", lambda *a, **kw: pytest.fail("identity must fail first"))
    with pytest.raises(holdout.HoldoutDataError, match="preparation_metadata_identity"):
        verify(sample)


@pytest.mark.parametrize("name", [
    "feature_contract_sha256", "feature_builder_sha256", "calendar_sha256", "calendar_version",
])
def test_candidate_config_identity_must_match_current_exact_bytes(sample, name):
    sample[3][name] = "wrong"
    with pytest.raises(holdout.HoldoutDataError, match="feature_or_timezone_identity"):
        verify(sample)


def test_candidate_timezone_pin_must_match_actual_tzif(sample):
    sample[3]["timezone_identity"] = {**sample[3]["timezone_identity"], "tzif_sha256": "0" * 64}
    with pytest.raises(holdout.HoldoutDataError, match="feature_or_timezone_identity"):
        verify(sample)


@pytest.mark.parametrize("change", ["prior_identity", "prior_hash", "prior_count", "source", "holdout_accessed"])
def test_candidate_lineage_is_not_used_as_august_identity_but_must_remain_pinned(sample, change):
    if change == "prior_identity":
        sample[3]["sample_identity_sha256"] = "0" * 64
    elif change == "prior_hash":
        sample[3]["sample_hashes"]["train"]["features_sha256"] = "0" * 64
    elif change == "prior_count":
        sample[3]["sample_counts"]["train"] += 1
    elif change == "source":
        sample[3]["preparation_input_lineage"] = {}
    else:
        sample[3]["holdout_data_accessed"] = True
    with pytest.raises(holdout.HoldoutDataError, match="(lineage|identity)_mismatch"):
        verify(sample)


@pytest.mark.parametrize("change", ["path", "rows", "alignment", "sampling", "test_window", "overlap", "overlap_scope"])
def test_rejects_changed_policy_even_if_synthetic_metadata_is_resigned(sample, change):
    report = sample[2]
    if change == "path":
        report["outputs"]["test"]["features"] = "../../raw.parquet"
    elif change == "rows":
        report["outputs"]["test"]["rows"] = 3
    elif change == "alignment":
        report["outputs"]["test"]["row_alignment"] = "unordered"
    elif change == "sampling":
        report["sampling"]["seed"] = 43
    elif change == "test_window":
        report["split_policy"]["test"] = "July"
    elif change == "overlap":
        report["overlap_counts"]["train_test"] = 1
    else:
        report["overlap_scope"] = "full raw was checked"
    write_metadata(sample, refresh_output=False)
    with pytest.raises(common.TrainingDataError):
        verify(sample)


def test_rejects_frozen_metadata_drift_before_reading_holdout(sample, monkeypatch):
    frozen = verify(sample)
    frozen["expected_rows"] = 3
    monkeypatch.setattr(pq, "ParquetFile", lambda *a, **kw: pytest.fail("drift must fail before open"))
    with pytest.raises(holdout.HoldoutDataError, match="frozen_holdout_metadata_changed"):
        load(sample, verified_metadata=frozen)


@pytest.mark.parametrize("kind", ["features", "labels"])
def test_rejects_august_bytes_changed_from_pinned_metadata_before_decode(sample, monkeypatch, kind):
    with (sample[0] / f"test_{kind}.parquet").open("ab") as stream:
        stream.write(b"changed")
    monkeypatch.setattr(pq, "ParquetFile", lambda *a, **kw: pytest.fail("checksum must fail before decode"))
    with pytest.raises(holdout.HoldoutDataError, match="file_checksum"):
        load(sample)


def test_rejects_feature_label_reordering_even_if_synthetic_file_hashes_are_updated(sample):
    mutate(sample, "features", lambda rows: list(reversed(rows)))
    with pytest.raises(holdout.HoldoutDataError, match="time_alignment"):
        load(sample)


@pytest.mark.parametrize("month", [6, 7, 9])
def test_rejects_non_august_actual_pickup(sample, month):
    def wrong_window(rows):
        rows[0]["pickup_datetime_local"] = datetime(2026, month, 5, 9)
    mutate(sample, "labels", wrong_window)
    with pytest.raises(holdout.HoldoutDataError, match="actual_pickup_window"):
        load(sample)


@pytest.mark.parametrize("column", ["fare_amount", "recorded_distance", "dropoff_datetime", "journey_key"])
def test_rejects_any_predictor_leakage_column(sample, column):
    def add(rows):
        for row in rows:
            row[column] = 1.0
    mutate(sample, "features", add)
    with pytest.raises(common.TrainingDataError, match="schema_or_leakage"):
        load(sample)


@pytest.mark.parametrize("value", ["0", "264", "0164", "foreign-zone"])
def test_rejects_noncanonical_or_outside_named_zones(sample, value):
    def wrong_zone(rows):
        rows[0]["pickup_zone"] = value
    mutate(sample, "features", wrong_zone)
    with pytest.raises(common.TrainingDataError, match="zone_category"):
        load(sample)


def test_keeps_valid_zone_unseen_by_candidate_no_dictionary_fitting_or_drop(sample):
    def unseen(rows):
        for row in rows:
            row["pickup_zone"], row["dropoff_zone"] = "1", "263"
    mutate(sample, "features", unseen)
    result = load(sample)
    assert len(result["test"]) == 4
    assert all(row["pickup_zone"] == "1" and row["dropoff_zone"] == "263" for row in result["test"])


@pytest.mark.parametrize("fare", [0.0, -1.0, float("nan"), float("inf")])
def test_rejects_nonfinite_or_ineligible_label_instead_of_dropping(sample, fare):
    def invalid(rows):
        rows[0]["fare_amount"] = fare
    mutate(sample, "labels", invalid)
    with pytest.raises(holdout.HoldoutDataError, match="finite_positive"):
        load(sample)


def test_keeps_extreme_fare_instead_of_clipping(sample):
    def extreme(rows):
        rows[0]["fare_amount"] = 1e12
    mutate(sample, "labels", extreme)
    result = load(sample)
    assert len(result["test"]) == 4
    assert max(row["fare_amount"] for row in result["test"]) == 1e12


def test_rejects_duplicate_or_unordered_keys(sample):
    def duplicate(rows):
        rows[1]["journey_key"] = rows[0]["journey_key"]
    mutate(sample, "labels", duplicate)
    with pytest.raises(holdout.HoldoutDataError, match="ascending_unique"):
        load(sample)


def test_rejects_wrong_journey_set_even_if_file_hashes_match(sample):
    sample[2]["outputs"]["test"]["journey_set_sha256"] = "0" * 64
    write_metadata(sample, refresh_output=False)
    with pytest.raises(holdout.HoldoutDataError, match="journey_set_or_count"):
        load(sample)


def test_rejects_key_outside_seed42_sample(sample):
    feature_rows = pq.read_table(sample[0] / "test_features.parquet").to_pylist()
    label_rows = pq.read_table(sample[0] / "test_labels.parquet").to_pylist()
    key = hashlib.sha256(b"not-selected-holdout").hexdigest()
    while int.from_bytes(hashlib.sha256(f"42:{key}".encode("ascii")).digest(), "big") % 1_000_000 < 10_000:
        key = hashlib.sha256(key.encode("ascii")).hexdigest()
    label_rows[0]["journey_key"] = key
    pairs = sorted(zip(feature_rows, label_rows), key=lambda pair: pair[1]["journey_key"])
    for kind, rows in (("features", [pair[0] for pair in pairs]), ("labels", [pair[1] for pair in pairs])):
        pq.write_table(pa.Table.from_pylist(rows), sample[0] / f"test_{kind}.parquet")
    write_metadata(sample)
    with pytest.raises(holdout.HoldoutDataError, match="pinned_hash_sample"):
        load(sample)


def test_rejects_footer_row_count_mismatch_without_dropping(sample):
    mutate(sample, "features", lambda rows: rows[:-1])
    with pytest.raises(holdout.HoldoutDataError, match="row_count"):
        load(sample)


def test_rejects_file_signature_change_during_loading(sample, monkeypatch):
    original, calls = common._signature, 0

    def change(path):
        nonlocal calls
        signature = original(path)
        if path.name == "test_features.parquet":
            calls += 1
            if calls > 1:
                return signature[0], signature[1] + 1, signature[2], signature[3]
        return signature

    monkeypatch.setattr(common, "_signature", change)
    with pytest.raises(holdout.HoldoutDataError, match="file_changed_during_loading"):
        load(sample)
