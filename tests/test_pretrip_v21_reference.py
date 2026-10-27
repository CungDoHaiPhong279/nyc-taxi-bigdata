"""Tiny reference-artifact guards, with no Spark/real samples/holdout access."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ml import pretrip_training_reference as guard
from nyc_taxi.ml.pretrip_features import FEATURE_COLUMNS
from nyc_taxi.ml.pretrip_training_model import BASELINE_POLICY, GBT_PARAMETERS


def _write_json(bundle, name, value):
    path = bundle[0] / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    _refresh_entry(bundle, name)


def _refresh_entry(bundle, name):
    path, manifest = bundle
    target = path / name
    entry = {"path": name, "bytes": target.stat().st_size,
             "sha256": hashlib.sha256(target.read_bytes()).hexdigest()}
    manifest["files"] = [item for item in manifest["files"] if item["path"] != name] + [entry]


def _mutate_json(bundle, name, mutate):
    value = json.loads((bundle[0] / name).read_text())
    mutate(value)
    _write_json(bundle, name, value)


@pytest.fixture
def bundle(tmp_path, monkeypatch):
    # Exercise all guards without creating 50k rows: only the row-count constant
    # is patched for this synthetic bundle. Real model/sample hashes stay pinned.
    monkeypatch.setattr(guard, "REFERENCE_COUNTS", {"train": 2, "validation": 1})
    root = tmp_path / "reference"
    root.mkdir()
    manifest = {"version": "pretrip-v2-training-artifacts-1", "files": [],
                "model_tree_sha256": guard.REFERENCE_MODEL_TREE_SHA256,
                "holdout_opened": False, "promoted": False, "deployed": False}
    result = (root, manifest)
    lineage = {
        "sample_identity_sha256": guard.REFERENCE_SAMPLE_IDENTITY_SHA256,
        "sample_counts": dict(guard.REFERENCE_COUNTS), "feature_columns": list(FEATURE_COLUMNS),
        "journey_overlap_train_validation": 0, "holdout_data_accessed": False, "raw_sources_accessed": False,
        "sample_hashes": {"train": {"features_sha256": "a" * 64, "labels_sha256": "b" * 64},
                          "validation": {"features_sha256": "c" * 64, "labels_sha256": "d" * 64}},
        "feature_contract_sha256": "e" * 64, "calendar_sha256": "f" * 64,
        "timezone_identity": {"timezone": "America/New_York", "tzif_sha256": "1" * 64},
        "preparation_input_lineage": {"2026-08": {"sha256": "2" * 64}},
    }
    config = {"gbt_parameters": dict(GBT_PARAMETERS), "baseline_policy": dict(BASELINE_POLICY),
              "features": list(FEATURE_COLUMNS), "holdout_opened": False,
              "sample_identity_sha256": guard.REFERENCE_SAMPLE_IDENTITY_SHA256,
              "runtime": {"spark": "4.2.0", "jvm_max_heap_bytes": 1536 * 1024 * 1024},
              "gate": {"baseline_mae_usd": guard.REFERENCE_BASELINE_MAE_USD,
                       "required_mae_improvement_percent": 10.0, "pass": False}}
    metrics = {split: {name: {"count": count, "mae_usd": mae} for name, mae in
                       (("global_median", 8.0), ("hierarchical_median", guard.REFERENCE_BASELINE_MAE_USD),
                        ("gbt", 6.3))} for split, count in guard.REFERENCE_COUNTS.items()}
    baseline = {**BASELINE_POLICY, "train_count": 2, "global_median": 11.0,
                "pair_groups": [], "time_groups": []}
    for name, value in (("training_config.json", config), ("sample_lineage.json", lineage),
                        ("metrics.json", metrics), ("baselines.json", baseline)):
        _write_json(result, name, value)
    for stage, labels in ((0, ["1", "2"]), (1, ["2", "3"])):
        name = f"pipeline/stages/{stage}_StringIndexer_fixture/data/part-00000.parquet"
        target = root / name
        target.parent.mkdir(parents=True)
        pq.write_table(pa.table({"labelsArray": pa.array([[labels]],
                       type=pa.list_(pa.list_(pa.string())))}), target)
        _refresh_entry(result, name)
    _write_json(result, "pipeline/stages/4_GBTRegressionModel_fixture/metadata/part-00000.txt",
                {"class": "org.apache.spark.ml.regression.GBTRegressionModel",
                 "paramMap": {"lossType": "squared"}, "defaultParamMap": {"lossType": "squared"}})
    # Explicit invalid files: using directory discovery rather than manifest
    # members would read these traps. No corresponding manifest entry exists.
    (root / "test_features.parquet").write_bytes(b"NEVER OPEN AUGUST")
    (root / "yellow_tripdata_2026-08.parquet").write_bytes(b"NEVER OPEN RAW")
    return result


def _loaded(reference):
    return {"lineage": deepcopy(reference["lineage"]),
            "train": [{"pickup_zone": "1", "dropoff_zone": "2"},
                      {"pickup_zone": "2", "dropoff_zone": "3"}],
            # Unseen validation categories must not enter train dictionaries.
            "validation": [{"pickup_zone": "263", "dropoff_zone": "262"}]}


def test_reads_only_manifest_metadata_and_two_indexer_files_never_raw_august(bundle, monkeypatch):
    opened = []
    original_open, original_parquet = Path.open, pq.ParquetFile

    def tracked_open(path, *args, **kwargs):
        assert "2026-08" not in path.name and not path.name.startswith("test_")
        opened.append(path.relative_to(bundle[0]).as_posix())
        return original_open(path, *args, **kwargs)

    def exact_parquet(path, *args, **kwargs):
        assert Path(path).as_posix().endswith("/data/part-00000.parquet")
        opened.append(Path(path).relative_to(bundle[0]).as_posix())
        return original_parquet(path, *args, **kwargs)

    def no_discovery(*args, **kwargs):
        raise AssertionError("reference must not discover directories")

    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr(Path, "glob", no_discovery)
    monkeypatch.setattr(Path, "rglob", no_discovery)
    monkeypatch.setattr(pq, "ParquetFile", exact_parquet)
    reference = guard.read_reference(*bundle)
    audit = guard.verify_current(reference, _loaded(reference), reference["baselines"])
    assert reference["dictionaries"] == {"pickup_zone": ["1", "2"], "dropoff_zone": ["2", "3"]}
    assert audit["train_dictionary"]["dictionary_sha256"] == reference["dictionary_sha256"]
    assert audit["baseline_artifact_equal"] and audit["sample_hashes_and_full_lineage_equal"]
    assert audit["holdout_data_accessed"] is audit["raw_sources_accessed"] is False
    assert set(opened) == {entry["path"] for entry in bundle[1]["files"]}


@pytest.mark.parametrize("name,mutate,error", [
    ("training_config.json", lambda value: value["gbt_parameters"].update(maxIter=21), "configuration"),
    ("training_config.json", lambda value: value["gbt_parameters"].update(lossType="absolute"), "configuration"),
    ("training_config.json", lambda value: value["baseline_policy"].update(hour_bin_width=3), "configuration"),
    ("training_config.json", lambda value: value["features"].append("fare_amount"), "configuration"),
    ("training_config.json", lambda value: value.update(holdout_opened=True), "configuration"),
    ("sample_lineage.json", lambda value: value.update(sample_identity_sha256="0" * 64), "sample_identity"),
    ("sample_lineage.json", lambda value: value["sample_counts"].update(train=3), "sample_identity"),
    ("sample_lineage.json", lambda value: value.update(journey_overlap_train_validation=1), "sample_identity"),
    ("sample_lineage.json", lambda value: value.update(holdout_data_accessed=True), "sample_identity"),
    ("baselines.json", lambda value: value.update(minimum_group_count=4), "baseline_policy"),
    ("metrics.json", lambda value: value["validation"]["gbt"].update(count=2), "metric_count"),
    ("metrics.json", lambda value: value["validation"]["hierarchical_median"].update(mae_usd=5.0), "validation_gate"),
    ("training_config.json", lambda value: value["gate"].update({"pass": True, "required_mae_improvement_percent": 5.0}), "validation_gate"),
])
def test_reference_drift_rejected_before_new_fit(bundle, name, mutate, error):
    _mutate_json(bundle, name, mutate)
    with pytest.raises(ValueError, match=error):
        guard.read_reference(*bundle)


@pytest.mark.parametrize("field,value", [("model_tree_sha256", "0" * 64),
                                          ("promoted", True), ("deployed", True),
                                          ("holdout_opened", True)])
def test_reference_must_match_locked_squared_model(bundle, field, value):
    bundle[1][field] = value
    with pytest.raises(ValueError, match="manifest_identity"):
        guard.read_reference(*bundle)


@pytest.mark.parametrize("name", ["../outside.json", "/outside.json", "C:/outside.json",
                                  "pipeline/../outside.json", "pipeline\\outside.json", "./outside.json"])
def test_reference_manifest_path_escape_rejected_without_read(bundle, name):
    bundle[1]["files"].append({"path": name, "bytes": 0, "sha256": "0" * 64})
    with pytest.raises(ValueError, match="manifest_path"):
        guard.read_reference(*bundle)


def test_reference_duplicate_manifest_entry_rejected(bundle):
    bundle[1]["files"].append(dict(bundle[1]["files"][0]))
    with pytest.raises(ValueError, match="manifest_path"):
        guard.read_reference(*bundle)


def test_reference_hash_failure_rejected(bundle):
    (bundle[0] / "metrics.json").write_text(" " * (bundle[0] / "metrics.json").stat().st_size)
    with pytest.raises(ValueError, match="hash_mismatch"):
        guard.read_reference(*bundle)


def test_reference_metadata_member_size_limit_is_enforced_before_open(bundle):
    entry = next(value for value in bundle[1]["files"] if value["path"] == "training_config.json")
    entry["bytes"] = guard.MAX_REFERENCE_MEMBER_BYTES + 1
    with pytest.raises(ValueError, match="member_limit"):
        guard.read_reference(*bundle)


def test_reference_required_metadata_not_found_in_manifest_rejected(bundle):
    bundle[1]["files"] = [entry for entry in bundle[1]["files"] if entry["path"] != "sample_lineage.json"]
    with pytest.raises(ValueError, match="required_member"):
        guard.read_reference(*bundle)


def test_gbt_effective_squared_is_parammap_not_library_default(bundle):
    name = "pipeline/stages/4_GBTRegressionModel_fixture/metadata/part-00000.txt"
    _mutate_json(bundle, name, lambda value: value["defaultParamMap"].update(lossType="absolute"))
    assert guard.read_reference(*bundle)["gbt_parameters"]["lossType"] == "squared"
    _mutate_json(bundle, name, lambda value: value["paramMap"].update(lossType="absolute"))
    with pytest.raises(ValueError, match="effective_loss"):
        guard.read_reference(*bundle)


@pytest.mark.parametrize("labels", [["2", "1"], ["1", "1"], ["01"], ["264"], [], [1]])
def test_reference_indexer_dictionary_must_be_valid_alphabetical_zones(bundle, labels):
    name = "pipeline/stages/0_StringIndexer_fixture/data/part-00000.parquet"
    # Incompatible numeric value intentionally uses an integer Arrow schema.
    pq.write_table(pa.table({"labelsArray": [[labels]]}), bundle[0] / name)
    _refresh_entry(bundle, name)
    with pytest.raises(ValueError, match="indexer_dictionary"):
        guard.read_reference(*bundle)


def test_reference_indexer_requires_one_data_member_per_stage(bundle):
    entry = next(value for value in bundle[1]["files"] if "/0_StringIndexer_" in value["path"])
    bundle[1]["files"].append({**entry, "path": entry["path"].replace("00000", "00001")})
    with pytest.raises(ValueError, match="stage_member_not_unique"):
        guard.read_reference(*bundle)


@pytest.mark.parametrize("field,mutate", [
    ("calendar_sha256", lambda _: "0" * 64),
    ("feature_contract_sha256", lambda _: "0" * 64),
    ("sample_hashes", lambda value: {**value, "train": {"features_sha256": "0" * 64}}),
    ("timezone_identity", lambda value: {**value, "tzif_sha256": "0" * 64}),
])
def test_current_full_lineage_cannot_drift(bundle, field, mutate):
    reference = guard.read_reference(*bundle)
    loaded = _loaded(reference)
    loaded["lineage"][field] = mutate(loaded["lineage"][field])
    with pytest.raises(ValueError, match="sample_or_lineage"):
        guard.verify_current(reference, loaded, reference["baselines"])


def test_current_recomputed_baseline_must_match_artifact_exactly(bundle):
    reference = guard.read_reference(*bundle)
    baseline = deepcopy(reference["baselines"])
    baseline["global_median"] += 0.001
    with pytest.raises(ValueError, match="train_baseline"):
        guard.verify_current(reference, _loaded(reference), baseline)


def test_current_train_count_and_dictionaries_do_not_use_validation(bundle):
    reference = guard.read_reference(*bundle)
    loaded = _loaded(reference)
    assert guard.verify_current(reference, loaded, reference["baselines"])["train_dictionary"]["matches_locked_v2"]
    loaded["train"][0]["pickup_zone"] = "263"
    with pytest.raises(ValueError, match="categorical_dictionary"):
        guard.verify_current(reference, loaded, reference["baselines"])
    loaded = _loaded(reference)
    loaded["train"].append(dict(loaded["train"][0]))
    with pytest.raises(ValueError, match="sample_or_lineage"):
        guard.verify_current(reference, loaded, reference["baselines"])


def test_fitted_dictionary_guard_matches_order_unknown_bucket_and_exact_features(bundle):
    reference = guard.read_reference(*bundle)
    assert guard.verify_dictionaries(reference, reference["dictionaries"])["matches_locked_v2"]
    for changed in ({"pickup_zone": ["1", "2"], "dropoff_zone": ["3", "2"]},
                    {"pickup_zone": ["1", "2", "263"], "dropoff_zone": ["2", "3"]},
                    {"pickup_zone": ["1", "2"]}):
        with pytest.raises(ValueError, match="categorical_dictionary"):
            guard.verify_dictionaries(reference, changed)
