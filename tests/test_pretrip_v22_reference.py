"""Bounded saved OHE comparison guards; no Spark, model or sample access."""

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


GBT_METADATA = "pipeline/stages/4_GBTRegressionModel_fixture/metadata/part-00000.txt"


def _refresh(bundle, name):
    root, manifest, _ = bundle
    path = root / name
    entry = {"path": name, "bytes": path.stat().st_size,
             "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    manifest["files"] = [item for item in manifest["files"] if item["path"] != name] + [entry]


def _write(bundle, name, value):
    path = bundle[0] / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    _refresh(bundle, name)


def _mutate(bundle, name, mutate):
    value = json.loads((bundle[0] / name).read_text(encoding="utf-8"))
    mutate(value)
    _write(bundle, name, value)


@pytest.fixture
def absolute_bundle(tmp_path, monkeypatch):
    # Only the row-count constant is reduced; actual pinned model/sample hashes
    # and the baseline/gate values remain unchanged in this synthetic bundle.
    monkeypatch.setattr(guard, "REFERENCE_COUNTS", {"train": 2, "validation": 1})
    root = tmp_path / "absolute"
    root.mkdir()
    lineage = {"sample_identity_sha256": guard.REFERENCE_SAMPLE_IDENTITY_SHA256,
               "sample_counts": dict(guard.REFERENCE_COUNTS),
               "feature_columns": list(FEATURE_COLUMNS),
               "sample_hashes": {"train": "a" * 64, "validation": "b" * 64},
               "calendar_sha256": "c" * 64, "feature_contract_sha256": "d" * 64,
               "timezone_identity": {"timezone": "America/New_York", "tzif_sha256": "e" * 64},
               "journey_overlap_train_validation": 0,
               "holdout_data_accessed": False, "raw_sources_accessed": False,
               "preparation_input_lineage": {"2026-08": {"sha256": "f" * 64}}}
    metrics = {split: {name: {"count": count, "mae_usd": mae,
                              "metrics_status": "finite_all_rows",
                              "nonfinite_prediction_count": 0}
                       for name, mae in (("global_median", 8.0),
                                         ("hierarchical_median", guard.REFERENCE_BASELINE_MAE_USD),
                                         ("gbt", 6.3))}
               for split, count in guard.REFERENCE_COUNTS.items()}
    baseline = {**BASELINE_POLICY, "train_count": 2, "global_median": 11.0,
                "pair_groups": [], "time_groups": []}
    runtime = {"spark": "4.2.0", "jvm_max_heap_bytes": 1536 * 1024 * 1024,
               "image_id": "sha256:" + "a" * 64}
    squared = {"model_tree_sha256": guard.REFERENCE_MODEL_TREE_SHA256,
               "sample_identity_sha256": guard.REFERENCE_SAMPLE_IDENTITY_SHA256,
               "lineage": deepcopy(lineage), "runtime": deepcopy(runtime),
               "baselines": deepcopy(baseline), "metrics": deepcopy(metrics),
               "dictionaries": {"pickup_zone": ["1", "2"], "dropoff_zone": ["2", "3"]}}
    manifest = {"version": "pretrip-v2-training-artifacts-1", "files": [],
                "model_tree_sha256": guard.ABSOLUTE_REFERENCE_MODEL_TREE_SHA256,
                "holdout_opened": False, "promoted": False, "deployed": False}
    bundle = root, manifest, squared
    config = {"loss_profile": "absolute", "gbt_parameters": {**GBT_PARAMETERS, "lossType": "absolute"},
              "baseline_policy": dict(BASELINE_POLICY), "features": list(FEATURE_COLUMNS),
              "holdout_opened": False, "sample_identity_sha256": guard.REFERENCE_SAMPLE_IDENTITY_SHA256,
              "runtime": runtime, "gate_policy": {
                  "reference_baseline_mae_usd": guard.REFERENCE_BASELINE_MAE_USD,
                  "reference_gbt_max_mae_usd": guard.REFERENCE_GATE_THRESHOLD_USD,
                  "required_mae_improvement_percent": 10.0}}
    for name, value in (("training_config.json", config), ("sample_lineage.json", lineage),
                        ("metrics.json", metrics), ("baselines.json", baseline)):
        _write(bundle, name, value)
    for stage, labels in enumerate(squared["dictionaries"].values()):
        name = f"pipeline/stages/{stage}_StringIndexer_fixture/data/part-00000.parquet"
        path = root / name
        path.parent.mkdir(parents=True)
        pq.write_table(pa.table({"labelsArray": pa.array([[labels]],
                       type=pa.list_(pa.list_(pa.string())))}), path)
        _refresh(bundle, name)
    _write(bundle, GBT_METADATA,
           {"class": "org.apache.spark.ml.regression.GBTRegressionModel",
            "paramMap": {**GBT_PARAMETERS, "lossType": "absolute"},
            "defaultParamMap": {"lossType": "squared"}})
    (root / "test_features.parquet").write_bytes(b"NEVER OPEN HOLDOUT")
    (root / "yellow_tripdata_2026-08.parquet").write_bytes(b"NEVER OPEN RAW")
    return bundle


def test_absolute_reference_reads_only_named_metadata_and_two_indexers(absolute_bundle, monkeypatch):
    root, manifest, squared = absolute_bundle
    opened = []
    original_open, original_parquet = Path.open, pq.ParquetFile

    def tracked_open(path, *args, **kwargs):
        assert "2026-08" not in path.name and not path.name.startswith("test_")
        opened.append(path.relative_to(root).as_posix())
        return original_open(path, *args, **kwargs)

    def exact_parquet(path, *args, **kwargs):
        assert Path(path).as_posix().endswith("/data/part-00000.parquet")
        opened.append(Path(path).relative_to(root).as_posix())
        return original_parquet(path, *args, **kwargs)

    def no_discovery(*args, **kwargs):
        raise AssertionError("reference must not discover sample/source directories")

    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr(Path, "glob", no_discovery)
    monkeypatch.setattr(Path, "rglob", no_discovery)
    monkeypatch.setattr(pq, "ParquetFile", exact_parquet)
    audit = guard.read_absolute_reference(root, manifest, squared)
    assert audit["model_tree_sha256"] == guard.ABSOLUTE_REFERENCE_MODEL_TREE_SHA256
    assert audit["squared_reference_model_tree_sha256"] == guard.REFERENCE_MODEL_TREE_SHA256
    assert audit["sample_hashes_and_full_lineage_equal"]
    assert audit["baseline_artifact_equal"] and audit["baseline_metrics_equal"]
    assert audit["runtime_equal"] and audit["category_counts"] == {"pickup_zone": 2, "dropoff_zone": 2}
    assert audit["gbt_parameters"] == {**GBT_PARAMETERS, "lossType": "absolute"}
    assert audit["metrics"] == squared["metrics"]
    assert audit["holdout_data_accessed"] is audit["raw_sources_accessed"] is False
    assert set(opened) == {entry["path"] for entry in manifest["files"]}


@pytest.mark.parametrize("name,mutate,error", [
    ("training_config.json", lambda v: v.update(loss_profile="native"), "configuration"),
    ("training_config.json", lambda v: v["gbt_parameters"].update(lossType="squared"), "configuration"),
    ("training_config.json", lambda v: v["gbt_parameters"].update(maxBins=256), "configuration"),
    ("training_config.json", lambda v: v["features"].append("fare_amount"), "configuration"),
    ("training_config.json", lambda v: v["baseline_policy"].update(minimum_group_count=4), "configuration"),
    ("training_config.json", lambda v: v.update(holdout_opened=True), "configuration"),
    ("training_config.json", lambda v: v.update(sample_identity_sha256="0" * 64), "sample_or_lineage"),
    ("sample_lineage.json", lambda v: v["sample_counts"].update(train=3), "sample_or_lineage"),
    ("sample_lineage.json", lambda v: v.update(calendar_sha256="0" * 64), "sample_or_lineage"),
    ("sample_lineage.json", lambda v: v["timezone_identity"].update(tzif_sha256="0" * 64), "sample_or_lineage"),
    ("sample_lineage.json", lambda v: v["sample_hashes"].update(train="0" * 64), "sample_or_lineage"),
    ("training_config.json", lambda v: v["runtime"].update(spark="different"), "runtime"),
    ("baselines.json", lambda v: v.update(global_median=12.0), "baseline_drift"),
    ("training_config.json", lambda v: v["gate_policy"].update(required_mae_improvement_percent=5.0), "gate"),
    ("metrics.json", lambda v: v["validation"]["gbt"].update(count=2), "metric_count"),
    ("metrics.json", lambda v: v["train"]["global_median"].update(mae_usd=7.9), "baseline_metrics"),
    ("metrics.json", lambda v: v["validation"]["hierarchical_median"].update(mae_usd=5.0), "baseline_metrics"),
    (GBT_METADATA, lambda v: v["paramMap"].update(lossType="squared"), "effective_parameters"),
    (GBT_METADATA, lambda v: v["paramMap"].update(maxBins=256), "effective_parameters"),
    (GBT_METADATA, lambda v: v.update({"class": "other.Model"}), "effective_parameters"),
])
def test_absolute_comparison_rejects_drift(absolute_bundle, name, mutate, error):
    _mutate(absolute_bundle, name, mutate)
    with pytest.raises(ValueError, match=error):
        guard.read_absolute_reference(*absolute_bundle)


def test_absolute_effective_loss_not_library_default(absolute_bundle):
    assert guard.read_absolute_reference(*absolute_bundle)["gbt_parameters"]["lossType"] == "absolute"
    _mutate(absolute_bundle, GBT_METADATA, lambda v: v["defaultParamMap"].update(lossType="absolute"))
    assert guard.read_absolute_reference(*absolute_bundle)["gbt_parameters"]["lossType"] == "absolute"


def test_absolute_comparison_dictionary_order_must_equal_train_squared_dictionary(absolute_bundle):
    name = "pipeline/stages/0_StringIndexer_fixture/data/part-00000.parquet"
    pq.write_table(pa.table({"labelsArray": [[["1", "3"]]]}), absolute_bundle[0] / name)
    _refresh(absolute_bundle, name)
    with pytest.raises(ValueError, match="categorical_dictionary"):
        guard.read_absolute_reference(*absolute_bundle)


def test_absolute_comparison_rejects_unpinned_squared_reference(absolute_bundle):
    absolute_bundle[2]["model_tree_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="squared_reference_identity"):
        guard.read_absolute_reference(*absolute_bundle)


def test_absolute_tree_pin_does_not_change_legacy_squared_default(absolute_bundle):
    manifest = absolute_bundle[1]
    with pytest.raises(ValueError, match="manifest_identity"):
        guard._members(manifest)
    squared_manifest = {**manifest, "model_tree_sha256": guard.REFERENCE_MODEL_TREE_SHA256}
    assert guard._members(squared_manifest)
    with pytest.raises(ValueError, match="manifest_identity"):
        guard.read_absolute_reference(absolute_bundle[0], squared_manifest, absolute_bundle[2])


def test_absolute_comparison_hash_guard_precedes_read(absolute_bundle):
    path = absolute_bundle[0] / "metrics.json"
    path.write_bytes(b" " * path.stat().st_size)
    with pytest.raises(ValueError, match="hash_mismatch"):
        guard.read_absolute_reference(*absolute_bundle)
