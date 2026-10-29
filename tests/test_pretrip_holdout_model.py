"""Synthetic frozen-candidate audits and offline holdout metrics; no real data."""

from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nyc_taxi.ml import pretrip_holdout_model as holdout
from nyc_taxi.ml import pretrip_training_model as model_api
from nyc_taxi.ml.pretrip_features import CATEGORICAL_FEATURE_COLUMNS, FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS


def _hash(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write(root, name, value):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _repin(root, monkeypatch):
    """Only synthetic fixtures may replace independent pins after a mutation."""
    files = [{"path": path.relative_to(root).as_posix(), "bytes": path.stat().st_size,
              "sha256": _hash(path)} for path in sorted(root.rglob("*"))
             if path.is_file() and path.name != "artifact_manifest.json"]
    tree = holdout._digest([item for item in files if item["path"].startswith("pipeline/")])
    manifest = {"version": "pretrip-v2-training-artifacts-1", "files": files,
                "model_tree_sha256": tree, "holdout_opened": False, "promoted": False, "deployed": False}
    _write(root, "artifact_manifest.json", manifest)
    monkeypatch.setattr(holdout, "MODEL_TREE_SHA256", tree)
    monkeypatch.setattr(holdout, "ARTIFACT_MANIFEST_SHA256", _hash(root / "artifact_manifest.json"))
    monkeypatch.setattr(holdout, "PINNED_MEMBERS", {name: _hash(root / name) for name in holdout.PINNED_MEMBERS})
    gbt = root / "pipeline/stages/3_gbt_fixture/metadata/part-00000.txt"
    monkeypatch.setattr(holdout, "GBT_METADATA_SHA256", _hash(gbt))


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    root = tmp_path / "candidate"
    root.mkdir()
    dictionaries = {"pickup_zone": sorted(str(index) for index in range(1, 118)),
                    "dropoff_zone": sorted(str(index) for index in range(1, 225))}
    monkeypatch.setattr(holdout, "DICTIONARY_SHA256", holdout._digest(dictionaries))
    # Contract/calendar hashes are fixture-local. Their runtime files are not
    # used to prepare, infer, train or evaluate any real row.
    _write(root, "pretrip_v2_calendar.json", {"fixture": True})
    calendar_hash = _hash(root / "pretrip_v2_calendar.json")
    _write(root, "pretrip_v2_contract.json", {"timezone": "America/New_York",
           "numeric_features": list(NUMERIC_FEATURE_COLUMNS),
           "categorical_features": list(CATEGORICAL_FEATURE_COLUMNS), "calendar": {"sha256": calendar_hash}})
    _write(root, "sample_lineage.json", {"sample_identity_sha256": holdout.SAMPLE_IDENTITY_SHA256,
           "sample_counts": dict(holdout.EXPECTED_TRAIN_COUNTS), "feature_columns": list(FEATURE_COLUMNS),
           "journey_overlap_train_validation": 0, "holdout_data_accessed": False,
           "raw_sources_accessed": False, "calendar_sha256": calendar_hash,
           "feature_contract_sha256": _hash(root / "pretrip_v2_contract.json"),
           "timezone_identity": {"timezone": "America/New_York", "tzif_sha256": holdout.TZIF_SHA256}})
    _write(root, "training_config.json", {"encoding_profile": "native", "loss_profile": "squared",
           "gbt_parameters": model_api.gbt_parameters(encoding_profile="native"),
           "features": list(FEATURE_COLUMNS), "baseline_policy": dict(model_api.BASELINE_POLICY),
           "holdout_opened": False, "sample_identity_sha256": holdout.SAMPLE_IDENTITY_SHA256,
           "experiment_changes": ["encoding_one_hot_to_native", "maxBins_32_to_256"]})
    _write(root, "baselines.json", {**model_api.BASELINE_POLICY, "train_count": 26573,
           "global_median": 12.8, "pair_groups": [{}] * 1402, "time_groups": [{}] * 1601})
    _write(root, "metrics.json", {"fixture": "previously stored validation aggregates"})
    for index, (name, labels) in enumerate(dictionaries.items()):
        folder = f"pipeline/stages/{index}_indexer{index}_fixture"
        path = root / folder / "data/part-00000.parquet"
        path.parent.mkdir(parents=True)
        pq.write_table(pa.table({"labelsArray": pa.array([[labels]], type=pa.list_(pa.list_(pa.string())))}), path)
        _write(root, folder + "/metadata/part-00000.txt", {"class": "org.apache.spark.ml.feature.StringIndexerModel",
               "uid": f"indexer{index}_fixture", "paramMap": {"handleInvalid": "keep", "inputCol": name,
               "outputCol": model_api.INDEX_COLUMNS[index], "stringOrderType": "alphabetAsc"}})
    _write(root, "pipeline/stages/2_assembler_fixture/metadata/part-00000.txt",
           {"class": "org.apache.spark.ml.feature.VectorAssembler", "uid": "assembler_fixture",
            "paramMap": {"handleInvalid": "error", "inputCols": [*NUMERIC_FEATURE_COLUMNS, *model_api.INDEX_COLUMNS],
                         "outputCol": "features"}})
    _write(root, "pipeline/stages/3_gbt_fixture/metadata/part-00000.txt",
           {"class": "org.apache.spark.ml.regression.GBTRegressionModel", "uid": "gbt_fixture",
            "paramMap": model_api.gbt_parameters(encoding_profile="native"), "numFeatures": 8, "numTrees": 20,
            "defaultParamMap": {"maxBins": 32, "lossType": "squared"}})
    _write(root, "pipeline/metadata/part-00000.txt", {"class": "org.apache.spark.ml.PipelineModel",
           "paramMap": {"stageUids": ["indexer0_fixture", "indexer1_fixture", "assembler_fixture", "gbt_fixture"]}})
    _repin(root, monkeypatch)
    return root, dictionaries


def _mutate(root, name, change):
    path = root / name
    value = json.loads(path.read_text())
    change(value)
    _write(root, name, value)


def test_complete_candidate_audit_pins_independently_before_spark_load(candidate, monkeypatch):
    root, dictionaries = candidate
    def forbidden(*args, **kwargs):
        raise AssertionError("No Spark or holdout may be opened during audit")
    monkeypatch.setattr(model_api, "load_pipeline", forbidden)
    audit = holdout.audit_candidate(root)
    assert audit["dictionaries"] == dictionaries
    assert audit["metadata"]["stage_count"] == 4
    assert audit["identities"]["category_counts_with_unknown"] == {"pickup_zone": 118, "dropoff_zone": 225}
    assert audit["identities"]["vector_dimensions"] == 8
    assert audit["identities"]["metadata_sha256"] == holdout.PINNED_MEMBERS
    assert audit["lineage"]["holdout_data_accessed"] is False


def test_manifest_tamper_rejected_even_if_claimed_model_identity_is_unchanged(candidate):
    root, _ = candidate
    _mutate(root, "artifact_manifest.json", lambda value: value.update(promoted=True))
    with pytest.raises(ValueError, match="manifest_identity"):
        holdout.audit_candidate(root)


def test_member_tamper_rejected_without_trusting_refreshed_local_checksum(candidate):
    root, _ = candidate
    _mutate(root, "baselines.json", lambda value: value.update(global_median=99))
    with pytest.raises(ValueError, match="member_(hash|size)"):
        holdout.audit_candidate(root)


@pytest.mark.parametrize("name", ["../outside", "/outside", "C:/outside", "pipeline/../outside", "a\\b", "a//b", "a/./b"])
def test_manifest_path_escape_or_noncanonical_path_is_rejected(candidate, monkeypatch, name):
    root, _ = candidate
    manifest = json.loads((root / "artifact_manifest.json").read_text())
    manifest["files"].append({"path": name, "bytes": 0, "sha256": "0" * 64})
    _write(root, "artifact_manifest.json", manifest)
    monkeypatch.setattr(holdout, "ARTIFACT_MANIFEST_SHA256", _hash(root / "artifact_manifest.json"))
    with pytest.raises(ValueError, match="path_invalid"):
        holdout.audit_candidate(root)


def test_unlisted_pipeline_file_rejected_before_spark_discovery(candidate):
    root, _ = candidate
    (root / "pipeline/extra.json").write_text("{}")
    with pytest.raises(ValueError, match="unlisted_pipeline"):
        holdout.audit_candidate(root)


def test_member_symlink_is_not_followed(candidate, monkeypatch):
    root, _ = candidate
    path = root / "pipeline/symlink.txt"
    try:
        path.symlink_to(root / "training_config.json")
    except OSError:
        import stat
        from types import SimpleNamespace
        path.write_text("{}")
        original_lstat = Path.lstat
        monkeypatch.setattr(
            Path,
            "lstat",
            lambda self: SimpleNamespace(st_mode=stat.S_IFLNK | 0o777, st_size=2, st_file_attributes=0)
            if self == path else original_lstat(self),
        )
    with pytest.raises(ValueError, match="link_rejected"):
        holdout.audit_candidate(root)


@pytest.mark.parametrize("name,change,error", [
    ("training_config.json", lambda value: value.update(encoding_profile="one_hot"), "configuration"),
    ("training_config.json", lambda value: value["gbt_parameters"].update(maxBins=512), "configuration"),
    ("training_config.json", lambda value: value["gbt_parameters"].update(lossType="absolute"), "configuration"),
    ("training_config.json", lambda value: value["features"].append("fare_amount"), "configuration"),
    ("sample_lineage.json", lambda value: value.update(sample_identity_sha256="0" * 64), "lineage"),
    ("sample_lineage.json", lambda value: value["sample_counts"].update(train=1), "lineage"),
    ("sample_lineage.json", lambda value: value["timezone_identity"].update(tzif_sha256="0" * 64), "lineage"),
    ("sample_lineage.json", lambda value: value.update(holdout_data_accessed=True), "lineage"),
    ("baselines.json", lambda value: value.update(global_median=2), "baseline"),
    ("pipeline/stages/2_assembler_fixture/metadata/part-00000.txt",
     lambda value: value["paramMap"]["inputCols"].append("fare_amount"), "vector_contract"),
    ("pipeline/stages/3_gbt_fixture/metadata/part-00000.txt",
     lambda value: value["paramMap"].update(maxDepth=8), "effective_parameters"),
    ("pipeline/stages/3_gbt_fixture/metadata/part-00000.txt",
     lambda value: value.update(numFeatures=9), "effective_parameters"),
    ("pipeline/metadata/part-00000.txt", lambda value: value["paramMap"].update(stageUids=[]), "stage_identity"),
])
def test_candidate_semantic_drift_is_rejected_even_in_rehashed_synthetic_bundle(candidate, monkeypatch, name, change, error):
    root, _ = candidate
    _mutate(root, name, change)
    _repin(root, monkeypatch)
    with pytest.raises(ValueError, match=error):
        holdout.audit_candidate(root)


def test_dictionary_unknown_bucket_cap_and_identity_are_verified(candidate, monkeypatch):
    root, _ = candidate
    path = root / "pipeline/stages/0_indexer0_fixture/data/part-00000.parquet"
    pq.write_table(pa.table({"labelsArray": [[["1", "2"]]]}), path)
    _repin(root, monkeypatch)
    with pytest.raises(ValueError, match="dictionary_drift"):
        holdout.audit_candidate(root)


def test_load_checks_saved_audit_again_before_pipeline_loading(candidate, monkeypatch):
    root, _ = candidate
    audit = holdout.audit_candidate(root)
    audit["config"]["gbt_parameters"]["maxDepth"] = 99
    monkeypatch.setattr(model_api, "load_pipeline", lambda *args: pytest.fail("Must reject before JVM loading"))
    with pytest.raises(ValueError, match="audit_changed_before_load"):
        holdout.load_candidate(root, audit)


def test_load_checks_actual_tzif_before_pipeline_loading(candidate, monkeypatch):
    root, _ = candidate
    monkeypatch.setattr(holdout, "timezone_identity", lambda: {"timezone": "America/New_York", "tzif_sha256": "0" * 64})
    monkeypatch.setattr(model_api, "load_pipeline", lambda *args: pytest.fail("Must reject before JVM loading"))
    with pytest.raises(ValueError, match="runtime_tzif_mismatch"):
        holdout.load_candidate(root)


def test_no_fit_guard_blocks_and_restores_every_owned_fit_entry_without_fitting(monkeypatch):
    # Lightweight CI deliberately has no Spark dependency. Test patch lifetime
    # with equivalent inherited classes; real Spark entrypoints have a separate
    # explicitly classified worker-only synthetic test.
    class Estimator:
        def fit(self, *args):
            pytest.fail("Synthetic estimator must not fit")
        def _fit(self, *args):
            pytest.fail("Synthetic estimator must not fit")
    class Pipeline(Estimator):
        def _fit(self, *args):
            pytest.fail("Synthetic pipeline must not fit")
    class JavaEstimator(Estimator):
        def _fit(self, *args):
            pytest.fail("Synthetic Java estimator must not fit")
    for name, attributes in (("pyspark", {}), ("pyspark.ml", {"Pipeline": Pipeline}),
                             ("pyspark.ml.base", {"Estimator": Estimator}),
                             ("pyspark.ml.wrapper", {"JavaEstimator": JavaEstimator})):
        module = ModuleType(name)
        module.__dict__.update(attributes)
        monkeypatch.setitem(sys.modules, name, module)
    old = model_api.fit_baselines
    with holdout.no_fit_guard() as state:
        with holdout.no_fit_guard() as nested:
            assert nested is state
        calls = [lambda: Estimator.fit(None, None), lambda: Pipeline.fit(None, None),
                 lambda: Pipeline._fit(None, None), lambda: JavaEstimator._fit(None, None),
                 lambda: model_api.fit(None), lambda: model_api.fit_baselines([])]
        for call in calls:
            with pytest.raises(RuntimeError, match="fit_or_refit_forbidden"):
                call()
        assert state == {"fit_calls": len(calls), "blocked_fit_calls": len(calls)}
    assert model_api.fit_baselines is old


def test_nominal_metadata_checks_dictionary_values_not_only_number_of_categories():
    dictionaries = {"pickup_zone": ["1", "2"], "dropoff_zone": ["2", "3"]}
    nominal = [{"idx": 6 + index, "name": name, "vals": [*values, "__unknown"]}
               for index, (name, values) in enumerate(zip(model_api.INDEX_COLUMNS, dictionaries.values(), strict=True))]
    metadata = {"ml_attr": {"num_attrs": 8, "attrs": {"numeric": [
        {"idx": index, "name": name} for index, name in enumerate(NUMERIC_FEATURE_COLUMNS)], "nominal": nominal}}}
    frame = SimpleNamespace(columns=["features"], schema={"features": SimpleNamespace(metadata=metadata)})
    assert holdout.validate_nominal_metadata(frame, {"dictionaries": dictionaries})["vector_dimensions"] == 8
    nominal[0]["vals"] = ["1", "3", "__unknown"]
    with pytest.raises(ValueError, match="nominal_dictionary_drift"):
        holdout.validate_nominal_metadata(frame, {"dictionaries": dictionaries})


def rows_and_baseline():
    rows = []
    for index, fare in enumerate((5., 15., 35., 75., 150.)):
        hour = (index * 6) % 24
        rows.append({"pickup_hour_sin": math.sin(2 * math.pi * hour / 24),
                     "pickup_hour_cos": math.cos(2 * math.pi * hour / 24),
                     "pickup_dow_sin": 0., "pickup_dow_cos": 1., "is_weekend": 0., "is_holiday": 0.,
                     "pickup_zone": "1" if index != 4 else "3", "dropoff_zone": "2",
                     "fare_amount": fare})
    baseline = {**model_api.BASELINE_POLICY, "train_count": 100, "global_median": 12.8,
                "pair_groups": [{"pickup_zone": "1", "dropoff_zone": "2", "count": 5, "median": 20.}],
                "time_groups": [{"pickup_zone": "1", "dropoff_zone": "2", "hour_bin": 0, "count": 5, "median": 7.}]}
    dictionaries = {"pickup_zone": ["1"], "dropoff_zone": ["2"]}
    return rows, baseline, dictionaries


def test_metrics_gate_and_all_predeclared_slices_use_same_complete_rows_without_fitting(monkeypatch):
    rows, baseline, dictionaries = rows_and_baseline()
    monkeypatch.setattr(model_api, "fit_baselines", lambda *args: pytest.fail("No new median fitting"))
    result = holdout.evaluate_predictions(rows, [row["fare_amount"] for row in rows], baseline,
                                          dictionaries=dictionaries, expected_count=5)
    assert result["gate"]["pass"]
    assert result["gate"]["july_numeric_threshold_applied"] is False
    assert result["gate"]["clipped_rows"] == result["gate"]["dropped_rows"] == result["fit_calls"] == 0
    assert result["metrics"]["native_gbt"]["mae_usd"] == 0
    assert result["metrics"]["native_gbt"]["r2"] == 1
    assert result["unseen"]["pickup_zone"]["count"] == result["unseen"]["either"]["count"] == 1
    assert result["unseen"]["dropoff_zone"]["count"] == 0
    assert {name: value["count"] for name, value in result["slices"]["fare_bands_usd"].items()} == {
        "(0,10]": 1, "(10,25]": 1, "(25,50]": 1, "(50,100]": 1, ">100": 1}
    for family in result["slices"].values():
        assert sum(item["count"] for item in family.values()) == len(rows)
        assert all(item["low_sample"] for item in family.values())


def test_august_gate_has_no_july_absolute_threshold_and_strictly_beats_best_baseline():
    rows, baseline, dictionaries = rows_and_baseline()
    # MAE6 is above July4.618, but improves both baselines on this different
    # synthetic August set, and must pass its independently predeclared gate.
    predictions = [row["fare_amount"] + 6 for row in rows]
    result = holdout.evaluate_predictions(rows, predictions, baseline, dictionaries=dictionaries, expected_count=5)
    assert result["gate"]["native_mae_usd"] == 6
    assert result["gate"]["pass"]
    equal = holdout.evaluate_predictions(rows, [12.8] * 5, baseline,
                                         dictionaries=dictionaries, expected_count=5)
    assert not equal["gate"]["pass"]


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_prediction_is_reported_counted_and_fails_without_dropping(bad):
    rows, baseline, dictionaries = rows_and_baseline()
    result = holdout.evaluate_predictions(rows, [bad, 15, 35, 75, 150], baseline,
                                          dictionaries=dictionaries, expected_count=5)
    assert not result["gate"]["pass"]
    assert result["row_count"] == result["metrics"]["native_gbt"]["count"] == 5
    assert result["metrics"]["native_gbt"]["nonfinite_prediction_count"] == 1
    assert result["metrics"]["native_gbt"]["mae_usd"] is None
    assert result["gate"]["dropped_rows"] == 0


def test_negative_predictions_are_kept_and_no_count_mismatch_can_pass_gate():
    rows, baseline, dictionaries = rows_and_baseline()
    result = holdout.evaluate_predictions(rows, [-1, 15, 35, 75, 150], baseline,
                                          dictionaries=dictionaries, expected_count=21833)
    assert not result["gate"]["pass"]
    assert result["metrics"]["native_gbt"]["negative_prediction_count"] == 1
    assert result["metrics"]["native_gbt"]["mae_usd"] == 6 / 5
    assert result["metrics"]["native_gbt"]["p95_absolute_error_usd"] == pytest.approx(4.8)


def test_prediction_count_alignment_is_required_before_metrics():
    rows, baseline, dictionaries = rows_and_baseline()
    with pytest.raises(ValueError, match="alignment"):
        holdout.evaluate_predictions(rows, [1], baseline, dictionaries=dictionaries)


def test_baseline_fallback_is_loaded_train_policy_not_holdout_target():
    rows, baseline, dictionaries = rows_and_baseline()
    before = deepcopy(baseline)
    global_predictions, hierarchical = model_api.predict_baseline(rows, baseline)
    assert global_predictions == [12.8] * 5
    assert hierarchical == [7., 20., 20., 20., 12.8]
    holdout.evaluate_predictions(rows, [1] * 5, baseline, dictionaries=dictionaries, expected_count=5)
    assert baseline == before


@pytest.mark.parametrize("tree_count", [20, 19])
def test_loaded_candidate_uses_spark_get_num_trees_property_without_fit(monkeypatch, tree_count):
    from types import SimpleNamespace

    parameters = model_api.gbt_parameters(encoding_profile="native")

    class FrozenRegressor:
        # The real PySpark 4.2 ensemble API is getNumTrees, not numTrees.
        getNumTrees = tree_count

        def getOrDefault(self, name):
            return parameters[name]

    dictionaries = {"pickup_zone": ["1"], "dropoff_zone": ["2"]}
    frozen = SimpleNamespace(stages=[SimpleNamespace(labels=["1"]),
                                     SimpleNamespace(labels=["2"]), object(), FrozenRegressor()])
    audit = {"dictionaries": dictionaries, "config": {"gbt_parameters": parameters}}
    monkeypatch.setattr(model_api, "_assert_fitted_pipeline", lambda model: None)
    if tree_count == 20:
        result = holdout.validate_loaded_candidate(frozen, audit)
        assert result["fit_calls"] == 0 and result["vector_dimensions"] == 8
    else:
        with pytest.raises(ValueError, match="effective_parameters_drift"):
            holdout.validate_loaded_candidate(frozen, audit)
