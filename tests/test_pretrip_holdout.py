"""Offline evaluator CLI/checkpoint guards; no real samples, Spark or models."""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock

import pytest

from nyc_taxi.ml import pretrip_holdout as evaluator
from nyc_taxi.ml.pretrip_budget import ManagedFileBudget


def arguments(*values):
    return evaluator.parser().parse_args(list(values))


def test_default_is_plan_and_cannot_change_model_features_profile_or_gate():
    defaults = arguments()
    assert defaults.execute is False
    assert isinstance(defaults.sample_dir, Path)
    assert isinstance(defaults.candidate_artifacts, Path)
    assert isinstance(defaults.output_dir, Path)
    for option in ("--loss-profile", "--encoding-profile", "--max-bins", "--seed",
                   "--features", "--gate-threshold", "--training", "--retry"):
        with pytest.raises(SystemExit) as caught:
            arguments(option, "unapproved")
        assert caught.value.code == 2


def test_plan_with_nonexistent_paths_does_not_read_metadata_holdout_or_load_model(tmp_path, monkeypatch):
    forbidden = Mock(side_effect=AssertionError("PLAN must not open data, artifacts, Spark or output"))
    monkeypatch.setattr(evaluator.model_api, "audit_candidate", forbidden)
    monkeypatch.setattr(evaluator.model_api, "load_candidate", forbidden)
    monkeypatch.setattr(evaluator.data_api, "verify_holdout_metadata", forbidden)
    monkeypatch.setattr(evaluator.data_api, "load_holdout_samples", forbidden)
    monkeypatch.setattr(evaluator, "create_spark", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(Path, "mkdir", forbidden)
    result = evaluator.run(arguments("--sample-dir", "/absent/holdout",
                                     "--candidate-artifacts", "/absent/candidate"), project_root=tmp_path)
    assert result["status"] == "PLAN_NOT_EXECUTED"
    assert defaults_no_execute(result)
    forbidden.assert_not_called()


def defaults_no_execute(result):
    # Status is the public guard; optional descriptive fields must agree.
    return result.get("default_execute", False) is False and result.get("fit_calls", 0) == 0


@pytest.mark.parametrize("relative", [".sandbox/pretrip-v2/existing-attempt", ".sandbox/pretrip-v2"])
def test_existing_output_is_rejected_before_loading_candidate_or_holdout(tmp_path, monkeypatch, relative):
    target = tmp_path / relative
    target.mkdir(parents=True)
    sentinel = target / "unchanged.json"
    sentinel.write_text('{"preserve": true}\n', encoding="utf-8")
    audit = Mock(side_effect=AssertionError("No overwrite must reject before opening candidate"))
    load = Mock(side_effect=AssertionError("No overwrite must reject before opening holdout"))
    monkeypatch.setattr(evaluator.model_api, "audit_candidate", audit)
    monkeypatch.setattr(evaluator.data_api, "load_holdout_samples", load)
    with pytest.raises((ValueError, FileExistsError)):
        evaluator.run(arguments("--execute", "--output-dir", relative, "--prior-task-bytes", "0"),
                      project_root=tmp_path)
    assert sentinel.read_text(encoding="utf-8") == '{"preserve": true}\n'
    assert list(target.iterdir()) == [sentinel]
    audit.assert_not_called()
    load.assert_not_called()


@pytest.mark.parametrize("relative", ["not-private/evaluation", ".sandbox/not-pretrip/evaluation",
                                      ".sandbox/pretrip-v2/../outside"])
def test_output_must_stay_in_private_v2_runtime(tmp_path, monkeypatch, relative):
    audit = Mock(side_effect=AssertionError("invalid output must fail before artifact I/O"))
    monkeypatch.setattr(evaluator.model_api, "audit_candidate", audit)
    with pytest.raises(ValueError):
        evaluator.run(arguments("--execute", "--output-dir", relative, "--prior-task-bytes", "0"),
                      project_root=tmp_path)
    audit.assert_not_called()


def test_evaluation_config_is_frozen_once_with_exact_identity_gate_and_lineage(tmp_path):
    root = tmp_path / "attempt"
    root.mkdir()
    budget = ManagedFileBudget([], root, 128 * 1024)
    config = {
        "candidate": {"model_tree_sha256": "1" * 64, "artifact_manifest_sha256": "2" * 64},
        "lineage": {"test_features_sha256": "3" * 64, "test_labels_sha256": "4" * 64},
        "expected_count": 21833,
        "gate": {"all_rows": True, "finite_predictions": True, "native_mae_less_than_best_baseline": True},
        "fit_calls": 0,
    }
    before = json.dumps(config, sort_keys=True)
    checksum = evaluator.freeze_evaluation_config(budget, root, config)
    path = root / "evaluation_config.json"
    original = path.read_bytes()
    assert checksum == hashlib.sha256(original).hexdigest()
    assert json.loads(original) == config
    assert json.dumps(config, sort_keys=True) == before
    with pytest.raises(FileExistsError):
        evaluator.freeze_evaluation_config(budget, root, {"tampered": True})
    assert path.read_bytes() == original
    assert list(root.iterdir()) == [path]


@pytest.mark.parametrize("value", [-1, 2048 * 1024 * 1024])
def test_invalid_or_exhausted_prior_envelope_stops_before_candidate_io(tmp_path, monkeypatch, value):
    audit = Mock(side_effect=AssertionError("invalid quota must reject before candidate I/O"))
    monkeypatch.setattr(evaluator.model_api, "audit_candidate", audit)
    with pytest.raises(ValueError):
        evaluator.run(arguments("--execute", "--prior-task-bytes", str(value)), project_root=tmp_path)
    audit.assert_not_called()


def test_alignment_restores_arrow_order_without_changing_negative_or_nonfinite_prediction():
    rows = [{"journey_key": "a"}, {"journey_key": "b"}]
    assert evaluator._align_predictions(rows, [("b", -1.0), ("a", 12.5)]) == [12.5, -1.0]
    found = evaluator._align_predictions(rows, [("b", float("inf")), ("a", 12.5)])
    assert found == [12.5, float("inf")]


@pytest.mark.parametrize("captured", [[], [("a", 1.0)], [("a", 1.0), ("a", 2.0)],
                                       [("a", 1.0), ("other", 2.0)],
                                       [("a", 1.0), ("b", 2.0), ("other", 3.0)]])
def test_prediction_alignment_rejects_missing_duplicate_unknown_or_extra_keys(captured):
    with pytest.raises(ValueError, match="alignment"):
        evaluator._align_predictions([{"journey_key": "a"}, {"journey_key": "b"}], captured)


def test_one_transform_and_collect_retains_unclipped_values_and_nominal_metadata(monkeypatch):
    rows = [{"journey_key": "a"}, {"journey_key": "b"}]
    audit = {"dictionaries": {"pickup_zone": ["1"], "dropoff_zone": ["2"]}}
    frame = Mock()
    transformed = Mock()
    transformed.select.return_value = transformed
    transformed.collect.return_value = [{"journey_key": "b", "prediction": -1.0},
                                        {"journey_key": "a", "prediction": 12.5}]
    model = Mock()
    model.transform.return_value = transformed
    spark = Mock()
    maker = Mock(return_value=frame)
    metadata = {"vector_dimensions": 8, "categorical_nominal_dimensions": {"pickup_zone": 2, "dropoff_zone": 2}}
    check = Mock(return_value=metadata)
    monkeypatch.setattr(evaluator, "_holdout_frame", maker)
    monkeypatch.setattr(evaluator.model_api, "validate_nominal_metadata", check)
    captured, vector = evaluator._collect_predictions_once(model, spark, rows, audit)
    assert captured == [("b", -1.0), ("a", 12.5)]
    assert vector == metadata
    maker.assert_called_once_with(spark, rows)
    model.transform.assert_called_once_with(frame)
    check.assert_called_once_with(transformed, audit)
    transformed.collect.assert_called_once_with()


@pytest.fixture
def execution(tmp_path, monkeypatch):
    output = tmp_path / ".sandbox/pretrip-v2/holdout-synthetic-01"
    owned_scratch = tmp_path / "owned-scratch"
    owned_scratch.mkdir()
    baseline = {**evaluator.training_model.BASELINE_POLICY, "train_count": 10,
                "global_median": 15.0, "pair_groups": [], "time_groups": []}
    audit = {"lineage": {"preparation_report_sha256": "a" * 64},
             "identities": {"model_tree_sha256": "b" * 64},
             "config": {"gbt_parameters": {"lossType": "squared", "maxBins": 256},
                        "baseline_policy": dict(evaluator.training_model.BASELINE_POLICY)},
             "baselines": baseline, "dictionaries": {"pickup_zone": ["1"], "dropoff_zone": ["2"]}}
    features = dict(zip(evaluator.NUMERIC_FEATURE_COLUMNS, (0.0, 1.0, 0.0, 1.0, 0.0, 0.0), strict=True))
    rows = [{**features, "journey_key": key * 64, "pickup_zone": "1", "dropoff_zone": "2",
             "pickup_datetime_local": "2026-08-01T00:00:00", "fare_amount": fare}
            for key, fare in (("a", 10.0), ("b", 20.0))]
    spark = Mock(name="never a real Spark session")
    model = Mock(name="never a real candidate")
    events = []
    real_sha256 = evaluator.sha256

    def synthetic_sha256(path):
        return real_sha256(path) if Path(path).is_file() else "c" * 64

    @contextmanager
    def guard():
        state = {"fit_calls": 0, "blocked_fit_calls": 0}
        yield state

    def load_rows(*args, **kwargs):
        assert (output / "evaluation_config.json").is_file()
        assert kwargs["verified_metadata"] == {"preparation_identity": "locked"}
        config = json.loads((output / "evaluation_config.json").read_text())
        assert config["candidate_identities"] == audit["identities"]
        assert config["fit_calls_permitted"] == 0
        assert config["model_prediction_collections_permitted"] == 1
        events.append("holdout_read_after_freeze")
        return {"test": rows, "lineage": {"sample_rows": 2, "overlap_scope": "retained sample only"}}

    def collect_once(*args):
        assert json.loads((output / "evaluation_checkpoint.json").read_text())["prediction_calls"] == 1
        events.append("predict_once")
        # Deliberately not Arrow order: evaluator must align by verified key.
        return ([("b" * 64, 19.0), ("a" * 64, 11.0)], {"vector_dimensions": 8})

    monkeypatch.setattr(evaluator, "EXPECTED_COUNT", 2)
    monkeypatch.setattr(evaluator, "sha256", synthetic_sha256)
    monkeypatch.setattr(evaluator.model_api, "audit_candidate", Mock(return_value=audit))
    monkeypatch.setattr(evaluator.model_api, "load_candidate", Mock(return_value=model))
    monkeypatch.setattr(evaluator.model_api, "no_fit_guard", guard)
    monkeypatch.setattr(evaluator.data_api, "verify_holdout_metadata", Mock(return_value={"preparation_identity": "locked"}))
    monkeypatch.setattr(evaluator.data_api, "load_holdout_samples", Mock(side_effect=load_rows))
    monkeypatch.setattr(evaluator, "claim_scratch", Mock(return_value=owned_scratch))
    monkeypatch.setattr(evaluator, "create_spark", Mock(return_value=spark))
    monkeypatch.setattr(evaluator, "_runtime_identity", Mock(return_value={"runtime": "synthetic"}))
    monkeypatch.setattr(evaluator, "telemetry", Mock(return_value={"memory.events": "oom 0"}))
    collector = Mock(side_effect=collect_once)
    monkeypatch.setattr(evaluator, "_collect_predictions_once", collector)
    return {"root": tmp_path, "output": output, "scratch": owned_scratch, "rows": rows,
            "events": events, "collector": collector, "spark": spark, "audit": audit}


def test_execute_freezes_before_holdout_and_persists_predictions_before_offline_metrics(execution, monkeypatch):
    import pyarrow.parquet as pq

    output = execution["output"]
    real_evaluate = evaluator.model_api.evaluate_predictions

    def metrics(rows, predictions, baseline, **kwargs):
        assert (output / "native_predictions.parquet").is_file()
        assert (output / "evaluation_rows.parquet").is_file()
        assert predictions == [11.0, 19.0]
        execution["events"].append("metrics_after_capture")
        return real_evaluate(rows, predictions, baseline, **kwargs, expected_count=2)

    monkeypatch.setattr(evaluator.model_api, "evaluate_predictions", Mock(side_effect=metrics))
    result = evaluator.run(arguments("--execute", "--output-dir", str(output), "--prior-task-bytes", "0"),
                           project_root=execution["root"])
    assert result["status"] == "EVALUATED_NOT_DEPLOYED"
    assert result["fit_calls"] == result["blocked_fit_calls"] == 0
    assert result["prediction_calls"] == 1
    assert result["gate"]["pass"] is True
    assert execution["events"] == ["holdout_read_after_freeze", "predict_once", "metrics_after_capture"]
    execution["collector"].assert_called_once()
    execution["spark"].stop.assert_called_once()
    assert not execution["scratch"].exists()
    captured = pq.read_table(output / "evaluation_rows.parquet").to_pydict()
    assert captured["journey_key"] == [row["journey_key"] for row in execution["rows"]]
    assert captured["native_gbt_prediction"] == [11.0, 19.0]
    checkpoint = json.loads((output / "evaluation_checkpoint.json").read_text())
    assert checkpoint["status"] == "EVALUATED_NOT_DEPLOYED"
    assert checkpoint["prediction_calls"] == 1
    original = (output / "native_predictions.parquet").read_bytes()
    with pytest.raises(ValueError, match="not_new_private_attempt"):
        evaluator.run(arguments("--execute", "--output-dir", str(output), "--prior-task-bytes", "0"),
                      project_root=execution["root"])
    execution["collector"].assert_called_once()
    assert (output / "native_predictions.parquet").read_bytes() == original


def test_failure_after_predict_keeps_capture_failed_checkpoint_and_does_not_retry(execution, monkeypatch):
    output = execution["output"]
    failure = Mock(side_effect=RuntimeError("synthetic_metrics_failure"))
    monkeypatch.setattr(evaluator.model_api, "evaluate_predictions", failure)
    with pytest.raises(RuntimeError, match="synthetic_metrics_failure"):
        evaluator.run(arguments("--execute", "--output-dir", str(output), "--prior-task-bytes", "0"),
                      project_root=execution["root"])
    execution["collector"].assert_called_once()
    failure.assert_called_once()
    assert (output / "native_predictions.parquet").is_file()
    assert (output / "evaluation_rows.parquet").is_file()
    checkpoint = json.loads((output / "evaluation_checkpoint.json").read_text())
    assert checkpoint["status"] == "FAILED"
    assert checkpoint["prediction_calls"] == 1
    assert checkpoint["prediction_capture"]["sha256"] == evaluator.sha256(output / "native_predictions.parquet")
    assert checkpoint["fit_calls"] == 0
    assert checkpoint["error_type"] == "RuntimeError"
    execution["spark"].stop.assert_called_once()
    assert not execution["scratch"].exists()
