"""Offline trainer orchestration/quotas/identity guards; never fit or open holdout."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from nyc_taxi.ml import pretrip_training as trainer
from nyc_taxi.ml import pretrip_training_model as model_api
from nyc_taxi.ml.pretrip_budget import ManagedBudgetExceeded, ManagedFileBudget, METADATA_RESERVE_BYTES


def arguments(*values):
    return trainer.parser().parse_args(list(values))


def evaluation(*, global_mae=20.0, hierarchical_mae=10.0, gbt_mae=9.0):
    result = {}
    for split in ("train", "validation"):
        result[split] = {
            name: {"mae_usd": mae, "metrics_status": "finite_all_rows", "nonfinite_prediction_count": 0}
            for name, mae in (("global_median", global_mae), ("hierarchical_median", hierarchical_mae), ("gbt", gbt_mae))
        }
    return result


def tree_identity(entries):
    return hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def bundle(tmp_path):
    root = tmp_path / "bundle"
    root.mkdir()
    files = ("pipeline/stages/part-00000", "pipeline/metadata/part-00000", "baselines.json",
             "parity_inputs.json", "parity_reference.json", "parity_baseline_reference.json",
             "training_config.json", "sample_lineage.json", "metrics.json",
             "pretrip_v2_contract.json", "pretrip_v2_calendar.json")
    entries = []
    for name in files:
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((name + " synthetic fixture").encode())
        entries.append({"path": name, "bytes": target.stat().st_size, "sha256": trainer.sha256(target)})
    manifest = {"version": "pretrip-v2-training-artifacts-1", "files": entries,
                "model_tree_sha256": tree_identity([entry for entry in entries if entry["path"].startswith("pipeline/")])}
    write_manifest(root, manifest)
    return root, manifest


def write_manifest(root, manifest):
    (root / "artifact_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


@pytest.mark.parametrize("extra", [[], ["--reload-artifacts", "/unavailable/explicit-model"]])
def test_plan_is_default_no_samples_spark_or_writes(tmp_path, monkeypatch, extra):
    monkeypatch.setattr(trainer, "load_contract", Mock(return_value={}))
    spark = Mock(side_effect=AssertionError("PLAN must not start Spark"))
    fit = Mock(side_effect=AssertionError("PLAN must not fit"))
    monkeypatch.setattr(trainer, "create_spark", spark)
    monkeypatch.setattr(model_api, "fit", fit)
    monkeypatch.setattr(Path, "open", Mock(side_effect=AssertionError("PLAN must not open sample/model files")))
    monkeypatch.setattr(Path, "mkdir", Mock(side_effect=AssertionError("PLAN must not create files")))
    result = trainer.run(arguments(*extra), project_root=tmp_path)
    assert result["status"] == "PLAN_NOT_EXECUTED"
    assert result["sample_splits"] == ["train", "validation"]
    assert result["features"] == list(trainer.FEATURE_COLUMNS)
    assert result["gbt_parameters"] == model_api.GBT_PARAMETERS
    assert result["baseline_policy"] == model_api.BASELINE_POLICY
    assert result["holdout_access"] is result["raw_source_access"] is result["default_execute"] is False
    assert result["mode"] == ("reload_only" if extra else "train_validation")
    spark.assert_not_called()
    fit.assert_not_called()


@pytest.mark.parametrize("prior", [None, -1, True, 1.5, "100"])
def test_execution_budget_requires_explicit_nonnegative_integer(prior):
    with pytest.raises(ValueError, match="explicit_prior_task_bytes_required"):
        trainer.artifact_budget(prior)


def test_budget_charges_prior_usage_once_and_reserves_full_scratch_ceiling():
    prior = 191797209
    assert trainer.artifact_budget(prior) == 2048 * trainer.MIB - prior - 512 * trainer.MIB
    last_usable_prior = trainer.ENVELOPE_BYTES - trainer.SCRATCH_CEILING_BYTES - METADATA_RESERVE_BYTES
    assert trainer.artifact_budget(last_usable_prior) == METADATA_RESERVE_BYTES
    with pytest.raises(ValueError, match="v2_envelope_exhausted_no_implicit_renewal"):
        trainer.artifact_budget(last_usable_prior + 1)


def test_execute_without_prior_budget_fails_before_creating_output(tmp_path, monkeypatch):
    monkeypatch.setattr(trainer, "load_contract", lambda: {})
    spark = Mock()
    monkeypatch.setattr(trainer, "create_spark", spark)
    with pytest.raises(ValueError, match="explicit_prior_task_bytes_required"):
        trainer.run(arguments("--execute"), project_root=tmp_path)
    assert not (tmp_path / ".sandbox").exists()
    spark.assert_not_called()


@pytest.mark.parametrize("value", [".sandbox/pretrip-v2", "data/model", "models/attempt", ".sandbox/pretrip-v2/../../escape"])
def test_output_requires_new_strict_v2_private_child(tmp_path, value):
    with pytest.raises(ValueError, match="training_output_not_new_private_attempt"):
        trainer.private_output(Path(value), tmp_path)
    assert not (tmp_path / ".sandbox").exists()


def test_output_existing_or_symlink_is_not_overwritten(tmp_path):
    directory = tmp_path / ".sandbox/pretrip-v2/existing"
    directory.mkdir(parents=True)
    sentinel = directory / "keep.txt"
    sentinel.write_bytes(b"preserve")
    with pytest.raises(ValueError, match="training_output_not_new_private_attempt"):
        trainer.private_output(directory, tmp_path)
    alias = directory.parent / "alias"
    alias.symlink_to(directory, target_is_directory=True)
    with pytest.raises(ValueError, match="training_output_symlink_or_junction"):
        trainer.private_output(alias / "new", tmp_path)
    assert sentinel.read_bytes() == b"preserve"


@pytest.mark.parametrize("value", ["/tmp", "/var/unowned", "/tmp/../var/escape"])
def test_scratch_rejects_broad_or_traversal_target_without_write(monkeypatch, value):
    mkdir = Mock(side_effect=AssertionError("unsafe target must not be created"))
    monkeypatch.setattr(Path, "mkdir", mkdir)
    with pytest.raises(ValueError, match="training_scratch_must_be_private_tmp_child"):
        trainer.claim_scratch(Path(value))
    mkdir.assert_not_called()


def test_scratch_claim_refuses_preexisting_directory_without_cleanup(monkeypatch):
    monkeypatch.setattr(Path, "exists", lambda self: True)
    monkeypatch.setattr(Path, "is_symlink", lambda self: False)
    mkdir = Mock()
    monkeypatch.setattr(Path, "mkdir", mkdir)
    with pytest.raises(FileExistsError, match="training_scratch_already_exists"):
        trainer.claim_scratch(Path("/tmp/preexisting-training-fixture"))
    mkdir.assert_not_called()


def test_gate_uses_best_validation_baseline_and_accepts_exact_ten_percent():
    result = trainer.evaluate_gate(evaluation())
    assert result["pass"] is True
    assert result["best_validation_baseline"] == "hierarchical_median"
    assert result["mae_improvement_percent"] == pytest.approx(10)
    values = evaluation(global_mae=10, hierarchical_mae=100, gbt_mae=9.000001)
    assert trainer.evaluate_gate(values)["pass"] is False
    assert trainer.evaluate_gate(values)["best_validation_baseline"] == "global_median"


@pytest.mark.parametrize("split,name", [("train", "gbt"), ("validation", "global_median"), ("validation", "hierarchical_median")])
def test_gate_cannot_hide_nonfinite_predictions_in_any_reported_model(split, name):
    values = evaluation()
    values[split][name]["nonfinite_prediction_count"] = 1
    result = trainer.evaluate_gate(values)
    assert result["pass"] is False and result["reason"] == "nonfinite_or_undefined_metrics"


def test_gate_reports_undefined_metrics_as_failure_and_never_changes_configuration():
    values = evaluation()
    values["validation"]["gbt"]["mae_usd"] = None
    original = deepcopy(values)
    assert trainer.evaluate_gate(values)["pass"] is False
    assert values == original
    assert trainer.evaluate_gate(evaluation(global_mae=0, hierarchical_mae=0, gbt_mae=0))["pass"] is False


def test_gate_does_not_drop_or_clip_negative_predictions():
    values = evaluation()
    values["validation"]["gbt"]["negative_prediction_count"] = 2
    assert trainer.evaluate_gate(values)["pass"] is True
    assert values["validation"]["gbt"]["negative_prediction_count"] == 2


def test_verified_bundle_checks_every_file_and_full_model_tree_identity(tmp_path):
    directory, manifest = bundle(tmp_path)
    assert trainer.verify_bundle(directory) == manifest
    assert len(manifest["model_tree_sha256"]) == 64
    (directory / "baselines.json").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="artifact_checksum_mismatch"):
        trainer.verify_bundle(directory)


@pytest.mark.parametrize("mutation", ["wrong_version", "tree_hash", "missing_required", "no_pipeline", "duplicate"])
def test_bundle_incomplete_or_identity_mismatch_is_rejected(tmp_path, mutation):
    directory, manifest = bundle(tmp_path)
    if mutation == "wrong_version":
        manifest["version"] = "latest"
    elif mutation == "tree_hash":
        manifest["model_tree_sha256"] = "0" * 64
    elif mutation == "missing_required":
        manifest["files"] = [entry for entry in manifest["files"] if entry["path"] != "metrics.json"]
    elif mutation == "no_pipeline":
        manifest["files"] = [entry for entry in manifest["files"] if not entry["path"].startswith("pipeline/")]
        manifest["model_tree_sha256"] = tree_identity([])
    else:
        manifest["files"].append(dict(manifest["files"][0]))
    write_manifest(directory, manifest)
    with pytest.raises(ValueError, match="unsupported_training_artifact_manifest|artifact_manifest_incomplete_or_model_identity_mismatch|artifact_manifest_path_escape"):
        trainer.verify_bundle(directory)


@pytest.mark.parametrize("escape", ["../escape", "/absolute/escape"])
def test_manifest_rejects_path_escape_before_opening_target(tmp_path, escape):
    directory, manifest = bundle(tmp_path)
    manifest["files"].insert(0, {"path": escape, "bytes": 0, "sha256": "0" * 64})
    write_manifest(directory, manifest)
    with pytest.raises(ValueError, match="artifact_manifest_path_escape"):
        trainer.verify_bundle(directory)


def test_bundle_rejects_symlink_even_when_same_content_and_inside_bundle(tmp_path):
    directory, manifest = bundle(tmp_path)
    target = directory / "baselines.json"
    backup = directory / "baseline-source.json"
    target.rename(backup)
    target.symlink_to(backup)
    with pytest.raises(ValueError, match="artifact_manifest_path_escape"):
        trainer.verify_bundle(directory)


def test_artifact_writes_are_guarded_and_never_replace_existing_output(tmp_path):
    destination = tmp_path / "owned"
    destination.mkdir()
    budget = ManagedFileBudget((), destination, METADATA_RESERVE_BYTES + 32)
    path = destination / "config.json"
    trainer.write_document(budget, path, {"seed": 42})
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        trainer.write_document(budget, path, {"seed": 43})
    assert path.read_bytes() == before
    with pytest.raises(ManagedBudgetExceeded):
        trainer.write_document(budget, destination / "too-large.json", {"payload": "synthetic" * 100})
    assert sum(path.stat().st_size for path in destination.iterdir()) <= 32


def test_pipeline_publish_streams_through_quota_and_rejects_republish(tmp_path):
    staged, owned = tmp_path / "staged", tmp_path / "owned"
    staged.mkdir()
    owned.mkdir()
    (staged / "part").write_bytes(b"synthetic-model")
    budget = ManagedFileBudget((), owned, METADATA_RESERVE_BYTES + 32)
    entries = trainer.publish_pipeline(budget, staged, owned / "pipeline")
    assert entries == [{"path": "pipeline/part", "bytes": 15, "sha256": trainer.sha256(staged / "part")}]
    with pytest.raises(FileExistsError):
        trainer.publish_pipeline(budget, staged, owned / "pipeline")
    assert (owned / "pipeline/part").read_bytes() == b"synthetic-model"


def test_pipeline_quota_failure_keeps_bounded_checkpoint_space(tmp_path):
    staged, owned = tmp_path / "staged", tmp_path / "owned"
    staged.mkdir()
    owned.mkdir()
    (staged / "part").write_bytes(b"small synthetic model")
    budget = ManagedFileBudget((), owned, METADATA_RESERVE_BYTES + 1)
    with pytest.raises(ManagedBudgetExceeded):
        trainer.publish_pipeline(budget, staged, owned / "pipeline")
    budget.write_json(owned / "training_checkpoint.json", {"status": "FAILED"}, exclusive=True)
    assert json.loads((owned / "training_checkpoint.json").read_text())["status"] == "FAILED"
    assert budget.snapshot()["used_bytes"] <= budget.limit_bytes


@pytest.mark.parametrize("scratch_owned", [False, True])
def test_failed_start_retains_checkpoint_and_only_cleans_owned_scratch(tmp_path, monkeypatch, scratch_owned):
    from nyc_taxi.ml import pretrip_training_data

    scratch = tmp_path / "mock-private-scratch"
    protected = tmp_path / "preexisting-protected"
    protected.mkdir()
    sentinel = protected / "keep.txt"
    sentinel.write_bytes(b"preserve")
    monkeypatch.setattr(trainer, "load_contract", lambda: {})
    monkeypatch.setattr(trainer, "telemetry", lambda: {"synthetic": True})
    monkeypatch.setattr(pretrip_training_data, "load_training_samples", lambda path: {
        "train": [{}], "validation": [{}], "lineage": {"sample_identity_sha256": "0" * 64},
    })
    monkeypatch.setattr(model_api, "fit_baselines", lambda rows: {"synthetic": True})

    def claim(path):
        if not scratch_owned:
            raise FileExistsError("training_scratch_already_exists")
        scratch.mkdir()
        return scratch

    monkeypatch.setattr(trainer, "claim_scratch", claim)
    spark = Mock(side_effect=RuntimeError("synthetic-start-failure"))
    monkeypatch.setattr(trainer, "create_spark", spark)
    target = Path(".sandbox/pretrip-v2/synthetic-attempt/artifacts")
    with pytest.raises((FileExistsError, RuntimeError)):
        trainer.run(arguments("--execute", "--prior-task-bytes", "0", "--output-dir", str(target),
                              "--scratch-dir", str(protected)), project_root=tmp_path)
    checkpoint = json.loads((tmp_path / target / "training_checkpoint.json").read_text())
    assert checkpoint["status"] == "FAILED" and checkpoint["fit_calls"] == 0
    assert checkpoint["disk_budget"]["used_bytes"] <= checkpoint["disk_budget"]["limit_bytes"]
    assert sentinel.read_bytes() == b"preserve"
    assert not scratch.exists()
    assert spark.call_count == int(scratch_owned)


@pytest.mark.parametrize("result,exit_code", [({"status": "PLAN_NOT_EXECUTED"}, 0), ({"gate": {"pass": True}}, 0), ({"gate": {"pass": False}}, 2)])
def test_main_reports_gate_failure_without_claiming_fit_failed(monkeypatch, capsys, result, exit_code):
    monkeypatch.setattr(trainer, "run", Mock(return_value=result))
    assert trainer.main([]) == exit_code
    assert json.loads(capsys.readouterr().out) == result


def test_main_does_not_print_sensitive_exception_text(monkeypatch, capsys):
    monkeypatch.setattr(trainer, "run", Mock(side_effect=RuntimeError("PRIVATE synthetic raw response")))
    assert trainer.main([]) == 1
    output = capsys.readouterr().out
    assert "PRIVATE" not in output
    assert json.loads(output) == {"status": "FAILED", "error_type": "RuntimeError",
                                  "code": "pretrip_training_failed_see_private_checkpoint"}


def test_telemetry_reads_only_explicit_cgroup_counters(monkeypatch):
    expected = {"memory.peak", "memory.current", "memory.events", "memory.max",
                "memory.swap.max", "cpu.max", "pids.max"}
    reads = []
    monkeypatch.setattr(Path, "is_file", lambda path: path.parent == Path("/sys/fs/cgroup"))

    def read(path):
        reads.append(path.name)
        return " 1\n"

    monkeypatch.setattr(Path, "read_text", read)
    result = trainer.telemetry()
    assert set(reads) == expected
    assert set(result) == expected | {"python_peak_rss_kib"}
    assert all(result[name] == "1" for name in expected)
