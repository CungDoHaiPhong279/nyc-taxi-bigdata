"""Native-profile PLAN/configuration/serialization guards; no real Spark or data."""

from copy import deepcopy
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from nyc_taxi.ml import pretrip_training as trainer
from nyc_taxi.ml import pretrip_training_model as model_api


def arguments(*values):
    return trainer.parser().parse_args(list(values))


def saved_config(encoding="native", loss="squared", *, legacy=False):
    config = {
        "gbt_parameters": {**model_api.GBT_PARAMETERS, "lossType": loss,
                           "maxBins": 256 if encoding == "native" else 32},
        "features": list(trainer.FEATURE_COLUMNS),
        "baseline_policy": dict(model_api.BASELINE_POLICY),
    }
    if not legacy:
        config.update(encoding_profile=encoding, loss_profile=loss)
    return config


def test_encoding_parser_preserves_one_hot_squared_defaults_and_explicit_native():
    defaults = arguments()
    assert defaults.encoding_profile == "one_hot"
    assert defaults.loss_profile == "squared"
    assert defaults.comparison_artifacts is None
    chosen = arguments("--encoding-profile", "native", "--comparison-artifacts", "/explicit/absolute")
    assert chosen.encoding_profile == "native"
    assert chosen.comparison_artifacts == Path("/explicit/absolute")
    with pytest.raises(SystemExit) as caught:
        arguments("--encoding-profile", "ordinal")
    assert caught.value.code == 2


def test_native_plan_keeps_eight_raw_features_and_changes_encoding_plus_maxbins_without_io(tmp_path, monkeypatch):
    monkeypatch.setattr(trainer, "load_contract", lambda: {})
    monkeypatch.setattr(Path, "open", Mock(side_effect=AssertionError("PLAN must not open artifacts or sample files")))
    monkeypatch.setattr(Path, "mkdir", Mock(side_effect=AssertionError("PLAN must not write")))
    spark = Mock(side_effect=AssertionError("PLAN must not start Spark"))
    fit = Mock(side_effect=AssertionError("PLAN must not fit"))
    monkeypatch.setattr(trainer, "create_spark", spark)
    monkeypatch.setattr(model_api, "fit", fit)
    before = deepcopy(model_api.GBT_PARAMETERS)
    result = trainer.run(arguments("--encoding-profile", "native",
                                   "--reference-artifacts", "/unavailable/squared",
                                   "--comparison-artifacts", "/unavailable/absolute"), project_root=tmp_path)
    assert result["status"] == "PLAN_NOT_EXECUTED"
    assert result["encoding_profile"] == "native"
    assert result["loss_profile"] == "squared"
    assert result["gbt_parameters"] == {**before, "maxBins": 256}
    assert result["features"] == list(trainer.FEATURE_COLUMNS)
    assert len(result["features"]) == 8
    assert result["baseline_policy"] == model_api.BASELINE_POLICY
    assert result["sample_splits"] == ["train", "validation"]
    assert result["holdout_access"] is result["raw_source_access"] is result["default_execute"] is False
    assert model_api.GBT_PARAMETERS == before
    spark.assert_not_called()
    fit.assert_not_called()


def test_native_absolute_combination_rejected_before_io_or_spark(tmp_path, monkeypatch):
    monkeypatch.setattr(trainer, "load_contract", lambda: {})
    monkeypatch.setattr(Path, "open", Mock(side_effect=AssertionError("invalid profile must fail before read")))
    mkdir = Mock(side_effect=AssertionError("invalid profile must fail before outputs"))
    spark = Mock(side_effect=AssertionError("invalid profile must fail before Spark"))
    monkeypatch.setattr(Path, "mkdir", mkdir)
    monkeypatch.setattr(trainer, "create_spark", spark)
    with pytest.raises(ValueError):
        trainer.run(arguments("--execute", "--encoding-profile", "native", "--loss-profile", "absolute",
                              "--prior-task-bytes", "0"), project_root=tmp_path)
    mkdir.assert_not_called()
    spark.assert_not_called()


@pytest.mark.parametrize("references", [[], ["--reference-artifacts", "/absent/squared"],
                                         ["--comparison-artifacts", "/absent/absolute"]])
def test_native_execution_requires_both_pinned_references_before_outputs_or_spark(tmp_path, monkeypatch, references):
    monkeypatch.setattr(trainer, "load_contract", lambda: {})
    mkdir = Mock(side_effect=AssertionError("missing reference must fail before outputs"))
    spark = Mock(side_effect=AssertionError("missing reference must fail before Spark"))
    monkeypatch.setattr(Path, "mkdir", mkdir)
    monkeypatch.setattr(trainer, "create_spark", spark)
    with pytest.raises(ValueError):
        trainer.run(arguments("--execute", "--encoding-profile", "native", "--prior-task-bytes", "0",
                              *references), project_root=tmp_path)
    mkdir.assert_not_called()
    spark.assert_not_called()


@pytest.mark.parametrize("encoding,loss,legacy", [
    ("native", "squared", False), ("one_hot", "squared", True),
    ("one_hot", "absolute", True), ("one_hot", "squared", False),
    ("one_hot", "absolute", False),
])
def test_reload_recovers_native_and_both_legacy_profiles_without_mutating_configuration(encoding, loss, legacy):
    config = saved_config(encoding, loss, legacy=legacy)
    before = deepcopy(config)
    assert trainer.profile_from_saved_config(config) == loss
    assert trainer.encoding_from_saved_config(config) == encoding
    assert config == before


@pytest.mark.parametrize("mutation", ["old_maxbins", "too_many_bins", "absolute", "unknown_encoding",
                                     "loss_profile", "iterations", "extra_param", "features", "baseline"])
def test_saved_native_configuration_rejects_drift(mutation):
    config = saved_config()
    if mutation == "old_maxbins":
        config["gbt_parameters"]["maxBins"] = 32
    elif mutation == "too_many_bins":
        config["gbt_parameters"]["maxBins"] = 257
    elif mutation == "absolute":
        config["gbt_parameters"]["lossType"] = "absolute"
        config["loss_profile"] = "absolute"
    elif mutation == "unknown_encoding":
        config["encoding_profile"] = "ordinal"
    elif mutation == "loss_profile":
        config["loss_profile"] = "absolute"
    elif mutation == "iterations":
        config["gbt_parameters"]["maxIter"] = 21
    elif mutation == "extra_param":
        config["gbt_parameters"]["unapproved"] = 1
    elif mutation == "features":
        config["features"].append("fare_amount")
    else:
        config["baseline_policy"]["hour_bin_width"] = 3
    with pytest.raises(ValueError):
        trainer.profile_from_saved_config(config)


def write_metadata(tmp_path, *, stage=3, loss="squared", maxbins=256, default_loss="squared"):
    relative = f"pipeline/stages/{stage}_GBTRegressionModel_fixture/metadata/part-00000.txt"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({
        "class": "org.apache.spark.ml.regression.GBTRegressionModel",
        "paramMap": {"lossType": loss, "maxBins": maxbins},
        "defaultParamMap": {"lossType": default_loss, "maxBins": 32},
    }), encoding="utf-8")
    return path, {"path": relative, "sha256": trainer.sha256(path)}


def test_native_saved_metadata_reads_stage_three_effective_values_not_library_defaults(tmp_path):
    _, entry = write_metadata(tmp_path, default_loss="absolute")
    result = trainer.saved_loss_metadata(tmp_path, [entry], "squared", encoding_profile="native")
    assert result["path"] == entry["path"]
    assert result["paramMap_lossType"] == "squared"
    assert result["defaultParamMap_lossType"] == "absolute"


@pytest.mark.parametrize("mutation", ["wrong_stage", "wrong_loss", "wrong_bins", "bad_hash", "duplicate"])
def test_native_saved_metadata_rejects_wrong_stage_loss_bins_hash_or_ambiguity(tmp_path, mutation):
    _, entry = write_metadata(tmp_path, stage=4 if mutation == "wrong_stage" else 3,
                              loss="absolute" if mutation == "wrong_loss" else "squared",
                              maxbins=32 if mutation == "wrong_bins" else 256)
    entries = [entry]
    if mutation == "bad_hash":
        entry["sha256"] = "0" * 64
    if mutation == "duplicate":
        entries.append(dict(entry))
    with pytest.raises(ValueError):
        trainer.saved_loss_metadata(tmp_path, entries, "squared", encoding_profile="native")


@pytest.mark.parametrize("loss", ["squared", "absolute"])
def test_legacy_stage_four_metadata_remains_reload_compatible(tmp_path, loss):
    _, entry = write_metadata(tmp_path, stage=4, loss=loss, maxbins=32)
    result = trainer.saved_loss_metadata(tmp_path, [entry], loss)
    assert result["paramMap_lossType"] == loss
    assert result["defaultParamMap_lossType"] == "squared"
