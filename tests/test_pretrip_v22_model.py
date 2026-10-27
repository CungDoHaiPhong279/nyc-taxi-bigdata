"""Offline native-profile guards; no Spark session, source or model is opened."""

from __future__ import annotations

from copy import deepcopy
import inspect
from types import SimpleNamespace

import pytest

from nyc_taxi.ml.pretrip_features import (
    CATEGORICAL_FEATURE_COLUMNS, FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS,
)
from nyc_taxi.ml.pretrip_training_model import (
    GBT_PARAMETERS, INDEX_COLUMNS, fit, gbt_parameters, make_pipeline,
    validate_native_categories, validate_vector_metadata,
)


def native_metadata():
    numeric = [{"idx": index, "name": name} for index, name in enumerate(NUMERIC_FEATURE_COLUMNS)]
    nominal = [{"idx": index + 6, "name": name, "vals": ["1", "2", "__unknown"]}
               for index, name in enumerate(INDEX_COLUMNS)]
    return {"ml_attr": {"num_attrs": 8, "attrs": {"numeric": numeric, "nominal": nominal}}}


def frame(metadata):
    return SimpleNamespace(columns=["features"], schema={"features": SimpleNamespace(metadata=metadata)})


def test_native_profile_changes_encoding_and_only_max_bins_in_regressor_policy():
    original, native = gbt_parameters(), gbt_parameters(encoding_profile="native")
    assert original == GBT_PARAMETERS
    assert {name for name in original if original[name] != native[name]} == {"maxBins"}
    assert native["maxBins"] == 256 and native["lossType"] == "squared"
    for function in (gbt_parameters, make_pipeline, fit, validate_vector_metadata):
        assert inspect.signature(function).parameters["encoding_profile"].default == "one_hot"
    assert gbt_parameters("absolute")["lossType"] == "absolute"
    native["maxDepth"] = 999
    assert gbt_parameters(encoding_profile="native")["maxDepth"] == 4


@pytest.mark.parametrize("encoding", ["", "Native", "ordinal", "vector_indexer", None, 0])
def test_invalid_encoding_rejected_before_spark(encoding):
    with pytest.raises(ValueError, match="encoding profile"):
        gbt_parameters(encoding_profile=encoding)
    with pytest.raises(ValueError, match="encoding profile"):
        make_pipeline(encoding_profile=encoding)
    with pytest.raises(ValueError, match="encoding profile"):
        fit(object(), encoding_profile=encoding)


def test_absolute_native_combination_is_not_an_approved_experiment():
    for action in (lambda: gbt_parameters("absolute", encoding_profile="native"),
                   lambda: make_pipeline(loss_profile="absolute", encoding_profile="native"),
                   lambda: fit(object(), loss_profile="absolute", encoding_profile="native")):
        with pytest.raises(ValueError, match="only permits squared"):
            action()


def test_max_bins_count_includes_unknown_at_exact_boundary():
    labels = sorted(str(index) for index in range(1, 256))
    assert validate_native_categories({name: labels for name in CATEGORICAL_FEATURE_COLUMNS}) == {
        name: 256 for name in CATEGORICAL_FEATURE_COLUMNS}
    labels = sorted(str(index) for index in range(1, 257))
    with pytest.raises(ValueError, match="including unknown exceed"):
        validate_native_categories({name: labels for name in CATEGORICAL_FEATURE_COLUMNS})


@pytest.mark.parametrize("labels", [[], ["1", "1"], ["2", "1"], ["264"], ["01"], [1], ["__unknown"]])
def test_native_dictionary_guard_rejects_missing_duplicate_ordinal_or_invalid_labels(labels):
    with pytest.raises(ValueError, match="dictionary"):
        validate_native_categories({name: labels for name in CATEGORICAL_FEATURE_COLUMNS})


@pytest.mark.parametrize("bins", [32, 255, 257, 512, True])
def test_native_bins_policy_cannot_be_tuned_or_raised(bins):
    with pytest.raises(ValueError, match="fixed at 256"):
        validate_native_categories({name: ["1"] for name in CATEGORICAL_FEATURE_COLUMNS}, max_bins=bins)


def test_native_dictionary_keys_cannot_include_an_outcome_or_omit_zone():
    with pytest.raises(ValueError, match="allowlist"):
        validate_native_categories({"pickup_zone": ["1"], "fare_amount": ["2"]})


def test_native_metadata_requires_exact_eight_dimensions_and_nominal_zones():
    result = validate_vector_metadata(frame(native_metadata()), encoding_profile="native")
    assert result == {"input_feature_names": list(FEATURE_COLUMNS), "vector_dimensions": 8,
                      "categorical_nominal_dimensions": {"pickup_zone": 3, "dropoff_zone": 3}}


@pytest.mark.parametrize("mutation", [
    "missing", "ordinal", "binary", "numeric_category", "outcome", "ninth_feature",
    "missing_unknown", "duplicate_index", "reordered_numeric", "duplicate_nominal",
])
def test_native_metadata_rejects_lost_nominal_tags_wrong_dimensions_and_leakage(mutation):
    metadata = deepcopy(native_metadata())
    attributes = metadata["ml_attr"]["attrs"]
    if mutation == "missing":
        metadata = {}
    elif mutation == "ordinal":
        attributes["numeric"].extend(attributes.pop("nominal"))
    elif mutation == "binary":
        attributes["binary"] = attributes.pop("nominal")
    elif mutation == "numeric_category":
        attributes["nominal"].append(attributes["numeric"].pop())
    elif mutation == "outcome":
        attributes["nominal"][0]["name"] = "fare_amount"
    elif mutation == "ninth_feature":
        metadata["ml_attr"]["num_attrs"] = 9
        attributes["numeric"].append({"idx": 8, "name": "trip_distance"})
    elif mutation == "missing_unknown":
        attributes["nominal"][0]["vals"] = ["1", "2"]
    elif mutation == "duplicate_index":
        attributes["nominal"][0]["idx"] = 5
    elif mutation == "reordered_numeric":
        attributes["numeric"][0]["name"], attributes["numeric"][1]["name"] = (
            attributes["numeric"][1]["name"], attributes["numeric"][0]["name"])
    elif mutation == "duplicate_nominal":
        attributes["nominal"].append(deepcopy(attributes["nominal"][0]))
    with pytest.raises(ValueError, match="allowlist|native"):
        validate_vector_metadata(frame(metadata), encoding_profile="native")


def test_nominal_metadata_does_not_accept_categories_past_fixed_bins():
    metadata = native_metadata()
    metadata["ml_attr"]["attrs"]["nominal"][0]["vals"] = (
        sorted(str(index) for index in range(1, 257)) + ["__unknown"])
    with pytest.raises(ValueError, match="including unknown exceed"):
        validate_vector_metadata(frame(metadata), encoding_profile="native")
