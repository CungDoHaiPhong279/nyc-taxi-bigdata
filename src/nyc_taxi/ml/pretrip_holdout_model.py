"""Frozen native-V2.2 artifact audit and no-fit holdout metrics.

Importing this module opens no data, model, JVM or network connection. The only
approved candidate is pinned in code, independently of its mutable manifest.
Training loaders and the frozen production-v1 helpers are not imported here.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import stat
from typing import Any, Mapping, Sequence

from nyc_taxi.ml.pretrip_features import (
    CATEGORICAL_FEATURE_COLUMNS, FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS,
    TIMEZONE_NAME, timezone_identity,
)
from nyc_taxi.ml import pretrip_training_model as model_api
from nyc_taxi.ml.pretrip_training_reference import _labels


MODEL_TREE_SHA256 = "27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3"
ARTIFACT_MANIFEST_SHA256 = "8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75"
SAMPLE_IDENTITY_SHA256 = "7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86"
DICTIONARY_SHA256 = "aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c"
TZIF_SHA256 = "e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95"
PINNED_MEMBERS = {
    "training_config.json": "b8ed00ab0a6e19d92fd986c86b14f97f1651cbe2a87ceabaf02bee3499e63e90",
    "baselines.json": "bc03cc2e418955b8236322fd6987cf2da39ea1a52c38afe06af386d71d095d8e",
    "metrics.json": "61b1bf2b4c8abbf858a7b7c88dce8bc2f74ab45550cb83ed70483f4b45c3f4f1",
    "sample_lineage.json": "3ab1c24996401b4333c7b2942769bd6c1a239e2705e3997efc8da892507edccf",
    "pretrip_v2_contract.json": "a1ddbe55f0128c20d8189d9f37b3b6172e90f20be71ae7a99300e21fb306add5",
    "pretrip_v2_calendar.json": "00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac",
}
GBT_METADATA_SHA256 = "bef445ce21bc2468d9e4530f0afcce42bb77c682e91fadca5049935456a48120"
EXPECTED_CATEGORY_COUNTS = {"pickup_zone": 117, "dropoff_zone": 224}
EXPECTED_TRAIN_COUNTS = {"train": 26573, "validation": 23526}
EXPECTED_HOLDOUT_COUNT = 21833
MAX_MEMBER_BYTES = 2 * 1024 * 1024
LOW_SAMPLE_THRESHOLD = 30
_FIT_STATE: ContextVar[dict[str, int] | None] = ContextVar("holdout_fit_state", default=None)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _sha256(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            result.update(block)
    return result.hexdigest()


def _regular_member(root: Path, name: str, *, expected_bytes: int | None = None) -> Path:
    relative = PurePosixPath(name)
    if (not name or "\\" in name or ":" in name or relative.is_absolute()
            or any(part in ("", ".", "..") for part in name.split("/"))
            or relative.as_posix() != name):
        raise ValueError("holdout_candidate_path_invalid")
    path = root
    for part in (None, *relative.parts):
        if part is not None:
            path = path / part
        info = path.lstat()
        if (stat.S_ISLNK(info.st_mode)
                or getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
            raise ValueError("holdout_candidate_link_rejected")
    if (not stat.S_ISREG(info.st_mode) or not 0 <= info.st_size <= MAX_MEMBER_BYTES
            or (expected_bytes is not None and info.st_size != expected_bytes)):
        raise ValueError("holdout_candidate_member_size_invalid")
    return path


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _stage_member(members: Mapping[str, Any], index: int) -> str:
    matches = [name for name in members if len(PurePosixPath(name).parts) == 5
               and name.startswith(f"pipeline/stages/{index}_")
               and PurePosixPath(name).parts[3] == "metadata"
               and PurePosixPath(name).name.startswith("part-") and name.endswith(".txt")]
    if len(matches) != 1:
        raise ValueError("holdout_candidate_stage_metadata_not_unique")
    return matches[0]


def audit_candidate(root: str | Path) -> dict[str, Any]:
    """Verify every manifest-listed file and pinned identity before JVM loading.

    The manifest hash, independent metadata hashes and canonical model-tree hash
    are hard-coded from the committed candidate report. Extra files under the
    pipeline are forbidden because Spark readers may discover those files.
    Non-pipeline attempt checkpoints/reports are neither trusted nor opened.
    """
    root = Path(root).absolute()
    manifest_path = _regular_member(root, "artifact_manifest.json")
    if _sha256(manifest_path) != ARTIFACT_MANIFEST_SHA256:
        raise ValueError("holdout_candidate_manifest_identity_mismatch")
    manifest = _read_json(manifest_path)
    if (manifest.get("version") != "pretrip-v2-training-artifacts-1"
            or manifest.get("model_tree_sha256") != MODEL_TREE_SHA256
            or any(manifest.get(name) is not False for name in ("holdout_opened", "promoted", "deployed"))):
        raise ValueError("holdout_candidate_manifest_contract_drift")
    members: dict[str, Any] = {}
    files = manifest.get("files")
    if not isinstance(files, list) or not files:
        raise ValueError("holdout_candidate_manifest_empty")
    for entry in files:
        if not isinstance(entry, dict) or set(entry) != {"path", "bytes", "sha256"}:
            raise ValueError("holdout_candidate_member_invalid")
        name, size, checksum = entry["path"], entry["bytes"], entry["sha256"]
        if (not isinstance(name, str) or name in members or type(size) is not int
                or not 0 <= size <= MAX_MEMBER_BYTES or not isinstance(checksum, str)
                or len(checksum) != 64 or any(char not in "0123456789abcdef" for char in checksum)):
            raise ValueError("holdout_candidate_member_invalid")
        path = _regular_member(root, name, expected_bytes=size)
        if _sha256(path) != checksum:
            raise ValueError("holdout_candidate_member_hash_mismatch")
        members[name] = entry
    for name, checksum in PINNED_MEMBERS.items():
        if members.get(name, {}).get("sha256") != checksum:
            raise ValueError("holdout_candidate_metadata_identity_mismatch")
    pipeline_entries = [entry for entry in files if entry["path"].startswith("pipeline/")]
    if not pipeline_entries or _digest(pipeline_entries) != MODEL_TREE_SHA256:
        raise ValueError("holdout_candidate_model_tree_identity_mismatch")
    discovered = set()
    for path in (root / "pipeline").rglob("*"):
        info = path.lstat()
        if (stat.S_ISLNK(info.st_mode)
                or getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)):
            raise ValueError("holdout_candidate_link_rejected")
        if stat.S_ISREG(info.st_mode):
            discovered.add(path.relative_to(root).as_posix())
        elif not stat.S_ISDIR(info.st_mode):
            raise ValueError("holdout_candidate_special_file_rejected")
    if discovered != {entry["path"] for entry in pipeline_entries}:
        raise ValueError("holdout_candidate_unlisted_pipeline_file")

    config = _read_json(root / "training_config.json")
    lineage = _read_json(root / "sample_lineage.json")
    baselines = _read_json(root / "baselines.json")
    expected_parameters = model_api.gbt_parameters("squared", encoding_profile="native")
    if (config.get("encoding_profile") != "native" or config.get("loss_profile") != "squared"
            or config.get("gbt_parameters") != expected_parameters
            or config.get("features") != list(FEATURE_COLUMNS)
            or config.get("baseline_policy") != model_api.BASELINE_POLICY
            or config.get("holdout_opened") is not False
            or config.get("experiment_changes") != ["encoding_one_hot_to_native", "maxBins_32_to_256"]):
        raise ValueError("holdout_candidate_configuration_drift")
    if (config.get("sample_identity_sha256") != SAMPLE_IDENTITY_SHA256
            or lineage.get("sample_identity_sha256") != SAMPLE_IDENTITY_SHA256
            or lineage.get("sample_counts") != EXPECTED_TRAIN_COUNTS
            or lineage.get("feature_columns") != list(FEATURE_COLUMNS)
            or lineage.get("journey_overlap_train_validation") != 0
            or lineage.get("holdout_data_accessed") is not False
            or lineage.get("raw_sources_accessed") is not False
            or lineage.get("calendar_sha256") != PINNED_MEMBERS["pretrip_v2_calendar.json"]
            or lineage.get("feature_contract_sha256") != PINNED_MEMBERS["pretrip_v2_contract.json"]
            or lineage.get("timezone_identity", {}).get("tzif_sha256") != TZIF_SHA256
            or lineage.get("timezone_identity", {}).get("timezone") != TIMEZONE_NAME):
        raise ValueError("holdout_candidate_lineage_drift")
    if (any(baselines.get(name) != value for name, value in model_api.BASELINE_POLICY.items())
            or baselines.get("train_count") != EXPECTED_TRAIN_COUNTS["train"]
            or baselines.get("global_median") != 12.8
            or len(baselines.get("pair_groups", [])) != 1402
            or len(baselines.get("time_groups", [])) != 1601):
        raise ValueError("holdout_candidate_baseline_drift")
    dictionaries = {name: _labels(root, members, index)
                    for index, name in enumerate(CATEGORICAL_FEATURE_COLUMNS)}
    if (_digest(dictionaries) != DICTIONARY_SHA256
            or {name: len(values) for name, values in dictionaries.items()} != EXPECTED_CATEGORY_COUNTS):
        raise ValueError("holdout_candidate_dictionary_drift")
    model_api.validate_native_categories(dictionaries)
    metadata = _audit_metadata(root, members, expected_parameters)
    contract = _read_json(root / "pretrip_v2_contract.json")
    if (contract.get("timezone") != TIMEZONE_NAME
            or contract.get("numeric_features") != list(NUMERIC_FEATURE_COLUMNS)
            or contract.get("categorical_features") != list(CATEGORICAL_FEATURE_COLUMNS)
            or contract.get("calendar", {}).get("sha256") != PINNED_MEMBERS["pretrip_v2_calendar.json"]):
        raise ValueError("holdout_candidate_feature_contract_drift")
    identities = {"model_tree_sha256": MODEL_TREE_SHA256,
                  "artifact_manifest_sha256": ARTIFACT_MANIFEST_SHA256,
                  "metadata_sha256": dict(PINNED_MEMBERS),
                  "sample_identity_sha256": SAMPLE_IDENTITY_SHA256,
                  "dictionary_sha256": DICTIONARY_SHA256, "tzif_sha256": TZIF_SHA256,
                  "gbt_metadata_sha256": GBT_METADATA_SHA256,
                  "encoding_profile": "native", "loss_profile": "squared",
                  "gbt_parameters": expected_parameters,
                  "category_counts_with_unknown": {name: count + 1 for name, count in EXPECTED_CATEGORY_COUNTS.items()},
                  "vector_dimensions": 8}
    return {"identities": identities, "manifest": manifest, "config": config,
            "lineage": lineage, "baselines": baselines, "dictionaries": dictionaries,
            "metadata": metadata}


def _audit_metadata(root: Path, members: Mapping[str, Any], parameters: Mapping[str, Any]) -> dict[str, Any]:
    metadata_names = [_stage_member(members, index) for index in range(4)]
    stage_dirs = {PurePosixPath(name).parts[2] for name in members if name.startswith("pipeline/stages/")}
    if stage_dirs != {PurePosixPath(name).parts[2] for name in metadata_names}:
        raise ValueError("holdout_candidate_unapproved_stage")
    stages = [_read_json(root / name) for name in metadata_names]
    top_names = [name for name in members if len(PurePosixPath(name).parts) == 3
                 and name.startswith("pipeline/metadata/part-") and name.endswith(".txt")]
    if len(top_names) != 1:
        raise ValueError("holdout_candidate_pipeline_metadata_not_unique")
    top = _read_json(root / top_names[0])
    if (top.get("class") != "org.apache.spark.ml.PipelineModel"
            or top.get("paramMap", {}).get("stageUids") != [item.get("uid") for item in stages]
            or any(PurePosixPath(name).parts[2] != f"{index}_{stage.get('uid')}"
                   for index, (name, stage) in enumerate(zip(metadata_names, stages, strict=True)))):
        raise ValueError("holdout_candidate_pipeline_stage_identity_drift")
    for index, (name, column) in enumerate(zip(CATEGORICAL_FEATURE_COLUMNS, model_api.INDEX_COLUMNS, strict=True)):
        if (stages[index].get("class") != "org.apache.spark.ml.feature.StringIndexerModel"
                or stages[index].get("paramMap") != {"handleInvalid": "keep", "inputCol": name,
                                                     "outputCol": column, "stringOrderType": "alphabetAsc"}):
            raise ValueError("holdout_candidate_indexer_contract_drift")
    if (stages[2].get("class") != "org.apache.spark.ml.feature.VectorAssembler"
            or stages[2].get("paramMap") != {"handleInvalid": "error", "inputCols": [*NUMERIC_FEATURE_COLUMNS, *model_api.INDEX_COLUMNS],
                                            "outputCol": "features"}):
        raise ValueError("holdout_candidate_vector_contract_drift")
    if (members[metadata_names[3]]["sha256"] != GBT_METADATA_SHA256
            or stages[3].get("class") != "org.apache.spark.ml.regression.GBTRegressionModel"
            or stages[3].get("paramMap") != parameters
            or stages[3].get("numFeatures") != 8 or stages[3].get("numTrees") != 20):
        raise ValueError("holdout_candidate_effective_parameters_drift")
    return {"stage_count": 4, "gbt_metadata_path": metadata_names[3],
            "effective_parameters": dict(parameters), "num_features": 8, "num_trees": 20}


@contextmanager
def no_fit_guard():
    """Block all evaluation-owned fitting and the standard Spark fit paths.

    No estimator is constructed. ``fit_calls`` is zero on a successful run;
    attempted calls increment both counters and raise before doing any work.
    Monkeypatches are restored even if load/transform fails.
    """
    inherited = _FIT_STATE.get()
    if inherited is not None:
        yield inherited
        return
    from pyspark.ml.base import Estimator
    from pyspark.ml import Pipeline
    from pyspark.ml.wrapper import JavaEstimator
    state = {"fit_calls": 0, "blocked_fit_calls": 0}
    token = _FIT_STATE.set(state)
    original = []

    def blocked(*args, **kwargs):
        state["fit_calls"] += 1
        state["blocked_fit_calls"] += 1
        raise RuntimeError("holdout_fit_or_refit_forbidden")

    targets = [(Estimator, "fit"), (Estimator, "_fit"), (Pipeline, "fit"),
               (Pipeline, "_fit"), (JavaEstimator, "_fit"),
               (model_api, "fit"), (model_api, "fit_baselines")]
    try:
        for owner, name in targets:
            present = name in vars(owner)
            original.append((owner, name, present, vars(owner).get(name)))
            setattr(owner, name, blocked)
        yield state
    finally:
        for owner, name, present, value in reversed(original):
            if present:
                setattr(owner, name, value)
            else:
                delattr(owner, name)
        _FIT_STATE.reset(token)


def validate_loaded_candidate(model: Any, audit: Mapping[str, Any]) -> dict[str, Any]:
    model_api._assert_fitted_pipeline(model)
    if len(model.stages) != 4:
        raise ValueError("holdout_loaded_candidate_stage_drift")
    dictionaries = {name: list(stage.labels) for name, stage in
                    zip(CATEGORICAL_FEATURE_COLUMNS, model.stages[:2], strict=True)}
    if dictionaries != audit["dictionaries"]:
        raise ValueError("holdout_loaded_candidate_dictionary_drift")
    regressor = model.stages[-1]
    effective = {name: regressor.getOrDefault(name) for name in audit["config"]["gbt_parameters"]}
    if effective != audit["config"]["gbt_parameters"] or regressor.getNumTrees != 20:
        raise ValueError("holdout_loaded_candidate_effective_parameters_drift")
    return {"stage_count": 4, "effective_parameters": effective, "vector_dimensions": 8,
            "dictionary_sha256": _digest(dictionaries), "fit_calls": 0}


def load_candidate(root: str | Path, audit: Mapping[str, Any] | None = None, spark: Any = None) -> Any:
    """Audit before PipelineModel.load; never discover a latest/fallback model."""
    verified = audit_candidate(root)
    if audit is not None and any(verified[name] != audit.get(name)
                                 for name in ("identities", "config", "lineage", "baselines", "dictionaries")):
        raise ValueError("holdout_candidate_audit_changed_before_load")
    current_tz = timezone_identity()
    if current_tz.get("timezone") != TIMEZONE_NAME or current_tz.get("tzif_sha256") != TZIF_SHA256:
        raise ValueError("holdout_candidate_runtime_tzif_mismatch")
    with no_fit_guard():
        model = model_api.load_pipeline(Path(root) / "pipeline")
        validate_loaded_candidate(model, verified)
    return model


def validate_nominal_metadata(frame: Any, audit: Mapping[str, Any]) -> dict[str, Any]:
    result = model_api.validate_vector_metadata(frame, encoding_profile="native")
    expected = {name: len(values) + 1 for name, values in audit["dictionaries"].items()}
    if result.get("categorical_nominal_dimensions") != expected:
        raise ValueError("holdout_native_nominal_dictionary_size_drift")
    attrs = frame.schema["features"].metadata["ml_attr"]["attrs"]["nominal"]
    attrs = sorted(attrs, key=lambda item: item["idx"])
    for name, attribute in zip(CATEGORICAL_FEATURE_COLUMNS, attrs, strict=True):
        if attribute["vals"] != [*audit["dictionaries"][name], "__unknown"]:
            raise ValueError("holdout_native_nominal_dictionary_drift")
    return result


def evaluate_predictions(rows: Sequence[Mapping[str, Any]], predictions: Sequence[float],
                         baselines: Mapping[str, Any], *, dictionaries: Mapping[str, Sequence[str]],
                         expected_count: int = EXPECTED_HOLDOUT_COUNT) -> dict[str, Any]:
    """Evaluate immutable predictions once on every row; slices never select a model.

    This function performs no Spark action, fit or model prediction. It can be
    replayed offline against a retained prediction ledger to repair reporting.
    Non-finite rows remain counted and fail the gate instead of being dropped.
    """
    if not rows or len(rows) != len(predictions):
        raise ValueError("holdout_prediction_alignment_or_empty")
    if type(expected_count) is not int or expected_count <= 0:
        raise ValueError("holdout_expected_count_invalid")
    if set(dictionaries) != set(CATEGORICAL_FEATURE_COLUMNS):
        raise ValueError("holdout_dictionary_allowlist_invalid")
    actual = [float(row[model_api.LABEL_COLUMN]) for row in rows]
    values = list(map(float, predictions))
    global_predictions, group_predictions = model_api.predict_baseline(rows, baselines)
    by_model = {"global_median": global_predictions,
                "hierarchical_median": group_predictions, "native_gbt": values}
    overall = {name: model_api.metrics(actual, predicted) for name, predicted in by_model.items()}
    known = {name: set(dictionaries[name]) for name in CATEGORICAL_FEATURE_COLUMNS}
    pu = [row["pickup_zone"] not in known["pickup_zone"] for row in rows]
    do = [row["dropoff_zone"] not in known["dropoff_zone"] for row in rows]
    either = [left or right for left, right in zip(pu, do, strict=True)]
    unseen = {name: {"count": sum(flags), "rate": sum(flags) / len(rows)}
              for name, flags in (("pickup_zone", pu), ("dropoff_zone", do), ("either", either))}
    unseen["seen_both"] = {"count": len(rows) - sum(either), "rate": 1 - sum(either) / len(rows)}

    def summarize(indices: list[int]) -> dict[str, Any]:
        result = {"count": len(indices), "low_sample": len(indices) < LOW_SAMPLE_THRESHOLD,
                  "low_sample_threshold": LOW_SAMPLE_THRESHOLD,
                  "metrics": {name: model_api.metrics([actual[index] for index in indices],
                                                        [predicted[index] for index in indices])
                              for name, predicted in by_model.items()} if indices else {}}
        return result

    hour_bins = [model_api._hour_bin(row) for row in rows]
    slices = {"pickup_hour_6h": {f"{hour:02d}:00-{hour + 6:02d}:00": summarize(
                  [index for index, value in enumerate(hour_bins) if value == hour // 6])
                  for hour in range(0, 24, 6)},
              "category_seen": {"seen_both": summarize([index for index, flag in enumerate(either) if not flag]),
                                "unseen_either": summarize([index for index, flag in enumerate(either) if flag])}}
    fare_bands = (("(0,10]", 0, 10), ("(10,25]", 10, 25), ("(25,50]", 25, 50),
                  ("(50,100]", 50, 100), (">100", 100, math.inf))
    slices["fare_bands_usd"] = {name: summarize([index for index, value in enumerate(actual)
                                                  if math.isfinite(value) and lower < value <= upper])
                                 for name, lower, upper in fare_bands}
    invalid_fare_indices = [index for index, value in enumerate(actual) if not math.isfinite(value) or value <= 0]
    if invalid_fare_indices:
        slices["fare_bands_usd"]["invalid_cohort_fare"] = summarize(invalid_fare_indices)
    finite_all = all(item["metrics_status"] == "finite_all_rows"
                     and item["nonfinite_prediction_count"] == 0
                     and item["nonfinite_label_count"] == 0 for item in overall.values())
    usable = finite_all and all(item["mae_usd"] is not None for item in overall.values())
    best_name = min(("global_median", "hierarchical_median"), key=lambda name: overall[name]["mae_usd"]) if usable else None
    baseline_mae = overall[best_name]["mae_usd"] if best_name else None
    native_mae = overall["native_gbt"]["mae_usd"]
    passed = bool(len(rows) == expected_count and usable and not invalid_fare_indices
                  and native_mae < baseline_mae)
    gate = {"pass": passed, "expected_count": expected_count, "actual_count": len(rows),
            "all_rows_retained": len(rows) == expected_count, "all_predictions_finite": finite_all,
            "best_holdout_baseline": best_name, "best_baseline_mae_usd": baseline_mae,
            "native_mae_usd": native_mae, "comparison": "native_mae_strictly_less_than_best_fitted_baseline_same_rows",
            "mae_improvement_percent": (100 * (baseline_mae - native_mae) / baseline_mae
                                        if usable and baseline_mae > 0 else None),
            "reason": "holdout_gate_passed" if passed else "holdout_count_finiteness_or_mae_gate_failed",
            "clipped_rows": 0, "dropped_rows": 0, "july_numeric_threshold_applied": False}
    return {"row_count": len(rows), "metrics": overall, "unseen": unseen, "slices": slices,
            "gate": gate, "scope": "retained August 2026 sample; zone/time-only; not current-request accuracy",
            "fit_calls": 0}
