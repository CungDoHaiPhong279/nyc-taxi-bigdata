"""One fixed v2 train/validation attempt; PLAN by default, no holdout access.

No acquisition, v1 helper, MLflow/database, production inference or deployment.
Only verified Arrow-joined June/July samples enter Spark. JVM writes are staged
in a bounded private tmpfs, then artifacts are published through the byte quota.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import platform
import shutil
import sys
import time
from typing import Any
from uuid import uuid4

from nyc_taxi.ml.pretrip_budget import ManagedFileBudget, METADATA_RESERVE_BYTES
from nyc_taxi.ml.pretrip_features import FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS, load_contract


ROOT = Path(__file__).resolve().parents[3]
MIB = 1024 * 1024
ENVELOPE_BYTES = 2048 * MIB
SCRATCH_CEILING_BYTES = 512 * MIB
RELOAD_TOLERANCE = 1e-9


def sha256(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(MIB), b""):
            checksum.update(block)
    return checksum.hexdigest()


def private_output(path: Path, root: Path = ROOT) -> Path:
    import stat
    root = root.resolve()
    target = path if path.is_absolute() else root / path
    for ancestor in (target, *target.parents):
        if ancestor.exists() or ancestor.is_symlink():
            info = ancestor.lstat()
            if ancestor.is_symlink() or getattr(info, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400):
                raise ValueError("training_output_symlink_or_junction")
        if ancestor == root:
            break
    target = target.resolve()
    base = root / ".sandbox/pretrip-v2"
    if target == base or not target.is_relative_to(base) or target.exists():
        raise ValueError("training_output_not_new_private_attempt")
    return target


def artifact_budget(prior_task_bytes: int) -> int:
    if type(prior_task_bytes) is not int or prior_task_bytes < 0:
        raise ValueError("explicit_prior_task_bytes_required")
    remaining = ENVELOPE_BYTES - prior_task_bytes - SCRATCH_CEILING_BYTES
    if remaining < METADATA_RESERVE_BYTES:
        raise ValueError("v2_envelope_exhausted_no_implicit_renewal")
    return remaining


def telemetry() -> dict[str, Any]:
    import resource
    result: dict[str, Any] = {"python_peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
    for name in ("memory.peak", "memory.current", "memory.events", "memory.max", "memory.swap.max", "cpu.max", "pids.max"):
        path = Path("/sys/fs/cgroup") / name
        if path.is_file():
            result[name] = path.read_text().strip()
    return result


def evaluate_gate(evaluation: dict[str, Any]) -> dict[str, Any]:
    """Predeclared 10% validation-MAE gate; negative counts reported, not clipped."""
    validation = evaluation["validation"]
    baseline_names = ("global_median", "hierarchical_median")
    finite = all(
        item["metrics_status"] == "finite_all_rows" and item["nonfinite_prediction_count"] == 0
        for split in evaluation.values() for item in split.values()
    )
    usable = all(validation[name]["mae_usd"] is not None for name in (*baseline_names, "gbt"))
    if not finite or not usable:
        return {"pass": False, "reason": "nonfinite_or_undefined_metrics", "required_mae_improvement_percent": 10.0}
    best = min(baseline_names, key=lambda name: validation[name]["mae_usd"])
    baseline, candidate = validation[best]["mae_usd"], validation["gbt"]["mae_usd"]
    improvement = (baseline - candidate) / baseline if baseline > 0 else None
    passed = baseline > 0 and candidate <= baseline * 0.9
    return {"pass": passed, "reason": "validation_mae_gate_passed" if passed else "validation_mae_gate_failed",
            "best_validation_baseline": best, "baseline_mae_usd": baseline, "gbt_mae_usd": candidate,
            "mae_improvement_percent": 100 * improvement if improvement is not None else None,
            "required_mae_improvement_percent": 10.0}


def claim_scratch(path: Path) -> Path:
    """Claim one new private child; never clean a pre-existing directory."""
    target = path.absolute()
    if ".." in target.parts or target == Path("/tmp") or not target.is_relative_to(Path("/tmp")):
        raise ValueError("training_scratch_must_be_private_tmp_child")
    if any(parent.is_symlink() for parent in (target, *target.parents)):
        raise ValueError("training_scratch_symlink_rejected")
    if target.exists():
        raise FileExistsError("training_scratch_already_exists")
    # Only the final directory is ours; do not create/clean unrelated parents.
    target.mkdir(exist_ok=False)
    return target


def create_spark(scratch: Path):
    from pyspark.sql import SparkSession
    # A tmpfs hard ceiling bounds JVM shuffle/model staging, which Python file
    # sinks cannot intercept. Reserve the entire ceiling in the disk envelope.
    mountinfo = Path("/proc/self/mountinfo").read_text()
    if not any(" /tmp " in line and " - tmpfs " in line for line in mountinfo.splitlines()):
        raise ValueError("training_requires_private_bounded_tmpfs")
    import os
    space = os.statvfs("/tmp")
    if space.f_blocks * space.f_frsize > SCRATCH_CEILING_BYTES:
        raise ValueError("training_tmpfs_exceeds_reserved_ceiling")
    builder = (SparkSession.builder.appName("NYC-Taxi-Pretrip-V2-Fixed-Pilot")
               .master("local[2]").config("spark.driver.memory", "1536m")
               .config("spark.ui.enabled", "false").config("spark.eventLog.enabled", "false")
               .config("spark.driver.host", "127.0.0.1").config("spark.driver.bindAddress", "127.0.0.1")
               .config("spark.sql.session.timeZone", "America/New_York")
               .config("spark.sql.shuffle.partitions", "2").config("spark.default.parallelism", "2")
               .config("spark.local.dir", str(scratch / "spark"))
               .config("spark.sql.warehouse.dir", str(scratch / "warehouse")))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    heap = int(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory())
    if heap != 1536 * MIB:
        spark.stop()
        raise ValueError("unexpected_jvm_heap_not_1536_mib")
    return spark


def spark_frame(spark, rows: list[dict[str, Any]], *, labels: bool):
    """Rows were paired in Arrow first; no separate Spark frames are joined."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    fields = [StructField(name, DoubleType(), False) for name in NUMERIC_FEATURE_COLUMNS]
    fields += [StructField(name, StringType(), False) for name in FEATURE_COLUMNS[len(NUMERIC_FEATURE_COLUMNS):]]
    extra = ("fare_amount", "journey_key") if labels else ("parity_id",)
    fields += ([StructField("fare_amount", DoubleType(), False), StructField("journey_key", StringType(), False)]
               if labels else [StructField("parity_id", StringType(), False)])
    names = [*FEATURE_COLUMNS, *extra]
    return spark.createDataFrame([tuple(row[name] for name in names) for row in rows], StructType(fields))


def predictions(model, frame, order_column: str) -> list[tuple[str, float]]:
    return [(row[order_column], float(row["prediction"]))
            for row in model.transform(frame).select(order_column, "prediction").orderBy(order_column).collect()]


def write_document(budget: ManagedFileBudget, path: Path, value: Any) -> None:
    # Large baseline/manifest JSON are artifact data, not the protected metadata
    # pool. Fully encode and pre-check the write through the same guarded sink.
    payload = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode("utf-8")
    with budget.open_file(path) as stream:
        stream.write(payload)


def publish_pipeline(budget: ManagedFileBudget, staged: Path, destination: Path) -> list[dict[str, Any]]:
    destination.mkdir(exist_ok=False)
    entries = []
    for source in sorted(staged.rglob("*")):
        if source.is_symlink():
            raise ValueError("model_artifact_symlink_rejected")
        if not source.is_file():
            continue
        relative = source.relative_to(staged)
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as incoming, budget.open_file(target) as outgoing:
            for block in iter(lambda: incoming.read(MIB), b""):
                outgoing.write(block)
        entries.append({"path": "pipeline/" + relative.as_posix(), "bytes": target.stat().st_size,
                        "sha256": sha256(target)})
    if not entries:
        raise ValueError("empty_fitted_pipeline")
    return entries


def verify_bundle(path: Path) -> dict[str, Any]:
    manifest = json.loads((path / "artifact_manifest.json").read_text())
    if manifest.get("version") != "pretrip-v2-training-artifacts-1":
        raise ValueError("unsupported_training_artifact_manifest")
    seen = set()
    pipeline_entries = []
    for entry in manifest["files"]:
        relative = Path(entry["path"])
        raw = path / relative
        candidate = raw.resolve()
        if (relative.is_absolute() or ".." in relative.parts or relative.as_posix() in seen
                or not candidate.is_relative_to(path.resolve())
                or any(parent.is_symlink() for parent in (raw, *raw.parents))):
            raise ValueError("artifact_manifest_path_escape")
        seen.add(relative.as_posix())
        if candidate.stat().st_size != entry["bytes"] or sha256(candidate) != entry["sha256"]:
            raise ValueError("artifact_checksum_mismatch")
        if relative.parts[0] == "pipeline":
            pipeline_entries.append(entry)
    required = {"baselines.json", "parity_inputs.json", "parity_reference.json", "parity_baseline_reference.json",
                "training_config.json", "sample_lineage.json", "metrics.json",
                "pretrip_v2_contract.json", "pretrip_v2_calendar.json"}
    tree_hash = hashlib.sha256(json.dumps(pipeline_entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if not required.issubset(seen) or not pipeline_entries or tree_hash != manifest.get("model_tree_sha256"):
        raise ValueError("artifact_manifest_incomplete_or_model_identity_mismatch")
    return manifest


def profile_from_saved_config(config: dict[str, Any]) -> str:
    """Infer legacy squared/new absolute explicitly, never change a saved model."""
    from nyc_taxi.ml import pretrip_training_model as model_api
    parameters = config.get("gbt_parameters", {})
    profile = parameters.get("lossType")
    if profile not in ("squared", "absolute") or parameters != model_api.gbt_parameters(profile):
        raise ValueError("saved_gbt_configuration_drift")
    if config.get("loss_profile", profile) != profile:
        raise ValueError("saved_loss_profile_mismatch")
    if (config.get("features", list(FEATURE_COLUMNS)) != list(FEATURE_COLUMNS)
            or config.get("baseline_policy", model_api.BASELINE_POLICY) != model_api.BASELINE_POLICY):
        raise ValueError("saved_feature_or_baseline_configuration_drift")
    return profile


def saved_loss_metadata(root: Path, entries: list[dict[str, Any]], expected: str) -> dict[str, Any]:
    """Read the saved effective paramMap, not just a library default or CLI flag."""
    selected = [entry for entry in entries if len(Path(entry["path"]).parts) == 5
                and Path(entry["path"]).parts[:2] == ("pipeline", "stages")
                and Path(entry["path"]).parts[2].startswith("4_")
                and Path(entry["path"]).parts[3] == "metadata"
                and Path(entry["path"]).name.startswith("part-")]
    if len(selected) != 1:
        raise ValueError("saved_gbt_metadata_not_unique")
    entry = selected[0]
    path = root / entry["path"]
    if sha256(path) != entry["sha256"]:
        raise ValueError("saved_gbt_metadata_checksum_mismatch")
    metadata = json.loads(path.read_text())
    if (metadata.get("class") != "org.apache.spark.ml.regression.GBTRegressionModel"
            or metadata.get("paramMap", {}).get("lossType") != expected):
        raise ValueError("saved_effective_loss_drift")
    return {"path": entry["path"], "sha256": entry["sha256"],
            "paramMap_lossType": metadata["paramMap"]["lossType"],
            "defaultParamMap_lossType": metadata.get("defaultParamMap", {}).get("lossType")}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--execute", action="store_true")
    result.add_argument("--sample-dir", type=Path, default=Path(".sandbox/pretrip-v2/pilot-2026-06-08"))
    result.add_argument("--output-dir", type=Path, default=Path(f".sandbox/pretrip-v2/training-{uuid4().hex}/artifacts"))
    result.add_argument("--prior-task-bytes", type=int, help="Existing V2 logical bytes, already charged to the 2048 MiB envelope")
    result.add_argument("--scratch-dir", type=Path, default=Path("/tmp/pretrip-v2-attempt"))
    result.add_argument("--code-commit", default="uncommitted")
    result.add_argument("--runtime-image-id", default="not-recorded")
    result.add_argument("--reload-artifacts", type=Path, help="Fresh worker reload only; never fit or read samples")
    result.add_argument("--loss-profile", choices=("squared", "absolute"), default="squared",
                        help="Explicit loss-only experiment; default/legacy training remains squared")
    result.add_argument("--reference-artifacts", type=Path,
                        help="Required for absolute training: pinned squared V2 artifacts mounted read-only")
    return result


def run(arguments: argparse.Namespace, *, project_root: Path = ROOT) -> dict[str, Any]:
    from nyc_taxi.ml import pretrip_training_model as model_api
    root = project_root.resolve()
    output = private_output(arguments.output_dir, root)
    parameters = model_api.gbt_parameters(arguments.loss_profile)
    plan = {"status": "PLAN_NOT_EXECUTED", "mode": "reload_only" if arguments.reload_artifacts else "train_validation",
            "features": list(FEATURE_COLUMNS), "loss_profile": arguments.loss_profile, "gbt_parameters": parameters,
            "baseline_policy": dict(model_api.BASELINE_POLICY), "sample_splits": ["train", "validation"],
            "holdout_access": False, "raw_source_access": False, "default_execute": False,
            "envelope_bytes": ENVELOPE_BYTES, "prior_task_bytes": arguments.prior_task_bytes,
            "scratch_ceiling_reserved_bytes": SCRATCH_CEILING_BYTES,
            "output": str(output), "gate_required_mae_improvement_percent": 10.0}
    load_contract()
    if not arguments.execute:
        return plan
    if arguments.loss_profile == "absolute" and not arguments.reload_artifacts and not arguments.reference_artifacts:
        raise ValueError("absolute_profile_requires_explicit_reference_artifacts")
    limit = artifact_budget(arguments.prior_task_bytes)
    budget = ManagedFileBudget((), output, limit)
    output.mkdir(parents=True, exist_ok=False)
    report: dict[str, Any] = {**plan, "status": "INCOMPLETE", "phases": {}, "fit_calls": 0,
                              "promoted": False, "deployed": False}
    checkpoint = output / "training_checkpoint.json"
    budget.write_json(checkpoint, report, exclusive=True)
    spark = None
    owned_scratch = None
    started = time.monotonic()

    @contextmanager
    def phase(name):
        phase_start = time.monotonic()
        report["phase"] = name
        budget.write_json(checkpoint, report)
        print(json.dumps({"phase": name, "state": "START"}), file=sys.stderr, flush=True)
        try:
            yield
        finally:
            report["phases"][name] = {"elapsed_seconds": round(time.monotonic() - phase_start, 3)}
            print(json.dumps({"phase": name, "state": "END", **report["phases"][name]}), file=sys.stderr, flush=True)

    try:
        if arguments.reload_artifacts:
            bundle = arguments.reload_artifacts
            bundle = bundle if bundle.is_absolute() else root / bundle
            with phase("verify_explicit_bundle"):
                manifest = verify_bundle(bundle)
                stored_config = json.loads((bundle / "training_config.json").read_text())
                stored_profile = profile_from_saved_config(stored_config)
                report.update(loss_profile=stored_profile, gbt_parameters=model_api.gbt_parameters(stored_profile))
                report["saved_loss_metadata"] = saved_loss_metadata(bundle, manifest["files"], stored_profile)
                reference = json.loads((bundle / "parity_reference.json").read_text())
                inputs = json.loads((bundle / "parity_inputs.json").read_text())
                baseline = json.loads((bundle / "baselines.json").read_text())
            with phase("spark_start"):
                owned_scratch = claim_scratch(arguments.scratch_dir)
                spark = create_spark(owned_scratch)
            with phase("fresh_worker_reload"):
                reloaded = model_api.load_pipeline(bundle / "pipeline")
                actual = predictions(reloaded, spark_frame(spark, inputs, labels=False), "parity_id")
                expected = [(row["parity_id"], row["prediction"]) for row in reference]
                if [row[0] for row in actual] != [row[0] for row in expected]:
                    raise ValueError("reload_input_order_changed")
                differences = [abs(a[1] - b[1]) for a, b in zip(actual, expected, strict=True)]
                if not differences or not all(math.isfinite(x) for x in differences) or max(differences) > RELOAD_TOLERANCE:
                    raise ValueError("fresh_worker_prediction_parity_failed")
                saved_baselines = json.loads((bundle / "parity_baseline_reference.json").read_text())
                if model_api.predict_baseline(inputs, baseline) != (saved_baselines["global"], saved_baselines["hierarchical"]):
                    raise ValueError("reloaded_baseline_predictions_changed")
                report.update(status="FRESH_RELOAD_VERIFIED", fit_calls=0,
                              model_tree_sha256=manifest["model_tree_sha256"],
                              reload={"count": len(differences), "max_absolute_difference": max(differences),
                                      "absolute_tolerance": RELOAD_TOLERANCE, "baseline_reload": True})
        else:
            from nyc_taxi.ml.pretrip_training_data import load_training_samples
            sample = arguments.sample_dir if arguments.sample_dir.is_absolute() else root / arguments.sample_dir
            with phase("verified_arrow_join"):
                loaded = load_training_samples(sample)
                rows = {name: loaded[name] for name in ("train", "validation")}
                report["counts"] = {name: len(values) for name, values in rows.items()}
                write_document(budget, output / "sample_lineage.json", loaded["lineage"])
            with phase("train_only_baselines"):
                baseline = model_api.fit_baselines(rows["train"])
                write_document(budget, output / "baselines.json", baseline)
            reference = None
            if arguments.loss_profile == "absolute":
                from nyc_taxi.ml import pretrip_training_reference as reference_api
                with phase("verify_pinned_squared_reference"):
                    reference_path = arguments.reference_artifacts
                    reference_path = reference_path if reference_path.is_absolute() else root / reference_path
                    reference_manifest = verify_bundle(reference_path)
                    reference = reference_api.read_reference(reference_path, reference_manifest)
                    report["v2_reference"] = reference_api.verify_current(reference, loaded, baseline)
            with phase("spark_start"):
                owned_scratch = claim_scratch(arguments.scratch_dir)
                spark = create_spark(owned_scratch)
                report["runtime"] = {"python": platform.python_version(), "spark": spark.version,
                    "java": str(spark.sparkContext._jvm.java.lang.System.getProperty("java.version")),
                    "java_active_processors": int(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().availableProcessors()),
                    "jvm_max_heap_bytes": int(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()),
                    "spark_master": spark.sparkContext.master, "image_id": arguments.runtime_image_id,
                    "container_cpu_limit": 2, "container_memory_limit_mib": 3072,
                    "tmpfs_ceiling_bytes": SCRATCH_CEILING_BYTES}
                if reference and report["runtime"] != reference["runtime"]:
                    raise ValueError("v21_reference_runtime_identity_drift")
                frames = {name: spark_frame(spark, values, labels=True).cache() for name, values in rows.items()}
                for name, frame in frames.items():
                    if frame.count() != len(rows[name]):
                        raise ValueError("joined_spark_row_count_changed")
            with phase("freeze_configuration_before_fit"):
                sources = {path.name: sha256(path) for path in Path(__file__).parent.glob("pretrip_*.py")}
                config = {"loss_profile": arguments.loss_profile, "gbt_parameters": parameters,
                          "baseline_policy": dict(model_api.BASELINE_POLICY), "features": list(FEATURE_COLUMNS),
                          "code_commit_base": arguments.code_commit, "code_sha256": sources,
                          "runtime": report["runtime"], "holdout_opened": False,
                          "sample_identity_sha256": loaded["lineage"]["sample_identity_sha256"],
                          "gate_policy": {"required_mae_improvement_percent": 10.0,
                                          "reference_baseline_mae_usd": 5.1312024993624075 if reference else None,
                                          "reference_gbt_max_mae_usd": 4.618082249426167 if reference else None},
                          "v2_reference": report.get("v2_reference")}
                write_document(budget, output / "training_config.json", config)
                report["prefit_configuration_sha256"] = sha256(output / "training_config.json")
            with phase("fixed_gbt_fit_once"):
                report["fit_calls"] = 1
                fitted = model_api.fit(frames["train"], loss_profile=arguments.loss_profile)
                actual_parameters = {name: fitted.stages[-1].getOrDefault(name) for name in model_api.GBT_PARAMETERS}
                if actual_parameters != parameters:
                    raise ValueError("fitted_gbt_configuration_drift")
                report["gbt_parameters"] = actual_parameters
                report["gbt_tree_count"] = len(fitted.stages[-1].trees)
                report["vector_metadata"] = model_api.validate_vector_metadata(fitted.transform(frames["train"]))
                if reference:
                    report["dictionary_verification"] = reference_api.verify_dictionaries(reference, {
                        name: list(stage.labels) for name, stage in zip(
                            ("pickup_zone", "dropoff_zone"), fitted.stages[:2], strict=True)})
            evaluation = {}
            with phase("train_validation_metrics"):
                for name, values in rows.items():
                    ordered = predictions(fitted, frames[name], "journey_key")
                    if [key for key, _ in ordered] != [row["journey_key"] for row in values]:
                        raise ValueError("evaluation_row_alignment_changed")
                    global_predictions, grouped_predictions = model_api.predict_baseline(values, baseline)
                    labels = [row["fare_amount"] for row in values]
                    evaluation[name] = {"global_median": model_api.metrics(labels, global_predictions),
                                        "hierarchical_median": model_api.metrics(labels, grouped_predictions),
                                        "gbt": model_api.metrics(labels, [prediction for _, prediction in ordered])}
                report["metrics"] = evaluation
                report["gate"] = evaluate_gate(evaluation)
                report["unseen_categories"] = {name: model_api.fitted_unknown_counts(fitted, frame) for name, frame in frames.items()}
                if reference:
                    for name in rows:
                        for baseline_name in ("global_median", "hierarchical_median"):
                            if evaluation[name][baseline_name] != reference["metrics"][name][baseline_name]:
                                raise ValueError("v2_baseline_evaluation_drift")
                    report["reference_squared_metrics"] = reference["metrics"]
            with phase("save_fixed_candidate"):
                parity_inputs = [{**{key: row[key] for key in FEATURE_COLUMNS}, "parity_id": f"{name}-{index:04d}"}
                                 for name, values in rows.items() for index, row in enumerate(values[:8])]
                parity_predictions = predictions(fitted, spark_frame(spark, parity_inputs, labels=False), "parity_id")
                write_document(budget, output / "parity_inputs.json", parity_inputs)
                write_document(budget, output / "parity_reference.json",
                               [{"parity_id": key, "prediction": value} for key, value in parity_predictions])
                global_reference, group_reference = model_api.predict_baseline(parity_inputs, baseline)
                write_document(budget, output / "parity_baseline_reference.json",
                               {"global": global_reference, "hierarchical": group_reference})
                staging = arguments.scratch_dir / "fitted-pipeline"
                model_api.save_pipeline(fitted, staging)
                entries = publish_pipeline(budget, staging, output / "pipeline")
                report["saved_loss_metadata"] = saved_loss_metadata(output, entries, arguments.loss_profile)
                tree_hash = hashlib.sha256(json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                report["model_tree_sha256"] = tree_hash
                if sha256(output / "training_config.json") != report["prefit_configuration_sha256"]:
                    raise ValueError("frozen_prefit_configuration_changed")
                for name in ("pretrip_v2_contract.json", "pretrip_v2_calendar.json"):
                    source = root / "config" / name
                    with budget.open_file(output / name) as stream:
                        stream.write(source.read_bytes())
                write_document(budget, output / "metrics.json", evaluation)
                for name in ("sample_lineage.json", "baselines.json", "parity_inputs.json", "parity_reference.json",
                             "parity_baseline_reference.json", "training_config.json", "pretrip_v2_contract.json",
                             "pretrip_v2_calendar.json", "metrics.json"):
                    path = output / name
                    entries.append({"path": name, "bytes": path.stat().st_size, "sha256": sha256(path)})
                write_document(budget, output / "artifact_manifest.json", {
                    "version": "pretrip-v2-training-artifacts-1", "model_tree_sha256": tree_hash,
                    "files": entries, "code_commit_base": arguments.code_commit,
                    "holdout_opened": False, "promoted": False, "deployed": False})
                for frame in frames.values():
                    frame.unpersist()
                report.update(status="TRAINED_NOT_DEPLOYED", reload_status="PENDING_FRESH_WORKER")
        if spark is not None:
            spark.stop()
            spark = None
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        report["telemetry"] = telemetry()
        report["envelope"] = {"limit_bytes": ENVELOPE_BYTES, "prior_task_bytes": arguments.prior_task_bytes,
                              "scratch_reserved_ceiling_bytes": SCRATCH_CEILING_BYTES,
                              "persistent_quota_bytes": limit,
                              "scope": "managed V2 logical files plus conservative private scratch ceiling, not host C/VHDX/pagefile"}
        budget.write_json(output / "training_report.json", report, exclusive=True)
        budget.write_json(checkpoint, report)
        return report
    except Exception as error:
        report.update(status="FAILED", error_type=type(error).__name__, elapsed_seconds=round(time.monotonic()-started, 3))
        report["telemetry"] = telemetry()
        try:
            budget.write_json(checkpoint, report)
        except Exception:
            pass
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            # Only the freshly created private scratch belonging to this call.
            if owned_scratch is not None:
                shutil.rmtree(owned_scratch)


def main(argv: list[str] | None = None) -> int:
    try:
        report = run(parser().parse_args(argv))
        print(json.dumps(report, allow_nan=False))
        return 2 if report.get("gate", {}).get("pass") is False else 0
    except Exception as error:
        print(json.dumps({"status": "FAILED", "error_type": type(error).__name__,
                          "code": "pretrip_training_failed_see_private_checkpoint"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
