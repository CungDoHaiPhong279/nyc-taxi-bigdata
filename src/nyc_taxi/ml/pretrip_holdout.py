"""PLAN-first, frozen native V2.2 evaluation of the August retained sample.

No training, network, raw data, June/July samples or OHE model access. The
immutable evaluation config is published before either holdout Parquet opens.
One Spark prediction collection is persisted before metrics/report generation.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import platform
import shutil
import time
from typing import Any

from nyc_taxi.ml import pretrip_holdout_data as data_api
from nyc_taxi.ml import pretrip_holdout_model as model_api
from nyc_taxi.ml import pretrip_training_model as training_model
from nyc_taxi.ml.pretrip_budget import ManagedFileBudget
from nyc_taxi.ml.pretrip_features import FEATURE_COLUMNS, NUMERIC_FEATURE_COLUMNS
from nyc_taxi.ml.pretrip_training import (
    ENVELOPE_BYTES, ROOT, SCRATCH_CEILING_BYTES, artifact_budget, claim_scratch,
    create_spark, private_output, sha256, telemetry, write_document,
)


EXPECTED_COUNT = 21_833
GATE_POLICY = {
    "version": "pretrip-v22-august-frozen-1", "expected_rows": EXPECTED_COUNT,
    "finite_prediction_fraction": 1.0, "keep_all_rows": True,
    "clip_target_or_prediction": False, "drop_rows": False,
    "mae_comparison": "native MAE strictly below minimum of fitted baseline MAEs on the same August rows",
    "validation_numeric_threshold_applies": False, "tune_after_holdout": False,
    "slices": {"hour_bins": ["00-06", "06-12", "12-18", "18-24"],
               "categories": ["seen", "unseen"],
               "fare_bands": ["(0,10]", "(10,25]", "(25,50]", "(50,100]", ">100"],
               "low_sample_below": 30},
}


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--execute", action="store_true", help="Explicitly open and evaluate the locked August sample once")
    result.add_argument("--sample-dir", type=Path, default=Path(".sandbox/pretrip-v2/pilot-2026-06-08"))
    result.add_argument("--candidate-artifacts", type=Path,
                        default=Path(".sandbox/pretrip-v2/training-20261027-native-01/artifacts"))
    result.add_argument("--output-dir", type=Path, default=Path(".sandbox/pretrip-v2/holdout-20261027-native-01"))
    result.add_argument("--scratch-dir", type=Path, default=Path("/tmp/pretrip-v22-august"))
    result.add_argument("--prior-task-bytes", type=int)
    result.add_argument("--runtime-image-id", default="")
    result.add_argument("--code-commit", default="")
    return result


def freeze_evaluation_config(budget: ManagedFileBudget, output: Path, document: dict[str, Any]) -> str:
    """Write a new immutable config; never infer a gate from holdout outcomes."""
    path = output / "evaluation_config.json"
    write_document(budget, path, document)
    return sha256(path)


def _runtime_identity(spark: Any, arguments: argparse.Namespace, audit: dict[str, Any]) -> dict[str, Any]:
    expected = audit["config"]["runtime"]
    actual = {
        "python": platform.python_version(), "spark": spark.version,
        "java": str(spark.sparkContext._jvm.java.lang.System.getProperty("java.version")),
        "image_id": arguments.runtime_image_id,
        "jvm_max_heap_bytes": int(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().maxMemory()),
        "java_active_processors": int(spark.sparkContext._jvm.java.lang.Runtime.getRuntime().availableProcessors()),
        "spark_master": spark.sparkContext.master,
    }
    if any(actual[name] != expected[name] for name in actual):
        raise ValueError("holdout_runtime_differs_from_frozen_candidate")
    limits = telemetry()
    for name, value in {"memory.max": "3221225472", "memory.swap.max": "0",
                        "cpu.max": "200000 100000", "pids.max": "512"}.items():
        if limits.get(name) != value:
            raise ValueError("holdout_worker_limits_differ_from_verified_configuration")
    return actual


def _holdout_frame(spark: Any, rows: list[dict[str, Any]]):
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    # Arrow-paired rows enter one frame, with the key retained only for output
    # alignment. Neither labels nor dates enter the fitted model's input frame.
    fields = [StructField(name, DoubleType(), False) for name in NUMERIC_FEATURE_COLUMNS]
    fields += [StructField(name, StringType(), False) for name in FEATURE_COLUMNS[6:]]
    fields.append(StructField("journey_key", StringType(), False))
    names = [*FEATURE_COLUMNS, "journey_key"]
    return spark.createDataFrame([tuple(row[name] for name in names) for row in rows], StructType(fields))


def _collect_predictions_once(model: Any, spark: Any, rows: list[dict[str, Any]],
                              audit: dict[str, Any]) -> tuple[list[tuple[str, float]], dict[str, Any]]:
    frame = _holdout_frame(spark, rows)
    transformed = model.transform(frame)
    vector = model_api.validate_nominal_metadata(transformed, audit)
    # Schema validation above is lazy; this is the only action executing the
    # model on holdout. No count/unknown/slice action can trigger predictions.
    collected = transformed.select("journey_key", "prediction").collect()
    return [(item["journey_key"], float(item["prediction"]) if item["prediction"] is not None
             else math.nan) for item in collected], vector


def _write_parquet(budget: ManagedFileBudget, path: Path, columns: dict[str, list[Any]]) -> dict[str, Any]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    with budget.open_file(path) as sink:
        pq.write_table(pa.table(columns), sink, compression="snappy")
    return {"path": path.name, "bytes": path.stat().st_size, "sha256": sha256(path)}


def _align_predictions(rows: list[dict[str, Any]], collected: list[tuple[str, float]]) -> list[float]:
    expected = [row["journey_key"] for row in rows]
    actual = [key for key, _ in collected]
    if len(actual) != len(expected) or len(set(actual)) != len(actual) or set(actual) != set(expected):
        raise ValueError("holdout_prediction_key_alignment_mismatch")
    lookup = dict(collected)
    return [lookup[key] for key in expected]


def run(arguments: argparse.Namespace, *, project_root: Path = ROOT) -> dict[str, Any]:
    plan = {"status": "PLAN_NOT_EXECUTED", "default_execute": False,
            "sample_splits": ["test"], "expected_rows": EXPECTED_COUNT,
            "candidate": str(arguments.candidate_artifacts), "sample_dir": str(arguments.sample_dir),
            "output": str(arguments.output_dir), "gate_policy": GATE_POLICY,
            "fit_calls": 0, "prediction_calls": 0, "holdout_data_accessed": False,
            "raw_sources_accessed": False, "promoted": False, "deployed": False,
            "envelope_bytes": ENVELOPE_BYTES, "prior_task_bytes": arguments.prior_task_bytes,
            "scratch_reserved_ceiling_bytes": SCRATCH_CEILING_BYTES}
    if not arguments.execute:
        return plan
    root = project_root.resolve()
    output = private_output(arguments.output_dir, root)
    limit = artifact_budget(arguments.prior_task_bytes)
    sample = arguments.sample_dir if arguments.sample_dir.is_absolute() else root / arguments.sample_dir
    candidate = arguments.candidate_artifacts if arguments.candidate_artifacts.is_absolute() else root / arguments.candidate_artifacts
    output.mkdir(parents=True, exist_ok=False)
    budget = ManagedFileBudget((), output, limit_bytes=limit)
    started = time.monotonic()
    report = {**plan, "status": "STARTED", "phases": {},
              "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "output": str(output), "code_commit_base": arguments.code_commit}
    checkpoint = output / "evaluation_checkpoint.json"
    spark = scratch = None
    fit_state = {"fit_calls": 0, "blocked_fit_calls": 0}

    def save_checkpoint() -> None:
        summary = {key: report[key] for key in (
            "status", "phase", "fit_calls", "prediction_calls", "holdout_data_accessed",
            "raw_sources_accessed", "promoted", "deployed", "code_commit_base",
            "evaluation_config_sha256", "error_type", "error_code", "elapsed_seconds",
            "prediction_capture", "prediction_rows", "gate", "report_artifact",
        ) if key in report}
        summary.update(fit_calls=fit_state["fit_calls"], blocked_fit_calls=fit_state["blocked_fit_calls"])
        budget.write_json(checkpoint, summary)

    @contextmanager
    def phase(name: str):
        report["phase"] = name
        save_checkpoint()
        print(json.dumps({"phase": name, "state": "START"}), flush=True)
        phase_started = time.monotonic()
        yield
        elapsed = round(time.monotonic() - phase_started, 3)
        report["phases"][name] = {"elapsed_seconds": elapsed}
        print(json.dumps({"phase": name, "state": "END", "elapsed_seconds": elapsed}), flush=True)

    try:
        with phase("audit_frozen_candidate_and_preparation_metadata"):
            audit = model_api.audit_candidate(candidate)
            metadata = data_api.verify_holdout_metadata(sample, candidate_lineage=audit["lineage"],
                                                        config_dir=root / "config")
        with model_api.no_fit_guard() as fit_state:
            with phase("load_frozen_pipeline_without_holdout"):
                scratch = claim_scratch(arguments.scratch_dir)
                spark = create_spark(scratch)
                runtime = _runtime_identity(spark, arguments, audit)
                model = model_api.load_candidate(candidate, audit=audit, spark=spark)
            with phase("freeze_evaluation_config_before_holdout_read"):
                code = {path.name: sha256(path) for path in (
                    Path(__file__), Path(data_api.__file__), Path(model_api.__file__),
                    Path(training_model.__file__), root / "scripts/evaluate_pretrip_holdout.py",
                )}
                frozen = {"version": "pretrip-v22-holdout-evaluation-1",
                          "candidate_identities": audit["identities"],
                          "preparation": metadata, "expected_rows": EXPECTED_COUNT,
                          "features": list(FEATURE_COLUMNS), "encoding": "native", "runtime": runtime,
                          "gbt_parameters": audit["config"]["gbt_parameters"],
                          "baseline_policy": audit["config"]["baseline_policy"],
                          "gate_policy": GATE_POLICY, "code_commit_base": arguments.code_commit,
                          "code_sha256": code, "fit_calls_permitted": 0,
                          "model_prediction_collections_permitted": 1,
                          "envelope_bytes": ENVELOPE_BYTES,
                          "prior_task_bytes": arguments.prior_task_bytes,
                          "scratch_reserved_ceiling_bytes": SCRATCH_CEILING_BYTES}
                report["evaluation_config_sha256"] = freeze_evaluation_config(budget, output, frozen)
                report["candidate_identities"] = audit["identities"]
                report["runtime"] = runtime
            with phase("verify_august_arrow_alignment_and_lineage"):
                report["holdout_data_accessed"] = True
                loaded = data_api.load_holdout_samples(sample, candidate_lineage=audit["lineage"],
                                                       config_dir=root / "config", verified_metadata=metadata)
                rows = loaded["test"]
                if len(rows) != EXPECTED_COUNT:
                    raise ValueError("holdout_locked_row_count_mismatch")
                report["lineage"] = loaded["lineage"]
                write_document(budget, output / "holdout_lineage.json", loaded["lineage"])
            with phase("predict_once_and_persist_capture"):
                report["prediction_calls"] = 1
                save_checkpoint()  # Never silently resume/retry after this boundary.
                collected, vector = _collect_predictions_once(model, spark, rows, audit)
                # Persist the raw keyed model outputs before metrics, alignment
                # reports or baseline arithmetic. A later failure needs no predict.
                report["prediction_capture"] = _write_parquet(budget, output / "native_predictions.parquet", {
                    "journey_key": [item[0] for item in collected],
                    "prediction": [item[1] for item in collected]})
                save_checkpoint()
                native = _align_predictions(rows, collected)
                global_values, group_values = training_model.predict_baseline(rows, audit["baselines"])
                columns = {name: [row[name] for row in rows]
                           for name in (*FEATURE_COLUMNS, "journey_key", "pickup_datetime_local", "fare_amount")}
                columns.update(native_gbt_prediction=native, global_median_prediction=global_values,
                               hierarchical_median_prediction=group_values)
                report["prediction_rows"] = _write_parquet(budget, output / "evaluation_rows.parquet", columns)
                report["vector_metadata"] = vector
                save_checkpoint()
            with phase("metrics_and_predeclared_slices_offline"):
                evaluation = model_api.evaluate_predictions(rows, native, audit["baselines"],
                                                              dictionaries=audit["dictionaries"])
                report.update(metrics=evaluation["metrics"], gate=evaluation["gate"],
                              unseen=evaluation["unseen"], row_count=evaluation["row_count"])
                write_document(budget, output / "slices.json", evaluation["slices"])
                report["slices_artifact"] = {"path": "slices.json", "sha256": sha256(output / "slices.json")}
                if sha256(output / "evaluation_config.json") != report["evaluation_config_sha256"]:
                    raise ValueError("frozen_holdout_evaluation_config_changed")
                if fit_state["fit_calls"] or fit_state["blocked_fit_calls"]:
                    raise ValueError("holdout_fit_or_refit_was_attempted")
        if spark is not None:
            spark.stop()
            spark = None
        report.update(status="EVALUATED_NOT_DEPLOYED", fit_calls=fit_state["fit_calls"],
                      blocked_fit_calls=fit_state["blocked_fit_calls"],
                      elapsed_seconds=round(time.monotonic() - started, 3), telemetry=telemetry(),
                      envelope={"limit_bytes": ENVELOPE_BYTES, "prior_task_bytes": arguments.prior_task_bytes,
                                "scratch_reserved_ceiling_bytes": SCRATCH_CEILING_BYTES,
                                "persistent_quota_bytes": limit}, disk_budget=budget.snapshot())
        write_document(budget, output / "evaluation_report.json", report)
        report["report_artifact"] = {"path": "evaluation_report.json", "sha256": sha256(output / "evaluation_report.json")}
        save_checkpoint()
        return report
    except Exception as error:
        report.update(status="FAILED", error_type=type(error).__name__,
                      error_code="holdout_failed_no_automatic_retry_see_checkpoint",
                      elapsed_seconds=round(time.monotonic() - started, 3), telemetry=telemetry())
        try:
            save_checkpoint()
        except Exception:
            pass
        raise
    finally:
        try:
            if spark is not None:
                spark.stop()
        finally:
            if scratch is not None:
                shutil.rmtree(scratch)


def main(argv: list[str] | None = None) -> int:
    try:
        arguments = parser().parse_args(argv)
        report = run(arguments)
        summary = report if not arguments.execute else {key: value for key, value in report.items()
                          if key in {"status", "expected_rows", "row_count", "metrics", "gate", "unseen",
                                     "fit_calls", "prediction_calls", "output", "elapsed_seconds", "telemetry"}}
        print(json.dumps(summary, allow_nan=False), flush=True)
        return 2 if report.get("gate", {}).get("pass") is False else 0
    except Exception as error:
        print(json.dumps({"status": "FAILED", "error_type": type(error).__name__,
                          "code": "holdout_failed_no_automatic_retry_see_checkpoint"}), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
