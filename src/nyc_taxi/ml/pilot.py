"""Explicit, fixed-budget ML P2 pilot. June has no prediction/evaluation path."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import math
import os
from pathlib import Path
import resource
import signal
import sys
import time

from nyc_taxi.ml import pilot_data as data
from nyc_taxi.ml import pilot_model as model

EVALUATED_SPLITS = ("train", "validation")
SAMPLE_CAPS = {"train": 200_000, "validation": 50_000, "test": 50_000}


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def memory_snapshot(spark=None):
    result = {"python_peak_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024}
    for key, name in (("container_current_bytes", "memory.current"), ("container_peak_bytes", "memory.peak"), ("container_limit_bytes", "memory.max")):
        path = Path("/sys/fs/cgroup") / name
        if path.is_file():
            text = path.read_text().strip()
            result[key] = int(text) if text.isdigit() else text
    if spark is not None:
        try:
            bean = spark._jvm.java.lang.management.ManagementFactory.getMemoryMXBean()
            heap = bean.getHeapMemoryUsage()
            result.update(jvm_heap_used_bytes=heap.getUsed(), jvm_heap_max_bytes=heap.getMax(),
                          jvm_processors=spark._jvm.java.lang.Runtime.getRuntime().availableProcessors())
        except Exception as error:
            # A dead JVM must not hide the original failure or prevent run cleanup.
            result["jvm_telemetry_unavailable"] = type(error).__name__
    return result


def start_spark(output: Path, *, diagnostics: bool = False):
    from pyspark.sql import SparkSession

    local_dirs = os.environ.get("SPARK_LOCAL_DIRS", str(output / "spark-tmp"))
    builder = (SparkSession.builder.appName("NYC-Taxi-ML-P2-Pilot").master("local[2]")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.default.parallelism", "8")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.local.dir", local_dirs)
             .config("spark.sql.warehouse.dir", str(output / "warehouse"))
             .config("spark.sql.codegen.wholeStage", "true"))
    if diagnostics:
        events = output / "spark-events"
        events.mkdir(exist_ok=False)
        builder = (builder.config("spark.eventLog.enabled", "true")
                   .config("spark.eventLog.dir", events.as_uri())
                   .config("spark.eventLog.compress", "false")
                   .config("spark.eventLog.rolling.enabled", "false")
                   .config("spark.sql.debug.maxToStringFields", "200"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_gold(spark, root: Path, profile: dict):
    import pyarrow.parquet as pq

    raw = (root / "gold_manifest.json").read_bytes()
    if hashlib.sha256(raw).hexdigest() != profile["manifest_sha256"]:
        raise ValueError("Gold manifest differs from the reviewed ML P1 profile")
    manifest = json.loads(raw)
    entries = sorted(manifest["datasets"], key=lambda item: item["dataset_name"])
    if [e["dataset_name"] for e in entries] != [f"taxi_2025-{month:02d}" for month in range(1, 7)]:
        raise ValueError("Pilot requires exactly the six reviewed Gold source months")
    files, lineage, rows = [], [], 0
    expected_files = {item["path"]: item for item in profile["file_inventory"]}
    for entry in entries:
        if entry["validation_status"] != "valid":
            raise ValueError("Gold partition is not valid")
        path = root / entry["gold_path"].removeprefix("data/gold/")
        selected = sorted(path.glob("*.parquet"))
        for file in selected:
            expected = expected_files.get(file.relative_to(root).as_posix())
            observed = file.stat()
            if expected is None or (observed.st_size, observed.st_mtime_ns) != (expected["bytes"], int(expected["mtime_ns"])):
                raise ValueError("Gold file metadata differs from the reviewed P1 snapshot")
        count = sum(pq.ParquetFile(file).metadata.num_rows for file in selected)
        if not selected or count != entry["gold_rows"]:
            raise ValueError("Gold physical metadata differs from manifest")
        rows += count
        files.extend(map(str, selected))
        lineage.append({"source_partition": entry["partition"], "gold_batch_id": entry["gold_batch_id"], "rows": count})
    if rows != profile["total"]["rows"]:
        raise ValueError("Gold total differs from ML P1")
    if {Path(file).relative_to(root).as_posix() for file in files} != set(expected_files):
        raise ValueError("Gold file inventory differs from ML P1")
    frame = spark.read.parquet(*files).select(*data.BUSINESS_COLUMNS, "_source_file")
    data.assert_ntz(frame)
    return frame, {"manifest_sha256": hashlib.sha256(raw).hexdigest(), "rows": rows, "partitions": lineage}


def prepare_sample_first(spark, args, profile, report, cached, phase):
    """Same model sample as P2, with dedup/overlap explicitly sample-scoped.

    The journey key is a function of the business identity's original fields.
    Exact duplicates therefore have the same sample predicate; filtering the
    whole equivalence class commutes with deterministic business-key dedup.
    No claim of a newly verified full-cohort overlap/dedup is made here.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from nyc_taxi.ml.pilot_diagnostics import save_plan

    lean = [*model.INPUT_COLUMNS, model.LABEL_COLUMN, data.BUSINESS_KEY_COLUMN,
            data.GROUP_KEY_COLUMN, data.SPLIT_COLUMN, "_source_file"]
    report["preparation_scope"] = {
        "cohort_counts": "full reviewed Gold cohort",
        "dedup": "hash-selected 1% cohort only",
        "overlap": "deduplicated 1% sample, including June counts/keys only",
        "full_cohort_overlap_reverified": False,
        "full_cohort_dedup_reverified": False,
        "equivalence": "business equality implies identical journey SHA and sampling decision",
    }
    with phase("gold_metadata_and_plan_only"):
        gold, report["gold_lineage"] = load_gold(spark, args.gold_root, profile)
        report["gold_scan_partitions"] = gold.rdd.getNumPartitions()
        cohort = data.with_temporal_split(data.apply_cohort(gold))
        # Reconstruct old plans for inspection ONLY; no persistence/actions on
        # these old full-cohort shuffle plans, and no replay of the failed job.
        old_keyed = data.with_identities(cohort).select(*lean)
        old_unique = data.deduplicate_business(old_keyed)
        report["plans"] = {
            "old_full_dedup_reconstructed_no_action": save_plan(args.output_root, "old-full-dedup-reconstructed", old_unique),
            "old_full_overlap_reconstructed_no_action": save_plan(args.output_root, "old-full-overlap-reconstructed", data.overlap_summary_frame(old_unique)),
            "new_cohort_filter": save_plan(args.output_root, "new-cohort-filter", cohort),
        }
    with phase("read_filter_profile"):
        report["cohort"] = data.cohort_summary(gold, include_splits=True)
        if report["cohort"]["waterfall_kept"] != profile["total"]["waterfall_kept"]:
            raise ValueError("Spark cohort differs from the fixed ML P1 waterfall")
        report["full_cohort_counts"] = report["cohort"]["split_eligible"]
        expected = profile["total"]["split_eligible"]
        if report["full_cohort_counts"] != {"train": expected["train_Jan_Apr"], "validation": expected["validation_May"], "test": expected["test_June"]}:
            raise ValueError("Actual pickup splits differ from ML P1")
    with phase("hash_sample_and_materialize"):
        # Business20-field floating-point formatting runs only AFTER the exact
        # unchanged journey-hash sample filter; no full-cohort key cache.
        selected = data.sample_first_with_identities(cohort).select(*lean)
        report["plans"]["new_sample_before_shuffles"] = save_plan(args.output_root, "new-sample-before-shuffles", selected)
        selected = selected.persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(selected)
        report["counts_before_dedup"] = data.split_counts(selected)
    with phase("sample_dedup"):
        unique = data.deduplicate_business(selected)
        report["plans"]["new_sample_dedup"] = save_plan(args.output_root, "new-sample-dedup", unique)
        unique = unique.persist(StorageLevel.MEMORY_AND_DISK)
        cached.append(unique)
        report["counts_after_dedup"] = data.split_counts(unique)
        report["duplicate_rows_removed"] = {name: report["counts_before_dedup"][name] - report["counts_after_dedup"][name] for name in data.SPLITS}
        report["sample_counts"] = dict(report["counts_after_dedup"])
        for name, count in report["sample_counts"].items():
            if not 1 <= count <= SAMPLE_CAPS[name]:
                raise ValueError("Hash sample exceeds the predeclared cap or has an empty split")
        selected.unpersist()
    with phase("sample_temporal_guard"):
        data.assert_temporal_splits(unique)
    with phase("sample_overlap_guard"):
        report["plans"]["new_sample_overlap"] = save_plan(args.output_root, "new-sample-overlap", data.overlap_summary_frame(unique))
        report["overlap"] = data.assert_no_split_overlap(unique)
    with phase("sample_partition_diagnostics"):
        # Bounded cached-sample aggregation, NOT another full Gold action.
        rows = unique.groupBy(F.spark_partition_id().alias("partition_id")).count().collect()
        report["sample_partition_counts"] = sorted([r.asDict() for r in rows], key=lambda r: r["partition_id"])
    return unique


def run_training(args) -> int:
    from pyspark import StorageLevel
    from pyspark.ml import PipelineModel
    from pyspark.sql import functions as F
    import mlflow
    import mlflow.spark
    from mlflow.tracking import MlflowClient
    from nyc_taxi.ml.pilot_tracking import verify_probe_fresh_worker, validate_tracking_uri

    output = args.output_root
    validate_tracking_uri(args.tracking_uri)
    if (output / "training_report.json").exists() or (output / "training_started.json").exists():
        raise ValueError("Pilot output already contains an attempt; never overwrite/retrain it silently")
    profile = read_json(args.profile_contract)
    identity = read_json(output / "code_identity.json")
    if not identity.get("verified"):
        raise ValueError("A matching image/code hash probe is required before training")
    if identity.get("preparation_mode", "full") != args.preparation_mode:
        raise ValueError("Preparation mode differs from the verified attempt identity")
    probe_check = verify_probe_fresh_worker(args.tracking_uri, output / "artifact_probe.json", output / "probe_fresh_worker")
    probe = read_json(output / "artifact_probe.json")
    mlflow.set_tracking_uri(args.tracking_uri)
    client = MlflowClient(tracking_uri=args.tracking_uri)
    run = client.create_run(probe["experiment_id"], tags={
        "mlflow.runName": "ML-P2.1-distance-pilot" if args.preparation_mode == "sample-first" else "ML-P2-distance-pilot", "phase": "pilot", "test_evaluated": "false",
        "preparation_mode": args.preparation_mode,
        "code_identity_sha256": identity["inventory_sha256"], "scope": "ML P2",
    })
    run_id = run.info.run_id
    started = time.perf_counter()
    report = {
        "schema_version": 2, "status": "RUNNING", "run_id": run_id,
        "preparation_mode": args.preparation_mode,
        "experiment_id": probe["experiment_id"], "probe_run_id": probe["probe_run_id"],
        "artifact_uri": run.info.artifact_uri, "probe_fresh_worker": probe_check,
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "test_evaluated": False,
        "evaluated_splits": list(EVALUATED_SPLITS), "code_identity": identity,
        "p1_profile_sha256": hashlib.sha256(args.profile_contract.read_bytes()).hexdigest(),
        "sample": {"seed": 42, "fraction": 0.01, "method": "pmod(xxhash64(lit(42), canonical_journey_sha256),1000000)<10000", "hard_caps": SAMPLE_CAPS},
        "identity": {"version": data.IDENTITY_VERSION, "business_fields": list(data.BUSINESS_COLUMNS), "group_fields": list(data.GROUP_COLUMNS)},
        "features": {"input": list(model.INPUT_COLUMNS), "prepared": list(model.PREPARED_COLUMNS), "vector": list(model.FEATURE_COLUMNS),
                     "contract": "positive standard-rate fare USD conditioned on user-provided distance; not routing", "month_year_predictors": False},
        "parameters": dict(model.GBT_PARAMETERS), "phases": {},
    }
    parameters_to_log = {"preparation_mode": args.preparation_mode, "identity_version": data.IDENTITY_VERSION,
                       "overlap_scope": "model sample including June keys" if args.preparation_mode == "sample-first" else "full cohort",
                       "seed": 42, "sample_fraction": 0.01, "sample_method": report["sample"]["method"],
                       "target": "fare_amount", "target_unit": "USD", "distance_unit": "mile", "distance_max": 200,
                       "fare_positive": True, "ratecode": 1, "zone_min": 1, "zone_max": 263,
                       "train_boundary": "[2025-01-01,2025-05-01)", "validation_boundary": "[2025-05-01,2025-06-01)",
                       "test_boundary": "[2025-06-01,2025-07-01), UNEVALUATED", "pickup_semantics": "NYC-assumed TIMESTAMP_NTZ wall clock, no UTC conversion",
                       "spark_master": "local[2]", "container_cpus": 2, "container_memory_mib": 3072, "driver_memory_mib": 1536,
                       "feature_vector": ",".join(model.FEATURE_COLUMNS), **{"gbt." + k: v for k, v in model.GBT_PARAMETERS.items()}}
    spark, cached = None, []
    previous_term_handler = signal.getsignal(signal.SIGTERM)
    deadline_reached = False

    def deadline_signal(signum, frame):
        nonlocal deadline_reached
        deadline_reached = True
        raise TimeoutError("The fixed pilot execution deadline was reached")

    signal.signal(signal.SIGTERM, deadline_signal)

    @contextmanager
    def phase(name):
        begin = time.perf_counter()
        succeeded = False
        report["current_phase"] = name
        write_json(output / "progress.json", report)
        if spark is not None:
            spark.sparkContext.setJobGroup("ML-P2:" + name, name, interruptOnCancel=True)
        print(f"ML_P2 START {name}", flush=True)
        try:
            yield
            succeeded = True
        finally:
            report["phases"][name] = {"status": "FINISHED" if succeeded else "FAILED", "seconds": round(time.perf_counter() - begin, 3), "memory": memory_snapshot(spark)}
            write_json(output / "progress.json", report)
            print(f"ML_P2 END {name}: seconds={report['phases'][name]['seconds']}", flush=True)

    try:
        write_json(output / "training_started.json", {"run_id": run_id, "started_at_utc": report["started_at_utc"]})
        for key, value in parameters_to_log.items():
            client.log_param(run_id, key, str(value))
        spark = start_spark(output, diagnostics=args.preparation_mode == "sample-first")
        report["spark_settings"] = {key: spark.sparkContext.getConf().get(key, "unset") for key in ("spark.master", "spark.driver.memory", "spark.local.dir", "spark.sql.shuffle.partitions", "spark.default.parallelism", "spark.eventLog.enabled", "spark.eventLog.dir")}
        report["versions"] = {name: importlib.metadata.version(name) for name in ("pyspark", "mlflow", "numpy", "pyarrow", "scikit-learn")}
        report["versions"]["python"] = sys.version.split()[0]
        report["versions"]["java"] = spark._jvm.java.lang.System.getProperty("java.version")
        if args.preparation_mode == "sample-first":
            sample = prepare_sample_first(spark, args, profile, report, cached, phase)
        else:
            with phase("gold_contract_and_cohort"):
                gold, report["gold_lineage"] = load_gold(spark, args.gold_root, profile)
                report["cohort"] = data.cohort_summary(gold)
                if report["cohort"]["waterfall_kept"] != profile["total"]["waterfall_kept"]:
                    raise ValueError("Spark cohort differs from the fixed ML P1 waterfall")
                lean_columns = [*model.INPUT_COLUMNS, model.LABEL_COLUMN, data.BUSINESS_KEY_COLUMN, data.GROUP_KEY_COLUMN, data.SPLIT_COLUMN, "_source_file"]
                cohort = data.with_temporal_split(data.with_identities(data.apply_cohort(gold))).select(*lean_columns).persist(StorageLevel.DISK_ONLY)
                cached.append(cohort)
                report["counts_before_dedup"] = data.split_counts(cohort)
                expected_splits = profile["total"]["split_eligible"]
                if report["counts_before_dedup"] != {"train": expected_splits["train_Jan_Apr"], "validation": expected_splits["validation_May"], "test": expected_splits["test_June"]}:
                    raise ValueError("Actual pickup splits differ from ML P1")
            with phase("cross_source_dedup"):
                unique = data.deduplicate_business(cohort).persist(StorageLevel.DISK_ONLY)
                cached.append(unique)
                report["counts_after_dedup"] = data.split_counts(unique)
                report["duplicate_rows_removed"] = {name: report["counts_before_dedup"][name] - report["counts_after_dedup"][name] for name in data.SPLITS}
                cohort.unpersist()
            with phase("overlap_and_sample"):
                data.assert_temporal_splits(unique)
                report["overlap"] = data.assert_no_split_overlap(unique)
                sample = data.deterministic_sample(unique).persist(StorageLevel.MEMORY_AND_DISK)
                cached.append(sample)
                report["sample_counts"] = data.split_counts(sample)
                for name, count in report["sample_counts"].items():
                    if not 1 <= count <= SAMPLE_CAPS[name]:
                        raise ValueError("Hash sample exceeds the predeclared cap or has an empty split")
                unique.unpersist()
        with phase("train_only_preprocessing"):
            # There is deliberately no prepare/transform/predict call for test.
            prepared = {}
            for name in EVALUATED_SPLITS:
                prepared[name] = model.prepare_features(sample.filter(F.col(data.SPLIT_COLUMN) == name)).persist(StorageLevel.MEMORY_AND_DISK)
                cached.append(prepared[name])
            baseline = model.baseline_median(prepared["train"])
            report["baseline"] = {"type": "train_sample_median", "value_usd": baseline, "relative_error": model.BASELINE_RELATIVE_ERROR}
            preprocessor = model.fit_preprocessor(prepared["train"])
            transformed_train = preprocessor.transform(prepared["train"]).persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(transformed_train)
            report["feature_metadata"] = model.assert_category_metadata(transformed_train)
            report["unseen_categories"] = {name: model.summarize_unseen_categories(preprocessor, prepared[name]) for name in EVALUATED_SPLITS}
        with phase("gbt_fit"):
            fitted = model.make_regressor().fit(transformed_train)
            combined = PipelineModel(stages=[*preprocessor.stages, fitted])
        with phase("train_validation_metrics"):
            report["metrics"] = {}
            for name in EVALUATED_SPLITS:
                result = model.evaluate_predictions(combined.transform(prepared[name]), baseline)
                if result["count"] != report["sample_counts"][name]:
                    raise ValueError("Model and baseline evaluation lost sample rows")
                report["metrics"][name] = result
                for estimator in ("baseline", "gbt"):
                    for metric, value in result[estimator].items():
                        if value is not None:
                            client.log_metric(run_id, f"{name}.{estimator}.{metric}", value)
            base_mae = report["metrics"]["validation"]["baseline"]["mae_usd"]
            gbt_mae = report["metrics"]["validation"]["gbt"]["mae_usd"]
            report["validation_selection"] = {"candidate": "gbt" if gbt_mae < base_mae else "baseline", "mae_improvement_usd": base_mae - gbt_mae, "mae_improvement_percent": 100 * (base_mae - gbt_mae) / base_mae if base_mae else None, "promoted": False, "test_used": False}
        with phase("model_artifacts_and_reload"):
            model_path = output / "fitted_model"
            mlflow.spark.save_model(combined, str(model_path), dfs_tmpdir=str(output / "spark-model-temp"),
                                   pip_requirements=[f"pyspark=={report['versions']['pyspark']}", f"mlflow=={report['versions']['mlflow']}", f"numpy=={report['versions']['numpy']}"])
            write_json(output / "baseline.json", report["baseline"])
            write_json(output / "feature_contract.json", report["features"])
            client.log_artifacts(run_id, str(model_path), artifact_path="model")
            client.log_artifact(run_id, str(output / "baseline.json"))
            client.log_artifact(run_id, str(output / "feature_contract.json"))
            reload_root = output / "reloaded_artifacts"
            reload_root.mkdir()
            downloaded = client.download_artifacts(run_id, "model", str(reload_root))
            reloaded = mlflow.spark.load_model(downloaded, dfs_tmpdir=str(output / "spark-model-reload-temp"))
            batch = prepared["validation"].select(*model.PREPARED_COLUMNS).limit(16)
            report["reload_verification"] = model.compare_reloaded_predictions(combined, reloaded, batch, max_rows=16)
            before_rows = combined.transform(batch).select(*model.PREPARED_COLUMNS, "prediction").collect()
            write_json(output / "reload_batch.json", {"prepared_rows": [r.asDict() for r in before_rows], "baseline_value_usd": baseline})
            client.log_artifact(run_id, str(output / "reload_batch.json"), artifact_path="verification")
            baseline_download = client.download_artifacts(run_id, "baseline.json", str(reload_root))
            if read_json(Path(baseline_download))["value_usd"] != baseline:
                raise ValueError("Reloaded baseline differs")
        report.update(status="FINISHED", seconds=round(time.perf_counter() - started, 3), memory=memory_snapshot(spark))
        write_json(output / "training_report.json", report)
        client.log_artifact(run_id, str(output / "training_report.json"))
        client.set_terminated(run_id, status="FINISHED")
        print("ML_P2_RESULT " + json.dumps({key: report[key] for key in ("run_id", "sample_counts", "duplicate_rows_removed", "metrics", "validation_selection", "reload_verification", "seconds", "memory", "test_evaluated")}, allow_nan=False), flush=True)
        return 0
    except Exception as error:
        report.update(status="FAILED", seconds=round(time.perf_counter() - started, 3), failure={"type": type(error).__name__, "message": str(error)[:3000], "deadline_reached": deadline_reached}, memory=memory_snapshot(spark))
        write_json(output / "training_report.json", report)
        try:
            client.log_artifact(run_id, str(output / "training_report.json"))
        except Exception:
            report["tracking_failure_artifact_pending"] = True
            write_json(output / "training_report.json", report)
        try:
            client.set_terminated(run_id, status="FAILED")
        except Exception:
            report["tracking_failure_status_pending"] = True
            write_json(output / "training_report.json", report)
        print(f"ML P2 FAILED: {type(error).__name__}; inspect the dedicated runtime report.", file=sys.stderr)
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_term_handler)
        for frame in reversed(cached):
            try:
                frame.unpersist()
            except Exception:
                pass
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                print("Spark stop could not contact the JVM; original pilot result preserved.", file=sys.stderr)
        # Diagnostics run after Spark.stop so event files are closed. They do
        # not replay actions or change a successful/failed training outcome.
        if args.preparation_mode == "sample-first":
            try:
                from nyc_taxi.ml.pilot_diagnostics import summarize_events
                diagnostics = summarize_events(output / "spark-events")
                write_json(output / "execution_diagnostics.json", diagnostics)
                client.log_artifact(run_id, str(output / "execution_diagnostics.json"), artifact_path="diagnostics")
                if (output / "plans").is_dir():
                    client.log_artifacts(run_id, str(output / "plans"), artifact_path="diagnostics/plans")
            except Exception as error:
                write_json(output / "diagnostics_failure.json", {"type": type(error).__name__, "message": str(error)[:1000]})
                print("Post-stop diagnostics incomplete; see diagnostics_failure.json.", file=sys.stderr)


def fresh_reload(args) -> int:
    import mlflow
    import mlflow.spark
    from mlflow.tracking import MlflowClient
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    from nyc_taxi.ml.pilot_tracking import validate_tracking_uri

    validate_tracking_uri(args.tracking_uri)
    report = read_json(args.output_root / "training_report.json")
    if report["status"] != "FINISHED" or report["test_evaluated"]:
        raise ValueError("Only a finished, test-held-out pilot may be verified")
    destination = args.output_root / "fresh_worker_reload"
    destination.mkdir()
    mlflow.set_tracking_uri(args.tracking_uri)
    client = MlflowClient(tracking_uri=args.tracking_uri)
    downloaded = client.download_artifacts(report["run_id"], "model", str(destination))
    batch_path = client.download_artifacts(report["run_id"], "verification/reload_batch.json", str(destination))
    baseline_path = client.download_artifacts(report["run_id"], "baseline.json", str(destination))
    expected = read_json(Path(batch_path))
    schema = StructType([StructField(name, StringType() if name in model.CATEGORICAL_INPUT_COLUMNS else DoubleType(), False) for name in model.PREPARED_COLUMNS])
    spark = None
    try:
        spark = start_spark(args.output_root)
        fitted = mlflow.spark.load_model(downloaded, dfs_tmpdir=str(destination / "spark-temp"))
        records = expected["prepared_rows"]
        if not 1 <= len(records) <= 16:
            raise ValueError("Fresh-worker reload batch must contain 1 to 16 records")
        frame = spark.createDataFrame([tuple(row[name] for name in model.PREPARED_COLUMNS) for row in records], schema)
        got = fitted.transform(frame).select(*model.PREPARED_COLUMNS, "prediction").collect()
        key = lambda row: tuple(row[name] for name in model.PREPARED_COLUMNS)
        before = sorted(records, key=key)
        after = sorted([row.asDict() for row in got], key=key)
        if len(before) != len(after) or any(key(a) != key(b) for a, b in zip(before, after, strict=True)):
            raise ValueError("Fresh-worker reload inputs/count changed")
        if any(not math.isfinite(row["prediction"]) for row in [*before, *after]):
            raise ValueError("Fresh-worker reload produced a nonfinite prediction")
        difference = max(abs(a["prediction"] - b["prediction"]) for a, b in zip(before, after, strict=True))
        if difference > 1e-9 or read_json(Path(baseline_path))["value_usd"] != expected["baseline_value_usd"]:
            raise ValueError("Fresh-worker artifact/model reload mismatch")
        result = {"run_id": report["run_id"], "count": len(records), "max_absolute_difference": difference, "absolute_tolerance": 1e-9, "fresh_worker": True, "test_evaluated": False}
        write_json(args.output_root / "fresh_worker_reload.json", result)
        client.log_artifact(report["run_id"], str(args.output_root / "fresh_worker_reload.json"), artifact_path="verification")
        print("ML_P2_RELOAD " + json.dumps(result), flush=True)
        return 0
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                print("Reload Spark cleanup could not contact the JVM.", file=sys.stderr)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("probe", "train", "verify-reload"))
    parser.add_argument("--gold-root", type=Path, default=Path("/gold"))
    parser.add_argument("--output-root", type=Path, default=Path("/ml-output"))
    parser.add_argument("--profile-contract", type=Path, default=Path("/ml-contract/p1-profile.json"))
    parser.add_argument("--tracking-uri", default=None)
    parser.add_argument("--experiment-name", default="nyc-taxi-distance-pilot-ml-p2-20261022")
    parser.add_argument("--artifact-root", default="file:///mlflow/artifacts/ml-p2-20261022")
    parser.add_argument("--preparation-mode", choices=("full", "sample-first"), default="full",
                        help="P2 full-cohort preparation or equivalent P2.1 sample-first preparation")
    args = parser.parse_args(argv)
    if args.tracking_uri is None:
        import os
        args.tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "")
    try:
        if args.command == "probe":
            from nyc_taxi.ml.pilot_tracking import artifact_probe
            result = artifact_probe(args.tracking_uri, args.experiment_name, args.artifact_root, args.output_root / "probe")
            write_json(args.output_root / "artifact_probe.json", result)
            print("ML_P2_ARTIFACT_PROBE " + json.dumps(result), flush=True)
            return 0
        if args.command == "train":
            return run_training(args)
        return fresh_reload(args)
    except Exception as error:
        print(f"ML P2 command failed ({type(error).__name__}); no automatic retry/tuning.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
