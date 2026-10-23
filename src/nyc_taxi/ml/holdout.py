"""One-shot June evaluation of the frozen P2.1 candidate. No fitting API is used."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import time

TRAINING_RUN = "d2772f1904d7472490a73b0ac1c0c5d4"
EXPECTED_COUNTS = {"train": 110356, "validation": 29979, "test": 27310}
CORE_FILES = (
    "src/nyc_taxi/ml/pilot.py", "src/nyc_taxi/ml/pilot_data.py",
    "src/nyc_taxi/ml/pilot_model.py", "src/nyc_taxi/ml/pilot_tracking.py",
    "src/nyc_taxi/etl/silver.py", "requirements.txt",
    "requirements-worker-constraints.txt", "pyproject.toml",
)


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def tree_identity(root: Path) -> dict:
    """Content identity: sorted relative POSIX path, bytes and SHA256, not mtime."""
    if not root.is_dir() or root.is_symlink():
        raise ValueError("Model artifact root must be a real directory")
    files = []
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        if path.is_symlink():
            raise ValueError("Model artifact contains a symlink")
        if path.is_file():
            files.append({"path": path.relative_to(root).as_posix(),
                          "bytes": path.stat().st_size, "sha256": digest(path)})
    if not files:
        raise ValueError("Model artifact is empty")
    payload = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return {"sha256": hashlib.sha256(payload).hexdigest(), "files": files}


def verify_training_contract(training: dict, baseline: dict, contract: dict, app: Path) -> dict:
    """Fail closed before model loading/prediction; dependencies and derivation stay frozen."""
    from nyc_taxi.ml import pilot_data as data, pilot_model as model

    if (training.get("run_id") != TRAINING_RUN or training.get("status") != "FINISHED"
            or training.get("test_evaluated") is not False
            or training.get("evaluated_splits") != ["train", "validation"]
            or training.get("sample_counts") != EXPECTED_COUNTS
            or training.get("parameters") != model.GBT_PARAMETERS
            or training.get("features") != contract
            or contract.get("vector") != list(model.FEATURE_COLUMNS)
            or contract.get("input") != list(model.INPUT_COLUMNS)
            or contract.get("prepared") != list(model.PREPARED_COLUMNS)
            or training.get("baseline") != baseline or baseline.get("value_usd") != 12.1
            or training.get("sample", {}).get("seed") != 42
            or training.get("sample", {}).get("fraction") != 0.01
            or training.get("identity", {}).get("version") != data.IDENTITY_VERSION):
        raise ValueError("Frozen training contract differs from the selected P2.1 candidate")
    hashes = training["code_identity"]["file_hashes"]
    verified = {name: digest(app / name) for name in CORE_FILES}
    if any(verified[name] != hashes.get(name) for name in CORE_FILES):
        raise ValueError("Training feature/cohort/identity code or dependency inputs changed")
    return verified


def read_zones(path: Path) -> list[tuple[str, str]]:
    """Static public lookup only, no DB access or fitted/category transformations."""
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    lookup = {int(row["LocationID"]): row["Borough"] for row in rows}
    if len(rows) != 265 or len(lookup) != 265 or set(lookup) != set(range(1, 266)):
        raise ValueError("Taxi zone lookup must contain exactly 265 unique IDs")
    allowed = {"Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island", "EWR"}
    if any(lookup[key] not in allowed for key in range(1, 264)):
        raise ValueError("Named taxi zones have an unexpected borough")
    return [(str(key), lookup[key]) for key in range(1, 264)]


def _write_new(path: Path, value) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def run(args) -> int:
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    import mlflow.spark
    from nyc_taxi.ml import pilot_data as data, pilot_model as model
    from nyc_taxi.ml.pilot import load_gold, memory_snapshot, read_json, start_spark, write_json
    from nyc_taxi.ml.pilot_tracking import get_client
    from nyc_taxi.ml.holdout_metrics import summarize_holdout

    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    if (output / "evaluation_started.json").exists():
        raise ValueError("Evaluation attempt already exists; never overwrite holdout results")
    image_identity = read_json(output / "code_identity.json")
    if not image_identity.get("verified") or image_identity.get("image_id") != args.image_id:
        raise ValueError("Matching image/code hash verification is required")
    training = read_json(args.training_dir / "training_report.json")
    baseline = read_json(args.training_dir / "baseline.json")
    contract = read_json(args.training_dir / "feature_contract.json")
    core_hashes = verify_training_contract(training, baseline, contract, Path("/app"))
    profile = read_json(args.profile)
    if digest(args.profile) != training["p1_profile_sha256"]:
        raise ValueError("P1 profile differs from the training snapshot")
    zones = read_zones(args.zones)
    client = get_client(args.tracking_uri)
    source_run = client.get_run(TRAINING_RUN)
    if (source_run.info.status != "FINISHED" or source_run.info.experiment_id != training["experiment_id"]
            or source_run.info.artifact_uri != training["artifact_uri"]
            or source_run.info.artifact_uri != "file:///mlflow/artifacts/ml-p2-20261022/" + TRAINING_RUN + "/artifacts"):
        raise ValueError("Authoritative training run identity/status differs")
    experiment = client.get_experiment(training["experiment_id"])
    if (experiment.artifact_location != "file:///mlflow/artifacts/ml-p2-20261022"
            or experiment.tags.get("ml_p2.owner") != "pilot_tracking_v1"):
        raise ValueError("Dedicated persistent experiment ownership differs")
    _write_new(output / "evaluation_started.json", {"training_run_id": TRAINING_RUN,
               "started_at_utc": datetime.now(timezone.utc).isoformat(), "image_id": args.image_id})
    report = {"status": "RUNNING", "training_run_id": TRAINING_RUN, "test_evaluated": False,
              "phases_seconds": {}, "code_identity": image_identity}
    spark, cached, run_id = None, [], None
    started = time.perf_counter()

    @contextmanager
    def phase(name):
        print("BEGIN " + name, flush=True)
        tick = time.perf_counter()
        try:
            yield
        finally:
            report["phases_seconds"][name] = round(time.perf_counter() - tick, 3)
            print("END " + name + " " + str(report["phases_seconds"][name]) + "s", flush=True)

    try:
        with phase("freeze_artifacts_before_gold"):
            download_root = output / "training-artifacts"
            download_root.mkdir()
            downloaded = {}
            for artifact in ("model", "baseline.json", "feature_contract.json", "training_report.json"):
                downloaded[artifact] = Path(client.download_artifacts(TRAINING_RUN, artifact, str(download_root)))
            local_model = tree_identity(args.training_dir / "fitted_model")
            if tree_identity(downloaded["model"]) != local_model:
                raise ValueError("MLflow model differs from the preserved fitted training model")
            for name in ("baseline.json", "feature_contract.json", "training_report.json"):
                if digest(downloaded[name]) != digest(args.training_dir / name):
                    raise ValueError("MLflow training metadata differs from preserved outputs")
            freeze = {"training_run_id": TRAINING_RUN, "training_image_id": training["code_identity"]["image_id"],
                      "evaluation_image_id": args.image_id, "model_tree": local_model,
                      "model_uri": f"runs:/{TRAINING_RUN}/model", "baseline": baseline,
                      "baseline_sha256": digest(downloaded["baseline.json"]),
                      "feature_contract_sha256": digest(downloaded["feature_contract.json"]),
                      "features": contract, "parameters": training["parameters"],
                      "cohort": "finite positive fare; ratecode=1; PU/DO IDs 1..263; finite distance [0,200]; actual local pickup Jan-Jun 2025; zero distance and uncapped fare retained",
                      "split": {"train": "[2025-01-01,2025-05-01)", "validation": "[2025-05-01,2025-06-01)", "test": "[2025-06-01,2025-07-01)"},
                      "pickup_semantics": "NYC-assumed NTZ wall clock, no UTC conversion",
                      "sample": training["sample"], "identity": training["identity"],
                      "gold_lineage": training["gold_lineage"], "core_file_sha256": core_hashes,
                      "p1_profile_sha256": digest(args.profile), "zone_lookup_sha256": digest(args.zones),
                      "locked_at_utc": datetime.now(timezone.utc).isoformat(), "no_fit": True}
            _write_new(output / "candidate_lock.json", freeze)
            report["model_tree_sha256"] = local_model["sha256"]
            evaluation = client.create_run(training["experiment_id"], tags={
                "mlflow.runName": "ML-P3-frozen-June-holdout", "scope": "ML P3",
                "training_run_id": TRAINING_RUN, "model_tree_sha256": local_model["sha256"],
                "ml_p2.owner": "pilot_tracking_v1", "test_used_for_tuning": "false"})
            candidate_id = evaluation.info.run_id
            if (evaluation.info.experiment_id != training["experiment_id"]
                    or evaluation.info.artifact_uri != experiment.artifact_location + "/" + candidate_id + "/artifacts"
                    or evaluation.data.tags.get("training_run_id") != TRAINING_RUN
                    or evaluation.data.tags.get("scope") != "ML P3"):
                raise ValueError("Evaluation artifact routing differs from persistent subpath")
            run_id = report["evaluation_run_id"] = candidate_id
            report["artifact_uri"] = evaluation.info.artifact_uri
            client.log_artifact(run_id, str(output / "candidate_lock.json"))
            for key, value in {"training_run_id": TRAINING_RUN, "model_tree_sha256": local_model["sha256"],
                               "sample_fraction": 0.01, "seed": 42, "cohort_changed": False,
                               "refit": False, "test_boundary": freeze["split"]["test"],
                               "baseline_usd": baseline["value_usd"], "cpus": 2, "memory_mib": 3072}.items():
                client.log_param(run_id, key, value)
        with phase("spark_and_fitted_model_load"):
            spark = start_spark(output)
            fitted = mlflow.spark.load_model(str(downloaded["model"]), dfs_tmpdir=str(output / "model-load-temp"))
            if len(fitted.stages) != 4:
                raise ValueError("Expected two fitted indexers, assembler and GBT")
            if [type(stage).__name__ for stage in fitted.stages] != ["StringIndexerModel", "StringIndexerModel", "VectorAssembler", "GBTRegressionModel"]:
                raise ValueError("Loaded fitted stage classes differ from the frozen pipeline")
            for stage, source, target in zip(fitted.stages[:2], model.CATEGORICAL_INPUT_COLUMNS, model.CATEGORICAL_FEATURE_COLUMNS):
                if stage.getInputCol() != source or stage.getOutputCol() != target or stage.getHandleInvalid() != "keep":
                    raise ValueError("Fitted category preprocessing differs")
            if fitted.stages[2].getInputCols() != list(model.FEATURE_COLUMNS):
                raise ValueError("Loaded vector differs from frozen feature allowlist")
            for key, value in training["parameters"].items():
                if fitted.stages[3].getOrDefault(key) != value:
                    raise ValueError("Loaded GBT parameters differ from frozen candidate")
        with phase("gold_lineage_and_exact_sample_recreation"):
            gold, lineage = load_gold(spark, args.gold_root, profile)
            if lineage != training["gold_lineage"]:
                raise ValueError("Gold lineage differs from the training run")
            cohort = data.with_temporal_split(data.apply_cohort(gold))
            lean = [*model.INPUT_COLUMNS, model.LABEL_COLUMN, data.BUSINESS_KEY_COLUMN,
                    data.GROUP_KEY_COLUMN, data.SPLIT_COLUMN, "_source_file"]
            selected = data.sample_first_with_identities(cohort).select(*lean).persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(selected)
            report["counts_before_dedup"] = data.split_counts(selected)
            unique = data.deduplicate_business(selected).persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(unique)
            report["sample_counts"] = data.split_counts(unique)
            if (report["counts_before_dedup"] != training["counts_before_dedup"]
                    or report["sample_counts"] != EXPECTED_COUNTS):
                raise ValueError("Recreated sample counts differ from the frozen training run")
            selected.unpersist()
        with phase("sample_overlap_and_identity_lock_before_prediction"):
            data.assert_temporal_splits(unique)
            report["sample_overlap"] = data.assert_no_split_overlap(unique)
            identities = unique.groupBy(data.SPLIT_COLUMN).agg(
                F.count("*").alias("rows"),
                F.sha2(F.concat_ws("", F.sort_array(F.collect_list(F.sha2(F.concat(
                    F.col(data.BUSINESS_KEY_COLUMN), F.col(data.GROUP_KEY_COLUMN)), 256)))), 256).alias("sha256")
            ).collect()
            report["sample_identity"] = {row[data.SPLIT_COLUMN]: {"rows": row["rows"], "sha256": row["sha256"]} for row in identities}
            sample_lock = {"training_run_id": TRAINING_RUN, "counts": report["sample_counts"],
                           "fingerprints": report["sample_identity"], "overlap": report["sample_overlap"],
                           "overlap_scope": "recreated deduplicated 1% sample, all three splits; not full cohort",
                           "fingerprint_method": "SHA256(concatenated sorted SHA256(businessSHA256+journeySHA256)); fixed-width hex",
                           "prior_sample_fingerprint_available": False,
                           "recreation_evidence": "same frozen code, Gold manifest/file size/mtime/footer lineage, seed, counts and guards; P2.1 persisted no sample or fingerprint",
                           "locked_before_prediction": True, "locked_at_utc": datetime.now(timezone.utc).isoformat()}
            _write_new(output / "sample_lock.json", sample_lock)
            client.log_artifact(run_id, str(output / "sample_lock.json"))
        with phase("june_frozen_transform_and_predictions"):
            june = unique.where(F.col(data.SPLIT_COLUMN) == "test")
            prepared = model.prepare_features(june)
            lookup = spark.createDataFrame(zones, "pickup_zone string, pickup_borough string")
            predictions = fitted.transform(prepared).join(F.broadcast(lookup), "pickup_zone", "left").persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(predictions)
            model.assert_category_metadata(predictions)
            counts = predictions.agg(F.count("*").alias("rows"),
                F.sum(F.col("pickup_borough").isNull().cast("long")).alias("missing_borough"),
                *[F.sum((F.col(index) == len(stage.labels)).cast("long")).alias(name)
                  for stage, index, name in zip(fitted.stages[:2], model.CATEGORICAL_FEATURE_COLUMNS, ("pickup", "dropoff"))]
            ).first().asDict()
            report["test_evaluated"] = True
            if counts["rows"] != EXPECTED_COUNTS["test"] or counts["missing_borough"]:
                raise ValueError("Inference or borough join lost/duplicated sample rows")
            report["unseen_categories"] = {"count": counts["rows"], "pickup": counts["pickup"], "dropoff": counts["dropoff"],
                                           "handling": "saved train-only StringIndexer unknown bucket; no rows dropped"}
        with phase("june_metrics_and_error_slices"):
            result = summarize_holdout(predictions, baseline["value_usd"])
            report["evaluation"] = result
            current = result["overall"]
            report["validation_metrics"] = training["metrics"]["validation"]
            report["comparison"] = {
                "june_mae_improvement_usd": current["baseline"]["mae_usd"] - current["gbt"]["mae_usd"],
                "june_mae_improvement_percent": 100 * (1 - current["gbt"]["mae_usd"] / current["baseline"]["mae_usd"]),
                "gbt_mae_delta_vs_validation_usd": current["gbt"]["mae_usd"] - training["metrics"]["validation"]["gbt"]["mae_usd"],
                "test_used_for_tuning": False, "promoted": False}
            for estimator in ("baseline", "gbt"):
                for key, value in current[estimator].items():
                    if isinstance(value, (float, int)) and math.isfinite(value):
                        client.log_metric(run_id, "june." + estimator + "." + key, value)
            client.log_metric(run_id, "june.rows", EXPECTED_COUNTS["test"])
        report.update(status="FINISHED", test_evaluated=True, seconds=round(time.perf_counter() - started, 3), memory=memory_snapshot(spark))
        write_json(output / "evaluation_report.json", report)
        client.log_artifact(run_id, str(output / "evaluation_report.json"))
        client.set_terminated(run_id, status="FINISHED")
        print("ML_P3_RESULT " + json.dumps({"run_id": run_id, "overall": report["evaluation"]["overall"],
                                             "comparison": report["comparison"], "seconds": report["seconds"]}, allow_nan=False), flush=True)
        return 0
    except Exception as error:
        report.update(status="FAILED", error_type=type(error).__name__, seconds=round(time.perf_counter() - started, 3), memory=memory_snapshot(spark))
        if hasattr(error, "counts"):
            report["invalid_prediction_counts"] = error.counts
        write_json(output / "evaluation_report.json", report)
        if run_id:
            try:
                client.log_artifact(run_id, str(output / "evaluation_report.json"))
                client.set_terminated(run_id, status="FAILED")
            except Exception:
                pass
        raise
    finally:
        for frame in reversed(cached):
            try:
                frame.unpersist()
            except Exception:
                pass
        if spark is not None:
            spark.stop()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("training-dir", "gold-root", "profile", "zones", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--tracking-uri", required=True)
    parser.add_argument("--image-id", required=True)
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
