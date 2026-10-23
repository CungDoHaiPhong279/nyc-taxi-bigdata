"""Synthetic input parity only: independent frozen-model reference or HTTP check.

Does not read Gold, evaluate June, train, contact MLflow or change a database.
Reference mode also runs through stdin in the immutable P3 worker for comparison.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen

MODEL_SHA256 = "759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee"
TRAINING_RUN_ID = "d2772f1904d7472490a73b0ac1c0c5d4"
EVALUATION_RUN_ID = "58d5008f0ed048f58599a859877ae336"


def reference(fixture, model_dir):
    from pyspark.sql.types import StructType, StructField, DoubleType, TimestampNTZType, IntegerType
    import mlflow.spark
    from nyc_taxi.ml.holdout import tree_identity
    from nyc_taxi.ml.pilot import start_spark, memory_snapshot
    from nyc_taxi.ml.pilot_model import prepare_features

    if tree_identity(model_dir)["sha256"] != MODEL_SHA256:
        raise ValueError("Original model hash mismatch")
    scratch = Path("/tmp/ml-p4-reference")
    scratch.mkdir(exist_ok=False)
    spark = None
    tick = time.perf_counter()
    try:
        spark = start_spark(scratch)
        original = mlflow.spark.load_model(str(model_dir), dfs_tmpdir="/tmp/ml-p4-original-load")
        schema = StructType([StructField("trip_distance", DoubleType(), False),
                             StructField("pickup_datetime_local", TimestampNTZType(), False),
                             StructField("pickup_location_id", IntegerType(), False),
                             StructField("dropoff_location_id", IntegerType(), False)])
        predictions = []
        for case in fixture["cases"]:
            values = case["inputs"]
            frame = spark.createDataFrame([(float(values["distance_miles"]), datetime.fromisoformat(values["pickup_datetime"]),
                values["pickup_location_id"], values["dropoff_location_id"])], schema)
            fare = float(original.transform(prepare_features(frame, include_labels=False)).select("prediction").first()[0])
            if not math.isfinite(fare):
                raise ValueError("Reference prediction is not finite")
            predictions.append({"id": case["id"], "fare_amount_usd": fare})
        return {"predictions": predictions, "seconds": time.perf_counter() - tick, "memory": memory_snapshot(spark)}
    finally:
        if spark is not None:
            spark.stop()


def service(fixture, base, reference_report):
    def request(path, payload=None):
        body = json.dumps(payload, allow_nan=False).encode() if payload is not None else None
        req = Request(base.rstrip("/") + path, data=body, headers={"Content-Type": "application/json"})
        tick = time.perf_counter()
        with urlopen(req, timeout=45) as response:
            value = json.loads(response.read(262144))
        elapsed = time.perf_counter() - tick
        if (value.get("model_sha256") != MODEL_SHA256 or value.get("training_run_id") != TRAINING_RUN_ID
                or value.get("evaluation_run_id") != EVALUATION_RUN_ID):
            raise ValueError("Service candidate identity differs")
        return value, elapsed

    ready_before, _ = request("/ready")
    if not ready_before["ready"]:
        raise ValueError("Service is not ready")
    expected = {row["id"]: row["fare_amount_usd"] for row in reference_report["predictions"]}
    results = []
    for case in fixture["cases"]:
        result, elapsed = request("/predict", case["inputs"])
        difference = abs(result["fare_amount_usd"] - expected[case["id"]])
        if difference > 1e-9 or result["unseen_categories"] != case["expected_unseen"]:
            raise ValueError("Service prediction/category parity failed")
        results.append({"id": case["id"], "reference_usd": expected[case["id"]],
                        "service_usd": result["fare_amount_usd"], "absolute_difference": difference,
                        "http_seconds": elapsed, "server_seconds": result["request_seconds"],
                        "unseen_categories": result["unseen_categories"], "warnings": result["warnings"]})
    repeated, repeated_seconds = request("/predict", fixture["cases"][0]["inputs"])
    if abs(repeated["fare_amount_usd"] - results[0]["reference_usd"]) > 1e-9:
        raise ValueError("Repeated prediction differs")
    invalid_cases = [({}, "missing"), ({**fixture["cases"][0]["inputs"], "distance_miles": -1}, "negative_distance"),
                     ({**fixture["cases"][0]["inputs"], "pickup_datetime": "2026-01-01T00:00:00"}, "outside_year"),
                     ({**fixture["cases"][0]["inputs"], "dropoff_location_id": 264}, "invalid_zone"),
                     ({**fixture["cases"][0]["inputs"], "fare_amount": 10}, "leakage_extra_field")]
    invalid = []
    for payload, name in invalid_cases:
        try:
            request("/predict", payload)
        except HTTPError as error:
            if error.code != 400:
                raise
            body = json.loads(error.read())
            if "fare_amount_usd" in body:
                raise ValueError("Error response returned a fake fare")
            invalid.append({"case": name, "status": error.code})
        else:
            raise ValueError("Invalid input was accepted")
    ready_after, _ = request("/ready")
    if (ready_after["model_load_count"] != 1 or ready_before["model_load_count"] != 1
            or ready_before["spark_application_id"] != ready_after["spark_application_id"]):
        raise ValueError("Model/Spark was reloaded between requests")
    return {"parity": results, "max_absolute_difference": max(row["absolute_difference"] for row in results),
            "tolerance_usd": 1e-9, "repeated_first_case_http_seconds": repeated_seconds,
            "invalid_inputs": invalid, "ready_before": ready_before, "ready_after": ready_after}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("reference", "service"), required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--service-url")
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    raw = args.fixture.read_bytes()
    fixture = json.loads(raw)
    fingerprint = hashlib.sha256(raw).hexdigest()
    if args.mode == "reference":
        result = reference(fixture, args.model_dir)
    else:
        old = json.loads(args.reference.read_text(encoding="utf-8"))
        if old["fixture_sha256"] != fingerprint or old["model_sha256"] != MODEL_SHA256:
            raise ValueError("Reference/fixture/model identity differs")
        result = service(fixture, args.service_url, old)
    result.update(mode=args.mode, fixture_sha256=fingerprint, model_sha256=MODEL_SHA256,
                  training_run_id=TRAINING_RUN_ID, evaluation_run_id=EVALUATION_RUN_ID, synthetic_only=True)
    if args.output:
        with args.output.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
    print("ML_P4_CHECK " + json.dumps(result, allow_nan=False), flush=True)


if __name__ == "__main__":
    main()
