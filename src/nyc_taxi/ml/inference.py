"""Frozen, single-process Spark inference. No training or tracking server access."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import csv
from datetime import datetime
import json
import math
from pathlib import Path
import re
import threading
import time
from uuid import uuid4

from nyc_taxi.ml.holdout import digest, tree_identity

TRAINING_RUN_ID = "d2772f1904d7472490a73b0ac1c0c5d4"
EVALUATION_RUN_ID = "58d5008f0ed048f58599a859877ae336"
MODEL_SHA256 = "759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee"
CANDIDATE_LOCK_SHA256 = "12442b5684943bb81169bb084cffc8c1b54043882d75711e295a0f1a0ff7338d"
INPUT_FIELDS = frozenset({"distance_miles", "pickup_datetime", "pickup_location_id", "dropoff_location_id"})
REQUEST_TIMEOUT_SECONDS = 30


class InputError(ValueError):
    """A public validation error, containing no connection/configuration details."""


class InferenceError(RuntimeError):
    def __init__(self, code: str, status: int = 503):
        super().__init__(code)
        self.code, self.status = code, status


def identity() -> dict:
    return {"training_run_id": TRAINING_RUN_ID, "evaluation_run_id": EVALUATION_RUN_ID,
            "model_sha256": MODEL_SHA256}


def validate_inputs(payload: dict) -> dict:
    if not isinstance(payload, dict) or set(payload) != INPUT_FIELDS:
        raise InputError("Cần đúng distance_miles, pickup_datetime, pickup_location_id và dropoff_location_id.")
    distance = payload["distance_miles"]
    try:
        numeric = float(distance) if type(distance) in (int, float) else float("nan")
    except (ValueError, OverflowError):
        numeric = float("nan")
    if not math.isfinite(numeric) or not 0 <= numeric <= 200:
        raise InputError("Distance phải hữu hạn trong [0, 200] mile.")
    for name in ("pickup_location_id", "dropoff_location_id"):
        if type(payload[name]) is not int or not 1 <= payload[name] <= 263:
            raise InputError("PU/DO zone phải là LocationID nguyên trong 1–263.")
    raw = payload["pickup_datetime"]
    if not isinstance(raw, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?", raw):
        raise InputError("Pickup datetime phải là giờ địa phương ISO không timezone/UTC offset.")
    try:
        pickup = datetime.fromisoformat(raw)
    except ValueError:
        raise InputError("Pickup datetime không hợp lệ.") from None
    if pickup.year != 2025 or pickup.tzinfo is not None:
        raise InputError("Demo chỉ hỗ trợ pickup datetime năm 2025, giờ địa phương NYC-assumed.")
    return {"distance_miles": numeric, "pickup_datetime": pickup,
            "pickup_location_id": payload["pickup_location_id"], "dropoff_location_id": payload["dropoff_location_id"]}


def input_warnings(inputs: dict) -> list[str]:
    result = []
    if inputs["distance_miles"] == 0:
        result.append("Zero-distance: nhóm lịch sử có sai số cao hơn, không phải chuyến đi miễn phí.")
    if inputs["distance_miles"] > 20:
        result.append("Chuyến xa: ít bằng chứng; June có 88 chuyến 20–50 mile và không có mẫu >50 mile.")
    if inputs["pickup_datetime"].month > 6:
        result.append("Ngoài Jan–Jun/2025: chưa được đánh giá theo thời gian; chỉ minh họa ngoại suy năm 2025.")
    return result


def validate_artifacts(model_dir: Path, contract_dir: Path, zones_file: Path, app: Path = Path("/app")) -> dict:
    """Full hashes, including provenance/feature derivation, before creating Spark."""
    from nyc_taxi.ml import pilot_model as model

    lock_file = contract_dir / "candidate_lock.json"
    feature_file = contract_dir / "feature_contract.json"
    if digest(lock_file) != CANDIDATE_LOCK_SHA256:
        raise ValueError("Pinned candidate-lock hash mismatch")
    lock = json.loads(lock_file.read_text(encoding="utf-8"))
    observed = tree_identity(model_dir)
    if (lock.get("training_run_id") != TRAINING_RUN_ID or observed["sha256"] != MODEL_SHA256
            or observed != lock.get("model_tree") or len(observed["files"]) != 40
            or lock.get("parameters") != model.GBT_PARAMETERS
            or digest(feature_file) != lock.get("feature_contract_sha256")
            or digest(zones_file) != lock.get("zone_lookup_sha256")):
        raise ValueError("Frozen model/feature/zone artifact identity mismatch")
    contract = json.loads(feature_file.read_text(encoding="utf-8"))
    if (contract != lock.get("features") or contract.get("vector") != list(model.FEATURE_COLUMNS)
            or contract.get("input") != list(model.INPUT_COLUMNS)
            or contract.get("prepared") != list(model.PREPARED_COLUMNS)):
        raise ValueError("Feature contract mismatch")
    required = {"src/nyc_taxi/ml/pilot_model.py", "src/nyc_taxi/etl/silver.py"}
    hashes = lock.get("core_file_sha256", {})
    if not required.issubset(hashes) or any(digest(app / name) != expected for name, expected in hashes.items()):
        raise ValueError("Frozen preprocessing/dependency source hash mismatch")
    with zones_file.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    lookup = {int(row["LocationID"]): row for row in rows}
    if len(rows) != 265 or len(lookup) != 265 or set(lookup) != set(range(1, 266)):
        raise ValueError("Zone IDs are not unique and complete")
    zones = [{"location_id": key, "zone": lookup[key]["Zone"], "borough": lookup[key]["Borough"]}
             for key in range(1, 264)]
    if any(not row["zone"] or not row["borough"] for row in zones):
        raise ValueError("A named zone is missing")
    return {"lock": lock, "zones": zones, "model_sha256": observed["sha256"]}


class FrozenPredictor:
    """One Spark session and fitted model per process; serialized bounded inference."""

    def __init__(self, model_dir: Path, contract_dir: Path, zones_file: Path):
        self.model_dir, self.contract_dir, self.zones_file = model_dir, contract_dir, zones_file
        self.ready = False
        self.status = "starting"
        self.spark = None
        self.model = None
        self.zones = []
        self.startup_seconds = None
        self.spark_application_id = None
        self._busy = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="fare-spark")
        self._closed = False
        self._load_started = False

    def load(self) -> None:
        if self._load_started:
            raise RuntimeError("Model load is once per process; no automatic reload/fallback")
        self._load_started = True
        tick = time.perf_counter()
        try:
            artifacts = validate_artifacts(self.model_dir, self.contract_dir, self.zones_file)
            from pyspark.sql import SparkSession
            import mlflow.spark
            from nyc_taxi.ml import pilot_model as feature_code

            self.spark = (SparkSession.builder.master("local[2]").appName("NYC-Taxi-Frozen-Fare-Inference")
                .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "2").config("spark.default.parallelism", "2")
                .config("spark.sql.warehouse.dir", "file:///tmp/fare-warehouse").getOrCreate())
            self.spark.sparkContext.setLogLevel("ERROR")
            self.model = mlflow.spark.load_model(str(self.model_dir), dfs_tmpdir="/tmp/fare-model-load")
            if [type(stage).__name__ for stage in self.model.stages] != ["StringIndexerModel", "StringIndexerModel", "VectorAssembler", "GBTRegressionModel"]:
                raise ValueError("Fitted model stage classes mismatch")
            for stage, source, target in zip(self.model.stages[:2], feature_code.CATEGORICAL_INPUT_COLUMNS, feature_code.CATEGORICAL_FEATURE_COLUMNS, strict=True):
                if (stage.getInputCol() != source or stage.getOutputCol() != target or stage.getHandleInvalid() != "keep"):
                    raise ValueError("Fitted indexer mismatch")
            if self.model.stages[2].getInputCols() != list(feature_code.FEATURE_COLUMNS):
                raise ValueError("Vector assembler feature allowlist mismatch")
            if any(self.model.stages[3].getOrDefault(key) != value for key, value in feature_code.GBT_PARAMETERS.items()):
                raise ValueError("Fitted GBT parameters mismatch")
            self._known_pickup = set(self.model.stages[0].labels)
            self._known_dropoff = set(self.model.stages[1].labels)
            self.zones = artifacts["zones"]
            self.spark_application_id = self.spark.sparkContext.applicationId
            self.startup_seconds = round(time.perf_counter() - tick, 6)
            if self._closed:
                self.spark.stop()
                return
            self.status, self.ready = "ready", True
        except Exception:
            self.status, self.ready = "model_load_failed", False
            if self.spark is not None:
                try:
                    self.spark.stop()
                except Exception:
                    pass
            # A stable public failure code, never a raw path/URI/exception string.
            print(json.dumps({"event": "model_load_failed", **identity()}), flush=True)

    def readiness(self) -> dict:
        return {"ready": self.ready, "status": self.status, "startup_seconds": self.startup_seconds,
                "model_load_count": int(self._load_started),
                "spark_application_id": self.spark_application_id if self.ready else None,
                **identity()}

    def _predict(self, inputs: dict, group_id: str) -> dict:
        try:
            from pyspark.sql.types import StructType, StructField, DoubleType, TimestampNTZType, IntegerType
            from nyc_taxi.ml.pilot_model import prepare_features, assert_category_metadata
            self.spark.sparkContext.setJobGroup(group_id, "single synthetic/user fare input", interruptOnCancel=True)
            schema = StructType([StructField("trip_distance", DoubleType(), False),
                                 StructField("pickup_datetime_local", TimestampNTZType(), False),
                                 StructField("pickup_location_id", IntegerType(), False),
                                 StructField("dropoff_location_id", IntegerType(), False)])
            source = self.spark.createDataFrame([(inputs["distance_miles"], inputs["pickup_datetime"],
                inputs["pickup_location_id"], inputs["dropoff_location_id"])], schema)
            # Exact frozen unfitted derivation, then saved fitted indexers/GBT.
            transformed = self.model.transform(prepare_features(source, include_labels=False))
            assert_category_metadata(transformed)
            rows = transformed.select("prediction").take(2)
            if len(rows) != 1 or not math.isfinite(float(rows[0]["prediction"])):
                raise ValueError("Prediction must be exactly one finite value")
            fare = float(rows[0]["prediction"])
            unseen = {"pickup": str(inputs["pickup_location_id"]) not in self._known_pickup,
                      "dropoff": str(inputs["dropoff_location_id"]) not in self._known_dropoff}
            warnings = input_warnings(inputs)
            if any(unseen.values()):
                warnings.append("Category PU/DO chưa thấy trong train: dùng unknown bucket đã fit, không fit lại.")
            if fare < 0:
                warnings.append("Prediction âm: giữ nguyên để báo lỗi mô hình; không dùng làm báo giá và không clip về 0.")
            if fare > 100:
                warnings.append("Cước cao: June chỉ có 15 dòng fare>100 USD; độ tin cậy hạn chế.")
            return {"fare_amount_usd": fare, "unit": "USD", "unseen_categories": unseen,
                    "warnings": warnings, **identity()}
        finally:
            try:
                self.spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            finally:
                self._busy.release()

    def predict(self, payload: dict, *, timeout: float = REQUEST_TIMEOUT_SECONDS) -> dict:
        inputs = validate_inputs(payload)
        if not self.ready or self._closed:
            raise InferenceError("model_not_ready")
        if not self._busy.acquire(blocking=False):
            raise InferenceError("service_busy")
        tick = time.perf_counter()
        group_id = "fare-" + uuid4().hex
        try:
            future = self._executor.submit(self._predict, inputs, group_id)
        except Exception:
            self._busy.release()
            raise InferenceError("service_unavailable") from None
        try:
            result = future.result(timeout=timeout)
        except FutureTimeout:
            self.ready, self.status = False, "prediction_timeout"
            def cancel():
                try:
                    self.spark.sparkContext.cancelJobGroup(group_id)
                except Exception:
                    pass
            # A dead JVM must not block returning the bounded HTTP timeout.
            threading.Thread(target=cancel, daemon=True).start()
            raise InferenceError("prediction_timeout", 504) from None
        except Exception:
            self.ready, self.status = False, "prediction_failed"
            raise InferenceError("prediction_failed") from None
        result["request_seconds"] = round(time.perf_counter() - tick, 6)
        return result

    def close(self) -> None:
        self._closed, self.ready = True, False
        if self.spark is not None:
            try:
                self.spark.sparkContext.cancelAllJobs()
            except Exception:
                pass
        self._executor.shutdown(wait=False, cancel_futures=True)
        if self.spark is not None:
            self.spark.stop()
