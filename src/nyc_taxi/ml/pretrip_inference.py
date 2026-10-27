"""One explicitly selected frozen V2.2 predictor; no training, fallback or DB.

This serving policy rejects role-specific unseen zones *after* the holdout was
evaluated with every row. It does not change model artifacts or holdout metrics.
All feature derivation delegates to the unchanged train/serve V2 helper.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import threading
import time
from uuid import uuid4

from nyc_taxi.ml.inference import InferenceError, InputError, REQUEST_TIMEOUT_SECONDS
from nyc_taxi.ml import pretrip_features as features
from nyc_taxi.ml import pretrip_holdout_model as frozen
from nyc_taxi.ml import pretrip_serving_contract as contract


class UnsupportedCategories(InferenceError):
    """No estimate for a role absent from the corresponding fitted dictionary."""

    def __init__(self, unseen: dict[str, bool]):
        super().__init__("unsupported_categories", 422)
        self.public_payload = {"status": "unsupported", "unseen_categories": dict(unseen)}


class SpecialRateUnsupported(InferenceError):
    """Known airport special-rate route; never substitute a model or baseline."""

    def __init__(self):
        super().__init__("special_rate_no_estimate", 422)
        self.public_payload = {"status": "unsupported", "reason": "special_rate_route"}


def guard_special_rate(payload, zones):
    """Same known zone-level scope as the UI, independent of browser claims.

    Point-in-polygon and the explicit metered-taxi confirmation remain UI guards.
    This API rejects EWR (1) at either end and JFK (132) <-> Manhattan using the
    SHA-verified lookup, without trusting an extra browser borough/rate field.
    """
    pickup, dropoff = payload["pickup_location_id"], payload["dropoff_location_id"]
    boroughs = {row["location_id"]: row["borough"] for row in zones}
    if (1 in (pickup, dropoff)
            or pickup == 132 and boroughs.get(dropoff) == "Manhattan"
            or dropoff == 132 and boroughs.get(pickup) == "Manhattan"):
        raise SpecialRateUnsupported()


def validate_request(payload: dict, *, calendar=None, now: datetime | None = None) -> dict:
    """Return exactly the eight shared features, with an independent server clock."""
    try:
        derived = features.build_serving_features(payload, calendar=calendar)
        clock = now if now is not None else datetime.now(timezone.utc)
        if clock.tzinfo is None or clock.utcoffset().total_seconds() != 0:
            raise ValueError("server_clock_invalid")
        submitted = datetime.fromisoformat(payload["submitted_at"])
        age = (clock - submitted).total_seconds()
        if not -contract.MAX_FUTURE_CLOCK_SECONDS <= age <= contract.MAX_SUBMISSION_AGE_SECONDS:
            raise ValueError("stale_or_future_submission")
    except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
        raise InputError("Invalid V2 submission identity, zones, calendar or NYC time.") from None
    if tuple(derived) != features.FEATURE_COLUMNS:
        raise InputError("Invalid V2 feature allowlist.")
    return derived


def validate_artifacts(model_dir: Path, contract_dir: Path, zones_file: Path) -> dict:
    """Full candidate audit and public runtime derivation before starting Spark."""
    audit = frozen.audit_candidate(model_dir)
    if (audit["identities"]["model_tree_sha256"] != contract.MODEL_SHA256
            or audit["identities"]["artifact_manifest_sha256"] != contract.ARTIFACT_MANIFEST_SHA256):
        raise ValueError("pretrip_serving_candidate_identity_drift")
    if (audit["lineage"].get("feature_builder_sha256") != contract.FEATURE_BUILDER_SHA256
            or hashlib.sha256(Path(features.__file__).read_bytes()).hexdigest() != contract.FEATURE_BUILDER_SHA256):
        raise ValueError("pretrip_serving_feature_builder_drift")
    for name in ("pretrip_v2_contract.json", "pretrip_v2_calendar.json"):
        actual = hashlib.sha256((contract_dir / name).read_bytes()).hexdigest()
        if actual != frozen.PINNED_MEMBERS[name]:
            raise ValueError("pretrip_serving_runtime_contract_drift")
    calendar = features.load_calendar(model_dir / "pretrip_v2_calendar.json")
    if (calendar.sha256 != contract.CALENDAR_SHA256 or calendar.version != contract.CALENDAR_VERSION
            or features.timezone_identity()["tzif_sha256"] != contract.TZDB_SHA256):
        raise ValueError("pretrip_serving_calendar_or_tzif_drift")
    if hashlib.sha256(zones_file.read_bytes()).hexdigest() != contract.ZONE_LOOKUP_SHA256:
        raise ValueError("pretrip_serving_zone_lookup_identity_drift")
    with zones_file.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    lookup = {int(row["LocationID"]): row for row in rows}
    if len(rows) != 265 or set(lookup) != set(range(1, 266)):
        raise ValueError("pretrip_serving_zone_lookup_invalid")
    known = audit["dictionaries"]
    zones = [{"location_id": key, "zone": lookup[key]["Zone"], "borough": lookup[key]["Borough"],
              "supported_pickup": str(key) in known["pickup_zone"],
              "supported_dropoff": str(key) in known["dropoff_zone"]}
             for key in range(1, 264)]
    if any(not row["zone"] or not row["borough"] for row in zones):
        raise ValueError("pretrip_serving_zone_name_missing")
    return {"audit": audit, "calendar": calendar, "zones": zones}


def feature_frame(spark, rows: list[dict]):
    """Explicit eight-column schema: never accept a label/distance/lineage field."""
    from pyspark.sql.types import DoubleType, StringType, StructType, StructField
    if not rows or any(set(row) != set(features.FEATURE_COLUMNS) for row in rows):
        raise ValueError("pretrip_serving_feature_allowlist_invalid")
    schema = StructType([
        *[StructField(name, DoubleType(), nullable=False) for name in features.NUMERIC_FEATURE_COLUMNS],
        *[StructField(name, StringType(), nullable=False) for name in features.CATEGORICAL_FEATURE_COLUMNS],
    ])
    return spark.createDataFrame([tuple(row[name] for name in features.FEATURE_COLUMNS) for row in rows], schema)


class FrozenPretripPredictor:
    """Bounded serialized inference, one audited Spark model load per process."""

    identity = staticmethod(contract.identity)

    def __init__(self, model_dir: Path, contract_dir: Path, zones_file: Path, *, clock=None):
        self.model_dir, self.contract_dir, self.zones_file = model_dir, contract_dir, zones_file
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self.ready, self.status = False, "starting"
        self.spark = self.model = self.audit = self.calendar = None
        self.zones = []
        self.startup_seconds = self.spark_application_id = None
        self.fit_calls = 0
        self._known_pickup, self._known_dropoff = set(), set()
        self._busy = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="pretrip-spark")
        self._closed = self._load_started = False

    def load(self):
        if self._load_started:
            raise RuntimeError("Model load is once per process; no reload or fallback")
        self._load_started = True
        tick = time.perf_counter()
        try:
            verified = validate_artifacts(self.model_dir, self.contract_dir, self.zones_file)
            self.audit, self.calendar = verified["audit"], verified["calendar"]
            from pyspark.sql import SparkSession
            self.spark = (SparkSession.builder.master("local[2]").appName("NYC-Taxi-Frozen-Pretrip-V22")
                .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", features.TIMEZONE_NAME)
                .config("spark.sql.shuffle.partitions", "2").config("spark.default.parallelism", "2")
                .config("spark.sql.warehouse.dir", "file:///tmp/pretrip-warehouse").getOrCreate())
            self.spark.sparkContext.setLogLevel("ERROR")
            with frozen.no_fit_guard() as state:
                try:
                    self.model = frozen.load_candidate(self.model_dir, self.audit, self.spark)
                finally:
                    self.fit_calls += state["fit_calls"]
            self._known_pickup = set(self.audit["dictionaries"]["pickup_zone"])
            self._known_dropoff = set(self.audit["dictionaries"]["dropoff_zone"])
            self.zones = verified["zones"]
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
            print(json.dumps({"event": "model_load_failed", **contract.identity()}), flush=True)

    def readiness(self):
        return {"ready": self.ready, "status": self.status, "startup_seconds": self.startup_seconds,
                "model_load_count": int(self._load_started), "fit_calls": self.fit_calls,
                "spark_application_id": self.spark_application_id if self.ready else None, **contract.identity()}

    def zone_response(self):
        return {"zones": self.zones,
                "pickup_location_ids": sorted(map(int, self._known_pickup)),
                "dropoff_location_ids": sorted(map(int, self._known_dropoff)), **contract.identity()}

    def _predict(self, payload, derived, group_id):
        try:
            self.spark.sparkContext.setJobGroup(group_id, "single pretrip input", interruptOnCancel=True)
            with frozen.no_fit_guard() as state:
                try:
                    transformed = self.model.transform(feature_frame(self.spark, [derived]))
                    frozen.validate_nominal_metadata(transformed, self.audit)
                    rows = transformed.select("prediction").take(2)
                finally:
                    self.fit_calls += state["fit_calls"]
            if len(rows) != 1 or not math.isfinite(float(rows[0]["prediction"])):
                raise ValueError("pretrip_prediction_not_one_finite_row")
            value = float(rows[0]["prediction"])
            warnings = []
            if value < 0:
                warnings.append("Model returned a negative fare; not a usable quote and not clipped.")
            return {"status": "supported", "fare_amount_usd": value, "unit": "USD",
                    "unseen_categories": {"pickup": False, "dropoff": False}, "warnings": warnings,
                    "submission": dict(payload),
                    **{name: payload[name] for name in ("submission_id", "pickup_datetime", "submitted_at", "mode")},
                    **contract.identity()}
        finally:
            try:
                self.spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            finally:
                self._busy.release()

    def predict(self, payload, *, timeout=REQUEST_TIMEOUT_SECONDS):
        derived = validate_request(payload, calendar=self.calendar, now=self._clock())
        if not self.ready or self._closed:
            raise InferenceError("model_not_ready")
        unseen = {"pickup": derived["pickup_zone"] not in self._known_pickup,
                  "dropoff": derived["dropoff_zone"] not in self._known_dropoff}
        if any(unseen.values()):
            raise UnsupportedCategories(unseen)
        guard_special_rate(payload, self.zones)
        if not self._busy.acquire(blocking=False):
            raise InferenceError("service_busy")
        tick = time.perf_counter()
        group_id = "pretrip-" + uuid4().hex
        try:
            future = self._executor.submit(self._predict, dict(payload), derived, group_id)
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
            threading.Thread(target=cancel, daemon=True).start()
            raise InferenceError("prediction_timeout", 504) from None
        except Exception:
            self.ready, self.status = False, "prediction_failed"
            raise InferenceError("prediction_failed") from None
        result["request_seconds"] = round(time.perf_counter() - tick, 6)
        return result

    def close(self):
        self._closed, self.ready = True, False
        if self.spark is not None:
            try:
                self.spark.sparkContext.cancelAllJobs()
            except Exception:
                pass
        self._executor.shutdown(wait=False, cancel_futures=True)
        if self.spark is not None:
            self.spark.stop()
