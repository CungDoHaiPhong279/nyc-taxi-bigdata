"""Small, bounded HTTP client for the frozen fare model; no Spark or database."""

from __future__ import annotations

from datetime import datetime
import json
import math
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


MODEL_SHA256 = "759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee"
TRAINING_RUN_ID = "d2772f1904d7472490a73b0ac1c0c5d4"
EVALUATION_RUN_ID = "58d5008f0ed048f58599a859877ae336"
DEFAULT_INFERENCE_URL = "http://fare-inference:8000"
METADATA_TIMEOUT_SECONDS = 5
PREDICTION_TIMEOUT_SECONDS = 45
MAX_RESPONSE_BYTES = 262_144


class PredictionError(RuntimeError):
    """Only safe, user-facing messages are exposed to the dashboard."""


def selected_model_version() -> str:
    """Explicit opt-in only; an invalid setting must never fall back to V1."""
    value = os.environ.get("FARE_MODEL_VERSION", "v1")
    if value not in {"v1", "v2.2"}:
        raise PredictionError("FARE_MODEL_VERSION phải là v1 hoặc v2.2; không tự chọn mô hình thay thế.")
    return value


def _text(value: Any, *, maximum: int = 200) -> str:
    if (
        not isinstance(value, str) or not value.strip() or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise PredictionError("Phản hồi dịch vụ ước tính không hợp lệ. Không hiển thị giá.")
    return value


def _lineage(payload: dict[str, Any]) -> dict[str, str]:
    if (
        payload.get("training_run_id") != TRAINING_RUN_ID
        or payload.get("evaluation_run_id") != EVALUATION_RUN_ID
        or payload.get("model_sha256") != MODEL_SHA256
    ):
        raise PredictionError("Dịch vụ không khớp mô hình đã kiểm chứng. Không hiển thị giá.")
    return {"training_run_id": TRAINING_RUN_ID, "evaluation_run_id": EVALUATION_RUN_ID, "model_sha256": MODEL_SHA256}


def _finite_number(value: Any) -> bool:
    try:
        return type(value) in {int, float} and math.isfinite(value)
    except OverflowError:
        return False


def build_prediction_request(
    *, distance_miles: float, pickup_datetime: datetime | str,
    pickup_location_id: int, dropoff_location_id: int,
) -> dict[str, Any]:
    """Validate the exact serving inputs without inventing a route or timestamp."""
    if (
        not _finite_number(distance_miles) or not 0 <= distance_miles <= 200
    ):
        raise PredictionError("Khoảng cách phải là số hữu hạn từ 0 đến 200 mile.")
    for value in (pickup_location_id, dropoff_location_id):
        if type(value) is not int or not 1 <= value <= 263:
            raise PredictionError("Điểm đón và điểm đến phải là zone có tên, ID từ 1 đến 263.")
    if isinstance(pickup_datetime, str):
        try:
            if "T" not in pickup_datetime:
                raise ValueError
            pickup_datetime = datetime.fromisoformat(pickup_datetime)
        except ValueError:
            raise PredictionError("Thời gian đón phải là ngày giờ địa phương NYC hợp lệ trong năm 2025.") from None
    if (
        not isinstance(pickup_datetime, datetime) or pickup_datetime.tzinfo is not None
        or pickup_datetime.year != 2025
    ):
        raise PredictionError("Chỉ hỗ trợ giờ địa phương NYC trong năm 2025, không kèm UTC offset.")
    return {
        "distance_miles": float(distance_miles), "pickup_datetime": pickup_datetime.isoformat(),
        "pickup_location_id": pickup_location_id, "dropoff_location_id": dropoff_location_id,
    }


class InferenceClient:
    def __init__(self, base_url: str | None = None) -> None:
        selected = base_url if base_url is not None else os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL)
        try:
            parsed = urlsplit(selected)
            valid = (
                isinstance(selected, str) and not any(character.isspace() or ord(character) < 32 for character in selected)
                and parsed.scheme in {"http", "https"} and bool(parsed.hostname)
                and parsed.username is None and parsed.password is None
                and parsed.path in {"", "/"} and not parsed.query and not parsed.fragment
                and (parsed.port is None or 1 <= parsed.port <= 65535)
            )
        except (TypeError, ValueError):
            valid = False
        if not valid:
            raise PredictionError("Địa chỉ dịch vụ ước tính không hợp lệ. Hãy kiểm tra cấu hình.")
        self.base_url = selected.rstrip("/")

    def _request(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if payload is None else json.dumps(payload, allow_nan=False, separators=(",", ":")).encode("utf-8")
        request = Request(
            self.base_url + path, data=data, method="GET" if data is None else "POST",
            headers={"Accept": "application/json", **({"Content-Type": "application/json"} if data is not None else {})},
        )
        timeout = METADATA_TIMEOUT_SECONDS if data is None else PREDICTION_TIMEOUT_SECONDS
        try:
            with urlopen(request, timeout=timeout) as response:
                if response.status != 200:
                    raise PredictionError("Dịch vụ ước tính chưa sẵn sàng. Hãy thử lại; không có giá thay thế.")
                raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                raise PredictionError("Phản hồi dịch vụ quá lớn. Không hiển thị giá.")
            result = json.loads(raw.decode("utf-8"))
            if not isinstance(result, dict):
                raise ValueError
            return result
        except HTTPError as error:
            if error.code in {400, 422}:
                raise PredictionError("Dịch vụ từ chối yêu cầu. Kiểm tra khoảng cách, năm 2025 và các zone đã chọn.") from None
            raise PredictionError("Dịch vụ hoặc mô hình chưa sẵn sàng. Hãy thử lại; không có giá thay thế.") from None
        except (URLError, TimeoutError, OSError):
            raise PredictionError("Không kết nối được dịch vụ hoặc đã hết thời gian chờ. Hãy thử lại.") from None
        except (UnicodeError, ValueError, TypeError):
            raise PredictionError("Phản hồi dịch vụ ước tính không hợp lệ. Không hiển thị giá.") from None

    def ready(self) -> dict[str, Any]:
        payload = self._request("/ready")
        if payload.get("ready") is not True:
            raise PredictionError("Mô hình ước tính chưa sẵn sàng. Hãy thử lại.")
        lineage = _lineage(payload)
        return {"ready": True, **lineage}

    def zones(self) -> dict[str, Any]:
        payload = self._request("/zones")
        lineage = _lineage(payload)
        rows = payload.get("zones")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 263:
            raise PredictionError("Danh sách zone từ dịch vụ không hợp lệ.")
        zones, seen = [], set()
        for row in rows:
            if not isinstance(row, dict):
                raise PredictionError("Danh sách zone từ dịch vụ không hợp lệ.")
            location = row.get("location_id")
            if type(location) is not int or not 1 <= location <= 263 or location in seen:
                raise PredictionError("Danh sách zone từ dịch vụ không hợp lệ.")
            seen.add(location)
            zones.append({"location_id": location, "zone": _text(row.get("zone")), "borough": _text(row.get("borough"))})
        return {**lineage, "zones": sorted(zones, key=lambda row: row["location_id"])}

    def predict(self, **values: Any) -> dict[str, Any]:
        request = build_prediction_request(**values)
        payload = self._request("/predict", request)
        lineage = _lineage(payload)
        fare, elapsed = payload.get("fare_amount_usd"), payload.get("request_seconds")
        if (
            not _finite_number(fare)
            or not _finite_number(elapsed) or elapsed < 0
            or payload.get("unit") != "USD"
        ):
            raise PredictionError("Kết quả mô hình không phải giá trị USD hữu hạn. Không hiển thị giá.")
        unseen, warnings = payload.get("unseen_categories"), payload.get("warnings")
        if (
            not isinstance(unseen, dict) or type(unseen.get("pickup")) is not bool
            or type(unseen.get("dropoff")) is not bool
            or not isinstance(warnings, list) or len(warnings) > 20
        ):
            raise PredictionError("Thông tin giới hạn mô hình không hợp lệ. Không hiển thị giá.")
        safe_warnings = [_text(message, maximum=1000) for message in warnings]
        return {
            **lineage, "fare_amount_usd": float(fare), "unit": "USD",
            "request_seconds": float(elapsed),
            "unseen_categories": {"pickup": unseen["pickup"], "dropoff": unseen["dropoff"]},
            "warnings": safe_warnings,
        }
