"""Bounded, fail-closed client for the explicitly selected frozen V2.2 model."""

from __future__ import annotations

import json
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from nyc_taxi.ml.pretrip_features import build_serving_features
from nyc_taxi.ml.pretrip_serving_contract import identity

from .prediction import (
    InferenceClient, PredictionError, MAX_RESPONSE_BYTES,
    METADATA_TIMEOUT_SECONDS, PREDICTION_TIMEOUT_SECONDS, _finite_number, _text,
)


class UnsupportedEstimate(PredictionError):
    """Role-specific serving policy, not a baseline or a predicted price."""

    def __init__(self, unseen: dict[str, bool]) -> None:
        self.unseen_categories = dict(unseen)
        roles = " và ".join(label for role, label in (("pickup", "đón"), ("dropoff", "đến")) if unseen[role])
        super().__init__(f"Chưa hỗ trợ ước tính: vùng {roles} chưa có trong dictionary huấn luyện tương ứng. Không có giá thay thế.")


def validate_identity(payload: dict[str, Any]) -> dict[str, str]:
    expected = identity()
    if any(payload.get(key) != value for key, value in expected.items()):
        raise PredictionError("Dịch vụ không khớp model/contract/calendar V2.2 đã khóa. Không hiển thị giá.")
    return expected


def _unseen(payload: dict[str, Any]) -> dict[str, bool]:
    values = payload.get("unseen_categories")
    if not isinstance(values, dict) or any(type(values.get(role)) is not bool for role in ("pickup", "dropoff")):
        raise PredictionError("Trạng thái vùng phục vụ V2.2 không hợp lệ. Không hiển thị giá.")
    return {role: values[role] for role in ("pickup", "dropoff")}


class PretripInferenceClient(InferenceClient):
    """Never falls back to V1, baseline, latest model, or a second request."""

    def _request(self, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None if payload is None else json.dumps(payload, allow_nan=False, separators=(",", ":")).encode("utf-8")
        request = Request(self.base_url + path, data=data, method="GET" if data is None else "POST",
                          headers={"Accept": "application/json", **({"Content-Type": "application/json"} if data else {})})
        try:
            with urlopen(request, timeout=METADATA_TIMEOUT_SECONDS if data is None else PREDICTION_TIMEOUT_SECONDS) as response:
                if response.status != 200:
                    raise PredictionError("Dịch vụ V2.2 chưa sẵn sàng. Không có giá thay thế.")
                raw = response.read(MAX_RESPONSE_BYTES + 1)
            return self._decode(raw)
        except HTTPError as error:
            if error.code == 422:
                try:
                    result = self._decode(error.read(MAX_RESPONSE_BYTES + 1))
                    validate_identity(result)
                    unseen = _unseen(result)
                    if (result.get("status") == "unsupported" and result.get("error") == "unsupported_categories"
                            and any(unseen.values()) and "fare_amount_usd" not in result):
                        raise UnsupportedEstimate(unseen)
                except UnsupportedEstimate:
                    raise
                except (PredictionError, ValueError, TypeError):
                    pass
            raise PredictionError("Dịch vụ V2.2 từ chối yêu cầu hoặc chưa sẵn sàng. Không có giá thay thế.") from None
        except (URLError, TimeoutError, OSError):
            raise PredictionError("Không kết nối được dịch vụ V2.2 hoặc đã hết thời gian chờ. Không tự gọi lại.") from None

    @staticmethod
    def _decode(raw: bytes) -> dict[str, Any]:
        try:
            if len(raw) > MAX_RESPONSE_BYTES:
                raise ValueError
            result = json.loads(raw.decode("utf-8"))
            if not isinstance(result, dict):
                raise ValueError
            return result
        except (UnicodeError, ValueError, TypeError):
            raise PredictionError("Phản hồi dịch vụ V2.2 không hợp lệ. Không hiển thị giá.") from None

    def ready(self) -> dict[str, Any]:
        result = self._request("/ready")
        model = validate_identity(result)
        if result.get("ready") is not True or type(result.get("fit_calls")) is not int or result["fit_calls"] != 0:
            raise PredictionError("Mô hình V2.2 chưa sẵn sàng hoặc không đúng chế độ frozen serving.")
        return {**model, "ready": True, "fit_calls": 0}

    def zones(self) -> dict[str, Any]:
        result = self._request("/zones")
        model = validate_identity(result)
        rows = result.get("zones")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 263:
            raise PredictionError("Danh sách vùng V2.2 không hợp lệ.")
        dictionaries = {}
        for role in ("pickup", "dropoff"):
            values = result.get(role + "_location_ids")
            if (not isinstance(values, list) or not values
                    or any(type(value) is not int or not 1 <= value <= 263 for value in values)
                    or len(set(values)) != len(values)):
                raise PredictionError("Dictionary vùng V2.2 không hợp lệ.")
            dictionaries[role + "_location_ids"] = sorted(values)
        checked, seen = [], set()
        for row in rows:
            if not isinstance(row, dict):
                raise PredictionError("Danh sách vùng V2.2 không hợp lệ.")
            location = row.get("location_id")
            if type(location) is not int or not 1 <= location <= 263 or location in seen:
                raise PredictionError("Danh sách vùng V2.2 không hợp lệ.")
            seen.add(location)
            supported = {"supported_" + role: location in dictionaries[role + "_location_ids"] for role in ("pickup", "dropoff")}
            if any(row.get(key) is not value for key, value in supported.items()):
                raise PredictionError("Vai trò vùng V2.2 không khớp dictionary đã fit.")
            checked.append({"location_id": location, "zone": _text(row.get("zone")), "borough": _text(row.get("borough")), **supported})
        if any(not set(values) <= seen for values in dictionaries.values()):
            raise PredictionError("Dictionary V2.2 thiếu tên vùng.")
        return {**model, **dictionaries, "zones": checked}

    def predict(self, **values: Any) -> dict[str, Any]:
        try:
            build_serving_features(values)
            if any(values[key] != identity()[key] for key in ("contract_version", "calendar_version", "calendar_sha256", "tzdb_sha256")):
                raise ValueError
        except (ValueError, TypeError, KeyError):
            raise PredictionError("Thời gian, múi giờ hoặc hợp đồng yêu cầu V2.2 không hợp lệ.") from None
        result = self._request("/predict", values)
        model = validate_identity(result)
        unseen = _unseen(result)
        if any(unseen.values()):
            raise UnsupportedEstimate(unseen)
        fare, elapsed = result.get("fare_amount_usd"), result.get("request_seconds")
        if (result.get("status") != "supported" or result.get("unit") != "USD"
                or not _finite_number(fare) or not _finite_number(elapsed) or elapsed < 0
                or result.get("submission") != values
                or any(result.get(key) != values[key] for key in ("submission_id", "pickup_datetime", "submitted_at", "mode"))):
            raise PredictionError("Kết quả V2.2 không khớp yêu cầu hoặc không hữu hạn. Không hiển thị giá.")
        warnings = result.get("warnings")
        if not isinstance(warnings, list) or len(warnings) > 20:
            raise PredictionError("Thông tin giới hạn V2.2 không hợp lệ.")
        return {**model, "status": "supported", "unit": "USD", "fare_amount_usd": float(fare),
                "request_seconds": float(elapsed), "unseen_categories": unseen,
                "warnings": [_text(value, maximum=1000) for value in warnings],
                **{key: values[key] for key in ("submission_id", "pickup_datetime", "submitted_at", "mode")}}
