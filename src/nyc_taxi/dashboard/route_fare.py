"""Session-scoped address-to-fare orchestration for the frozen model.

No provider response, token, or address is logged or persisted.  The injected
geometry/router/predictor make this flow testable without any network access.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import hashlib
import json
import math
from types import MappingProxyType
from typing import Any, Mapping, MutableMapping
from uuid import UUID

from .mapbox_client import MapboxError, validate_coordinates
from .prediction import PredictionError, build_prediction_request
from .taxi_zones import GeometryError


METERS_PER_MILE = 1609.344
NYC_BOUNDS = (-74.30, 40.45, -73.65, 40.95)
NYC_BOROUGHS = frozenset({"Manhattan", "Bronx", "Brooklyn", "Queens", "Staten Island"})
MAX_SESSION_SUBMISSIONS = 32
MAX_ROUTE_GEOMETRY_POINTS = 20_000
_SESSION_KEY = "_route_fare_submissions"


class RouteFareError(ValueError):
    """A safe, user-facing failure; upstream response details are never reused."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _thaw(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw(item) for item in value]
    return value


@dataclass(frozen=True)
class RouteFareEstimate:
    """Immutable submitted inputs and their result, never the edited form draft."""

    pickup: Mapping[str, Any]
    dropoff: Mapping[str, Any]
    pickup_datetime: str
    distance_meters: float
    distance_km: float
    request: Mapping[str, Any]
    prediction: Mapping[str, Any]
    route_geometry: Mapping[str, Any]
    warnings: tuple[str, ...]
    submission_id: str | None = None
    model_version: str = "v1"
    time_selection: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """A fresh display copy; mutations cannot change the saved submission."""
        result = {
            "pickup": _thaw(self.pickup), "dropoff": _thaw(self.dropoff),
            "pickup_datetime": self.pickup_datetime,
            "distance_meters": self.distance_meters, "distance_km": self.distance_km,
            "request": _thaw(self.request), "prediction": _thaw(self.prediction),
            "route_geometry": _thaw(self.route_geometry),
            "warnings": list(self.warnings),
            "submission_id": self.submission_id,
        }
        if self.model_version != "v1":
            result.update(model_version=self.model_version, time_selection=_thaw(self.time_selection))
        return result


@dataclass(frozen=True)
class SubmissionOutcome:
    success: bool
    estimate: RouteFareEstimate | None = None
    error: str | None = None


def _route_geometry(value: Any) -> Mapping[str, Any]:
    """Preserve the one Directions result, also validating injected transports.

    This is a bounded display snapshot, not another routing request or a repair
    of a provider geometry. Reject malformed geometry before model inference so
    the displayed route and fare always belong to the same complete result.
    """
    if not isinstance(value, dict) or value.get("type") != "LineString":
        raise RouteFareError("Hình dạng tuyến đường không hợp lệ. Chưa gọi mô hình dự đoán.")
    points = value.get("coordinates")
    if not isinstance(points, list) or not 2 <= len(points) <= MAX_ROUTE_GEOMETRY_POINTS:
        raise RouteFareError("Hình dạng tuyến đường không hợp lệ. Chưa gọi mô hình dự đoán.")
    normalized = []
    for point in points:
        if not isinstance(point, list) or len(point) != 2:
            raise RouteFareError("Hình dạng tuyến đường không hợp lệ. Chưa gọi mô hình dự đoán.")
        try:
            normalized.append(validate_coordinates(*point))
        except (MapboxError, TypeError, ValueError, OverflowError):
            raise RouteFareError("Hình dạng tuyến đường không hợp lệ. Chưa gọi mô hình dự đoán.") from None
    return _freeze({"type": "LineString", "coordinates": normalized})


def _selection(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RouteFareError("Hãy chọn địa chỉ từ danh sách gợi ý.", code="invalid_selection")
    label = value.get("label")
    if (
        not isinstance(label, str) or not label.strip() or len(label) > 500
        or any(ord(character) < 32 for character in label)
    ):
        raise RouteFareError("Lựa chọn địa chỉ không hợp lệ. Hãy chọn lại từ danh sách gợi ý.", code="invalid_selection")
    longitude, latitude = value.get("longitude"), value.get("latitude")
    try:
        validate_coordinates(longitude, latitude)
    except (MapboxError, TypeError, ValueError, OverflowError):
        raise RouteFareError("Tọa độ địa chỉ không hợp lệ. Hãy chọn lại từ danh sách gợi ý.", code="invalid_selection") from None
    west, south, east, north = NYC_BOUNDS
    if not west <= longitude <= east or not south <= latitude <= north:
        raise RouteFareError("Địa chỉ nằm ngoài vùng NYC được hỗ trợ. Hãy chọn một địa chỉ trong NYC.", code="outside_nyc")
    return {"label": label.strip(), "longitude": float(longitude), "latitude": float(latitude)}


def _pickup_time(value: Any) -> datetime:
    if isinstance(value, str):
        try:
            if "T" not in value:
                raise ValueError
            value = datetime.fromisoformat(value)
        except ValueError:
            raise RouteFareError("Thời gian đón phải là ngày giờ địa phương NYC hợp lệ.") from None
    if (
        not isinstance(value, datetime) or value.tzinfo is not None
        or not datetime(2025, 1, 1) <= value < datetime(2025, 7, 1)
    ):
        raise RouteFareError("Demo chỉ hỗ trợ giờ địa phương NYC từ tháng 1 đến hết tháng 6/2025, không kèm UTC offset.")
    return value


def _zone(geometry: Any, selected: Mapping[str, Any]) -> dict[str, Any]:
    try:
        result = geometry.resolve(selected["longitude"], selected["latitude"])
    except GeometryError as error:
        failures = {
            "outside_zones": "Điểm này nằm ngoài vùng Taxi Zones mà mô hình hỗ trợ. Hãy chọn địa chỉ hoặc cổng vào cụ thể khác trong vùng hỗ trợ.",
            "boundary": "Điểm này nằm trên ranh giới Taxi Zones nên chưa xác định được khu vực. Hãy chọn địa chỉ hoặc cổng vào cụ thể khác.",
            "overlap": "Điểm này thuộc vùng ranh giới chồng lấn nên chưa xác định duy nhất được Taxi Zone. Hãy chọn địa chỉ hoặc cổng vào cụ thể khác.",
        }
        if error.code in failures:
            raise RouteFareError(failures[error.code], code=error.code) from None
        raise RouteFareError("Chưa kiểm tra được vùng hỗ trợ do geometry không hợp lệ hoặc chưa sẵn sàng. Không tự thay bằng khu vực gần nhất.", code="invalid_geometry") from None
    return _validated_zone(result)


def _validated_zone(result: Any) -> dict[str, Any]:
    """Validate local metadata without trusting text from injected geometry."""
    if not isinstance(result, dict):
        raise RouteFareError("Thông tin Taxi Zone không hợp lệ.", code="invalid_geometry")
    location_id = result.get("location_id")
    borough, zone = result.get("borough"), result.get("zone")
    if type(location_id) is not int or not 1 <= location_id <= 263:
        raise RouteFareError("Địa chỉ không thuộc Taxi Zone được mô hình hỗ trợ. Hãy chọn địa chỉ khác.", code="outside_zones")
    if location_id == 1:
        raise RouteFareError("Tuyến Newark/EWR áp dụng cước đặc biệt, không thuộc standard-rate của mô hình.", code="special_rate")
    if not isinstance(borough, str) or borough not in NYC_BOROUGHS or not isinstance(zone, str) or not zone.strip():
        raise RouteFareError("Địa chỉ không thuộc năm borough NYC được hỗ trợ. Hãy chọn địa chỉ khác trong NYC.", code="outside_nyc")
    return {"location_id": location_id, "borough": borough, "zone": zone}


def validate_address_support(selection: Any, geometry: Any) -> dict[str, Any]:
    """Check one selection locally, without Search, routing or model requests.

    The returned zone is a fresh validated mapping. Boundary, hole and outside
    points remain rejected; there is no snapping or nearest-zone fallback.
    """
    return _zone(geometry, _selection(selection))


def validate_standard_route(pickup_zone: Any, dropoff_zone: Any) -> None:
    """Fail closed on known special-rate pairs using local zone metadata only."""
    pickup_zone, dropoff_zone = _validated_zone(pickup_zone), _validated_zone(dropoff_zone)
    if (
        pickup_zone["location_id"] == 132 and dropoff_zone["borough"] == "Manhattan"
        or dropoff_zone["location_id"] == 132 and pickup_zone["borough"] == "Manhattan"
    ):
        raise RouteFareError("Tuyến JFK ↔ Manhattan dùng cước cố định, không thuộc standard-rate của mô hình.", code="special_rate")


class RouteFareCoordinator:
    """Call Directions once, then the unchanged four-feature /predict contract."""

    def __init__(self, *, router: Any, geometry: Any, predictor: Any) -> None:
        self.router, self.geometry, self.predictor = router, geometry, predictor

    def estimate(self, payload: Mapping[str, Any]) -> RouteFareEstimate:
        if not isinstance(payload, Mapping):
            raise RouteFareError("Yêu cầu ước tính không hợp lệ.")
        pickup = _selection(payload.get("pickup"))
        dropoff = _selection(payload.get("dropoff"))
        pickup_time = _pickup_time(payload.get("pickup_datetime"))
        if payload.get("standard_rate") is not True:
            raise RouteFareError("Hãy xác nhận chuyến yellow taxi dùng cước đồng hồ standard-rate; không hỗ trợ cước thỏa thuận, nhóm hoặc upfront.")
        pickup_zone = validate_address_support(pickup, self.geometry)
        dropoff_zone = validate_address_support(dropoff, self.geometry)
        validate_standard_route(pickup_zone, dropoff_zone)
        warnings = [
            "Quãng đường là tuyến ô tô dự kiến; có thể khác quãng đường đồng hồ taxi thực tế.",
            "Đây là demo lịch sử Jan–Jun/2025. Cước cơ bản USD chưa gồm tip, toll và phụ phí.",
        ]
        if 132 in {pickup_zone["location_id"], dropoff_zone["location_id"]} or 138 in {pickup_zone["location_id"], dropoff_zone["location_id"]}:
            warnings.append("Chuyến sân bay có thể có phí sân bay và phụ phí chưa nằm trong cước cơ bản ước tính.")
        try:
            route = self.router.directions(
                (pickup["longitude"], pickup["latitude"]),
                (dropoff["longitude"], dropoff["latitude"]),
            )
        except MapboxError:
            raise RouteFareError("Không lấy được tuyến ô tô từ Mapbox. Hãy thử lại bằng lần gửi mới; không có giá thay thế.") from None
        distance = route.get("distance_meters") if isinstance(route, dict) else None
        try:
            valid_distance = type(distance) in {int, float} and math.isfinite(distance) and 0 <= distance <= 200 * METERS_PER_MILE
        except OverflowError:
            valid_distance = False
        if not valid_distance:
            raise RouteFareError("Quãng đường routing phải hữu hạn và nằm trong giới hạn 0–200 mile của mô hình.")
        route_geometry = _route_geometry(route.get("geometry"))
        # Never round either unit before inference.  Presentation rounding is UI-only.
        request = build_prediction_request(
            distance_miles=distance / METERS_PER_MILE, pickup_datetime=pickup_time,
            pickup_location_id=pickup_zone["location_id"], dropoff_location_id=dropoff_zone["location_id"],
        )
        try:
            prediction = self.predictor.predict(**request)
        except PredictionError:
            raise RouteFareError("Dịch vụ mô hình chưa trả được kết quả hợp lệ. Không có giá thay thế; hãy thử lại bằng lần gửi mới.") from None
        # InferenceClient validates the pinned identity and finite unmodified fare.
        if not isinstance(prediction, dict):
            raise RouteFareError("Phản hồi mô hình không hợp lệ. Không hiển thị giá.")
        return RouteFareEstimate(
            pickup=_freeze({**pickup, **pickup_zone}), dropoff=_freeze({**dropoff, **dropoff_zone}),
            pickup_datetime=request["pickup_datetime"], distance_meters=float(distance),
            distance_km=distance / 1000, request=_freeze(request),
            prediction=_freeze(prediction), route_geometry=route_geometry,
            warnings=tuple(warnings),
        )


class PretripRouteFareCoordinator(RouteFareCoordinator):
    """Route is display-only; the exact V2 request has no distance predictor."""

    def __init__(self, *, router: Any, geometry: Any, predictor: Any, metadata: Mapping[str, Any] | None = None) -> None:
        super().__init__(router=router, geometry=geometry, predictor=predictor)
        self.metadata = metadata

    def estimate(self, payload: Mapping[str, Any]) -> RouteFareEstimate:
        from nyc_taxi.ml.pretrip_features import build_serving_features
        from .pretrip_prediction import UnsupportedEstimate

        if not isinstance(payload, Mapping):
            raise RouteFareError("Yêu cầu ước tính không hợp lệ.")
        pickup, dropoff = _selection(payload.get("pickup")), _selection(payload.get("dropoff"))
        pickup_zone, dropoff_zone = validate_address_support(pickup, self.geometry), validate_address_support(dropoff, self.geometry)
        validate_standard_route(pickup_zone, dropoff_zone)
        if payload.get("standard_rate") is not True:
            raise RouteFareError("Hãy xác nhận chuyến taxi vàng dùng cước đồng hồ tiêu chuẩn, không phải cước đặc biệt.")
        request = payload.get("pretrip_request")
        try:
            if (not isinstance(request, dict) or request.get("pickup_location_id") != pickup_zone["location_id"]
                    or request.get("dropoff_location_id") != dropoff_zone["location_id"]):
                raise ValueError
            build_serving_features(request)
        except (ValueError, TypeError, KeyError):
            raise RouteFareError("Thời gian, vùng hoặc hợp đồng V2.2 không khớp yêu cầu đã gửi.") from None
        try:
            # Metadata is public and bounded, not a price cache. This final
            # role-specific check precedes both Directions and prediction.
            metadata = self.metadata if self.metadata is not None else self.predictor.zones()
            unseen = {role: zone["location_id"] not in metadata[role + "_location_ids"]
                      for role, zone in (("pickup", pickup_zone), ("dropoff", dropoff_zone))}
            if any(unseen.values()):
                raise UnsupportedEstimate(unseen)
        except UnsupportedEstimate as error:
            raise RouteFareError(str(error), code="unsupported_categories") from None
        except (PredictionError, KeyError, TypeError):
            raise RouteFareError("Chưa xác minh được phạm vi phục vụ V2.2. Không có giá thay thế.") from None
        try:
            route = self.router.directions((pickup["longitude"], pickup["latitude"]), (dropoff["longitude"], dropoff["latitude"]))
        except MapboxError:
            raise RouteFareError("Không lấy được tuyến ô tô từ Mapbox. Không tự gọi lại hoặc tạo giá thay thế.") from None
        distance = route.get("distance_meters") if isinstance(route, dict) else None
        if (type(distance) not in (int, float) or not math.isfinite(distance)
                or not 0 <= distance <= 200 * METERS_PER_MILE):
            raise RouteFareError("Quãng đường tham khảo phải hữu hạn và trong vùng hỗ trợ 0–200 mile.")
        route_geometry = _route_geometry(route.get("geometry"))
        try:
            prediction = self.predictor.predict(**request)
        except UnsupportedEstimate as error:
            raise RouteFareError(str(error), code="unsupported_categories") from None
        except PredictionError:
            raise RouteFareError("Dịch vụ V2.2 chưa trả được kết quả hợp lệ. Không có giá thay thế.") from None
        if not isinstance(prediction, dict):
            raise RouteFareError("Phản hồi V2.2 không hợp lệ. Không hiển thị giá.")
        warnings = []
        if {132, 138} & {pickup_zone["location_id"], dropoff_zone["location_id"]}:
            warnings.append("Chuyến sân bay có thể có phí sân bay và phụ phí chưa nằm trong cước cơ bản ước tính.")
        return RouteFareEstimate(
            pickup=_freeze({**pickup, **pickup_zone}), dropoff=_freeze({**dropoff, **dropoff_zone}),
            pickup_datetime=request["pickup_datetime"], distance_meters=float(distance), distance_km=distance / 1000,
            request=_freeze(request), prediction=_freeze(prediction), route_geometry=route_geometry,
            warnings=tuple(warnings), model_version="v2.2", time_selection=_freeze(payload.get("time_selection")),
        )


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError


def execute_submission(
    state: MutableMapping[str, Any], submission_id: str,
    payload: Mapping[str, Any], coordinator: RouteFareCoordinator,
) -> SubmissionOutcome:
    """Reuse a session event including failures; a deliberate retry needs a new UUID.

    An event UUID is bound to the full submitted payload, so form edits cannot
    relabel a saved result.  State stays in the browser's Streamlit session;
    there is no process-wide, persistent, or cross-user address cache.
    """
    try:
        if not isinstance(submission_id, str):
            raise ValueError
        submission_id = str(UUID(submission_id))
        canonical = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), allow_nan=False,
            default=_json_default,
        )
        if len(canonical) > 65_536:
            raise ValueError
    except (ValueError, TypeError, OverflowError):
        raise RouteFareError("Lần gửi biểu mẫu không hợp lệ. Hãy gửi lại biểu mẫu.") from None
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    entries = state.setdefault(_SESSION_KEY, {})
    existing = entries.get(submission_id)
    if existing is not None:
        if existing["fingerprint"] != fingerprint:
            raise RouteFareError("Lần gửi đã gắn với bộ đầu vào khác. Hãy gửi biểu mẫu mới.")
        return existing["outcome"]
    if len(entries) >= MAX_SESSION_SUBMISSIONS:
        # Retain earlier UUIDs so a delayed component event cannot accidentally
        # replay Directions/predict after an eviction. Bound address retention
        # within this session by rejecting new work instead.
        raise RouteFareError("Phiên đã đạt giới hạn số lần gửi. Hãy mở phiên mới để tiếp tục.")
    outcome = SubmissionOutcome(False, error="Lần gửi trước chưa hoàn tất. Hãy gửi biểu mẫu mới nếu muốn thử lại.")
    entries[submission_id] = {"fingerprint": fingerprint, "outcome": outcome}
    try:
        outcome = SubmissionOutcome(
            True, estimate=replace(coordinator.estimate(payload), submission_id=submission_id),
        )
    except RouteFareError as error:
        outcome = SubmissionOutcome(False, error=str(error))
    except Exception:
        # Injected transports and unexpected errors may contain credentials or
        # personal input; never put their messages/tracebacks in UI/session logs.
        outcome = SubmissionOutcome(False, error="Không hoàn tất được yêu cầu địa chỉ → cước. Hãy gửi biểu mẫu mới để thử lại.")
    entries[submission_id]["outcome"] = outcome
    return outcome
