"""Bounded Mapbox requests with injectable transport and safe public errors.

Search results are temporary session data: this module has no logging, disk cache,
or retry loop. The caller owns the UUID used throughout suggest -> retrieve.
"""

from __future__ import annotations

import json
import math
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import UUID


MAPBOX_ORIGIN = "https://api.mapbox.com"
MAX_RESPONSE_BYTES = 1_048_576
SEARCH_TIMEOUT_SECONDS = 10
ROUTING_TIMEOUT_SECONDS = 20
# Explicit NYC bounds/proximity avoid Mapbox's default IP-based search bias.
# These are fixed regional search settings, never coordinates from a user's trip.
NYC_SEARCH_BBOX = "-74.3,40.45,-73.65,40.95"
NYC_SEARCH_PROXIMITY = "-73.9857,40.7484"


class MapboxError(RuntimeError):
    """Safe error text; raw URLs, tokens and response bodies are never exposed."""


def validate_coordinates(longitude: Any, latitude: Any) -> tuple[float, float]:
    """Return WGS84 longitude/latitude, excluding booleans and nonfinite values."""
    try:
        valid = (
            type(longitude) in {int, float} and type(latitude) in {int, float}
            and math.isfinite(longitude) and math.isfinite(latitude)
            and -180 <= longitude <= 180 and -90 <= latitude <= 90
        )
    except OverflowError:
        valid = False
    if not valid:
        raise MapboxError("Tọa độ phải là kinh độ/vĩ độ WGS84 hữu hạn và đúng giới hạn.")
    return float(longitude), float(latitude)


def _session_token(value: Any) -> str:
    try:
        parsed = UUID(value) if isinstance(value, str) else None
        valid = parsed is not None and parsed.version == 4 and str(parsed) == value.lower()
    except (ValueError, AttributeError):
        valid = False
    if not valid:
        raise MapboxError("Phiên tìm địa chỉ không hợp lệ. Hãy bắt đầu tìm kiếm lại.")
    return value


def _text(value: Any, *, maximum: int = 1000) -> str:
    if (
        not isinstance(value, str) or not value.strip() or len(value) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise MapboxError("Phản hồi Mapbox không hợp lệ. Chưa có kết quả tuyến đường.")
    return value.strip()


def _label(properties: dict[str, Any]) -> str:
    name = _text(properties.get("name"), maximum=300)
    address = properties.get("full_address")
    if not address:
        # full_address is optional. Keep the street when only address and the
        # regional context are returned, so similarly named POIs are distinct.
        parts = [properties.get("address"), properties.get("place_formatted")]
        address = ", ".join(_text(part) for part in parts if part)
    if address:
        address = _text(address)
        return address if address.startswith(name) else f"{name}, {address}"
    return name


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        # Never forward a request containing the token to a redirected host.
        return None


def _reject_constant(value: str) -> None:
    raise ValueError("Nonfinite JSON number")


def _http_json(url: str, *, timeout: int, max_bytes: int) -> dict[str, Any]:
    request = Request(url, headers={"Accept": "application/json"}, method="GET")
    with build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
        if response.status != 200:
            raise HTTPError(url, response.status, "Mapbox response", None, None)
        raw = response.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("Response too large")
    result = json.loads(raw.decode("utf-8"), parse_constant=_reject_constant)
    if not isinstance(result, dict):
        raise ValueError("Expected JSON object")
    return result


class MapboxClient:
    def __init__(self, token: str, *, http: Callable[..., dict[str, Any]] | None = None) -> None:
        if (
            not isinstance(token, str) or not token or len(token) > 4096
            or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in token)
        ):
            raise MapboxError("Chưa cấu hình token Mapbox hợp lệ.")
        self._token = token
        self._http = _http_json if http is None else http

    def __repr__(self) -> str:
        return "MapboxClient(token=<redacted>)"

    def _request(self, path: str, parameters: dict[str, Any], *, timeout: int) -> dict[str, Any]:
        url = MAPBOX_ORIGIN + path + "?" + urlencode({**parameters, "access_token": self._token})
        try:
            result = self._http(url, timeout=timeout, max_bytes=MAX_RESPONSE_BYTES)
            if not isinstance(result, dict):
                raise ValueError
            return result
        except HTTPError as error:
            if error.code in {401, 403}:
                raise MapboxError("Mapbox từ chối token hoặc quyền truy cập. Hãy kiểm tra cấu hình.") from None
            if error.code == 429:
                raise MapboxError("Mapbox đã giới hạn số yêu cầu. Hãy thử lại sau.") from None
            raise MapboxError("Mapbox chưa sẵn sàng hoặc đã từ chối yêu cầu. Hãy thử lại sau.") from None
        except (URLError, TimeoutError, OSError):
            raise MapboxError("Không kết nối được Mapbox hoặc đã hết thời gian chờ.") from None
        except Exception:
            # Injected transports and malformed responses must not leak a token,
            # searched address, response body, or credential-bearing request URL.
            raise MapboxError("Phản hồi Mapbox không hợp lệ. Chưa có kết quả tuyến đường.") from None

    def suggest(self, query: str, session_token: str) -> list[dict[str, str]]:
        session_token = _session_token(session_token)
        if (
            not isinstance(query, str) or len(query) > 250 or not 1 <= len(query.strip()) <= 250
            or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in query)
        ):
            raise MapboxError("Địa chỉ tìm kiếm phải có từ 1 đến 250 ký tự hợp lệ.")
        result = self._request(
            "/search/searchbox/v1/suggest",
            {
                "q": query.strip(), "session_token": session_token,
                "country": "US", "limit": 5, "types": "address,poi",
                "bbox": NYC_SEARCH_BBOX, "proximity": NYC_SEARCH_PROXIMITY,
            },
            timeout=SEARCH_TIMEOUT_SECONDS,
        )
        rows = result.get("suggestions")
        if not isinstance(rows, list) or len(rows) > 5:
            raise MapboxError("Danh sách gợi ý Mapbox không hợp lệ.")
        suggestions = []
        for row in rows:
            if not isinstance(row, dict):
                raise MapboxError("Danh sách gợi ý Mapbox không hợp lệ.")
            suggestions.append({"mapbox_id": _text(row.get("mapbox_id"), maximum=500), "label": _label(row)})
        return suggestions

    def retrieve(self, mapbox_id: str, session_token: str) -> dict[str, Any]:
        session_token = _session_token(session_token)
        mapbox_id = _text(mapbox_id, maximum=500)
        result = self._request(
            "/search/searchbox/v1/retrieve/" + quote(mapbox_id, safe=""),
            {"session_token": session_token}, timeout=SEARCH_TIMEOUT_SECONDS,
        )
        features = result.get("features")
        if not isinstance(features, list) or len(features) != 1 or not isinstance(features[0], dict):
            raise MapboxError("Mapbox chưa trả về một địa điểm xác định. Hãy chọn lại địa chỉ.")
        feature = features[0]
        geometry, properties = feature.get("geometry"), feature.get("properties")
        if not isinstance(geometry, dict) or geometry.get("type") != "Point" or not isinstance(properties, dict):
            raise MapboxError("Tọa độ địa điểm từ Mapbox không hợp lệ.")
        coordinates = geometry.get("coordinates")
        if not isinstance(coordinates, list) or len(coordinates) != 2:
            raise MapboxError("Tọa độ địa điểm từ Mapbox không hợp lệ.")
        longitude, latitude = validate_coordinates(*coordinates)
        return {"label": _label(properties), "longitude": longitude, "latitude": latitude}

    def directions(self, pickup: tuple[float, float], dropoff: tuple[float, float]) -> dict[str, Any]:
        if any(not isinstance(point, (list, tuple)) or len(point) != 2 for point in (pickup, dropoff)):
            raise MapboxError("Tuyến đường cần đúng hai tọa độ đón và đến.")
        pickup = validate_coordinates(*pickup)
        dropoff = validate_coordinates(*dropoff)
        coordinates = ";".join(f"{longitude},{latitude}" for longitude, latitude in (pickup, dropoff))
        result = self._request(
            "/directions/v5/mapbox/driving/" + coordinates,
            {"alternatives": "false", "geometries": "geojson", "overview": "full", "steps": "false"},
            timeout=ROUTING_TIMEOUT_SECONDS,
        )
        rows = result.get("routes")
        if result.get("code") != "Ok" or not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            raise MapboxError("Mapbox không tìm được tuyến ô tô được hỗ trợ giữa hai điểm.")
        route = rows[0]
        distance = route.get("distance")
        try:
            valid_distance = type(distance) in {int, float} and math.isfinite(distance) and distance >= 0
        except OverflowError:
            valid_distance = False
        if not valid_distance:
            raise MapboxError("Quãng đường do Mapbox trả về không hợp lệ.")
        selected: dict[str, Any] = {"distance_meters": float(distance)}
        geometry = route.get("geometry")
        if geometry is not None:
            if not isinstance(geometry, dict) or geometry.get("type") != "LineString":
                raise MapboxError("Hình dạng tuyến đường Mapbox không hợp lệ.")
            points = geometry.get("coordinates")
            if not isinstance(points, list) or not 2 <= len(points) <= 20_000:
                raise MapboxError("Hình dạng tuyến đường Mapbox không hợp lệ.")
            normalized = []
            for point in points:
                if not isinstance(point, list) or len(point) != 2:
                    raise MapboxError("Hình dạng tuyến đường Mapbox không hợp lệ.")
                normalized.append(list(validate_coordinates(*point)))
            selected["geometry"] = {"type": "LineString", "coordinates": normalized}
        return selected
