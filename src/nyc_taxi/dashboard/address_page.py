"""Opt-in address flow. Offline transport/geometry/coordinator can be injected."""

from __future__ import annotations

from datetime import date, datetime, time, timezone
import os
from uuid import uuid4

import streamlit as st

from nyc_taxi.dashboard.address_component import (
    AddressSelectionError, address_picker, new_address_state, process_address_event,
    wait_for_address_request,
)
from nyc_taxi.dashboard.mapbox_client import MapboxClient, MapboxError
from nyc_taxi.dashboard.geometry_assets import load_geometry_bundle
from nyc_taxi.dashboard.prediction import InferenceClient, DEFAULT_INFERENCE_URL, PredictionError, selected_model_version
from nyc_taxi.dashboard.route_fare import (
    RouteFareCoordinator, PretripRouteFareCoordinator, GoldV3RouteFareCoordinator,
    RouteFareError, execute_submission, validate_address_support, validate_standard_route,
)
from nyc_taxi.dashboard.route_map import route_map
from nyc_taxi.dashboard.taxi_zones import GeometryError


DIRTY_INPUTS_MESSAGE = "Thông tin đã thay đổi. Bấm Ước tính để cập nhật."
_GENERAL_NOTICES = {
    "Quãng đường là tuyến ô tô dự kiến; có thể khác quãng đường đồng hồ taxi thực tế.",
    "Đây là demo lịch sử Jan–Jun/2025. Cước cơ bản USD chưa gồm tip, toll và phụ phí.",
}


def inputs_changed(stored: dict, draft: dict) -> bool:
    """Compare draft inputs with the immutable submitted result, not map state."""
    def address_identity(value):
        if not isinstance(value, dict):
            return None
        return tuple(value.get(key) for key in ("label", "longitude", "latitude"))

    return (
        any(address_identity(draft.get(slot)) != address_identity(stored.get(slot))
            for slot in ("pickup", "dropoff"))
        or (draft.get("time_selection") != stored.get("time_selection")
            if stored.get("model_version") in ("v2.2", "gold-v3")
            else draft.get("pickup_datetime") != stored.get("pickup_datetime"))
        or draft.get("weather_override") != stored.get("request", {}).get("weather_override")
        or draft.get("standard_rate") is not True
    )


def address_flow_enabled() -> bool:
    return os.environ.get("FARE_ADDRESS_ENABLED", "0") == "1"


def make_search_client():
    # 2A cannot issue accidental API calls, even with the UI feature enabled.
    # Offline tests inject this factory. 2B must explicitly enable live transport.
    if not address_flow_enabled() or os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") != "1":
        raise AddressSelectionError("Luồng địa chỉ đang ở chế độ offline. API thật chưa được bật.")
    return MapboxClient(os.environ.get("MAPBOX_ACCESS_TOKEN", ""))


class _DeferredSearchClient:
    """Construct transport only after version/session guards inside the bridge."""

    def suggest(self, query, session_token):
        return make_search_client().suggest(query, session_token)

    def retrieve(self, mapbox_id, session_token):
        return make_search_client().retrieve(mapbox_id, session_token)


@st.cache_resource(max_entries=2, show_spinner=False)
def load_geometry(path: str, sha256: str, manifest_path: str = ""):
    # Only public, pinned geometry is cached process-wide; never user addresses.
    return load_geometry_bundle(path or None, manifest_path or None, sha256 or None)


def get_geometry():
    """Local, checksum-pinned support check; never constructs an HTTP client."""
    return load_geometry(os.environ.get("FARE_ADDRESS_GEOMETRY_PATH", ""),
                         os.environ.get("FARE_ADDRESS_GEOMETRY_SHA256", ""),
                         os.environ.get("FARE_ADDRESS_GEOMETRY_MANIFEST", ""))


def make_coordinator():
    version = selected_model_version()
    if version == "v2.2":
        from nyc_taxi.dashboard.pretrip_prediction import PretripInferenceClient
        return PretripRouteFareCoordinator(
            router=make_search_client(), geometry=get_geometry(), predictor=PretripInferenceClient(),
            metadata=get_pretrip_metadata(os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL)),
        )
    if version == "gold-v3":
        from nyc_taxi.dashboard.pretrip_prediction import GoldV3InferenceClient
        return GoldV3RouteFareCoordinator(
            router=make_search_client(), geometry=get_geometry(), predictor=GoldV3InferenceClient(),
            metadata=get_gold_v3_metadata(os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL)),
        )
    return RouteFareCoordinator(router=make_search_client(), geometry=get_geometry(), predictor=InferenceClient())


@st.cache_data(ttl=60, max_entries=4, show_spinner=False)
def load_pretrip_metadata(base_url: str) -> dict:
    """Bounded public metadata only; never cache addresses or predictions."""
    from nyc_taxi.dashboard.pretrip_prediction import PretripInferenceClient
    client = PretripInferenceClient(base_url)
    ready, zones = client.ready(), client.zones()
    from nyc_taxi.ml.pretrip_serving_contract import identity
    if any(ready[key] != zones[key] for key in identity()):
        raise PredictionError("Danh sách vùng không cùng candidate V2.2 đã khóa.")
    return zones


@st.cache_data(ttl=60, max_entries=4, show_spinner=False)
def load_gold_v3_metadata(base_url: str) -> dict:
    """Bounded public metadata for Gold V3; never cache addresses or predictions."""
    from nyc_taxi.dashboard.pretrip_prediction import GoldV3InferenceClient, GOLD_V3_IDENTITY
    client = GoldV3InferenceClient(base_url)
    ready, zones = client.ready(), client.zones()
    if ready.get("model_version") != "gold-v3" or zones.get("contract_version") != GOLD_V3_IDENTITY["contract_version"]:
        raise PredictionError("Danh sách vùng không khớp hợp đồng Gold V3 đã khóa.")
    return zones


def get_pretrip_metadata(base_url: str) -> dict:
    """Freeze verified public metadata per session, URL and full candidate identity."""
    from nyc_taxi.ml.pretrip_serving_contract import identity
    expected = identity()
    key = (base_url, tuple(sorted(expected.items())))
    state_key = "_pretrip_verified_public_metadata"
    cached = st.session_state.get(state_key)
    if cached is not None and cached["key"] == key:
        return cached["metadata"]
    st.session_state.pop(state_key, None)
    loaded = load_pretrip_metadata(base_url)
    if not isinstance(loaded, dict) or any(loaded.get(name) != value for name, value in expected.items()):
        raise PredictionError("Metadata không khớp đầy đủ candidate V2.2 đang chọn.")
    st.session_state[state_key] = {"key": key, "metadata": loaded}
    return loaded


def get_gold_v3_metadata(base_url: str) -> dict:
    """Freeze verified Gold V3 public metadata per session and URL."""
    from nyc_taxi.dashboard.pretrip_prediction import GOLD_V3_IDENTITY
    key = (base_url, tuple(sorted(GOLD_V3_IDENTITY.items())))
    state_key = "_gold_v3_verified_public_metadata"
    cached = st.session_state.get(state_key)
    if cached is not None and cached["key"] == key:
        return cached["metadata"]
    st.session_state.pop(state_key, None)
    loaded = load_gold_v3_metadata(base_url)
    if not isinstance(loaded, dict) or loaded.get("model_version") != "gold-v3":
        raise PredictionError("Metadata không khớp mô hình Gold V3 đang chọn.")
    st.session_state[state_key] = {"key": key, "metadata": loaded}
    return loaded


def submit_clock() -> datetime:
    """A single injectable UTC clock read made only by the submit branch."""
    return datetime.now(timezone.utc)


def build_pretrip_submission(*, pickup_id: int, dropoff_id: int, time_selection: dict,
                             submission_id: str) -> dict:
    from nyc_taxi.ml.pretrip_features import create_submission
    return create_submission(pickup_location_id=pickup_id, dropoff_location_id=dropoff_id,
                             mode=time_selection["mode"], scheduled_local=time_selection.get("scheduled_local"),
                             fold=time_selection.get("fold"), now=submit_clock(), submission_id=submission_id)


def format_pretrip_time_error(error: ValueError, time_selection: dict | None = None,
                              *, now: datetime | None = None) -> str:
    """Return a specific Vietnamese validation message for NYC pickup time errors."""
    text = str(error)
    if "does not exist" in text:
        return ("Thời gian không hợp lệ: giờ đón rơi vào khoảng nhảy giờ DST mùa xuân "
                "(giờ không tồn tại tại New York); vui lòng chọn giờ khác.")
    if "Ambiguous" in text or "fold" in text:
        return ("Thời gian không hợp lệ: giờ DST bị lặp cần chọn Lần thứ nhất/Lần thứ hai "
                "(hoặc lựa chọn lần lặp không hợp lệ cho giờ thường).")
    if "outside pinned calendar" in text:
        return "Thời gian không hợp lệ: ngày đón nằm ngoài phạm vi calendar 2025–2027 đã pin."
    if "submit tolerance and seven-day horizon" in text and isinstance(time_selection, dict):
        scheduled_raw = time_selection.get("scheduled_local")
        if isinstance(scheduled_raw, str):
            try:
                from zoneinfo import ZoneInfo
                reference = now if now is not None else datetime.now(timezone.utc)
                scheduled_dt = datetime.fromisoformat(scheduled_raw).replace(tzinfo=ZoneInfo("America/New_York"))
                if (scheduled_dt.astimezone(timezone.utc) - reference).total_seconds() < 0:
                    return ("Thời gian không hợp lệ: giờ đón đã qua so với hiện tại New York "
                            "(chỉ cho phép trễ tối đa 60 giây). Hãy chọn giờ trong tương lai hoặc chọn 'Đi ngay'.")
                return "Thời gian không hợp lệ: chỉ hỗ trợ hẹn trước tối đa 7 ngày tính từ hiện tại New York."
            except ValueError:
                pass
    return ("Thời gian không hợp lệ: hẹn trong 7 ngày và calendar đã pin; "
            "giờ DST bị lặp cần chọn lần, giờ không tồn tại bị từ chối.")


def selection_support(state: dict) -> dict | None:
    """Keep at most one local support result per field in this user's session."""
    selected = state.get("selected")
    if selected is None:
        state.pop("_zone_support", None)
        return None
    identity = (
        tuple(selected.get(key) for key in ("label", "longitude", "latitude"))
        if isinstance(selected, dict) else None,
        *(os.environ.get(key, "") for key in (
            "FARE_ADDRESS_GEOMETRY_PATH", "FARE_ADDRESS_GEOMETRY_SHA256",
            "FARE_ADDRESS_GEOMETRY_MANIFEST",
        )),
    )
    cached = state.get("_zone_support")
    if cached is not None and cached["identity"] == identity:
        return cached
    try:
        result = {"identity": identity, "zone": validate_address_support(selected, get_geometry()),
                  "error": None, "code": None}
    except RouteFareError as error:
        result = {"identity": identity, "zone": None, "error": str(error), "code": error.code}
    except (GeometryError, OSError, ValueError):
        return {"zone": None, "code": "geometry_unavailable",
                "error": "Chưa kiểm tra được vùng hỗ trợ. Cần kiểm tra cấu hình geometry; "
                         "đây chưa phải kết luận địa chỉ nằm ngoài vùng."}
    if result["code"] != "invalid_geometry":
        state["_zone_support"] = result
    return result


def render_page() -> None:
    try:
        version = selected_model_version()
    except PredictionError as error:
        st.error(str(error))
        return
    is_v22 = version == "v2.2"
    is_gold_v3 = version == "gold-v3"
    pretrip = is_v22 or is_gold_v3

    previous = st.session_state.get("address_fare_result")
    if previous and previous.get("model_version", "v1") != version:
        # Never display a price from another serving mode after a switch.
        st.session_state.pop("address_fare_result", None)

    st.title("Ước tính cước từ địa chỉ")
    if is_gold_v3:
        st.markdown(
            "**Cước cơ bản tiêu chuẩn · USD · chưa gồm tiền tip, phí cầu đường (tolls) và phụ phí "
            "(riêng tuyến Sân bay JFK ↔ Manhattan tự động áp dụng mức cước cố định 70.00 USD).**"
        )
        st.write(
            "Nhập địa chỉ đón và điểm đến tại New York để tự động nhận diện khu vực (263 NYC Taxi Zones), "
            "hiển thị lộ trình trên bản đồ và dự báo giá cước bằng mô hình học máy **Gold V3 (Spark GBTRegressor)**."
        )
        st.caption(
            "Mô hình được huấn luyện từ 23,88 triệu chuyến xe tầng Medallion Gold 2025 "
            "(kết hợp dữ liệu Taxi + Thời tiết ERA5 + Lịch nghỉ lễ + Bản đồ vùng), đạt sai số trung bình "
            "MAE = 2.92 USD (R² = 86.9%) trên tập kiểm thử 2025 và MAE = 3.08 USD (R² = 83.0%) trên dữ liệu thực tế năm 2026."
        )
    elif is_v22:
        st.markdown("**Cước đồng hồ tiêu chuẩn · USD · chưa gồm tip, tolls và phụ phí.**")
        st.write("Ước tính trước chuyến từ vùng đón/đến và thời gian New York. Khoảng cách tuyến trên bản đồ chỉ tham khảo, không phải đầu vào mô hình.")
        st.caption("Mô hình học từ dữ liệu 2026; kết quả kiểm định lịch sử không bảo đảm sai số cho chuyến hiện tại.")
    else:
        st.markdown("**Cước đồng hồ tiêu chuẩn · USD · chưa gồm tip, tolls và phụ phí.**")
        st.write("Bản hiện tại dùng mô hình lịch sử tháng 1–6/2025, chưa phục vụ ngày hiện tại.")

    if not address_flow_enabled() or os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") != "1":
        st.info("Chế độ offline: tìm địa chỉ và bản đồ trực tuyến chưa được bật.")

    st.subheader("1. Chọn điểm đón và điểm đến")
    st.write("Gõ địa chỉ hoặc tên địa điểm tại New York, sau đó bấm chọn một gợi ý trong danh sách.")
    support_by_slot = {}
    pending_events = []
    pretrip_metadata = None
    if is_v22:
        try:
            pretrip_metadata = get_pretrip_metadata(os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL))
        except PredictionError as error:
            st.error(str(error))
        except Exception:
            st.error("Chưa kiểm tra được candidate V2.2. Không có giá hoặc mô hình thay thế.")
    elif is_gold_v3:
        try:
            pretrip_metadata = get_gold_v3_metadata(os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL))
        except PredictionError as error:
            st.error(str(error))
        except Exception:
            st.error("Chưa kiểm tra được dịch vụ suy luận Gold V3. Không có giá thay thế.")

    picker_slots = {slot: st.container(key="address_" + slot + "_slot")
                    for slot in ("pickup", "dropoff")}
    for slot, label in (("pickup", "Địa chỉ đón"), ("dropoff", "Địa chỉ đến")):
        with picker_slots[slot]:
            key = "address_" + slot
            if key not in st.session_state:
                st.session_state[key] = new_address_state()
            state = st.session_state[key]
            wait_for_address_request(state)
            event = address_picker(
                state, label=label, key=key + "_picker",
                online=address_flow_enabled() and os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") == "1",
            )
            if isinstance(event, dict) and event.get("event_id") not in state["events"]:
                pending_events.append((state, event))
            if state["error"]:
                st.error(state["error"])

            support = selection_support(state)
            if is_v22 and support is not None and support["zone"] is not None:
                if pretrip_metadata is None:
                    support = {"zone": None, "error": "Chưa xác minh được mô hình V2.2.", "code": "model_unavailable"}
                elif support["zone"]["location_id"] not in pretrip_metadata[slot + "_location_ids"]:
                    support = {"zone": None, "error": "Chưa hỗ trợ ước tính: vùng này chưa có trong dictionary huấn luyện cho vai trò "
                               + ("đón." if slot == "pickup" else "đến."), "code": "unsupported_categories"}
            elif is_gold_v3 and support is not None and support["zone"] is not None:
                if pretrip_metadata is None:
                    support = {"zone": None, "error": "Chưa xác minh được mô hình Gold V3.", "code": "model_unavailable"}
            support_by_slot[slot] = support
            if support is not None:
                if support["zone"] is not None:
                    zone = support["zone"]
                    st.success(f"{label}: hợp lệ trong Taxi Zones · {zone['borough']} · {zone['zone']} (Zone {zone['location_id']})"
                               if is_gold_v3 else f"{label}: hợp lệ trong Taxi Zones · {zone['borough']} · {zone['zone']}")
                else:
                    st.warning(f"{label}: {support['error']}")

    route_error = None
    can_submit = not pending_events and all(result is not None and result["zone"] is not None
                                            for result in support_by_slot.values())
    if can_submit:
        pu_z = support_by_slot["pickup"]["zone"]
        do_z = support_by_slot["dropoff"]["zone"]
        if is_gold_v3:
            if (pu_z["location_id"] == 132 and do_z["borough"] == "Manhattan") or (do_z["location_id"] == 132 and pu_z["borough"] == "Manhattan"):
                st.info("✈️ Tuyến Sân bay JFK (Zone 132) ↔ Manhattan: Hệ thống tự động áp dụng quy tắc cước cố định 70.00 USD (Ratecode 2) theo quy định NYC TLC.")
        else:
            try:
                validate_standard_route(pu_z, do_z)
            except RouteFareError as error:
                can_submit = False
                route_error = str(error)
                st.warning(route_error)
    if not can_submit and route_error is None:
        st.info("Chọn đủ hai địa chỉ hợp lệ trước khi tính cước. "
                "Nếu một điểm chưa được hỗ trợ, chỉ cần sửa ô đó; điểm còn lại được giữ nguyên.")

    st.subheader("2. Chọn thời gian đón")
    st.write("Giờ địa phương America/New_York · hỗ trợ đi ngay hoặc hẹn giờ trước tối đa 7 ngày."
             if pretrip else "Giờ địa phương New York · định dạng 24 giờ · chỉ tháng 1–6/2025.")
    weather_override = None
    with st.container(border=True):
        mode = "now" if pretrip else "historical"
        fold = None
        if pretrip:
            mode_label = st.radio("Thời điểm đón", ["Đi ngay", "Hẹn giờ"], horizontal=True, key="address_pretrip_mode")
            mode = "now" if mode_label == "Đi ngay" else "scheduled"
        if mode != "now":
            day_col, hour_col, minute_col = st.columns([2, 1, 1])
            if pretrip:
                from datetime import timedelta
                from zoneinfo import ZoneInfo
                from nyc_taxi.ml.pretrip_features import load_calendar
                calendar = load_calendar()
                default_schedule = st.session_state.get("_pretrip_default_schedule")
                if default_schedule is None:
                    target_nyc = datetime.now(ZoneInfo("America/New_York")) + timedelta(minutes=30)
                    default_day = min(max(target_nyc.date(), calendar.coverage_start), calendar.coverage_end)
                    default_schedule = (default_day, target_nyc.hour, target_nyc.minute)
                    st.session_state["_pretrip_default_schedule"] = default_schedule
                default_day, default_hour, default_minute = default_schedule
                minimum, maximum = calendar.coverage_start, calendar.coverage_end
            else:
                default_day, minimum, maximum = date(2025, 6, 15), date(2025, 1, 1), date(2025, 6, 30)
                default_hour, default_minute = 12, 0
            pickup_date = day_col.date_input("Ngày đón", value=default_day, min_value=minimum,
                                             max_value=maximum, format="DD/MM/YYYY", key="address_pickup_date")
            hour = hour_col.selectbox("Giờ (00–23)", range(24), index=default_hour,
                                      format_func=lambda value: f"{value:02d}", key="address_pickup_hour")
            minute = minute_col.selectbox("Phút (00–59)", range(60), index=default_minute,
                                          format_func=lambda value: f"{value:02d}", key="address_pickup_minute")
            if pretrip:
                fold_label = st.selectbox("Khi giờ New York bị lặp do DST", ["Tự động nếu không bị lặp", "Lần thứ nhất", "Lần thứ hai"], key="address_pretrip_fold")
                fold = {"Tự động nếu không bị lặp": None, "Lần thứ nhất": 0, "Lần thứ hai": 1}[fold_label]

        if is_gold_v3:
            use_weather = st.checkbox(
                "🌦️ Tùy chọn thử nghiệm: Nhập thêm điều kiện thời tiết (nhiệt độ, mưa, tuyết, gió) để chạy biến thể mô hình có thời tiết (M3-w)",
                value=False,
                key="address_gold_v3_use_weather",
            )
            if use_weather:
                wc1, wc2, wc3, wc4 = st.columns(4)
                temp_c = wc1.number_input("Nhiệt độ (°C)", min_value=-30.0, max_value=45.0, value=20.0, step=1.0, key="addr_w_temp")
                precip_mm = wc2.number_input("Lượng mưa (mm/h)", min_value=0.0, max_value=100.0, value=0.0, step=0.5, key="addr_w_rain")
                snow_cm = wc3.number_input("Tuyết rơi (cm/h)", min_value=0.0, max_value=50.0, value=0.0, step=0.5, key="addr_w_snow")
                wind_kmh = wc4.number_input("Tốc độ gió (km/h)", min_value=0.0, max_value=120.0, value=12.0, step=1.0, key="addr_w_wind")
                weather_override = {
                    "temperature_2m": float(temp_c),
                    "precipitation": float(precip_mm),
                    "snowfall": float(snow_cm),
                    "wind_speed_10m": float(wind_kmh),
                }

        checkbox_text = (
            "Tôi hiểu đây là ước tính cước cơ bản (USD) chưa gồm tip, cầu đường và phụ phí; tuyến JFK ↔ Manhattan áp dụng cước cố định 70.00 USD."
            if is_gold_v3
            else "Tôi xác nhận đây là taxi vàng tính cước đồng hồ tiêu chuẩn, không phải cước cố định, thỏa thuận hoặc đi chung."
        )
        standard = st.checkbox(checkbox_text)
        submitted = st.button("Ước tính", type="primary", disabled=not can_submit, key="address_estimate_submit")
    payload = {"pickup": st.session_state.address_pickup["selected"],
               "dropoff": st.session_state.address_dropoff["selected"],
               "standard_rate": standard}
    if weather_override is not None:
        payload["weather_override"] = weather_override
    if pretrip:
        payload["time_selection"] = {"mode": mode, "scheduled_local":
                                     datetime.combine(pickup_date, time(hour, minute)).isoformat() if mode == "scheduled" else None,
                                     "fold": fold}
    else:
        payload["pickup_datetime"] = datetime.combine(pickup_date, time(hour, minute)).isoformat()
    if submitted and not pending_events:
        st.session_state.pop("address_fare_result", None)
        st.session_state.pop("address_fare_error", None)
        if payload["pickup"] is None or payload["dropoff"] is None:
            st.session_state.address_fare_error = "Hãy chọn và xác nhận đủ hai địa chỉ trước khi gửi."
        else:
            try:
                submission_id = str(uuid4())
                if pretrip:
                    req_obj = build_pretrip_submission(
                        pickup_id=support_by_slot["pickup"]["zone"]["location_id"],
                        dropoff_id=support_by_slot["dropoff"]["zone"]["location_id"],
                        time_selection=payload["time_selection"], submission_id=submission_id,
                    )
                    if weather_override is not None:
                        req_obj.update(weather_override)
                        req_obj["weather_override"] = weather_override
                    payload["pretrip_request"] = req_obj
                with st.spinner("Đang tính tuyến và ước tính cước…"):
                    outcome = execute_submission(st.session_state, submission_id, payload, make_coordinator())
                if outcome.success:
                    st.session_state.address_fare_result = outcome.estimate.to_dict()
                else:
                    st.session_state.address_fare_error = outcome.error
            except (AddressSelectionError, MapboxError, GeometryError):
                st.session_state.address_fare_error = "Chưa đủ cấu hình API/geometry đã pin. Chuẩn bị Chặng 2B trước khi dùng live."
            except ValueError as error:
                st.session_state.address_fare_error = format_pretrip_time_error(error, payload.get("time_selection"))
            except Exception:
                st.session_state.address_fare_error = "Không thể ước tính cước. Không có giá thay thế."

    if st.session_state.get("address_fare_error"):
        st.error(st.session_state.address_fare_error)
    stored = st.session_state.get("address_fare_result")
    if stored:
        if inputs_changed(stored, payload):
            st.info(DIRTY_INPUTS_MESSAGE)
        st.subheader("Kết quả ước tính")
        prediction = stored["prediction"]
        submitted_at = datetime.fromisoformat(stored["pickup_datetime"])
        if stored.get("model_version") == "gold-v3":
            m1, m2, m3 = st.columns(3)
            m1.metric(
                "Giá cước dự kiến (USD)",
                f"${prediction['fare_amount_usd']:,.2f}",
                delta="Cước cố định JFK ↔ Manhattan (Ratecode 2)" if prediction.get("flat_fare_applied") else f"Mức trung vị lịch sử: ${prediction.get('baseline_fare_usd', 0.0):,.2f}",
                delta_color="off",
            )
            m2.metric(
                "Quãng đường lộ trình (Mapbox)",
                f"{stored['distance_km']:.2f} km",
                delta=f"{stored['distance_km'] * 0.621371:.2f} dặm (trên bản đồ)",
                delta_color="off",
            )
            m3.metric(
                "Khoảng cách tâm vùng (Haversine)",
                f"{prediction.get('zone_centroid_distance_km', 0.0):.2f} km",
                delta="Đầu vào mô hình học máy",
                delta_color="off",
            )
            st.write(
                f"**Hành trình:** Zone {stored['pickup']['location_id']} ({stored['pickup']['borough']} — {stored['pickup']['zone']}) "
                f"→ Zone {stored['dropoff']['location_id']} ({stored['dropoff']['borough']} — {stored['dropoff']['zone']}) "
                f"· **Giờ đón:** {submitted_at:%d/%m/%Y %H:%M} (Giờ New York)"
            )
        else:
            st.metric("Ước tính cước cơ bản (USD)", f"${prediction['fare_amount_usd']:,.2f}")
            st.write(f"**{stored['distance_km']:.3f} km · {submitted_at:%d/%m/%Y %H:%M} — giờ New York**")
        online = address_flow_enabled() and os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") == "1"
        route_map(
            stored.get("route_geometry"), submission_id=stored.get("submission_id"),
            pickup=[stored["pickup"]["longitude"], stored["pickup"]["latitude"]],
            dropoff=[stored["dropoff"]["longitude"], stored["dropoff"]["latitude"]],
            access_token=os.environ.get("MAPBOX_ACCESS_TOKEN", "") if online else "",
            online=online, key="address_submitted_route_map",
        )
        case_warnings = [warning for warning in stored["warnings"] if warning not in _GENERAL_NOTICES]
        for warning in dict.fromkeys(case_warnings):
            st.warning(warning)
        unseen = any(prediction.get("unseen_categories", {}).values())
        if unseen and stored.get("model_version") != "gold-v3":
            st.warning("Vùng này chưa có trong dữ liệu huấn luyện; kết quả có thể kém chính xác hơn.")
        if stored.get("model_version") not in ("v2.2", "gold-v3") and stored["request"]["distance_miles"] == 0:
            st.warning("Quãng đường bằng 0: kết quả có độ bất định lớn hơn, không phải báo giá bảo đảm.")
        elif stored.get("model_version") not in ("v2.2", "gold-v3") and stored["request"]["distance_miles"] > 50:
            st.warning("Chuyến xa trên 50 mile có ít bằng chứng đánh giá; không dùng như báo giá bảo đảm.")
        if prediction["fare_amount_usd"] <= 0:
            st.warning("Mô hình trả cước không dương; không tự cắt hoặc thay giá trị dự đoán.")
        elif prediction["fare_amount_usd"] > 1000:
            st.warning("Cước cao trên 1.000 USD thuộc nhóm hiếm; độ chính xác chưa được bảo đảm.")
        with st.expander("Chi tiết mô hình và yêu cầu đã gửi"):
            st.text(f"{stored['pickup']['label']} → {stored['dropoff']['label']}")
            st.caption(f"{stored['pickup_datetime']} NYC")
            if stored.get("model_version") == "gold-v3":
                st.write(
                    "Mô hình Gold V3 (Spark ML GBTRegressor) huấn luyện trên tầng Medallion Gold 2025 (23,88 triệu chuyến). "
                    "Đầu vào gồm vùng đón/trả (263 Taxi Zones), khoảng cách tâm vùng Haversine Centroid, chu kỳ giờ/thứ và lịch nghỉ lễ OPM. "
                    "Tuyến JFK (Zone 132) ↔ Manhattan áp dụng quy tắc cước cố định 70.00 USD (Ratecode 2)."
                )
                st.write({
                    "pickup_zone": stored["pickup"]["location_id"],
                    "dropoff_zone": stored["dropoff"]["location_id"],
                    "model_version": "gold-v3",
                    "model_variant": prediction.get("model_variant"),
                    "ratecode_id": prediction.get("ratecode_id"),
                    "flat_fare_applied": prediction.get("flat_fare_applied"),
                    "zone_centroid_distance_km": prediction.get("zone_centroid_distance_km"),
                    "mapbox_driving_distance_km": round(stored["distance_km"], 3),
                    "baseline_fare_usd": prediction.get("baseline_fare_usd"),
                    "contract_version": prediction.get("contract_version"),
                    "geometry_sha256": prediction.get("geometry_sha256"),
                })
            elif stored.get("model_version") == "v2.2":
                pretrip_policy_note = (
                    "Chính sách phục vụ dùng unknown bucket (__unknown) kèm cảnh báo cho vùng chưa thấy trong tập train. "
                    if os.environ.get("PRETRIP_ALLOW_UNSEEN_ZONES", "0") == "1"
                    else "Chính sách phục vụ từ chối category chưa thấy theo từng vai trò. "
                )
                st.write("V2.2 dùng vùng/thời gian; khoảng cách tuyến chỉ tham khảo, không được đưa vào model. "
                         + pretrip_policy_note
                         + "MAE August giữ nguyên đủ rows, không phải bảo đảm sai số hiện tại hoặc từng chuyến.")
                st.write({"pickup_zone": stored["pickup"]["location_id"],
                          "dropoff_zone": stored["dropoff"]["location_id"],
                          "model_version": stored.get("model_version", "v1"),
                          "model_sha256": stored["prediction"]["model_sha256"]})
            else:
                st.write("Mô hình lịch sử 2025; quãng đường ô tô dự kiến có thể khác quãng đường đồng hồ taxi thực tế. "
                         "MAE lịch sử không bảo đảm sai số hiện tại hoặc sai số của từng chuyến.")
                st.write({"pickup_zone": stored["pickup"]["location_id"],
                          "dropoff_zone": stored["dropoff"]["location_id"],
                          "model_version": stored.get("model_version", "v1"),
                          **({"distance_miles": stored["request"]["distance_miles"]} if "distance_miles" in stored["request"] else {}),
                          "model_sha256": stored["prediction"]["model_sha256"]})

    if pending_events:
        st.session_state.pop("address_fare_error", None)
        for state, event in pending_events:
            try:
                client = _DeferredSearchClient() if event.get("kind") in {"suggest", "retrieve"} else None
                process_address_event(state, event, client)
            except (AddressSelectionError, MapboxError):
                if event.get("event_id") not in state["events"]:
                    state["events"] = (state["events"] + [event.get("event_id")])[-128:]
                state.update(selected=None, suggestions=[], status="error",
                             error="Chưa thể tìm địa chỉ. Kiểm tra kết nối/cấu hình; không tự gọi lại.")
        st.rerun()
