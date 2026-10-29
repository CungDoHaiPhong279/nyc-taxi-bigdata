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
    RouteFareCoordinator, PretripRouteFareCoordinator, RouteFareError, execute_submission,
    validate_address_support, validate_standard_route,
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
            if stored.get("model_version") == "v2.2"
            else draft.get("pickup_datetime") != stored.get("pickup_datetime"))
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
    if selected_model_version() == "v2.2":
        from nyc_taxi.dashboard.pretrip_prediction import PretripInferenceClient
        return PretripRouteFareCoordinator(
            router=make_search_client(), geometry=get_geometry(), predictor=PretripInferenceClient(),
            metadata=get_pretrip_metadata(os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL)),
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


def get_pretrip_metadata(base_url: str) -> dict:
    """Freeze verified public metadata per session, URL and full candidate identity.

    The initial loader retains its bounded process cache/TTL. Once verified for
    this session, ordinary reruns and edited drafts do not call even readiness
    or zones again. This is not a health assertion: each explicit prediction
    still validates the actual response identity. Errors are never cached.
    """
    from nyc_taxi.ml.pretrip_serving_contract import identity
    expected = identity()
    key = (base_url, tuple(sorted(expected.items())))
    state_key = "_pretrip_verified_public_metadata"
    cached = st.session_state.get(state_key)
    if cached is not None and cached["key"] == key:
        return cached["metadata"]
    # A changed URL/candidate invalidates the previous entry before loading,
    # so a failed replacement cannot silently reuse old model metadata.
    st.session_state.pop(state_key, None)
    loaded = load_pretrip_metadata(base_url)
    if not isinstance(loaded, dict) or any(loaded.get(name) != value for name, value in expected.items()):
        raise PredictionError("Metadata không khớp đầy đủ candidate V2.2 đang chọn.")
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
    """Keep at most one local support result per field in this user's session.

    Editing/clearing a choice invalidates it. No addresses or coordinates enter
    the process-wide geometry cache, logs or external services during this check.
    Configuration changes also invalidate the result; geometry failures are not
    cached as if they were evidence of an unsupported address.
    """
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
        pretrip = selected_model_version() == "v2.2"
    except PredictionError as error:
        st.error(str(error))
        return
    previous = st.session_state.get("address_fare_result")
    if previous and previous.get("model_version", "v1") != ("v2.2" if pretrip else "v1"):
        # Never display a price from the other serving mode after a switch.
        st.session_state.pop("address_fare_result", None)
    st.title("Ước tính cước từ địa chỉ")
    st.markdown("**Cước đồng hồ tiêu chuẩn · USD · chưa gồm tip, tolls và phụ phí.**")
    if pretrip:
        st.write("Ước tính trước chuyến từ vùng đón/đến và thời gian New York. Khoảng cách tuyến trên bản đồ chỉ tham khảo, không phải đầu vào mô hình.")
        st.caption("Mô hình học từ dữ liệu 2026; kết quả kiểm định lịch sử không bảo đảm sai số cho chuyến hiện tại.")
    else:
        st.write("Bản hiện tại dùng mô hình lịch sử tháng 1–6/2025, chưa phục vụ ngày hiện tại.")
    if not address_flow_enabled() or os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") != "1":
        st.info("Chế độ offline: tìm địa chỉ và bản đồ trực tuyến chưa được bật.")

    st.subheader("1. Chọn điểm đón và điểm đến")
    st.write("Gõ địa chỉ hoặc tên địa điểm, rồi chọn một gợi ý.")
    support_by_slot = {}
    pending_events = []
    pretrip_metadata = None
    if pretrip:
        try:
            pretrip_metadata = get_pretrip_metadata(os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL))
        except PredictionError as error:
            st.error(str(error))
        except Exception:
            st.error("Chưa kiểm tra được candidate V2.2. Không có giá hoặc mô hình thay thế.")
    # Reserve both stable parents before handling either role. Conditional
    # feedback must not move/remount the other field's iframe and debounce.
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
            if pretrip and support is not None and support["zone"] is not None:
                if pretrip_metadata is None:
                    support = {"zone": None, "error": "Chưa xác minh được mô hình V2.2.", "code": "model_unavailable"}
                elif support["zone"]["location_id"] not in pretrip_metadata[slot + "_location_ids"]:
                    support = {"zone": None, "error": "Chưa hỗ trợ ước tính: vùng này chưa có trong dictionary huấn luyện cho vai trò "
                               + ("đón." if slot == "pickup" else "đến."), "code": "unsupported_categories"}
            support_by_slot[slot] = support
            if support is not None:
                if support["zone"] is not None:
                    zone = support["zone"]
                    st.success(f"{label}: hợp lệ trong Taxi Zones · {zone['borough']} · {zone['zone']}")
                else:
                    st.warning(f"{label}: {support['error']}")

    route_error = None
    can_submit = not pending_events and all(result is not None and result["zone"] is not None
                                            for result in support_by_slot.values())
    if can_submit:
        try:
            validate_standard_route(support_by_slot["pickup"]["zone"], support_by_slot["dropoff"]["zone"])
        except RouteFareError as error:
            can_submit = False
            route_error = str(error)
            st.warning(route_error)
    if not can_submit and route_error is None:
        st.info("Chọn đủ hai địa chỉ hợp lệ trước khi tính cước. "
                "Nếu một điểm chưa được hỗ trợ, chỉ cần sửa ô đó; điểm còn lại được giữ nguyên.")

    st.subheader("2. Chọn thời gian đón")
    st.write("Giờ địa phương America/New_York · hẹn tối đa 7 ngày, trong phạm vi calendar đã pin."
             if pretrip else "Giờ địa phương New York · định dạng 24 giờ · chỉ tháng 1–6/2025.")
    # Native widgets outside a form commit draft edits on rerun, so the stale
    # notice is immediate. Only this explicit button can route or predict.
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
        standard = st.checkbox("Tôi xác nhận đây là taxi vàng tính cước đồng hồ tiêu chuẩn, "
                               "không phải cước cố định, thỏa thuận hoặc đi chung.")
        submitted = st.button("Ước tính", type="primary", disabled=not can_submit, key="address_estimate_submit")
    payload = {"pickup": st.session_state.address_pickup["selected"],
               "dropoff": st.session_state.address_dropoff["selected"],
               "standard_rate": standard}
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
                    payload["pretrip_request"] = build_pretrip_submission(
                        pickup_id=support_by_slot["pickup"]["zone"]["location_id"],
                        dropoff_id=support_by_slot["dropoff"]["zone"]["location_id"],
                        time_selection=payload["time_selection"], submission_id=submission_id,
                    )
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
            # Native Streamlit alert follows the theme's text/background colors;
            # no opacity or washed-out custom text is applied.
            st.info(DIRTY_INPUTS_MESSAGE)
        st.subheader("Kết quả ước tính")
        st.metric("Ước tính cước cơ bản (USD)", f"${stored['prediction']['fare_amount_usd']:,.2f}")
        submitted_at = datetime.fromisoformat(stored["pickup_datetime"])
        st.write(f"**{stored['distance_km']:.3f} km · {submitted_at:%d/%m/%Y %H:%M} — giờ New York**")
        online = address_flow_enabled() and os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") == "1"
        # Use only the immutable submitted route, never the current form draft.
        # A stable component key preserves the browser map across Streamlit reruns.
        route_map(
            stored.get("route_geometry"), submission_id=stored.get("submission_id"),
            pickup=[stored["pickup"]["longitude"], stored["pickup"]["latitude"]],
            dropoff=[stored["dropoff"]["longitude"], stored["dropoff"]["latitude"]],
            access_token=os.environ.get("MAPBOX_ACCESS_TOKEN", "") if online else "",
            online=online, key="address_submitted_route_map",
        )
        prediction = stored["prediction"]
        case_warnings = [warning for warning in stored["warnings"] if warning not in _GENERAL_NOTICES]
        for warning in dict.fromkeys(case_warnings):
            st.warning(warning)
        unseen = any(prediction.get("unseen_categories", {}).values())
        if unseen:
            st.warning("Vùng này chưa có trong dữ liệu huấn luyện; kết quả có thể kém chính xác hơn.")
        if stored.get("model_version") != "v2.2" and stored["request"]["distance_miles"] == 0:
            st.warning("Quãng đường bằng 0: kết quả có độ bất định lớn hơn, không phải báo giá bảo đảm.")
        elif stored.get("model_version") != "v2.2" and stored["request"]["distance_miles"] > 50:
            st.warning("Chuyến xa trên 50 mile có ít bằng chứng đánh giá; không dùng như báo giá bảo đảm.")
        if prediction["fare_amount_usd"] <= 0:
            st.warning("Mô hình trả cước không dương; không tự cắt hoặc thay giá trị dự đoán.")
        elif prediction["fare_amount_usd"] > 1000:
            st.warning("Cước cao trên 1.000 USD thuộc nhóm hiếm; độ chính xác chưa được bảo đảm.")
        with st.expander("Chi tiết mô hình và yêu cầu đã gửi"):
            st.text(f"{stored['pickup']['label']} → {stored['dropoff']['label']}")
            st.caption(f"{stored['pickup_datetime']} NYC")
            pretrip_policy_note = (
                "Chính sách phục vụ dùng unknown bucket (__unknown) kèm cảnh báo cho vùng chưa thấy trong tập train. "
                if os.environ.get("PRETRIP_ALLOW_UNSEEN_ZONES", "0") == "1"
                else "Chính sách phục vụ từ chối category chưa thấy theo từng vai trò. "
            )
            st.write("V2.2 dùng vùng/thời gian; khoảng cách tuyến chỉ tham khảo, không được đưa vào model. "
                     + pretrip_policy_note
                     + "MAE August giữ nguyên đủ rows, không phải bảo đảm sai số hiện tại hoặc từng chuyến."
                     if stored.get("model_version") == "v2.2" else
                     "Mô hình lịch sử 2025; quãng đường ô tô dự kiến có thể khác quãng đường đồng hồ taxi thực tế. "
                     "MAE lịch sử không bảo đảm sai số hiện tại hoặc sai số của từng chuyến.")
            st.write({"pickup_zone": stored["pickup"]["location_id"],
                      "dropoff_zone": stored["dropoff"]["location_id"],
                      "model_version": stored.get("model_version", "v1"),
                      **({"distance_miles": stored["request"]["distance_miles"]} if "distance_miles" in stored["request"] else {}),
                      "model_sha256": stored["prediction"]["model_sha256"]})

    # Register ALL keyed widgets before consuming events and requesting one
    # rerun; an early pickup rerun would otherwise clean up the dropoff widget.
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
