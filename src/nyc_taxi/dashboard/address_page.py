"""Opt-in address flow. Offline transport/geometry/coordinator can be injected."""

from __future__ import annotations

from datetime import date, datetime, time
import os
from uuid import uuid4

import streamlit as st

from nyc_taxi.dashboard.address_component import (
    AddressSelectionError, address_picker, new_address_state, process_address_event,
)
from nyc_taxi.dashboard.mapbox_client import MapboxClient, MapboxError
from nyc_taxi.dashboard.prediction import InferenceClient
from nyc_taxi.dashboard.route_fare import RouteFareCoordinator, execute_submission
from nyc_taxi.dashboard.taxi_zones import GeometryError, TaxiZoneGeometry


def address_flow_enabled() -> bool:
    return os.environ.get("FARE_ADDRESS_ENABLED", "0") == "1"


def make_search_client():
    # 2A cannot issue accidental API calls, even with the UI feature enabled.
    # Offline tests inject this factory. 2B must explicitly enable live transport.
    if os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") != "1":
        raise AddressSelectionError("Luồng địa chỉ đang ở chế độ offline. API thật chưa được bật.")
    return MapboxClient(os.environ.get("MAPBOX_ACCESS_TOKEN", ""))


@st.cache_resource(max_entries=2, show_spinner=False)
def load_geometry(path: str, sha256: str):
    # Only public, pinned geometry is cached process-wide; never user addresses.
    return TaxiZoneGeometry.load(path, sha256)


def make_coordinator():
    geometry = load_geometry(os.environ.get("FARE_ADDRESS_GEOMETRY_PATH", ""),
                             os.environ.get("FARE_ADDRESS_GEOMETRY_SHA256", ""))
    return RouteFareCoordinator(router=make_search_client(), geometry=geometry, predictor=InferenceClient())


def render_page() -> None:
    st.title("Ước tính cước từ địa chỉ")
    st.caption("Demo lịch sử Jan–Jun/2025 · NYC · standard metered yellow taxi")
    st.markdown("Ước tính **cước cơ bản `fare_amount`, USD**, chưa gồm tip, tolls và phụ phí. "
                "Quãng đường ô tô hiển thị bằng **km**; mô hình nhận **mile**. "
                "Tuyến dự kiến có thể khác quãng đường taximeter đã dùng để đánh giá mô hình.")
    if os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") != "1":
        st.info("Chặng 2A offline: chưa có dữ liệu geometry hoặc API live. "
                "Luồng được kiểm bằng dữ liệu tổng hợp; không có giá thay thế khi chưa đủ cấu hình.")

    for slot, label in (("pickup", "Địa chỉ đón"), ("dropoff", "Địa chỉ đến")):
        key = "address_" + slot
        if key not in st.session_state:
            st.session_state[key] = new_address_state()
        state = st.session_state[key]
        event = address_picker(state, label=label, key=key + "_picker")
        if isinstance(event, dict) and event.get("event_id") not in state["events"] and len(state["events"]) >= 64:
            state["error"] = "Phiên tìm kiếm đã đủ số thao tác. Hãy mở phiên trình duyệt mới."
        elif isinstance(event, dict) and event.get("event_id") not in state["events"]:
            try:
                process_address_event(state, event, make_search_client())
            except (AddressSelectionError, MapboxError):
                # Remember rejected events too, so a rerun cannot retry HTTP.
                if event.get("event_id") not in state["events"]:
                    state["events"].append(event.get("event_id"))
                state["selected"] = None
                state["error"] = "Chưa thể tìm địa chỉ. Cần cấu hình API/geometry ở Chặng 2B."
            st.rerun()
        if state["error"]:
            st.error(state["error"])

    with st.form("address_fare_form"):
        pickup_date = st.date_input("Ngày đón NYC", value=date(2025, 6, 15),
                                    min_value=date(2025, 1, 1), max_value=date(2025, 6, 30))
        pickup_time = st.time_input("Giờ đón NYC", value=time(12))
        standard = st.checkbox("Tôi xác nhận standard metered yellow taxi; không flat/negotiated/group fare.")
        submitted = st.form_submit_button("Tính tuyến và ước tính cước", type="primary")
    if submitted:
        st.session_state.pop("address_fare_result", None)
        st.session_state.pop("address_fare_error", None)
        payload = {"pickup": st.session_state.address_pickup["selected"],
                   "dropoff": st.session_state.address_dropoff["selected"],
                   "pickup_datetime": datetime.combine(pickup_date, pickup_time).isoformat(),
                   "standard_rate": standard}
        if payload["pickup"] is None or payload["dropoff"] is None:
            st.session_state.address_fare_error = "Hãy chọn và xác nhận đủ hai địa chỉ trước khi gửi."
        else:
            try:
                with st.spinner("Đang tính tuyến và gọi mô hình đã chốt…"):
                    outcome = execute_submission(st.session_state, str(uuid4()), payload, make_coordinator())
                if outcome.success:
                    st.session_state.address_fare_result = outcome.estimate.to_dict()
                else:
                    st.session_state.address_fare_error = outcome.error
            except (AddressSelectionError, MapboxError, GeometryError):
                st.session_state.address_fare_error = "Chưa đủ cấu hình API/geometry đã pin. Chuẩn bị Chặng 2B trước khi dùng live."
            except Exception:
                st.session_state.address_fare_error = "Không thể ước tính cước. Không có giá thay thế."

    if st.session_state.get("address_fare_error"):
        st.error(st.session_state.address_fare_error)
    stored = st.session_state.get("address_fare_result")
    if stored:
        st.subheader("Kết quả cho yêu cầu gần nhất đã gửi")
        st.metric("Ước tính cước cơ bản (USD)", f"${stored['prediction']['fare_amount_usd']:,.2f}")
        st.caption(f"{stored['pickup']['label']} → {stored['dropoff']['label']} · "
                   f"{stored['distance_km']:.3f} km · {stored['pickup_datetime']} NYC. "
                   "Thay đổi lựa chọn chưa làm thay đổi yêu cầu đã gửi này.")
        for warning in stored["warnings"]:
            st.warning(warning)
        prediction = stored["prediction"]
        unseen = any(prediction.get("unseen_categories", {}).values())
        if unseen:
            st.warning("Vùng này chưa có trong dữ liệu huấn luyện; kết quả có thể kém chính xác hơn.")
        if prediction["fare_amount_usd"] <= 0:
            st.warning("Mô hình trả cước không dương; không tự cắt hoặc thay giá trị dự đoán.")
        st.info("MAE lịch sử không phải bảo đảm sai số cho luồng địa chỉ → tuyến → giá. "
                "Chuyến xa, quãng đường 0, cước cao và vùng chưa thấy có độ bất định lớn hơn.")
        with st.expander("Chi tiết mô hình và yêu cầu đã gửi"):
            st.write({"pickup_zone": stored["pickup"]["location_id"],
                      "dropoff_zone": stored["dropoff"]["location_id"],
                      "distance_miles": stored["request"]["distance_miles"],
                      "model_sha256": stored["prediction"]["model_sha256"]})
