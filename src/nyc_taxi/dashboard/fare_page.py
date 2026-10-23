"""Streamlit fare form backed only by the explicit frozen-model HTTP service."""

from __future__ import annotations

from datetime import date, datetime, time
import os

import streamlit as st

from nyc_taxi.dashboard.prediction import DEFAULT_INFERENCE_URL, InferenceClient, PredictionError


UNSEEN_WARNING = "Vùng này chưa có trong dữ liệu huấn luyện; kết quả có thể kém chính xác hơn."


def _is_unseen_warning(message: str) -> bool:
    """Recognize duplicate category notices, not unrelated model limitations."""
    normalized = " ".join(message.casefold().replace("_", " ").split())
    return any(marker in normalized for marker in (
        "unknown bucket", "unknownbucket", "unseen",
        "chưa thấy trong train", "chưa xuất hiện trong train", "chưa có trong dữ liệu huấn luyện",
    ))


@st.cache_data(ttl=60, max_entries=4, show_spinner=False)
def load_model_metadata(base_url: str) -> dict:
    """Cache only bounded public readiness/zone metadata, not predicted prices."""
    client = InferenceClient(base_url)
    ready, zones = client.ready(), client.zones()
    if any(ready[key] != zones[key] for key in ("training_run_id", "evaluation_run_id", "model_sha256")):
        raise PredictionError("Danh sách zone và mô hình không cùng phiên bản. Không hiển thị giá.")
    return {"ready": ready, "zones": zones["zones"]}


def render_page() -> None:
    st.title("Ước tính cước")
    st.caption("Mô hình lịch sử 2025 · Quãng đường do người dùng cung cấp · Không định tuyến hoặc dự báo ETA")
    st.markdown(
        "Kết quả là **cước cơ bản `fare_amount`, USD**, cho chuyến **standard metered**. "
        "Không phải tổng tiền: **không bao gồm tip, tolls, thuế/phụ phí/surcharges**. "
        "Không hỗ trợ flat fare, negotiated fare, refund hoặc no-charge; không phải báo giá thương mại."
    )
    st.info(
        "Khoảng cách (mile) và điểm đến là thông tin bạn cung cấp, không phải tuyến đường hệ thống tính. "
        "Dữ liệu train dùng khoảng cách/điểm đến thực tế: sai khác với dự kiến có thể làm tăng sai số. "
        "MAE tổng thể không phải bảo đảm sai số hay khoảng tin cậy cho từng chuyến."
    )
    base_url = os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL)
    try:
        metadata = load_model_metadata(base_url)
    except PredictionError as error:
        st.error(str(error))
        st.button("Thử kết nối dịch vụ lại", key="fare_retry_metadata")
        return
    except Exception:
        st.error("Không thể kiểm tra dịch vụ ước tính. Hãy thử lại; không có giá thay thế.")
        st.button("Thử kết nối dịch vụ lại", key="fare_retry_metadata")
        return
    zones = {row["location_id"]: row for row in metadata["zones"]}
    ids = list(zones)

    def zone_label(location: int) -> str:
        row = zones[location]
        return f"{location} — {row['borough']} — {row['zone']}"

    with st.form("fare_estimate_form"):
        distance = st.number_input(
            "Khoảng cách do bạn cung cấp (mile)", min_value=0.0, max_value=200.0,
            value=1.0, step=0.1, format="%.2f", key="fare_distance",
        )
        left, right = st.columns(2)
        pickup_date = left.date_input(
            "Ngày đón (giờ địa phương NYC)", value=date(2025, 6, 15),
            min_value=date(2025, 1, 1), max_value=date(2025, 12, 31), key="fare_pickup_date",
        )
        pickup_time = right.time_input("Giờ đón NYC, không UTC offset", value=time(12, 0), key="fare_pickup_time")
        pickup = st.selectbox("Zone đón", ids, format_func=zone_label, key="fare_pickup_zone")
        dropoff = st.selectbox("Zone đến dự kiến", ids, index=min(1, len(ids) - 1), format_func=zone_label, key="fare_dropoff_zone")
        standard = st.checkbox(
            "Tôi xác nhận đang xét chuyến standard metered; không phải flat/negotiated/refund/no-charge.",
            key="fare_standard_rate",
        )
        submitted = st.form_submit_button("Ước tính cước", type="primary")

    if pickup_date.month > 6:
        st.warning("Ngày đã chọn ngoài Jan–Jun/2025 đã kiểm chứng. Jul–Dec/2025 chưa được đánh giá; kết quả chỉ tham khảo.")
    if submitted:
        # A failed/new request must never retain a previous successful price.
        st.session_state.pop("fare_result", None)
        if not standard:
            st.error("Cần xác nhận phạm vi standard metered trước khi gửi yêu cầu.")
            return
        try:
            request = {
                "distance_miles": distance, "pickup_datetime": datetime.combine(pickup_date, pickup_time).isoformat(),
                "pickup_location_id": pickup, "dropoff_location_id": dropoff,
            }
            with st.spinner("Đang gọi mô hình đã đóng băng; không huấn luyện hoặc truy vấn database…"):
                result = InferenceClient(base_url).predict(**request)
            if any(result[key] != metadata["ready"][key] for key in ("training_run_id", "evaluation_run_id", "model_sha256")):
                raise PredictionError("Phiên bản mô hình đã đổi. Hãy mở lại trang trước khi ước tính.")
            st.session_state.fare_result = {"request": request, "result": result}
        except PredictionError as error:
            st.error(str(error))
            return
        except Exception:
            st.error("Không thể thực hiện ước tính. Hãy thử lại; không có giá thay thế.")
            return
    stored = st.session_state.get("fare_result")
    if stored:
        request, result = stored["request"], stored["result"]
        st.subheader("Kết quả cho yêu cầu gần nhất đã gửi")
        st.metric("Ước tính cước cơ bản (USD)", f"${result['fare_amount_usd']:,.2f}")
        st.caption(
            f"{request['distance_miles']:.2f} mile · {request['pickup_datetime']} NYC · "
            f"Điểm đón: {zones[request['pickup_location_id']]['borough']} — {zones[request['pickup_location_id']]['zone']} → "
            f"Điểm đến: {zones[request['dropoff_location_id']]['borough']} — {zones[request['dropoff_location_id']]['zone']}. "
            "Đây là yêu cầu gần nhất đã gửi. Thay đổi form rồi bấm nút để gửi yêu cầu mới."
        )
        if request["distance_miles"] == 0:
            st.warning("Khoảng cách bằng 0 được giữ đúng, không tự thay bằng giá trị khác; đây là nhóm cần thận trọng.")
        if request["distance_miles"] > 50:
            st.warning("Chuyến dài trên 50 mile có ít bằng chứng đánh giá hơn; không dùng kết quả như báo giá bảo đảm.")
        if result["fare_amount_usd"] > 1000:
            st.warning("Cước lớn trên 1.000 USD thuộc tail hiếm; không được cắt/clamp và không có bảo đảm độ chính xác.")
        if result["fare_amount_usd"] <= 0:
            st.warning("Mô hình trả kết quả không dương ngoài mục tiêu cước dương. Đây không phải giá thực thu; không tự clip kết quả.")
        has_unseen = any(result["unseen_categories"].values())
        if has_unseen:
            st.warning(UNSEEN_WARNING)
        for warning in result["warnings"]:
            if not (has_unseen and _is_unseen_warning(warning)):
                st.warning(warning)
    with st.expander("Phạm vi và mô hình đang phục vụ", expanded=False):
        st.write("Chỉ năm 2025; mô hình lịch sử, không sử dụng dữ liệu thời tiết trực tuyến hoặc giá dự đoán giả.")
        st.write("Distance 0, chuyến dài, cước lớn và zone unseen là các nhóm có giới hạn; không có nút huấn luyện.")
        if stored:
            st.caption(f"Thời gian xử lý API cho yêu cầu gần nhất đã gửi: {stored['result']['request_seconds']:.2f} giây.")
        st.code(
            "training_run_id=" + metadata["ready"]["training_run_id"] + "\n"
            + "evaluation_run_id=" + metadata["ready"]["evaluation_run_id"] + "\n"
            + "model_sha256=" + metadata["ready"]["model_sha256"], language=None,
        )
