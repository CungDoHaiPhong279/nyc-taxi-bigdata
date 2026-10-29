"""Historical analytics or an explicitly selected database-free pretrip app."""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

CACHE_TTL_SECONDS = 300


@st.cache_data(ttl=CACHE_TTL_SECONDS, max_entries=32, show_spinner=False)
def load_dashboard(months: tuple[str, ...], boroughs: tuple[str, ...]) -> dict:
    """Both filters are hashed; failures are not cached and credentials are not returned."""
    from nyc_taxi.dashboard import queries

    return queries.fetch_dashboard(queries.DashboardFilters.from_selection(months, boroughs))


def main() -> None:
    mode = os.environ.get("NYC_TAXI_DASHBOARD_MODE", "full")
    if mode == "pretrip":
        st.set_page_config(page_title="NYC Taxi Pretrip", page_icon="🚕", layout="wide")
        from nyc_taxi.dashboard.prediction import PredictionError, selected_model_version

        try:
            if selected_model_version() not in ("v2.2", "gold-v3"):
                raise PredictionError("Chế độ pretrip yêu cầu FARE_MODEL_VERSION=v2.2 hoặc gold-v3; không tự chuyển sang v1.")
        except PredictionError as error:
            st.error(str(error))
            return
        if (os.environ.get("FARE_ADDRESS_ENABLED", "0") != "1"
                or os.environ.get("FARE_ADDRESS_ALLOW_NETWORK", "0") != "1"
                or not os.environ.get("MAPBOX_ACCESS_TOKEN", "").strip()):
            st.title("Ước tính cước trước chuyến đi")
            st.info(
                "Để tìm địa chỉ, đặt MAPBOX_ACCESS_TOKEN của bạn trong môi trường runtime và bật "
                "FARE_ADDRESS_ENABLED=1, FARE_ADDRESS_ALLOW_NETWORK=1. "
                "Chưa có cấu hình này thì ứng dụng không gọi API hoặc dùng mô hình thay thế."
            )
            return
        from nyc_taxi.dashboard.fare_page import render_page

        render_page()
        return
    if mode != "full":
        st.set_page_config(page_title="NYC Taxi", page_icon="🚕", layout="wide")
        st.error("NYC_TAXI_DASHBOARD_MODE chỉ chấp nhận full hoặc pretrip; chưa mở kết nối dữ liệu.")
        return

    from nyc_taxi.dashboard import queries

    st.set_page_config(page_title="NYC Taxi Analytics", page_icon="🚕", layout="wide")
    page = st.sidebar.radio("Trang", ("Phân tích lịch sử", "Ước tính cước"), key="dashboard_page")
    if page == "Ước tính cước":
        from nyc_taxi.dashboard.fare_page import render_page
        render_page()
        return
    st.title("NYC Taxi Analytics")
    st.caption("Dashboard lịch sử · PostgreSQL · Ước tính mô hình nằm ở trang riêng, không thay thế số liệu thực tế")
    st.markdown(
        "`fare_amount` là **cước cơ bản, đơn vị USD**, không phải `total_amount`. "
        "`trip_distance` có đơn vị **mile**. Giá trị fare **âm và 0 vẫn được giữ** "
        "trong các chỉ số và biểu đồ phân tích."
    )

    try:
        # Reuse the first full aggregate for options: no separate fact scan.
        # Options stay with the session; reopen the page after loading new months.
        if "filter_options" not in st.session_state:
            with st.spinner("Đang tổng hợp dữ liệu lịch sử từ PostgreSQL…"):
                baseline = load_dashboard((), ())
            st.session_state.filter_options = {
                "months": [row["source_month"] for row in baseline["by_month"]],
                "boroughs": [row["pickup_borough"] for row in baseline["by_borough"]],
            }

        options = st.session_state.filter_options
        with st.form("dashboard_filters"):
            month_column, borough_column = st.columns(2)
            months = month_column.multiselect(
                "Tháng nguồn", options["months"], key="selected_months",
                help="Không chọn = tất cả. Đây là source_year/source_month, không phải tháng đón.",
            )
            boroughs = borough_column.multiselect(
                "Pickup borough", options["boroughs"], key="selected_boroughs",
                help="Không chọn = tất cả. Có thể chọn nhiều borough.",
            )
            submitted = st.form_submit_button("Áp dụng bộ lọc", type="primary")

        if submitted or "active_filters" not in st.session_state:
            normalized = queries.DashboardFilters.from_selection(months, boroughs)
            st.session_state.active_filters = (normalized.months, normalized.boroughs)
        active_months, active_boroughs = st.session_state.active_filters
        st.caption(
            f"Đang áp dụng — Tháng nguồn: {', '.join(active_months) or 'Tất cả'}; "
            f"Pickup borough: {', '.join(active_boroughs) or 'Tất cả'}. "
            "Thay đổi lựa chọn rồi bấm Áp dụng bộ lọc."
        )
        with st.spinner("Đang truy vấn aggregate; bộ lọc hẹp có thể giảm thời gian chờ…"):
            data = load_dashboard(active_months, active_boroughs)
    except queries.DashboardQueryError as exc:
        st.error(str(exc))
        st.button("Thử kết nối lại", key="retry_connection")
        st.stop()

    columns = st.columns(4)
    columns[0].metric("Tổng số chuyến (Total Trips)", f"{data['total_trips']:,}")
    columns[1].metric(
        "Giá cước TB (Average Fare)", "—" if data["average_fare"] is None else f"${data['average_fare']:,.2f}"
    )
    columns[2].metric(
        "Quãng đường TB (Average Distance)", "—" if data["average_distance"] is None else f"{data['average_distance']:,.2f} mi"
    )
    columns[3].metric("Tổng doanh thu cước (Total Fare)", f"${data['total_fare']:,.2f}")
    st.caption(
        f"Aggregate SQL: {data['query_seconds']:.2f} giây · "
        f"Snapshot UTC: {data['queried_at']} · Cache tối đa {CACHE_TTL_SECONDS // 60} phút."
    )
    if data["total_trips"] == 0:
        st.info("Không có chuyến đi khớp bộ lọc. Hãy bỏ bớt lựa chọn rồi áp dụng lại.")
        st.stop()

    st.subheader("1. Số lượng chuyến xe theo từng Tháng (2025)")
    st.caption("Biểu đồ thể hiện sản lượng chuyến đi theo từng tháng nguồn trong kho dữ liệu PostgreSQL (23,88 triệu chuyến).")
    st.bar_chart(pd.DataFrame(data["by_month"]), x="source_month", y="trips", color="#3b82f6")
    hour_column, borough_column = st.columns(2)
    with hour_column:
        st.subheader("2. Phân bố chuyến xe theo 24 Giờ đón (Giờ NYC)")
        st.caption("Nhịp độ di chuyển trong ngày: thấp điểm lúc 3h–5h sáng và đạt đỉnh vào khung giờ cao điểm chiều 17h–19h.")
        st.bar_chart(pd.DataFrame(data["by_hour"]), x="pickup_hour", y="trips", color="#10b981")
    with borough_column:
        st.subheader("3. Phân bố chuyến xe theo Quận đón (Borough)")
        st.caption("Manhattan và Queens (gồm sân bay JFK & LaGuardia) chiếm tỷ trọng lớn nhất trong toàn mạng lưới.")
        st.bar_chart(pd.DataFrame(data["by_borough"]), x="pickup_borough", y="trips", color="#f59e0b")

    st.divider()
    st.subheader("4. Tổng hợp Tích hợp Đa nguồn Dữ liệu (Taxi × Thời tiết ERA5 × Lịch Nghỉ lễ)")
    st.caption(
        "Số liệu đối chiếu từ tầng Medallion Gold (23,88 triệu chuyến khớp 99,92% thời tiết ERA5 và 100% lịch nghỉ lễ Mỹ):"
    )
    ms_col1, ms_col2 = st.columns(2)
    with ms_col1:
        st.markdown("##### Tác động của Điều kiện Thời tiết ERA5 đến Chuyến đi")
        st.dataframe(
            [
                {
                    "Điều kiện Thời tiết (ERA5)": "1. Trời quang / Khô ráo (0 mm/h)",
                    "Tỷ trọng chuyến (%)": "74.6%",
                    "Giá cước TB (USD)": 16.71,
                    "Khoảng cách tâm vùng TB (km)": 3.17,
                },
                {
                    "Điều kiện Thời tiết (ERA5)": "2. Mưa nhỏ & Mưa vừa (< 2 mm/h)",
                    "Tỷ trọng chuyến (%)": "23.6%",
                    "Giá cước TB (USD)": 16.87,
                    "Khoảng cách tâm vùng TB (km)": 3.07,
                },
                {
                    "Điều kiện Thời tiết (ERA5)": "3. Mưa lớn / Có tuyết (>= 2 mm/h)",
                    "Tỷ trọng chuyến (%)": "1.8%",
                    "Giá cước TB (USD)": 16.29,
                    "Khoảng cách tâm vùng TB (km)": 2.79,
                },
            ]
        )
    with ms_col2:
        st.markdown("##### So sánh Ngày làm việc vs Cuối tuần & Ngày Nghỉ lễ")
        st.dataframe(
            [
                {
                    "Nhóm Ngày (US Calendar)": "1. Ngày làm việc (Thứ 2 – Thứ 6)",
                    "Tỷ trọng chuyến (%)": "71.3%",
                    "Giá cước TB (USD)": 16.87,
                    "Khoảng cách tâm vùng TB (km)": 3.09,
                },
                {
                    "Nhóm Ngày (US Calendar)": "2. Cuối tuần (Thứ 7 & Chủ Nhật)",
                    "Tỷ trọng chuyến (%)": "26.1%",
                    "Giá cước TB (USD)": 16.43,
                    "Khoảng cách tâm vùng TB (km)": 3.26,
                },
                {
                    "Nhóm Ngày (US Calendar)": "3. Ngày nghỉ lễ Liên bang (Holiday)",
                    "Tỷ trọng chuyến (%)": "2.6%",
                    "Giá cước TB (USD)": 16.34,
                    "Khoảng cách tâm vùng TB (km)": 3.32,
                },
            ]
        )


if __name__ == "__main__":
    main()
