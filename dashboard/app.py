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
            if selected_model_version() != "v2.2":
                raise PredictionError("Chế độ pretrip yêu cầu FARE_MODEL_VERSION=v2.2; không tự chuyển sang v1.")
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
    columns[0].metric("Total Trips", f"{data['total_trips']:,}")
    columns[1].metric(
        "Average Fare", "—" if data["average_fare"] is None else f"${data['average_fare']:,.2f}"
    )
    columns[2].metric(
        "Average Distance", "—" if data["average_distance"] is None else f"{data['average_distance']:,.2f} mi"
    )
    columns[3].metric("Total Fare", f"${data['total_fare']:,.2f}")
    st.caption(
        f"Aggregate SQL: {data['query_seconds']:.2f} giây · "
        f"Snapshot UTC: {data['queried_at']} · Cache tối đa {CACHE_TTL_SECONDS // 60} phút."
    )
    if data["total_trips"] == 0:
        st.info("Không có chuyến đi khớp bộ lọc. Hãy bỏ bớt lựa chọn rồi áp dụng lại.")
        st.stop()

    st.subheader("Số chuyến theo tháng nguồn")
    st.bar_chart(pd.DataFrame(data["by_month"]), x="source_month", y="trips")
    hour_column, borough_column = st.columns(2)
    with hour_column:
        st.subheader("Số chuyến theo giờ đón (NYC)")
        st.bar_chart(pd.DataFrame(data["by_hour"]), x="pickup_hour", y="trips")
    with borough_column:
        st.subheader("Số chuyến theo pickup borough")
        st.bar_chart(pd.DataFrame(data["by_borough"]), x="pickup_borough", y="trips")


if __name__ == "__main__":
    main()
