"""Streamlit entry point: historical PostgreSQL analytics only."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from nyc_taxi.dashboard import queries

CACHE_TTL_SECONDS = 300


@st.cache_data(ttl=CACHE_TTL_SECONDS, max_entries=32, show_spinner=False)
def load_dashboard(months: tuple[str, ...], boroughs: tuple[str, ...]) -> dict:
    """Both filters are hashed; failures are not cached and credentials are not returned."""
    return queries.fetch_dashboard(queries.DashboardFilters.from_selection(months, boroughs))


def main() -> None:
    st.set_page_config(page_title="NYC Taxi Analytics", page_icon="🚕", layout="wide")
    st.title("NYC Taxi Analytics")
    st.caption("Dashboard lịch sử · PostgreSQL · Chưa có mô hình dự đoán")
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
