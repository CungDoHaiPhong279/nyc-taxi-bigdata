"""Streamlit Gold V3 Unified Page:
1. Pre-trip Fare Estimation (M3 with ERA5 weather + M3-nw without weather + JFK $70 flat-fare rule)
2. 5-Model Comparison, Weather Ablation Study & 2025->2026 Out-of-Time Drift Evaluation
3. Big Data Pipeline Monitoring (5 Committed Course Metrics from etl_load_log & manifests)
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
import json
import os
from typing import Any
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import text
import streamlit as st

from nyc_taxi.dashboard.prediction import DEFAULT_INFERENCE_URL
from nyc_taxi.database.postgres import get_engine
from nyc_taxi.monitoring.metrics import (
    fetch_etl_monitoring_metrics,
    fetch_model_and_serving_metrics,
    fetch_quality_and_reject_metrics,
)


NYC_TZ = ZoneInfo("America/New_York")


def _call_gold_v3_inference(base_url: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    req = Request(
        base_url.rstrip("/") + path,
        data=data,
        method="GET" if data is None else "POST",
        headers={"Accept": "application/json", **({"Content-Type": "application/json"} if data else {})},
    )
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


@st.cache_data(ttl=120, show_spinner=False)
def _load_zones_from_db_or_service(base_url: str) -> list[dict[str, Any]]:
    try:
        res = _call_gold_v3_inference(base_url, "/zones")
        if isinstance(res.get("zones"), list) and res["zones"]:
            return sorted(res["zones"], key=lambda r: int(r["location_id"]))
    except Exception:
        pass
    eng = get_engine()
    with eng.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT location_id, borough, zone FROM analytics.dim_location "
                "WHERE location_id BETWEEN 1 AND 263 ORDER BY location_id"
            )
        )
        return [dict(r._mapping) for r in rows]


@st.cache_data(ttl=180, show_spinner=False)
def _load_prediction_aggregates() -> dict[str, Any]:
    eng = get_engine()
    with eng.connect() as conn:
        by_hour = [
            dict(r._mapping)
            for r in conn.execute(
                text(
                    """
                    SELECT
                        pickup_hour,
                        COUNT(*) AS trips,
                        ROUND(AVG(actual_fare_usd)::numeric, 2) AS actual_fare_usd,
                        ROUND(AVG(pred_m3_gbt_weather_usd)::numeric, 2) AS pred_m3_weather_usd,
                        ROUND(AVG(pred_b0_hierarchical_usd)::numeric, 2) AS pred_b0_usd,
                        ROUND(AVG(abs_error_m3_weather_usd)::numeric, 2) AS mae_m3_usd
                    FROM analytics.vw_fare_prediction_enriched
                    WHERE split = 'test'
                    GROUP BY pickup_hour
                    ORDER BY pickup_hour
                    """
                )
            )
        ]
        by_borough_weather = [
            dict(r._mapping)
            for r in conn.execute(
                text(
                    """
                    SELECT
                        pickup_borough,
                        weather_condition,
                        COUNT(*) AS trips,
                        ROUND(AVG(actual_fare_usd)::numeric, 2) AS actual_fare_usd,
                        ROUND(AVG(pred_m3_gbt_weather_usd)::numeric, 2) AS pred_m3_weather_usd,
                        ROUND(AVG(abs_error_m3_weather_usd)::numeric, 3) AS mae_m3_weather_usd,
                        ROUND(AVG(abs_error_m3_nw_usd)::numeric, 3) AS mae_m3_no_weather_usd
                    FROM analytics.vw_fare_prediction_enriched
                    WHERE split = 'test'
                    GROUP BY pickup_borough, weather_condition
                    ORDER BY trips DESC
                    """
                )
            )
        ]
    return {"by_hour": by_hour, "by_borough_weather": by_borough_weather}


def render_gold_v3_page() -> None:
    base_url = os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL)
    st.title("NYC Taxi Big Data — Dự báo Giá cước & Đánh giá Mô hình (Gold V3)")
    st.caption(
        "Huấn luyện trực tiếp trên 23,88 triệu chuyến tầng Gold 2025 (NYC Taxi + ERA5 Weather + Holiday Calendar + Taxi Zones) · "
        "Phủ 100% Zone hoạt động · Hỗ trợ quy tắc giá cố định JFK ↔ Manhattan ($70.00 USD)"
    )

    tab_predict, tab_eval, tab_monitor = st.tabs(
        [
            "🚕 Ước tính Cước Trước Chuyến Đi (Gold V3)",
            "📊 Đánh giá 5 Mô hình, Thời tiết & Độ trôi 2026",
            "🛡️ Giám sát Pipeline Dữ liệu Lớn (5 Metrics)",
        ]
    )

    with tab_predict:
        try:
            zones = _load_zones_from_db_or_service(base_url)
        except Exception as exc:
            st.error(f"Không thể tải danh sách Taxi Zones: {exc}")
            return

        zone_map = {int(z["location_id"]): z for z in zones}
        zone_ids = [int(z["location_id"]) for z in zones if int(z["location_id"]) != 1]

        def _fmt_zone(zid: int) -> str:
            z = zone_map[zid]
            return f"{zid} — {z['borough']} — {z['zone']}"

        col_mode, col_weather_toggle = st.columns(2)
        time_mode = col_mode.radio(
            "Chế độ thời gian đón",
            ("Đi ngay (Giờ hiện tại tại New York)", "Đặt lịch trước / Chọn ngày giờ"),
            horizontal=True,
            key="gold_v3_time_mode",
        )
        include_weather = col_weather_toggle.checkbox(
            "Nhập điều kiện thời tiết ERA5 (So sánh M3 Weather vs M3-nw)",
            value=True,
            key="gold_v3_include_weather",
        )

        with st.form("gold_v3_predict_form"):
            c1, c2 = st.columns(2)
            default_pu = 161 if 161 in zone_ids else zone_ids[0]
            default_do = 236 if 236 in zone_ids else zone_ids[min(1, len(zone_ids) - 1)]
            pu_id = c1.selectbox(
                "Điểm đón (Pickup Zone)",
                zone_ids,
                index=zone_ids.index(default_pu),
                format_func=_fmt_zone,
                key="gold_v3_pu",
            )
            do_id = c2.selectbox(
                "Điểm trả (Dropoff Zone)",
                zone_ids,
                index=zone_ids.index(default_do),
                format_func=_fmt_zone,
                key="gold_v3_do",
            )

            if time_mode == "Đi ngay (Giờ hiện tại tại New York)":
                now_nyc = datetime.now(timezone.utc).astimezone(NYC_TZ).replace(tzinfo=None)
                st.info(f"Giờ hiện tại tại New York (America/New_York): **{now_nyc.strftime('%Y-%m-%d %H:%M:%S')}**")
                chosen_dt = now_nyc
            else:
                d_col, t_col = st.columns(2)
                p_date = d_col.date_input(
                    "Ngày đón (NYC)",
                    value=date(2025, 6, 15),
                    min_value=date(2025, 1, 1),
                    max_value=date(2027, 12, 31),
                )
                p_time = t_col.time_input("Giờ đón (NYC)", value=time(8, 30))
                chosen_dt = datetime.combine(p_date, p_time)

            temp_2m, precip, snow, wind_10m = 18.0, 0.0, 0.0, 10.0
            if include_weather:
                wc1, wc2, wc3, wc4 = st.columns(4)
                temp_2m = wc1.number_input("Nhiệt độ (°C)", min_value=-25.0, max_value=45.0, value=18.0, step=1.0)
                precip = wc2.number_input("Lượng mưa (mm/h)", min_value=0.0, max_value=100.0, value=0.0, step=0.5)
                snow = wc3.number_input("Lượng tuyết (cm/h)", min_value=0.0, max_value=50.0, value=0.0, step=0.5)
                wind_10m = wc4.number_input("Tốc độ gió (km/h)", min_value=0.0, max_value=120.0, value=10.0, step=1.0)

            submitted = st.form_submit_button("Ước tính Giá cước (Gold V3)", type="primary")

        if submitted:
            payload: dict[str, Any] = {
                "pickup_location_id": int(pu_id),
                "dropoff_location_id": int(do_id),
                "pickup_datetime": chosen_dt.isoformat(),
            }
            if include_weather:
                payload.update(
                    temperature_2m=float(temp_2m),
                    precipitation=float(precip),
                    snowfall=float(snow),
                    wind_speed_10m=float(wind_10m),
                )
            try:
                with st.spinner("Đang suy luận trên mô hình PySpark GBT Gold V3..."):
                    res = _call_gold_v3_inference(base_url, "/predict", payload)
                st.session_state["gold_v3_last_result"] = res
            except Exception as exc:
                st.error(f"Không thể gọi dịch vụ suy luận Gold V3 tại `{base_url}`: {exc}")

        last_res = st.session_state.get("gold_v3_last_result")
        if last_res:
            for w in last_res.get("warnings", []):
                st.warning(w)
            m1, m2, m3, m4 = st.columns(4)
            m1.metric(
                "Giá cước Khuyến nghị (USD)",
                f"${last_res['fare_amount_usd']:,.2f}",
                delta="Flat Fare (Ratecode 2)" if last_res.get("flat_fare_applied") else "Standard Metered (Ratecode 1)",
            )
            m2.metric(
                "M3 (GBT + Thời tiết ERA5)",
                "—"
                if last_res.get("m3_weather_estimate_usd") is None
                else f"${last_res['m3_weather_estimate_usd']:,.2f}",
            )
            m3.metric(
                "M3-nw (GBT Không Thời tiết)",
                f"${last_res['m3_no_weather_estimate_usd']:,.2f}",
            )
            m4.metric(
                "Khoảng cách Tâm Vùng (Haversine)",
                f"{last_res['zone_centroid_distance_km']:,.2f} km",
                delta=(
                    f"Baseline B0: ${last_res['baseline_hierarchical_usd']:,.2f}"
                    if last_res.get("baseline_hierarchical_usd")
                    else None
                ),
            )

    with tab_eval:
        st.subheader("Bảng So sánh Định lượng 5 Mô hình & Kiểm tra Độ trôi 2025 → 2026")
        model_info = fetch_model_and_serving_metrics(inference_url=base_url)["metric_5_model_and_serving"]
        m_rows = model_info["model_metrics"]
        if m_rows:
            df_metrics = pd.DataFrame(m_rows)
            split_filter = st.selectbox(
                "Chọn tập đánh giá (Split)",
                ["test", "validation", "train", "drift_2026-06", "drift_2026-07", "drift_2026-08"],
                index=0,
            )
            sub_df = df_metrics[df_metrics["split"] == split_filter].copy()
            if not sub_df.empty:
                st.dataframe(
                    sub_df[
                        [
                            "model_name",
                            "uses_weather",
                            "row_count",
                            "mae_usd",
                            "rmse_usd",
                            "r2",
                            "p50_abs_error_usd",
                            "p90_abs_error_usd",
                            "p95_abs_error_usd",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

        try:
            aggs = _load_prediction_aggregates()
            if aggs["by_hour"]:
                st.subheader("Giá Thực tế vs Giá Dự đoán (M3 GBT + Weather & Baseline B0) theo Giờ Đón — Tập Test 06/2025")
                df_hour = pd.DataFrame(aggs["by_hour"])
                st.line_chart(
                    df_hour.set_index("pickup_hour")[["actual_fare_usd", "pred_m3_weather_usd", "pred_b0_usd"]]
                )
            if aggs["by_borough_weather"]:
                st.subheader("Phân tích Sai số MAE theo Quận (Borough) và Điều kiện Thời tiết (Mưa vs Tạnh)")
                st.dataframe(pd.DataFrame(aggs["by_borough_weather"]), use_container_width=True, hide_index=True)
        except Exception:
            st.info("Bảng `analytics.vw_fare_prediction_enriched` chưa được nạp hoặc đang cập nhật.")

    with tab_monitor:
        st.subheader("5 Chỉ số Giám sát Hệ thống Tích hợp & Phân tích Dữ liệu Lớn")
        try:
            etl_m = fetch_etl_monitoring_metrics()
            qual_m = fetch_quality_and_reject_metrics()["metric_4_quality_and_rejects"]
            srv_m = fetch_model_and_serving_metrics(inference_url=base_url)["metric_5_model_and_serving"]

            k1, k2, k3, k4, k5 = st.columns(5)
            k1.metric(
                "1. Job Status",
                f"{etl_m['metric_1_job_status']['loaded_jobs']}/{etl_m['metric_1_job_status']['total_jobs']} Loaded",
                delta=f"{etl_m['metric_1_job_status']['success_rate_pct']}% thành công",
            )
            k2.metric(
                "2. Processing Time",
                f"{etl_m['metric_2_processing_time']['total_load_seconds']:,.1f} s",
                delta=f"TB {etl_m['metric_2_processing_time']['avg_job_seconds']:,.1f} s/batch",
            )
            k3.metric(
                "3. Row Reconciliation",
                f"{etl_m['metric_3_row_reconciliation']['total_loaded_rows']:,}",
                delta=f"Delta = {etl_m['metric_3_row_reconciliation']['row_delta']} dòng",
            )
            k4.metric(
                "4. Silver Reject Rate",
                f"{qual_m['silver_cleaning']['silver_reject_rate_pct']}%",
                delta=f"Loại {qual_m['silver_cleaning']['silver_rejected_rows']:,} dòng bẩn",
            )
            k5.metric(
                "5. Serving Readiness",
                "READY" if srv_m["serving_health"].get("ready") else "OFFLINE",
                delta=f"Model: {srv_m['serving_health'].get('model_version', 'N/A')}",
            )

            st.markdown("#### Chi tiết Nhật ký Nạp Dữ liệu (`analytics.etl_load_log`)")
            st.dataframe(pd.DataFrame(etl_m["jobs"]), use_container_width=True, hide_index=True)
        except Exception as exc:
            st.error(f"Không thể truy vấn bảng giám sát ETL: {exc}")
