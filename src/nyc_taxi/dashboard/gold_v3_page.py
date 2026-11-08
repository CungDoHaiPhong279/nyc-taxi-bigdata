"""Streamlit Page for Gold V3 (`FARE_MODEL_VERSION=gold-v3`).

Redesigned into 3 clean, self-explanatory tabs:
1. Tab 1 (`🚕 Ước tính cước chuyến đi`): Mapbox Address Autocomplete + Interactive Route Map UI
   (`address_page.render_page`) backed by the frozen Gold V3 Spark GBT model (`M3-nw` pricing without weather, `M3-w` only for manually entered weather, & JFK 70 USD Rule + Official NYC TLC Surcharges).
2. Tab 2 (`📊 Độ chính xác & So sánh Mô hình`): Clear visual comparison of all 5 models (`B0`, `M1`, `M2`, `M3-w`, `M3-nw`),
   24-hour actual vs predicted fare tracking, and 2026 Out-of-Time Drift verification (`721.513` trips across June–August 2026).
3. Tab 3 (`🩺 Giám sát Hệ thống & Dữ liệu`): Medallion Data Pipeline Flow Diagram + 5 Pipeline Monitoring Metrics
   (`Job Status`, `Processing Time`, `Row Reconciliation`, `Silver Reject Rate 0.82%`, `Serving Health & Container RAM Snapshot`).
"""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
import json
import os
from typing import Any
import urllib.request
from zoneinfo import ZoneInfo

import streamlit as st
from sqlalchemy import text

from nyc_taxi.dashboard.address_page import address_flow_enabled, render_page as render_address_page
from nyc_taxi.dashboard.ai_fare_advisor import (
    build_fare_explanation_and_advice,
    load_route_hour_history,
    fetch_nyc_hourly_weather,
    render_smart_fare_advisor_panel,
)
from nyc_taxi.database.postgres import get_engine
from nyc_taxi.dashboard.prediction import DEFAULT_INFERENCE_URL
from nyc_taxi.monitoring.metrics import (
    fetch_etl_monitoring_metrics,
    fetch_model_and_serving_metrics,
    fetch_quality_and_reject_metrics,
)

NYC_TZ = ZoneInfo("America/New_York")

BASELINE_MODEL_KEY = "B0_hierarchical_median"
SERVED_MODEL_KEY = "M3_nw_gbt_no_weather"

MODEL_DISPLAY_LABELS: dict[str, str] = {
    "B0_global_median": "1. Trung vị toàn cục (Mốc cơ sở đơn giản)",
    "B0_hierarchical_median": "2. Thống kê theo Tuyến & Giờ (Baseline B0)",
    "M1_linear_regression": "3. Hồi quy Tuyến tính (Linear Regression - M1)",
    "M2_random_forest": "4. Rừng Ngẫu nhiên (Random Forest - M2)",
    "M3_gbt_weather": "5. GBT + Thời tiết ERA5 (M3-w - Đối chứng)",
    "M3_nw_gbt_no_weather": "6. GBT Tối ưu Không cần Thời tiết (M3-nw - Cốt lõi)",
}

SPLIT_DISPLAY_LABELS: dict[str, str] = {
    "train": "Huấn luyện (01–04/2025)",
    "validation": "Kiểm định (05/2025)",
    "test": "Kiểm thử Độc lập (06/2025)",
    "drift_2026-06": "Thực tế Tháng 06/2026",
    "drift_2026-07": "Thực tế Tháng 07/2026",
    "drift_2026-08": "Thực tế Tháng 08/2026",
}


def _drift_chart_rows(metrics_rows: list[dict]) -> list[dict]:
    return [
        r
        for r in metrics_rows
        if str(r["split"]).startswith("drift_2026-")
        and r["model_key"] in (BASELINE_MODEL_KEY, SERVED_MODEL_KEY)
    ]


def _clean_row(mapping_row: Any) -> dict[str, Any]:
    """Convert SQLAlchemy RowMapping to plain dict, casting Decimal to float for Vega-Lite."""
    out: dict[str, Any] = {}
    for k, v in dict(mapping_row).items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        else:
            out[k] = v
    return out


@st.cache_data(ttl=60, show_spinner=False)
def _load_gold_v3_zones(base_url: str) -> dict[str, Any]:
    req = urllib.request.Request(f"{base_url.rstrip('/')}/zones", method="GET")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _predict_gold_v3(base_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload, allow_nan=False).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url.rstrip('/')}/predict",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


@st.cache_data(ttl=120, show_spinner=False)
def _cached_model_metrics() -> list[dict[str, Any]]:
    eng = get_engine()
    try:
        with eng.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                        model_key,
                        model_name,
                        split,
                        uses_weather,
                        row_count,
                        ROUND(mae_usd::numeric, 4)::double precision AS mae_usd,
                        ROUND(rmse_usd::numeric, 4)::double precision AS rmse_usd,
                        ROUND(r2::numeric, 4)::double precision AS r2,
                        ROUND(p50_abs_error_usd::numeric, 4)::double precision AS p50_usd,
                        ROUND(p90_abs_error_usd::numeric, 4)::double precision AS p90_usd,
                        ROUND(p95_abs_error_usd::numeric, 4)::double precision AS p95_usd
                    FROM analytics.model_metrics
                    ORDER BY
                        CASE split
                            WHEN 'train' THEN 1
                            WHEN 'validation' THEN 2
                            WHEN 'test' THEN 3
                            WHEN 'drift_2026-06' THEN 4
                            WHEN 'drift_2026-07' THEN 5
                            WHEN 'drift_2026-08' THEN 6
                            ELSE 7
                        END,
                        model_key
                    """
                )
            ).mappings().all()
            return [_clean_row(r) for r in rows]
    finally:
        eng.dispose()


def _seconds_label(value: Any) -> str:
    return "N/A" if value is None else f"{float(value):.1f}"


def _query_model_metrics() -> list[dict[str, Any]]:
    try:
        return _cached_model_metrics()
    except Exception:
        return []


@st.cache_data(ttl=120, show_spinner=False)
def _cached_prediction_summary() -> dict[str, Any]:
    eng = get_engine()
    try:
        with eng.connect() as conn:
            by_hour = conn.execute(
                text(
                    """
                    SELECT
                        pickup_hour::int AS pickup_hour,
                        COUNT(*)::int AS trips,
                        ROUND(AVG(actual_fare_usd)::numeric, 2)::double precision AS avg_actual_fare,
                        ROUND(AVG(pred_m3_gbt_weather_usd)::numeric, 2)::double precision AS avg_pred_m3_weather,
                        ROUND(AVG(pred_m3_nw_gbt_usd)::numeric, 2)::double precision AS avg_pred_m3_no_weather,
                        ROUND(AVG(abs_error_m3_weather_usd)::numeric, 2)::double precision AS mae_m3_weather
                    FROM analytics.vw_fare_prediction_enriched
                    WHERE split = 'test'
                    GROUP BY pickup_hour
                    ORDER BY pickup_hour
                    """
                )
            ).mappings().all()

            by_borough = conn.execute(
                text(
                    """
                    SELECT
                        COALESCE(pickup_borough, 'Unknown') || ' -> ' || COALESCE(dropoff_borough, 'Unknown') AS borough_pair,
                        COUNT(*)::int AS trips,
                        ROUND(AVG(actual_fare_usd)::numeric, 2)::double precision AS avg_actual_fare,
                        ROUND(AVG(pred_m3_gbt_weather_usd)::numeric, 2)::double precision AS avg_pred_m3_weather,
                        ROUND(AVG(pred_m3_nw_gbt_usd)::numeric, 2)::double precision AS avg_pred_m3_no_weather,
                        ROUND(AVG(abs_error_m3_nw_usd)::numeric, 2)::double precision AS mae_m3_nw_usd
                    FROM analytics.vw_fare_prediction_enriched
                    WHERE split = 'test'
                    GROUP BY 1
                    HAVING COUNT(*) >= 50
                    ORDER BY trips DESC
                    LIMIT 12
                    """
                )
            ).mappings().all()

            return {
                "by_hour": [_clean_row(r) for r in by_hour],
                "by_borough": [_clean_row(r) for r in by_borough],
            }
    finally:
        eng.dispose()


def _query_prediction_summary() -> dict[str, Any]:
    try:
        return _cached_prediction_summary()
    except Exception:
        return {"by_hour": [], "by_borough": []}


@st.cache_data(ttl=60, show_spinner=False)
def _cached_monitoring_snapshot(base_url: str) -> dict[str, Any]:
    etl = fetch_etl_monitoring_metrics()
    qual = fetch_quality_and_reject_metrics()
    srv = fetch_model_and_serving_metrics(inference_base_url=base_url)
    return {"etl": etl, "qual": qual, "srv": srv}


def render_zone_weather_simulator(base_url: str) -> None:
    """Fallback zone-based UI used only when Mapbox address flow is disabled."""
    st.subheader("Ước tính cước theo Taxi Zone (1–263) & Thời tiết ERA5")
    st.caption(
        "Chọn trực tiếp bất kỳ cặp Taxi Zone nào trong 263 vùng NYC TLC và tùy chọn nhập thông số thời tiết ERA5 "
        "để ước tính giá cước bằng mô hình Gold V3."
    )
    try:
        zone_meta = _load_gold_v3_zones(base_url)
        zones = zone_meta.get("zones", [])
    except Exception:
        st.error("Dịch vụ suy luận Gold V3 hiện chưa sẵn sàng. Vui lòng kiểm tra trạng thái container.")
        return

    zone_labels = {
        int(z["location_id"]): f"{z['location_id']} — {z['borough']} — {z['zone']}"
        for z in zones
    }
    sorted_ids = sorted(zone_labels.keys())
    default_pu_idx = sorted_ids.index(161) if 161 in sorted_ids else 0
    default_do_idx = sorted_ids.index(236) if 236 in sorted_ids else min(1, len(sorted_ids) - 1)

    with st.form("gold_v3_predict_form"):
        col1, col2 = st.columns(2)
        with col1:
            pu_id = st.selectbox(
                "Điểm đón (Pickup Taxi Zone — Hỗ trợ đủ 263 vùng)",
                options=sorted_ids,
                index=default_pu_idx,
                format_func=lambda x: zone_labels[x],
            )
            pickup_date = st.date_input(
                "Ngày khởi hành (Giờ địa phương NYC)",
                value=date(2025, 6, 15),
                min_value=date(2025, 1, 1),
                max_value=date(2027, 12, 31),
            )
        with col2:
            do_id = st.selectbox(
                "Điểm trả (Dropoff Taxi Zone — Hỗ trợ đủ 263 vùng)",
                options=sorted_ids,
                index=default_do_idx,
                format_func=lambda x: zone_labels[x],
            )
            pickup_time = st.time_input(
                "Giờ khởi hành (America/New_York)",
                value=time(17, 30),
            )

        use_weather = st.checkbox(
            "Tự nhập thông số thời tiết để mô phỏng bằng M3-w (mặc định giá dùng M3-nw, không cần thời tiết)",
            value=False,
        )
        wcol1, wcol2, wcol3, wcol4 = st.columns(4)
        with wcol1:
            temp_c = st.number_input("Nhiệt độ (°C)", min_value=-30.0, max_value=45.0, value=22.0, step=1.0)
        with wcol2:
            precip_mm = st.number_input("Lượng mưa (mm/h)", min_value=0.0, max_value=100.0, value=0.0, step=0.5)
        with wcol3:
            snow_cm = st.number_input("Tuyết rơi (cm/h)", min_value=0.0, max_value=50.0, value=0.0, step=0.5)
        with wcol4:
            wind_kmh = st.number_input("Tốc độ gió (km/h)", min_value=0.0, max_value=120.0, value=12.0, step=1.0)

        submitted = st.form_submit_button("Dự đoán giá cước (Gold V3)", type="primary")

    if submitted:
        target_dt = datetime.combine(pickup_date, pickup_time).replace(tzinfo=NYC_TZ)
        dt_iso = target_dt.isoformat()
        req_payload: dict[str, Any] = {
            "pickup_location_id": int(pu_id),
            "dropoff_location_id": int(do_id),
            "pickup_datetime": dt_iso,
        }
        if use_weather:
            weather_info_used = {
                "temperature_2m": float(temp_c),
                "precipitation": float(precip_mm),
                "snowfall": float(snow_cm),
                "wind_speed_10m": float(wind_kmh),
                "condition_vi": "Thông số thời tiết giả lập thủ công",
                "source": "manual-override",
            }
            for w_k in ("temperature_2m", "precipitation", "snowfall", "wind_speed_10m"):
                req_payload[w_k] = float(weather_info_used[w_k])
        else:
            # Auto weather is display-only: it is never sent to /predict.
            auto_w = fetch_nyc_hourly_weather(target_dt, allow_network=True)
            weather_info_used = {
                k: v for k, v in auto_w.items() if k not in ("hourly_24h", "hourly_24h_compact")
            }

        zone_by_id = {int(z["location_id"]): z for z in zones}
        try:
            res = _predict_gold_v3(base_url, req_payload)
            advice = build_fare_explanation_and_advice(
                prediction=res,
                pickup_zone=zone_by_id.get(int(pu_id), {"location_id": int(pu_id), "borough": "", "zone": ""}),
                dropoff_zone=zone_by_id.get(int(do_id), {"location_id": int(do_id), "borough": "", "zone": ""}),
                pickup_dt=target_dt,
                mapbox_distance_km=None,
                weather_info=weather_info_used,
                mode="scheduled",
                route_hour_history=load_route_hour_history(int(pu_id), int(do_id), target_dt),
            )
            is_flat_jfk = bool(res.get("flat_fare_applied"))
            m1, m2, m3, m4 = st.columns(4)
            m1.metric(
                "Tổng thanh toán gồm phí TLC" if is_flat_jfk else "Tổng giá cước dự kiến",
                f"{advice['total_estimated_fare_usd']:.2f} USD",
            )
            m2.metric(
                "Cước cố định JFK (Ratecode 2)" if is_flat_jfk else f"Cước đồng hồ ML ({advice['metered_model_label']})",
                f"{res['fare_amount_usd']:.2f} USD",
            )
            m3.metric("Khoảng cách tâm vùng (Haversine)", f"{res.get('zone_centroid_distance_km', 0.0):.2f} km")
            m4.metric("Biến thể phục vụ", "Ratecode 2 (70 USD)" if is_flat_jfk else advice["metered_model_label"])

            render_smart_fare_advisor_panel(advice)

            for w in res.get("warnings", []):
                if "Ratecode 2" not in str(w):
                    st.info(str(w).replace("$", ""))

            with st.expander("Chi tiết kỹ thuật & hợp đồng mô hình (Model Lineage)", expanded=False):
                st.json(res)
        except Exception:
            st.error("Không thể hoàn tất yêu cầu dự đoán từ dịch vụ Gold V3. Vui lòng thử lại.")


def render_model_evaluation_and_drift() -> None:
    st.subheader("Đánh giá Độ chính xác 6 Cấu hình Mô hình & Kiểm chứng Thực tế Năm 2026")
    st.caption(
        "Mô hình học trên mẫu 336.864 chuyến rút từ 23,88 triệu chuyến Taxi năm 2025 (chia theo thời gian: Tháng 1–4 Huấn luyện, Tháng 5 Kiểm định, "
        "Tháng 6 Kiểm thử độc lập) và kiểm chứng khả năng dùng lâu dài trên 721.513 chuyến mới của Năm 2026."
    )

    k1, k2, k3, k4 = st.columns(4)
    k1.metric(
        "Mô hình tốt nhất đang dùng",
        "GBT (Gold V3)",
        "Vượt trội cả 5 mô hình",
    )
    k2.metric(
        "MAE 2025 (chuyến đồng hồ chuẩn)",
        "2.92 USD / chuyến",
        "Giảm 13% lỗi so với B0 (3.35 USD)",
    )
    k3.metric(
        "R² Holdout 2025 (đồng hồ chuẩn)",
        "86.9%",
        "Giải thích 86.9% biến thiên giá cước",
    )
    k4.metric(
        "Kiểm nghiệm thực tế Năm 2026",
        "MAE 3.08 USD",
        "721.513 chuyến (R² đạt 83.0%)",
    )
    st.caption(
        "Phạm vi: các số trên chỉ tính chuyến đồng hồ chuẩn (`ratecode_id = 1`, khoảng 69% chuyến hợp lệ). "
        "Với mọi loại chuyến (mẫu 197.930 chuyến tháng 06/2025): **MAE 4.06 USD, R² 75.3%**, do chuyến không có ratecode "
        "(25.5%) có MAE 5.58 USD và nhóm ratecode đặc biệt (2.3%) có MAE 22.92 USD."
    )

    metrics_rows = _query_model_metrics()
    if metrics_rows:
        st.divider()
        st.markdown("#### Sơ đồ 1: So sánh Sai số Trung bình (MAE — USD) giữa các Mô hình (Thấp hơn = Chính xác hơn)")
        st.caption(
            "Đánh giá trên tập Kiểm thử độc lập (Tháng 06/2025 — 80.770 chuyến chưa từng xuất hiện khi học). "
            "Cột màu xanh là mô hình đang phục vụ, **M3-nw** (**2.92 USD/chuyến** trên chuyến tính đồng hồ chuẩn)."
        )

        test_only = [r for r in metrics_rows if r["split"] == "test"]
        if test_only:
            chart1_data = []
            for r in test_only:
                m_key = str(r["model_key"])
                mae_val = float(r["mae_usd"])
                r2_pct = round(float(r["r2"]) * 100, 1)
                is_best = m_key == SERVED_MODEL_KEY
                chart1_data.append(
                    {
                        "Mô hình": MODEL_DISPLAY_LABELS.get(m_key, str(r["model_name"])),
                        "Sai số MAE (USD)": round(mae_val, 2),
                        "Nhãn hiển thị": f"{mae_val:.2f} USD (R²: {r2_pct}%)",
                        "Độ chính xác R² (%)": r2_pct,
                        "Sai số trung vị P50 (USD)": round(float(r["p50_usd"]), 2),
                        "Nhóm": "Mô hình tốt nhất (Đang dùng)" if is_best else "Mô hình đối sánh",
                    }
                )

            st.vega_lite_chart(
                chart1_data,
                {
                    "height": 300,
                    "layer": [
                        {
                            "mark": {"type": "bar", "cornerRadiusEnd": 6, "height": 28},
                            "encoding": {
                                "y": {
                                    "field": "Mô hình",
                                    "type": "nominal",
                                    "sort": {"field": "Sai số MAE (USD)", "order": "ascending"},
                                    "title": None,
                                    "axis": {"labelLimit": 360, "labelFontSize": 12},
                                },
                                "x": {
                                    "field": "Sai số MAE (USD)",
                                    "type": "quantitative",
                                    "title": "Sai số trung bình mỗi chuyến — MAE (USD, càng ngắn càng chính xác)",
                                    "scale": {"domain": [0, 9.5]},
                                },
                                "color": {
                                    "field": "Nhóm",
                                    "type": "nominal",
                                    "scale": {
                                        "domain": ["Mô hình tốt nhất (Đang dùng)", "Mô hình đối sánh"],
                                        "range": ["#10b981", "#64748b"],
                                    },
                                    "legend": {"orient": "top", "title": None},
                                },
                                "tooltip": [
                                    {"field": "Mô hình", "title": "Mô hình"},
                                    {"field": "Sai số MAE (USD)", "title": "Sai số TB MAE (USD)"},
                                    {"field": "Độ chính xác R² (%)", "title": "Độ chính xác R² (%)"},
                                    {"field": "Sai số trung vị P50 (USD)", "title": "Sai số trung vị P50 (USD)"},
                                ],
                            },
                        },
                        {
                            "mark": {
                                "type": "text",
                                "align": "left",
                                "baseline": "middle",
                                "dx": 6,
                                "fontWeight": "bold",
                                "fontSize": 12,
                                "color": "#e2e8f0",
                            },
                            "encoding": {
                                "y": {
                                    "field": "Mô hình",
                                    "type": "nominal",
                                    "sort": {"field": "Sai số MAE (USD)", "order": "ascending"},
                                },
                                "x": {"field": "Sai số MAE (USD)", "type": "quantitative"},
                                "text": {"field": "Nhãn hiển thị", "type": "nominal"},
                            },
                        },
                    ],
                },
                use_container_width=True,
            )

        pred_summary = _query_prediction_summary()
        st.divider()
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### Sơ đồ 2: Giá cước Thực tế vs Dự đoán theo 24 Giờ trong ngày")
            st.caption(
                "Đường dự đoán của AI (màu xanh lá) bám sát đường giá cước thực tế (màu cam) qua mọi khung giờ thấp điểm và cao điểm (0h – 23h)."
            )
            if pred_summary["by_hour"]:
                hour_melted = []
                for r in pred_summary["by_hour"]:
                    h_val = int(r["pickup_hour"])
                    hour_melted.append(
                        {
                            "Giờ đón": h_val,
                            "Loại giá cước": "1. Giá cước Thực tế (Actual)",
                            "Giá trung bình (USD)": float(r["avg_actual_fare"]),
                        }
                    )
                    hour_melted.append(
                        {
                            "Giờ đón": h_val,
                            "Loại giá cước": "2. Giá AI Dự đoán (Gold V3)",
                            "Giá trung bình (USD)": float(r["avg_pred_m3_no_weather"]),
                        }
                    )
                st.vega_lite_chart(
                    hour_melted,
                    {
                        "height": 320,
                        "mark": {"type": "line", "point": True, "strokeWidth": 3},
                        "encoding": {
                            "x": {
                                "field": "Giờ đón",
                                "type": "ordinal",
                                "title": "Giờ đón khách trong ngày (0h đến 23h)",
                                "axis": {"labelAngle": 0},
                            },
                            "y": {
                                "field": "Giá trung bình (USD)",
                                "type": "quantitative",
                                "title": "Giá cước trung bình (USD)",
                                "scale": {"zero": False},
                            },
                            "color": {
                                "field": "Loại giá cước",
                                "type": "nominal",
                                "scale": {
                                    "domain": ["1. Giá cước Thực tế (Actual)", "2. Giá AI Dự đoán (Gold V3)"],
                                    "range": ["#f59e0b", "#10b981"],
                                },
                                "legend": {"orient": "top", "title": None},
                            },
                            "tooltip": [
                                {"field": "Giờ đón", "title": "Giờ trong ngày (h)"},
                                {"field": "Loại giá cước", "title": "Dòng dữ liệu"},
                                {"field": "Giá trung bình (USD)", "title": "Giá cước TB (USD)"},
                            ],
                        },
                    },
                    use_container_width=True,
                )
            else:
                st.info("Chưa có dữ liệu tổng hợp theo giờ.")

        with col_right:
            st.markdown("#### Sơ đồ 3: Kiểm chứng Độ ổn định sang Năm 2026 (721.513 chuyến)")
            st.caption(
                "Đem mô hình học từ năm 2025 chạy thử trên dữ liệu tương lai **Tháng 06, 07, 08/2026**: "
                "Mô hình AI Gold V3 giữ sai số ổn định quanh mức **2.90 – 3.28 USD**, thấp hơn hẳn thống kê truyền thống."
            )
            drift_rows = _drift_chart_rows(metrics_rows)
            if drift_rows:
                drift_chart_data = []
                month_labels = {
                    "drift_2026-06": "Tháng 06/2026 (268.539 chuyến)",
                    "drift_2026-07": "Tháng 07/2026 (235.025 chuyến)",
                    "drift_2026-08": "Tháng 08/2026 (217.949 chuyến)",
                }
                for r in drift_rows:
                    is_gbt = r["model_key"] == SERVED_MODEL_KEY
                    drift_chart_data.append(
                        {
                            "Tháng Năm 2026": month_labels.get(str(r["split"]), str(r["split"])),
                            "Phương pháp": "Mô hình AI Gold V3 (GBT)" if is_gbt else "Thống kê truyền thống (Baseline B0)",
                            "Sai số MAE (USD)": round(float(r["mae_usd"]), 2),
                            "Độ chính xác R² (%)": round(float(r["r2"]) * 100, 1),
                        }
                    )
                st.vega_lite_chart(
                    drift_chart_data,
                    {
                        "height": 320,
                        "mark": {"type": "bar", "cornerRadiusEnd": 5},
                        "encoding": {
                            "x": {
                                "field": "Tháng Năm 2026",
                                "type": "nominal",
                                "title": "Dữ liệu thực tế Năm 2026 (Out-of-Time Drift)",
                                "axis": {"labelAngle": 0},
                            },
                            "y": {
                                "field": "Sai số MAE (USD)",
                                "type": "quantitative",
                                "title": "Sai số trung bình MAE (USD — Thấp hơn = Tốt hơn)",
                                "scale": {"domain": [0, 4.5]},
                            },
                            "xOffset": {"field": "Phương pháp"},
                            "color": {
                                "field": "Phương pháp",
                                "type": "nominal",
                                "scale": {
                                    "domain": ["Mô hình AI Gold V3 (GBT)", "Thống kê truyền thống (Baseline B0)"],
                                    "range": ["#10b981", "#94a3b8"],
                                },
                                "legend": {"orient": "top", "title": None},
                            },
                            "tooltip": [
                                {"field": "Tháng Năm 2026", "title": "Giai đoạn"},
                                {"field": "Phương pháp", "title": "Mô hình"},
                                {"field": "Sai số MAE (USD)", "title": "Sai số TB MAE (USD)"},
                                {"field": "Độ chính xác R² (%)", "title": "Độ chính xác R² (%)"},
                            ],
                        },
                    },
                    use_container_width=True,
                )

        st.info(
            "**Tóm tắt cách hệ thống ước tính giá:**\n"
            "- **Mô hình phục vụ:** Giá dùng `M3-nw` (**MAE = 2.92 USD/chuyến, R² = 86.85%** với chuyến đồng hồ chuẩn; **MAE = 4.06 USD, R² = 75.3%** với mọi loại chuyến). `M3-w` (**MAE = 2.99 USD/chuyến, R² = 86.52%**) được giữ để đối chứng và mô phỏng thời tiết thủ công. Thời tiết tự lấy theo giờ đón chỉ để tham khảo.\n"
            "- **Tích hợp Phụ phí Chính thức NYC TLC & Gợi ý Giờ đi:** Kết hợp cước đồng hồ ML với bảng kê các khoản phụ phí thực tế phát sinh "
            "(đối chiếu 60.000 chuyến 06/2025: khớp đến từng xu 94,1%, sai lệch trung bình 0,11 USD) và gợi ý giờ đi dựa trên phụ phí TLC và lịch sử cước của từng tuyến."
        )

        with st.expander("📋 Xem bảng số liệu chi tiết của tất cả các Mô hình & Hành trình Liên Quận", expanded=False):
            clean_table = []
            for r in metrics_rows:
                clean_table.append(
                    {
                        "Mô hình": MODEL_DISPLAY_LABELS.get(str(r["model_key"]), str(r["model_name"])),
                        "Giai đoạn đánh giá": SPLIT_DISPLAY_LABELS.get(str(r["split"]), str(r["split"])),
                        "Số chuyến": f"{int(r['row_count']):,}",
                        "Sai số TB MAE (USD)": round(float(r["mae_usd"]), 2),
                        "Sai số RMSE (USD)": round(float(r["rmse_usd"]), 2),
                        "Độ chính xác R² (%)": round(float(r["r2"]) * 100, 2),
                        "Sai số trung vị P50 (USD)": round(float(r["p50_usd"]), 2),
                    }
                )
            st.markdown("##### 1. Bảng kết quả thực nghiệm đầy đủ (`analytics.model_metrics`)")
            st.dataframe(clean_table)

            if pred_summary["by_borough"]:
                borough_table = [
                    {
                        "Hành trình Liên Quận (Điểm đón -> Điểm đến)": r["borough_pair"],
                        "Số chuyến kiểm thử": f"{int(r['trips']):,}",
                        "Giá thực tế TB (USD)": r["avg_actual_fare"],
                        "Giá AI dự đoán TB (USD)": r["avg_pred_m3_no_weather"],
                        "Sai số TB MAE (USD)": r["mae_m3_nw_usd"],
                    }
                    for r in pred_summary["by_borough"]
                ]
                st.markdown("##### 2. Top 12 Hành trình Liên Quận phổ biến nhất — Thực tế vs AI Dự đoán")
                st.dataframe(borough_table)
    else:
        st.warning("Chưa tìm thấy dữ liệu trong bảng `analytics.model_metrics`.")


def render_pipeline_monitoring(base_url: str) -> None:
    st.subheader("Giám sát Luồng Dữ liệu Lớn & Sức khỏe Hệ thống (5 Chỉ số Vận hành)")
    st.caption(
        "Theo dõi toàn diện từ khâu làm sạch dữ liệu thô (24,08 triệu chuyến), tích hợp Thời tiết & Lịch nghỉ lễ, "
        "nạp vào Kho dữ liệu PostgreSQL đến trạng thái hoạt động của máy chủ AI dự đoán cước."
    )
    try:
        snap = _cached_monitoring_snapshot(base_url)
        etl_metrics = snap["etl"]
        qual_metrics = snap["qual"]
        srv_metrics = snap["srv"]

        m1 = etl_metrics["metric_1_job_status"]
        m2 = etl_metrics["metric_2_processing_time"]
        m3 = etl_metrics["metric_3_row_reconciliation"]
        m4_silver = qual_metrics["metric_4_quality_and_rejects"]["silver_cleaning"]
        m4_gold = qual_metrics["metric_4_quality_and_rejects"].get("gold_enrichment", {})
        m5_srv = srv_metrics["metric_5_model_and_serving"]["serving_health"]
        m5_dash_res = srv_metrics["metric_5_model_and_serving"].get("dashboard_container_resources", {})

        st.markdown("#### Sơ đồ Luồng Xử lý & Đối soát Dữ liệu (Medallion Architecture)")
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            st.info(
                "**Bước 1: Dữ liệu Thô (Raw)**\n\n"
                f"📥 **{m4_silver['raw_taxi_rows']:,} chuyến**\n\n"
                "Dữ liệu gốc NYC Yellow Taxi 2025 + Thời tiết ERA5 + Lịch nghỉ lễ + 263 vùng."
            )
        with f2:
            st.warning(
                "**Bước 2: Tầng Sạch (Silver)**\n\n"
                f"🧹 **Loại bỏ {m4_silver['silver_rejected_rows']:,} dòng lỗi ({m4_silver['silver_reject_rate_pct']}%)**\n\n"
                "Lọc các chuyến có thời gian lỗi (dropoff <= pickup), thiếu vùng đón/trả hoặc trùng lặp."
            )
        with f3:
            st.success(
                "**Bước 3: Tầng Tích hợp (Gold)**\n\n"
                f"✨ **{m4_silver['silver_valid_rows']:,} chuyến hợp lệ**\n\n"
                f"Khớp **100%** vùng bản đồ và **{m4_gold.get('weather_match_rate_pct', 'N/A')}%** thời tiết ERA5 theo giờ."
            )
        with f4:
            st.success(
                "**Bước 4: Kho PostgreSQL & AI**\n\n"
                f"🗄️ **{m3['total_loaded_rows']:,} dòng (Chênh lệch = {m3['row_delta']})**\n\n"
                f"Nạp {m1['loaded_jobs']}/{m1['total_jobs']} phân vùng vào PostgreSQL; suy luận trực tuyến khoảng 0,3–0,5 giây/lượt (đo ngày 06/11/2026)."
            )

        st.divider()
        st.markdown("#### 5 Chỉ số Giám sát Cốt lõi (DataOps & MLOps Metrics)")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric(
            "1. Trạng thái ETL",
            f"{m1['loaded_jobs']}/{m1['total_jobs']} THÀNH CÔNG",
            f"Đạt {m1['success_rate_pct']}%",
        )
        c2.metric(
            "2. Thời gian nạp Kho",
            f"{m2['total_load_seconds']:.0f} giây",
            f"TB {m2['avg_job_seconds']:.0f}s / tháng",
        )
        c3.metric(
            "3. Đối soát số dòng",
            f"{m3['total_loaded_rows']:,}",
            f"Chênh lệch = {m3['row_delta']}" + (" (Zero Loss)" if m3["row_delta"] == 0 else ""),
        )
        c4.metric(
            "4. Tỷ lệ lọc dữ liệu bẩn",
            f"{m4_silver['silver_reject_rate_pct']}%",
            f"Đã lọc {m4_silver['silver_rejected_rows']:,} dòng lỗi",
        )
        inf_mem = m5_srv.get("container_memory_mb")
        inf_lim = m5_srv.get("container_memory_limit_mb")
        mem_label = f"RAM {inf_mem:.0f}/{inf_lim:.0f} MB" if inf_mem and inf_lim else f"Hỗ trợ {m5_srv.get('supported_pickup_zones', 258)}/263 vùng"
        c5.metric(
            "5. Máy chủ AI & RAM",
            f"SẴN SÀNG ({m5_srv.get('http_status', 200)})" if m5_srv.get("ready") else "NGỪNG HOẠT ĐỘNG",
            mem_label,
        )

        jobs = etl_metrics.get("jobs", [])
        if jobs:
            st.divider()
            col_j1, col_j2 = st.columns(2)
            with col_j1:
                st.markdown("#### Sơ đồ Khối lượng Chuyến xe Đã nạp theo Tháng (2025)")
                job_rows = [int(j.get("loaded_row_count", 0)) for j in jobs]
                st.caption(
                    f"Mỗi tháng nạp và đối soát từ {min(job_rows) / 1_000_000:.2f} đến {max(job_rows) / 1_000_000:.2f} triệu chuyến xe."
                )
                jobs_chart = [
                    {
                        "Tháng": str(j.get("dataset", "")).replace("fact_trip (", "Tháng ").replace("taxi (", "Tháng ").replace(")", ""),
                        "Số triệu chuyến": round(int(j.get("loaded_row_count", 0)) / 1_000_000, 2),
                        "Số chuyến chính xác": f"{int(j.get('loaded_row_count', 0)):,} chuyến",
                        "Thời gian nạp (giây)": round(float(j.get("duration_seconds", 0.0)), 1),
                    }
                    for j in jobs
                ]
                st.vega_lite_chart(
                    jobs_chart,
                    {
                        "height": 280,
                        "layer": [
                            {
                                "mark": {"type": "bar", "cornerRadiusEnd": 5, "color": "#3b82f6"},
                                "encoding": {
                                    "x": {"field": "Tháng", "type": "ordinal", "title": "Tháng dữ liệu (Năm 2025)", "axis": {"labelAngle": 0}},
                                    "y": {"field": "Số triệu chuyến", "type": "quantitative", "title": "Số lượng chuyến (Triệu chuyến)", "scale": {"domain": [0, 5.5]}},
                                    "tooltip": [
                                        {"field": "Tháng", "title": "Tháng"},
                                        {"field": "Số chuyến chính xác", "title": "Số dòng đã nạp"},
                                        {"field": "Thời gian nạp (giây)", "title": "Thời gian nạp (giây)"},
                                    ],
                                },
                            },
                            {
                                "mark": {"type": "text", "dy": -10, "fontWeight": "bold", "color": "#e2e8f0"},
                                "encoding": {
                                    "x": {"field": "Tháng", "type": "ordinal"},
                                    "y": {"field": "Số triệu chuyến", "type": "quantitative"},
                                    "text": {"field": "Số triệu chuyến", "type": "quantitative"},
                                },
                            },
                        ],
                    },
                    use_container_width=True,
                )

            with col_j2:
                st.markdown("#### Sơ đồ Thời gian Nạp Dữ liệu vào PostgreSQL theo Tháng (Giây)")
                st.caption(f"Thời gian nạp trung bình {m2['avg_job_seconds']:.0f} giây/tháng, có đối soát số dòng tự động.")
                st.vega_lite_chart(
                    jobs_chart,
                    {
                        "height": 280,
                        "layer": [
                            {
                                "mark": {"type": "bar", "cornerRadiusEnd": 5, "color": "#10b981"},
                                "encoding": {
                                    "x": {"field": "Tháng", "type": "ordinal", "title": "Tháng dữ liệu (Năm 2025)", "axis": {"labelAngle": 0}},
                                    "y": {"field": "Thời gian nạp (giây)", "type": "quantitative", "title": "Thời gian xử lý & nạp (Giây)", "scale": {"domain": [0, 1200]}},
                                    "tooltip": [
                                        {"field": "Tháng", "title": "Tháng"},
                                        {"field": "Thời gian nạp (giây)", "title": "Thời gian nạp (giây)"},
                                        {"field": "Số chuyến chính xác", "title": "Số dòng đã nạp"},
                                    ],
                                },
                            },
                            {
                                "mark": {"type": "text", "dy": -10, "fontWeight": "bold", "color": "#e2e8f0"},
                                "encoding": {
                                    "x": {"field": "Tháng", "type": "ordinal"},
                                    "y": {"field": "Thời gian nạp (giây)", "type": "quantitative"},
                                    "text": {"field": "Thời gian nạp (giây)", "type": "quantitative"},
                                },
                            },
                        ],
                    },
                    use_container_width=True,
                )

        dash_mem = m5_dash_res.get("memory_current_mb")
        dash_lim = m5_dash_res.get("memory_limit_mb")
        ml_cohort = qual_metrics["metric_4_quality_and_rejects"].get("ml_gold_v3_cohort", {})
        prep_s = _seconds_label(ml_cohort.get("preparation_elapsed_seconds"))
        train_s = _seconds_label(ml_cohort.get("training_elapsed_seconds"))
        startup_s = _seconds_label(m5_srv.get("startup_seconds"))
        st.info(
            f"**Hiệu năng Huấn luyện & Phục vụ AI:**\n"
            f"- **Huấn luyện Spark ML:** Trích xuất mẫu phân tầng từ 23,88 triệu dòng mất **{prep_s} giây**; huấn luyện 4 mô hình Spark ML mất **{train_s} giây** (số ghi nhận lúc huấn luyện).\n"
            f"- **Tài nguyên Container hiện tại:** Máy chủ suy luận `fare-inference` dùng **{inf_mem or 'N/A'} / {inf_lim or 'N/A'} MB RAM** "
            f"(mô hình chỉ nạp để dự đoán, luồng phục vụ không có bước huấn luyện; khởi động **{startup_s}s**); "
            f"Giao diện `dashboard` dùng **{dash_mem or 'N/A'} / {dash_lim or 'N/A'} MB RAM**."
        )

        with st.expander("📋 Xem nhật ký đối soát ETL chi tiết & Thông số kỹ thuật JSON", expanded=False):
            if jobs:
                clean_jobs = [
                    {
                        "Tháng phân vùng": str(j.get("dataset", "")).replace("fact_trip (", "Tháng ").replace("taxi (", "Tháng ").replace(")", ""),
                        "Trạng thái": "Thành công (LOADED)" if str(j.get("status", "")).lower() == "loaded" else j.get("status", ""),
                        "Số dòng Parquet (Gold)": f"{int(j.get('source_row_count', 0)):,}",
                        "Số dòng nạp PostgreSQL": f"{int(j.get('loaded_row_count', 0)):,}",
                        "Chênh lệch": int(j.get("row_delta", 0)),
                        "Thời gian nạp (giây)": round(float(j.get("duration_seconds", 0.0)), 1),
                    }
                    for j in jobs
                ]
                st.dataframe(clean_jobs)
            col_q1, col_q2 = st.columns(2)
            with col_q1:
                st.markdown("##### Thống kê Làm sạch Silver & Tuyển chọn Cohort ML Gold V3")
                st.json(qual_metrics["metric_4_quality_and_rejects"])
            with col_q2:
                st.markdown("##### Trạng thái Microservice Suy luận (`fare-inference:8000/ready`)")
                st.json(m5_srv)
    except Exception:
        st.error("Không thể tải đầy đủ bảng giám sát vận hành. Vui lòng kiểm tra kết nối PostgreSQL.")


def render_gold_v3_page() -> None:
    base_url = os.environ.get("FARE_INFERENCE_URL", DEFAULT_INFERENCE_URL)
    tab_calc, tab_eval, tab_mon = st.tabs(
        [
            "🚕 Ước tính cước chuyến đi",
            "📊 Độ chính xác & So sánh Mô hình",
            "🩺 Giám sát Hệ thống & Dữ liệu",
        ]
    )
    with tab_calc:
        if address_flow_enabled():
            render_address_page()
        else:
            render_zone_weather_simulator(base_url)
    with tab_eval:
        render_model_evaluation_and_drift()
    with tab_mon:
        render_pipeline_monitoring(base_url)
