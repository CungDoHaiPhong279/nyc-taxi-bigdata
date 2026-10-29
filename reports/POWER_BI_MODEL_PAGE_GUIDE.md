# Hướng dẫn bổ sung Trang Đánh giá Mô hình & Dự đoán Giá cước (Gold V3) trên Power BI Desktop

Tài liệu này hướng dẫn từng bước để thêm trang **"Model Evaluation & Fare Prediction (Gold 2025)"** vào file báo cáo `reports/NYC_Taxi_Dashboard.pbix` mà không làm thay đổi hay ảnh hưởng tới các trang phân tích hiện có (`analytics.fact_trip`, `dim_location`, `dim_weather`, `dim_calendar`).

---

## 1. Các bảng dữ liệu mới trong PostgreSQL (`analytics` schema)

Giai đoạn **G3** đã bổ sung 2 bảng và 1 view vào cơ sở dữ liệu PostgreSQL (`localhost:5432`, database `nyc_taxi`, schema `analytics`):

1. **`analytics.model_metrics`**:
   - Lưu kết quả đánh giá định lượng (`MAE`, `RMSE`, `R2`, `P50`, `P90`, `P95`) của cả **5 cấu hình mô hình** (`B0_global_median`, `B0_hierarchical_median`, `M1_linear_regression`, `M2_random_forest`, `M3_gbt_weather`, `M3_nw_gbt_no_weather`) trên các tập `train` (01–04/2025), `validation` (05/2025), `test` (06/2025), cùng kết quả kiểm tra độ trôi thời gian năm 2026 (`drift_2026-06`, `drift_2026-07`, `drift_2026-08`).
2. **`analytics.fact_fare_prediction`**:
   - Lưu **170.395 dòng** kết quả dự đoán thực tế (`89.625` dòng `validation` tháng 05/2025 + `80.770` dòng `test` tháng 06/2025), bao gồm giá thực tế (`actual_fare_usd`), giá dự đoán của từng mô hình (`pred_b0_hierarchical_usd`, `pred_m1_lr_usd`, `pred_m2_rf_usd`, `pred_m3_gbt_weather_usd`, `pred_m3_nw_gbt_usd`), sai số tuyệt đối (`abs_error_m3_weather_usd`, `abs_error_m3_nw_usd`), khoảng cách tâm vùng Haversine (`zone_centroid_distance_km`) và các biến thời tiết ERA5 (`temperature_2m`, `precipitation`, `snowfall`, `wind_speed_10m`).
3. **`analytics.vw_fare_prediction_enriched`**:
   - View đã kết nối sẵn `fact_fare_prediction` với `dim_location` (tên Quận/Borough và Zone đón/trả) cùng phân nhóm điều kiện thời tiết (`Rain / Precip` vs `Clear / Dry`).

---

## 2. Cách nạp bảng mới vào Power BI Desktop (`NYC_Taxi_Dashboard.pbix`)

1. Mở file `reports/NYC_Taxi_Dashboard.pbix` bằng **Power BI Desktop**.
2. Trên thanh công cụ **Home**, chọn **Get Data** -> **PostgreSQL database**.
   - **Server**: `localhost:5432`
   - **Database**: `nyc_taxi`
   - **Data Connectivity mode**: chọn **Import** (hoặc **DirectQuery** tùy cấu hình hiện tại của bạn).
3. Trong cửa sổ **Navigator**, tích chọn:
   - `analytics.model_metrics`
   - `analytics.vw_fare_prediction_enriched`
4. Nhấn **Load**.
5. (Tùy chọn) Trong tab **Model View**:
   - Nối `vw_fare_prediction_enriched[pickup_location_id]` -> `dim_location[location_id]` (Many-to-One, Single).
   - Nối `vw_fare_prediction_enriched[pickup_date]` -> `dim_calendar[date_key]` (Many-to-One, Single).

---

## 3. Các công thức DAX (Measures) tạo sẵn (Copy & Paste)

Chuột phải vào bảng `vw_fare_prediction_enriched` -> **New measure** và dán lần lượt các công thức DAX sau:

```dax
// 1. Giá cước thực tế trung bình (USD)
Actual Fare Avg (USD) = AVERAGE('analytics vw_fare_prediction_enriched'[actual_fare_usd])

// 2. Giá cước dự đoán trung bình - Mô hình M3 GBT + Weather (USD)
Predicted Fare M3 Weather Avg (USD) = AVERAGE('analytics vw_fare_prediction_enriched'[pred_m3_gbt_weather_usd])

// 3. Sai số tuyệt đối trung bình MAE - M3 GBT + Weather (USD)
MAE M3 Weather (USD) = AVERAGE('analytics vw_fare_prediction_enriched'[abs_error_m3_weather_usd])

// 4. Sai số tuyệt đối trung bình MAE - M3-nw Không thời tiết (USD)
MAE M3 No Weather (USD) = AVERAGE('analytics vw_fare_prediction_enriched'[abs_error_m3_nw_usd])

// 5. Sai số tuyệt đối trung bình MAE - Baseline B0 (USD)
MAE Baseline B0 (USD) = 
AVERAGEX(
    'analytics vw_fare_prediction_enriched',
    ABS('analytics vw_fare_prediction_enriched'[actual_fare_usd] - 'analytics vw_fare_prediction_enriched'[pred_b0_hierarchical_usd])
)

// 6. Phần trăm cải thiện MAE của M3 so với Baseline B0 (%)
MAE Improvement vs Baseline (%) = 
DIVIDE(
    [MAE Baseline B0 (USD)] - [MAE M3 Weather (USD)],
    [MAE Baseline B0 (USD)],
    0
)
```

---

## 4. Bố cục đề xuất cho Trang mới: "Đánh giá Mô hình & Dự báo Giá cước"

Tạo một Page mới tên **`ML Evaluation (Gold V3)`** với 4 cụm biểu đồ trực quan:

1. **Hàng KPI Cards trên cùng (4 thẻ Card)**:
   - `MAE M3 Weather (USD)` (Hiển thị sai số trung bình USD trên tập Test tháng 06/2025).
   - `MAE Improvement vs Baseline (%)` (Mức giảm sai số so với Baseline B0).
   - `Actual Fare Avg (USD)` vs `Predicted Fare M3 Weather Avg (USD)`.
   - Tổng số chuyến kiểm thử (`COUNTROWS('analytics vw_fare_prediction_enriched')`).
2. **Biểu đồ 1 — So sánh 5 Mô hình (`Clustered Column Chart` từ bảng `analytics.model_metrics`)**:
   - **Slicer**: `split = "test"` (hoặc `"validation"`).
   - **X-axis**: `model_name` (`B0`, `M1 LinearRegression`, `M2 RandomForest`, `M3 GBT + Weather`, `M3-nw GBT No Weather`).
   - **Y-axis**: `mae_usd`, `rmse_usd`, `p90_abs_error_usd`.
3. **Biểu đồ 2 — Giá Thực tế vs Giá Dự đoán theo Khoảng cách & Giờ cao điểm (`Line and Clustered Column Chart`)**:
   - **X-axis**: `pickup_hour` (0..23).
   - **Line y-axis**: `Actual Fare Avg (USD)` và `Predicted Fare M3 Weather Avg (USD)`.
   - **Column y-axis**: `MAE M3 Weather (USD)`.
4. **Biểu đồ 3 — Tác động của Thời tiết ERA5 (`Matrix` hoặc `Bar Chart`)**:
   - **Rows / Axis**: `weather_condition` (`Clear / Dry` vs `Rain / Precip`) và `pickup_borough`.
   - **Values**: `Actual Fare Avg (USD)`, `Predicted Fare M3 Weather Avg (USD)`, `MAE M3 Weather (USD)`, `MAE M3 No Weather (USD)`.
5. **Biểu đồ 4 — Kiểm tra Độ trôi Dữ liệu 2025 -> 2026 (`Line Chart` từ `analytics.model_metrics`)**:
   - **Filter**: `model_key = "M3_nw_gbt_no_weather"` và `split IN ("validation", "test", "drift_2026-06", "drift_2026-07", "drift_2026-08")`.
   - **X-axis**: `split`.
   - **Y-axis**: `mae_usd` và `r2`.
