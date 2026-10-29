# Việc hoãn lại — Cập nhật hệ thống đang chạy & Model V2.3

Ngày ghi: 2026-10-29. Các commit liên quan: `53d6521`, `16eb345` và commit dọn code chết tiếp theo.
Chủ dự án đã quyết định **tự làm sau** 2 hạng mục dưới đây. Agent không được tự làm khi chưa được yêu cầu.

---

## Phần 2 — Đưa các sửa đổi lên hệ thống đang chạy (Docker + PostgreSQL) — ✅ ĐÃ HOÀN TẤT (2026-10-29)

**Kết quả triển khai (2026-10-29):**

| Thành phần | Trạng thái sau cập nhật | Kết quả kiểm chứng |
|---|---|---|
| `nyc-taxi-bigdata-dashboard-1` (`127.0.0.1:8501`) | Đã build lại image `nyc-taxi-dashboard:pretrip-v22-24fab1e` (bản cũ lưu tại `-prev`) và recreate container (`3f646fa7441b`, `healthy`) | Đã có giờ hẹn mặc định +30 phút, thông báo lỗi giờ tiếng Việt chi tiết, ghi chú khi bật `PRETRIP_ALLOW_UNSEEN_ZONES` |
| `nyc-taxi-bigdata-fare-inference-1` | Đã build lại image `nyc-taxi-inference:pretrip-v22-bc808337` (bản cũ lưu tại `-prev`) và recreate container (`66d15376b1df`, `healthy`) | Đã làm nóng Spark khi khởi động: độ trễ dự đoán đầu tiên giảm từ ~4.300 ms xuống **1.294 ms** (`fare_amount_usd = 11.57 USD`), `scripts/start.ps1 -WithInference -ModelVersion v2.2` đạt `exit 0` (0 drift) |
| `nyc-taxi-postgres` | Đã tạo index `analytics.fact_trip_dashboard_bucket_idx` (`1131 MB`) và chạy `ANALYZE analytics.fact_trip` | Hỗ trợ Index-Only Scan cho các truy vấn lọc `source_year, source_month, pickup_hour, pickup_location_id` |

---

## Phần 4 — Model Pre-trip V2.3 (huấn luyện trên dữ liệu lớn, phủ khoảng 260 zone)

### ✅ Bước 4.1 — Thiết kế Contract & Feature V2.3 (ĐÃ HOÀN TẤT — 2026-10-29)
- **Contract V2.3 (`config/pretrip_v23_contract.json`)**:
  - `contract_version`: `"pretrip-v23-zone-centroid-time-1"`
  - `max_bins`: `264` (đủ cho toàn bộ 263 zone TLC `1..263` cộng thêm 1 bucket `__unknown` của `StringIndexer(handleInvalid="keep")`).
  - `cohort_policy`: `sampling_fraction = 0.25` (lấy mẫu 25% dữ liệu lớn, gấp 25 lần V2.2 để bao phủ ~99% zone có chuyến xe).
  - `model_status`: hỗ trợ tiến hóa qua các trạng thái `("not_trained_or_deployed", "trained_candidate", "evaluated_holdout", "deployed_frozen")`.
  - Ghim SHA256 của cả `config/pretrip_v2_calendar.json` và `assets/taxi_zones/taxi_zones.geojson` (`6513c39044b8dee18c00af0dd6f97919d681b1e85efbd2d6980786c4cfbb8c9c`).
- **Module Feature V2.3 (`src/nyc_taxi/ml/pretrip_v23_features.py`)**:
  - 10 features (8 numeric + 2 categorical):
    - `pickup_hour_sin`, `pickup_hour_cos`: tính theo giờ liên tục độ phân giải phút `(hour + minute / 60.0) / 24.0`.
    - `pickup_dow_sin`, `pickup_dow_cos`, `is_weekend`, `is_holiday`.
    - `zone_centroid_distance_km`: khoảng cách Haversine (`R = 6371.0088 km`) giữa trọng tâm diện tích (area-weighted planar polygon centroid) của `pickup_location_id` và `dropoff_location_id` trích xuất trực tiếp từ `taxi_zones.geojson` (có `@lru_cache`).
    - `is_same_zone`: `1.0` nếu đón và trả trong cùng 1 zone (`zone_centroid_distance_km == 0.0`), ngược lại `0.0`.
    - `pickup_zone`, `dropoff_zone`: chuỗi `"1".."263"`.
- **Kiểm thử (`tests/test_pretrip_v23_features.py`)**:
  - Đã thêm 19 bài test kiểm chứng tính bất biến của V2.2, độ phủ 263 tâm vùng, tính đối xứng Haversine, độ phân giải theo phút, và parity tuyệt đối giữa `build_training_features` và `build_serving_features`.

### ⏳ Các bước tiếp theo của Phần 4 (Bước 4.2 – 4.4)
- **Bước 4.2 — Chuẩn bị & Lấy mẫu Dữ liệu lớn (Data Prep)**: Quét Parquet theo `cohort_policy` (25% sampling) và materialize vào `.sandbox/pretrip-v2/data-v23-01/`.
- **Bước 4.3 — Huấn luyện Native GBT (`maxBins=264`, `squared` vs `absolute`) & Đánh giá Holdout**: Lưu tại `.sandbox/pretrip-v2/training-YYYYMMDD-v23-01/`.
- **Bước 4.4 — Đóng gói Serving Bundle V2.3 & Tích hợp UI (`FARE_MODEL_VERSION=v2.3`)**.

### Ràng buộc bắt buộc
- Không sửa: `src/nyc_taxi/ml/pretrip_features.py`, `config/pretrip_v2_contract.json`,
  `config/pretrip_v2_calendar.json`, `assets/taxi_zones/taxi_zones.geojson`, `requirements.txt`,
  `.sandbox/pretrip-v2/training-20261027-native-01/`, `.sandbox/pretrip-v2/serving-bundle-v22-01/`.

---

## Đã quyết định giữ lại (không phải code chết)
- `src/nyc_taxi/dashboard/fare_page.py` (form nhập khoảng cách V1): đây là **luồng mặc định**
  khi `FARE_MODEL_VERSION=v1` (mặc định ở `prediction.py:30`) và `FARE_ADDRESS_ENABLED=0`
  (mặc định ở `docker-compose.inference.yml:67`).
- `.sandbox/pretrip-v2/training-20261027-pilot-01/` và `training-20261027-absolute-01/`: là
  `--reference-artifacts` / `--comparison-artifacts` của lượt huấn luyện native V2.2 (xem `plans/PRETRIP_V22_NATIVE_REPORT.md`).
- `.sandbox/pretrip-v2/holdout-20261027-native-01/`: là `--output-dir` mặc định của `pretrip_holdout.py`.
  Checkpoint trong thư mục này góp phần chặn việc vô tình chạy holdout lại.
- `.sandbox/pv22setup-26edfea-01/` (7,2 MB): bằng chứng của `plans/PACKAGE_V22_EXECUTION_REPORT.md`.
- `scikit-learn` trong `requirements.txt`: không còn code nào import (`pandas` vẫn được `dashboard/app.py` dùng).
  Tuy nhiên `requirements.txt` bị ghim hash (bootstrap, image identity, `holdout.CORE_FILES`), nên chỉ gỡ khi
  đổi phiên bản dependency có chủ đích.
