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

**Vấn đề gốc:** V2.2 chỉ huấn luyện trên mẫu khoảng 1% cohort, nên chỉ phục vụ 117 zone pickup (khoảng 44,5%).
Các zone khác bị từ chối (HTTP 422), hoặc phải đi qua bucket `__unknown` nếu bật cờ tạm.

### Phạm vi đề xuất
- Native categorical GBT (`maxBins` ≥ số category), huấn luyện trên **20%–100% cohort** thay vì 1%.
- Feature mới (cần **contract V2.3 mới**; **không** sửa các file V2.2 đã ghim SHA256):
  - `zone_centroid_distance_km`, tính từ `assets/taxi_zones/taxi_zones.geojson`.
  - Giờ liên tục `hour + minute/60`.
- So sánh `lossType="absolute"` với `squared` trên validation. Holdout chỉ chạy **một lần** sau khi đã chốt model.

### Ràng buộc bắt buộc
- Không sửa: `src/nyc_taxi/ml/pretrip_features.py`, `config/pretrip_v2_contract.json`,
  `config/pretrip_v2_calendar.json`, `assets/taxi_zones/taxi_zones.geojson`, `requirements.txt`,
  `.sandbox/pretrip-v2/training-20261027-native-01/`, `.sandbox/pretrip-v2/serving-bundle-v22-01/`.
- Output mới đặt ở thư mục attempt mới trong `.sandbox/pretrip-v2/` (ví dụ `training-YYYYMMDD-v23-01/`).
- Cần ước lượng RAM/disk/thời gian trước khi chạy (máy local: Spark `local[2]`, container 3 GiB).
- Nên lập kế hoạch chi tiết riêng trước khi thực thi.

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
