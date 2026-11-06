# PRETRIP V2.3 — Báo cáo Chuẩn bị & Lấy mẫu Dữ liệu lớn (Bước 4.2)

Ngày thực thi: 29/10/2026 (Asia/Saigon).
Trạng thái: **`PREPARED_NOT_TRAINED`** (Đã chuẩn bị và kiểm chứng độc lập toàn bộ 721.513 dòng dữ liệu lớn; chưa huấn luyện model hoặc đánh giá holdout).

---

## 1. Mục tiêu & Cải tiến so với V2.2

1. **Tăng quy mô mẫu dữ liệu lớn gấp 10 lần (`71.932` → `721.513` dòng):**
   - Quét toàn bộ **10.704.073 dòng** từ 3 file Parquet TLC Yellow Taxi tháng 06, 07, 08/2026 (`.sandbox/pretrip-v2/sources/yellow_tripdata_2026-{06,07,08}.parquet`).
   - Sử dụng hàm băm 64-bit tất định **SplitMix64** (`seed = 42`, tỷ lệ `10%` = `100.000 / 1.000.000`) kết hợp **bảo toàn vùng hiếm trên tập Train (`min_train_zone_support = 30`)**.
2. **Nâng độ phủ vùng đón (Pickup Zone) trên tập Train từ `117` lên `247` vùng (+111,1%) và vùng trả (Dropoff Zone) từ `224` lên `259` vùng:**
   - Đạt **100% toàn bộ các vùng có phát sinh chuyến xe hợp lệ trong tháng 06/2026** (`247/247` vùng đón và `259/259` vùng trả; 4 vùng trả không xuất hiện là các đảo không có cầu đường bộ `103/104/105` Liberty/Ellis/Governor's Island và `199` Rikers Island).
   - Việc bổ sung `4.054` dòng vùng hiếm chỉ thực hiện **trên tập Train**, giữ nguyên phân phối lấy mẫu độc lập 10% trên tập `validation` (tháng 07) và `test` (tháng 08).
3. **Tăng tốc độ xử lý gấp ~38 lần (`721,1 giây` → `19,0 giây`):**
   - Vector hóa toàn bộ 6 tầng lọc waterfall và tính toán 10 đặc trưng V2.3 bằng PyArrow + NumPy, kèm kiểm tra đối chiếu trực tiếp với hàm chuẩn `build_training_features()` (`max_abs_diff <= 1e-12`).

---

## 2. Thống kê Waterfall & Phân bổ Dữ liệu theo Split

### 2.1. Waterfall lọc Cohort trên 10.704.073 dòng đầu vào

| Tầng lọc Cohort (`COHORT_STAGES`) | Số dòng giữ lại sau tầng | Số dòng bị loại (nguyên nhân đầu tiên) |
| :--- | ---: | ---: |
| **Tổng dòng đọc từ 3 file Parquet** | **10.704.073** | — |
| 1. `valid_local_pickup` (timestamp hợp lệ) | 10.704.073 | 0 |
| 2. `pickup_in_pilot_window` (`[2026-06-01, 2026-09-01)`) | 10.704.054 | 19 |
| 3. `finite_positive_fare` (`fare_amount > 0` hữu hạn) | 10.653.081 | 50.973 |
| 4. `standard_final_ratecode_1` (`RatecodeID == 1`) | 7.203.743 | 3.449.338 |
| 5. `named_zones_1_263` (`1 <= PU, DO <= 263`) | 7.178.867 | 24.876 |
| 6. `valid_completed_journey_identity` (`VendorID > 0`, `dropoff >= pickup`) | **7.178.867** | 0 |

*(Khớp tuyệt đối từng dòng với báo cáo `PRETRIP_V2_PREPARATION_REPORT.md`).*

### 2.2. Kết quả lấy mẫu và Độ phủ Zone theo Split

| Split (theo giờ đón thực tế NYC) | Dòng hợp lệ (Eligible) | Lấy mẫu đều 10% | Bổ sung vùng hiếm (`min=30`) | Loại cặp trùng giây khác giá | Dòng cuối cùng (`selected_rows`) | Unique Pickup Zones (V2.2 → **V2.3**) | Unique Dropoff Zones (V2.2 → **V2.3**) |
| :--- | ---: | ---: | ---: | ---: | ---: | :---: | :---: |
| **June (`train`)** | 2.650.423 | 264.485 | +4.054 | 0 | **268.539** / 350.000 | 117 → **247** | 224 → **259** |
| **July (`validation`)** | 2.343.375 | 235.027 | 0 | -2 | **235.025** / 300.000 | — → **203** | — → **253** |
| **August (`test`)** | 2.185.069 | 217.949 | 0 | 0 | **217.949** / 300.000 | — → **193** | — → **254** |
| **Tổng cộng** | **7.178.867** | **717.461** | **+4.054** | **-2** | **721.513** | **247 / 247 active** | **259 / 259 active** |

- **Xử lý bản ghi trùng giây khác giá (`ambiguous_coincident_rows_excluded`):** Trong 235.027 dòng validation tháng 07, phát hiện đúng 1 cặp chuyến xe có cùng 5 trường `(VendorID=2, pickup=2026-07-22T16:56:23, dropoff=2026-07-22T17:01:22, PU=237, DO=236)` nhưng 2 mức cước khác nhau (`6,50 USD` và `7,20 USD`). Theo chính sách `conflict_policy="exclude_ambiguous"`, cả 2 dòng của cặp mơ hồ này được loại bỏ minh bạch (không chọn bừa 1 giá).
- **Kiểm tra giao thoa (Overlap Guard):** `train ∩ validation = 0`, `train ∩ test = 0`, `validation ∩ test = 0`.

---

## 3. Kiểm chứng Độc lập & Định danh Artifact

Thư mục đầu ra: `.sandbox/pretrip-v2/pilot-2026-06-08-v23-01/` (tổng dung lượng **39.586.177 bytes ≈ 37,75 MiB**, thời gian chạy **18,997 giây**).

| File đầu ra | Số dòng | Dung lượng (Bytes) | SHA256 (`features` / `labels`) |
| :--- | ---: | ---: | :--- |
| `train_features.parquet` | 268.539 | 2.110.180 | `ec940170ba0a2c19...` |
| `train_labels.parquet` | 268.539 | 12.583.371 | `09dc0bbec7c355d6...` |
| `validation_features.parquet` | 235.025 | 1.810.601 | `e4e56c3534c82974...` |
| `validation_labels.parquet` | 235.025 | 11.065.748 | `4b63c23733ad78ef...` |
| `test_features.parquet` | 217.949 | 1.668.899 | `cec9813ede0cb067...` |
| `test_labels.parquet` | 217.949 | 10.295.906 | `016052e4ff6296b9...` |
| `preparation_report.json` | — | 25.736 | Khớp `preparation_checkpoint.json` |
