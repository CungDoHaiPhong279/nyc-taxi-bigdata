# Báo cáo Huấn luyện & Đánh giá Mô hình Dự báo Giá cước NYC Taxi — Gold V3 (2025 + ERA5 Weather + 2026 Drift)

- **Phiên bản hợp đồng (Contract Version)**: `gold-ml-v3-2025-weather-centroid-1`
- **Nguồn dữ liệu huấn luyện**: Tầng **Gold Medallion (`data/gold/taxi_enriched/year=2025/month=01..06`)** — `23.886.265` chuyến đã tích hợp **NYC Yellow Taxi + Khí tượng ERA5 (`temperature_2m`, `precipitation`, `snowfall`, `wind_speed_10m`) + Lịch nghỉ lễ (`dim_calendar`) + Vùng địa lý (`dim_location` / `taxi_zones.geojson`)**.
- **Experiment trên MLflow**: `nyc-taxi-fare-gold-v3` (`http://localhost:5000`)

---

## 1. Quy mô Dữ liệu, Phân tách Thời gian & Độ phủ Vùng (Zone Coverage)

| Tập dữ liệu (Split) | Khoảng thời gian (`pickup_datetime_local`) | Tổng chuyến hợp lệ (`ratecode_id = 1`) | Số mẫu chọn (`3% SplitMix64 + Rare-Zone Support`) | Độ phủ Pickup Zone (`Active / Total`) | Độ phủ Dropoff Zone (`Active / Total`) |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **Train** | `2025-01-01` → `< 2025-05-01` (4 tháng) | `11.053.616` | **`336.864`** (gồm `4.469` dòng bù zone hiếm) | **`258 / 258` (100%)** | **`259 / 259` (100%)** |
| **Validation** | `2025-05-01` → `< 2025-06-01` (Tháng 5) | `2.992.409` | **`89.625`** | `167 / 244` | `244 / 259` |
| **Test** | `2025-06-01` → `< 2025-07-01` (Tháng 6) | `2.678.064` | **`80.770`** | `167 / 246` | `250 / 259` |
| **Tổng cộng (2025)** | **01/2025 – 06/2025** | **`16.724.089`** | **`507.259`** | **100% Zone hoạt động** | **100% Zone hoạt động** |

> **Xử lý quy tắc Giá cố định Sân bay JFK (`ratecode_id = 2`)**:
> Kiểm chứng trực tiếp trên `23.886.265` dòng Gold 2025 ghi nhận **`574.602` chuyến** giữa **JFK Airport (`LocationID = 132`) ↔ Manhattan** thuộc `ratecode_id = 2`, trong đó **`574.524` chuyến (`99,9864%`)** có mức giá cơ bản cố định đúng bằng **\$70,00 USD** (`mean_fare_usd = \$70,57`). Hệ thống tích hợp quy tắc tất định `JFK (132) ↔ Manhattan -> \$70,00 USD` song song với ước tính đồng hồ tính tiền của mô hình học máy.

---

## 2. Kết quả So sánh 6 Cấu hình Mô hình trên Tập Validation (05/2025) và Test (06/2025)

> **Phạm vi**: Các bảng trong mục 2 và 3 chỉ tính chuyến đồng hồ chuẩn (`ratecode_id = 1`). Kết quả trên mọi loại chuyến nằm ở mục 5.

### 2.1. Tập Validation (`2025-05`, `89.625` chuyến)

| Mô hình | Sử dụng Thời tiết ERA5 | MAE (USD) ↓ | RMSE (USD) ↓ | $R^2$ ↑ | P50 Error (USD) | P90 Error (USD) | P95 Error (USD) | Thời gian Train (s) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **B0: Global Median Baseline** | Không | `7.9087` | `13.2563` | `-0.1451` | `4.2000` | `20.3000` | `30.8000` | `< 0.5s` |
| **B0: Hierarchical (PU, DO, 6h) Median** | Không | `3.4810` | `6.5825` | `0.7177` | `2.1000` | `7.0000` | `10.5000` | `< 0.5s` |
| **M1: `LinearRegression` (OHE + Weather)** | **Có** | `3.2631` | `4.8911` | `0.8441` | `2.3382` | `6.7880` | `9.4190` | `10.20s` |
| **M2: `RandomForestRegressor` (Native Cat + Weather)** | **Có** | `3.2013` | `4.7645` | `0.8521` | `2.3189` | `6.6933` | `9.1810` | `35.20s` |
| **M3: `GBTRegressor` (Native Cat + ERA5 Weather)** | **Có** | **`3.0148`** | **`4.5948`** | **`0.8624`** | `2.1175` | **`6.3716`** | **`8.7787`** | `45.24s` |
| **M3-nw: `GBTRegressor` (Không Thời tiết — Ablation)** | Không | **`2.9977`** | `4.5975` | `0.8623` | **`2.0810`** | `6.3986` | `8.7947` | `40.37s` |

### 2.2. Tập Test Holdout (`2025-06`, `80.770` chuyến)

| Mô hình | Sử dụng Thời tiết ERA5 | MAE (USD) ↓ | RMSE (USD) ↓ | $R^2$ ↑ | P50 Error (USD) | P90 Error (USD) | P95 Error (USD) | Cải thiện MAE so với B0 Hierarchical |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **B0: Global Median Baseline** | Không | `7.8979` | `13.3313` | `-0.1322` | `4.2000` | `20.3000` | `30.8000` | — |
| **B0: Hierarchical (PU, DO, 6h) Median** | Không | `3.3466` | `6.5173` | `0.7294` | `2.1000` | `6.6500` | `10.1500` | Baseline tham chiếu |
| **M1: `LinearRegression` (OHE + Weather)** | **Có** | `3.2092` | `4.8180` | `0.8521` | `2.3621` | `6.4908` | `8.9146` | `-4,11%` (RMSE `-26,07%`) |
| **M2: `RandomForestRegressor` (Native Cat + Weather)** | **Có** | `3.1202` | `4.6736` | `0.8609` | `2.2952` | `6.4394` | `8.8220` | `-6,77%` (RMSE `-28,29%`) |
| **M3: `GBTRegressor` (Native Cat + ERA5 Weather)** | **Có** | **`2.9886`** | **`4.5993`** | **`0.8652`** | `2.1381` | `6.1489` | `8.5139` | **`-10,70%` (RMSE `-29,43%`)** |
| **M3-nw: `GBTRegressor` (Không Thời tiết — Ablation)** | Không | **`2.9157`** | **`4.5427`** | **`0.8685`** | **`2.0529`** | **`6.1010`** | **`8.4456`** | **`-12,88%` (RMSE `-30,30%`)** |

---

## 3. Phân tích Ablation Thời tiết ERA5 (`M3` vs `M3-nw`) & Chẩn đoán theo Lát cắt (Slice Diagnostics)

1. **Nhận xét về vai trò của biến thời tiết ERA5 (`temperature_2m`, `precipitation`, `snowfall`, `wind_speed_10m`)**:
   - Trên tập **Validation (05/2025)**, mô hình **M3 (có thời tiết)** đạt **RMSE tốt hơn** (`$4,5948` so với `$4,5975`),**$R^2$ cao hơn** (`0,8624` so với `0,8623`), và **P90/P95 Error thấp hơn** (`$6,3716 / $8,7787` so với `$6,3986 / $8,7947`). Đặc biệt trong các chuyến có mưa lớn (`heavy_rain_ge_2mm`, `2.237` chuyến), `M3_weather` giảm `RMSE` xuống **`$5,3653`** so với **`$5,4085`** của `M3_no_weather` (nhưng MAE lại kém hơn: `$3,3481` so với `$3,3400`). Trên tập **Test (06/2025)**, ưu thế này không còn: `M3_weather` kém `M3_no_weather` ở cả 3 điều kiện thời tiết, kể cả mưa lớn (`752` chuyến, MAE `$2,9106` so với `$2,6621`). Vì vậy không thể kết luận biến thời tiết giúp dự đoán tốt hơn khi mưa.
   - Khi chuyển sang tập **Test (06/2025 — đầu mùa hè nắng nóng)**, phân phối nhiệt độ dịch chuyển mạnh từ mùa đông/xuân (`01–04/2025`, nhiệt độ thấp) sang tháng 6 (`> 25°C`), khiến các nhánh cây quyết định cắt trên `temperature_2m` có sai số lệch nhẹ (`+$0,07` MAE) so với mô hình không phụ thuộc nhiệt độ `M3-nw` (`MAE = $2,9157`, `$R^2 = 0,8685`).
   - **Quyết định đóng gói (Serving Decision)**: Đóng gói **cả hai mô hình `M3` (có thời tiết) và `M3-nw` (không thời tiết)** trong gói `distribution/models/gold-v3/` để:
     - Phục vụ tức thời ở thời điểm hiện tại (`M3-nw`) mà không phụ thuộc độ trễ API thời tiết bên ngoài và không bị lệch mùa nhiệt độ.
     - Cho phép người dùng/nhà phân tích bật chế độ mô phỏng thời tiết ERA5 (`M3`) ngay trên giao diện Streamlit và Power BI.

2. **Chẩn đoán theo Khoảng cách & Quận (Tập Test `2025-06`)**:
   - **Chuyến ngắn `< 2 km` (`37.705` chuyến, giá TB `$9,56`)**: `MAE = $2,2552 USD`.
   - **Chuyến trung bình `2–5 km` (`29.681` chuyến, giá TB `$16,08`)**: `MAE = $2,9000 USD`.
   - **Chuyến dài `5–10 km` (`9.847` chuyến, giá TB `$32,00`)**: `MAE = $4,1321 USD`.
   - **Theo Quận đón (`Manhattan`, `74.563` chuyến, chiếm `92,3%`)**: `MAE = $2,6965 USD`, `RMSE = $3,9982 USD`.

---

## 4. Kiểm tra Độ trôi Dữ liệu Ngoài mẫu Thời gian (Out-of-Time 2026 Drift Check — G2.5)

Đánh giá trực tiếp mô hình **`M3-nw` (được huấn luyện hoàn toàn trên Gold 01–04/2025)** trên **`721.513` chuyến đi năm 2026** (`2026-06`, `2026-07`, `2026-08`):

| Giai đoạn Đánh giá Năm 2026 | Số chuyến (`rows`) | Baseline B0 MAE (USD) | Baseline B0 $R^2$ | Gold V3 (`M3-nw`) MAE (USD) ↓ | Gold V3 (`M3-nw`) RMSE (USD) ↓ | Gold V3 (`M3-nw`) $R^2$ ↑ | P50 Error (USD) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Tháng 06/2026** | `268.539` | `3.9650` | `0.5966` | **`3.2840`** | `6.3107` | **`0.7869`** | `2.1616` |
| **Tháng 07/2026** | `235.025` | `3.4783` | `0.7035` | **`3.0179`** | `4.7770` | **`0.8497`** | `2.1038` |
| **Tháng 08/2026** | `217.949` | `3.3574` | `0.7223` | **`2.8974`** | `4.5176` | **`0.8710`** | `2.0429` |
| **Toàn bộ 2026 (`06–08/2026`)** | **`721.513`** | `3.6229` | `0.6645` | **`3.0806`** | **`5.3311`** | **`0.8297`** | **`2.1061`** |

> **Kết luận về Khả năng Dự báo Hiện tại (2026)**:
> Mô hình huấn luyện từ tầng Gold 2025 giữ vững độ chính xác rất cao khi dự báo sang năm 2026 (**MAE tổng thể `= $3,08 USD`**, **sai số trung vị `P50 = $2,11 USD`**, **$R^2 = 0,8297$** trên `721.513` chuyến), vượt xa mô hình V2.2 cũ (`MAE = $4,44 USD`, `$R^2 = 0,5625`).

Tập `721.513` chuyến năm 2026 lấy từ `.sandbox/pretrip-v2/pilot-2026-06-08-v23-01`, hiện không còn trên máy nên các số 2026 được giữ theo lần chạy gốc và chưa tái lập được.

---

## 5. Đánh giá trên Mọi loại Chuyến (Tháng 06/2025)

Chấm mẫu ngẫu nhiên 5% (`197.930` chuyến, sai số MAE khoảng `± 0,03 USD`) của `3.946.190` chuyến hợp lệ tháng 06/2025 bằng đúng `M3-nw` + quy tắc JFK đang phục vụ:

| Nhóm chuyến | Tỷ trọng | MAE (USD) | RMSE (USD) | R² | P50 (USD) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Mọi loại chuyến** | **`100%`** | **`4,06`** | **`8,18`** | **`75,3%`** | **`2,36`** |
| Đồng hồ chuẩn (`ratecode_id = 1`) | `69,3%` | `2,93` | `4,60` | `86,5%` | `2,04` |
| Không có ratecode (`NULL`) | `25,5%` | `5,58` | `8,25` | `50,7%` | `3,91` |
| `ratecode_id = 2`, tuyến JFK ↔ Manhattan (quy tắc 70 USD) | `2,6%` | `0,00` | `0,00` | — | `0,00` |
| `ratecode_id = 2`, tuyến khác | `0,2%` | `44,01` | `49,13` | — | `57,25` |
| Ratecode khác (`3, 4, 5, 6, 99`) | `2,3%` | `22,92` | `36,35` | `-23,3%` | `14,55` |

Nhóm `ratecode_id = 1` ra `2,93 USD`, khớp với `2,9157 USD` ở mục 2.2 (chênh lệch do mục 2.2 còn loại chuyến thiếu thời tiết và dòng trùng). Chuyến hợp lệ có cước trong `(0; 500]` USD và vùng `1..263`; tháng 06/2025 có `4.254.377` chuyến, trong đó `277.468` chuyến cước không dương, `86` chuyến trên 500 USD và `32.145` chuyến vùng `264/265`. Tái lập bằng `scripts/eval_all_ratecodes_sample.py` và `scripts/eval_all_ratecodes_score.py`.
