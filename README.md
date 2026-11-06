# 🚕 NYC Taxi Big Data — Medallion Lakehouse & Pre-trip Fare AI

Đồ án cuối kỳ học phần **Tích hợp và Phân tích Dữ liệu Lớn** (Khoa Công nghệ Thông tin — Đại học Phenikaa).  
Hệ thống tích hợp **24,08 triệu chuyến xe NYC Yellow Taxi (2025)** với **Thời tiết khí tượng ERA5**, **Lịch nghỉ lễ Liên bang Mỹ** và **Bản đồ 263 vùng Taxi Zones** theo kiến trúc **Medallion (`Raw -> Bronze -> Silver -> Gold`)**, lưu trữ lai (**Parquet Data Lake + PostgreSQL 16 Star Schema**) và phục vụ dự đoán giá cước trước chuyến đi bằng **Spark MLlib (`GBTRegressor Gold V3`) + Bản đồ tương tác đặt xe Mapbox 2 chiều**.

---

## 1. Tóm tắt Bài toán & Phương pháp Giải quyết (Problem & Solution)

| Thành phần Cốt lõi | Chi tiết Thực thi trong Dự án |
| :--- | :--- |
| **1. Bài toán đặt ra** | **Dự báo cước trước chuyến đi (*Pre-trip Fare Estimation*)**: Ước tính cước cơ bản (`fare_amount`, USD) ngay khi khách chọn điểm đón/đến trên ứng dụng, trước khi xe lăn bánh. |
| **2. Thách thức lớn nhất** | **Tuyệt đối tránh rò rỉ dữ liệu (*Zero Data Leakage*)**: Không dùng quãng đường hay thời gian di chuyển thực tế trên đồng hồ. Dự báo hoàn toàn dựa trên: khoảng cách tâm vùng Taxi Zones (*Haversine*), chu kỳ giờ/thứ/ngày lễ và thời tiết khí tượng ERA5. |
| **3. Xử lý Dữ liệu Lớn** | Pipeline **PySpark Batch ELT** xử lý **24,08M** dòng thô qua 4 tầng Medallion (`Raw -> Bronze -> Silver -> Gold`), lọc sạch `0,82%` dữ liệu bẩn, lưu trữ **23,88M** dòng sạch (khớp 100% bản đồ, 99,92% thời tiết ERA5). |
| **4. Kho Dữ liệu Phục vụ** | **PostgreSQL 16 Star Schema** (`analytics.fact_trip`: 23,88M dòng, Zero-Loss), tối ưu Deferred Indexing KPI Dashboard mất 1,2–4,7 giây khi đã có cache và 26–52 giây ở lần đầu; bảng đa nguồn đọc từ bảng tổng hợp sẵn `analytics.mv_multisource_summary` (`10.147` dòng) nên chỉ mất dưới 0,5 giây. |
| **5. Mô hình Học máy (AI)** | **`GBTRegressor` (Chuỗi cây tăng cường độ dốc) trên Spark MLlib** kết hợp **Quy tắc cước cố định JFK 70 USD** (`Ratecode 2`). Vượt trội 4 mô hình đối chứng (Baseline B0, LR, RF, GBT-w) với **`MAE = 2,92 USD`** và **`R² = 86,85%`** trên chuyến đồng hồ chuẩn (`ratecode_id = 1`); trên mọi loại chuyến là **`MAE = 4,06 USD`**, **`R² = 75,3%`** (xem mục 4.3). |
| **6. Triển khai & Giao diện** | Microservice suy luận đóng băng (`fare-inference:8000`, chấm điểm đồng thời đường cong 24h trong khoảng 0,3–0,5 giây/lượt) + Giao diện Streamlit Bản đồ tương tác Mapbox 2 chiều + **Bảng kê phụ phí và gợi ý giờ khởi hành (`ai_fare_advisor.py`)**: định giá bằng `M3-nw`, thời tiết tự động chỉ để tham khảo (`M3-w` chỉ chạy khi người dùng tự nhập thời tiết), công khai Phụ phí chính thức NYC TLC và chủ động đề xuất khung giờ gần nhất có giá tốt hơn. |

---

## 2. Kiến trúc Tổng thể & Giải thích Luồng Dữ liệu

```text
[4 Nguồn Dữ liệu: NYC Yellow Taxi (24.08M) + ERA5 Weather + US Holidays + 263 Taxi Zones]
                                        │
                        Apache Spark (PySpark Batch ELT)
                                        ▼
         ┌──────────────────────────────┴──────────────────────────────┐
         │              Tầng Data Lake (Parquet / Snappy)              │
         │    Raw (24.08M) ──► Bronze ──► Silver (23.88M) ──► Gold     │
         └──────────────────────────────┬──────────────────────────────┘
                                        │
                 ┌──────────────────────┴──────────────────────┐
                 ▼                                             ▼
   [Kho Dữ liệu PostgreSQL 16]                    [Học máy Phân tán Spark ML]
   Star Schema (`analytics.*`)                    MLflow Tracking (5 Mô hình)
   • fact_trip (23.886.265 dòng)                  • B0, M1 (LR), M2 (RF)
   • dim_location (265 vùng)                      • M3-w (GBT + ERA5 Weather)
   • dim_weather (4.344 giờ)                      • M3-nw (GBT No-Weather)
   • dim_calendar (365 ngày)                      • Quy tắc JFK ↔ Manhattan 70 USD
   • fact_fare_prediction (170.395 dòng)                       │
                 │                                             ▼
                 │                              [Frozen Inference Microservice]
                 │                              (`fare-inference:8000` - Docker/K8s)
                 └──────────────────────┬──────────────────────┘
                                        ▼
                   [Trực quan hóa BI & Ứng dụng Web Dự báo]
                   • Power BI Desktop (`NYC_Taxi_Dashboard.pbix`)
                   • Streamlit Web App (`http://127.0.0.1:8501`)
                     ├─ Trang 1: Phân tích lịch sử (23,88M chuyến & Đa nguồn)
                     └─ Trang 2: Ước tính cước (Bản đồ tương tác Mapbox 2 chiều + Đánh giá ML + 5 Metrics)
```

### Vai trò của 4 Tầng Dữ liệu (Medallion Architecture)
1. **Tầng Raw (`data/raw/`)**: Lưu trữ nguyên bản dữ liệu tải về từ nguồn (file Parquet chuyến đi của NYC TLC, JSON thời tiết theo giờ từ Open-Meteo ERA5, CSV lịch nghỉ lễ Mỹ, GeoJSON/CSV ranh giới 263 vùng) kèm kiểm tra mã băm `SHA-256`.
2. **Tầng Bronze (`data/bronze/`)**: Đưa toàn bộ dữ liệu thô về định dạng chuẩn **Apache Parquet (nén Snappy)**, giữ nguyên giá trị gốc và gắn thêm siêu dữ liệu kiểm toán (`_source_file`, `_ingestion_batch_id`) để truy vết nguồn gốc dữ liệu.
3. **Tầng Silver (`data/silver/`)**: Làm sạch dữ liệu bẩn và chuẩn hóa múi giờ địa phương `America/New_York`:
   - Loại bỏ `197.119` dòng lỗi (`0,82%`) trên tổng số `24.083.384` dòng Raw (chủ yếu do thời gian trả khách nhỏ hoặc bằng thời gian đón khách `invalid_duration`, thiếu tọa độ vùng hoặc trùng lặp).
   - Giữ lại **`23.886.265` chuyến xe hợp lệ**.
4. **Tầng Gold (`data/gold/taxi_enriched/`)**: Tích hợp đa nguồn dữ liệu bằng phép nối phân tán trên PySpark (`Taxi` ⋈ `Taxi Zones` ⋈ `US Calendar` ⋈ `ERA5 Weather` theo ngày và giờ địa phương NYC). Đạt tỷ lệ khớp **100% vùng bản đồ** và **99,92% bản ghi thời tiết ERA5** (`23.866.019 / 23.886.265` chuyến).

---

## 3. Giải thích Thuật ngữ Chuyên môn trong Dự án (Glossary)

Để người đọc và hội đồng chấm thi dễ dàng nắm bắt ngay các chỉ số trên Dashboard và trong Báo cáo, dưới đây là ý nghĩa của các thuật ngữ cốt lõi:

### 3.1. Thuật ngữ Bài toán & Nghiệp vụ Taxi NYC
- **`fare_amount` (Cước cơ bản — USD)**: Số tiền cước tính theo đồng hồ (hoặc cước cố định sân bay) cho quãng đường và thời gian di chuyển, **chưa bao gồm** tiền tip, phí cầu đường (`tolls`) và thuế phụ thu. Đây là biến mục tiêu mà mô hình AI dự đoán.
- **Pre-trip Fare Estimation (Dự báo cước trước chuyến đi)**: Bài toán ước tính giá cước ngay tại thời điểm khách đặt xe. Do xe chưa lăn bánh nên mô hình **tuyệt đối không được dùng** quãng đường thực tế (`trip_distance`) hay thời lượng chuyến đi thực tế để tránh rò rỉ dữ liệu tương lai (*Data Leakage*).
- **Taxi Zone (`LocationID 1..263`)**: Ủy ban Taxi và Limousine New York (NYC TLC) chia toàn bộ 5 quận (*Manhattan, Queens, Brooklyn, Bronx, Staten Island* và sân bay Newark) thành 263 vùng địa lý. Người dùng có 3 cách chọn điểm đặt xe linh hoạt: **(1) Click hoặc kéo thả ghim A/B trực tiếp trên bản đồ tương tác NYC Mapbox**, **(2) Bấm 1-click chọn nhanh cụm điểm nóng** (*Sân bay JFK/LGA, Times Square, Phố Wall...*), hoặc **(3) Gõ địa chỉ vào ô tìm kiếm**. Hệ thống tự động phân giải tọa độ vào 263 vùng (*Point-in-Polygon*) cục bộ siêu tốc (<2ms, 0 tốn quota API) và đồng bộ 2 chiều tức thì (`external_sync`) lên thanh nhập địa chỉ.
- **Khoảng cách tâm vùng Haversine (`zone_centroid_distance_km`)**: Khoảng cách đường chim bay (tính theo mặt cầu Trái Đất, đơn vị km) nối từ tâm vùng đón đến tâm vùng trả khách — đặc trưng quan trọng nhất thay thế cho quãng đường đồng hồ khi dự báo trước chuyến đi.
- **Ratecode 2 (Quy tắc Cước cố định JFK ↔ Manhattan = 70,00 USD)**: Quy định chính thức của NYC TLC áp mức giá cố định `70,00 USD` cho mọi chuyến taxi vàng đi giữa Sân bay Quốc tế JFK (`Zone 132`) và bất kỳ vùng nào thuộc quận `Manhattan`. Kiểm chứng trên `574.602` chuyến thực tế cho thấy `99,9864%` chuyến tuân theo đúng mức giá này.

### 3.2. Thuật toán Học máy & Chỉ số Đánh giá Mô hình
- **Phân biệt Nền tảng (Platform) vs. Mô hình (Model)**:
  - **Nền tảng thực thi**: **Apache Spark (PySpark)** + **Spark MLlib** (bộ công cụ tính toán phân tán giúp huấn luyện song song trên hàng chục triệu bản ghi dữ liệu lớn).
  - **Mô hình / Thuật toán AI cụ thể**: **`GBTRegressor` (Gradient Boosted Trees)** — thuật toán chuỗi cây quyết định tăng cường độ dốc, mỗi cây kế tiếp học cách sửa sai số thặng dư của cây phía trước.
- **Baseline `B0` (Mô hình Thống kê Cơ sở)**: Phương pháp tra cứu giá cước trung vị lịch sử theo cặp *(Vùng đón, Vùng trả, Khung giờ)* mà không dùng trí tuệ nhân tạo, dùng làm mốc chuẩn để chứng minh hiệu quả vượt trội của Học máy.
- **`M1` (Linear Regression - Hồi quy Tuyến tính)**: Mô hình học máy cơ bản giả định mối quan hệ tuyến tính giữa các đặc trưng và giá cước.
- **`M2` (Random Forest - Rừng Ngẫu nhiên)**: Tập hợp nhiều cây quyết định độc lập cùng bỏ phiếu dự đoán giá cước.
- **`M3-w` & `M3-nw` (Gradient Boosted Trees - GBT)**:
  - **`M3-w` (Full Weather)**: Dùng đủ **14 đặc trưng** (10 đặc trưng hành trình/thời gian + 4 đặc trưng thời tiết ERA5: nhiệt độ, lượng mưa, lượng tuyết, tốc độ gió).
  - **`M3-nw` (No-Weather — Mặc định phục vụ)**: Dùng **10 đặc trưng cơ bản** (không bắt buộc người dùng phải nhập thông số thời tiết khi đặt xe mà vẫn đạt độ chính xác cao nhất).
- **MAE (*Mean Absolute Error* — Sai số tuyệt đối trung bình, USD)**: Trung bình mỗi chuyến đi mô hình dự đoán lệch bao nhiêu đô-la so với thực tế. **MAE càng thấp thì mô hình càng chính xác** (Gold V3 đạt `MAE = 2,92 USD` trên chuyến đồng hồ chuẩn, `4,06 USD` trên mọi loại chuyến).
- **RMSE (*Root Mean Squared Error* — Sai số bình phương trung bình căn, USD)**: Tương tự MAE nhưng phạt nặng các chuyến có sai số lớn bất thường. **RMSE càng thấp càng tốt** (Gold V3 đạt `RMSE = 4,54 USD` trên chuyến đồng hồ chuẩn, `8,18 USD` trên mọi loại chuyến).
- **R² (*Coefficient of Determination* — Hệ số xác định, %)**: Cho biết mô hình giải thích được bao nhiêu phần trăm sự biến thiên của giá cước thực tế. **R² càng gần 100% (1.0) càng tốt** (Gold V3 đạt `R² = 86,85%` trên chuyến đồng hồ chuẩn, `75,3%` trên mọi loại chuyến).
- **P50 / P90 Abs Error (Sai số phân vị thứ 50 và 90, USD)**: `P50 = 2,05 USD` nghĩa là có tới **50% số chuyến xe được dự đoán lệch không quá 2,05 USD** so với thực tế (chuyến đồng hồ chuẩn; mọi loại chuyến là `2,36 USD`).
- **Out-of-Time Drift (Kiểm định độ trôi dữ liệu theo thời gian — Năm 2026)**: Đem mô hình học từ dữ liệu năm 2025 chạy kiểm nghiệm trên `721.513` chuyến xe hoàn toàn mới của **Tháng 06, 07, 08/2026** để chứng minh mô hình không bị lỗi thời.
- **Frozen Inference**: Cơ chế đóng băng mô hình đã huấn luyện thành gói bất biến (`distribution/models/gold-v3/`). Máy chủ suy luận chỉ nạp mô hình lên RAM để trả kết quả (đo thực tế trung vị `0,34–0,54 giây/lượt`, lượt đầu sau thời gian rảnh có thể tới vài chục giây), không có bước huấn luyện trong luồng phục vụ (bộ đếm `fit_calls` luôn bằng 0 nên chỉ mang tính khai báo).

---

## 4. Kết quả Thực nghiệm Cốt lõi

### 4.1. Đối soát Dữ liệu Lớn & Vận hành Pipeline (5 Metrics Giám sát)
| Chỉ số Giám sát | Kết quả Thực nghiệm | Ý nghĩa & Ghi chú Kiểm chứng |
| :--- | :--- | :--- |
| **1. Trạng thái ETL (`Job Status`)** | **`6/6 PASS (100%)`** | Nạp thành công 100% cả 6 phân vùng tháng (`2025-01` → `2025-06`) và 3 bảng chiều vào PostgreSQL. |
| **2. Thời gian xử lý (`Processing Time`)** | **`864 giây / tháng`** | Tổng nạp PostgreSQL `5.184s` (~4.600 dòng/giây); trích mẫu ML `28,3s`; huấn luyện 5 mô hình `100,2s`. |
| **3. Đối soát số dòng (`Reconciliation`)** | **`23.886.265 dòng`** | Khớp tuyệt đối giữa Silver/Gold Parquet và bảng `analytics.fact_trip` (`Chênh lệch = 0 / Zero-Loss`). |
| **4. Tỷ lệ lọc dữ liệu bẩn (`Reject Rate`)** | **`0,82%` (`197.119` dòng)** | Lọc từ `24.083.384` dòng Raw; tầng Gold khớp **100% Taxi Zones** và **99,92% Thời tiết ERA5**. |
| **5. Dịch vụ AI & RAM (`Serving Health`)** | **`READY (200 OK)`** | Mô hình đóng băng, luồng phục vụ không có bước huấn luyện; container `fare-inference` dùng `~850–930 / 3072 MB RAM`. |

### 4.2. So sánh 6 Cấu hình Mô hình (Holdout Test 06/2025 — `80.770` chuyến đồng hồ chuẩn `ratecode_id = 1`) & Kiểm chứng Năm 2026 (`721.513` chuyến)
| Mô hình | Thuật toán Cốt lõi | Dùng Thời tiết | MAE 2025 (USD) ↓ | RMSE 2025 (USD) ↓ | R² 2025 (%) ↑ | P50 2025 (USD) ↓ | MAE Năm 2026 (USD) ↓ |
| :--- | :--- | :---: | ---: | ---: | ---: | ---: | ---: |
| **1. Trung vị toàn cục (`B0_global`)** | Thống kê cơ sở đơn giản | Không | `7,90` | `13,33` | `-13,22%` | `4,20` | `8,54` |
| **2. Thống kê Tuyến & Giờ (`Baseline B0`)** | Nhóm theo Tuyến và Khung giờ | Không | `3,35` | `6,51` | `72,94%` | `2,10` | `3,53` |
| **3. Hồi quy Tuyến tính (`M1 - LR`)** | Spark MLlib `LinearRegression` | Có | `3,21` | `4,81` | `85,21%` | `2,33` | — |
| **4. Rừng Ngẫu nhiên (`M2 - RF`)** | Spark MLlib `RandomForestRegressor` | Có | `3,12` | `4,67` | `86,09%` | `2,24` | — |
| **5. GBT + Thời tiết ERA5 (`M3-w`)** | Spark MLlib `GBTRegressor` | Có | `2,99` | `4,60` | `86,52%` | `2,11` | — |
| **6. GBT Tối ưu (`M3-nw` — Đang dùng)** | **Spark MLlib `GBTRegressor` + JFK Rule** | **Không** | **`2,92`** | **`4,54`** | **`86,85%`** | **`2,05`** | **`3,08` (`R² = 83,0%`)** |

### 4.3. Phạm vi của các con số: chuyến đồng hồ chuẩn và mọi loại chuyến
Bảng 4.2 chỉ tính chuyến `ratecode_id = 1`, đúng phạm vi huấn luyện. Khi người dùng đặt xe, mô hình không biết trước loại ratecode, nên cần xem cả các nhóm còn lại. Bảng dưới chấm mẫu ngẫu nhiên 5% (`197.930` chuyến, sai số MAE khoảng `± 0,03 USD`) của `3.946.190` chuyến hợp lệ tháng 06/2025 bằng đúng `M3-nw` + quy tắc JFK. Nhóm `ratecode_id = 1` ra `2,93 USD`, khớp với `2,92 USD` của bảng 4.2.

| Nhóm chuyến | Tỷ trọng | MAE (USD) | RMSE (USD) | R² | P50 (USD) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Mọi loại chuyến** | **`100%`** | **`4,06`** | **`8,18`** | **`75,3%`** | **`2,36`** |
| Đồng hồ chuẩn (`ratecode_id = 1`) | `69,3%` | `2,93` | `4,60` | `86,5%` | `2,04` |
| Không có ratecode (`NULL`) | `25,5%` | `5,58` | `8,25` | `50,7%` | `3,91` |
| `ratecode_id = 2`, tuyến JFK ↔ Manhattan (quy tắc 70 USD) | `2,6%` | `0,00` | `0,00` | — | `0,00` |
| `ratecode_id = 2`, tuyến khác | `0,2%` | `44,01` | `49,13` | — | `57,25` |
| Ratecode khác (`3, 4, 5, 6, 99`) | `2,3%` | `22,92` | `36,35` | `-23,3%` | `14,55` |

Chuyến hợp lệ là chuyến có cước trong `(0; 500]` USD và vùng `1..263`; tháng 06/2025 có `4.254.377` chuyến, trong đó `277.468` chuyến cước không dương, `86` chuyến trên 500 USD và `32.145` chuyến vùng `264/265`. Tái lập bằng `scripts/eval_all_ratecodes_sample.py` và `scripts/eval_all_ratecodes_score.py`. Tập `721.513` chuyến năm 2026 lấy từ `.sandbox/pretrip-v2/pilot-2026-06-08-v23-01`, hiện không còn trên máy nên các số 2026 được giữ theo lần chạy gốc và chưa tái lập được.

**Hạn chế cần lưu ý:** Con số `Cước đồng hồ ML` trên giao diện là dự đoán của mô hình cộng thêm một phần điều chỉnh theo quãng đường Mapbox; các chỉ số MAE ở trên chỉ đánh giá phần mô hình, chưa đánh giá phần điều chỉnh này. Gợi ý giờ đi dựa trên chênh lệch của mô hình, không dùng dữ liệu giao thông thời gian thực, và mức chênh có thể nhỏ hơn sai số trung bình. Airflow và Kubernetes mới dừng ở mức cấu hình, chưa triển khai thật.

---

## 5. Cấu trúc Thư mục Dự án

```text
nyc-taxi-bigdata/
├── airflow/dags/                  # DAG điều phối tự động pipeline ELT & ML hàng tháng
├── assets/taxi_zones/             # Bản đồ ranh giới 263 vùng NYC TLC (GeoJSON & Manifest)
├── config/                        # Cấu hình hệ thống, hợp đồng đặc trưng ML & 5 Metrics
├── dashboard/app.py               # Entrypoint ứng dụng Web Streamlit
├── distribution/models/gold-v3/   # Gói mô hình Spark GBT Gold V3 đóng băng (24 file kèm SHA-256)
├── k8s/                           # Bản kê khai Kubernetes (Deployment, Service, HPA, Probes)
├── reports/
│   ├── FINAL_REPORT_OUTLINE.md    # Nội dung chi tiết 6 Chương Báo cáo Cuối kỳ
│   ├── GOLD_V3_TRAINING_REPORT.md # Báo cáo thực nghiệm 5 mô hình ML, Ablation & Drift 2026
│   ├── POWER_BI_MODEL_PAGE_GUIDE.md
│   └── NYC_Taxi_Dashboard.pbix    # File báo cáo Power BI Desktop kết nối PostgreSQL
├── scripts/                       # Các script thực thi ELT (Raw/Bronze/Silver/Gold), nạp DB & ML
├── sql/                           # DDL khởi tạo Star Schema, Deferred Indexes & Views
├── src/nyc_taxi/
│   ├── common/                    # Quản lý cấu hình & tiện ích dùng chung
│   ├── ingestion/                 # Thu thập dữ liệu Taxi, ERA5 Weather, Calendar, Zones
│   ├── etl/                       # Xử lý PySpark Medallion (Bronze -> Silver -> Gold)
│   ├── database/                  # Kết nối và nạp dữ liệu COPY vào PostgreSQL
│   ├── ml/                        # Chuẩn bị mẫu phân tầng, huấn luyện Spark ML & HTTP Server
│   ├── monitoring/                # Bộ thu thập 5 chỉ số giám sát vận hành Pipeline
│   └── dashboard/                 # Giao diện Streamlit (Bản đồ Mapbox 2 chiều, Gợi ý giờ khởi hành, Đánh giá ML, Giám sát)
└── tests/                         # Bộ kiểm thử tự động (1.474 bài test Python + 30 bài test frontend Node.js)
```

---

## 6. Hướng dẫn Khởi chạy Nhanh (Quick Start)

### 6.1. Khởi chạy Hệ thống 4 Containers (PostgreSQL + MLflow + AI Inference + Streamlit UI)
**Yêu cầu**: Cài đặt **Docker Desktop** và tạo file `.env` từ `.env.example` (điền `MAPBOX_ACCESS_TOKEN` để bật tính năng tìm địa chỉ và vẽ bản đồ đường đi thực tế).

```powershell
# 1. Khởi động Kho dữ liệu PostgreSQL 16 và máy chủ MLflow
docker compose up -d postgres mlflow

# 2. Khởi động Microservice Suy luận Gold V3 và Giao diện Web Streamlit
docker compose -f docker-compose.yml -f docker-compose.inference.yml -f docker-compose.gold-v3.yml up -d fare-inference dashboard
```

*(Hoặc gộp cả 4 dịch vụ vào 1 lệnh duy nhất: `docker compose -f docker-compose.yml -f docker-compose.inference.yml -f docker-compose.gold-v3.yml up -d`)*.

**Truy cập các dịch vụ sau khi khởi chạy**:
1. **Ứng dụng Web Streamlit (`http://127.0.0.1:8501`)**:
   - **Trang "Phân tích lịch sử"**: Bộ lọc theo Tháng & Quận, 4 thẻ KPI tổng quan (`23.886.265` chuyến), 3 biểu đồ phân bố theo Tháng/Giờ/Quận và 2 bảng đối chiếu Đa nguồn (**Thời tiết ERA5** & **Lịch nghỉ lễ Mỹ**).
   - **Trang "Ước tính cước" (Gồm 3 Tab trực quan)**:
     - `🚕 Ước tính cước chuyến đi`: Đặt xe chuẩn trải nghiệm thực tế với 3 cách chọn điểm linh hoạt: (1) Click/kéo thả trực tiếp ghim A/B trên bản đồ NYC Mapbox, (2) 1-click chọn nhanh cụm điểm nóng (Sân bay JFK/LGA, Times Square, Phố Wall...), (3) Gõ địa chỉ qua Mapbox Autocomplete. Giá được tính bằng mô hình không thời tiết **`M3-nw`**; hệ thống vẫn **tự động lấy Dự báo Thời tiết NYC (Open-Meteo ERA5)** theo giờ đón nhưng chỉ để tham khảo, còn **`M3-w`** chỉ chạy khi người dùng tự nhập thời tiết, hiển thị **Bảng kê Tổng giá cước kèm các khoản Phụ phí chính thức NYC TLC thực tế phát sinh** (Giờ cao điểm `+$2.50`, Ban đêm `+$1.00`, Tắc đường Manhattan `+$2.50`, Sân bay `+$1.75`) và **đề xuất khung giờ gần nhất có mức giá tiết kiệm hơn**.
     - `📊 Độ chính xác & So sánh Mô hình`: 4 thẻ KPI kết quả + 3 Sơ đồ trực quan (So sánh MAE giữa các mô hình; Đường Giá thực tế vs Giá AI dự đoán theo 24 giờ; Biểu đồ kiểm chứng độ ổn định sang Năm 2026).
     - `🩺 Giám sát Hệ thống & Dữ liệu`: Sơ đồ luồng Medallion 4 bước, 5 thẻ chỉ số vận hành và 2 biểu đồ cột theo tháng.
2. **MLflow Tracking UI (`http://127.0.0.1:5000`)**: Quản lý tham số và chỉ số của các lần huấn luyện trong experiment `nyc-taxi-fare-gold-v3`.
3. **Kiểm tra nhanh trạng thái sẵn sàng của AI Server**:
   ```powershell
   docker exec nyc-taxi-bigdata-dashboard-1 python -c "import urllib.request; print(urllib.request.urlopen('http://fare-inference:8000/ready').read().decode())"
   ```

### 6.2. Chạy Lại Pipeline ELT & Huấn luyện Mô hình từ Đầu (Tùy chọn)
Nếu muốn tự chạy lại từng bước xử lý dữ liệu lớn và huấn luyện mô hình:
```powershell
# Bước 1: Tải dữ liệu thô & chạy pipeline Medallion (Raw -> Bronze -> Silver -> Gold -> PostgreSQL)
python scripts/download_raw_data.py --all
python scripts/build_bronze.py --all
python scripts/build_silver.py --all
python scripts/build_gold.py --all
python scripts/load_postgres.py --all --create-indexes --validate

# Bước 2: Trích xuất mẫu phân tầng (bảo toàn 100% vùng hiếm), huấn luyện 5 mô hình & lưu vào PostgreSQL
python scripts/prepare_gold_v3.py --execute
python scripts/train_gold_v3.py --tracking-uri http://localhost:5000
python scripts/write_gold_v3_predictions.py
python scripts/package_gold_v3_bundle.py
```

### 6.3. Sao lưu Cơ sở Dữ liệu & Chạy Kiểm thử Tự động (CI/CD)
```powershell
# Sao lưu PostgreSQL (Schema + Metadata hoặc Full Dump ra thư mục backups/)
.\scripts\backup.ps1

# Chạy toàn bộ 1.474 bài kiểm thử Python theo đúng profile GitHub Actions CI
python scripts/ci_python.py

# Chạy bộ kiểm thử giao diện người dùng (30 bài test frontend Node.js)
node --test tests/test_address_frontend.js tests/test_route_map_frontend.js
```

---

## 7. Tài liệu Báo cáo & Hướng dẫn Chi tiết
- **Đề cương & Nội dung Báo cáo Cuối kỳ (Đầy đủ 6 Chương)**: [`reports/FINAL_REPORT_OUTLINE.md`](reports/FINAL_REPORT_OUTLINE.md)
- **Báo cáo Thực nghiệm 5 Mô hình ML & Kiểm định Drift 2026**: [`reports/GOLD_V3_TRAINING_REPORT.md`](reports/GOLD_V3_TRAINING_REPORT.md)
- **Hướng dẫn Tích hợp Trang Đánh giá AI trên Power BI Desktop**: [`reports/POWER_BI_MODEL_PAGE_GUIDE.md`](reports/POWER_BI_MODEL_PAGE_GUIDE.md)
