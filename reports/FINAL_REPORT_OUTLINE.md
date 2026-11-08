# Đề cương & Nội dung Chi tiết Báo cáo Tổng kết Cuối kỳ — Học phần "Tích hợp và Phân tích Dữ liệu Lớn"

- **Trường**: Đại học Phenikaa — Khoa Công nghệ Thông tin
- **Học phần**: Tích hợp và Phân tích Dữ liệu Lớn (Big Data Integration and Analytics)
- **Giảng viên hướng dẫn**: Thầy Phạm Kim Thành
- **Đề tài**: Xây dựng Hệ thống Tích hợp, Phân tích Dữ liệu Lớn và Dự báo Giá cước Taxi New York (NYC Yellow Taxi + Khí tượng ERA5) theo Kiến trúc Medallion & Microservices

---

## 1. Bảng Ánh xạ Toàn diện: Lý thuyết Bài giảng & Báo cáo Giữa kỳ → Bằng chứng Thực thi trong Dự án

| Chủ đề / Cam kết trong Slide & Báo cáo đã nộp | Minh chứng Cụ thể trong Kho mã nguồn (`c:\nyc-taxi-bigdata`) | Số liệu Thực nghiệm Đã Kiểm chứng |
| :--- | :--- | :--- |
| **Bài giảng 3 — Các loại Cơ sở dữ liệu & Báo cáo Giữa kỳ Chọn Database**:<br>- Kiến trúc Hybrid Storage: **Data Lake (Parquet)** + **Serving DB (PostgreSQL 16)**.<br>- Mô hình **Star Schema** (`fact_trip`, `dim_location`, `dim_weather`, `dim_calendar`, `etl_load_log`).<br>- Tạo Index tối ưu sau khi nạp (`postload/create_indexes.sql`) và Chiến lược Sao lưu (`Backup & Recovery`). | - Tầng Data Lake: `data/raw/`, `data/bronze/`, `data/silver/`, `data/gold/`<br>- Tầng Serving DB: `sql/01_create_schema.sql`, `sql/02_create_tables.sql`, `sql/postload/create_indexes.sql`, `sql/03_gold_v3_predictions.sql`<br>- Script nạp: `scripts/load_postgres.py`, `scripts/write_gold_v3_predictions.py`<br>- Script sao lưu tự động: `scripts/backup.ps1` | - **`23.886.265` dòng** trong `analytics.fact_trip` (`0` dòng thất thoát so với Silver/Gold).<br>- `265` vùng `dim_location`, `365` ngày `dim_calendar`, `4.344` bản ghi giờ `dim_weather`.<br>- **`170.395` dòng** dự đoán trong `analytics.fact_fare_prediction` + `24` dòng chỉ số trong `analytics.model_metrics`. |
| **Bài giảng 4 — Pipeline Dữ liệu Lớn & Báo cáo Thiết kế Data Pipeline**:<br>- Kiến trúc **ELT Medallion 4 tầng (`Raw -> Bronze -> Silver -> Gold`)** bằng **Apache Spark (PySpark)**.<br>- Tích hợp đa nguồn: **NYC TLC Yellow Taxi + Thời tiết ERA5 + Lịch nghỉ lễ + Bản đồ Taxi Zones**.<br>- Điều phối tự động (**Airflow DAG**) & **5 Metrics Giám sát Pipeline**. | - Pipeline PySpark: `scripts/download_raw_data.py`, `scripts/build_bronze.py`, `scripts/build_silver.py`, `scripts/build_gold.py`<br>- Điều phối Airflow: `airflow/dags/nyc_taxi_elt_ml_dag.py`<br>- Module giám sát 5 Metrics: `src/nyc_taxi/monitoring/metrics.py` | - `24.083.384` dòng Raw/Bronze → làm sạch `197.119` dòng bẩn (`0,82%`, lý do chính: `invalid_duration`) → còn **`23.886.265` dòng** hợp lệ.<br>- Tỷ lệ khớp thời tiết ERA5 (`weather_matched`): **`99,92%`** (`23.866.019 / 23.886.265`).<br>- `6/6` phân vùng fact + `3/3` bảng chiều nạp PostgreSQL trạng thái `loaded` (`100%`), `row_delta = 0`. |
| **Phân tích Chuyên sâu & Học máy Phân tán (Spark MLlib + MLflow)**:<br>- Huấn luyện trực tiếp từ dữ liệu **Gold 2025 đã tích hợp Thời tiết ERA5** (`gold-v3`).<br>- So sánh đa mô hình (`Baseline B0`, `LinearRegression M1`, `RandomForest M2`, `GBTRegressor M3` có thời tiết & `M3-nw` không thời tiết).<br>- Đánh giá khả năng tổng quát hóa sang năm hiện tại **2026**. | - Chuẩn bị dữ liệu Gold V3: `src/nyc_taxi/ml/gold_v3_data.py`, `scripts/prepare_gold_v3.py`<br>- Huấn luyện Spark ML & MLflow: `src/nyc_taxi/ml/gold_v3_training.py`, `scripts/train_gold_v3.py`<br>- Báo cáo kết quả: `reports/GOLD_V3_TRAINING_REPORT.md`<br>- MLflow Experiment: `nyc-taxi-fare-gold-v3` (ID `2`, 5 runs). | - Phủ **100% Zone hoạt động** trong tập Train (`258/258` Pickup, `259/259` Dropoff) trên mẫu `507.259` dòng.<br>- Quy tắc cố định JFK ↔ Manhattan (`ratecode_id = 2`): **`99,9864%`** khớp **70,00 USD** (`574.524 / 574.602` chuyến).<br>- Tập Test 06/2025, chuyến đồng hồ chuẩn: **MAE `= 2,92 USD`**, **`R² = 0,8685`**; mọi loại chuyến: **MAE `= 4,06 USD`**, **`R² = 0,753`** (cải thiện `R² +30,6%` so với V2.2 cũ).<br>- Kiểm tra năm 2026 (`721.513` chuyến): **MAE `= 3,08 USD`**, **`R² = 0,8297`**. |
| **Bài giảng Hạ tầng Ứng dụng Dữ liệu (Microservices, Docker, CI/CD, Kubernetes)**:<br>- Tách biệt **Data Processing (`worker`)**, **Model Serving (`fare-inference`)**, **Web UI (`dashboard`)**, **Tracking (`mlflow`)**, **OLAP DB (`postgres`)**.<br>- Đóng gói Container, kiểm thử tự động CI/CD và cấu hình **Kubernetes (Deployment, Service, HPA, Probes)**. | - Docker & Compose: `Dockerfile.spark`, `Dockerfile.inference`, `Dockerfile.dashboard`, `docker-compose.yml`, `docker-compose.gold-v3.yml`<br>- Suy luận Microservice: `src/nyc_taxi/ml/inference_server.py`, `src/nyc_taxi/ml/gold_v3_inference.py`<br>- Giao diện Streamlit, Bản đồ 2 chiều & Gợi ý giờ khởi hành: `src/nyc_taxi/dashboard/gold_v3_page.py`, `src/nyc_taxi/dashboard/address_page.py`, `src/nyc_taxi/dashboard/ai_fare_advisor.py`, `src/nyc_taxi/dashboard/interactive_map.py`, `src/nyc_taxi/dashboard/interactive_map_frontend/`<br>- Kubernetes Manifest: `k8s/fare-inference-deployment.yaml`<br>- Bộ kiểm thử CI/CD: `scripts/ci_python.py`, `tests/test_address_frontend.js`, `tests/test_route_map_frontend.js` | - 4 container chạy độc lập với Healthcheck (`/health`, `/ready`, `pg_isready`, `_stcore/health`).<br>- Gói mô hình đóng băng `distribution/models/gold-v3/` (`24` file, kiểm chứng SHA-256).<br>- **1.511 bài kiểm thử tự động đạt 100% PASS** (`1.481` bài test Python + `30` bài test giao diện Node.js).<br>- Hướng dẫn tích hợp trang mới trên Power BI: `reports/POWER_BI_MODEL_PAGE_GUIDE.md`. |

---

## 2. Cấu trúc & Nội dung Soạn sẵn cho Quyển Báo cáo Tổng kết Cuối kỳ (6 Chương)

### Chương 1: Tổng quan Đề tài và Bài toán Tích hợp Dữ liệu Lớn
1.1. **Bối cảnh bài toán**: Hệ thống giao thông đô thị New York (NYC TLC Yellow Taxi) phát sinh hàng triệu chuyến đi mỗi tháng. Nhu cầu thực tế đòi hỏi vừa phải phân tích lịch sử đa chiều ở quy mô hàng chục triệu dòng, vừa phải ước tính chính xác cước phí trước chuyến đi (Pre-trip Fare Estimation) từ địa chỉ thực tế và bản đồ tương tác đặt xe trực quan.
1.2. **Đặc trưng 5V của Dữ liệu Lớn trong dự án**:
- **Volume (Khối lượng)**: `24.083.384` bản ghi chuyến đi đầu vào (`23.886.265` dòng hợp lệ ở tầng Silver/Gold/PostgreSQL, cộng thêm `721.513` chuyến kiểm định năm 2026).
- **Variety (Đa dạng nguồn và định dạng)**:
  1. Dữ liệu hành trình NYC Yellow Taxi 6 tháng đầu năm 2025 (`Parquet`).
  2. Dữ liệu khí tượng lịch sử theo giờ từ Open-Meteo ERA5 (`JSON` — nhiệt độ `temperature_2m`, lượng mưa `precipitation`, lượng tuyết `snowfall`, tốc độ gió `wind_speed_10m`, mã thời tiết WMO `weather_code`).
  3. Dữ liệu không gian 263 vùng Taxi Zones (`GeoJSON` polygons & `CSV` lookup).
  4. Dữ liệu lịch nghỉ lễ Liên bang Mỹ (`JSON` — US Federal Holidays & Weekends).
- **Velocity & Veracity (Tốc độ & Độ tin cậy)**: Làm sạch `197.119` bản ghi lỗi ở tầng Silver (`0,82%`, chủ yếu do thời gian trả khách nhỏ hơn hoặc bằng thời gian đón khách `invalid_duration`) và lọc các bản ghi nhiễu cực đoan (cước âm, đồng hồ nhảy số `863.372 USD`, mã vùng `264/265` không xác định) khi tuyển chọn tập huấn luyện Học máy.
- **Value (Giá trị mang lại)**: Cung cấp Kho dữ liệu Star Schema phục vụ BI (Power BI & Streamlit) và dịch vụ AI dự báo giá cước trước chuyến đi với sai số trung vị `2,05 USD/chuyến` trên chuyến đồng hồ chuẩn (`2,36 USD/chuyến` trên mọi loại chuyến).

---

### Chương 2: Thiết kế Kiến trúc Lưu trữ Lai (Hybrid Data Lakehouse & Star Schema)
2.1. **Kiến trúc Lưu trữ Lai (Parquet Data Lake + PostgreSQL Serving Mart)**:
- **Tầng Data Lake (`Parquet` nén Snappy)**: Lưu trữ chi phí thấp, hỗ trợ đọc song song theo cột (columnar) và đẩy vị từ lọc (predicate pushdown) cho công cụ xử lý phân tán **Apache Spark**.
- **Tầng Serving Database (`PostgreSQL 16`)**: Lưu trữ mô hình **Star Schema** trong schema `analytics` để phục vụ truy vấn OLAP tốc độ cao từ Power BI và Streamlit.

2.2. **Bảng Thống kê Dung lượng & Số dòng trong Kho Dữ liệu Star Schema (`analytics`)**:

| Tên Bảng / View | Vai trò trong Star Schema | Số lượng Bản ghi | Khóa chính / Ràng buộc đối soát |
| :--- | :--- | ---: | :--- |
| `analytics.fact_trip` | Bảng Fact trung tâm lưu toàn bộ chuyến đi 2025 | `23.886.265` | `trip_id` (Zero-loss: khớp 100% với Gold Parquet) |
| `analytics.dim_location` | Bảng chiều Không gian vùng đón/trả NYC TLC | `265` | `location_id` (`1..263` vùng bản đồ + `264, 265`) |
| `analytics.dim_weather` | Bảng chiều Thời tiết theo giờ ERA5 (`America/New_York`) | `4.344` | `weather_id` (`2025-01-01 00:00` → `2025-06-30 23:00`) |
| `analytics.dim_calendar` | Bảng chiều Thời gian & Lịch nghỉ lễ Liên bang Mỹ | `365` | `date_id` (`is_weekend`, `is_holiday`, `holiday_name`) |
| `analytics.fact_fare_prediction` | Bảng Fact lưu kết quả dự đoán của 5 mô hình ML | `170.395` | `prediction_id` (`89.625` dòng Validation + `80.770` dòng Test) |
| `analytics.model_metrics` | Bảng tổng hợp kết quả đánh giá mô hình & Drift 2026 | `24` | `(model_key, split)` |
| `analytics.mv_multisource_summary` | Materialized View tổng hợp sẵn theo Tháng × Vùng đón × Thời tiết × Loại ngày cho Dashboard | `10.147` | Dựng lại bằng `load_postgres.py --create-indexes` sau khi nạp dữ liệu |
| `analytics.mv_kpi_buckets` | Materialized View gom chuyến theo Tháng × Giờ × Vùng đón cho KPI trang lịch sử | `34.802` | Tổng `23.886.265` chuyến, khớp `fact_trip` |
| `analytics.mv_route_hour_fare` | Materialized View thống kê cước theo Tuyến × Loại ngày × Giờ (01–05/2025) cho gợi ý giờ đi | `69.217` | Mỗi khung giờ có ít nhất `30` chuyến |
| `analytics.etl_load_log` | Nhật ký kiểm toán và đối soát quá trình nạp dữ liệu | `6` (fact) / `9` (tổng) | `100% status = 'loaded'`, `row_delta = 0` |

2.3. **Tối ưu hóa Truy vấn & Sao lưu**:
- Áp dụng kỹ thuật **Deferred Indexing** (tắt index khi `COPY` hàng loạt 23,88 triệu dòng, sau đó tạo B-Tree index bằng `sql/postload/create_indexes.sql`).
- Truy vấn Dashboard sử dụng tiền tổng hợp theo cụm (`WITH buckets AS (...)`) kết hợp `GROUP BY GROUPING SETS` trong giao dịch `REPEATABLE READ, READ ONLY`, nhưng vẫn mất `26–52 giây` ở lần đầu vì phải quét cả 23,88 triệu dòng. Phần gom nhóm `buckets` nay được tính sẵn thành `analytics.mv_kpi_buckets` (`34.802` dòng, tổng `23.886.265` chuyến), nên câu SQL KPI chỉ còn khoảng `0,04 giây` với mọi tháng và `0,01–0,17 giây` khi lọc một tháng và một borough, kết quả giữ nguyên. Bảng đa nguồn ban đầu phải nối toàn bộ fact với thời tiết và lịch, mất khoảng `1–2,7 phút` khi lọc một tháng và vượt giới hạn `180 giây` khi không lọc. Dự án xử lý bằng Materialized View `analytics.mv_multisource_summary` (`10.147` dòng, dựng lại trong `~32 giây` bằng `load_postgres.py --create-indexes`), giảm thời gian truy vấn xuống dưới `0,5 giây` và cho kết quả trùng khớp với truy vấn gốc.

---

### Chương 3: Xây dựng Data Pipeline ELT theo Kiến trúc Medallion bằng PySpark
3.1. **Luồng xử lý 4 tầng Medallion (`Raw -> Bronze -> Silver -> Gold`)**:
- **Tầng Raw**: Tải dữ liệu gốc từ NYC TLC, Open-Meteo ERA5 API, US Holidays và Taxi Zones kèm kiểm tra mã băm `SHA-256`.
- **Tầng Bronze**: Chuẩn hóa kiểu dữ liệu, gắn siêu dữ liệu kiểm toán (`_source_file`, `_ingestion_batch_id`).
- **Tầng Silver**: Làm sạch dữ liệu bẩn, chuẩn hóa múi giờ địa phương `America/New_York` (xử lý chính xác giờ mùa hè DST). Loại bỏ `197.119` dòng lỗi (`0,82%`) trên tổng số `24.083.384` dòng Raw, giữ lại `23.886.265` dòng sạch.
- **Tầng Gold**: Thực hiện phép nối đa nguồn (`Fact Trip` ⋈ `Dim Location` ⋈ `Dim Calendar` ⋈ `Dim Weather`). Đạt tỷ lệ khớp **100% Taxi Zones** và **99,92% Thời tiết ERA5** (`23.866.019 / 23.886.265` chuyến).

3.2. **Kết quả Phân tích Đa nguồn (Taxi × Thời tiết ERA5 × Lịch Nghỉ lễ)**:

| Nhóm Phân tích Đa nguồn | Phân loại Chi tiết | Tỷ trọng Chuyến (%) | Giá cước TB (USD) | Quãng đường TB (km) |
| :--- | :--- | ---: | ---: | ---: |
| **Theo Điều kiện Thời tiết ERA5** | 1. Trời quang / Khô ráo (`0 mm/h`) | `81,2%` | `19,43 USD` | `5,48 km` |
| **Theo Điều kiện Thời tiết ERA5** | 2. Mưa nhỏ & Mưa vừa (`< 2 mm/h`) | `15,1%` | `19,89 USD` | `5,41 km` |
| **Theo Điều kiện Thời tiết ERA5** | 3. Mưa lớn / Có tuyết (`>= 2 mm/h`) | `3,7%` | `18,76 USD` | `5,00 km` |
| **Theo Điều kiện Thời tiết ERA5** | 4. Không có dữ liệu thời tiết (`20.246` chuyến tối 30/06) | `0,1%` | `20,01 USD` | `6,50 km` |
| **Theo Lịch Nghỉ lễ (US Calendar)** | 1. Ngày làm việc (`Thứ 2 – Thứ 6`) | `68,7%` | `19,43 USD` | `5,33 km` |
| **Theo Lịch Nghỉ lễ (US Calendar)** | 2. Cuối tuần (`Thứ 7 & Chủ Nhật`) | `29,1%` | `19,51 USD` | `5,65 km` |
| **Theo Lịch Nghỉ lễ (US Calendar)** | 3. Ngày nghỉ lễ Liên bang (`US Holiday`) | `2,1%` | `20,37 USD` | `6,36 km` |
| **Theo Lịch Nghỉ lễ (US Calendar)** | 4. Ngoài lịch 2025 (ngày đón lỗi, `24` chuyến) | `0,0%` | `20,64 USD` | `6,37 km` |

Giá cước TB chỉ tính cước trong `(0; 500]` USD và quãng đường TB chỉ tính `(0; 100]` dặm. Trên toàn bộ `23.886.265` chuyến, cước TB là `19,48 USD` (`17,90 USD` nếu tính cả `1.322.979` chuyến cước âm) và quãng đường TB là `3,39 dặm` (`6,86 dặm` nếu tính cả bản ghi lỗi, lớn nhất `386.088 dặm`). Tổng số chuyến và tổng doanh thu vẫn giữ mọi bản ghi để đối soát khớp với kho dữ liệu.

3.3. **5 Chỉ số Giám sát Pipeline (DataOps & MLOps Monitoring)**:
1. **Metric 1 — Trạng thái tác vụ ETL (`Job Status`)**: `6/6` phân vùng tháng (`2025-01` → `2025-06`) đạt trạng thái `LOADED` (`100% Healthy`).
2. **Metric 2 — Thời gian xử lý (`Processing Time`)**: Tổng thời gian nạp PostgreSQL `5.184,25 giây` (trung bình `864,0 giây/tháng`); chuẩn bị mẫu ML `28,3 giây`; huấn luyện 4 mô hình Spark ML `131,0 giây`.
3. **Metric 3 — Đối soát số dòng (`Row Reconciliation`)**: `source_row_count = 23.886.265` khớp tuyệt đối `loaded_row_count = 23.886.265` (`row_delta = 0`, Zero-Loss).
4. **Metric 4 — Tỷ lệ lọc dữ liệu bẩn (`Silver Reject Rate`)**: `0,82%` (`197.119 / 24.083.384` dòng).
5. **Metric 5 — Sức khỏe Dịch vụ AI & Tài nguyên (`Serving Health & RAM`)**: Endpoint `http://fare-inference:8000/ready` phản hồi `200 OK`, mô hình đóng băng (luồng phục vụ không có bước huấn luyện), tiêu thụ `~850–930 MB / 3072 MB RAM` (container suy luận) và `~149 MB / 1024 MB RAM` (container giao diện).

---

### Chương 4: Huấn luyện & Đánh giá Mô hình Học máy Phân tán (Spark MLlib & MLflow)
4.1. **Thiết kế Đặc trưng Trước chuyến đi (Zero Data Leakage)**:
- Chỉ sử dụng thông tin biết trước khi xe lăn bánh: vùng đón `pickup_zone` và vùng trả `dropoff_zone` (biến phân loại), khoảng cách tâm vùng Haversine `zone_centroid_distance_km`, cờ nội vùng `is_same_zone`, chu kỳ giờ đón `pickup_hour_sin/pickup_hour_cos`, chu kỳ thứ trong tuần `pickup_dow_sin/pickup_dow_cos`, `is_weekend`, `is_holiday` (10 đặc trưng của `M3-nw`); `M3-w` thêm 4 biến thời tiết ERA5 `temperature_2m`, `precipitation`, `snowfall`, `wind_speed_10m` (14 đặc trưng).
- **Bảo toàn 100% vùng hoạt động**: Kỹ thuật lấy mẫu phân tầng bổ sung (`rare_zone_stratified_rows_added = 4.469`) đảm bảo tập Train phủ đủ `258/258` vùng đón và `259/259` vùng trả có phát sinh chuyến chuẩn, kết hợp fallback về toàn bộ `263/263` vùng bản đồ NYC.
- **Quy tắc nghiệp vụ Sân bay JFK ↔ Manhattan (`Ratecode 2`)**: Phát hiện `574.524 / 574.602` chuyến (`99,9864%`) tuân theo mức giá cố định `70,00 USD` của NYC TLC, được tích hợp thành quy tắc chính xác tuyệt đối trước khi gọi cây quyết định.

4.2. **Bảng So sánh Thực nghiệm 6 Cấu hình Mô hình trên Tập Kiểm thử Độc lập (Holdout Test 06/2025 — `80.770` chuyến đồng hồ chuẩn `ratecode_id = 1`; huấn luyện trên `336.864` dòng)**:

| Thứ tự | Mã Mô hình | Tên Mô hình & Cấu hình | Dùng Thời tiết ERA5 | MAE (USD) ↓ | RMSE (USD) ↓ | Độ chính xác R² ↑ | Sai số Trung vị P50 (USD) ↓ |
| :---: | :--- | :--- | :--- | :---: | ---: | ---: | ---: | ---: |
| 1 | `B0_global_median` | Trung vị toàn cục (Mốc cơ sở đơn giản) | Không | `7,90 USD` | `13,33 USD` | `-13,22%` | `4,20 USD` |
| 2 | `B0_hierarchical_median` | Thống kê theo Tuyến & Giờ (Baseline B0) | Không | `3,35 USD` | `6,51 USD` | `72,94%` | `2,10 USD` |
| 3 | `M1_linear_regression` | Hồi quy Tuyến tính (`LinearRegression` — Ridge, `regParam = 0,1`) | Có | `3,21 USD` | `4,81 USD` | `85,21%` | `2,36 USD` |
| 4 | `M2_random_forest` | Rừng Ngẫu nhiên (`RandomForestRegressor` — 30 cây) | Có | `3,12 USD` | `4,67 USD` | `86,09%` | `2,30 USD` |
| 5 | `M3_gbt_weather` | Gradient Boosted Trees + Thời tiết ERA5 (`M3-w`) | Có | `2,99 USD` | `4,60 USD` | `86,52%` | `2,14 USD` |
| **6** | **`M3_nw_gbt_no_weather`** | **GBT Tối ưu Không cần Thời tiết (`M3-nw` — Đang dùng)** | **Không** | **`2,92 USD`** | **`4,54 USD`** | **`86,85%`** | **`2,05 USD`** |

4.3. **Phạm vi của các con số**: Bảng 4.2 chỉ gồm chuyến đồng hồ chuẩn. Chấm mẫu ngẫu nhiên 5% (`197.930` chuyến) của `3.946.190` chuyến hợp lệ tháng 06/2025 bằng `M3-nw` + quy tắc JFK cho kết quả:

| Nhóm chuyến | Tỷ trọng | MAE (USD) | RMSE (USD) | R² | P50 (USD) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Mọi loại chuyến** | **`100%`** | **`4,06`** | **`8,18`** | **`75,3%`** | **`2,36`** |
| Đồng hồ chuẩn (`ratecode_id = 1`) | `69,3%` | `2,93` | `4,60` | `86,5%` | `2,04` |
| Không có ratecode (`NULL`) | `25,5%` | `5,58` | `8,25` | `50,7%` | `3,91` |
| `ratecode_id = 2`, tuyến JFK ↔ Manhattan (quy tắc 70 USD) | `2,6%` | `0,00` | `0,00` | — | `0,00` |
| `ratecode_id = 2`, tuyến khác | `0,2%` | `44,01` | `49,13` | — | `57,25` |
| Ratecode khác (`3, 4, 5, 6, 99`) | `2,3%` | `22,92` | `36,35` | `-23,3%` | `14,55` |

Nhóm `ratecode_id = 1` ra `2,93 USD`, khớp với `2,92 USD` của bảng 4.2, nên cách đo đáng tin. Sai số tăng chủ yếu vì chuyến không có ratecode và các ratecode đặc biệt nằm ngoài phạm vi huấn luyện. Tái lập bằng `scripts/eval_all_ratecodes_sample.py` và `scripts/eval_all_ratecodes_score.py`.

4.4. **Bảng Kiểm chứng Độ ổn định trên Dữ liệu Tương lai Năm 2026 (Out-of-Time Drift — `721.513` chuyến)**:

| Giai đoạn Kiểm chứng Năm 2026 | Số lượng Chuyến | MAE — Thống kê B0 (USD) | MAE — Mô hình AI Gold V3 (USD) | RMSE — Gold V3 (USD) | Độ chính xác R² — Gold V3 | P50 — Gold V3 (USD) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| **Tháng 06/2026** (`drift_2026-06`) | `268.539` | `3,97 USD` | **`3,28 USD`** | `6,31 USD` | `78,69%` | `2,16 USD` |
| **Tháng 07/2026** (`drift_2026-07`) | `235.025` | `3,48 USD` | **`3,02 USD`** | `4,78 USD` | `84,97%` | `2,10 USD` |
| **Tháng 08/2026** (`drift_2026-08`) | `217.949` | `3,36 USD` | **`2,90 USD`** | `4,52 USD` | **`87,10%`** | **`2,04 USD`** |
| **Tổng hợp 3 tháng Năm 2026** | **`721.513`** | `3,62 USD` | **`3,08 USD`** | `5,33 USD` | **`82,97%`** | **`2,11 USD`** |

Tập `721.513` chuyến năm 2026 lấy từ `.sandbox/pretrip-v2/pilot-2026-06-08-v23-01`, hiện không còn trên máy nên các số 2026 được giữ theo lần chạy gốc và chưa tái lập được.

4.5. **Kiểm chứng tính năng gợi ý giờ đi**: Gợi ý giờ đi dựa trên hai nguồn: phụ phí TLC theo giờ (chắc chắn) và chênh lệch cước lịch sử của chính tuyến đó trong 01–05/2025, lấy từ `analytics.mv_route_hour_fare`, chỉ tính khi vượt 3 lần sai số chuẩn và mỗi khung giờ có ít nhất 30 chuyến. Đường cong theo giờ của mô hình không còn được dùng để gợi ý vì chỉ đúng chiều tăng giảm giữa hai giờ liền kề `56,5%` số lần.

Kiểm chứng ngược trên 300 tuyến đông nhất tháng 06/2025 (dữ liệu chưa dùng để dựng lịch sử), so với cước trung bình thực tế theo giờ:

Chế độ "Đi ngay" (chỉ xét 1–4 giờ sau):

| Gợi ý không nhờ phụ phí | Mô hình (cách cũ) | Lịch sử tuyến (hiện tại) |
| :--- | ---: | ---: |
| Số gợi ý | `765` | `1.147` |
| Thực sự rẻ hơn | `79,9%` | `88,8%` |
| Rẻ hơn rõ ràng về thống kê | `33,3%` | `46,3%` |
| Làm tốn thêm từ 0,40 USD | `9,3%` | `3,8%` |
| Mức hứa → thực tế (USD) | `0,64 → 0,82` | `1,01 → 0,99` |
| Tương quan hứa và thực tế | `0,21` | `0,57` |

Gợi ý nhờ né phụ phí đúng `99,7%`. Kiểm tra độc lập trên mọi tuyến, với lịch sử 01–04/2025 và tháng 05/2025, cho `86,6%` rẻ hơn thực sự, nhưng mức hứa cao hơn thực tế (`1,26 → 1,04 USD`).

Chế độ "Đặt lịch" (xét 1–4 giờ trước và sau), cùng 300 tuyến và tháng 06/2025, chỉ cách hiện tại:

| Chỉ tiêu | Không nhờ phụ phí | Có né phụ phí |
| :--- | ---: | ---: |
| Số gợi ý | `2.467` | `1.720` |
| Thực sự rẻ hơn | `91,0%` | `95,5%` |
| Rẻ hơn rõ ràng về thống kê | `60,1%` | `80,5%` |
| Làm tốn thêm từ 0,40 USD | `3,4%` | `2,4%` |
| Mức hứa → thực tế (USD) | `1,46 → 1,49` | `2,01 → 2,02` |
| Tương quan hứa và thực tế | `0,83` | — |

Gợi ý né phụ phí ở chế độ này thấp hơn chế độ "Đi ngay" (`95,5%` so với `99,7%`) vì đi sớm hơn để né phụ phí đêm có thể rơi vào giờ chiều đông xe. Kiểm tra độc lập trên mọi tuyến với tháng 05/2025 cho `93,6%` gợi ý không nhờ phụ phí rẻ hơn thực sự (`1,72 → 1,83 USD`). Đây là mức trung bình theo tháng, không bảo đảm cho một chuyến cụ thể. Tái lập bằng `scripts/eval_departure_advice.py` (thêm `--mode scheduled` cho chế độ "Đặt lịch").

---

### Chương 5: Hạ tầng Triển khai Microservices, Docker, Kubernetes & Trực quan hóa BI
5.1. **Kiến trúc Microservices Đóng gói Docker**:
- 4 dịch vụ chạy độc lập: `nyc-taxi-postgres` (Kho dữ liệu OLAP), `nyc-taxi-mlflow` (Quản lý thực nghiệm ML), `nyc-taxi-bigdata-fare-inference-1` (Microservice suy luận PySpark GBT đóng băng, read-only rootfs, giới hạn 3GB RAM), và `nyc-taxi-bigdata-dashboard-1` (Ứng dụng Web Streamlit).
5.2. **Giao diện Người dùng Trực quan & Dễ hiểu (`http://127.0.0.1:8501`)**:
- **Trang 1 — Phân tích lịch sử**: Bộ lọc theo Tháng & Quận, 4 thẻ KPI tổng quan (`23.886.265` chuyến), 3 biểu đồ phân bố theo Tháng/Giờ/Quận và 2 bảng đối chiếu Đa nguồn (**Thời tiết ERA5** & **Lịch nghỉ lễ Mỹ**).
- **Trang 2 — Ước tính cước (3 Tab tinh gọn)**:
  - **Tab 1 (`🚕 Ước tính cước chuyến đi`)**: Bản đồ đặt xe tương tác 2 chiều chuẩn trải nghiệm thực tế (tương tự Uber/Grab) với 3 phương thức linh hoạt:
    1. *Click hoặc kéo thả trực tiếp ghim đón (A) và ghim đến (B) trên bản đồ tương tác NYC Mapbox*: Tự do di chuyển ghim với hiệu ứng căn khung hình tự động và gợi ý tên vùng tức thì.
    2. *Bấm 1-click chọn nhanh cụm điểm nóng*: Danh sách các hub giao thông trọng điểm (Sân bay JFK, Sân bay LaGuardia, Quảng trường Thời đại, Phố Wall, Công viên Trung tâm, Cầu Brooklyn...).
    3. *Gõ tìm kiếm địa chỉ thực tế (Mapbox Autocomplete)*: Tự động gợi ý địa chỉ sau 400ms debounce, hỗ trợ phím mũi tên và cô lập session token an toàn.
    - *Cơ chế đồng bộ 2 chiều (`external_sync`)*: Mọi thao tác chọn điểm trên bản đồ hoặc bấm chip điểm nóng lập tức đẩy dữ liệu lên thanh nhập địa chỉ và ngược lại. Tọa độ được phân giải cục bộ vào 263 vùng (*Point-in-Polygon*) siêu tốc (`<2ms`, không tốn quota API).
    - *Trực quan hóa lộ trình, Thời tiết tham khảo & Gợi ý giờ khởi hành (`ai_fare_advisor.py`)*: định giá bằng mô hình không thời tiết **`M3-nw`**; Dự báo Thời tiết NYC (Open-Meteo ERA5) tự động tra cứu theo giờ đón chỉ để tham khảo, **`M3-w`** chỉ chạy khi người dùng tự nhập thời tiết, công khai **Bảng kê Tổng giá cước kèm các khoản Phụ phí chính thức NYC TLC thực tế phát sinh** (`extra`, `congestion_surcharge`, `airport_fee`, `mta_tax`, `improvement_surcharge`) và đề xuất **khung giờ gần đó thường rẻ hơn**, dựa trên phụ phí TLC và lịch sử tuyến (xem mục 4.5).
  - **Tab 2 (`📊 Độ chính xác & So sánh Mô hình`)**: 4 thẻ KPI kết quả + 3 Sơ đồ trực quan (So sánh MAE 6 cấu hình có nhãn số trên cột; Đường Giá thực tế vs Giá AI dự đoán theo 24 giờ; Biểu đồ cột đôi Kiểm chứng Năm 2026).
  - **Tab 3 (`🩺 Giám sát Hệ thống & Dữ liệu`)**: Sơ đồ Luồng xử lý 4 bước Medallion + 5 Thẻ chỉ số vận hành + 2 Sơ đồ cột theo tháng (Sản lượng chuyến & Thời gian nạp PostgreSQL).
5.3. **Chiến lược Đảm bảo Chất lượng & Kiểm thử Tự động (Automated Quality Assurance & CI/CD)**:
- Kiểm thử toàn diện 2 tầng với tổng cộng **1.511 bài test tự động** (đạt tỷ lệ vượt qua **100% PASS**, chưa tính các bộ Pester, Spark và PostgreSQL trực tiếp chạy riêng):
  - **Tầng Backend Python (1.481 bài test)**: Thực thi qua `python scripts/ci_python.py` (Pytest + Streamlit AppTest) bao phủ pipeline PySpark Medallion, Star Schema PostgreSQL, đối soát số dòng (`row_delta = 0`), suy luận mô hình đóng băng (`fare-inference:8000`), Gợi ý giờ khởi hành (Smart Fare Advisor) và phân giải không gian Taxi Zones.
  - **Tầng Frontend Web / Mapbox (30 bài test Node.js)**: Thực thi qua `node --test tests/test_address_frontend.js tests/test_route_map_frontend.js` bao phủ tương tác bản đồ, debounce gõ địa chỉ, cô lập iframe an toàn, xử lý ghim A/B và cơ chế đồng bộ 2 chiều `external_sync`.

---

### Chương 6: Kết luận và Hướng phát triển
- Hệ thống đã giải quyết trọn vẹn bài toán tích hợp dữ liệu lớn từ `24,08 triệu` bản ghi thô đa nguồn sang kho dữ liệu phân tích Star Schema với độ chính xác đối soát tuyệt đối (`Zero-Loss`), đồng thời nâng độ chính xác dự báo giá cước trước chuyến đi từ `R² = 66,5%` (bản cũ) lên **`R² = 86,85%`** trên chuyến đồng hồ chuẩn của tập Holdout 2025 (`R² = 75,3%` trên mọi loại chuyến) và duy trì **`R² = 82,97%`** trên `721.513` chuyến của năm 2026.
- Giao diện người dùng được nâng cấp lên trải nghiệm đặt xe 2 chiều hiện đại tương đương các ứng dụng gọi xe thương mại (Uber/Grab), hoàn thiện toàn diện cả về mặt kỹ thuật kỹ thuật dữ liệu lớn, học máy phân tán, và sản phẩm phần mềm thực tế sẵn sàng chuyển giao.
