# Đề cương Báo cáo Tổng kết & Bảo vệ Cuối kỳ — Học phần "Tích hợp và Phân tích Dữ liệu Lớn"

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
| **Phân tích Chuyên sâu & Học máy Phân tán (Spark MLlib + MLflow)**:<br>- Huấn luyện trực tiếp từ dữ liệu **Gold 2025 đã tích hợp Thời tiết ERA5** (`gold-v3`).<br>- So sánh đa mô hình (`Baseline B0`, `LinearRegression M1`, `RandomForest M2`, `GBTRegressor M3` có thời tiết & `M3-nw` không thời tiết).<br>- Đánh giá khả năng tổng quát hóa sang năm hiện tại **2026**. | - Chuẩn bị dữ liệu Gold V3: `src/nyc_taxi/ml/gold_v3_data.py`, `scripts/prepare_gold_v3.py`<br>- Huấn luyện Spark ML & MLflow: `src/nyc_taxi/ml/gold_v3_training.py`, `scripts/train_gold_v3.py`<br>- Báo cáo kết quả: `reports/GOLD_V3_TRAINING_REPORT.md`<br>- MLflow Experiment: `nyc-taxi-fare-gold-v3` (ID `2`, 5 runs). | - Phủ **100% Zone hoạt động** trong tập Train (`258/258` Pickup, `259/259` Dropoff) trên mẫu `507.259` dòng.<br>- Quy tắc cố định JFK ↔ Manhattan (`ratecode_id = 2`): **`99,9864%`** khớp **\$70,00 USD** (`574.524 / 574.602` chuyến).<br>- Tập Test 06/2025: **MAE `= $2,92 USD`**, **$R^2 = 0,8685$** (cải thiện `R² +30,6%` so với V2.2 cũ).<br>- Kiểm tra năm 2026 (`721.513` chuyến): **MAE `= $3,08 USD`**, **$R^2 = 0,8297$**. |
| **Bài giảng Hạ tầng Ứng dụng Dữ liệu (Microservices, Docker, CI/CD, Kubernetes)**:<br>- Tách biệt **Data Processing (`worker`)**, **Model Serving (`fare-inference`)**, **Web UI (`dashboard`)**, **Tracking (`mlflow`)**, **OLAP DB (`postgres`)**.<br>- Đóng gói Container, kiểm thử tự động CI/CD và cấu hình **Kubernetes (Deployment, Service, HPA, Probes)**. | - Docker & Compose: `Dockerfile.spark`, `Dockerfile.inference`, `Dockerfile.dashboard`, `docker-compose.yml`, `docker-compose.gold-v3.yml`<br>- Suy luận Microservice: `src/nyc_taxi/ml/inference_server.py`, `src/nyc_taxi/ml/gold_v3_inference.py`<br>- Giao diện Streamlit: `src/nyc_taxi/dashboard/gold_v3_page.py`<br>- Kubernetes Manifest: `k8s/fare-inference-deployment.yaml` | - 4 container chạy độc lập với Healthcheck (`/health`, `/ready`, `pg_isready`, `_stcore/health`).<br>- Gói mô hình đóng băng `distribution/models/gold-v3/` (`79` file, kiểm chứng SHA-256).<br>- Hướng dẫn tích hợp trang mới trên Power BI: `reports/POWER_BI_MODEL_PAGE_GUIDE.md`. |

---

## 2. Cấu trúc Đề xuất cho Quyển Báo cáo Tổng kết Cuối kỳ (6 Chương)

### Chương 1: Tổng quan Đề tài và Bài toán Tích hợp Dữ liệu Lớn
1.1. Bối cảnh bài toán giao thông đô thị New York (NYC TLC) và vai trò của dữ liệu khí tượng (ERA5).
1.2. Đặc trưng 5V của dữ liệu trong dự án:
- **Volume**: `24,08 triệu` bản ghi chuyến đi đầu vào (`23,88 triệu` dòng hợp lệ ở tầng Silver/Gold/PostgreSQL, `~3,8 GB` Parquet/PostgreSQL).
- **Variety**: Dữ liệu giao dịch chuyến đi (Parquet), dữ liệu khí tượng theo giờ (JSON/API ERA5), dữ liệu không gian ranh giới 263 vùng (GeoJSON/CSV), dữ liệu lịch nghỉ lễ liên bang (JSON).
- **Velocity & Veracity**: Xử lý nhiễu (`197.119` bản ghi lỗi thời gian đảo ngược ở Silver `0,82%`, cộng thêm lọc các bản ghi âm tiền, tọa độ ngoài vùng, đồng hồ nhảy số `\$863.372` khi chuẩn bị cohort ML Gold V3).
- **Value**: Phân tích BI đa chiều và dự báo giá cước trước chuyến đi (Pre-trip Fare Estimation).

### Chương 2: Thiết kế Kiến trúc Lưu trữ Lai (Hybrid Data Lakehouse & Star Schema)
2.1. Lý do lựa chọn kiến trúc kết hợp **Parquet Data Lake** + **PostgreSQL Serving Mart** (Liên hệ Bài giảng 3 & Báo cáo Giữa kỳ).
2.2. Thiết kế lược đồ **Star Schema** trong schema `analytics`:
- Bảng sự kiện trung tâm: `analytics.fact_trip` (`23.886.265` dòng) và bảng dự báo `analytics.fact_fare_prediction` (`170.395` dòng).
- Các bảng chiều: `analytics.dim_location` (`265` dòng), `analytics.dim_weather` (`4.344` dòng), `analytics.dim_calendar` (`365` dòng).
2.3. Tối ưu hóa truy vấn với Deferred Indexing, `GROUPING SETS` và Chiến lược Sao lưu (`scripts/backup.ps1`).

### Chương 3: Xây dựng Data Pipeline ELT theo Kiến trúc Medallion bằng PySpark
3.1. Luồng xử lý 4 tầng (`Raw -> Bronze -> Silver -> Gold`) và cơ chế kiểm soát Idempotency qua Manifest SHA-256.
3.2. Quy trình làm sạch ở tầng Silver và tích hợp đa nguồn (Taxi + Weather + Calendar + Zones) ở tầng Gold.
3.3. Thiết kế DAG điều phối trên Apache Airflow (`airflow/dags/nyc_taxi_elt_ml_dag.py`) và hệ thống **5 Metrics Giám sát Pipeline** (`src/nyc_taxi/monitoring/metrics.py`).

### Chương 4: Huấn luyện & Đánh giá Mô hình Học máy Phân tán (Spark MLlib & MLflow)
4.1. Thiết kế tập đặc trưng Pre-trip không rò rỉ dữ liệu tương lai (`zone_centroid_distance_km` Haversine, mã hóa chu kỳ thời gian `sin/cos`, lịch nghỉ lễ, biến thời tiết ERA5) và kỹ thuật lấy mẫu bảo toàn 100% Zone hiếm (`min_train_zone_support = 30`).
4.2. Xử lý quy tắc nghiệp vụ Sân bay JFK ↔ Manhattan (`ratecode_id = 2`, giá cố định `\$70,00 USD`).
4.3. Kết quả thực nghiệm so sánh 5 cấu hình (`B0`, `M1 LinearRegression`, `M2 RandomForest`, `M3 GBT + Weather`, `M3-nw GBT No Weather`) được quản lý trên **MLflow**.
4.4. Phân tích Ablation Thời tiết ERA5 và Kiểm tra Độ trôi Dữ liệu sang năm 2026 (`721.513` chuyến).

### Chương 5: Hạ tầng Triển khai Microservices, Docker, Kubernetes & Trực quan hóa BI
5.1. Kiến trúc tách biệt Microservices (`postgres`, `mlflow`, `fare-inference`, `dashboard`) so với Monolith (Liên hệ Bài giảng Hạ tầng Ứng dụng Dữ liệu).
5.2. Thiết kế bản kê khai Kubernetes (`k8s/fare-inference-deployment.yaml`) với `livenessProbe`, `readinessProbe`, giới hạn tài nguyên và `HorizontalPodAutoscaler`.
5.3. Hệ thống trực quan hóa kép: **Power BI Desktop** (`NYC_Taxi_Dashboard.pbix` + `vw_fare_prediction_enriched`) và **Streamlit Web Application** (`gold_v3_page.py`).

### Chương 6: Kết luận và Hướng phát triển
6.1. Tổng kết các kết quả đạt được (Pipeline hoàn chỉnh 23,88 triệu chuyến, mô hình đạt `$R^2 = 0,8685$`, sai số trung vị `P50 = $2,05 USD`).
6.2. Hướng mở rộng: Streaming thời gian thực với Apache Kafka / Spark Structured Streaming và tích hợp thời tiết dự báo trực tuyến.
