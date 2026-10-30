# 🚕 NYC Taxi Big Data — Medallion Lakehouse & Pre-trip Fare AI

Đồ án cuối kỳ học phần **Tích hợp và Phân tích Dữ liệu Lớn** (Khoa CNTT — Đại học Phenikaa).  
Hệ thống tích hợp **24,08 triệu chuyến xe NYC Yellow Taxi (2025)** với **Thời tiết khí tượng ERA5**, **Lịch nghỉ lễ Liên bang Mỹ** và **Bản đồ 263 vùng Taxi Zones** theo kiến trúc **Medallion (`Raw -> Bronze -> Silver -> Gold`)**, lưu trữ lai (**Parquet Data Lake + PostgreSQL 16 Star Schema**) và phục vụ dự đoán giá cước trước chuyến đi bằng **Spark MLlib (`GBTRegressor Gold V3`) + Mapbox**.

---

## 1. Kiến trúc Tổng thể

```text
[Dữ liệu Đa nguồn: NYC Yellow Taxi (24.08M) + ERA5 Weather + US Holidays + 263 Taxi Zones]
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
                     └─ Trang 2: Ước tính cước (Mapbox + Đánh giá ML + 5 Metrics)
```

---

## 2. Kết quả Thực nghiệm Cốt lõi

### 2.1. Đối soát Dữ liệu Lớn & Vận hành Pipeline (5 Metrics)
| Chỉ số Giám sát | Kết quả Thực nghiệm | Ghi chú Kiểm chứng |
| :--- | :--- | :--- |
| **1. Trạng thái ETL (`Job Status`)** | **`6/6 PASS (100%)`** | Nạp thành công cả 6 phân vùng tháng (`2025-01` → `2025-06`) + 3 bảng chiều. |
| **2. Thời gian xử lý (`Processing Time`)** | **`864 giây / tháng`** | Tổng nạp PostgreSQL `5.184s`; chuẩn bị mẫu ML `28,3s`; huấn luyện 5 mô hình `100,2s`. |
| **3. Đối soát số dòng (`Reconciliation`)** | **`23.886.265 dòng`** | Khớp tuyệt đối giữa Silver/Gold Parquet và `analytics.fact_trip` (`Chênh lệch = 0`). |
| **4. Tỷ lệ lọc dữ liệu bẩn (`Reject Rate`)** | **`0,82%` (`197.119` dòng)** | Lọc từ `24.083.384` dòng Raw; khớp **100% Taxi Zones** và **99,92% Thời tiết ERA5**. |
| **5. Dịch vụ AI & RAM (`Serving Health`)** | **`READY (200 OK)`** | Mô hình đóng băng (`fit_calls = 0`), RAM `~709 / 3072 MB` (`fare-inference`). |

### 2.2. So sánh 5 Mô hình Học máy (Holdout Test 06/2025 — `80.770` chuyến) & Kiểm chứng Năm 2026 (`721.513` chuyến)
| Mô hình | Dùng Thời tiết | MAE 2025 (USD) ↓ | RMSE 2025 (USD) ↓ | R² 2025 (%) ↑ | P50 2025 (USD) ↓ | MAE Năm 2026 (USD) ↓ |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: |
| **1. Trung vị toàn cục (`B0_global`)** | Không | `7,90` | `11,76` | `11,76%` | `5,60` | `8,54` |
| **2. Thống kê Tuyến & Giờ (`Baseline B0`)** | Không | `3,35` | `6,51` | `72,94%` | `2,10` | `3,53` |
| **3. Hồi quy Tuyến tính (`M1 - LR`)** | Có | `3,21` | `4,81` | `85,21%` | `2,33` | — |
| **4. Rừng Ngẫu nhiên (`M2 - RF`)** | Có | `3,12` | `4,67` | `86,09%` | `2,24` | — |
| **5. GBT + Thời tiết ERA5 (`M3-w`)** | Có | `2,99` | `4,60` | `86,52%` | `2,11` | — |
| **6. GBT Tối ưu (`M3-nw` — Đang dùng)** | **Không** | **`2,92`** | **`4,54`** | **`86,85%`** | **`2,05`** | **`3,08` (`R² = 83,0%`)** |

> **Quy tắc nghiệp vụ Sân bay JFK ↔ Manhattan (`Ratecode 2`)**: Kiểm chứng trên `574.602` chuyến cho thấy `99,9864%` chuyến có giá cố định **70,00 USD**, được tích hợp trực tiếp vào dịch vụ suy luận.

---

## 3. Cấu trúc Thư mục Chính

```text
nyc-taxi-bigdata/
├── airflow/dags/                  # DAG điều phối pipeline ELT & ML hàng tháng
├── assets/taxi_zones/             # Bản đồ ranh giới 263 vùng NYC TLC (GeoJSON)
├── config/                        # Cấu hình hệ thống, hợp đồng đặc trưng ML & 5 Metrics
├── dashboard/app.py               # Entrypoint ứng dụng Web Streamlit
├── distribution/models/gold-v3/   # Gói mô hình Spark GBT Gold V3 đóng băng (kèm SHA-256)
├── k8s/                           # Bản kê khai Kubernetes (Deployment, Service, HPA)
├── reports/
│   ├── FINAL_REPORT_OUTLINE.md    # Nội dung chi tiết 6 Chương Báo cáo Cuối kỳ
│   ├── GOLD_V3_TRAINING_REPORT.md # Báo cáo thực nghiệm 5 mô hình ML & Drift 2026
│   ├── POWER_BI_MODEL_PAGE_GUIDE.md
│   └── NYC_Taxi_Dashboard.pbix    # File báo cáo Power BI Desktop
├── scripts/                       # Các script chạy ELT (Raw/Bronze/Silver/Gold), nạp DB & ML
├── sql/                           # DDL tạo Star Schema, Indexes & Views trong PostgreSQL
├── src/nyc_taxi/
│   ├── common/                    # Quản lý cấu hình & tiện ích dùng chung
│   ├── ingestion/                 # Thu thập dữ liệu Taxi, ERA5 Weather, Calendar, Zones
│   ├── etl/                       # Xử lý PySpark Medallion (Bronze -> Silver -> Gold)
│   ├── database/                  # Kết nối và nạp dữ liệu vào PostgreSQL
│   ├── ml/                        # Chuẩn bị mẫu, huấn luyện Spark ML & HTTP Inference Server
│   ├── monitoring/                # Bộ thu thập 5 chỉ số giám sát vận hành Pipeline
│   └── dashboard/                 # Giao diện Streamlit (Mapbox Address, Đánh giá ML, Giám sát)
└── tests/                         # Bộ kiểm thử tự động (Pytest, AppTest, Pester, Node DOM)
```

---

## 4. Hướng dẫn Khởi chạy Nhanh (Quick Start)

### 4.1. Khởi chạy Hệ thống 4 Containers (PostgreSQL + MLflow + AI Inference + Streamlit UI)
Yêu cầu: Đã cài đặt **Docker Desktop** và cấu hình biến môi trường trong file `.env` (sao chép từ `.env.example`, thêm `MAPBOX_ACCESS_TOKEN` nếu dùng gợi ý địa chỉ Mapbox).

```powershell
# 1. Khởi động Kho dữ liệu PostgreSQL và MLflow
docker compose up -d postgres mlflow

# 2. Khởi động Microservice Suy luận Gold V3 và Giao diện Web Streamlit
docker compose -f docker-compose.yml -f docker-compose.inference.yml -f docker-compose.gold-v3.yml up -d fare-inference dashboard
```

Sau khi khởi động:
- **Ứng dụng Web Streamlit**: Mở `http://127.0.0.1:8501`
  - **Trang "Phân tích lịch sử"**: Thống kê 23,88 triệu chuyến xe theo Tháng, Giờ, Quận và Bảng đối chiếu Đa nguồn (Thời tiết ERA5 & Lịch nghỉ lễ).
  - **Trang "Ước tính cước" (3 Tab)**:
    1. `🚕 Ước tính cước chuyến đi`: Nhập địa chỉ đón/đến tại NYC (Mapbox), vẽ lộ trình bản đồ, dự đoán giá cước Gold V3 kèm tùy chọn thử nghiệm Thời tiết ERA5.
    2. `📊 Độ chính xác & So sánh Mô hình`: 3 biểu đồ trực quan so sánh 5 mô hình, đường dự đoán 24h và kiểm định thực tế Năm 2026.
    3. `🩺 Giám sát Hệ thống & Dữ liệu`: Sơ đồ luồng Medallion 4 bước và 5 chỉ số giám sát vận hành.
- **MLflow UI**: Mở `http://127.0.0.1:5000`
- **Kiểm tra trạng thái AI Server**:
  ```powershell
  docker exec nyc-taxi-bigdata-dashboard-1 python -c "import urllib.request; print(urllib.request.urlopen('http://fare-inference:8000/ready').read().decode())"
  ```

### 4.2. Chạy Pipeline ELT & Huấn luyện Mô hình từ Dữ liệu Thô (Tùy chọn)
```powershell
# Tải dữ liệu thô & chạy pipeline Medallion (Raw -> Bronze -> Silver -> Gold -> PostgreSQL)
python scripts/download_raw_data.py
python scripts/build_bronze.py
python scripts/build_silver.py
python scripts/build_gold.py
python scripts/load_postgres.py

# Trích xuất mẫu phân tầng, huấn luyện 5 mô hình Gold V3 & ghi kết quả vào PostgreSQL
python scripts/prepare_gold_v3.py
python scripts/train_gold_v3.py
python scripts/write_gold_v3_predictions.py
python scripts/package_gold_v3_bundle.py
```

### 4.3. Sao lưu Cơ sở dữ liệu & Chạy Kiểm thử Tự động (CI)
```powershell
# Sao lưu PostgreSQL (Schema + Metadata hoặc Full Dump)
.\scripts\backup.ps1

# Chạy bộ kiểm thử Python CI Offline (1.441 bài test)
python scripts/ci_python.py
```

---

## 5. Tài liệu Chi tiết trong Dự án
- **Đề cương & Nội dung Báo cáo Cuối kỳ (6 Chương)**: [`reports/FINAL_REPORT_OUTLINE.md`](reports/FINAL_REPORT_OUTLINE.md)
- **Báo cáo Huấn luyện & Đánh giá Mô hình Gold V3**: [`reports/GOLD_V3_TRAINING_REPORT.md`](reports/GOLD_V3_TRAINING_REPORT.md)
- **Hướng dẫn Tích hợp Power BI Desktop**: [`reports/POWER_BI_MODEL_PAGE_GUIDE.md`](reports/POWER_BI_MODEL_PAGE_GUIDE.md)
