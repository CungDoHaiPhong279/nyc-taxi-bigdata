# Airflow Orchestration DAG (`nyc_taxi_medallion_elt_and_ml_v3`)

Thư mục này chứa DAG điều phối toàn trình [`nyc_taxi_elt_ml_dag.py`](nyc_taxi_elt_ml_dag.py) theo kiến trúc Medallion ELT + Spark ML Gold V3:

1. `acquire_raw_data`: Tải và kiểm chứng dữ liệu gốc (`scripts/download_raw_data.py --all --skip-existing`).
2. `build_bronze_layer`: Chuẩn hóa schema và nạp tầng Bronze (`scripts/build_bronze.py --all`).
3. `build_silver_layer`: Làm sạch, khử trùng lặp và kiểm định chất lượng tầng Silver (`scripts/build_silver.py --all`).
4. `build_gold_layer`: Tích hợp đa nguồn Taxi × Thời tiết ERA5 × Lịch 2025 × Vùng TLC (`scripts/build_gold.py --all`).
5. `load_postgres_serving_mart`: Nạp Star Schema vào PostgreSQL Serving Mart (`scripts/load_postgres.py --all --create-indexes --validate`).
6. `prepare_gold_v3_ml_splits`: Trích mẫu phân tầng và tạo đặc trưng học máy (`scripts/prepare_gold_v3.py --execute --overwrite`).
7. `train_spark_ml_v3`: Huấn luyện 5 cấu hình mô hình Spark ML và ghi log MLflow (`scripts/train_gold_v3.py`).
8. `write_gold_v3_predictions_to_postgres`: Ghi kết quả dự báo và bảng chỉ số vào PostgreSQL (`scripts/write_gold_v3_predictions.py`).

## Biến môi trường hỗ trợ
- `NYC_TAXI_APP_DIR`: Đường dẫn gốc của dự án trong container (mặc định `/app`).
- `MLFLOW_TRACKING_URI`: Địa chỉ máy chủ MLflow (mặc định `http://mlflow:5000`).
