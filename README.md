# NYC Taxi Big Data

Project cuối kỳ môn **Tích hợp và phân tích dữ liệu lớn**, xây dựng nền tảng cho
pipeline tích hợp dữ liệu chuyến taxi New York với thời tiết, lịch/ngày lễ và
taxi-zone lookup để hướng tới bài toán dự đoán `fare_amount`.

**STEP 1–6 đã hoàn tất:** project có dữ liệu Raw thật cho 01–06/2025, 9 output
Bronze, 9 output Silver, 6/6 partition Gold và PostgreSQL analytical data mart
đã nạp, kiểm chứng 23.886.265 chuyến. Model training và Power BI dashboard
chưa thực hiện.

## 1. Kiến trúc hiện tại và định hướng

```text
NYC Taxi + Weather + Calendar/Holiday + Taxi Zone Lookup
                            |
                     Batch ingestion
                            |
                         PySpark
                            |
              Bronze -> Silver -> Gold
                            |
                 PostgreSQL (STEP 6)
                            |
               ML + MLflow (dự kiến)
                            |
                   Power BI (đề xuất)

                 Airflow orchestration (sau này)
```

Vai trò các tầng dữ liệu:

- **Raw:** file nguồn chưa thay đổi.
- **Bronze:** Parquet giữ nguyên các cột và giá trị nguồn, bổ sung metadata ingestion
  để truy vết Raw → Bronze.
- **Silver:** loại trùng, xử lý thiếu, ép kiểu, kiểm tra range và chuẩn hóa.
- **Gold:** kết hợp Taxi + Weather + Calendar + Taxi Zone để phân tích/ML.

## 2. Cấu trúc thư mục

```text
nyc-taxi-bigdata/
|-- .env.example
|-- .gitignore
|-- README.md
|-- requirements.txt
|-- pyproject.toml
|-- docker-compose.yml
|-- config/
|   `-- config.yaml
|-- data/
|   |-- raw/
|   |   |-- taxi/
|   |   |-- weather/
|   |   |-- calendar/
|   |   `-- taxi_zones/
|   |-- bronze/
|   |-- silver/
|   `-- gold/
|-- notebooks/
|-- src/nyc_taxi/
|   |-- common/
|   |-- ingestion/
|   |-- etl/
|   |-- features/
|   |-- database/
|   `-- ml/
|-- sql/
|-- scripts/
|-- tests/
|-- airflow/dags/
|-- models/
|-- logs/
`-- reports/
```

Các file dữ liệu, log, model artifact và `mlruns/` được loại khỏi Git. `.gitkeep`
chỉ dùng để giữ lại cấu trúc thư mục rỗng.

## 3. Công nghệ

- Python 3.11, PySpark, Pandas, PyArrow, NumPy
- PostgreSQL, SQLAlchemy và Psycopg 3
- Scikit-learn và MLflow
- Docker Compose, Jupyter, pytest
- Power BI ở giai đoạn trực quan hóa

Không dùng Kafka, Flink, Kubernetes hoặc cloud service trong phạm vi hiện tại.

PySpark được giữ ở `4.2.0`; Pandas dùng range `>=2.2,<3.0` và PyArrow `>=18` theo
ma trận dependency của Spark SQL. MLflow Python client dùng cùng phiên bản `3.5.1`
với Docker image để tránh lệch giao thức client/server. Các thư viện còn lại không
bị pin cứng ở bước bootstrap.

## 4. Điều kiện cần

Trên Windows cần cài:

- Python 3.11 (kiểm tra bằng `python --version`)
- Java/JDK 17 trở lên cho PySpark hiện hành; đặt `JAVA_HOME`
- Docker Desktop với Docker Compose v2
- Git và VS Code (khuyến nghị)
- Power BI Desktop chỉ cần ở giai đoạn dashboard

## 5. Setup trên Windows (PowerShell)

Mở PowerShell tại thư mục project, sau đó tạo và kích hoạt môi trường ảo:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install -e . --no-deps
```

Lệnh cuối cài package local ở chế độ editable để `nyc_taxi` import được từ
notebook, terminal và test trong khi code vẫn nằm trong layout `src/`.

Nếu PowerShell chặn activation script, có thể cho phép trong đúng phiên hiện tại:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Tạo file cấu hình local từ mẫu:

```powershell
Copy-Item .env.example .env
```

Giá trị `change_me` chỉ là placeholder dùng cho local. Có thể thay trong `.env`;
file `.env` đã được ignore và không được commit.

## 6. Khởi động PostgreSQL và MLflow

Kiểm tra cấu hình Compose rồi chạy hai service:

```powershell
docker compose config
docker compose up -d
docker compose ps
```

PostgreSQL chạy tại `localhost:5432`. Trong lần tạo volume đầu tiên, container tự
chạy các file trong `sql/` để tạo schema `analytics`. Các file này không chạy
lại khi volume đã tồn tại; loader kiểm tra compatibility trước khi nạp dữ liệu.

Kiểm tra PostgreSQL:

```powershell
docker compose exec postgres pg_isready -U nyc_taxi -d nyc_taxi
docker compose exec postgres psql -U nyc_taxi -d nyc_taxi -c "\dt analytics.*"
```

Nếu đã đổi user/database trong `.env`, thay các giá trị tương ứng trong lệnh.
Các SQL init script chỉ tự chạy khi volume PostgreSQL còn mới.

MLflow dùng SQLite backend và Docker volume local cho metadata/artifact ở bước
bootstrap. Kiểm tra health endpoint:

```powershell
Invoke-WebRequest -UseBasicParsing http://localhost:5000/health
```

Giao diện MLflow: `http://localhost:5000`.

Tắt service khi không dùng:

```powershell
docker compose down
```

Lệnh này giữ nguyên named volumes và dữ liệu local của hai service.

## 7. Kiểm tra Python và PySpark

Script sau kiểm tra Python 3.11, config, thư mục, import PySpark, khả năng tạo local
Spark Session và kết nối PostgreSQL:

```powershell
python scripts/check_environment.py
```

PostgreSQL chưa chạy chỉ tạo cảnh báo, không khiến script thất bại. Thiếu Java,
PySpark, config hoặc thư mục bắt buộc sẽ khiến script trả mã lỗi.

## 8. Tạo calendar dataset nhỏ

Đây là generator có thể chạy thật và không cần download dữ liệu:

```powershell
python scripts/create_calendar.py
```

Kết quả: `data/raw/calendar/calendar_2025.csv`, gồm ngày từ `2025-01-01` đến
`2025-12-31`. `is_holiday` ở Raw luôn là `False`; holiday enrichment được thực
hiện ở Silver. File CSV sinh ra được `.gitignore` loại khỏi Git.

Có thể truyền range/output khác:

```powershell
python scripts/create_calendar.py --start 2025-01-01 --end 2025-12-31 --output data/raw/calendar/calendar_2025.csv
```

## 9. Chạy test

```powershell
pytest
```

Test gồm smoke test và các unit test offline cho Raw/Bronze/Silver, calendar,
metadata, idempotency, manifest và PostgreSQL loader; mặc định không kết nối
production database. Integration test PostgreSQL chỉ chạy khi thiết lập rõ
`NYC_TAXI_RUN_POSTGRES_INTEGRATION=1` và `NYC_TAXI_TEST_POSTGRES_URL` trỏ tới
database thử nghiệm riêng có tên chứa `_test`, chưa có schema `analytics`;
fixture nhỏ được rollback cuối test. Ví dụ đặt URL test trong biến môi trường
rồi chạy `pytest -q tests/test_postgres_integration.py`; không dùng database
production. Trên
Windows thiếu `winutils.exe`, integration test ghi Parquet có thể được skip;
để chạy đầy đủ trong Docker, thay phần `python scripts/build_bronze.py ...`
của lệnh `docker run` ở mục Bronze bằng `python -m pytest -q -p no:cacheprovider`.

## 10. Data Sources

Tải và kiểm tra dữ liệu raw bằng `python scripts/download_raw_data.py`. Mặc định script
lấy Yellow Taxi từ tháng 01 đến tháng 06/2025, taxi zone lookup, thời tiết cùng kỳ,
và kiểm tra calendar đã có. Chạy lại không ghi đè file hợp lệ; dùng `--force` khi
muốn tải lại. Để thử một tháng taxi trước:

```powershell
python scripts/download_raw_data.py --taxi-start-month 2025-01 --taxi-end-month 2025-01
```

| Dataset | Nguồn và phạm vi | Định dạng gốc | Đường dẫn local |
| --- | --- | --- | --- |
| Yellow Taxi Trip Data | [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), 2025-01 đến 2025-06 | Parquet, một file/tháng | `data/raw/taxi/yellow_tripdata_2025-MM.parquet` |
| Taxi Zone Lookup | [NYC TLC Taxi Zone Lookup Table](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), bảng tra cứu địa điểm | CSV | `data/raw/taxi_zones/taxi_zone_lookup.csv` |
| Weather NYC | [Open-Meteo Historical Weather API](https://open-meteo.com/en/docs/historical-weather-api), ERA5 tại 40.7128, -74.0060, 2025-01-01 đến 2025-06-30 | JSON gốc, theo giờ | `data/raw/weather/weather_nyc_2025-01_2025-06.json` |
| Calendar | Tạo bởi `scripts/create_calendar.py`, 2025-01-01 đến 2025-12-31 | CSV | `data/raw/calendar/calendar_2025.csv` |

Sáu file taxi được tải trực tiếp từ các liên kết chính thức của TLC, dạng
`https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-MM.parquet`.
TLC lưu ý dữ liệu do các nhà cung cấp gửi và không bảo đảm độ chính xác; schema
có thể thay đổi. [NYC Open Data FAQ](https://www.nyc.gov/opendata/get-started/FAQs)
nêu rằng dữ liệu mở không bị hạn chế sử dụng.

Weather giữ nguyên tên trường API: `time`, `temperature_2m`, `precipitation`,
`snowfall`, `wind_speed_10m`, `weather_code`. Yêu cầu API dùng UTC (phản hồi ghi
`timezone: GMT`); timestamp chưa chuyển sang giờ New York ở tầng raw. ERA5 là dữ
liệu tái phân tích từ mô hình và quan trắc,
không phải số đo của một trạm taxi. [Điều khoản Open-Meteo](https://open-meteo.com/en/terms)
cho phép Free API dùng cho mục đích phi thương mại; dữ liệu theo CC BY 4.0 và
cần ghi nguồn. API không cần key cho trường hợp này.

Calendar có thể tái tạo bằng `python scripts/create_calendar.py`; cột `is_holiday`
ở Raw luôn là `False`, còn Silver bổ sung ngày lễ từ lịch OPM. Kết quả kiểm tra
và thông tin nguồn được ghi trong `data/raw/raw_manifest.json`; file dữ liệu lớn
không commit.

## 11. Bronze Layer

Raw là các file nguồn ở định dạng gốc, giữ nguyên để có thể kiểm tra và tái xử lý.
Bronze là bản Parquet nén Snappy, giữ nguyên cột và giá trị nghiệp vụ từ Raw,
thêm metadata kỹ thuật (`_ingested_at` theo UTC, `_source_file`,
`_source_dataset`, `_ingestion_batch_id`; Taxi còn có `_source_year` và
`_source_month`). Weather JSON có các mảng theo giờ nên được chuyển thành một
record cho mỗi timestamp, vẫn giữ thời gian GMT/UTC gốc. Bronze **chưa phải dữ
liệu đã làm sạch**: không lọc chuyến, sửa giá trị, đổi múi giờ, gắn nhãn thời
tiết hay kết hợp các nguồn. Với Calendar, Spark đọc ô `holiday_name` rỗng trong
CSV thành `null` ở Bronze; file Raw không đổi và không có holiday enrichment.

Chạy từ thư mục project sau khi cài môi trường Python:

```powershell
python scripts/build_bronze.py --dataset taxi --month 2025-01
python scripts/build_bronze.py --dataset taxi --start-month 2025-02 --end-month 2025-06
python scripts/build_bronze.py --dataset weather
python scripts/build_bronze.py --dataset taxi-zones
python scripts/build_bronze.py --dataset calendar
```

Lệnh đầu tiên xử lý và kiểm tra riêng Taxi tháng 01 trước khi chạy các tháng
còn lại, mỗi tháng một lượt để phù hợp máy local. Có thể dùng `--all` để chạy
tất cả nguồn. Mặc định, output đã có sẽ được bỏ qua, tránh ghi lặp dữ liệu;
thêm `--force` để xây dựng lại output của nguồn hoặc tháng đã chọn.

Nếu Spark local trên Windows gặp lỗi khi ghi Parquet, có thể chạy cùng CLI trong
Docker mà không cần cài `winutils.exe`:

```powershell
docker build -f Dockerfile.spark -t nyc-taxi-spark:step3 .
docker run --rm --network none --hostname bronze-local --add-host bronze-local:127.0.0.1 --cpus 4 --memory 8g --mount "type=bind,source=C:\nyc-taxi-bigdata,target=/app" -w /app -e PYTHONPATH=/app/src -e SPARK_LOCAL_IP=127.0.0.1 nyc-taxi-spark:step3 python scripts/build_bronze.py --dataset taxi --month 2025-01
```

Thay `C:\nyc-taxi-bigdata` bằng đường dẫn tuyệt đối đến project trên
máy của bạn. Lệnh mount dùng cùng dữ liệu Raw và Bronze trên máy host.

Taxi được ghi vào `data/bronze/taxi/year=2025/month=MM/`; các nguồn khác vào
`data/bronze/weather/`, `data/bronze/taxi_zones/` và `data/bronze/calendar/`.
`data/bronze/bronze_manifest.json` ghi đường dẫn Raw và Bronze, số dòng, schema,
metadata batch và trạng thái validation để truy vết từng file nguồn đến output.
Ví dụ:

```text
data/raw/taxi/yellow_tripdata_2025-01.parquet
    → data/bronze/taxi/year=2025/month=01/
```

Bronze Parquet được Git ignore; manifest nhỏ có thể commit. Cả 9 output đã được
kiểm chứng về số dòng Raw/Bronze và cột nguồn: sáu tháng Taxi, weather 4.344
dòng, taxi zones 265 dòng và calendar 365 dòng.

## 12. Silver Layer

Silver đọc **Bronze**, không đọc lại Raw. Khác với Bronze giữ nguyên dữ liệu nguồn,
Silver chuẩn hóa schema, kiểm tra chất lượng, loại bản ghi không hợp lệ và trùng
lặp; không kết hợp Taxi với Weather, Zones hay Calendar (việc đó thuộc Gold).
Taxi đổi `VendorID` → `vendor_id`, `PULocationID` → `pickup_location_id`,
`DOLocationID` → `dropoff_location_id`, `RatecodeID` → `ratecode_id` và tạo
`pickup_date`/`pickup_hour` từ giờ đón để chuẩn bị khóa thời gian.
Taxi xử lý từng tháng và ghi Parquet nén Snappy vào
`data/silver/taxi/year=2025/month=MM/`; Weather theo tháng tại
`data/silver/weather/year=2025/month=MM/`; Zones và Calendar có thư mục riêng.
`data/silver/silver_manifest.json` lưu số dòng đầu vào/đầu ra, số rejected,
duplicates, lý do, schema và lineage Bronze → Silver. Parquet được Git ignore;
manifest nhỏ có thể commit. Rejected rows chỉ được lưu dưới dạng **số lượng theo
lý do** trong manifest, không lưu toàn bộ rejected records thành Parquet.

Quy tắc Taxi: loại dòng thiếu timestamp đón/trả, địa điểm đón/trả, quãng đường
hoặc giá cước; thời gian trả không sau thời gian đón; quãng đường âm; hoặc số
hành khách âm. Dedupe dựa trên toàn bộ cột nghiệp vụ nguồn, **không** dùng
ingestion metadata, và giữ một bản theo tiêu chí xác định. Ghi riêng số dòng bị
loại theo từng lý do và số duplicate. Giá cước âm hoặc bằng 0, quãng đường
bằng 0 và pickup ngoài tháng file được **giữ lại nhưng thống kê** vì chưa đủ
cơ sở nghiệp vụ để loại. Không điền khuyết tùy tiện và không cắt outlier bằng
ngưỡng tùy ý.

Timestamp Taxi trong Bronze là `timestamp_ntz` (giờ đồng hồ không gắn
timezone). [Data dictionary của NYC TLC](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
không xác nhận timezone của hai cột `tpep_*_datetime` trong Parquet công khai;
vì vậy Silver giữ nguyên giờ local-naive, không tự chuyển sang UTC hay áp offset
cố định. STEP 4.5 chọn **giả định phân tích** rằng Taxi là giờ đồng hồ
`America/New_York` để ghép theo giờ địa phương ở Gold: ngày chuyển DST
09/03/2025 có 0 pickup/dropoff trong Silver lúc 02:xx, nhưng đây không phải
bảo đảm chính thức của TLC. Xem [báo cáo STEP 4.5](reports/STEP_4_5_FINDINGS.md). Weather
nguồn UTC/GMT; Silver parse UTC rồi chuyển sang `America/New_York` bằng hàm
timezone-aware để xử lý DST (không dùng UTC−5 cố định).

Calendar Silver bổ sung **11 ngày nghỉ liên bang áp dụng toàn quốc năm 2025**
theo [lịch US Office of Personnel Management](https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/).
Inauguration Day chỉ áp dụng vùng Washington,
DC nên không thêm vào lịch NYC. Việc bổ sung là deterministic, không gọi mạng
khi chạy. Taxi Zones chỉ chuẩn hóa cột/kiểm tra khóa `location_id`, chưa join
vào Taxi.

Các lệnh CLI chạy từ thư mục project:

```powershell
python scripts/build_silver.py --dataset taxi --month 2025-01
python scripts/build_silver.py --dataset taxi --start-month 2025-02 --end-month 2025-06
python scripts/build_silver.py --dataset weather
python scripts/build_silver.py --dataset taxi-zones
python scripts/build_silver.py --dataset calendar
python scripts/build_silver.py --all
```

Chạy tháng 01 trước, kiểm tra thống kê chất lượng rồi mới chạy tháng 02–06.
Chạy lại phải idempotent; dùng `--force` để xây dựng lại partition đã chọn.
Nếu Windows thiếu `winutils.exe`, tái sử dụng `Dockerfile.spark` của STEP 3 và
chạy CLI trong Docker Linux, không tải binary không rõ nguồn:

```powershell
docker build -f Dockerfile.spark -t nyc-taxi-spark:step3 .
docker run --rm --network none --hostname silver-local --add-host silver-local:127.0.0.1 --cpus 4 --memory 8g --mount "type=bind,source=C:\nyc-taxi-bigdata,target=/app" -w /app -e PYTHONPATH=/app/src -e SPARK_LOCAL_IP=127.0.0.1 nyc-taxi-spark:step3 python scripts/build_silver.py --dataset taxi --month 2025-01
```

Thay `C:\nyc-taxi-bigdata` bằng đường dẫn project thực tế nếu cần. Lệnh trên
là ví dụ cho tháng 01.

Đã build và kiểm chứng 9 output Silver trên dữ liệu thật:

| Dataset | Bronze rows | Silver rows | Rejected | Duplicates |
| --- | ---: | ---: | ---: | ---: |
| Taxi 01–06/2025 | 24,083,384 | 23,886,265 | 197,119 | 0 |
| Weather | 4,344 | 4,344 | 0 | 0 |
| Taxi Zones | 265 | 265 | 0 | 0 |
| Calendar | 365 | 365 | 0 | 0 |

Toàn bộ 197,119 Taxi rejected rows có lý do chính là `invalid_duration`
(dropoff không sau pickup). Các giá trị còn nghi vấn nhưng chưa đủ cơ sở loại
được giữ và thống kê trong manifest.

## Gold Layer

Silver là các dataset đã chuẩn hóa và kiểm tra chất lượng; Gold tích hợp chúng
thành một bảng chuyến đi phục vụ phân tích. Taxi Silver là bảng gốc. Pickup
Zone, Dropoff Zone, Calendar và Weather đều được **LEFT JOIN**; chuyến không
khớp dimension vẫn ở Gold với các cột enrichment là `null` và flag tương ứng
là `false`. Pipeline phải kiểm tra khóa dimension duy nhất và số dòng không
tăng sau từng join, không dùng `dropDuplicates` để che lỗi fan-out. **6/6
partition Gold đã hợp lệ**, tổng **23.886.265** dòng, bằng đúng Taxi Silver;
không mất dòng và không có fan-out sau các join.

Khóa join: `pickup_location_id` và `dropoff_location_id` lần lượt nối với
`location_id` của Taxi Zones; `pickup_date` nối với `calendar.date`;
`pickup_date`/`pickup_hour` nối với `weather_date_nyc`/`weather_hour_nyc`.
Weather UTC đã được Silver chuyển sang giờ địa phương NYC bằng timezone-aware
conversion. Việc coi Taxi `timestamp_ntz` là giờ đồng hồ `America/New_York`
để căn chỉnh là **giả định phân tích của project**, được hỗ trợ bởi kiểm tra
DST thực nghiệm ở STEP 4.5, **không phải bảo đảm timezone chính thức từ TLC**.
Gold không đổi Taxi sang UTC hay dùng offset UTC−5 cố định để join. Weather
nguồn chỉ phủ tới 30/06/2025 23:00 UTC (khoảng 19:00 NYC), nên các chuyến
cuối ngày 30/06 có thể không match và vẫn phải được giữ, thống kê.

Gold bổ sung `pickup_zone_matched`, `dropoff_zone_matched`,
`calendar_matched`, `weather_matched` và các cột lineage `_gold_batch_id`,
`_gold_processed_at`. Chạy theo **tháng file nguồn Taxi** vào
`data/gold/taxi_enriched/year=2025/month=MM/` (Parquet + Snappy), không
repartition theo tháng lịch pickup. `data/gold/gold_manifest.json` lưu số dòng,
match/unmatch, schema, lineage Silver → Gold và trạng thái validation; manifest
nhỏ có thể commit, Gold Parquet được Git ignore. Gold không thêm quy tắc làm
sạch Taxi: fare âm, distance bằng 0 và passenger null vẫn được giữ.

Kết quả join trên cả sáu tháng: pickup/dropoff Zone đều **0 unmatched**;
Calendar **24 unmatched** (21 pickup tháng 12/2024, 3 pickup lịch sử năm
2007/2009); Weather **20.246 unmatched**, gồm **20.243** chuyến cuối ngày
30/06 vượt rìa phủ Weather local và **3** pickup lịch sử. Không có khóa Weather
thiếu bất thường. Tất cả chuyến không match vẫn ở Gold và được ghi trong
`data/gold/gold_manifest.json`.

Chạy từ thư mục project, kiểm tra tháng 01 trước rồi mới chạy các tháng còn lại:

```powershell
python scripts/build_gold.py --month 2025-01
python scripts/build_gold.py --start-month 2025-02 --end-month 2025-06
python scripts/build_gold.py --all
python scripts/build_gold.py --month 2025-01 --force
```

Chạy lại mặc định bỏ qua output đã hợp lệ; `--force` xây dựng lại partition đã
chọn. Nếu Spark ghi Parquet trên Windows gặp vấn đề với `winutils.exe`, dùng
Docker Linux với cùng dữ liệu trên host:

```powershell
docker build -f Dockerfile.spark -t nyc-taxi-spark:step3 .
docker run --rm --network none --hostname gold-local --add-host gold-local:127.0.0.1 --cpus 4 --memory 8g --mount "type=bind,source=C:\nyc-taxi-bigdata,target=/app" -w /app -e PYTHONPATH=/app/src -e SPARK_LOCAL_IP=127.0.0.1 nyc-taxi-spark:step3 python scripts/build_gold.py --month 2025-01
```

Gold giữ cả các trường chỉ biết **sau** chuyến đi như thời gian trả khách,
`payment_type`, `tip_amount`, `tolls_amount`, `total_amount` và quãng đường
thực đo. Chúng hữu ích cho phân tích nhưng không tự động là predictor hợp lệ
cho bài toán ước lượng giá cước *trước* chuyến; chọn feature ML là bước riêng.

## PostgreSQL Analytical Data Mart

STEP 6 nạp Gold Parquet vào PostgreSQL 16 qua PyArrow đọc theo batch và Psycopg
`COPY`, theo từng tháng nguồn Taxi. Loader không đọc toàn bộ 23,9 triệu dòng vào
RAM hoặc dùng ORM insert từng dòng. Schema `analytics` gồm `dim_location`
(Silver Taxi Zones), `dim_calendar` (Silver Calendar), `dim_weather` (Silver
Weather), `fact_trip` (Gold) và `etl_load_log`. `fact_trip.trip_id` là khóa thay
thế do PostgreSQL sinh, vì nguồn TLC không có trip ID tự nhiên ổn định.
`payment_type` giữ mã số; `pickup_date` gốc được lưu riêng. Khóa ngoại
`calendar_date_id` và `weather_id` cho phép `NULL`, giữ nguyên 24 chuyến ngoài
Calendar 2025 và 20.246 chuyến không khớp Weather. Weather được định danh duy
nhất theo timestamp UTC; cặp ngày/giờ NYC chỉ được đánh index, không ép unique
để không phá vỡ dữ liệu khi có giờ lặp vào mùa thu do DST.

Từ thư mục project, thiết lập thông tin kết nối trong `.env` theo
`.env.example` (không commit mật khẩu) rồi khởi động và kiểm tra dịch vụ:

```powershell
docker compose up -d
docker compose ps
python scripts/load_postgres.py --dimensions
python scripts/load_postgres.py --month 2025-01
python scripts/load_postgres.py --start-month 2025-02 --end-month 2025-06
python scripts/load_postgres.py --all
```

Windows kết nối qua `localhost:5432` theo cấu hình mặc định; tên database,
user, port và password lấy từ `.env`/config, không hard-code trong loader.
Lệnh `--all` sẽ nạp hoặc kiểm tra dimensions rồi xử lý các tháng 01–06/2025.
Mặc định partition đã nạp cùng Gold batch ID và số dòng sẽ được bỏ qua.
`--month 2025-01 --force` thay thế đúng partition tháng 01 trong một transaction:
xóa các dòng của tháng đó, `COPY` lại, cập nhật load log rồi commit; lỗi sẽ
rollback. Không dùng `docker compose down -v` vì lệnh đó xóa dữ liệu volume.
Chỉ `sql/01_create_schema.sql` và `sql/02_create_tables.sql` ở thư mục gốc
`sql/` tự chạy lúc PostgreSQL tạo volume mới. Script migration trong
`sql/migrations/` và index/view sau load trong `sql/postload/` không tự chạy;
loader kiểm tra schema hiện hữu và không âm thầm xóa hoặc sửa dữ liệu. Nếu đang giữ schema
skeleton cũ, cần xem lỗi compatibility và quyết định backup/migration trước
khi dùng tùy chọn phá hủy `--recreate-schema`; không dùng cờ này theo mặc định.

Để xác nhận mà không tải toàn bộ fact về terminal, chạy validator/report CLI
và các SQL aggregate nhỏ:

```powershell
python scripts/load_postgres.py --validate
docker compose exec postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT source_year, source_month, count(*) FROM analytics.fact_trip GROUP BY 1, 2 ORDER BY 1, 2"'
docker compose exec postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT count(*) AS trips, count(*) FILTER (WHERE calendar_date_id IS NULL) AS calendar_nulls, count(*) FILTER (WHERE weather_id IS NULL) AS weather_nulls FROM analytics.fact_trip"'
```

Các biến `POSTGRES_USER`/`POSTGRES_DB` trong ví dụ SQL được container lấy từ
Compose/.env. Kết quả đã kiểm chứng trên cả sáu tháng: dimensions
265 / 365 / 4.344 dòng, fact 23.886.265 dòng; Calendar FK `NULL` 24, Weather
FK `NULL` 20.246, Pickup/Dropoff Location FK thiếu 0. Sáu load log có trạng thái
`loaded`; `reports/postgres_load_report.json` ghi tóm tắt validation,
thời gian nạp và Gold batch lineage nhưng không chứa secret. Index phân tích
không thiết yếu được tạo sau bulk load bằng `--create-indexes`; lệnh này cũng
tạo `analytics.vw_trip_enriched` từ `sql/postload/create_views.sql`. Đây chỉ là
SQL view, không sao chép 23,9 triệu dòng.
Power BI ở bước sau có thể kết nối trực tiếp tới star schema với host
`localhost`, port `5432` và database/user trong `.env`; dashboard chưa thuộc
STEP 6.

## 13. Thiết kế ML dự kiến

Target: `fare_amount`.

Feature ứng viên cho **ước lượng trước chuyến đi**: thời gian đón, pickup
location, dropoff location nếu đã biết trước, thông tin Calendar và thời tiết
dự báo sẵn tại thời điểm dự đoán. `passenger_count` chỉ dùng nếu thực sự đã biết
trước chuyến. Thời tiết ERA5 hiện tại phù hợp phân tích lịch sử, không tự coi
là dữ liệu dự báo có sẵn khi phục vụ mô hình thực tế.

Ba model ứng viên: `LinearRegression`, `RandomForestRegressor` và
`GradientBoostingRegressor`. Metrics: MAE, RMSE và R².

`trip_distance` thực đo, thời gian trả khách, `payment_type`, `total_amount`,
`tip_amount`, `tolls_amount` và thông tin chỉ biết sau chuyến đi không được dùng
làm predictor cho bài toán trước chuyến vì có thể gây data leakage. Gold vẫn
giữ các trường này cho phân tích; tập train và feature selection là bước riêng.

## 14. Trạng thái triển khai

### Đã triển khai

- Cấu trúc package Python và cấu hình YAML + `.env`
- Logging console/file và helper tạo local Spark Session
- Validation input cơ bản cho các entry point ingestion/ETL
- PostgreSQL + MLflow trong Docker Compose
- Schema `analytics` với dimension/fact tables và khóa ngoại
- Feature allow-list, model factory, metrics helper và MLflow configuration hook
- Calendar generator, environment checker và smoke tests
- Tải dữ liệu raw thật (Yellow Taxi 01–06/2025, zone lookup, ERA5 weather),
  raw validation và manifest có SHA256
- PySpark Raw → Bronze cho Taxi, weather, taxi zones và calendar; Parquet Snappy,
  metadata ingestion, validation và manifest truy vết nguồn; 9/9 output đã được
  build và kiểm chứng trên dữ liệu thật.
- PySpark Bronze → Silver cho sáu tháng Taxi, Weather, Taxi Zones và Calendar;
  chuẩn hóa, quality rules, dedupe, timezone-aware weather, holiday enrichment
  và lineage/quality manifest; 9/9 output đã được kiểm chứng trên dữ liệu thật.
- PySpark Silver → Gold và CLI xử lý theo tháng với LEFT JOIN Taxi Zones,
  Calendar, Weather; giữ unmatched, kiểm tra fan-out và ghi quality/lineage
  manifest; 6/6 partition Gold đã được build và đánh dấu `valid` trong
  `gold_manifest.json`.
- Gold → PostgreSQL 16 bằng PyArrow batch + Psycopg `COPY` theo từng tháng,
  schema `analytics` gồm ba dimension, `fact_trip` và `etl_load_log`; 6/6 tháng
  đã nạp, 23.886.265 fact rows bằng Gold, không mất dòng ở Calendar/Weather
  nullable FK. Index và SQL view không materialize đã được tạo; report có trạng
  thái `valid` tại `reports/postgres_load_report.json`.

### Chưa triển khai / planned

- Versioning/snapshot dữ liệu nguồn theo nhiều lần cập nhật
- Train/tune/compare model và ghi MLflow runs/artifacts thật
- Power BI dashboard
- Airflow DAG, scheduling, monitoring và deployment production

Model training chưa triển khai; các entry point liên quan sẽ báo
`NotImplementedError` thay vì tạo output giả.

## 15. Roadmap đề xuất

1. Giữ khả năng kiểm tra lineage Gold → PostgreSQL và reload từng tháng khi
   Gold batch ID thay đổi; nếu TLC/TSP xác nhận khác về timezone Parquet thì
   đánh giá lại giả định STEP 4.5 và tái xử lý có kiểm soát.
2. **Đề xuất STEP 7:** xây Power BI analytical dashboard kết nối star schema
   PostgreSQL; chưa triển khai dashboard trong STEP 6.
3. Xây train/validation pipeline và MLflow experiments ở giai đoạn riêng sau
   khi chốt feature, tránh leakage từ các trường hậu chuyến.
4. Chỉ sau khi pipeline ổn định mới bổ sung Airflow orchestration.

