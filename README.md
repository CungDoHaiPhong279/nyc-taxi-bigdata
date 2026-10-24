# NYC Taxi Big Data

Project cuối kỳ môn **Tích hợp và phân tích dữ liệu lớn**, xây dựng nền tảng cho
pipeline tích hợp dữ liệu chuyến taxi New York với thời tiết, lịch/ngày lễ và
taxi-zone lookup để hướng tới bài toán dự đoán `fare_amount`.

**STEP 1–6 đã hoàn tất:** project có dữ liệu Raw thật cho 01–06/2025, 9 output
Bronze, 9 output Silver, 6/6 partition Gold và PostgreSQL analytical data mart
đã nạp, kiểm chứng 23.886.265 chuyến. Pilot Spark GBT/MLflow, June holdout và
demo Streamlit/inference đã kiểm chứng ở ML P2.1–P5. PBIX có trong repo; bản
đang sửa được giữ riêng, không xác nhận nội dung Power BI bằng audit ML này.
Xem [checklist bàn giao](reports/PROJECT_HANDOFF_CHECKLIST.md) để phân biệt phần
đã đạt, chưa kiểm chứng, chưa triển khai và nâng cao.

**PACKAGE P2:** entrypoint dùng bundle model cố định kèm source tại
`distribution/models/ml-p4-v1/`, không cần tải model hoặc chuẩn bị model thủ công.
Clone từ commit có PACKAGE P1/P2 sẽ nhận cả source và bundle đã pin này.
Full bootstrap dữ liệu thật Jan–Jun/2025 từ máy mới vẫn **chưa E2E kiểm chứng**;
không suy từ bundle tests/PLAN. Xem phần PACKAGE P2 cuối file.

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
             +--------------+-------------------+
             |                                  |
       PostgreSQL                         Spark ML + MLflow
             |                                  |
     +-------+--------+                   Frozen inference
     |                |                         |
 Power BI/PBIX   Streamlit analytics      Streamlit fare demo

              Airflow (chưa triển khai)
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
- MLflow; pilot dùng Spark ML, scikit-learn còn ở scaffold cũ
- Docker Compose, Jupyter, pytest
- Power BI ở giai đoạn trực quan hóa

Không dùng Kafka, Flink, Kubernetes hoặc cloud service trong phạm vi hiện tại.

PySpark được giữ ở `4.2.0`; Pandas dùng range `>=2.2,<3.0` và PyArrow `>=18` theo
ma trận dependency của Spark SQL. MLflow Python client dùng cùng phiên bản `3.5.1`
với Docker image để tránh lệch giao thức client/server. Các thư viện còn lại không
bị pin cứng ở bước bootstrap.

## 4. Điều kiện cần

Với Docker worker/inference, host cần Docker Desktop, Compose, Git và PowerShell;
không cần Python/Java/Spark trên host. Danh sách sau dành cho cách chạy local cũ:

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
docker compose config --quiet
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

## 13. Thiết kế ML và pilot đã kiểm chứng

Ý tưởng before-trip bên dưới là mốc STEP 6, **không phải contract serving hiện tại**.
Pilot distance-conditioned đã triển khai nằm ở các mục ML P1–P5 tiếp theo.

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

### ML P1 → ML P2: pilot distance-conditioned, chạy tường minh

Thiết kế/cohort đã khảo sát nằm ở `reports/ML_P1_DESIGN.md` và
`reports/ml_p1_profile.json`. CLI khảo sát theo dõi bằng Git là
`scripts/profile_ml_data.py`; bản gốc `reports/ml_p1_profile.py` chỉ được giữ
local, không nằm trong clone. Đây không phải pipeline huấn luyện production. Mã ML chính
hiện nằm trong `src/nyc_taxi/ml/pilot*.py`, với CLI `scripts/train_pilot.py`;
các scaffold ML cũ không phải entry point của pilot mới.

CLI khảo sát có thể theo dõi bằng Git: `scripts/profile_ml_data.py`, bản sao
nguyên byte của script P1; bản gốc trong `reports/` được giữ nguyên. CLI này
không tham gia pilot. `reports/ml_p1_profile.json` là aggregate 36.272 bytes,
được Git theo dõi riêng từ ML P6 và dùng làm **freeze contract**; không tự ghi đè
hoặc tái sinh nó trong pilot. CLI khảo sát chỉ in progress/aggregate ra stdout.
Sau khi build/hash image chứa CLI mới, có thể tái hiện trong worker:

```powershell
docker run --rm --pull never --network none --read-only --cpus 1 --memory 1g nyc-taxi-worker:ml-p2 python -I -B scripts/profile_ml_data.py --self-test
$goldDirectory = (Resolve-Path -LiteralPath .\data\gold).Path
docker run --rm --pull never --network none --read-only --cpus 1 --memory 1g --mount "type=bind,source=$goldDirectory,target=/gold,readonly" nyc-taxi-worker:ml-p2 python -I -B scripts/profile_ml_data.py --gold-root /gold
```

Bản CLI mới đã PASS cú pháp AST, đối chiếu byte/hash và `--self-test` trong
image refresh; chưa khảo sát lại Gold bằng đường dẫn mới (không cần quét lặp).

Pilot có contract riêng so với ý tưởng before-trip ở trên: ước tính **cước cơ
bản dương `fare_amount` (USD)** cho standard-rate, có điều kiện theo distance
người dùng cung cấp (mile). Distance thực đo trong Gold chỉ là proxy lịch sử,
không phải routing/ETA hoặc bằng chứng độ chính xác trước chuyến. Cohort giữ
fare >0, ratecode=1, PU/DO IDs 1–263 và distance 0–200 mile; không cap fare,
không dùng charges, payment, actual duration hay weather reanalysis làm feature.

Giữ đủ sáu nguồn Gold để kiểm tra lineage, dedup/group overlap rồi lấy sample
**1% cố định, seed 42** theo journey-group hash. Split theo **actual pickup**:
Jan–Apr train, May validation; **June UNEVALUATED** (chỉ audit/split/sample
counts, không prediction/metrics/chọn model). Một baseline median và một GBT
cố định, không grid/CV hoặc tự đổi sample/filter/tham số để cải thiện kết quả.

Từ PowerShell tại project root, không cần Python/Java trên host:

```powershell
# PLAN/preflight; không build, tạo artifact directory hoặc chạy training.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_ml_pilot.ps1

# Build code-refresh riêng từ nyc-taxi-worker:p6-clean đã kiểm chứng.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_ml_pilot.ps1 -Build

# Chỉ chạy các tests ML fixture/mock trong worker; không train dữ liệu thật.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_ml_pilot.ps1 -TestOnly

# Artifact probe → đúng một pilot → model reload trong worker mới.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\run_ml_pilot.ps1 -Execute
```

`-Build` dùng `Dockerfile.ml`, kiểm tra dependency inputs khớp base và `pip check`,
không cài/nâng dependencies; **đây không phải clean dependency build mới**.
Tag `nyc-taxi-worker:ml-p2` riêng, không ghi đè `p2`/`p6-clean`; trước các job có
hash code/CLI/config và image identity checks. Worker COPY code, không mount repo.
Các lệnh tests/execute cần các services hiện tại healthy và đủ tài nguyên;
script không bật/restart/recreate PostgreSQL, MLflow hoặc dashboard.
Override ML được driver validate riêng bằng `config --quiet` và đã thêm vào
helper CI (10 tổ hợp); CI không build worker ML hoặc chạy pilot.

`docker-compose.ml-pilot.yml` chỉ dùng cho one-off worker thuộc profile `jobs`,
`run --rm --no-deps --pull never`. Gold và P1 profile được mount **read-only**;
Outputs/shuffle ML nằm riêng tại `models/ml-p2/20261022-pilot01/` (`/ml-output`);
temp ngắn hạn/pytest dùng `/tmp` riêng trong container (tránh lỗi anonymous
temporary file trên Windows bind mount), tự mất khi one-off container kết thúc.
Worker chỉ gắn subpath `artifacts/ml-p2-20261022` của **volume MLflow cũ** tại
`/mlflow/artifacts/ml-p2-20261022`, không mount SQLite hay toàn bộ volume vào
worker. Experiment/probe/training run riêng; không sửa database PostgreSQL.

MLflow 3.5.1 hiện chặn Host DNS `mlflow` ở API dù `/health` đạt, và proxy default
`/mlartifacts` không nằm trong persistent volume. Driver dùng private IP đã đối
chiếu ownership/network **chỉ cho job**, giữ nguyên Compose/server security;
artifact routing dùng file URI + shared subpath, không tuyên bố HTTP proxy
persistence. Probe phải đối chiếu SHA256 từ download riêng và file trên server;
không restart server để thử persistence.

Giới hạn riêng pilot: **2 CPU / 3 GiB**, Spark `local[2]`, JVM **1.536 MiB**,
shuffle 8, UI off, timeout training **30 phút**; không đổi Spark/global config.
Output cố định có ownership/training-start marker; khi lỗi/OOM/timeout phải đọc
report và dừng review, không tự xóa marker, retry/tune hay tăng budget. Không
promote/register model, sửa dashboard/Power BI, chạy ETL/loader hoặc full bootstrap.

**Kết quả ML P2 ngày 2026-10-22: pilot CHƯA PASS.** 81 ML tests, 91 helper/tracking
tests (có 47 tracking tests trùng giữa hai lượt), 10 Compose checks, `pip check`
và self-test survey PASS. Artifact probe + fresh-worker download + server SHA256
PASS, nhưng pilot dừng exit **124** ở deadline **1.800 giây** trong kiểm tra giao
tập. Cohort khớp P1, dedup loại 0 dòng; **chưa có overlap/sample counts cuối,
baseline, GBT, MAE/RMSE/R² hoặc model reload verification**. Không tự retry/tune.
Chi tiết và run IDs: [ML P2 REPORT](reports/ML_P2_REPORT.md). Đọc thêm
`training_report.json`, `artifact_probe.json`, `final_tracking_audit.json`,
`code_identity.json` và console logs trong runtime trên. June chưa được đánh giá.
Marker cố ý chặn chạy lại/ghi đè cùng attempt; bước tiếp cần review chi phí
preparation/shuffle và thiết kế attempt mới, không xóa marker để chạy lại ngầm.
Clean build P6.2 và fixture ETL P6.3 PASS không thay thế kiểm chứng training.

### ML P2.1: sample trước shuffle, attempt mới độc lập

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1 -Attempt p21
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1 -Attempt p21 -Build -TestOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1 -Attempt p21 -Execute
```

Mặc định `-Attempt p2` vẫn giữ đường chạy cũ. P2.1 dùng tag riêng
`nyc-taxi-worker:ml-p21`, output `models/ml-p2/20261023-pilot02/`, không ghi đè
attempt P2 hoặc tự bỏ marker. Giữ 2 CPU/3 GiB, JVM 1.536 MiB, timeout 1.800 giây;
Spark shuffle/temp P2.1 dùng `/tmp/ml-p21-spark` riêng trong container, không phải
Windows bind mount. Các kết quả, plans và event logs nằm trong output mới.

Cohort, canonical identities, seed 42, hash 1%, split và GBT không đổi.
Business-key equality kéo theo journey-key equality, nên mọi duplicate cùng
business key có cùng quyết định lấy mẫu: `dedup(sample(C)) = sample(dedup(C))`,
kể cả chọn source canonical. Group hash được tính trước; business SHA/format
20 trường chỉ tính cho survivors. Full Gold chỉ cần aggregate waterfall/split
counts rồi lượt đọc sampling; không cache/sort toàn cohort. Dedup và temporal/
overlap guards vẫn chạy, nhưng **phạm vi overlap là mẫu mô hình (gồm June keys)**,
không phải kiểm chứng lại full-cohort overlap. June không predict/metrics.

Attempt mới lưu timing riêng cho profile, sampling/materialize, dedup và overlap,
plans cũ được dựng lại bằng EXPLAIN **không action**; plans/event logs mới giữ
task/shuffle/spill metrics để chẩn đoán. Attempt cũ không có event log nên chưa
đủ bằng chứng quy timeout riêng cho skew hoặc Windows I/O. Chỉ kết luận kết quả
training và reload sau khi đọc artifacts/run thực tế; tests không thay thế pilot.

Kết quả thực tế P2.1 (2026-10-23): preparation đã hoàn tất; sample train /
validation / June = **110.356 / 29.979 / 27.310**, dedup loại 0, business và
journey overlap đều 0 **trên sample**. GBT P2 fit 35,331 giây; report elapsed
281,965 giây, Docker pilot wall 300,23 giây; peak cgroup 1,705 GiB/3 GiB.
Validation MAE baseline **7,887 USD** → GBT **2,415 USD** (giảm **69,38%**).
Model round-trip cùng worker và fresh worker đều khớp 16 predictions (max diff 0,
tolerance 1e-9). Run `d2772f1904d7472490a73b0ac1c0c5d4` FINISHED; June chưa
predict/metrics, model không được promote. Sampling cố định không chọn các fare
>1.000 USD trong train/validation lần này; không suy rộng hiệu quả cho rare tails.
Parser event log được bổ sung hỗ trợ single-file `local-*`; chỉ reparse log đã
lưu sau training, không chạy lại model. Bản đầy đủ là
`execution_diagnostics_reparsed.json` (bản `execution_diagnostics.json` ban đầu
được giữ để truy vết lỗi layout). Xem [ML P2.1 REPORT](reports/ML_P2_1_REPORT.md)
để phân biệt scope/timing cũ–mới và trạng thái diagnostics.

### ML P3: June holdout của ứng viên đã khóa

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/evaluate_holdout.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/evaluate_holdout.ps1 -Build -TestOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/evaluate_holdout.ps1 -Execute
```

PLAN mặc định; `-Execute` chỉ đánh giá run training cố định
`d2772f1904d7472490a73b0ac1c0c5d4`, không fit/tuning hoặc promote model.
Worker riêng `nyc-taxi-worker:ml-p3-20261023`, 2 CPU/3 GiB (heap 1.536 MiB),
`run --rm --no-deps`, không mount repo/source. Gold, P1 profile, zone CSV và
training output đều read-only; chỉ ghi runtime riêng
`models/ml-p3/20261023-june01/` và evaluation run/artifacts MLflow mới.
Marker attempt chặn tự đánh giá lại/ghi đè cùng output; các lệnh trên mô tả
quy trình một lần, không phải hướng dẫn retry sau khi đã xem June.

Chương trình so SHA256 toàn bộ model artifact với bản P2.1, baseline/feature
contract và hashes code cohort/identity/feature/holiday trước khi đọc June.
Không còn file sample P2.1: tái tạo đúng seed 42/hash 1%/dedup, đối chiếu lineage
Gold, counts cả ba split và overlap trên mẫu, rồi khóa fingerprint trước predict.
P2.1 chưa có fingerprint lưu sẵn để đối chiếu byte-wise; không gọi kiểm tra này
là full-cohort overlap. `candidate_lock.json`/`sample_lock.json` ghi bằng create-new.

Hai mô hình dùng cùng June rows; không clip hoặc bỏ prediction lỗi. Metrics và
distance/zero-distance/borough/fare slices aggregate trong Spark, chỉ trả kết quả
nhỏ. Quantiles dùng `percentile_approx`, accuracy 100.000, sai số rank 1e-5,
không phải sai số USD hay median nội suy. Nhóm n<100 ghi bằng chứng hạn chế;
nonfinite/overflow làm evaluation fail thay vì chỉ tính trên dòng hữu hạn.
Xem [ML P3 REPORT](reports/ML_P3_REPORT.md) và
[model card](reports/ML_P3_MODEL_CARD.md) cho kết quả thực tế/giới hạn.
Đây là đánh giá lịch sử 2025 theo distance người dùng cung cấp, chưa chứng minh
before-trip routing hoặc độ chính xác ở hiện tại; chưa tích hợp dashboard.

### ML P4: trang Ước tính cước và Spark inference riêng

Giữ trang **Phân tích lịch sử** làm mặc định. Mở `http://127.0.0.1:8501`, chọn
**Ước tính cước** ở sidebar, nhập mile, pickup local datetime năm 2025, chọn tên
PU/DO zone (hiện cả LocationID), xác nhận standard metered rồi bấm nút.
Đổi form không tự predict. Đây là cước cơ bản USD theo distance cung cấp,
**chưa gồm tip/tolls/taxes/surcharges**, không routing/ETA hoặc giá hiện tại.
MAE không phải khoảng bảo đảm cho từng chuyến. Jul–Dec/2025 được cảnh báo là
ngoại suy chưa đánh giá; ngoài 2025 bị chặn. Zero-distance, chuyến xa/cước cao
và unseen zones có cảnh báo; không clip hoặc dùng giá giả khi lỗi.

Dashboard vẫn **không có Java/PySpark**. Service `fare-inference` giữ một Spark
`local[2]`/fitted PipelineModel mỗi process, 2 CPU/3 GiB, heap1536MiB. API chỉ
ở network `inference` nội bộ, **không publish port**; không có DB credentials,
MLflow URI hoặc SQLite mount, không nối network PostgreSQL. Root FS read-only,
non-root10001, drop capabilities; riêng tmpfs `/tmp`1GiB cần `exec` để Snappy
load native library, vẫn `nosuid,nodev`. Đây là HTTP server cho demo nội bộ,
chưa phải public production serving/authentication/TLS.

Compose override riêng `docker-compose.inference.yml`; **không trộn với các
ML job/bootstrap overrides, không `up` cả stack**. Trước khi chạy, PostgreSQL và
MLflow hiện có phải healthy. Chỉ các lệnh sau đã được dùng để build/start/update
hai services liên quan; PostgreSQL/MLflow không restart/recreate:

```powershell
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml config --quiet
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml build fare-inference
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml up -d --no-deps --no-build --pull never fare-inference
# Chờ fare-inference healthy (/ready phải trả ready=true, không chỉ /health 200).
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml ps fare-inference
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml build dashboard
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml up -d --no-deps --no-build --pull never dashboard
```

Các lệnh `up` trên là **cập nhật tường minh cần review**, có thể thay hai container
được chỉ định; không phải thao tác mở lại hằng ngày. Từ ML P5, dùng
`start.ps1 -WithInference` để bật lại mà không tự recreate (hướng dẫn dưới).
Không truyền switch vẫn giữ luồng analytics/base Compose. Model không nằm trong Git/image.
Một clone mới **chưa đủ để chạy inference**: cần chuẩn bị đúng bốn read-only
bind inputs dưới đây; Compose chặn tự tạo path thiếu, service fail readiness nếu
file/model/hash không khớp, không tự download hay thay bằng latest:

- `models/ml-p2/20261023-pilot02/fitted_model/` → `/model`.
- `models/ml-p3/20261023-june01/candidate_lock.json` và
  `models/ml-p2/20261023-pilot02/feature_contract.json` → `/model-contract/`.
- `data/raw/taxi_zones/taxi_zone_lookup.csv` → `/model-contract/zones.csv`.

Training run `d2772f1904d7472490a73b0ac1c0c5d4`, evaluation run
`58d5008f0ed048f58599a859877ae336`, full model tree SHA256
`759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`.
Startup kiểm tra toàn bộ40files + pinned P3 lock/contract/zone/source hashes;
không cần đọc MLflow hay Gold lúc serving. `/health` chỉ báo process còn sống;
`/ready`503 khi model chưa load/lỗi. Request deadline30s, client45s, bận trả503;
timeout/model failure làm unready, không reload/fallback. Sửa artifacts cần bước
chuẩn bị/cập nhật tường minh, không âm thầm đổi model đã đánh giá.

Kiểm chứng đã chạy: 8 synthetic cases trong `config/ml_serving_parity.json`,
fingerprint `22292e16ef60b74890bbf834e3c93211f8c506b61bf4438165602b7ef4881d99`.
Model gốc chạy độc lập bằng immutable P3 image; API cả8case max diff0 USD
(tolerance1e-9). AppTest gọi API thật 3case normal/zero/unseen cũng max diff0;
không có target labels, không đánh giá lại June. Model load27,03s / Docker
healthy30,10s; HTTP đầu5,56s, lặp0,94s, không tạo Spark mới. Analytics aggregate
READ ONLY vẫn23.886.265 rows và ba chart totals khớp; SQL29,18s lần đo này,
không phải benchmark cold-cache.

Tests backend/CI132PASS; web87PASS/1skip (DB integration opt-in chưa chạy), gồm
đủ AppTests mới/cũ. Test web đầu tiên timeout3s ở một AppTest; chạy lại nguyên
suite tuần tự PASS, không đổi timeout/bỏ test. Thiếu model thử riêng trả503,
inputs sai400, service connection-refused hiện lỗi/no-price. **Chưa có browser
render test**, HTTP200 không được coi là bằng chứng UI; đã kiểm tra AppTest và
chart datasets từ aggregate thật. Xem [ML P4 REPORT](reports/ML_P4_REPORT.md).
Local CI Python image không Spark/MLflow/Java đạt **374PASS/1deselected (SQL
integration opt-in)**; image test này COPY fixture công khai mới. Không có
GitHub Actions remote run cho thay đổi chưa commit.

Tái hiện web checks (không cần Python/Java trên host):

```powershell
docker build --target test -f Dockerfile.dashboard -t nyc-taxi-dashboard:ml-p4-test .
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=268435456 nyc-taxi-dashboard:ml-p4-test python -B -m pytest -q -p no:cacheprovider tests/test_dashboard_queries.py tests/test_dashboard_app.py tests/test_dashboard_prediction.py
```

`scripts/check_fare_inference.py` có `--mode reference|service`; cần fixture RO,
model RO cho reference, reference report RO + network nội bộ cho service.
`scripts/check_fare_ui.py` chỉ chạy trong optional web test image, đọc cùng
fixture/reference và gọi API thật; không cần DB. Outputs/logs tại
`models/ml-p4/20261023-serving01/` được Git ignore; không thay `.env`/PBIX.

### ML P5: bật lại inference và kiểm tra artifact cố định

Ghi chú lịch sử: yêu cầu nhận bundle riêng dưới đây thuộc ML P5. **PACKAGE P2**
đã kèm bundle trong source và `setup_system.ps1` tự chuẩn bị model runtime riêng;
không còn cần bước export/download thủ công cho luồng máy mới đó. Bốn paths
dưới đây vẫn mô tả startup legacy `start.ps1 -WithInference` đang dùng.

Clone mới phải **chuẩn bị bundle riêng từ nguồn tin cậy**, không chọn `latest`:
`fitted_model/` đầy đủ và `feature_contract.json` của training run
`d2772f1904d7472490a73b0ac1c0c5d4`, `candidate_lock.json` của evaluation run
`58d5008f0ed048f58599a859877ae336`, cùng đúng zone CSV đã khóa. Nguồn hiện có là
MLflow/backup local của người giữ các runs trên; cần họ export/cung cấp bundle
đúng identity. **Chưa xác nhận nơi phân phối bundle công khai/release hoặc URL
tải cho máy mới**; không tự upload trong bước này. Git clone không chứa model,
dữ liệu hay Docker dependency image và tự nó chưa đủ chạy demo.
Chưa có script export/download tự động; không tự train lại khi thiếu model.
Giữ nguyên **40 files**, kể cả `.crc` và `_SUCCESS`, dưới đúng model root
`fitted_model/` (không chỉ copy thư mục con `sparkml/` hoặc đổi tên file).
Bốn bind inputs cố định, đều read-only, tính từ project root:

| Đường dẫn trên host | Đường dẫn trong inference container |
|---|---|
| `models/ml-p2/20261023-pilot02/fitted_model/` | `/model/` |
| `models/ml-p2/20261023-pilot02/feature_contract.json` | `/model-contract/feature_contract.json` |
| `models/ml-p3/20261023-june01/candidate_lock.json` | `/model-contract/candidate_lock.json` |
| `data/raw/taxi_zones/taxi_zone_lookup.csv` | `/model-contract/zones.csv` |

Kiểm tra độc lập trước khi bật dịch vụ, không cần Python/Docker, không tải hoặc
ghi file; thiếu/sai artifact trả exit khác 0:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\model_artifacts.ps1
```

Script kiểm full model tree SHA256
`759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`, pinned
candidate-lock SHA256
`12442b5684943bb81169bb084cffc8c1b54043882d75711e295a0f1a0ff7338d`,
feature contract, zone CSV và tám core source/dependency hashes trong lock.
Không sửa hash để bỏ qua mismatch; cần phục hồi đúng bundle/code đã đánh giá.
Tám core inputs phải giữ nguyên bytes, kể cả line endings sau copy/checkout;
code tương đương về ý nghĩa nhưng khác bytes vẫn bị từ chối có chủ đích.
`.gitattributes` cố định LF cho đúng tám inputs và profile P1, không đổi model lock.

```powershell
# Lần đầu bật inference: artifact đã chuẩn bị và runtime p6-clean đã có sẵn.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference -Build
# Mở lại, không build/pull mặc định.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference
```

`-Build` tường minh build worker/dashboard/inference; Dockerfile inference tái sử
dụng dependency runtime `nyc-taxi-worker:p6-clean` đã kiểm chứng, **không phải**
clean build mới hay training model. Không build/pull PostgreSQL/MLflow. Script
kiểm artifact trước build/lifecycle; luồng postgres → mlflow → fare-inference →
dashboard có health/command timeout hữu hạn, kiểm thêm `/ready` đúng ba identities
đã khóa và `model_load_count=1` (không chỉ `/health`200). Container healthy được
tái sử dụng; stopped được start; chỉ tạo service thiếu bằng lệnh nhắm đúng target.
Drift config/image báo **exit2**, không tự recreate: sau review, cập nhật riêng
đúng service bằng các lệnh base+inference ở trên rồi chạy lại script.
`-WithInference -Environment bootstrap-test` bị chặn; không trộn overrides thử/ML.
Không truyền `-WithInference` vẫn là startup analytics cũ; nếu container hiện dùng
inference override, base startup có thể báo drift chứ không tự đổi nó về base.

UI giữ giá gắn với **yêu cầu gần nhất đã gửi**, chưa bấm nút thì không predict lại.
Cảnh báo vùng unseen chỉ hiện một lần: “Vùng này chưa có trong dữ liệu huấn luyện;
kết quả có thể kém chính xác hơn.” Run IDs, SHA và thời gian API nằm trong expander
thu gọn; USD/mile, giới hạn lịch sử 2025, zero/long/high fare và cảnh báo sai số vẫn giữ.
Kiểm tra local ML P5: Python **380PASS/1deselected**, web **93PASS/1deselected**;
SQL opt-in không chạy. Pester 4.10.1 **204/204 PASS**, đủ bảy suites offline/mock;
nhánh stopped/missing/drift/timeout không được thử bằng cách dừng services thật.
Chưa kiểm chứng browser thật hay GitHub Actions remote. Xem [ML P5 REPORT](reports/ML_P5_REPORT.md).

### ML P6: bàn giao và CI remote

Phần ML P1–P5 đã được commit/push trên `main` ở
`afbf22c4886e10aaf6a81fe6221b2807154362c9`. [CI remote40001163976](https://github.com/CungDoHaiPhong279/nyc-taxi-bigdata/actions/runs/40001163976)
**PASS cả ba jobs** Python offline, PowerShell mocks và Compose/dashboard packaging.
Các ghi chú “chưa remote” ở reports P1–P5 là trạng thái lịch sử tại thời điểm đó.
Không có training/evaluation/ETL trong CI; không thiết lập auto-merge/required checks.
Commit bổ sung tài liệu bàn giao được kiểm CI riêng; xem SHA/run chính xác trong
báo cáo bàn giao của lượt P6, không lấy run của SHA khác để xác nhận HEAD mới.
Xem [checklist](reports/PROJECT_HANDOFF_CHECKLIST.md) và [ML P6 REPORT](reports/ML_P6_REPORT.md).
Tại ML P6, model bundle chỉ từ chủ MLflow/backup và chưa có trong Git. PACKAGE P2
đã chuẩn bị bundle kèm source, xem mục P2 cuối file; full
bootstrap máy mới vẫn chưa E2E kiểm chứng.

## 14. Trạng thái triển khai

Danh sách bên dưới giữ **mốc STEP 6 ban đầu** để đối chiếu lịch sử. Pilot Spark
ML/MLflow và demo inference đã được triển khai sau đó ở ML P2–P5, xem các mục ML
và reports tương ứng; không hiểu danh sách planned cũ là trạng thái demo hiện tại.

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

Ở mốc STEP 6, model training chưa triển khai; các entry point scaffold cũ báo
`NotImplementedError` thay vì tạo output giả.

## 15. Roadmap đề xuất tại STEP 6 (lịch sử)

Trạng thái và việc còn lại hiện tại được kiểm kê ở
[PROJECT_HANDOFF_CHECKLIST.md](reports/PROJECT_HANDOFF_CHECKLIST.md).

1. Giữ khả năng kiểm tra lineage Gold → PostgreSQL và reload từng tháng khi
   Gold batch ID thay đổi; nếu TLC/TSP xác nhận khác về timezone Parquet thì
   đánh giá lại giả định STEP 4.5 và tái xử lý có kiểm soát.
2. **Đề xuất STEP 7:** xây Power BI analytical dashboard kết nối star schema
   PostgreSQL; chưa triển khai dashboard trong STEP 6.
3. Xây train/validation pipeline và MLflow experiments ở giai đoạn riêng sau
   khi chốt feature, tránh leakage từ các trường hậu chuyến.
4. Chỉ sau khi pipeline ổn định mới bổ sung Airflow orchestration.

## Docker worker image (P2)

`Dockerfile.spark` nay đóng gói Python 3.11, Java 17, PySpark 4.2.0,
dependencies từ `requirements.txt`, mã `src/`, các CLI `scripts/`,
`config/config.yaml`, SQL và offline tests. Build và smoke test **không mount
repository**, không kết nối dịch vụ hoặc nạp dữ liệu:

```powershell
docker build -f Dockerfile.spark -t nyc-taxi-worker:p2 .
docker run --rm --network none -w /app nyc-taxi-worker:p2 python -c "import nyc_taxi, pyspark, pyarrow, psycopg, sqlalchemy; print('imports OK')"
docker run --rm --network none -w /app nyc-taxi-worker:p2 python scripts/load_postgres.py --help
docker run --rm --network none -w /app nyc-taxi-worker:p2 python -m pytest -q -p no:cacheprovider tests/test_imports.py tests/test_config.py tests/test_check_environment.py tests/test_postgres_loader.py
```

Image chỉ tạo các thư mục `data/`, `logs/`, `models/`, `reports/` **rỗng** để
`check_environment.py` nhận ra cấu trúc runtime. Dữ liệu ghi vào container tạm
sẽ mất khi container bị xóa; khi chạy pipeline thật phải mount riêng các thư
mục dữ liệu bền vững và cấp cấu hình PostgreSQL/MLflow thích hợp. Lệnh
`docker run` cũ có bind-mount `/app` vẫn dùng được, nhưng mount đó che mã đã
đóng gói trong image nên không chứng minh image chạy độc lập. Không chạy
`--help` với `check_environment.py` hoặc `profile_step4_5.py` vì hai script
này không có parser help và sẽ thực thi kiểm tra/profiling.

## Docker worker trong Compose (P3)

Ở P3, lệnh `docker compose up -d` bật PostgreSQL và MLflow; P4 thêm dashboard
vào nhóm dịch vụ mặc định. Với stack đang chạy, dùng các lệnh theo service bên dưới;
worker thuộc profile `jobs` nên không tự chạy pipeline. Build worker và kiểm
tra cấu hình Compose mà không in các biến đã resolve:

```powershell
docker compose config --quiet
docker compose --profile jobs build worker
```

Lệnh không truyền job chỉ kiểm tra `SELECT 1` trong transaction read-only,
tên database hiện tại và endpoint MLflow `/health`. Lệnh import cũng dùng mã
đã đóng gói trong image, không mount `src/` hay toàn bộ repository:

```powershell
docker compose ps
docker compose --profile jobs run --rm --no-deps worker
docker compose --profile jobs run --rm --no-deps worker python -c "import nyc_taxi, psycopg; print('imports OK')"
```

Chỉ dùng `--no-deps` sau khi `docker compose ps` xác nhận hai service đã
healthy: lệnh này tránh Compose tự recreate dependency trong lúc chạy job.

Chỉ chạy bước xử lý dữ liệu khi chọn CLI tường minh, ví dụ **không phải lệnh
khởi động dịch vụ**:

```powershell
docker compose --profile jobs run --rm --no-deps worker python scripts/build_gold.py --month 2025-01
```

Worker chỉ mount `data/`, `logs/`, `models/` và `reports/` để dữ liệu runtime
tồn tại qua các lần chạy; mã nguồn, cấu hình công khai và SQL đã nằm trong image.
Không dùng `docker compose down -v`. File Power BI trong `reports/` không được
worker kiểm tra kết nối sửa đổi.

## Dashboard Streamlit chỉ đọc (Docker P4)

Dashboard **NYC Taxi Analytics** đọc lịch sử từ PostgreSQL, giữ nguyên Power BI.
Image web riêng dùng Python 3.11 và `requirements-dashboard.txt` đã pin phiên bản;
không cần Java/Spark. Mã giao diện và các helper cấu hình/database cần thiết được
COPY vào image; dashboard không có bind mount, dữ liệu, PBIX hoặc `.env`.

Với PostgreSQL/MLflow hiện đang chạy, kiểm tra PostgreSQL **healthy** trước khi
chỉ khởi động dashboard. Không chạy `up` toàn bộ stack cho bước này:

```powershell
docker compose ps postgres mlflow
docker compose config --quiet
docker compose build dashboard
docker compose up -d --no-deps dashboard
docker compose ps dashboard
Invoke-WebRequest -UseBasicParsing http://127.0.0.1:8501/_stcore/health
```

Mở **http://127.0.0.1:8501**. Cổng chỉ publish trên loopback. Worker vẫn thuộc
profile `jobs`. Healthcheck dashboard xác nhận tiến trình web hoạt động; kết nối
database và nội dung biểu đồ phải được kiểm tra thêm trên giao diện/AppTest.

- Bốn KPI: Total Trips, Average Fare, Average Distance, Total Fare; ba biểu đồ
  số chuyến theo tháng nguồn, giờ đón NYC và pickup borough.
- Chọn nhiều tháng nguồn/borough trong form rồi bấm **Áp dụng bộ lọc**. Không chọn
  nghĩa là tất cả; hai bộ lọc áp dụng đồng thời cho toàn bộ KPI/biểu đồ.
- `fare_amount` là cước cơ bản **USD**, không phải `total_amount`;
  `trip_distance` tính bằng **mile**. Fare âm/0 vẫn tham gia aggregate.
- Một SQL có tham số gom fact theo tháng/giờ/location trước JOIN, rồi dùng
  `GROUPING SETS` trả các nhóm nhỏ; không tải fact về pandas.
  Mỗi lần cache miss dùng transaction `REPEATABLE READ READ ONLY`, connection
  timeout 5 giây, lock timeout 5 giây và statement timeout 180 giây; không sửa
  schema/index. Engine/kết nối được đóng sau mỗi lần đọc.
- Cache trong RAM có TTL 300 giây, tối đa 32 tổ hợp, khóa gồm cả tháng và borough.
  Giao diện hiển thị thời gian SQL và thời điểm snapshot; có thể thấy dữ liệu cũ
  tối đa 5 phút. Danh sách tùy chọn lấy từ aggregate đầu tiên của phiên; mở lại
  trang sau khi nạp thêm tháng mới.
- Đây là dashboard lịch sử; chưa có dự đoán, training hoặc nút dự đoán. Lỗi DB
  được hiển thị gọn, không đưa connection string/server exception ra giao diện.
  Hiện dùng credentials của Compose với transaction chỉ đọc; tài khoản PostgreSQL
  chuyên biệt chỉ có SELECT và xác thực web là phần triển khai sau, không tạo ở P4.

Tests offline dùng fixture nhỏ, không kết nối database và không chạy Spark:

```powershell
docker build -f Dockerfile.dashboard --target test -t nyc-taxi-dashboard:p4-test .
docker run --rm --network none nyc-taxi-dashboard:p4-test python -m pytest -q -p no:cacheprovider tests/test_dashboard_queries.py tests/test_dashboard_app.py
```

Kiểm chứng dữ liệu thật mong đợi: không lọc **23.886.265** chuyến; chỉ chọn
tháng nguồn `2025-01` **3.473.175** chuyến. SQL aggregate trên toàn bộ fact có
thể mất hàng chục giây; chờ spinner và xem thời gian thực tế, không lấy load log
thay cho số liệu. Chưa có materialized aggregate hoặc cơ chế tự tối ưu database.
Lần kiểm chứng P4: SQL không lọc **82,97 giây**; `2025-01` **30,56 giây**;
`2025-01` + Manhattan **18,26 giây** (3.087.471 chuyến). Áp dụng lại bộ lọc đã
cache mất **0,096 giây** và không gọi SQL mới. Đây là thời gian trên máy hiện tại,
không phải cam kết hiệu năng. AppTest với database thật đã kiểm tra bốn KPI và
dữ liệu Arrow của cả ba biểu đồ; chưa kiểm tra render bằng browser thực tế.
Form và cache dùng API chính thức của
[Streamlit forms](https://docs.streamlit.io/develop/api-reference/execution-flow/st.form)
và [st.cache_data](https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data).

## Docker P4.1 — tối ưu truy vấn ứng dụng

P4 đã dùng một aggregate cho cả KPI và biểu đồ, không có bốn lần quét fact riêng.
Mỗi cache miss được instrument ghi nhận một aggregate, hai `SELECT set_config`
cho timeout và một `SET TRANSACTION`; cache hit không chạy các lệnh này. Bộ đếm
này không tính thông điệp BEGIN/COMMIT hoặc metadata handshake của driver.
Trang đầu gọi helper hai lần để lấy options và kết quả, nhưng lần thứ hai hit
cùng cache; submit bộ lọc chỉ gọi một aggregate khi cache miss.

`EXPLAIN` trước tối ưu cho không lọc/tháng 01: `Seq Scan` fact → `Hash Join`
trên từng chuyến → `MixedAggregate` không partial. Index tháng nguồn/location
đã tồn tại nhưng plan này vẫn chọn scan; không thêm hoặc ép index. Với Manhattan,
scan/JOIN có thể song song nhưng aggregate cuối vẫn làm trên các dòng fact.
[PostgreSQL 16](https://www.postgresql.org/docs/16/parallel-plans.html#PARALLEL-AGGREGATION)
không hỗ trợ partial aggregation ở cấp `GROUPING SETS` đó.

SQL mới gom `(source_year, source_month, pickup_hour, pickup_location_id)` bằng
`GROUP BY` thường, rồi JOIN dimension và áp dụng borough trên các bucket nhỏ,
trước khi rollup KPI/biểu đồ. Average Fare/Distance lần lượt bằng tổng fare/distance
chia tổng số giá trị không NULL của đúng trường; không lấy trung bình các trung
bình. Fare âm/0, USD, mile, tham số SQL, READ ONLY và timeout giữ nguyên. TTL cache
vẫn **300 giây**, tối đa 32 tổ hợp, đủ hai bộ lọc.

Đo lại trước/sau trong P4.1 (giây):

| Bộ lọc | SQL trước | SQL sau | AppTest trước | AppTest sau |
|---|---:|---:|---:|---:|
| Không lọc | 125,88 | 51,42 | 132,51 | 54,95 |
| 2025-01 | 89,27 | 33,83 | 91,84 | 34,49 |
| 2025-01 + Manhattan | 36,73 | 24,78 | 38,21 | 25,19 |

Mỗi case chỉ đo một lần mỗi phiên bản, cùng script/thứ tự trên cùng database,
tuần tự trong một AppTest session mới; ba khóa khác nhau đều cache miss. SQL đo
execute + fetch aggregate; AppTest đo cả kết nối, Python, dựng/serialize biểu đồ
và test harness, **không phải thời gian render browser**. Không restart DB hoặc
xóa cache PostgreSQL/OS. Shared buffers/OS cache và tải máy không được kiểm soát,
nên các số này không phải benchmark cold database hoặc cam kết độ trễ. Các lần
P4 cũ 67–83 giây không dùng thay cho baseline mới cùng phương pháp này.
Quay lại tháng 01 sau tối ưu: **0,091 giây AppTest, 0 SQL mới**.

Đối chiếu trước/sau: cả ba Total Trips khớp checkpoint 23.886.265 / 3.473.175 /
3.087.471; mọi bin của tháng/giờ/borough khớp nguyên vẹn. Bốn KPI sau định dạng
hiển thị cũng giống nhau. Số thực được so với `rel_tol=1e-10, abs_tol=1e-8` cho
hai averages, và sai số tuyệt đối **0,01 USD** cho Total Fare. Chênh lớn nhất:
Average Fare khoảng `3.84e-11 USD`, Average Distance `2.51e-12 mile`, Total Fare
`0.000916 USD`; thứ tự cộng float thay đổi khi gom nhóm/song song.

Sau các phép đo trên, chỉ một `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)` không lọc
được chạy với READ ONLY/timeout 180 giây để xác thực plan: **2 workers launched**,
fact được gom còn **34.802 bucket** trước JOIN, đầu ra 39 nhóm; không spill ra
đĩa tạm. Lần chẩn đoán này không trộn vào bảng thời gian. Fact vẫn cần scan một
lần; có 1.072.244 shared blocks read (có thể được OS cache phục vụ, không đồng
nghĩa physical disk read). Chưa thể có độ trễ tức thì cho cache miss trên fact
lớn; bộ lọc borough hiếm cũng vẫn gom các tháng đã chọn trước khi lọc borough.

Build/test/triển khai vẫn chỉ tác động dashboard, sau khi PostgreSQL healthy:

```powershell
docker compose config --quiet
docker compose build dashboard
docker build -f Dockerfile.dashboard --target test -t nyc-taxi-dashboard:p4-test .
docker run --rm --network none nyc-taxi-dashboard:p4-test python -m pytest -q -p no:cacheprovider tests/test_dashboard_queries.py tests/test_dashboard_app.py
docker compose up -d --no-deps dashboard
Get-Content -Raw -Encoding utf8 scripts/benchmark_dashboard.py | docker compose exec -T dashboard python -
```

Chạy cùng lệnh benchmark trước và sau build để lấy các dòng `P41_RESULT` chứa
KPI/bins/timings nhỏ và `P41_CACHE_HIT`; script không ghi file hoặc dữ liệu.
Tests offline mới: **17 passed, 1 skipped**; test bị skip là opt-in PostgreSQL.
Test SQL đó đã được chạy riêng: **1 passed**, chỉ dùng `VALUES` trong transaction
READ ONLY để kiểm chứng nhóm lệch kích thước, NULL độc lập, fare âm/0, multi-year,
đa bộ lọc, nhóm toàn NULL/rỗng và từng bin, không đọc/sửa bảng fact:

```powershell
$env:NYC_TAXI_DASHBOARD_SQL_TEST = "1"
python -m pytest -q -p no:cacheprovider tests/test_dashboard_queries.py::test_postgres_readonly_weighted_aggregates_against_row_reference
Remove-Item Env:\NYC_TAXI_DASHBOARD_SQL_TEST
```

Nếu cần cache miss gần tức thì, bước sau có thể thiết kế persisted aggregate ở
mức bucket: thêm DDL/storage, job refresh sau ETL, tính nguyên tử và quy tắc độ
tươi dữ liệu; refresh vẫn phải quét fact. Đây là thay đổi database/vận hành riêng,
**chưa triển khai ở P4.1**. Không đổi bảng/schema/index/settings/volumes, không
restart PostgreSQL/MLflow và không sửa Power BI.

## Docker P5 — khởi động dịch vụ có thể chạy lại (Windows PowerShell 5.1)

Từ project root, lần đầu cần build các image worker/dashboard:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -Build
```

Các lần mở lại, không build mặc định:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1
```

Script tìm root theo vị trí chính nó, không theo cwd. Từ thư mục bất kỳ có thể
dùng `-File "C:\nyc-taxi-bigdata\scripts\start.ps1"`. `ExecutionPolicy Bypass`
chỉ áp dụng cho process này; không thay execution policy của máy/user. Cần Docker
Desktop/daemon đang chạy và Compose plugin. Nếu chưa có Docker/daemon, script
hướng dẫn rồi trả lỗi; không tự mở Docker Desktop hoặc cài phần mềm.

Nếu thiếu `.env`, script copy nguyên `.env.example` một lần, thông báo để người
dùng kiểm tra cấu hình; không in giá trị và không ghi đè file đã có. Compose được
validate bằng `config --quiet`. Mặc định `-Build` chỉ build `worker` và `dashboard`;
với `-WithInference` còn build `fare-inference` (xem ML P5); không
build/pull/nâng image PostgreSQL/MLflow. Trên clone mới, cần chuẩn bị riêng các
image PostgreSQL/MLflow đúng cấu hình Compose nếu máy chưa có. Nếu image cần tạo
service còn thiếu, script dừng và hướng dẫn, không tự pull.

Luồng postgres → mlflow → dashboard, chờ Docker healthcheck của từng service:

- Running/healthy: giữ nguyên container, không gọi `up`, `start` hoặc recreate.
- Running/starting: chỉ chờ health; unhealthy/paused/restarting/dead báo lỗi,
  không tự sửa hoặc restart.
- Existing stopped: `docker container start <ID>` sau khi xác minh project/service.
- Missing: chỉ gọi `up -d --no-deps --no-recreate --no-build --pull never <service>`.
- Tên project được lấy từ Compose hiện tại, không override; nhãn project/service,
  container name, config hash và image ID được kiểm tra trước thao tác lifecycle.
  Nhiều container hoặc xung đột ownership sẽ dừng an toàn.

Health timeout mặc định **120 giây/service**, CLI timeout **30 giây/lệnh**, build
timeout **1800 giây**. Có thể truyền `-HealthTimeoutSeconds`,
`-CommandTimeoutSeconds`, `-BuildTimeoutSeconds`, `-PollIntervalSeconds`.
Script in trạng thái cuối và hai URL: **http://127.0.0.1:8501** (dashboard),
**http://localhost:5000** (MLflow).

Exit codes:

- `0`: dịch vụ healthy và probe DB chạy được; **không có nghĩa đã bootstrap dữ liệu**.
- `1`: thiếu công cụ/image, cấu hình lỗi, health timeout/unhealthy hoặc DB probe lỗi.
- `2`: config hash/image khác container hiện tại, cần cập nhật riêng. Container đang
  chạy được bảo toàn; stopped container bị lệch cấu hình sẽ không được start.

Phân biệt ba thao tác:

1. **Bật lại dịch vụ**: chạy `start.ps1`, giữ nguyên container/image/volume hiện hữu.
2. **Cập nhật image/config**: `-Build` chỉ tạo image, không thay container đang chạy.
   Nếu cần triển khai image dashboard mới, review thay đổi rồi chạy riêng
   `docker compose -p nyc-taxi-bigdata -f docker-compose.yml up -d --no-deps --no-build --pull never dashboard`
   sau khi build riêng dashboard; nếu dùng inference, phải thêm override và theo
   lệnh ML P5. Thay đổi PostgreSQL/MLflow cần kế
   hoạch cập nhật/backup riêng; script không tự thực hiện. Hash cũng có thể khác
   do phiên bản Compose, nên cảnh báo không khẳng định nguyên nhân thay đổi.
3. **Khởi tạo dữ liệu**: quy trình riêng. P5 chỉ kiểm tra READ ONLY bằng `psql`
   trong container PostgreSQL: sự hiện diện schema/năm bảng, `EXISTS` fact và ba
   dimensions, cùng trạng thái `etl_load_log` nhỏ. Không COUNT toàn fact hoặc lấy
   loaded-row metadata làm số liệu dashboard. DB rỗng/thiếu tables/load chưa hoàn
   tất được thông báo cần bootstrap riêng; script không chạy worker, acquisition,
   ETL, loader, migration hoặc training.

**P5 chưa hoàn tất bootstrap dữ liệu từ clone mới**: vẫn cần chuẩn bị/tải raw,
thực thi Raw → Bronze → Silver → Gold, nạp PostgreSQL và validate dữ liệu bằng các
bước đã có, với thao tác dữ liệu được chọn tường minh. DB healthy hoặc có tables
không chứng minh checkpoint 23.886.265 dòng. Hai named volumes hiện tại được giữ
nguyên; không dùng `down -v`.

Kiểm thử P5: parser PowerShell 5.1 đạt; **31 tests Pester mocked đạt**. Các nhánh
stopped/missing services, `.env` thiếu, image thiếu, `-Build`, drift, lỗi daemon và
health timeout được mock; không dừng dịch vụ thật để thử. Hai lần chạy thật từ
cwd TEMP đều exit `0`, giữ nguyên IDs/StartedAt/mounts của cả ba services, hash
`.env` và PBIX. Probe thấy dữ liệu hiện hữu và 6/6 log entries hoàn tất, không
kiểm đếm lại fact. Chạy mock suite riêng (không cần Docker daemon):

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command '$result = Invoke-Pester -Script tests/start.Tests.ps1 -PassThru; if ($result.FailedCount -gt 0) { exit 1 }'
```

Suite tương thích Pester 3.4 đi kèm Windows PowerShell trên máy kiểm thử; script
startup không phụ thuộc Pester hoặc Python trên host. Cơ chế nhắm service và
`--no-recreate` theo [Docker Compose up](https://docs.docker.com/reference/cli/docker/compose/up/).

## Docker P6 — bootstrap dữ liệu tường minh qua worker

`scripts/start.ps1` **chỉ bật dịch vụ**. `scripts/bootstrap.ps1` **tạo/kiểm tra dữ
liệu**, mặc định chỉ PLAN, không tự gọi startup, build, pull hoặc thay container.
Không cần Python/Java/Spark trên Windows host; tất cả CLI Python chạy trong image
worker, không mount repository/src. Cần Docker Desktop/Compose hoạt động, `.env`
đã được chuẩn bị riêng và PostgreSQL/MLflow đang running/healthy đúng project.
Dashboard không phải dependency của bootstrap. Script không đọc/in secrets.

Từ project root (hoặc dùng đường dẫn tuyệt đối của script từ cwd bất kỳ):

```powershell
# Chỉ kiểm tra cấu hình/health/image metadata và in kế hoạch; KHÔNG chạy worker.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1

# Nếu worker chưa chứa code hiện tại, build RIÊNG khi được yêu cầu.
# Lệnh này không triển khai lại PostgreSQL/MLflow/dashboard.
docker compose --profile jobs build worker

# Có ghi dữ liệu thật: chỉ dùng sau khi đã review kế hoạch và chuẩn bị đủ tài nguyên.
# KHÔNG thực hiện lệnh này trong kiểm chứng P6 trên dữ liệu hiện tại.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1 -Execute
```

Phạm vi cố định **Taxi January–June/2025**, không có tùy chọn bootstrap một tháng.
Calendar đủ **365 ngày năm 2025**, zones và weather Jan–Jun. Kế hoạch có 14 bước:
Calendar → Raw acquisition → Bronze `--all` → Silver `--all` → Gold `--all` →
PostgreSQL `--dimensions` → sáu lần `--month` từ Jan đến Jun → `--create-indexes`
(kèm analytical view) → `--validate` (aggregate thực tế/sanity và
`reports/postgres_load_report.json`). Không dùng loader `--all`, `--force` hoặc
`--recreate-schema`; không chạy ML/training. Schema thiếu được tạo bằng DDL
idempotent sẵn có của loader; schema cũ không tương thích cần xử lý riêng, không
tự drop/recreate.

Dependencies và chạy lại:

- Raw CLI kiểm tra file đã có trước khi tải, cập nhật manifest/checksum; Calendar
  helper chỉ gọi CLI generator sẵn có nếu thiếu file. Calendar hợp lệ giữ nguyên
  bytes/mtime, file hỏng hoặc đường dẫn không phải file làm bước này dừng.
- Bronze cần Raw manifest/file hợp lệ; Silver cần Bronze hợp lệ. Gold yêu cầu
  **đủ 9 Silver entries** (6 Taxi + 3 dimensions) và đường dẫn/batch IDs hợp lệ.
  **Mọi lệnh loader**, kể cả indexes/validate, cần đủ **6 Gold entries/partitions**;
  dimensions còn cần Silver dimensions. Không bỏ qua prerequisites trên clone mới.
- Skip do từng CLI quyết định bằng manifest, lineage và kiểm tra output thực tế,
  không tự ghi manifest để giả skip. Không tải/xử lý lại partition hợp lệ khi CLI
  xác nhận khớp; Spark/read-back/checksum vẫn có thể tốn thời gian khi chạy lại.
- Dimensions vẫn thực hiện UPSERT nhỏ. Fact khớp batch/log/count thực tế được
  skip; **loader hiện có có thể sửa partition đã có log nhưng không khớp bằng
  DELETE/COPY trong transaction**, không cần `--force`. Dữ liệu không có log hoặc
  lineage không hợp lệ có thể bị từ chối. Rerun không có nghĩa hoàn toàn không ghi.
- Mỗi CLI chạy tuần tự bằng `compose --profile jobs run --rm --no-deps -T
  --pull never worker ...`, kiểm tra PostgreSQL/MLflow healthy trước khi chạy.
  Bước lỗi dừng ngay các bước sau, exit `1`; không rollback các bước đã commit
  trước đó. Xử lý nguyên nhân rồi chạy lại toàn kế hoạch bằng cơ chế skip sẵn có.

Trước bước dữ liệu đầu tiên, `-Execute` dùng Python stdlib trong worker để so sánh
SHA256 các file thực thi công khai trong image với checkout: `src/*.py` đệ quy,
các CLI liên quan, config, SQL, requirements và pyproject. Không hash `.env`, PBIX
hay dữ liệu. Thiếu/khác mã thì exit `2`, yêu cầu build riêng; tag image đổi giữa
các bước cũng dừng. **PLAN không kiểm tra nội dung/freshness của image** và không
chứng minh dữ liệu/manifest hợp lệ. Đây không phải dependency lock/SBOM hay kiểm
chứng toàn bộ môi trường; các dependencies vẫn theo yêu cầu hiện có của P2.

Console chỉ log START/RESULT của từng bước, thời gian và exit code; không replay
stdout/stderr gốc có thể chứa chi tiết kết nối. Các file logs/reports mà CLI tạo
theo cơ chế hiện tại vẫn nằm trong runtime mounts (không lưu lại raw streams của
orchestrator). Timeout CLI mặc định **30 giây**, mỗi bước
**21.600 giây (6 giờ)**; đổi bằng `-CommandTimeoutSeconds`/`-StepTimeoutSeconds`.
Nếu CLI timeout, chỉ client bị kết thúc: **worker/transaction có thể vẫn chạy**.
Phải kiểm tra one-off worker và xác nhận transaction đã xong/rollback trước khi
retry; không chạy đồng thời nhiều bootstrap trên cùng dữ liệu/database.

Lần đầu cần mạng để build dependencies/images và lấy Taxi TLC, zones, weather
Open-Meteo; Docker/host phải có đủ RAM/CPU cho Spark và đủ đĩa cho đồng thời Raw,
Bronze/Silver/Gold, PostgreSQL/indexes, COPY staging và Spark temp. Dataset khoảng
24 triệu dòng: dự trù **hàng chục GB và dung lượng trống bổ sung**, không coi đây
là số dung lượng tối thiểu đã đo. Không clone kèm dữ liệu thì startup healthy
chưa có nghĩa dữ liệu đã sẵn sàng. Image trước P6 cần build riêng để có Calendar
helper mới; P6 không tự rebuild image hay chạy acquisition/ETL/load thật.

Kiểm chứng P6: parser PowerShell, **64 tests Pester passed** (31 P5 + 33 P6),
**8 tests Calendar passed** (gồm generator subprocess thật trên CSV 365
dòng trong `/tmp`); PLAN chạy thật từ cwd ngoài project, exit `0`. Nhánh Execute/
thứ tự/fail-fast/rerun được mock, **bootstrap end-to-end CHƯA CHẠY/CHƯA PASS**.
Hash probe stdlib còn được chạy offline/read-only, không mount: kiểm tra 44 public
files, xác nhận image cũ khác checkout ở Calendar helper mới và hai file dashboard
source. Đây là kiểm tra image stale, không phải thực thi bootstrap. Unit tests
Calendar chạy offline trong worker cũ, chỉ overlay read-only đúng hai file helper
và test mới, dùng `/tmp`; điều này không chứng minh image cũ đã đóng gói code P6.
Mock suites không cần Docker daemon:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command '$result = Invoke-Pester -Script tests/start.Tests.ps1,tests/bootstrap.Tests.ps1 -PassThru; if ($result.FailedCount -gt 0) { exit 1 }'
```

Tại P6, môi trường end-to-end riêng mới là đề xuất. **P6.1 bên dưới đã bổ sung
selector và Compose override cách ly**, không cần sao chép repository/logic ETL.
Việc bootstrap đầy đủ, đối chiếu counts/KPI/report và chạy lại để xác nhận skip
vẫn cần một bước riêng được cho phép; smoke không thay thế end-to-end.

## Docker P6.1 — môi trường bootstrap thử riêng, chưa chạy dữ liệu

Các script dùng chung nay có `-Environment current|bootstrap-test`. Mặc định vẫn
là **current** với Compose/`.env`/runtime directories hiện hữu. Luôn ghi rõ
`-Environment bootstrap-test` khi PLAN hoặc thực thi công việc trong môi trường thử.
`docker-compose.bootstrap-test.yml` merge cùng Compose gốc, không sửa stack thật.
Cần Compose **>=2.24.4** cho `!reset`/`!override` (máy kiểm chứng: 5.6.0).

Lệnh từ root, hoặc dùng `-File` là đường dẫn tuyệt đối từ cwd khác:

```powershell
# Chỉ chuẩn bị thư mục/credentials thử và preflight, chưa build/start/worker.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_bootstrap_test.ps1 -CheckOnly

# Lần đầu: build CHỈ worker tag thử, start CHỈ postgres/mlflow thử, smoke + PLAN.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_bootstrap_test.ps1 -BuildWorker

# Lần sau: dùng lại image/container/volume thử, không build/pull/recreate.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\prepare_bootstrap_test.ps1

# Hai lệnh riêng nếu chỉ cần bật lại services thử hoặc chỉ xem kế hoạch dữ liệu.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -Environment bootstrap-test
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1 -Environment bootstrap-test
```

`prepare_bootstrap_test.ps1` tái sử dụng `start.ps1`, bootstrap PLAN/hash và CLI
kiểm tra kết nối hiện có; không có bản ETL thứ hai. `-CheckOnly` ưu tiên không
thực thi kể cả khi kèm `-BuildWorker`. Không build nếu không truyền cờ, không pull
PostgreSQL/MLflow và không chạy dashboard/worker daemon. Khi thiếu image thử,
build riêng bằng cờ trên; worker thật `nyc-taxi-worker:p2` không bị retag/ghi đè.

Build đầy đủ bằng `Dockerfile.spark` đã thất bại khi download PySpark 450,1 MB
bị dừng ở khoảng 101,5 MB và pip từ chối vì SHA256 không khớp. **Không bỏ kiểm tra
hash hoặc đổi dependency để né lỗi.** Môi trường thử dùng `Dockerfile.bootstrap-test`
kế thừa runtime P2 đã có tại máy, chỉ COPY mã công khai mới. Trước khi COPY, build
buộc `requirements.txt`/`pyproject.toml` giống byte với base và `pip check` phải đạt;
preparation kiểm tra ID base trước/sau không đổi. Vì vậy đây là reuse dependencies
P2, **không phải dependency build mới từ Internet hoặc lockfile tái lập**. Nếu máy
chưa có runtime P2 được tin cậy hoặc dependency inputs đã đổi, cần full build riêng
sau khi giải quyết nguồn download; không tự kéo/thay tag P2. File cũ còn thừa khi
COPY overlay sẽ bị hash inventory phát hiện; khi đó dừng, không tự xóa tùy tiện.

| Tài nguyên | Hệ thống hiện tại | Bootstrap thử |
|---|---|---|
| Compose project | `nyc-taxi-bigdata` | `nyc-taxi-bootstrap-test` |
| PostgreSQL container | `nyc-taxi-postgres` | `nyc-taxi-bootstrap-test-postgres` |
| MLflow container | `nyc-taxi-mlflow` | `nyc-taxi-bootstrap-test-mlflow` |
| Dashboard | `nyc-taxi-bigdata-dashboard-1` | Không khởi động; profile `ui-test` |
| Host ports | 5432, 5000, 127.0.0.1:8501 | **Không publish port nào** |
| PG volume | `nyc-taxi-bigdata_postgres-data` | `nyc-taxi-bootstrap-test_postgres-data` |
| MLflow volume | `nyc-taxi-bigdata_mlflow-data` | `nyc-taxi-bootstrap-test_mlflow-data` |
| Network | `nyc-taxi-bigdata_default` | `nyc-taxi-bootstrap-test_default` |
| Worker image | `nyc-taxi-worker:p2` | `nyc-taxi-worker:p6-test` |
| RW runtime roots | `data/`, `logs/`, `models/`, `reports/` | Cùng bốn thư mục dưới `.sandbox/bootstrap-test/` |
| Environment file | `.env` hiện hữu | `.sandbox/bootstrap-test/compose.env` riêng |

Credentials thử sinh ngẫu nhiên một lần, giữ nguyên khi chạy lại, không copy `.env`
thật hoặc in giá trị. Explicit `--env-file` tránh nạp ngầm `.env` thật cho stack
thử. Cả Git và Docker build context bỏ qua `.sandbox/`; không bake dữ liệu/credentials
thử vào image. Database thử có tên `nyc_taxi_bootstrap_test`, còn hostname nội bộ
vẫn là `postgres:5432` / `http://mlflow:5000` nhưng trên network thử riêng.
Không có URL host cho dashboard/MLflow thử ở bước này.

Guard dùng `config --quiet`, rồi kiểm tra JSON đã resolve **trong chương trình**;
không in JSON environment. Khi kiểm tra model, bật tất cả profiles **chỉ cho lệnh
config** để không bỏ sót service ẩn. Chặn project/container/volume/network dùng
chung, published ports, external resources/volume redirects, additional image
tags, host/shared namespaces và mọi bind ghi nằm ngoài sandbox hoặc chồng lấn
đường dẫn thật. Junction/symlink trên các path ghi bị từ chối. Kiểm tra cả cấu hình
và ownership/attachments của objects đã tồn tại; không tự nhận volume/container
trùng tên. Chỉ bind `sql/` công khai dạng read-only được dùng chung; không mount
dữ liệu hoặc PBIX thật vào worker thử. Không chạy `up` toàn stack, không tự `down -v`.

PostgreSQL thử mới chạy init SQL công khai có sẵn để tạo schema/tables **rỗng**;
đây không phải tải dữ liệu. Probe READ ONLY báo riêng schema, sự hiện diện rows và
load metadata; healthy không được diễn giải thành data ready. Worker smoke dùng
SELECT 1/current database và MLflow `/health`; probe bổ sung đối chiếu IP/DNS với
hai containers trên network thử, kiểm tra presence và dung lượng filesystem.
Hash probe so sánh 44 file công khai trong image với checkout trước bước dữ liệu
đầu tiên; các bước sau kiểm tra image ID không đổi, không hash lại toàn bộ từng bước.
PLAN vẫn không tự chạy hash probe; preparation script gọi probe riêng.

Tài nguyên đo trước build: host C còn khoảng **111 GiB**, Docker **16 CPU / 7,44 GiB
RAM**, dùng chung với hệ thống thật; không đổi allocation. Dữ liệu bốn lớp đang có
chiếm **1,84 GiB** theo file sizes host. Báo cáo load lịch sử ghi fact khoảng
**10,05 GiB** và tổng sáu COPY khoảng **86,4 phút** — đây không phải phép đo mới
trên DB thử. Dự trù **40–60 GiB bổ sung, 3–8 giờ hoặc lâu hơn** cho full bootstrap
là **ước tính**, gồm download, ETL, PostgreSQL/WAL/indexes/temp; phụ thuộc mạng,
RAM, I/O và tải đồng thời. Không coi đó là capacity/performance đã benchmark.
Script chặn smoke/build nếu host trống dưới 10 GiB, Docker không phải Linux hoặc
dưới 2 CPU / 2 GiB RAM; ngưỡng này **không bảo đảm đủ cho bootstrap đầy đủ**.
Filesystem trống trong Docker VM được đo riêng; dung lượng logical của sparse VHD
không thay thế dung lượng vật lý trống trên host. Không prune/cleanup tự động.

Tests điều phối/isolation dùng Pester mock, không gọi Docker thật:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command '$r = Invoke-Pester -Script tests/start.Tests.ps1,tests/bootstrap.Tests.ps1,tests/environment.Tests.ps1 -PassThru; if ($r.FailedCount -gt 0) { exit 1 }'
```

Kết quả kiểm chứng P6.1 (không phải bootstrap end-to-end):

- Parser đạt; **120/120 Pester mocked passed** (31 startup + 33 bootstrap + 56
  environment). **8/8 Calendar tests passed** trong image thử đã đóng gói từ `/app`,
  không overlay/mount source; CSV test chỉ nằm trong `/tmp` của worker tạm.
- Build refresh `nyc-taxi-worker:p6-test` đạt, requirements/pyproject byte-match và
  `pip check` đạt; hash **44/44 file** khớp. Runtime đo được: Python **3.11.17**,
  Java **17.0.20.1**, PySpark **4.2.0**, Psycopg **3.3.6**, MLflow **3.5.1**.
- Preparation ban đầu gặp health timeout khi MLflow thử mới khởi tạo. Sau đó cả
  hai services healthy; chạy lại dùng nguyên containers, không restart/recreate.
  Lần kiểm chứng cuối `prepare_bootstrap_test.ps1 -BuildWorker` exit **0**; SELECT 1,
  database `nyc_taxi_bootstrap_test`, DNS/IP của cả hai services và `/health` 200 đạt.
- DB thử có schema/tables nhưng **fact/dimensions chưa có rows, load log 0 entries**.
  Bốn runtime directories thử vẫn không có file dữ liệu. PLAN 14 bước chạy đạt cả
  qua preparation và CLI trực tiếp từ cwd ngoài repository; không truyền `-Execute`.
- Docker filesystem đo khoảng **928,83 GiB logical available / 1006,85 GiB total**;
  host C còn khoảng **110,8 GiB sau kiểm chứng**, mới là giới hạn vật lý cần dự trù.
  RAM Docker 7,44 GiB đang chia sẻ với stack thật (PostgreSQL thật có lúc dùng gần
  3,9 GiB); **cần review RAM/I/O trước bootstrap đầy đủ**, không coi smoke là đủ tải.
- Đối chiếu checkpoint trước P6.1: cả ba services thật giữ nguyên IDs/StartedAt/
  mounts/ports, đều healthy. Hash `.env`, PBIX, manifests, Compose gốc và hai
  Dockerfiles gốc không đổi; image worker `p2` không đổi. Hai volumes thật còn nguyên.
  Không đếm lại 24 triệu rows của database thật trong bước này.

**Lệnh dự kiến cho bước end-to-end tiếp theo, CHƯA thực thi trong P6.1:**

```powershell
# Sẽ download/ghi dữ liệu thử, chạy Spark và load khoảng 24 triệu rows vào DB THỬ.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1 -Environment bootstrap-test -Execute
```

Chỉ chạy sau khi được phép, review tài nguyên/mạng/disk và xác nhận không có worker
bootstrap cũ đang chạy. Sau đó cần kiểm tra đủ Jan–Jun, 23.886.265 fact rows, dimensions,
FK NULL, indexes/view/report, rồi chạy lại để đo skip/không trùng. **Preparation,
hash, SELECT 1, health và PLAN không chứng minh bootstrap end-to-end PASS.** Môi
trường thử được giữ nguyên sau smoke; không có bước tự xóa volumes hoặc dữ liệu.

## Docker P6.2 — kiểm chứng worker build đầy đủ, không dựa runtime P2

Hai đường build khác nhau:

- **Refresh P6.1**: `Dockerfile.bootstrap-test` kế thừa `nyc-taxi-worker:p2`, kiểm tra
  dependency inputs không đổi rồi COPY mã mới, tạo `p6-test`. Không chứng minh cài
  dependencies từ đầu được.
- **Full build P6.2**: `Dockerfile.spark` bắt đầu từ `python:3.11-slim-bookworm`,
  cài Java và requirements trực tiếp; tag riêng **`nyc-taxi-worker:p6-clean`**.
  Không dùng worker P2/refresh làm base, không retag hai image cũ.

Lỗi cũ: log build dừng progress PySpark 4.2.0 (450.129.423 bytes) ở khoảng
101,5/450,1 MB rồi pip từ chối SHA256, phù hợp với response tải thiếu. Hash kỳ vọng khớp
[metadata PyPI chính thức](https://pypi.org/pypi/pyspark/4.2.0/json), không phải hash
do dự án khai báo sai. Build dùng `--no-cache-dir`; không có bằng chứng pip cache
hỏng, ENOSPC hoặc OOM trong log. Docker có cấu hình proxy nhưng **chưa xác định
được** lỗi nằm ở proxy, mạng hay CDN; dung lượng hiện tại không chứng minh dung
lượng tại thời điểm lỗi cũ.

Khắc phục giới hạn ở Dockerfile: tối đa **3 lượt pip install**, socket timeout
**120 giây**, connection retries **5**, backoff **5/10 giây**. Mỗi lượt vẫn dùng
`--no-cache-dir` và kiểm tra hash/TLS bình thường; hết lượt thì build lỗi, không
đổi hash theo file thiếu, không nâng pip/dependency để né lỗi. `pip check` phải đạt
sau cài. Requirements/pyproject giữ nguyên. Lần clean build đầu (585,11 giây) đã
cài đủ nhưng so sánh 168 packages phát hiện ba transitive dependencies trôi theo
resolver. `requirements-worker-constraints.txt` giữ **certifi 2026.10.18, FastAPI
0.148.0, wcwidth 0.9.2** đúng runtime P6.1; không nâng cấp dependencies. Đã build lại
xác nhận cuối bằng `--no-cache` với constraints này. Đây không phải full
lockfile hay cam kết bit-for-bit reproducible; các ranges chưa pin vẫn cần review
khi build trong tương lai.

Lệnh build xác nhận (có thể tải các gói phần mềm lớn, **không tải dữ liệu Taxi**):

```powershell
docker build --no-cache --pull -f Dockerfile.spark -t nyc-taxi-worker:p6-clean .
```

`--no-cache` buộc chạy lại các bước Dockerfile, `--pull` kiểm tra base Python chính
thức. Không prune cache/images/volumes. Giữ log BuildKit để đối chiếu base digest,
cached steps, kết quả, thời gian và image size; không in environment/URLs có auth.
Chỉ được ghi **clean build PASS** khi lệnh trên hoàn tất thật và checks đạt.

`docker-compose.bootstrap-clean.yml` chỉ override image/Dockerfile của worker,
không đổi PostgreSQL/MLflow, project, ports, volumes hoặc runtime binds P6.1.
Chọn worker mới trong cùng môi trường thử đã healthy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\bootstrap.ps1 -Environment bootstrap-test -WorkerVariant clean
```

Mặc định vẫn `refresh`; `current + clean` bị chặn trước khi gọi Docker. Guard bắt
buộc `p6-clean` dùng `Dockerfile.spark`, không được trỏ về Dockerfile refresh.
PLAN chỉ kiểm tra cấu hình/health/image metadata; hash probe vẫn cần chạy riêng
bằng helper hiện có: **45 files cho clean**, gồm constraints; refresh vẫn 44.
Không chạy `-Execute`, startup/recreate hoặc full bootstrap
trong P6.2. Chỉ dùng worker `run --rm --no-deps --pull never` để kiểm tra kết nối.

Tests đóng gói nhỏ, không mount source hay dữ liệu thật:

```powershell
docker run --rm --network none --read-only nyc-taxi-worker:p6-clean python -B -m pip check
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m nyc-taxi-worker:p6-clean python -B -m pytest -q -p no:cacheprovider --basetemp=/tmp/p62-tests tests/test_imports.py tests/test_config.py tests/test_check_environment.py tests/test_bootstrap_calendar.py
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command '$r = Invoke-Pester -Script tests/start.Tests.ps1,tests/bootstrap.Tests.ps1,tests/environment.Tests.ps1,tests/p62.Tests.ps1 -PassThru; if ($r.FailedCount -gt 0) { exit 1 }'
```

Kiểm tra thư mục runtime của packaging phải làm trên image không mount data:
mount sandbox `data/` rỗng sẽ che các thư mục có sẵn trong image, không đồng nghĩa
lỗi COPY. Spark smoke chỉ tạo/dừng session nhỏ, UI tắt riêng; không chạy ETL/loader.

Kết quả P6.2 đã chạy thực tế (**clean build PASS, không phải bootstrap E2E PASS**):

- Lệnh `docker build --no-cache --pull ...` ở trên exit **0**, **751,64 giây**
  (12 phút 31,64 giây); image **2.818.932.419 bytes** (2,819 GB / 2,625 GiB), ID
  `sha256:32f066cdb5e0827d3b45abc387972ca45650cf43ed05e602cdcb6287dff43cd4`.
  Base thực tế `python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89`.
  BuildKit ref `3psx5uglddixo6ilqs449m4cn`: Completed, 16/16 steps. Có **1 CACHED
  step là FROM của base Python chính thức**; các bước RUN/COPY worker chạy lại,
  không dùng layer runtime P2/worker cũ. PySpark tải đủ 450,1 MB; không cần lượt
  retry thứ hai trong lần này. Thành công này không xác định nguyên nhân mạng cũ.
- Từ `/app`, không mount repository: `pip check` đạt; Python **3.11.17**, Java
  **17.0.20.1**, PySpark/Spark **4.2.0**, Psycopg **3.3.6**, SQLAlchemy **2.1.5**,
  MLflow **3.5.1**; import loader đạt. So sánh **168/168 package versions** với
  runtime P6.1 không còn khác biệt. Không có `.env`, `.git`, `.venv`, PBIX hay
  file dữ liệu runtime trong image. Spark session `local[1]`, UI off, luôn
  `stop()` trong finally và process exit 0; không chạy Spark job/ETL.
- **28/28 pytest passed** (0,67 giây), gồm 8 Calendar tests với dữ liệu `/tmp`;
  **138/138 Pester mocked passed**, 0 failed/skipped. Parser không lỗi.
  Ba nhánh retry shell cũng pass với pip/sleep stub offline (thành công lượt 1,
  thành công lượt 3, thất bại đủ 3 lượt); đây không phải mô phỏng mạng thật.
- Guard/Compose `--quiet` đạt; hash **45/45 public execution inputs** khớp.
  Worker `run --rm --no-deps --pull never` đạt SELECT 1 trong transaction READ ONLY,
  database `nyc_taxi_bootstrap_test`, DNS/IP trùng hai services trên mạng thử,
  MLflow `/health` HTTP 200. DB thử: fact/dimensions chưa có rows, load log 0.
  PLAN **14 bước** chọn `clean` exit 0, chạy CLI từ cwd ngoài repository;
  không dùng `-Execute`. PLAN tự nó chỉ kiểm tra metadata/health; hash và
  connectivity ở đây đã được kiểm tra riêng bằng worker.
- Đối chiếu trước/sau P6.2: ba containers thật và hai containers thử giữ nguyên
  IDs/StartedAt/mounts/ports, đều healthy; hai volumes thật giữ nguyên tên/mount.
  Hash `.env`, PBIX, bốn manifests, Compose gốc/test, Dockerfile refresh,
  requirements/pyproject và hai image tags `p2`/`p6-test` không đổi.
  Bốn runtime directories thử vẫn 0 files; host C còn **103,02 GiB** đo sau build.

Chưa chạy bootstrap end-to-end, kiểm tra tải/ETL/load đủ Jan–Jun hoặc rerun skips
trên dữ liệu thử. Trước bước đó cần review lại mạng/disk và RAM/I/O Docker đang
chia sẻ với hệ thống thật; khoảng trống hiện tại và smoke nhỏ không bảo đảm đủ
tải cho full bootstrap. Chưa có full dependency lock/base/apt pin để cam kết
build tương lai bit-for-bit; không tự mở rộng nâng cấp dependencies trong P6.2.

## Docker P6.3 — kiểm thử tích hợp bằng fixture tổng hợp nhỏ

P6.2 **clean build đã PASS**. P6.3 dùng nguyên image `nyc-taxi-worker:p6-clean`
(`32f066cdb5e0...`), không build lại hoặc mount source. Chỉ harness kiểm thử mới
`scripts/pipeline_fixture.py` được truyền qua stdin; Bronze/Silver/Gold/loader
chạy bằng các CLI **đã đóng gói trong image**, có hash probe 45 public inputs.
Đây không phải pipeline giả hoặc phép thử acquisition qua mạng.

```powershell
# Chỉ đọc cấu hình, guard cách ly/tài nguyên và in kế hoạch.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test_pipeline_fixture.ps1

# Chạy fixture nhỏ + đối chiếu độc lập + chạy lại; KHÔNG phải bootstrap -Execute.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\test_pipeline_fixture.ps1 -Run
```

Điều kiện: PostgreSQL/MLflow **thử** của P6.1 đang healthy, image clean có sẵn,
Compose hỗ trợ các merge tags P6.1. Script không build/pull/start/recreate services.
DB thử phải rỗng hoặc khớp chính xác fixture đã sở hữu (nội dung dimensions/fact,
Gold lineage, load log, snapshot nếu có); guard chặn trước loader khi phát hiện
dữ liệu khác, kể cả khi số dòng vẫn đúng. Không force/recreate-schema, không
truncate/delete dữ liệu để chạy lại. Mọi failure dừng ngay; timeout không có nghĩa
worker đã dừng, cần kiểm tra one-off container trước khi retry. Có file lock để
chặn hai lượt fixture đồng thời, không tự dừng/xóa container.

`docker-compose.fixture.yml` chỉ thay mounts/resource caps của worker: bốn thư mục
`.sandbox/bootstrap-test/p63-fixture/{data,logs,models,reports}`. Không ghi vào
`data/`, `reports/` thật hoặc các thư mục `.sandbox/bootstrap-test/{data,logs,models,reports}`
dành cho full bootstrap. Project, containers PostgreSQL/MLflow, network và volumes
thử giữ nguyên; không publish ports mới. Worker fixture giới hạn 1 CPU/1,5 GiB,
Java nhìn thấy 1 CPU, Spark driver 512 MiB và UI off riêng fixture.

Fixture độc lập, không tải Taxi/Open-Meteo:

| Nguồn | Raw/Bronze | Silver/Gold taxi |
|---|---:|---:|
| Taxi January | 14 | 4 |
| Taxi February–May | 3 mỗi tháng | 3 mỗi tháng |
| Taxi June | 4 | 4 |
| Zones / Calendar / Weather | 265 / 365 / 4.344 | giữ đủ dimensions bắt buộc |

Tổng Taxi **30 → 20**: mỗi 9 rules reject thực tế có một ca, thêm một exact duplicate.
Giữ fare âm/0 (mỗi loại 6), NULL passenger (6), distance 0, optional payment NULL;
zones có whitespace cần trim và service-zone NULL. Calendar kiểm tra đủ 11 holidays.
Weather có rain, temperature NULL và DST; ca pickup năm 2007 tạo Calendar FK NULL
1 và Weather FK NULL 1, June 30 23:00 NYC thêm Weather FK NULL 1 ở biên coverage.
Không sửa giảm validation: đủ sáu partitions và cardinalities phụ trợ gốc.

Expected được khai báo trước trong harness, không gọi ETL để tính oracle:
**20 fact rows**, cước cơ bản `fare_amount` tổng **80 USD**, trung bình **4 USD**;
`trip_distance` tổng **30 mile**, trung bình **1,5 mile**; Calendar/Weather NULL FK
**1 / 2**, location joins đều khớp, không fan-out/mất dòng. So sánh số thực dùng
absolute/relative tolerance `1e-9`; UTC checkpoints là literal, dimension DST
được đối chiếu độc lập bằng Python zoneinfo thay vì Spark transformations.

Một lượt `-Run` điều phối 23 bước: guard/prepare → Bronze → Silver → Gold →
đối chiếu files → guard → dimensions → sáu fact months → indexes/view → validation
→ đối chiếu DB → lặp lại ETL/load/validation. Không có acquisition/training.
Logs riêng từng lượt được sanitize trong fixture `logs/`; expected, ownership,
snapshots và kết quả kiểm tra nằm trong fixture `reports/`, gồm
`p63_fixture_report.json` và report thật của loader `postgres_load_report.json`.
Rerun kiểm tra batch IDs, Parquet hash/mtime, fact trip IDs, weather IDs và load-log
timestamps, không chỉ COUNT. Dimensions dùng UPSERT; identity sequence weather
có thể tăng khi conflict, đó không phải nhân đôi rows.

DB thử được giữ lại sau kiểm thử, **không tự dọn**. Sau P6.3, không được giả định DB
thử rỗng để full bootstrap: cần kế hoạch DB/môi trường riêng hoặc chuyển trạng
thái được phê duyệt; script này không xóa fixture. Acquisition và bootstrap với
dữ liệu thật đủ Jan–Jun vẫn **chưa được kiểm chứng end-to-end**.

Kết quả thực tế P6.3: **pipeline fixture nhỏ + rerun PASS**, không phải full bootstrap PASS.

- Tạo mới Bronze/Silver/Gold bằng các CLI thật đạt, lần lượt **93,55 / 152,85 /
  193,18 giây**. Oracle ban đầu đòi hai cột weather date/hour chỉ dùng làm join keys,
  không thuộc Gold projection, nên lượt đầu dừng trước loader. Đã sửa harness và
  thêm unit test; guard xác nhận DB vẫn 0 rows, oracle sửa kiểm tra toàn bộ outputs
  đạt. Không sửa ETL/loader/rules, không xóa hoặc build lại partitions valid.
- Lượt chạy đầy đủ sau đó đạt **23/23 bước**, exit 0, **438,08 giây** (7 phút 18,08),
  dùng lại outputs valid rồi load/validate và rerun. Đây không phải thời gian của
  một cold/fresh end-to-end run duy nhất; các lần tạo mới và kiểm chứng được ghi
  riêng, Docker/OS caches không được xóa hoặc restart để benchmark.
- Actual Taxi Bronze **30**, Silver/Gold/fact **20**; mỗi tháng **4 / 3 / 3 / 3 / 3 / 4**.
  Dimensions **265 / 365 / 4.344**, holidays **11**, NULL Calendar/Weather FK **1 / 2**,
  missing/null location FK **0**. Cả giá trị transform tiêu biểu và đầy đủ 20 rows
  đều khớp oracle; weather dimensions được đối chiếu đủ 4.344 UTC/NYC hours.
  KPI đúng **80 USD / 4 USD / 30 mile / 1,5 mile** (sum/average fare, sum/average distance).
- Report thật của loader `validation_status=valid`; indexes bắt buộc đều có,
  analytical view trả đúng **20 rows**. Rerun skip **9 Bronze / 9 Silver / 6 Gold**,
  facts skip **6 months**, vẫn **20 facts / 6 load-log entries**. **62 files** nguồn/
  đầu ra được kiểm tra hash/mtime cùng manifest batch IDs không đổi; fact trip IDs,
  weather IDs, nội dung dimensions và load-log timestamps cũng không đổi sau rerun.
- **165/165 Pester mocked passed**, **13/13 harness unit tests passed** (offline),
  **28/28 small pytest regression passed** trong image clean, không mount repo.
  Foreign-fact/stale-log guard được kiểm tra bằng mock; guard rỗng/fixture-owned
  và fail-fast trước loader được kiểm chứng thật. Không chèn/xóa foreign data để thử.
- Trước/sau: cả 3 services thật và 2 services thử giữ nguyên IDs/StartedAt/mounts/
  ports, đều healthy. Hash `.env`, test credentials file, PBIX, 4 manifests thật,
  Compose/Dockerfile/requirements/pyproject cũ không đổi; cả 3 worker tags giữ
  nguyên IDs. Hai volumes thật không đổi; 4 thư mục full-bootstrap thử vẫn 0 files.
  Đo host free cuối **103,14 GiB**; worker có lúc khoảng **765 MiB / 1,5 GiB**.

Giới hạn: chỉ 30 dòng Taxi tổng hợp, chưa đủ để suy ra hiệu năng/độ bao phủ mọi
trường hợp dữ liệu thật. Acquisition, full bootstrap Jan–Jun thực và huấn luyện
chưa chạy. DB thử hiện giữ fixture 20 facts và dimensions, không còn rỗng.

Chạy lại unit harness không cần Python trên host:

```powershell
Get-Content -Raw -Encoding UTF8 .\scripts\pipeline_fixture.py | docker run --rm -i --network none --read-only nyc-taxi-worker:p6-clean python -B - self-test
```

## DOCKER P7 — GitHub Actions CI cơ bản

Workflow `.github/workflows/ci.yml` chạy khi `push`, `pull_request` và
`workflow_dispatch`, không cần secrets hay truy cập máy local. Quyền chỉ
`contents: read`, checkout không giữ credentials; lượt cũ cùng event/branch/PR
được hủy. Mỗi job có timeout 15–20 phút, lỗi test/build làm CI fail.

| Job | Runner | Phạm vi |
|---|---|---|
| Python offline unit tests | Ubuntu 24.04, Python 3.11.17, venv mới | Imports/config, Calendar/acquisition offline, loader/query helpers, dashboard analytics/fare AppTest, CI helpers và ML/inference contract/tracking/diagnostic mocks; không Spark/artifact thật |
| PowerShell 5.1 mock tests | Windows 2022, Pester 4.10.1 | Parse toàn bộ scripts/tests PowerShell và đủ 11 suites; Docker/service calls dùng mock, bundle/hash/extraction kiểm offline thực; không bootstrap |
| Compose validation and dashboard packaging | Ubuntu 24.04 | actionlint 1.7.12; Compose 2.40.0 kiểm tra 18 tổ hợp bằng `config --quiet`, gồm package/package-test; chỉ build target `dashboard` |

Python chỉ cài `requirements-ci.txt` (tái sử dụng dependencies web đã pin,
thêm pytest 9.1.1 và PyArrow 21.0.0), **không** cài requirements worker/Spark/ML.
`scripts/ci_python.py` kiểm tra inventory, imports bắt buộc và discovery trước
khi chạy. Test mới/chưa phân loại, thiếu dependency, collection error, skip hoặc
xfail/xpass ngoài dự kiến đều làm fail; không để `importorskip` che lỗi đóng gói.
`scripts/ci_pester.ps1` cũng từ chối suites thiếu/mới và kết quả fail/skip/pending/rỗng.

Loại khỏi CI nhẹ một cách tường minh:

- `test_bronze.py`, `test_silver.py`, `test_gold.py`: cần Spark/JVM; giữ kiểm thử riêng.
- `test_ml_pilot_data.py`, `test_ml_pilot_model.py`: fixtures nhỏ cần Spark/JVM,
  chạy riêng bằng worker (`run_ml_pilot.ps1 -TestOnly`), không chạy training thật.
  `test_ml_pilot_tracking.py` chỉ dùng mock/stdlib, không import MLflow/Spark hay
  truy cập server, nên vẫn thuộc CI Python nhẹ; không coi đây là artifact integration PASS.
- `test_ml_holdout_metrics.py`: Spark fixture đánh giá, không fit, chạy riêng trong
  worker; `test_ml_holdout_contract.py` là unit guards offline thuộc CI nhẹ.
- `test_ml_inference.py` và `test_dashboard_prediction.py` vẫn thuộc CI nhẹ:
  HTTP/model/source guards dùng mock/fixture công khai, không cần Spark, fitted
  artifacts thật, PostgreSQL hoặc service inference đang chạy.
- `test_postgres_integration.py`: cần database thử cho migration/load, không dùng DB thật.
- Đúng một test `test_postgres_readonly_weighted_aggregates_against_row_reference`
  trong `test_dashboard_queries.py`: deselect vì cần PostgreSQL; các query/mock
  và AppTest còn lại vẫn chạy, không deselect cả file.
- P6.3 integration fixture: đã chạy local riêng ở bước trước, **không chạy trong
  CI mặc định**. Mock suite `fixture_pipeline.Tests.ps1` chỉ kiểm tra điều phối,
  không thực thi fixture/ETL. Acquisition tests dùng file tạm/network mock.

CI không khởi động stack/database, tải Taxi, chạy ETL/training/bootstrap, build
worker Spark, push image, deploy hoặc tự merge PR. Compose helper chỉ copy chín
YAML công khai vào thư mục tạm, dùng dummy env và kiểm tra default/all profiles
cho base → bootstrap-test → clean-worker → fixture, riêng base + ml-pilot và
base + ml-pilot + ml-holdout; base + inference riêng (không trộn chuỗi ML với bootstrap thử).
Không đọc `.env` thật hoặc
in Compose environment. Docker build dùng `.dockerignore`, không COPY secrets/data/PBIX;
dashboard image còn được kiểm tra `pip check`/imports trong container read-only,
không mạng, không mount host (không phải browser/UI integration test).

Actions `checkout` v6.0.2 và `setup-python` v6.2.0 được pin bằng SHA đầy đủ theo
[hướng dẫn bảo mật GitHub](https://docs.github.com/en/actions/reference/security/secure-use).
Các binary Compose/actionlint và package Pester tải đúng version, xác minh SHA256
trước khi chạy; Pester chỉ giải nén vào thư mục tạm, không thay module trên host.
Giữ cú pháp Pester v4 theo [tài liệu Invoke-Pester](https://pester.dev/docs/v4/commands/Invoke-Pester).
Đây chưa phải dependency lock toàn bộ transitive packages hay runner OS bất biến.

Tái hiện Python checks trong môi trường sạch, không cần Python/Java trên host,
không mount repo hoặc nối mạng tới database (build cần mạng tải packages):

```powershell
docker build -f .github/Dockerfile.python-tests -t nyc-taxi-ci-python:p7-check .
docker run --rm --network none nyc-taxi-ci-python:p7-check python -m pip check
docker run --rm --network none nyc-taxi-ci-python:p7-check python scripts/ci_python.py --collect-only
docker run --rm --network none nyc-taxi-ci-python:p7-check
```

Hoặc dùng venv Python 3.11 mới: cài `requirements-ci.txt`, chạy `pip check`,
`python scripts/ci_python.py --collect-only` rồi `python scripts/ci_python.py`.
Không chạy pytest mặc định trên host nếu không muốn thu thập cả Spark/DB tests.
Với Pester **4.10.1** đã có sẵn, và Docker Compose **>=2.24.4**:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\ci_pester.ps1
python scripts/ci_validate_compose.py
docker build --target dashboard -f Dockerfile.dashboard -t nyc-taxi-dashboard:p7-ci-check .
```

Nếu chưa có Pester đúng version, dùng lệnh download/hash-check/giải nén trong
job `powershell-mocks`, rồi truyền `-ModulePath <thư-mục-tạm>\Pester.psd1` vào
`ci_pester.ps1`; không cần thay module mặc định. Có thể truyền
`--compose-binary <binary-2.40.0>` cho Compose helper. Workflow dùng actionlint
để kiểm tra cú pháp/types GitHub Actions; chưa chạy ShellCheck/Pyflakes riêng.

**Chưa chạy trên GitHub**: P7 không commit/push, không gắn badge thành công.
Sau khi được phép đưa workflow lên repository, xem tab **Actions → CI** và
checks của PR; kiểm tra logs/kết quả cả ba jobs. Chạy thủ công bằng **Run workflow**
khi file workflow đã có trên default branch, theo
[hướng dẫn GitHub](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow).
Chỉ ghi GitHub Actions PASS sau một lượt remote thực sự thành công; branch
protection/required checks và auto-merge là bước cấu hình riêng, chưa triển khai.

Kết quả local P7 (2026-10-21): actionlint **PASS**, Compose **8/8 PASS**;
Pester đúng **4.10.1: 165/165 PASS** (145,23 giây, không skip/pending).
Image Python sạch từ official Python 3.11.17 build **212,15 giây**, không có
PySpark/MLflow/scikit-learn; `pip check` đạt, discovery **108/109** (đúng một test
SQL deselected), **108 passed, 1 deselected trong 5,81 giây**. Tests chạy không
mạng/host mounts, không dựa vào `.venv` trên máy. Dashboard target build
**253,15 giây**, `pip check` và packaged imports đạt trong container không mạng.
Các tag local riêng `nyc-taxi-ci-python:p7-check` / `nyc-taxi-dashboard:p7-ci-check`
không thay image của services/worker đang dùng. Đây là kiểm chứng local tương
đương, **không phải GitHub Actions remote PASS**.

## PACKAGE P1 — chuẩn bị và mở hệ thống bằng một entrypoint

Phần này ghi trạng thái P1 tại thời điểm kiểm chứng. **P2 bên dưới thay thế phần
model distribution/manual bundle**; giới hạn full bootstrap chưa kiểm chứng giữ nguyên.

P1 mới kiểm chứng **bundle local, tests mock và PLAN/preflight**; chưa build các
image package, bật stack package hoặc chạy acquisition/bootstrap thật từ đầu.
Không dùng fixture P6.3 làm bằng chứng cho dữ liệu thật. Chưa commit/push P1.

### Mở lại máy hiện có dữ liệu/model

Hệ thống cũ đã chuẩn bị theo ML P5 tiếp tục dùng lệnh cũ, không đổi Compose/volumes:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference
```

Lệnh này chỉ bật/tái sử dụng dịch vụ và kiểm tra nhẹ trạng thái dữ liệu, **không
bootstrap hoặc đếm toàn fact**. Model đã đánh giá June, không tự cập nhật khi thêm
dữ liệu. Không áp dụng package layers lên hệ thống cũ để “chuyển đổi” ngầm.

### Máy mới — lệnh mục tiêu, còn điều kiện model distribution

Cần Windows PowerShell 5.1, Docker Desktop Linux engine đang chạy, Compose
**>=2.24.4**, mạng tới registry/PyPI/apt/TLC/Open-Meteo và **bundle ml-p4-v1 từ
người giữ model**. Không cần Python/Java/Spark trên host. Chưa có URL/release phân
phối bundle được xác nhận; **chỉ clone + Docker hiện chưa đủ**. Không tự tải
`latest`, training thay thế hoặc lấy runtime P2/P6 làm nền cho package worker.

```powershell
# Mặc định PLAN: không tạo .env máy thật, build/pull/start, install model hay chạy data.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -BundleDirectory 'D:\trusted-bundles\ml-p4-v1'

# Lệnh mục tiêu cho MÁY MỚI/TRỐNG sau khi đủ điều kiện, chưa chạy E2E trong P1:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -BundleDirectory 'D:\trusted-bundles\ml-p4-v1' -Execute
```

Root được tìm từ script, không từ cwd. Khi Execute: preflight → tạo config còn
thiếu → khóa chống hai setup chạy đồng thời/checkpoint → build worker chuẩn
`Dockerfile.spark`, dashboard nhẹ và inference từ worker đó → PostgreSQL/MLflow →
gọi **bootstrap hiện có** → cài bundle vào thư mục model riêng → inference ready →
dashboard → kiểm tra report SQL đầy đủ/model identity. Không ghi đè `.env` đã có;
config mới lấy mẫu và sinh password ngẫu nhiên, không in giá trị. Infrastructure
images pin digest trong `docker-compose.package.yml`; app tags riêng
`package-p1-standard`/`package-p1`, không ghi đè p2/p6-test/p6-clean.

Bootstrap vẫn đủ **14 CLI steps**: Calendar 2025 → Raw sáu Taxi tháng + zones +
weather Jan–Jun → Bronze → Silver → Gold → dimensions → sáu tháng fact tuần tự →
indexes/view → validation. Gold cần đủ **9 Silver entries**, loader cần **6 Gold
entries**; chưa hứa chạy riêng một tháng. Raw dùng TLC Yellow Parquet/zone CSV
từ [TLC chính thức](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
weather dùng Open-Meteo archive ERA5 NYC theo UTC, Calendar tạo local. Nguồn/raw
có thể được bên cung cấp cập nhật: SHA/manifest/lineage, không load log, quyết định
outputs có còn hợp lệ. Free weather API có giới hạn và điều kiện sử dụng/
[attribution CC BY 4.0](https://open-meteo.com/en/terms); cần rà quyền phân phối
bundle riêng trước phát hành. P1 chưa tải Taxi hoặc xác minh acquisition qua mạng.

Chạy lại cùng lệnh `-Execute` kiểm tra hash code/images, manifests và lineage;
skip outputs hợp lệ, dimensions UPSERT, fact dùng cơ chế idempotency/repair hiện
có. Không truyền force/recreate-schema. Checkpoint theo stage ở
`.sandbox/package-current/package-state/checkpoint.json` (test: package-test);
chỉ ghi tiến độ/ownership, **không thay cho kiểm chứng dữ liệu**. Lỗi dừng ngay,
exit khác 0; giữ outputs/checkpoint. Trước chạy tiếp phải xác nhận worker/
transaction cũ không còn chạy; timeout không tự kill container/rollback hay retry.

Containers healthy được giữ, stopped được start, thiếu mới tạo đúng service bằng
`--no-deps --no-recreate`. Drift config/image/code làm dừng, in lệnh cập nhật riêng
đúng project/Compose layers để người dùng rà soát; script **không thực thi lệnh
cập nhật đó**. Resources cũ không có package ownership checkpoint bị từ chối
adopt/bootstrap. Không down/prune/thay volumes. Package PostgreSQL không publish
port host; Power BI và port của hệ thống cũ không đổi. Dashboard/MLflow package
current lần lượt `http://127.0.0.1:8501` / `http://127.0.0.1:5000` (không phải URL
stack test). Script in URL, không tự mở browser hoặc giả định browser đã render.

Startup light check chỉ kiểm tra health/schema/table/existence/load-state. Để
báo READY, Execute bắt buộc CLI validation cuối chạy **SQL COUNT/FK/sanity thật**
và sinh report mới của lần này; so tổng fact và sáu tháng với Gold counts/batch
lineage, dimensions 265/365/4.344, indexes/view, rồi model readiness/full hash.
Không lấy `loaded_row_count` thay cho COUNT. P1 **chưa chạy kiểm chứng đầy đủ này**.

### Bundle cố định và máy mới

P1 đã tạo local `.sandbox/package-bundles/ml-p4-v1/`, không upload hay đưa binary
vào Git/image. ZIP **63.680 bytes**, **43 files**: 40 fitted-model files (55.660
bytes), candidate lock, fitted feature contract và zone CSV; không dữ liệu chuyến,
MLflow SQLite hoặc credentials. Public pin ở `config/model_bundle.lock.json`:

- Training run: `d2772f1904d7472490a73b0ac1c0c5d4`;
  evaluation run: `58d5008f0ed048f58599a859877ae336`.
- Model tree SHA256: `759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`.
- Archive SHA256: `7770b7a64722b0a801015f99f0c6b5d2ea3aa39dbc15bc01cb7b6dd376293105`.
- Manifest SHA256: `da6d5229edd41bbfda490a8585f161024d7dcb3d532e2ee58309fb13e7d14ca7`.

Entrypoint chỉ nhận đúng version/manifest pin độc lập trong Git, xác minh toàn
inventory/hash/size và tám frozen source/dependency hashes. Không tin checksum
do một bundle lạ tự khai. Extraction có allowlist/bounds/path guards, cài atomic
vào `.sandbox/package-model/` (test: `package-test/model-bundle`); đúng bundle đã
cài thì skip, sai/incomplete thì dừng, không ghi đè model gốc. Inference chỉ mount
artifacts read-only. Chủ artifact có thể chuẩn bị bundle local bằng
`model_bundle.ps1 -Action Create -Execute` nếu thư mục version chưa tồn tại;
không dùng Create để thay thế một version đã phát hành. Kiểm tra riêng:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\model_bundle.ps1 -Action Check -BundlePath '.sandbox/package-bundles/ml-p4-v1/nyc-taxi-model-ml-p4-v1.zip' -ManifestPath '.sandbox/package-bundles/ml-p4-v1/nyc-taxi-model-ml-p4-v1.manifest.json' -ManifestSHA256 da6d5229edd41bbfda490a8585f161024d7dcb3d532e2ee58309fb13e7d14ca7
```

Để đạt mục tiêu máy mới chỉ một lệnh: chủ model cần xác nhận provenance/quyền
phân phối và cung cấp bản version bất biến (ví dụ Release asset với checksum pin),
rồi bổ sung nguồn tải được duyệt. **Chưa publish/upload trong P1** và chưa xác
nhận quyền phát hành model; dữ liệu public không tự chứng minh quyền model.

### Môi trường kiểm chứng trống — chưa Execute

```powershell
# P1 đã chạy: chỉ tạo config thử và thư mục runtime trống, rồi PLAN/preflight.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Environment package-test -PrepareTest

# PLAN lại, không build/start/chạy worker.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Environment package-test

# Dự kiến bước kiểm chứng sau, CHƯA THỰC THI trong P1:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Environment package-test -BundleDirectory '.sandbox/package-bundles/ml-p4-v1' -Execute
```

| Resource | Hệ thống đang có | Package test mới |
|---|---|---|
| Project / containers | `nyc-taxi-bigdata`, giữ nguyên | `nyc-taxi-package-test`, không fixed container_name |
| Ports host | PG5432 / MLflow5000 / dashboard8501 | PG/inference không publish; MLflow127.0.0.1:15000 / dashboard127.0.0.1:18501 |
| Named volumes | `nyc-taxi-bigdata_{postgres,mlflow}-data` | `nyc-taxi-package-test_{postgres,mlflow}-data` |
| Networks | project default/inference | `nyc-taxi-package-test_default`, `_inference` (internal) |
| Runtime RW binds | `data/ logs/ models/ reports/` | `.sandbox/package-test/{data,logs,models,reports}/` |
| Serving model | model gốc, không đổi | `.sandbox/package-test/model-bundle/`, RO |

SQL bind dùng code repo read-only; không mount source/repo vào worker/web. Guard
kiểm tra cấu hình resolved riêng tư và metadata Docker, chặn shared resources,
port/path collisions và junction/symlink. **Không tái sử dụng**
`.sandbox/bootstrap-test/` đang chứa fixture P6.3. P1 chỉ chuẩn bị thư mục/config;
package-test chưa có containers, networks, volumes hay dữ liệu/model cài đặt.

Preflight yêu cầu >=2 Docker CPUs, >=7 GiB Docker RAM, >=8 GiB host RAM, >=60 GiB
**physical host free** ở drive runtime; không coi dung lượng logical của VHD là
physical free. Đo khi tiếp tục P1: host15,35GiB RAM, Docker7,44GiB/16CPUs, physical
free 71,27–71,54GiB (lần PLAN trước ngắt: 87,99GiB). Nếu Docker disk image ở drive
khác, phải kiểm tra thêm physical free/disk limit drive đó. **Ước tính**, chưa đo
E2E: 60GiB headroom cho images/layers,
Raw→Gold, database/index/WAL, shuffle/temp; 3–8+ giờ tùy mạng/I/O/tài nguyên, có
thể lâu hơn. Mạng tải dependencies/images có thể thất bại; timeout hữu hạn và
checkpoint không bảo đảm thời gian này. RAM đo là tổng tài nguyên cấp phát, không
phải headroom đang trống; cần rà ngân sách khi chạy song song hai stack. Bootstrap
worker giới hạn 2CPU/3GiB, JVM1536MiB; inference chỉ bật sau bootstrap, không training.

Pester/Compose inventory CI đã bao gồm PACKAGE offline tests và overrides,
không cần bundle ngoài repo/secrets và không chạy Spark/full bootstrap trên CI. Xem
[PACKAGE P1 report](reports/PACKAGE_P1_REPORT.md) cho kết quả local cuối cùng.
Chỉ sau kiểm chứng Execute riêng: đối chiếu COUNT thật/manifests/model ready,
chạy lại kiểm tra skip, xác nhận môi trường thật không đổi, và kiểm tra UI thực
mới có thể gọi bootstrap máy mới PASS. Không suy từ mocks/PLAN hoặc fixture.

## PACKAGE P2 — model bundle đi kèm source, không cần tải riêng

Đã đưa bản ZIP nhỏ, manifest, checksum, `PROVENANCE.md` và `CONTENT_AUDIT.json`
vào `distribution/models/ml-p4-v1/` để đi cùng commit source. Trong bước kiểm
chứng P2 chưa commit/push hoặc publish bundle/image riêng; trạng thái Git/CI
của lần bàn giao sau đó phải đối chiếu đúng commit, không suy từ tests local.
Nguồn là preprocessing + GBT được fit bởi code dự án trong training run
`d2772f1904d7472490a73b0ac1c0c5d4`, đã đánh giá trong run
`58d5008f0ed048f58599a859877ae336`; không pretrained weights bên ngoài, không retrain.
Code/báo cáo training và đối chiếu nội dung được ghi trong
[provenance notice](distribution/models/ml-p4-v1/PROVENANCE.md).

ZIP **43 files** = **40 files model gốc** (55.660bytes, gồm Spark CRC/success
markers) + đúng **3 files**: `candidate_lock.json` (14.417bytes),
`feature_contract.json` (681bytes), `taxi_zone_lookup.csv` (12.331bytes/265zones).
Các Parquet trong model là fitted category dictionaries/GBT nodes/tree metadata,
không phải chuyến taxi. Audit từng file và kiểm CRC/schema không thấy secrets,
trip rows, MLflow SQLite, executable model code hoặc payload khác.

TLC là nguồn zone lookup không sửa đổi; giữ URL, ngày lấy, version/hash và
disclaimers. [NYC Open Data FAQ](https://www.nyc.gov/opendata/get-started/FAQs) và
[public policies](https://cityofnewyork.github.io/opendatatsm/publicpolicies.html)
là bằng chứng sử dụng/publication dữ liệu, không phải giấy phép pretrained model.
Không thấy hạn chế cụ thể đòi một “chủ model bên ngoài duyệt” cho model tự fit này;
P1 đã đặt gate đó quá chung. Không gán CSV là CC BY/CC0/public domain hoặc giả định
City endorsement. Dự án chưa chọn giấy phép tái sử dụng tổng thể/model-specific;
notice không tự cấp phép mới hay thay điều khoản của bên thứ ba.

Full model SHA256 **không đổi**:
`759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`.
ZIP63.680bytes **không đổi**:
`7770b7a64722b0a801015f99f0c6b5d2ea3aa39dbc15bc01cb7b6dd376293105`.
Manifest P2 cập nhật **metadata phân phối bên ngoài ZIP**, không đổi weights:
`5e2cbc312b404dc9d1fb0eae7848420b6077f07f99272b844a942719ab75719b`
(11.473bytes). `config/model_bundle.lock.json` là trust anchor được theo dõi;
sidecar `.sha256` để kiểm tra thủ công, không tự tin checksum từ archive lạ.

Lệnh mục tiêu trên **máy mới/trống** sau khi source có đầy đủ files P1/P2:

```powershell
# PLAN, không build/pull/start/install/bootstrap:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1

# Lệnh mục tiêu, CHƯA chạy full bootstrap trong P2:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Execute

# Kiểm model riêng, không Docker/Python/Java/training hoặc giải nén:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\model_bundle.ps1 -Action Check
```

Không cần `-BundleDirectory`, cache `.sandbox/package-bundles`, MLflow download
hay model gốc ở `models/`. Setup xác minh bundle trước xử lý dữ liệu; Execute
dùng installer có sẵn để giải nén vào model runtime riêng trước bật inference.
Missing/sai pin/hash/source/path thì exit khác0, không fallback cache/latest.
Hash từng member trước ghi và toàn model/contract/lock sau giải nén; chặn traversal,
symlink/junction, duplicate-case và oversized ZIP. Output đúng được verify/skip,
giữ mtime; output đã có nhưng sai/incomplete thì dừng, không overwrite/merge.
Bundle trong Git không đồng nghĩa được bake vào image: `.dockerignore` loại
`distribution/models/`; serving vẫn dùng artifact mounts read-only.
`.gitattributes` giữ ZIP binary và manifest/checksum LF để checkout Windows
không làm thay checksum. Hệ thống cũ vẫn dùng `start.ps1 -WithInference` như trước.

P2 kiểm bundle/check/install/rerun trong **bản sao chỉ có source dự kiến Git**,
không model/cache/dữ liệu/.env/PBIX local; PLAN/preflight không chạy bootstrap.
Thực tế: **335/335 Pester PASS** (không skip/pending); bản sao150files kiểm default
bundle, install43files/rerun giữ hashes+mtime và PLAN/preflight exit0.
Thiếu dependency build, mạng tải dữ liệu, tài nguyên và full acquisition → ETL →
DB counts/indexes/view → serving/readiness/rerun trên môi trường trống vẫn cần
kiểm chứng riêng. Xem [PACKAGE P2 report](reports/PACKAGE_P2_REPORT.md).
