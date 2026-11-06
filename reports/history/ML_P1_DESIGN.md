# ML P1 — Thiết kế ước tính fare_amount

Ngày: 2026-10-22 (Asia/Saigon). Checkout `fb0afae823697492360447f3e264b6f1e74d58c0`.
Trạng thái: khảo sát + thiết kế; **chưa training, chưa có model/metrics dự đoán**.
Chi tiết thống kê, file metadata, runtime và SHA256 script: `ml_p1_profile.json`.

## 1. Contract v1 và train–serving gap

Target `fare_amount` là **cước cơ bản USD**, không phải `total_amount` hay tổng tax/phí/tip.
Theo [TLC dictionary 2025](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf),
distance là quãng đường thực đo và ratecode là mã cuối chuyến.
V1 là **ước tính cước dương standard-rate, có điều kiện theo distance người dùng cung cấp**.
Input tại/trước pickup: `distance_miles` **[0, 200]**, pickup datetime/zone, destination zone
dự kiến và xác nhận standard metered; không bắt người dùng biết final ratecode/charges.
Không định tuyến, không tự biết khoảng cách/ETA, không cam kết taximeter/báo giá thương mại;
không hỗ trợ JFK flat fare, negotiated fare, refund/no-charge hoặc mọi chuyến taxi nói chung.

`trip_distance` và dropoff zone lịch sử là kết quả thực tế, chỉ làm **proxy** cho input dự kiến.
Metrics sẽ là retrospective distance-conditioned evaluation, không chứng minh before-trip
accuracy khi distance/destination sai. Kiểm tra độ nhạy distance ±10%/±20% trên validation.
Nếu giữ bài toán before-trip trong README §13, phải bỏ actual distance hoặc có estimated-route
distance lưu đúng thời điểm yêu cầu; Gold chưa có. Không âm thầm đổi contract train/serving.

## 2. Dữ liệu/code/worker đã kiểm tra

- Gold Jan–Jun/2025: **23.886.265 rows**, sáu partitions, 24 Parquet files, 567.977.487 bytes;
  cùng schema, manifests valid. Không có stable trip UUID; không dùng DB identity `trip_id`.
- Silver dedup trong source month; chưa chứng minh unique xuyên sáu sources.
- `build_features.py` chỉ chọn pandas columns, chưa khớp Gold: `month/temperature/snow/wind_speed`
  khác `pickup_month/temperature_2m/snowfall/wind_speed_10m`. `train_models()` vẫn NotImplementedError;
  chưa có train CLI, temporal split hoặc Spark preprocessing pipeline.
- `p2` lệch **4/45** public inputs, không dùng khảo sát; `p6-clean` (`32f066cdb5e0…`) khớp **45/45**
  theo bootstrap hash (gồm ML/features/config/SQL), không bao phủ toàn repo/P7 helpers/README.
- Spark ML Pipeline/preprocessors/GBT/evaluator và modules ML import đạt: Python 3.11.17,
  Java 17.0.20.1, PySpark 4.2.0, MLflow 3.5.1; versions còn lại trong JSON.
  Không cài thêm dependency, tạo SparkSession, gọi `configure_mlflow()`/`set_experiment()`;
  imports không chứng minh fit, serialization hoặc artifact logging.

## 3. Khảo sát exact đã chạy

Script truyền stdin vào clean worker, không mount source; Gold/root FS read-only, network none,
1 CPU/1 GiB. Đọc 16 cột, batch 65.536; **một lượt toàn Gold**, không pandas/full collect/sample.
Scanned count = footer = manifest từng partition; fixture literal 11 dòng đã kiểm tra waterfall,
split, NULL passenger, zero distance và tail fare. Survey **33,911 s**, Docker wall **35,930 s**,
peak process RSS **165,05 MiB**; không reset OS cache, không phải benchmark Spark training.

| Toàn Gold | Kết quả exact |
|---|---:|
| Fare NULL/NaN/Inf; âm / 0 | 0/0/0; 1.322.979 / 9.726 |
| Fare >1.000 / >10.000 USD; min / max | 24 / 4; -1.807,60 / 863.372,12 |
| Distance NULL/NaN/Inf/âm; 0 / >200 mile | đều 0; 655.348 / 1.222 |
| Passenger NULL / 0 / >6 | 5.417.233 / 137.707 / 83 |
| Calendar-derived NULL; pickup lệch source / ngoài Jan–Jun | 24 mỗi cột; 137 / 24 |

Ratecode=1: **17.250.973**, NULL: **5.417.233**; các mã/bins đầy đủ trong JSON.
Không đoán NULL/99 thành standard rate, không kết luận mọi fare âm là refund/cực trị là lỗi.
Actual June toàn sources **4.254.360**, khác source June **4.254.377**; 113/137 lệch nguồn vẫn
trong Jan–Jun. Không split theo partition/ratio 0,8. Timestamp NTZ dùng quy ước NYC của dự án;
dictionary TLC không xác nhận timezone/DST này. Derived time/lineage kiểm tra không sai lệch.
JSON `-999.0` chỉ là nhãn NULL counters, không imputation; `*_nan` gồm NULL/NaN sau conversion.

## 4. Feature allow/deny policy

| Nhóm | V1 |
|---|---|
| trip_distance | Dùng theo contract §1, input `distance_miles`; không gọi là routing estimate |
| pickup datetime/hour, day_of_week, weekend/holiday | Dẫn xuất từ input thời gian; sin/cos hour/DOW cố định, không fit test |
| PU/DO location IDs | Categorical; destination do user khai báo; lookup zone/borough/service-zone tĩnh, tránh alias dư thừa |
| passenger_count | Chưa dùng; không drop NULL/0. Nếu thêm: missing indicator + train-only imputer |
| pickup_year/month/date, source year/month | Actual datetime chỉ split/audit; không dùng month/year predictor. Jan–Apr thiếu support May/June |
| ratecode_id | Chỉ cohort=1, KHÔNG predictor: final-code leakage; user xác nhận scope, không nhập recorded code |
| vendor_id, store_and_fwd_flag, payment_type | Loại: provider shortcut/ngoài input, telemetry hoặc thanh toán hậu chuyến |
| dropoff_datetime, actual duration/speed | Loại; không tạo duration/speed từ timestamps |
| extra, mta_tax, tip, tolls, improvement/congestion/airport/cbd fees, total_amount | Loại mọi charge/kết quả hậu chuyến; không tái dựng target từ total trừ phí |
| temperature_2m, precipitation, snowfall, wind_speed_10m, weather_code/timestamps | Loại: ERA5 archive/reanalysis/aggregate giờ chưa chứng minh có tại pickup; thiếu issue-time/as-of forecast |
| holiday_name, *_matched, weather_unmatched_reason, mọi lineage/batch/processed/source-file | Không predictor: dư thừa/coverage shortcut; chỉ audit/identity |

Không target encoding/label-based features. Dropoff timestamp/target có thể thuộc audit identity,
**không trở thành X**. Lookup/calendar preprocessing phải giống lúc serving.
Calendar hiện chỉ phủ 2025; serving ngoài năm này cần thiết kế/kiểm chứng riêng trong ML P2.

## 5. Cohort riêng, temporal split và dedup chưa chạy

Đề xuất ML view, **không sửa Gold**. Fare>0 đổi estimand, không đánh giá general signed fare;
ratecode=1 là retrospective cohort, có selection bias (đặc biệt 5,4 triệu rows thiếu mã), không
đảm bảo final mode đã biết trước chuyến. Distance≤200 là ranh giới vận hành định trước, không
ngưỡng tối ưu metrics; không nói mọi 200+ mile sai. Không áp fare cap 1.000/winsorize/log1p/clip.

| Waterfall tuần tự, không cộng chồng | Loại thêm | Còn lại |
|---|---:|---:|
| Actual pickup [2025-01-01, 2025-07-01) | 24 | 23.886.241 |
| Fare finite | 0 | 23.886.241 |
| Fare >0 | 1.332.705 | 22.553.536 |
| Distance finite, không âm | 0 | 22.553.536 |
| PU/DO IDs 1–263, không Unknown/N/A 264/265 | 167.415 | 22.386.121 |
| Standard rate=1, không đoán NULL | 5.588.563 | 16.797.558 |
| Distance≤200 mile | 14 | **16.797.544** |

Giữ **70,323%**, loại **7.088.721**; vẫn giữ **108.345 distance=0**, **4 fare>1.000 (2>10.000)**,
130.985 passenger=0. Passenger NULL không là điều kiện lọc; tình cờ còn 0 sau rate filter.

| Actual pickup NTZ, khoảng nửa mở | Cohort trước dedup | Kỳ vọng sample 1% |
|---|---:|---:|
| Train [2025-01-01, 2025-05-01) | 11.053.615 | ~110.536 |
| Validation [2025-05-01, 2025-06-01) | 3.008.396 | ~30.084 |
| Test [2025-06-01, 2025-07-01) | 2.735.533 | ~27.355 |

ML P2 phải union sáu nguồn, canonical SHA256 **20 original business columns** (vendor_id đến
cbd_congestion_fee), bỏ metadata khỏi key; serialization schema/null/time/float cố định,
dedup exact xuyên partitions trong ML view, lineage chọn deterministic. Group key phụ:
vendor + PU/DO timestamps + PU/DO zones, audit bản sửa khác fare; không coi fields key là X.
Split theo actual pickup, không randomSplit/source. Assert exact/group-key giao ba cặp sets=0
và thời gian tách biệt; fail trước fit. Không stable UUID nên không bắt chắc mọi bản sửa timestamp.
**P1 CHƯA chạy full dedup/anti-overlap shuffle; chưa PASS**; counts ở trên đều trước dedup.
Đây là event-time, không as-of-publication backtest (late/backfill); June không dùng chọn model.

## 6. Pilot đề xuất — chỉ thực hiện khi được cho phép

- Sau cohort/dedup, hash journey group + **seed 42**, 1%:
  `pmod(xxhash64(lit(42), group_key), 1000000) < 10000`; không tùy thứ tự `limit`/đổi fraction theo metrics.
  Kỳ vọng ~168k; hard caps train/validation/test **200k/50k/50k**, vượt thì dừng review.
- Fit StringIndexer zones trên **train sample**, unknown=`keep`, assembler giữ categorical metadata;
  không coi indexed IDs là continuous. Kiểm tra cardinality+unknown≤`maxBins` (tối đa 263+1),
  ghi unseen-category rates holdout, không drop. Nếu thêm imputer sau này: chỉ fit train.
- Baseline median train sample, approximate quantile relative error **0,001**, log rõ.
- Một Spark **GBTRegressor**: `lossType=absolute`, seed 42, **20 iterations, depth 4**, stepSize 0.1,
  subsamplingRate 0.8, minInstancesPerNode 20, **maxBins=512**, maxMemoryInMB 128. Chưa fit/chọn bằng metrics.
  [GBT API](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.regression.GBTRegressor.html),
  [StringIndexer/categorical handling](https://spark.apache.org/docs/latest/ml-features.html#stringindexer).
- Baseline/model cùng holdout: MAE (USD cước cơ bản), RMSE, R²; slices actual month, distance (cả 0),
  zone/unknown, fare tails và distance ±10%/±20%. Chọn bằng validation; test chỉ đánh giá cuối,
  không chỉnh ngưỡng/clip labels. Báo rare-tail coverage sample và tail holdout bổ sung riêng.
- Snapshot: Docker **16 CPU/7,44 GiB**, host free **~100,32 GiB**; đây không phải RAM free đảm bảo
  và các services đang chia sẻ RAM. Pilot dự kiến **1 job, local[2], 2 CPU/3 GiB, Java heap 1.536 MiB**,
  shuffle 8, UI off, **timeout 30 phút**; preflight headroom lại. Runtime training chưa đo.
  Không đổi global `local[*]`; CLI override riêng, `spark.stop()` finally, không grid/CV song song
  hoặc full-data pandas. OOM/timeout phải báo lỗi, không tự nới máy/lọc target cho PASS.
  Runtime ML riêng cho temp/prepared/model outputs, không ghi đè data layers/PBIX.

## 7. MLflow, tái hiện và bàn giao

Giữ URI Compose `http://mlflow:5000`; đề xuất experiment/run pilot riêng. Log Gold batch/manifest,
commit/image/code hash, contract/features/filters/waterfall/dedup/splits, sample seed/hash/caps,
resource/model params; `validation.mae_usd/rmse_usd/r2`, `test.*` sau khi chốt. Artifacts gồm schema,
Spark preprocessing/model, split/sample manifest, aggregate residual/tails, design/environment;
không secrets/full trip records. **Chưa chứng minh artifact persistence/round-trip**:
server dùng `/mlflow/artifacts` nhưng worker không mount path đó. Phải đọc `artifact_location`,
phân biệt `mlflow-artifacts:` proxy và file/local; health/imports không đủ. Không thay flags/service
hoặc tạo experiment/log artifact trong P1. [MLflow architecture](https://mlflow.org/docs/latest/self-hosting/architecture/artifact-store/)
là tham chiếu chung; cần xác nhận routing thực tế runtime 3.5.1 trước logging.

Chỉ thêm ba reports: thiết kế này, `ml_p1_profile.json`, `ml_p1_profile.py`; giữ ignore rules,
không sửa ML/config/README/image/dashboard/ETL/DB/PBIX, không stage/commit/push.
Script mới chạy stdin minh bạch, không giả định đã COPY vào image; JSON ghi đúng script SHA256.
Tái hiện sau hash probe clean (không cần Python host):

```powershell
Get-Content -Raw -Encoding UTF8 reports/ml_p1_profile.py | docker run --rm -i --pull never --network none --read-only --cpus 1 --memory 1g -e OPENBLAS_NUM_THREADS=1 nyc-taxi-worker:p6-clean python -I -B - --self-test
Get-Content -Raw -Encoding UTF8 reports/ml_p1_profile.py | docker run --rm -i --pull never --network none --read-only --cpus 1 --memory 1g -e OPENBLAS_NUM_THREADS=1 --mount "type=bind,source=C:\nyc-taxi-bigdata\data\gold,target=/gold,readonly" nyc-taxi-worker:p6-clean python -I -B - --gold-root /gold
```

ML P2 cần duyệt contract/cohort, CLI/schema + leakage/split/unknown tests, full dedup/anti-overlap,
image tag/hash inventory mới, artifact routing và tài nguyên rồi mới chạy baseline/GBT pilot.
**P1 không có MAE/RMSE/R²; chưa xác nhận training pipeline hoặc artifact round-trip PASS.**

Ghi chú lưu trữ ML P6: nội dung trên giữ nguyên kết luận tại P1. Script tái hiện
đã chuyển sang `scripts/profile_ml_data.py`; bản `reports/*.py` không có trong
Git clone. Aggregate `ml_p1_profile.json` được Git theo dõi riêng từ P6, giữ nguyên
bytes/SHA256 và LF làm freeze contract, không tái sinh profile hoặc chạy lại Gold.
Kết quả training/evaluation/serving về sau nằm ở các reports P2.1–P5.
