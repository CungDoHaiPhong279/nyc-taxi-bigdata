# ML P2 REPORT — pilot chưa hoàn tất

Ngày kiểm chứng: 2026-10-22. Không commit/push, promote model hoặc tích hợp dashboard.

## 1. Code và lệnh

- `src/nyc_taxi/ml/pilot{,_data,_model,_tracking}.py`: CLI orchestration, cohort/identity/splits, features/model/metrics, artifact guards.
- `scripts/train_pilot.py`, `scripts/run_ml_pilot.ps1`: explicit worker jobs; mặc định PLAN, guards/hash/resource/deadline; không mount source.
- `Dockerfile.ml`, `docker-compose.ml-pilot.yml`: code-refresh từ clean worker P6.2, ML-only tag/limits/mounts; không sửa base Compose/services.
- `tests/test_ml_pilot_{data,model,tracking}.py`; cập nhật CI inventory/Compose helpers và tests, không thêm Spark vào CI nhẹ.
- `scripts/profile_ml_data.py`: bản sao byte-identical script khảo sát P1; SHA256 `149be4ff19c4f0b349e7063f64769ef032cba83b2d4bdb2cbdf6731aab8cd665`. Bản gốc/profile JSON giữ nguyên.
- README và ignore exceptions hẹp cho thiết kế P1/báo cáo này; không đưa runtime/model/sample vào Git.

Các lệnh đã chạy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1 -Build -TestOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1 -Execute
.\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider tests/test_ci_python.py tests/test_ci_compose.py tests/test_ml_pilot_tracking.py
.\.venv\Scripts\python.exe -B scripts/ci_validate_compose.py
docker run --rm --network none --read-only --memory 512m nyc-taxi-worker:ml-p2 python -B -m pip check
docker run --rm --network none --read-only --memory 512m nyc-taxi-worker:ml-p2 python -B scripts/train_pilot.py --help
docker run --rm --network none --read-only --memory 512m nyc-taxi-worker:ml-p2 python -B scripts/profile_ml_data.py --self-test
```

Python helper/Compose checks dùng `.venv/Scripts/python.exe` hiện có trên host; mọi ML/Spark program và survey self-test chạy trong image. Không cài dependencies mới. `-Execute` bây giờ bị attempt marker chặn có chủ ý, không phải lệnh retry.

## 2. Cohort, khóa, split, sample

Gold snapshot: 23.886.265 dòng, 24 Parquet files, sáu source months; manifest SHA256 `bb755aa6181615127c8d06454094789cb862a818a2862784a558fd46f08b57ff` khớp freeze contract P1.

Cohort khớp hoàn toàn waterfall P1: actual pickup Jan–Jun/2025; finite fare > 0; finite distance [0,200] mile; ratecode 1; PU/DO zones 1–263. Không loại zero-distance hoặc cap fare. Gold được mount read-only. `TIMESTAMP_NTZ` giữ wall-clock NYC-assumed; session UTC chỉ cố định serialization, không chuyển pickup thành UTC.

| Actual pickup split | Trước dedup | Sau dedup | Loại trùng | Sample cuối |
|---|---:|---:|---:|---|
| Jan–Apr train | 11.053.615 | 11.053.615 | 0 | Chưa có |
| May validation | 3.008.396 | 3.008.396 | 0 | Chưa có |
| June test | 2.735.533 | 2.735.533 | 0 | Chưa có |
| Tổng | 16.797.544 | 16.797.544 | 0 | Chưa có |

Business SHA256 dùng 20 original fields: vendor; pickup/dropoff NTZ; passenger; distance; ratecode; store-and-forward; PU/DO zones; payment; fare; extra; MTA tax; tip; tolls; improvement surcharge; total; congestion surcharge; airport fee; CBD fee. Không dùng ingestion/load IDs hoặc source lineage. Canonical JSON versioned: explicit NULL, 6 fractional timestamp digits, normalized signed zero/scientific doubles.

Journey/group SHA256 dùng vendor + pickup/dropoff NTZ + PU/DO zones, bỏ label/charges. Split dựa actual pickup [Jan1,May1), [May1,June1), [June1,July1). Phép thử giao tập theo cả hai key **chưa hoàn tất**; không khẳng định overlap=0. Fixed sample được triển khai là `pmod(xxhash64(lit(42), group_key),1000000)<10000`; cùng journey có cùng quyết định. Hard caps train/validation/test 200k/50k/50k. Job dừng trước khi lấy được sample counts.

## 3. Features, model và metrics

Contract là ước tính **fare_amount**, cước cơ bản USD, theo distance do người dùng cung cấp; không phải total_amount, định tuyến hoặc mô hình trước chuyến tự biết quãng đường. Inputs: distance, pickup wall-clock, PU/DO zones. Vector 9 trường: distance; sin/cos hour; sin/cos weekday; weekend; holiday; hai categorical zone indices. Allowlist/projection loại target và leakage khỏi vector; dropoff time chỉ dùng identity, không làm feature.

StringIndexer chỉ fit train, unknown=`keep`; VectorAssembler giữ nominal metadata cho zones (không coi IDs là thứ tự địa lý). Baseline median chỉ dùng train sample, quantile relative error 0,001. GBT fixed: absolute loss, seed 42, 20 iterations, depth 4, stepSize 0,1, subsampling 0,8, minInstancesPerNode 20, maxBins 512, maxMemoryInMB 128. Đây là cấu hình **đã triển khai, chưa fit trên Gold**. Không clip label/prediction hoặc lọc ca khó khi đánh giá.

| Split/model | MAE USD | RMSE USD | R² |
|---|---|---|---|
| Train / median baseline | Chưa chạy | Chưa chạy | Chưa chạy |
| Train / GBT | Chưa chạy | Chưa chạy | Chưa chạy |
| Validation / median baseline | Chưa chạy | Chưa chạy | Chưa chạy |
| Validation / GBT | Chưa chạy | Chưa chạy | Chưa chạy |

Chưa thể tính cải thiện MAE hoặc chọn ứng viên. June **chưa inference/evaluate**; `test_evaluated=false`.

## 4. MLflow và artifact

- Experiment `nyc-taxi-distance-pilot-ml-p2-20261022`, ID `1`.
- Probe run `41d703f04f0644ce8c2d4e7e6ff62138`: **FINISHED**, 88-byte artifact; download ở worker khác + SHA256 trên server PASS.
- Pilot run `95b4b9099c5843bbb608769b31d1f281`: **FAILED**, API audit xác nhận 32 parameters, **0 metrics**.
- File artifact root `file:///mlflow/artifacts/ml-p2-20261022`; worker chỉ mount subpath `artifacts/ml-p2-20261022` của volume MLflow cũ, không mount SQLite. Docker HostConfig xác nhận Subpath thực tế.
- API DNS `mlflow` bị Host security chặn dù health OK; dùng current private IP được xác minh trong network/project, chỉ cho job. Không disable security, đổi .env hoặc restart server. Proxy default `/mlartifacts` không dùng vì ngoài named volume.
- Aggregate failure report đã upload vào đúng FAILED run sau deadline; tải lại SHA256 khớp `82fdc6286c402444e30534d6f9fbf17836f25b64448c8f01e36d83c8826ee061`. Đây **không** là model reload PASS. Fitted preprocessing/model/baseline chưa được tạo; model reload chưa chạy.

## 5. Kiểm chứng, thời gian, tài nguyên

- 81 ML tests PASS trong worker: 12 data, 22 model (có tiny synthetic GBT fit), 47 tracking mocks; 131,18 giây. Không chạy Spark ETL suites.
- Lượt host helper/tracking: 91 PASS / 3,00 giây; 47 tracking tests trùng lượt trên. Compose 10 checks PASS; final PowerShell parse, diff whitespace checks, pip check và survey self-test PASS.
- Lượt test đầu lỗi anonymous temporary file trên Windows bind mount trước collection; chuyển TMPDIR/pytest temp sang `/tmp` riêng trong container. Outputs/shuffle vẫn nằm trong runtime ML riêng.
- Một real pilot duy nhất: Docker wall 1.813,99 giây, exit **124** tại GNU timeout **1.800 giây**. Report elapsed 1.787,289 giây (từ sau API/import startup). Console xác nhận SIGTERM handler `TimeoutError`; Py4J bọc thành `Py4JError` trong `collectToPython`.
- Gold/cohort/hash: 694,802 giây; dedup: 582,065 giây; overlap/sample: 486,648 giây trước ngắt. Chưa fit baseline/GBT. Không retry, tăng budget hoặc tuning để PASS.
- Actual limits: 2 CPU, container 3 GiB, JVM heap max 1.536 MiB. Cgroup peak **2,146 GiB**, Python peak 183,76 MiB; peak bao gồm JVM/native/Python/cache, không coi hiệu số với max heap là overhead chính xác. Không có bằng chứng OOM; nguyên nhân dừng là deadline.
- Runtime Python 3.11.17 / Java 17.0.20.1 / PySpark 4.2.0 / MLflow 3.5.1 / NumPy 2.4.6 / Arrow 21.0.0 / sklearn 1.9.1. Không đổi dependency inputs.
- Pilot image ID `sha256:c8283904f662b63404999d506d78d1b6bbfefd390b2c4aaf0833b2e9c6b6b4d1`; sau lượt lỗi, code-refresh bổ sung bản survey Git-trackable/hash input, latest ML image `sha256:86fb7cf33214399ae823c0089c123726c83cadaae2f3b75f538919192ccca48d`, 2.819.545.427 bytes, hash probe **49 files PASS**. Không training lại bằng image refresh này. Đây không phải clean dependency build mới.

## 6. Bảo toàn và giới hạn

Checkpoint trước/sau khớp IDs, StartedAt, mounts, ports, health của cả 5 services thật/thử. Ba services thật vẫn healthy:

| Service | ID trước = sau | StartedAt không đổi (UTC) |
|---|---|---|
| PostgreSQL | `930e1ad167c9` | 2026-10-19T13:21:06.864413476Z |
| MLflow | `6db6f54eda60` | 2026-10-13T15:39:00.627559898Z |
| Dashboard | `cd02addf38c8` | 2026-10-20T13:27:57.469874792Z |

Hashes .env, PBIX, bốn manifests, base Compose/overrides và dependencies khớp checkpoint; image IDs của ba worker tags p2/p6-test/p6-clean cũng không đổi. Hai named volumes thật vẫn cũ. P1 profile/archive SHA không đổi. Không ghi PostgreSQL/Gold, chạy ETL/training khác, thay dashboard, xóa volume hoặc để ML job chạy nền. MLflow chỉ thêm experiment/run/artifact ML được yêu cầu; không tuyên bố nội dung volume MLflow không đổi.

**P2 chưa PASS end-to-end.** Cần bước riêng xem lại chi phí full-cohort identity/dedup/overlap và thử private Linux temporary shuffle thay Windows bind mount (hiệu quả chưa benchmark), giữ nguyên cohort/keys/seed/sample/hyperparameters. Thiết kế attempt mới có provenance, không xóa marker/ghi đè run lỗi. Chỉ sau đó mới chạy pilot, so baseline/GBT và kiểm chứng model reload; June tiếp tục held out. Không tự mở rộng tuning hoặc đánh giá June trong bước này.
