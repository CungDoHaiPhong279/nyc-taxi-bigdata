# ML P2.1 REPORT — preparation + pilot PASS

Ngày kiểm chứng: **2026-10-23**. Chỉ một attempt mới trên Gold; không tuning, tăng sample/tài nguyên, hoặc dùng June cho prediction/metrics. P2 cũ giữ nguyên.

## 1. Timeout cũ: bằng chứng và giới hạn

P2 cũ: cohort/hash/cache **694,802 s**, full-cohort dedup **582,065 s**, overlap **486,648 s trước ngắt**, Docker wall **1.813,99 s**, exit **124** ở deadline 1.800 s. Sampling và GBT chưa chạy. Code cho thấy hai cache DISK_ONLY toàn cohort 16.797.544 dòng, window sort theo business key và explode hai loại identity (~33,6 triệu dòng) trước sampling.

Không có execution plan/event log/task metrics cũ được lưu. Console chỉ ghi task denominators 1/9/8; không đủ chứng minh skew hay Windows I/O là nguyên nhân riêng. Plans cũ được **dựng lại bằng EXPLAIN, không action** trong attempt mới, không chạy lại job lỗi. Peak cũ 2,146 GiB/3 GiB và timeout stack không cho thấy OOM. `END` trong log cũ phát từ finally, không phải bằng chứng stage thành công.

## 2. Thay đổi tối thiểu và tính tương đương

- `pilot_data.py`: group-only SHA giữ canonical v1; cố định `pmod(xxhash64(lit(42), group_key),1000000)<10000` trước business SHA 20 trường/shuffle; aggregate waterfall và full split counts chung một scan; expose overlap plan dạng lazy.
- `pilot.py`: chế độ `--preparation-mode sample-first`, chỉ materialize mẫu; tách timings/status/job groups và lưu plans/event log. Preprocessing, baseline và GBT giữ nguyên P2, không đổi `pilot_model.py`.
- `pilot_diagnostics.py` + tests: EXPLAIN không action, streaming event summaries có kiểm tra lỗi; thêm inventory CI nhẹ. Tests data bổ sung membership/key/dedup commutation/canonical source/NULL/±0/metadata/repartition và cross-split guard.
- `run_ml_pilot.ps1` + ML Compose override: `-Attempt p21`, tag/output/job names riêng; private Spark scratch `/tmp/ml-p21-spark`. Mặc định P2 cũ giữ nguyên. Không thay base Compose/services/volumes.

Trong contract identity P2, duplicate cùng 20 business fields có cùng 5 journey fields, nên cùng group hash và quyết định sampling. Filter giữ/bỏ nguyên nhóm, vì vậy `dedup(sample(C)) = sample(dedup(C))`, kể cả source canonical. SHA256 và mọi canonical normalization giữ nguyên, không thay bằng load ID. Physical plan thực tế xác nhận **Project business SHA/format_string ở trên Filter sampling**, không có full-cohort shuffle trước filter.

Cohort vẫn fare_amount finite >0, standard rate 1, zones 1–263, finite distance [0,200] mile; giữ distance=0 và không cap fare. Pickup giữ TIMESTAMP_NTZ wall-clock NYC-assumed, không chuyển thành UTC. Target là cước cơ bản USD, theo distance người dùng cung cấp, không phải routing/total_amount.

## 3. Counts và phạm vi guards

| Actual pickup split | Eligible toàn cohort | Sample trước dedup | Sample sau dedup | Loại trùng trong sample |
|---|---:|---:|---:|---:|
| Train Jan–Apr | 11.053.615 | 110.356 | 110.356 | 0 |
| Validation May | 3.008.396 | 29.979 | 29.979 | 0 |
| June test | 2.735.533 | 27.310 | 27.310 | 0 |
| Tổng | 16.797.544 | 167.645 | 167.645 | 0 |

Gold đầu vào 23.886.265 dòng; manifest + 24 file size/mtime/footer counts khớp P1, watermark/cohort/split counts khớp tuyệt đối. Split actual pickup: [Jan1,May1), [May1,June1), [June1,July1).

Temporal guard PASS. **Business và journey overlap/invalid counters đều 0 trên mẫu đã dedup**, gồm cả ba cặp train–validation, train–June, validation–June. Không kiểm chứng lại full-cohort overlap hoặc full-cohort dedup trong P2.1. P2 cũ đã ghi full dedup loại 0 nhưng overlap chưa xong; không gọi đó là full-overlap PASS.

## 4. Timings, plan/task evidence và tests

| Bước mới | Giây |
|---|---:|
| Metadata + EXPLAIN, không scan rows cũ | 14,465 |
| Read/filter/profile + full split counts | 12,162 |
| Hash sampling + lượt đọc thứ hai + business SHA/materialize mẫu | 135,291 |
| Sample dedup | 5,492 |
| Sample temporal guard | 0,713 |
| Sample overlap guard | 8,196 |
| Cached-sample partition diagnostics | 0,751 |
| Train-only preprocessing + median | 8,192 |
| GBT fit | 35,331 |
| Train/validation metrics | 3,817 |
| Model save/artifacts/reload trong job | 42,013 |

Preparation kể cả metadata/diagnostics **177,070 s**; report elapsed **281,965 s**, Docker pilot wall **300,230 s** (exit 0). Fresh-worker reload thêm **37,189 s**, exit 0. Không so các phase có scope khác nhau như benchmark full-overlap tương đương. OS/cache không bị reset; đây không phải benchmark cold-cache hoặc thí nghiệm tách riêng lợi ích scratch Linux.

Nguồn Gold **9 scan partitions**, shuffle/default parallelism **8**. Cached sample có 8 partitions, **20.815–21.063 rows/partition**. Event log đóng: 239 stage attempts, 1.586 TaskEnd-attempt records, 0 failed/retry attempts; không coi stage IDs/progress là số action chính xác. Stage 1 và 4 mỗi stage đọc 23.886.265 records: xác nhận đúng **hai lượt Gold lớn**, các action tiếp theo dùng sample cache/model data. Tổng shuffle write **68.358.730 bytes**, read **68.353.066 bytes**, memory/disk spill **0**.

Sampling stage: tổng executor CPU **231,828 s**, executor runtime **260,586 s**; task max/median **1,617** (53,211/32,912 s). Đây là số đo attempt mới, không chứng minh skew/I/O của attempt cũ; input-byte counters không được diễn giải thành physical disk throughput. Công việc còn lớn nhất là scan/hash sampling, không phải GBT.

Giữ **local[2], 2 CPU, container 3 GiB, JVM heap max 1.536 MiB**, deadline 1.800 s. Training cgroup peak ghi nhận **1,705 GiB**, Python peak **189,82 MiB**; không suy ra native overhead chính xác từ hiệu số max heap. Fresh-worker cùng limits. Dependencies không đổi: Python 3.11.17 / Java 17.0.20.1 / PySpark 4.2.0 / MLflow 3.5.1.

**105 ML tests PASS** trong worker / 173,55 s, không Spark ETL suites. Sau parser layout fix: **68 parser/tracking tests PASS** trong refreshed image / 0,41 s; host diagnostics+CI inventory **51 PASS**, helper/tracking trước đó **91 PASS** (các lượt có tests trùng). Compose **10 checks PASS**, PowerShell parse/PLAN, pip check/CLI help, hash probe **51 files PASS**. Một lệnh test read-only ban đầu thiếu writable temp; thêm private tmpfs rồi chạy lại PASS, không bỏ tests.

## 5. Model và metrics

9 features giữ P2: distance; sin/cos hour; sin/cos weekday; weekend; holiday; hai nominal zone indices. StringIndexer chỉ fit train, unknown=keep, metadata categorical được kiểm tra (178 PU/243 DO categories gồm unknown); validation có 9 unknown PU và 6 unknown DO, không drop. Median baseline train sample **12,1 USD**, relative error 0,001. GBT absolute loss, seed 42, 20 iterations, depth 4, stepSize 0,1, subsampling 0,8, minInstancesPerNode 20, maxBins 512, maxMemoryInMB 128.

| Split | Model | MAE USD | RMSE USD | R² |
|---|---|---:|---:|---:|
| Train | Median baseline | 7,1170 | 11,9936 | -0,0938 |
| Train | GBT | 1,9163 | 2,9466 | 0,9340 |
| Validation | Median baseline | 7,8874 | 13,2228 | -0,1427 |
| Validation | GBT | 2,4153 | 3,8018 | 0,9055 |

Cùng 110.356 train/29.979 validation rows cho hai mô hình; không clip label/prediction hoặc bỏ ca khó. Validation MAE giảm **5,4721 USD (69,3772%)**, chọn GBT làm ứng viên nhưng **không promote**. Fixed sample không có fare>1.000 USD trong train/validation (max 126,2/157 USD), dù cohort vẫn giữ tails; không tuyên bố metrics đại diện rare tails hoặc June.

## 6. Reproduction, MLflow, reload và bảo toàn

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1 -Attempt p21
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1 -Attempt p21 -Build -TestOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run_ml_pilot.ps1 -Attempt p21 -Execute
```

Đây là các lệnh đã thực thi, không phải yêu cầu retry: attempt marker cố ý chặn ghi đè cùng output. Output: `models/ml-p2/20261023-pilot02/`, Git ignored. Code-refresh từ clean dependency base P6.2, không clean-build lại dependencies.

- Experiment ID `1`, probe **`f399b3239c40403b8d71a88f103bfea5` FINISHED**; fresh-worker download và server SHA PASS.
- Pilot **`d2772f1904d7472490a73b0ac1c0c5d4` FINISHED**; API audit: **35 parameters, 12 metrics**.
- Artifact routing giữ file URI/shared **subpath** `/mlflow/artifacts/ml-p2-20261022`; không mount SQLite hoặc đổi server flags. Model, fitted preprocessing, baseline, contract/evaluation và plans/diagnostics được log.
- Reload trong job và worker mới: **16 validation predictions, max absolute diff 0, tolerance 1e-9**; baseline tải lại khớp. June chỉ counts/keys/overlap, **chưa predict/metrics**.
- Image dùng training/reload: `sha256:04374b74b5b016f54edf3436cfd49486750da310c89fed5e9fd32c3178846453`, giữ thêm tag `ml-p21-trained-20261023`. Parser fix sau training: tag `ml-p21`, ID `sha256:cf43bf6f83e35b2f3ed955c204f7ff93fde84d597164d20a5a71f5edafb49967`. Audit xác minh bốn training modules vẫn khớp hashes recorded; chỉ diagnostics được sửa, **không retrain**.
- Spark thực tế ghi single-file `local-*`; parser đầu chỉ tìm `events_*`, nên giữ `execution_diagnostics.json` ban đầu để truy vết. File authoritative là **`execution_diagnostics_reparsed.json`**: đọc lại raw event log đã đóng, 0 parse/read errors, complete=true. Upload/download SHA256 khớp `746bfdf29c0219b61070d94f7e2159b3ed7bc05118ed5dd29a50773e9fcd37ac`; audit tại `final_tracking_audit.json`. Raw event log không bị xóa.

Trước/sau: cả **5 services thật/thử healthy**, IDs/StartedAt/mounts/ports không đổi. Ba services thật vẫn `930e1ad167c9` PostgreSQL, `6db6f54eda60` MLflow, `cd02addf38c8` dashboard; hai named volumes thật không đổi. Hash .env/PBIX/manifests/base Compose/dependencies và IDs tags cũ khớp checkpoint. Toàn bộ **16 files output P2 cũ**, run `95b4b9099c5843bbb608769b31d1f281` FAILED và artifact metadata **không đổi**. Không ghi ETL/Gold/PostgreSQL, recreate services, xóa volume, commit/push; không còn ML job nền. MLflow chỉ thêm runs/artifacts mới được yêu cầu.

Giới hạn: chỉ pilot 1%, temporal validation May; chưa cold-cache benchmark, full-cohort overlap hoặc test June. Không có bằng chứng để quy timeout cũ riêng cho Windows I/O/skew. Không triển khai serving/dashboard/training bổ sung trong bước này.
