# PRETRIP V2 — training/validation report

Ngày 27/10/2026. Base HEAD của lượt train `36e7297c26be1c06235427c505cb58e5c6f5bb03`.
Báo cáo kết quả trước promotion/deployment. Acquisition/preparation không chạy lại. Đây là pilot
zone/time-only, **không phải model đã deploy hay accuracy cho yêu cầu hiện tại**.

## 1. Kết quả và gate đã chốt

Attempt `.sandbox/pretrip-v2/training-20261027-pilot-01/artifacts/`:
`TRAINED_NOT_DEPLOYED`, **1 real fit**, đủ **20 trees**. Không mở August,
không tuning/fit lại/chọn cấu hình sau khi xem metrics. GBT seed 42, maxIter 20,
maxDepth 4, stepSize 0.1, squared loss, subsampling 1, minInstancesPerNode 1,
maxBins 32, maxMemoryInMB 128; các tham số bổ sung đã cố định trước fit/metrics.

`lossType` thực tế đã xác minh **chỉ đọc artifact đã lưu**: metadata của
`org.apache.spark.ml.regression.GBTRegressionModel` (Spark 4.2.0), tại
`pipeline/stages/4_GBTRegressor_492685e36864/metadata/part-00000-0340e32a-db44-415c-b07a-9daefc221da4-c000.txt`,
có cả `paramMap.lossType="squared"` và `defaultParamMap.lossType="squared"`.
SHA256 file metadata khớp artifact manifest; `training_config.json` và
`training_report.json` cũng ghi `gbt_parameters.lossType="squared"`.
Không fit/predict/reload lại để xác minh; metrics/gate FAIL bên dưới giữ nguyên.

| Split | Model | Rows | MAE USD | RMSE USD | R² | P50 AE USD | P90 AE USD | P95 AE USD |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| June train | Global median | 26.573 | 7,955430 | 13,040237 | -0,105085 | 4,900000 | 19,600000 | 30,100000 |
| June train | PU/DO × 6h median | 26.573 | 4,685806 | 9,341241 | 0,432934 | 2,100000 | 11,200000 | 19,600000 |
| June train | GBT | 26.573 | 6,386564 | 8,786150 | 0,498325 | 5,249837 | 12,250163 | 17,850163 |
| July validation | Global median | 23.526 | 7,738003 | 12,606378 | -0,098534 | 4,900000 | 18,900000 | 28,700000 |
| July validation | PU/DO × 6h median | 23.526 | 5,131202 | 9,606163 | 0,362129 | 2,800000 | 11,900000 | 20,300000 |
| July validation | GBT | 23.526 | 6,321704 | 8,717798 | 0,474653 | 5,249837 | 11,881325 | 17,237995 |

Mọi model/split giữ toàn bộ rows; target và prediction không clip. Non-finite
labels/predictions và negative predictions đều **0**. Quantiles tính chính xác
trên sample bằng linear interpolation tại `(N-1)*p`, không phải approximate Spark.

Baseline tốt nhất validation là phân nhóm: MAE **5,1312024993624075**. Gate cần
GBT MAE ≤ **4,618082249426167** (cải thiện ≥10%). GBT thực tế
**6,321704389076525**, cải thiện **-23,201226%** (kém 23,20%) → **FAIL**.
GBT thắng global median 18,30% không đủ để PASS vì gate so baseline tốt nhất.
Không promote, không tự chọn baseline thay model phục vụ hoặc mở holdout.

## 2. Data boundary và predictors

- Loader chỉ mở `train_features.parquet`, `train_labels.parquet`,
  `validation_features.parquet`, `validation_labels.parquet` và hai JSON
  `preparation_report.json`/`preparation_checkpoint.json`. Worker không mount
  runtime cha, raw sources, `test_*.parquet` hay bất kỳ file August nào.
- SHA256, schema/column order, counts/caps, sample seed 42/1%, cohort-preparation
  identities, contract/calendar/TZif, actual pickup local June/July được kiểm lại.
  Arrow features/labels ghép theo row order đã pin trước khi tạo Spark frame;
  prediction thu về sort journey key và đối chiếu key với label trước metrics.
- Train **26.573**, validation **23.526**, train/validation retained-sample overlap
  **0**, key strictly sorted/unique. Đây không phải kiểm dedup/overlap toàn raw.
  Dedup/conflicts preparation trên sample đã bằng 0; không chạy preparation lại.
  August **21.833** chỉ là count checkpoint cũ, chưa đọc lại/predict/evaluate.
- Chỉ tám features: `pickup_hour_sin`, `pickup_hour_cos`, `pickup_dow_sin`,
  `pickup_dow_cos`, `is_weekend`, `is_holiday`, `pickup_zone`, `dropoff_zone`.
  Không có recorded distance, actual dropoff/duration, fares/phí/outcomes,
  journey key hoặc lineage trong vector.
- Hai StringIndexer `alphabetAsc/handleInvalid=keep` fit train → OneHotEncoder
  `dropLast=False/handleInvalid=keep` → VectorAssembler → GBT. Vector **351**
  dimensions: sáu numeric + 119 PU OHE + 226 DO OHE; binary categories không phải
  độ lớn LocationID. Dictionary train PU 117, DO 224; hai dimension bổ sung mỗi
  nhóm dành cho unknown từ indexer/encoder.
- Unseen train: **0**. Validation PU **32** (0,1360%), DO **25** (0,1063%),
  either **57** (0,2423%); không loại các rows này.
- Baselines exact train medians: global **$12,8**; PU/DO × khung giờ
  `00–05/06–11/12–17/18–23`, tối thiểu 5 rows, fallback PU/DO đủ 5 rồi global.
  Lưu **1.402** eligible pair groups và **1.601** eligible pair/time groups.
  Fit không nhận validation labels; baseline prediction không đọc label.

## 3. Identity, artifacts và fresh-worker reload

| Identity | Full SHA256 |
|---|---|
| Fitted pipeline tree | `2efee5c2c25769430078f15d9d2ab10db9022cba6fe9ee92dbe7559c8566bd05` |
| June/July sample aggregate | `7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86` |
| Contract `pretrip-v2-zone-time-1` | `a1ddbe55f0128c20d8189d9f37b3b6172e90f20be71ae7a99300e21fb306add5` |
| Calendar `us-opm-nationwide-observed-2025-2027-v1` | `00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac` |
| System TZif `America/New_York` | `e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95` |

Artifact manifest kiểm **53 files**: 44 pipeline files gồm metadata/Parquet/CRC
và 9 documents (baselines, sample lineage, config/code hashes, metrics, contract,
calendar, parity inputs/GBT reference/baseline reference). Training report,
checkpoint và manifest riêng ngoài danh sách tự hash; không có recursive self-hash.
Model tree identity là SHA256 canonical manifest của riêng pipeline entries
`path/bytes/sha256`, không phải hash ZIP và không phải hash rút gọn.

`training_config.json` giữ hash các code `pretrip_*.py`, base commit, toàn bộ
hyperparameters, runtime/image identity và sample aggregate. `sample_lineage.json`
giữ từng SHA256 sample, journey-set hashes, preparation metadata hashes và source
lineage đã có; không mở raw để tạo lineage mới. Sources TLC SHA256 vẫn là checksum
local tự tính từ acquisition trước, **không phải checksum TLC công bố**.

Fresh worker riêng, cùng image, chỉ mount artifacts read-only; không mount samples
hoặc gọi fit. Full manifest/file hashes/path guards/model-tree hash PASS.
Load fitted PipelineModel và JSON baselines; **16 inputs cố định** (8 train +
8 validation, đúng tám features; không upload dữ liệu) giữ prediction GBT sai lệch
**0 ≤ 1e-9**, baselines exact equality. Evidence:
`.sandbox/pretrip-v2/training-20261027-pilot-01/fresh-reload/training_report.json`
status **FRESH_RELOAD_VERIFIED**, `fit_calls=0`. Training report ban đầu ghi
`PENDING_FRESH_WORKER`; file reload riêng là bằng chứng bước sau, không sửa artifacts.

## 4. Tests, thời gian và tài nguyên thực tế

**149 distinct related tests PASS**, không skip/xfail và không chạy lại 112 tests
acquisition/preparation cũ hay toàn CI:

- 66 loader/pure model tests: sample/schema/hash/alignment, allowlist/leakage,
  train-only medians/fallback, all-row metrics/quantiles.
- 10 tiny Spark tests: categorical/unknown bucket, metadata/vector, validation,
  fixed actual params và save/reload/no overwrite. Synthetic fit dùng 1 tree cho
  unit test; real pilot ở trên dùng đủ 20 cây đúng một lần.
- 73 orchestration/PLAN/quota/path/scratch/FAILED checkpoint/safe logging/cgroup
  telemetry/CI-inventory tests. Inventory bổ sung tests offline, Spark fixture
  được kê riêng ngoài CI nhẹ; chưa chạy remote CI cho thay đổi chưa commit này.

Các lỗi harness gặp **trước** real training: hostname network-none; Spark native
thread creation với PID limit tự thêm 256; Snappy không mmap được native library
trên tmpfs noexec. Khắc phục scoped worker/test harness: loopback rõ ràng, PID512,
Java ActiveProcessorCount2 và native BLAS/OMP/MKL threads1, tmpfs `exec`.
Không tăng CPU/RAM/heap hay đổi GBT để vượt lỗi. Rerun chỉ tests bị ảnh hưởng;
real data chỉ fit **một** lần. PID snapshot khi pilot chạy **297** vượt 256;
không gọi đây là bằng chứng cgroup memory OOM. Log4j vẫn có cảnh báo hostname
không phân giải và BLAS native fallback, nhưng fit/save/reload hoàn tất;
không cài thêm thư viện để che cảnh báo.

| Phase | Seconds |
|---|---:|
| Verified Arrow load/pairing | 1,397 |
| Train-only baselines | 0,183 |
| Spark startup + cached joined frames/counts | 10,434 |
| Fixed GBT fit once | 19,039 |
| Train/validation metrics + unseen counts | 4,101 |
| Save fixed candidate/parity references | 20,191 |
| Train attempt total, gồm stop | **55,679** |
| Fresh worker verify/start/load/parity total | **17,647** |

Existing image `nyc-taxi-worker:ml-p3-20261023`, full ID
`sha256:d0b29b688b4255ae5d26aa18e23d25e917d24b029980f2e7e342267531cad55c`.
Python **3.11.17**, PySpark **4.2.0**, PyArrow **21.0.0**, Java **17.0.20.1**.
Code/config mounted read-only, **không build/pull/install/download**.
Worker network none/root read-only, 2 CPU, memory/swap total 3 GiB (swap0),
JVM max heap **1.610.612.736 bytes = 1536 MiB**, local[2], SQL timezone NYC,
shuffle/default partitions2, Spark UI/event log off, native CPU/thread limits.
Peak cgroup RAM train **1.592.074.240 bytes = 1518,32 MiB**; reload
**793.694.208 bytes = 756,93 MiB**. Python train peak RSS **226,78 MiB**.
`memory.events`: max/oom/oom_kill/oom_group_kill đều **0** ở cả hai worker.

Disk dùng lại **cùng envelope 2.147.483.648 bytes**:

- Trước: sources + samples + metadata **191.797.209 bytes / 182,91 MiB**.
- Artifacts/report/checkpoint mới + reload evidence: **458.305 bytes / 0,437 MiB**.
- Sau: **192.255.514 bytes / 183,35 MiB**, còn **1864,65 MiB**; không cấp mới 2048.
- Quota artifact trừ prior bytes một lần và **reserve toàn bộ scratch ceiling
  512 MiB** trước ghi. Managed sink/checkpoint giữ metadata reserve 64 KiB;
  pipeline JVM lưu vào bounded tmpfs rồi copy qua guarded sinks. Cả atomic
  checkpoint và bản staging tồn tại cùng lúc đều được tính/bound.
- Scratch tmpfs peak thực tế **chưa đo high-water**; không thay ceiling thành
  số đo. Conservative managed peak + toàn ceiling khoảng **695,351 MiB**, dưới
  envelope. Đây là logical task-file bound, không giới hạn cả C/VHDX/pagefile.
- C trống snapshot trước/sau **78,02794 → 77,99321 GiB** (giảm 35,56 MiB toàn ổ);
  không quy toàn bộ biến động đó cho file model 0,437 MiB. Host available RAM
  **2299,86 → 865 MiB**, commit headroom **13,60955 → 13,53952 GiB**; đây là
  snapshots toàn máy, không phải peak RAM pilot. Không áp ngưỡng host 2 GiB.

## 5. Files và giới hạn bàn giao

Code mới: `ml/pretrip_training_data.py`, `ml/pretrip_training_model.py`,
`ml/pretrip_training.py`; CLI `scripts/train_pretrip_pilot.py`; bốn training test
files. Chỉ chỉnh CI inventory + inventory assertion, README và report này.
Artifacts/sample/runtime ignored, không stage/commit/push.

PLAN (không sample/Spark/writes): `python /app/scripts/train_pretrip_pilot.py`.
Thực thi đã dùng `--execute --output-dir .sandbox/pretrip-v2/training-20261027-pilot-01/artifacts
--prior-task-bytes 191797209 --code-commit 36e7297c26be1c06235427c505cb58e5c6f5bb03
--runtime-image-id <full ID trên>` trong worker/mounts/limits nêu trên.
Reload dùng `--reload-artifacts <artifacts> --output-dir <attempt>/fresh-reload
--prior-task-bytes 192250170`, cùng image; không fit/sample mount.
Không chạy lại đường dẫn đã tồn tại: CLI từ chối overwrite; PLAN mặc định.

Containers thử `--rm`, private scratch dọn trong finally; không còn container
`nyc-pretrip-v2-*`. Bốn services production giữ nguyên full IDs/StartedAt/healthy:

| Service | ID prefix (full IDs đã đối chiếu) | StartedAt UTC trước = sau |
|---|---|---|
| postgres | `930e1ad167c9` | `2026-10-26T02:07:31.856555874Z` |
| mlflow | `6db6f54eda60` | `2026-10-26T02:07:31.839462412Z` |
| dashboard | `b2fe708bedf6` | `2026-10-26T10:14:07.988884398Z` |
| fare-inference | `a9b1b04d73e6` | `2026-10-26T03:55:50.019525219Z` |

15 protected-file SHA comparisons trước/sau đều unchanged, gồm `.env`, PBIX,
PACKAGE P3 files, v1 pilot/data/inference, Silver và model bundle lock.
Không mount artifacts model gốc, Gold hoặc database; không gọi Mapbox/inference/
DB và không thao tác vòng đời services/volumes. README giữ hunk PACKAGE P3 cũ.

**August chưa đánh giá. V2 chưa được phục vụ. Gate FAIL là blocker promotion;
không tuyên bố pilot này hỗ trợ hiện tại hay dùng MAE v1/June làm bảo đảm sai số.**
