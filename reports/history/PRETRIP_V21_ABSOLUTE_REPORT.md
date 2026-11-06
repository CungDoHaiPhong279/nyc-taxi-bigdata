# PRETRIP V2.1 — absolute-loss experiment

Ngày 27/10/2026; base HEAD `63359e381a9e015a535dcfae7cf2675e7e168dde`.
Giả thuyết được chốt trước: chỉ đổi effective GBT `lossType` squared → absolute;
không grid search hoặc thử cấu hình khác sau metrics. **Đúng một real fit, Gate FAIL.**
July là validation đã dùng để phát triển model, **không phải test độc lập**.
August chưa mở/mount/predict/evaluate; không promote, deploy hoặc phục vụ V2.1.

## 1. Profile, input và kiểm soát drift

- CLI cũ `scripts/train_pretrip_pilot.py` thêm `--loss-profile absolute` và
  `--reference-artifacts <pinned-squared-artifacts>`. Mặc định vẫn squared và PLAN;
  PLAN không mở sample/reference, Spark hoặc ghi file. Absolute execute thiếu
  reference dừng trước tạo output. Reload suy ra loss từ saved config, không
  đổi model; legacy squared config không cần có trường `loss_profile`.
- Attempt mới `.sandbox/pretrip-v2/training-20261027-absolute-01/artifacts/`,
  không overwrite V2 squared `.sandbox/pretrip-v2/training-20261027-pilot-01/`.
- June **26.573 train**, July **23.526 validation**, Arrow pairing/key ordering,
  schema, từng sample hash, toàn lineage, contract/calendar/TZif khớp tuyệt đối V2.
  Sample identity `7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86`.
  Chỉ bốn sample Parquet June/July + hai metadata preparation được mount RO;
  không raw hay August. Không chạy lại acquisition/preparation/dedup.
  Train/validation retained-sample overlap 0; không gọi đây là kiểm toàn raw.
- Tám features, sáu numeric + PU/DO categorical; preprocessing không đổi:
  train-only alphabetical StringIndexer/unknown bucket → OHE keep/dropLast=False
  → VectorAssembler → GBT. Vector 351 dimensions; train dictionary PU117/DO224
  khớp trước/sau fit với indexer Parquet V2 squared (đọc Arrow, không load/predict
  squared model). Dictionary SHA256
  `aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c`.
- Baselines fit train, artifact deep equality và **toàn bộ baseline metrics
  train/validation exact equality** với V2: median global $12,8; median PU/DO ×
  khung 6h đủ 5 rows, fallback PU/DO đủ 5 rồi global. Không đổi policy.
- GBT giữ seed42/maxIter20/maxDepth4/stepSize0.1 và các defaults khác của
  cùng Spark runtime, subsampling1/minInstancesPerNode1/maxBins32/maxMemory128;
  effective `lossType=absolute` là **tham số GBT duy nhất thay đổi**.
  Effective parameter dict và đủ 20 trees được kiểm sau fit.

`training_config.json` được ghi trước `fixed_gbt_fit_once`, không cập nhật bằng
metrics sau fit; hash kiểm lại không đổi:
`dd19afd62b1653a2159c801f541adff709296f9e62d333ed99f4015bb7b3e3af`.
Config giữ sample/reference/runtime/code identities, tham số, baseline policy và
gate đã chốt. Kết quả/gate thực tế nằm ở report/metrics, không ghi đè config.

## 2. Metrics toàn bộ rows

Tất cả lỗi bằng USD; quantiles exact linear interpolation `(N-1)*p`, không clip
target/prediction, không loại rows khó. Squared là metrics **đã lưu của V2**:
không fit/predict squared lại để lập bảng.

| Split | Model | Rows | MAE USD | RMSE USD | R² | P50 AE USD | P90 AE USD | P95 AE USD |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| train | Global median | 26573 | 7,955430 | 13,040237 | -0,105085 | 4,900000 | 19,600000 | 30,100000 |
| train | PU/DO × 6h median | 26573 | 4,685806 | 9,341241 | 0,432934 | 2,100000 | 11,200000 | 19,600000 |
| train | V2 squared GBT | 26573 | 6,386564 | 8,786150 | 0,498325 | 5,249837 | 12,250163 | 17,850163 |
| train | V2.1 absolute GBT | 26573 | 6,574013 | 9,320652 | 0,435431 | 5,014254 | 13,185746 | 19,039446 |
| validation | Global median | 23526 | 7,738003 | 12,606378 | -0,098534 | 4,900000 | 18,900000 | 28,700000 |
| validation | PU/DO × 6h median | 23526 | 5,131202 | 9,606163 | 0,362129 | 2,800000 | 11,900000 | 20,300000 |
| validation | V2 squared GBT | 23526 | 6,321704 | 8,717798 | 0,474653 | 5,249837 | 11,881325 | 17,237995 |
| validation | V2.1 absolute GBT | 23526 | 6,468402 | 9,138135 | 0,422771 | 5,014254 | 12,485746 | 18,785746 |

Gate cần MAE validation ≤ **4.618082249426167 USD**, từ baseline tốt nhất
**5.1312024993624075 USD**, cải thiện tối thiểu 10%. Absolute thực tế
**6.46840185670763 USD**: cải thiện **-26,060156%** → **FAIL**.
So squared **6.321704389076525 USD**, absolute kém **2,320537%**.
Giả thuyết cải thiện MAE không được xác nhận trong sample/cấu hình này;
không suy rộng rằng mọi absolute-loss model đều kém, không tiếp tục tuning.

Non-finite labels/predictions và negative predictions **0 ở mọi model/split**.
Unseen: train0; validation PU32/DO25/either57 (0,2423%), giống V2, giữ mọi row.

## 3. Artifacts và fresh-worker reload

Fitted absolute model tree SHA256:
`3f9a9ef33c4dda83b07d18efa4bc79b983324cddf287d79a5581151d4785be81`.
Reference squared tree:
`2efee5c2c25769430078f15d9d2ab10db9022cba6fe9ee92dbe7559c8566bd05`.
Model-tree hash là canonical manifest của pipeline files, không hash rút gọn.

Metadata đã lưu tại
`pipeline/stages/4_GBTRegressor_af568d0628bd/metadata/part-00000-3d71b09d-36f6-4c51-a94a-b16c92ad7d2a-c000.txt`:

- Class `org.apache.spark.ml.regression.GBTRegressionModel`, Spark4.2.0.
- `paramMap.lossType="absolute"` (**effective**, đã kiểm).
- `defaultParamMap.lossType="squared"` (mặc định thư viện, không phải loss lần fit).
- Metadata SHA256
  `abd521807856fe56bf135f86ea7c9918cb8333f56b890e8622268df2d0b14f89`.

Artifact manifest mới giữ pipeline/CRC, baselines, config/code hashes, sample
lineage/contract/calendar/TZif identity, metrics và parity references riêng.
Fresh worker cùng image, artifacts RO, **không mount samples/reference squared**,
không fit: full manifest/model hash và saved loss PASS; **16 inputs** train/val
cố định khớp prediction V2.1 với max absolute difference **0 ≤ 1e-9**;
baseline reload exact. Không yêu cầu prediction bằng model squared.
`fresh-reload/training_report.json`: `FRESH_RELOAD_VERIFIED`, `fit_calls=0`.

## 4. Tests, runtime và dung lượng

**76 tests chọn lọc PASS, 108 deselected**, 29,93 giây; không chạy toàn bộ 149
tests hoặc CI cũ. Gồm profile/defaults, no-I/O PLAN, pre-fit config, parameter/
lineage/baseline/dictionary drift, saved paramMap vs defaultParamMap và synthetic
absolute/squared reload. Synthetic fixtures có tiny fit; real sample chỉ fit1.
New reference tests được thêm vào CI inventory hiện có, không thêm workflow/job.

Runtime giữ nguyên image sẵn có `nyc-taxi-worker:ml-p3-20261023`, ID
`sha256:d0b29b688b4255ae5d26aa18e23d25e917d24b029980f2e7e342267531cad55c`;
Python3.11.17/PySpark4.2.0/Java17.0.20.1/PyArrow21.0.0. Source/config RO (không
bake code mới), network none/root read-only; 2 CPU, memory/swap total3GiB/swap0,
heap1536MiB, PID512, Java ActiveProcessorCount2, BLAS/OMP/MKL threads1,
tmpfs `/tmp:rw,exec,nosuid,nodev,size=512m`, Spark UI/eventlog off, partitions2.
Runtime identity so với config squared khớp trước fit.

| Phase | Seconds |
|---|---:|
| Arrow verify/pairing | 1,218 |
| Train-only baselines | 0,186 |
| Locked squared reference guards | 1,616 |
| Spark start/frames/counts | 10,386 |
| Freeze config | 0,121 |
| GBT fit once | 16,085 |
| All-row metrics/unseen | 3,735 |
| Save | 21,953 |
| Train total gồm stop | **55,873** |
| Fresh reload total gồm stop | **19,383** |

Train peak cgroup memory **1.351.753.728 bytes = 1289,13 MiB**; reload
**793.411.584 bytes = 756,66 MiB**. Python train RSS peak231,23MiB.
Memory max/oom/oom_kill/oom_group_kill counters đều0. Các cảnh báo
Log4j hostname/BLAS native fallback cũ không ngăn fit/save/reload; không cài
thư viện, tăng limits hoặc sửa dịch vụ để che cảnh báo.

Envelope vẫn **2048 MiB**, tính đủ V2 sources/samples/artifacts/evidence cũ:

- Trước **192.255.514 bytes = 183,349 MiB**.
- Mới **454.297 bytes = 0,433 MiB**, gồm absolute artifacts/checkpoint/report/reload.
- Sau **192.709.811 bytes = 183,782 MiB**, còn **1864,218 MiB**.
- Managed quota trừ prior bytes (gồm reference bundle đã tính một lần) và reserve
  toàn bộ scratch ceiling512MiB trước ghi; giữ metadata reserve64KiB/atomic temps.
  Conservative managed-high-water + toàn scratch ceiling dưới696MiB. Scratch
  actual peak chưa đo; không gọi ceiling là peak thực. Đây không phải quota C/
  VHDX/pagefile hoặc cam kết mọi biến động ổ đĩa do task.
- C trống **78,02693 → 78,02849 GiB** (+1,60MiB toàn ổ); host available RAM
  **2380,68 → 1209,10 MiB**, commit headroom **13,39970 → 13,57109 GiB**.
  Đây là snapshots toàn máy, không phải peak task; không áp ngưỡng host2GiB.

## 5. Bảo toàn và phạm vi files

19 protected hashes và **58 files/evidence squared V2 trước đó** giữ nguyên.
PostgreSQL/MLflow/dashboard/inference giữ nguyên ID/StartedAt, tất cả healthy.
Containers mới `--rm` và scratch private dọn finally; không còn job thử.
Không build/pull/install/download, gọi Mapbox/inference/DB, sửa .env/PBIX/v1/
PACKAGE P3/Gold/model gốc, đổi services/volumes. Chưa stage/commit/push.

Files V2.1 (ngoài các thay đổi P3/PBIX có trước):

- `src/nyc_taxi/ml/pretrip_training.py` — loss option, pre-fit lock, comparison/reload.
- `src/nyc_taxi/ml/pretrip_training_model.py` — loss-only factory, default squared.
- `src/nyc_taxi/ml/pretrip_training_reference.py` — pinned reference/drift guards mới.
- `tests/test_pretrip_v2_training.py`, `tests/test_pretrip_v2_training_model.py`,
  `tests/test_pretrip_v2_training_spark.py`, `tests/test_pretrip_v21_reference.py`.
- `scripts/ci_python.py`, `tests/test_ci_python.py` — inventory.
- README hunk V2.1 và report này; report V2 cũ không sửa.

Lệnh execute thực tế trong worker dùng options:
`--execute --loss-profile absolute --reference-artifacts .sandbox/pretrip-v2/training-20261027-pilot-01/artifacts`
`--output-dir .sandbox/pretrip-v2/training-20261027-absolute-01/artifacts --prior-task-bytes 192255514`
`--code-commit 63359e381a9e015a535dcfae7cf2675e7e168dde --runtime-image-id <ID trên>`.
Fresh reload dùng `--reload-artifacts <absolute artifacts> --loss-profile absolute`
`--output-dir <attempt>/fresh-reload --prior-task-bytes 192703763`, cùng runtime.
Đường dẫn đã tồn tại không được chạy đè; PLAN mặc định không thực thi.

**Kết luận: Gate FAIL. August vẫn đóng. V2.1 chưa được phục vụ trên ứng dụng.**
