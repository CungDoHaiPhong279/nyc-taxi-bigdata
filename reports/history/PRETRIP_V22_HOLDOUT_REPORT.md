# PRETRIP V2.2 — August holdout evaluation

Ngày 27/10/2026; base HEAD `6d3d16182a337f0fa3f34ead403e471c215fa942`.
**Holdout gate PASS, EVALUATED_NOT_DEPLOYED.** Đúng một lần thu predictions trên
sample August; không fit/refit/tuning, không so model OHE trên holdout. Đây là
zone/time-only trên sample lịch sử August2026, không chứng nhận accuracy của
yêu cầu hiện tại hoặc luồng địa chỉ/routing → giá. Chưa promote/deploy/nối app.

## 1. Ứng viên, cấu hình và dữ liệu đã khóa

Giữ nguyên candidate `.sandbox/pretrip-v2/training-20261027-native-01/artifacts/`,
toàn bộ fitted preprocessing/dictionaries/baselines và parameters từ
[candidate report](PRETRIP_V22_NATIVE_REPORT.md); report đó không sửa sau holdout.

| Identity | Full SHA256 |
|---|---|
| Model tree | `27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3` |
| Artifact manifest | `8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75` |
| Fitted baselines | `bc03cc2e418955b8236322fd6987cf2da39ea1a52c38afe06af386d71d095d8e` |
| Fitted dictionaries | `aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c` |
| Training config | `b8ed00ab0a6e19d92fd986c86b14f97f1651cbe2a87ceabaf02bee3499e63e90` |
| Training lineage file | `3ab1c24996401b4333c7b2942769bd6c1a239e2705e3997efc8da892507edccf` |
| Training/validation sample identity | `7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86` |
| Contract | `a1ddbe55f0128c20d8189d9f37b3b6172e90f20be71ae7a99300e21fb306add5` |
| Calendar | `00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac` |
| America/New_York TZif | `e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95` |

Audit độc lập pin manifest và từng member/size/hash, canonical model-tree,
stage metadata, dictionaries, baseline policy và mọi identities trong report
trước `PipelineModel.load`; chặn path traversal/symlink/extra pipeline files.
Vector vẫn **8 chiều**, sáu time numeric + PU/DO nominal, **118/225** categories
gồm unknown. Native squared, maxBins256, seed42/maxIter20/maxDepth4/stepSize0.1,
subsampling1/minInstancesPerNode1/maxMemory128; không đổi encoding/params.
Hai baseline train-only: median global12.8, PU/DO×6h tối thiểu5, fallback PU/DO
đủ5 rồi global; không tính lại median hoặc dictionaries trên August.

Evaluator riêng `pretrip_holdout*.py`, không mở rộng training loader. PLAN
không mở artifact/metadata/holdout, tạo Spark hoặc ghi output. Execute khóa
`evaluation_config.json` **trước lần đọc Parquet August đầu tiên**, gồm full
candidate identities, metadata/output hashes, expected21833, code/runtime
hashes, gate/slices và fit/prediction budgets. Config SHA256:
`3e8ea38b7d090847b8cec011d94c6f034e3266490e489a0e6d96ad71d13b871b`.

| August/preparation identity | Full SHA256 |
|---|---|
| Preparation report | `153f57c606f742b82668d3081133cdead90be2964a520576133870737771600e` |
| Preparation checkpoint | `2ea29a48e22040679f9135a894aae1e222e5a8005bea0d4a75b2550d0508ccc3` |
| `test_features.parquet` | `b3052a59d482aaca9b05102ee71f7d1a86f232433af0aaa3a6d2ff783b9360be` |
| `test_labels.parquet` | `fbf909fe1c4465ad67202ba05fc706b630cb0ce4ffb002a7d957f576adde5a7c` |
| August journey set | `baf3ba6d2b7c86119dc03c91ff7d629072ad53f615ba36f1db6d3bb482aca5bb` |
| **August sample identity** | `88a9c7911fa6bb9b04964e2a2bfffdff19b6a45c417431691dd6e1109d92b924` |

Preparation JSON bytes khớp pins trong candidate lineage rồi mới tin output
hashes August; không dùng aggregate June/July làm identity của August. Kiểm
schema/footer **21.833/21.833**, Arrow ordered zip trước Spark, journey keys
ascending unique, seed42/hash1%, pickup NYC local naive nằm trong
`2026-08-01 <= pickup < 2026-09-01`; độc lập rederive time/calendar features.
Giữ toàn bộ rows và unknown; không clip/drop hoặc sửa cohort.

Overlap train/test0, validation/test0, train/validation0 là **bằng chứng retained
sample từ preparation đã pin**, không chạy lại giao tập/raw. Không suy ra giao
tập từ hashes đơn lẻ; không tuyên bố kiểm toàn eligible cohort/raw. Worker chỉ
mount RO hai August Parquets, hai preparation JSON, candidate artifacts và
code/config; không raw, June/July samples hoặc hai OHE models.

## 2. Kết quả và gate không đổi

**fit_calls=0, blocked_fit_calls=0, prediction_calls=1**. Guard chặn Spark
Estimator/Pipeline fit/_fit, JavaEstimator._fit và helpers fit/fit_baselines;
không có estimator mới được fit. Frame dự đoán không chứa label; assembler chỉ
nhận tám allowlisted features, journey key chỉ để kiểm output alignment.

| Model | Rows | MAE USD | RMSE USD | R² | P50 AE USD | P90 AE USD | P95 AE USD |
|---|---:|---:|---:|---:|---:|---:|---:|
| Global median | 21833 | 7,879092 | 13,096744 | -0,095566 | 4,900000 | 19,600000 | 29,400000 |
| PU/DO × 6h median | 21833 | 5,283902 | 10,120840 | 0,345748 | 2,800000 | 12,600000 | 21,000000 |
| Native GBT | 21833 | **4,519596** | **6,789118** | **0,705599** | **3,428646** | **8,741524** | **12,047642** |

Cả ba có negative predictions **0**, non-finite predictions/labels **0**;
finite100%, đủ21833 rows, clipped0/dropped0. Unseen chung trên cùng inputs:
**PU22 / DO43 / either64**, seen-both21769; either **0,2931%**.

Native MAE full precision **4.519595538780006 USD** nhỏ hơn baseline tốt nhất
**5.283902349654193 USD**, cải thiện **14.464817104809049% → holdout gate PASS**.
Chỉ so trên cùng August rows/fitted baselines; **không áp ngưỡng July
4.618082249426167 USD sang August**. July là development validation, không
đổi gate/chọn lại model từ kết quả holdout này.

## 3. Slices đã định trước

MAE USD; tất cả slices của mỗi family cộng đúng21833. Runtime `slices.json`
giữ đủ MAE/RMSE/R²/P50/P90/P95/counts/negative/non-finite cho cả ba models.

| Slice | Rows | Global MAE | Group median MAE | Native MAE |
|---|---:|---:|---:|---:|
| 00:00–06:00 | 1575 | 8,98477 | 6,72900 | 4,92909 |
| 06:00–12:00 | 4695 | 7,56736 | 4,88475 | 4,30713 |
| 12:00–18:00 | 8330 | 7,83565 | 5,13611 | 4,68509 |
| 18:00–24:00 | 7233 | 7,89071 | 5,39853 | 4,37775 |
| Seen cả PU/DO | 21769 | 7,80341 | 5,20059 | 4,40768 |
| Unseen PU hoặc DO | 64 | 33,62219 | 33,62219 | **42,58689** |
| Fare (0–10] | 7673 | 5,18543 | **2,29274** | 4,55547 |
| Fare (10–25] | 10643 | 3,54736 | 3,41507 | 3,00451 |
| Fare (25–50] | 2954 | 21,81186 | 14,48840 | 8,33203 |
| Fare (50–100] | 540 | 51,52907 | 31,10463 | 12,05537 |
| Fare >100 | **23** | 96,69130 | 79,54130 | 27,06299 |

Gate tổng PASS không che hạn chế: native kém baseline ở fare≤10 và ở unseen;
nhóm unseen chỉ64 rows nên chưa đại diện tốt cho zones ngoài train. Nhóm fare
>100 chỉ23 rows, **low sample (<30)**; không khái quát khả năng xử lý cước cao.
Fare band chỉ dùng phân tích lỗi, không vào predictor. Không sửa/tune theo slices.

## 4. Predictions, kiểm offline và lỗi trước holdout

Giữ toàn bộ outputs ignored tại
`.sandbox/pretrip-v2/holdout-20261027-native-02/results/`:

- `native_predictions.parquet`: keyed raw model outputs, SHA256
  `2ca55963069204a302fc8f2d34df4017c8faf4183507e41ffd2fa09e2630784f`.
- `evaluation_rows.parquet`: aligned features/labels và ba prediction columns,
  SHA256 `2153637b0e00ce0aab66db28e78bc26328e6eeff4b90ca0b6fddf5fb0ed2ceda`.
- `evaluation_report.json`: SHA256
  `58af506f95ff50ab9bb6d194138e7de78b1b37ce56ecaaa8a825639cd378572f`.
- `slices.json`: SHA256
  `79ed195c6ef754f42ef1eb6ab43424c44c08028248fd6208298daaafe93f74af`.
- Config, holdout lineage, checkpoint và `offline_verification.json`.

Spark chỉ một transform/collect; raw capture được ghi **trước** metrics và
kiểm keyed alignment rồi mới publish ledger đầy đủ. Independent offline worker
chỉ đọc ledger/capture/report/slices: checksum/key/count PASS; tính lại metrics
bằng math thuần (không Spark/model/sample), sai lệch tối đa
**4.440892098500626e-14**, tolerance abs/rel1e-12. Không predict lại.

Có một startup FAILED **trước holdout** ở
`holdout-20261027-native-01/results/evaluation_checkpoint.json`: guard gọi
`regressor.numTrees`, trong PySpark4.2 API đúng là property `getNumTrees`.
Checkpoint chứng minh holdout_data_accessed=false/prediction_calls0/fit_calls0,
chưa tạo evaluation config hoặc mở August. Sửa tối thiểu evaluator guard và thêm
hai regression cases; giữ checkpoint872bytes, không overwrite. Attempt02 mới
là lượt đọc/dự đoán August duy nhất. Không liên quan inference production.

## 5. Kiểm thử, tài nguyên và bảo toàn

- **122 targeted tests PASS, 52,30 giây**, sau đó **2 regression cases PASS,
  0,79 giây** cho guard vừa sửa. Chỉ evaluator data/model/CLI/Spark synthetic
  và hai CI-inventory nodes; không full suites/CI/preparation/training.
- Synthetic Spark chỉ supplied StringIndexerModel.from_labels/assembler,
  preprocessing serialization/unknown/no-fit; không fit GBT. PLAN thực tế
  `PLAN_NOT_EXECUTED`, không mount/open samples/model và không ghi runtime.
- Worker image cũ `nyc-taxi-worker:ml-p3-20261023`, full ID
  `sha256:d0b29b688b4255ae5d26aa18e23d25e917d24b029980f2e7e342267531cad55c`;
  source/config mới mount RO, không build/pull/install. Python3.11.17,
  Spark4.2.0/Java17.0.20.1; 2CPU/3GiB memory+swap total, swap0, heap1536MiB,
  PID512/Java activeprocessors2/BLAS+OMP+MKL+NUMEXPR1, private tmpfs exec512MiB,
  rootRO/network none. Không áp host RAM2GiB.

| Phase | Seconds |
|---|---:|
| Audit candidate + preparation metadata | 1,707 |
| Load frozen pipeline/start Spark | 23,757 |
| Freeze evaluation config | 0,072 |
| Verify August Arrow rows/lineage | 1,077 |
| Single prediction + persist capture/ledger | 8,227 |
| Offline metrics/slices | 0,524 |
| Evaluation total gồm stop | **37,503** |

Startup thất bại trước holdout riêng27,254giây. Peak cgroup RAM evaluation
**975.839.232 bytes =930,63 MiB**; Python peak RSS197,68MiB. memory.events
max/oom/oom_kill/oom_group_kill đều0; hostname/network-none và BLAS fallback
warnings không cản đánh giá, không sửa limits/dependencies để che lỗi.

Envelope V2 vẫn **2048 MiB**: trước **193.200.113 bytes =184,250 MiB**; sau gồm
checkpoint lỗi, predictions, reports và offline evidence **196.658.168 bytes
=187,548 MiB**. Tăng **3,298 MiB**, còn **1860,452 MiB**; không cấp lại ngân sách.
Private scratch512MiB được reserve toàn ceiling trước ghi, chưa đo peak scratch
thực tế; không gọi ceiling là số đo. Đây là logical task-files quota, không
cam kết giới hạn biến động toàn C/VHDX/pagefile. Snapshot host trước/sau evaluation
(trước offline verifier): C trống78,92749→78,92180GiB, available RAM
1096,53→1011,20MiB, commit headroom13,02933→13,41191GiB; không quy biến động toàn
máy cho tác vụ và không coi snapshot là peak job.

Snapshot cuối sau offline verification và kiểm preservation: C trống
**78,90796 GiB** (toàn ổ giảm20MiB so baseline), available RAM**801,69MiB**,
commit headroom**13,10507GiB**. Số files tác vụ cuối188/196.658.168bytes không
đổi; không coi toàn biến động C/RAM là tác động của job.

**26 protected files/hashes**, **45 candidate manifest members** và bốn services
được đối chiếu sau: giữ ID/StartedAt,
running/healthy. Không sửa v1/training helpers/candidate artifact/.env/PBIX/P3,
dữ liệu gốc, services hoặc volumes. Spark luôn stop, container `--rm` và own
scratch cleanup; chỉ giữ outputs/checkpoint mới.

## 6. Files và cách dùng an toàn

- `src/nyc_taxi/ml/pretrip_holdout_data.py`: metadata pin + August-only loader.
- `src/nyc_taxi/ml/pretrip_holdout_model.py`: artifact audit/no-fit/metrics/slices.
- `src/nyc_taxi/ml/pretrip_holdout.py`, `scripts/evaluate_pretrip_holdout.py`:
  PLAN-first freeze/one-prediction/persist/checkpoint/quota/cleanup.
- Bốn `tests/test_pretrip_holdout*.py`; CI inventory và test inventory.
- Chỉ phần README holdout và report mới này; P3/PBIX giữ riêng.

PLAN: `python scripts/evaluate_pretrip_holdout.py`. Execute thực tế dùng cùng
worker limits và mounts tách biệt đã nêu, với options:

```text
--execute
--output-dir .sandbox/pretrip-v2/holdout-20261027-native-02/results
--scratch-dir /tmp/pretrip-v22-august-eval-20261027
--prior-task-bytes 193200985
--runtime-image-id sha256:d0b29b688b4255ae5d26aa18e23d25e917d24b029980f2e7e342267531cad55c
--code-commit 6d3d16182a337f0fa3f34ead403e471c215fa942
```

Đây là bản ghi **lệnh đã chạy**, không phải hướng dẫn tự chạy lại holdout.
Attempt tồn tại bị chặn overwrite; không đổi output để đánh giá lại/chọn kết quả.
Muốn sửa report/kiểm metrics chỉ đọc predictions đã giữ, không gọi model nữa.
Holdout đã được mở một lần; không còn là tập kín cho thử nghiệm/tuning tiếp.
PASS chỉ cho frozen candidate trên sample này; chưa promote/deploy/serving parity.
