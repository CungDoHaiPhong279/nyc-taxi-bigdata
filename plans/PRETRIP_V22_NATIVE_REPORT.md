# PRETRIP V2.2 — native categorical experiment

Ngày 27/10/2026, base HEAD `05e5fac29dc59d5214e3333eacfef74b8bb21259`.
**Đúng một real fit, gate validation PASS.** Đây là thí nghiệm **encoding kèm
maxBins**, không phải chỉ đổi một tham số. July đã dùng phát triển/chọn model;
không phải test độc lập. August chưa mở/mount/predict/evaluate. Chưa promote,
deploy, nối app hoặc chốt chất lượng cuối; không tự thêm thí nghiệm.

## 1. Profile và cấu hình đã chốt

CLI cũ thêm `--encoding-profile native` (V2.2), vẫn default `one_hot`/squared và
PLAN. Native chỉ cho squared, maxBins cố định256; native+absolute bị chặn trước
I/O/Spark. Execute native yêu cầu explicit squared `--reference-artifacts` và
absolute `--comparison-artifacts`, cả hai read-only. PLAN không mở samples/
reference, Spark hoặc ghi output. Saved config cũ thiếu encoding vẫn suy ra OHE;
reload không đổi profile của artifact và chấp nhận hai legacy models.

Actual GBT parameters kiểm sau fit và saved paramMap: **lossType squared,
maxBins256, seed42, maxIter20/maxDepth4/stepSize0.1**, subsampling1,
minInstancesPerNode1/maxMemoryInMB128; featuresCol=features, labelCol=fare_amount,
predictionCol=prediction. Đủ **20 trees**. So với V2 squared, chỉ encoding +
maxBins32→256 đổi; mọi default khác theo đúng cùng image/runtime. Không grid
search, tăng depth/iterations hoặc thử absolute/native.

Pipeline bốn stages: hai StringIndexer alphabetAsc/handleInvalid=keep fit train
→ VectorAssembler sáu numeric + pickup_zone_index/dropoff_zone_index → GBT.
Không OHE trong native pipeline, không VectorIndexer, không coi raw ID/index
là đại lượng địa lý liên tục.

| Vector index | Attribute/type | Train dictionary | Bao gồm unknown | maxBins |
|---|---|---:|---:|---:|
| 0–5 | pickup_hour_sin/cos, pickup_dow_sin/cos, is_weekend, is_holiday — numeric | — | — | 256 |
| 6 | pickup_zone_index — nominal | 117 | **118**, unknown index117 | 256 |
| 7 | dropoff_zone_index — nominal | 224 | **225**, unknown index224 | 256 |

Guard kiểm exact8D, allowlist, sáu numeric/hai nominal, đầy đủ unknown bucket và
categories≤256 **trước GBT fit** (sau train-only preprocessing). Vượt cap thì
dừng, không nâng bins/drop categories. Metadata còn nguyên sau fresh reload.

`training_config.json` lưu trước fit, không overwrite sau metrics; hash trước/
sau fit giữ nguyên:
`b8ed00ab0a6e19d92fd986c86b14f97f1651cbe2a87ceabaf02bee3499e63e90`.
Config ghi hai thay đổi explicit, parameters, code/runtime identities, reference
audits, sample lineage và gate đã chốt. Kết quả ở report/metrics riêng.

## 2. Dữ liệu, references và metrics

Cùng June **26.573 train**, July **23.526 validation**, sample identity
`7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86`.
Từng file hash/schema/Arrow alignment/contract/calendar/TZif/key ordering được
kiểm như V2. Chỉ bốn sample Parquets June/July + hai preparation JSON được mount;
không raw/August. Không acquisition/preparation lại. Retained-sample overlap0;
không gọi đây là dedup/overlap toàn raw.

Squared/absolute references kiểm full manifest/file/model hashes, cùng full
sample lineage/identity/counts, train dictionaries, runtime và baselines. Đọc
indexer dictionaries từ đúng Parquet đã pin, không load/predict/refit old real
models. Baseline artifact và mọi baseline metric train/validation khớp exact.
Policy giữ median global/train-only PU/DO×6h tối thiểu5, fallback PU/DO đủ5 rồi
global. Raw features vẫn đúng8, không target/outcomes/journey key/lineage/distance
thực tế vào vector. Không clip target/prediction hoặc loại rows khó.

Metrics lỗi USD; P50/P90/P95 exact linear interpolation `(N-1)*p`. Các cột OHE
lấy từ kết quả đã lưu, không đánh giá lại hai model cũ.

| Split | Model | Rows | MAE USD | RMSE USD | R² | P50 AE USD | P90 AE USD | P95 AE USD |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| train | Global median | 26573 | 7,955430 | 13,040237 | -0,105085 | 4,900000 | 19,600000 | 30,100000 |
| train | PU/DO × 6h median | 26573 | 4,685806 | 9,341241 | 0,432934 | 2,100000 | 11,200000 | 19,600000 |
| train | Squared OHE V2 | 26573 | 6,386564 | 8,786150 | 0,498325 | 5,249837 | 12,250163 | 17,850163 |
| train | Absolute OHE V2.1 | 26573 | 6,574013 | 9,320652 | 0,435431 | 5,014254 | 13,185746 | 19,039446 |
| train | Native squared V2.2 | 26573 | 4,211582 | 5,828678 | 0,779217 | 3,311798 | 8,506857 | 11,283892 |
| validation | Global median | 23526 | 7,738003 | 12,606378 | -0,098534 | 4,900000 | 18,900000 | 28,700000 |
| validation | PU/DO × 6h median | 23526 | 5,131202 | 9,606163 | 0,362129 | 2,800000 | 11,900000 | 20,300000 |
| validation | Squared OHE V2 | 23526 | 6,321704 | 8,717798 | 0,474653 | 5,249837 | 11,881325 | 17,237995 |
| validation | Absolute OHE V2.1 | 23526 | 6,468402 | 9,138135 | 0,422771 | 5,014254 | 12,485746 | 18,785746 |
| validation | Native squared V2.2 | 23526 | 4,491245 | 6,768670 | 0,683306 | 3,411354 | 8,698930 | 11,893107 |

Baseline validation tốt nhất **5.1312024993624075 USD**, threshold cố định
**4.618082249426167 USD**. V2.2 MAE **4.491245204232551 USD**:
cải thiện **12,471878% ≥10% → Gate PASS trên validation**. Không biến July thành
test độc lập hoặc coi đây là cam kết sai số cho dữ liệu/yêu cầu hiện tại.

Non-finite labels/predictions và negative predictions **0** ở mọi model/split.
Unseen train0; validation PU32/DO25/either57 (0,2423%), giữ mọi row như hai
attempt trước. Dictionaries alphabetAsc exact-match SHA256
`aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c`.

## 3. Artifact identity và reload

New attempt `.sandbox/pretrip-v2/training-20261027-native-01/artifacts/`.
Model tree SHA256:
`27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3`.
Reference squared:
`2efee5c2c25769430078f15d9d2ab10db9022cba6fe9ee92dbe7559c8566bd05`;
reference absolute:
`3f9a9ef33c4dda83b07d18efa4bc79b983324cddf287d79a5581151d4785be81`.

Saved stage3 metadata (native has4stages, old OHE has5):
`pipeline/stages/3_GBTRegressor_15019e64b1cb/metadata/part-00000-7b855363-39cf-4758-807d-a55c03cccaf3-c000.txt`.
`paramMap.lossType=squared`, `paramMap.maxBins=256`; library defaults
`lossType=squared`, `maxBins=32`. Metadata SHA256:
`bef445ce21bc2468d9e4530f0afcce42bb77c682e91fadca5049935456a48120`.
Tree identity là hash canonical file manifest, không hash rút gọn.

### Khóa ứng viên và tiêu chí August trước khi mở holdout

Ứng viên duy nhất được khóa là attempt native ở trên, với full model-tree hash
`27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3`.
Các SHA256 sau được lấy từ artifacts đã lưu, không fit/predict lại:

| Artifact | Full SHA256 |
|---|---|
| `artifact_manifest.json` | `8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75` |
| `training_config.json` | `b8ed00ab0a6e19d92fd986c86b14f97f1651cbe2a87ceabaf02bee3499e63e90` |
| `baselines.json` | `bc03cc2e418955b8236322fd6987cf2da39ea1a52c38afe06af386d71d095d8e` |
| `metrics.json` | `61b1bf2b4c8abbf858a7b7c88dce8bc2f74ab45550cb83ed70483f4b45c3f4f1` |
| `sample_lineage.json` | `3ab1c24996401b4333c7b2942769bd6c1a239e2705e3997efc8da892507edccf` |
| `pretrip_v2_contract.json` | `a1ddbe55f0128c20d8189d9f37b3b6172e90f20be71ae7a99300e21fb306add5` |
| `pretrip_v2_calendar.json` | `00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac` |

Giữ nguyên sample identity
`7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86`,
dictionary identity
`aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c`
và TZif America/New_York
`e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95`.
Preprocessing fit trên June, tám raw features, thứ tự vector/nominal metadata,
encoding native, squared loss và toàn bộ parameters trong cấu hình trước fit
ở mục 1 đều được khóa; không fit lại dictionaries hoặc chọn profile bằng August.

Hai baselines đã fit được khóa bằng `baselines.json`: global train median
**12.8 USD** và policy `pretrip-v2-medians-1`, PU/DO × khung **6 giờ**, tối thiểu
**5** mẫu; fallback median PU/DO đủ5 rồi global. Artifact chứa train26573,
1402 pair groups/1601 time groups; không tính lại median từ August.

Theo `plans/ML_PRETRIP_PLAN.md`, khi bước August được phép thực hiện riêng:

- Dùng nguyên ứng viên, fitted preprocessing và hai baselines đã khóa ở đây;
  kiểm identities/lineage trước khi đánh giá, không retrain/tuning.
- Giữ đủ mọi rows của cohort/sample holdout đã chốt; prediction phải hữu hạn
  **100%**, không clip target/prediction hoặc drop rows để đạt gate.
- **MAE ứng viên trên August phải nhỏ hơn MAE baseline tốt nhất trên chính
  cùng August rows**. Ngưỡng July **4.618082249426167 USD** không phải ngưỡng
  số áp cho August; không sửa gate sau khi xem holdout.
- Báo MAE/RMSE/R²/P50/P90/P95 absolute error, negative/non-finite, coverage và
  các slices đã định trong kế hoạch; nhóm ít mẫu phải ghi giới hạn.
- Nếu holdout không đạt, báo FAIL; không tune bằng August rồi gọi đó là test
  độc lập. July chỉ là validation phát triển model. August vẫn chưa mở trong
  checkpoint này; chưa promotion/deployment.

New manifest giữ fitted pipeline/CRC, baselines, config/code hashes, metrics,
sample/contract/calendar/TZif lineage, references của **chính V2.2**. Không sửa
artifact/report cũ. Fresh worker cùng image, chỉ artifacts mới RO, không samples/
old refs, không fit: manifest/hash/saved params PASS; vector8D/hai nominal counts
118/225 còn nguyên; **16 inputs**, max prediction difference **0 ≤1e-9**.
Baseline reload exact. Evidence `fresh-reload/training_report.json` status
`FRESH_RELOAD_VERIFIED`, `fit_calls=0`; không yêu cầu giống prediction OHE.

## 4. Tests, tài nguyên và cleanup

**100 targeted tests PASS**, 35,85 giây; không chạy lại full149/76 suites/CI cũ.
Bốn file tests V2.2 mới + config-before-fit/inventory cases bị ảnh hưởng:
nominal8D/allowlist, unknown/categories cap, train-only dictionaries, profile/
effective parameters, metadata, serialization, reference drift và hai legacy
OHE reload. Tiny synthetic fixtures không phải fit lại hai real models cũ.
CI inventory bổ sung ba pure files, kê Spark file ngoài lightweight CI; không
thêm job/workflow hoặc chạy remote CI trong lượt chưa commit này.

Image sẵn có `nyc-taxi-worker:ml-p3-20261023`, ID
`sha256:d0b29b688b4255ae5d26aa18e23d25e917d24b029980f2e7e342267531cad55c`.
Python3.11.17/PySpark4.2.0/Java17.0.20.1/PyArrow21.0.0. Source/config RO,
không bake/build/pull/install. Worker giữ 2CPU/3GiB memory+swap(total)/swap0,
heap1536MiB, PID512/Java ActiveProcessorCount2/BLAS+OMP+MKL threads1, tmpfs
`/tmp:rw,exec,nosuid,nodev,size=512m`, network none/rootRO, shuffle/default2,
UI/eventlog off. Runtime identity exact-match reference squared trước fit.

| Phase | Seconds |
|---|---:|
| Verify Arrow samples | 1,106 |
| Train-only baselines | 0,192 |
| Squared reference | 1,362 |
| Absolute comparison reference | 1,322 |
| Spark + frames/counts | 10,300 |
| Freeze config | 0,133 |
| Native fit once | 13,227 |
| Train/validation metrics | 3,822 |
| Save | 14,776 |
| Train total gồm stop | **46,963** |
| Fresh reload total gồm stop | **16,393** |

Peak cgroup memory train **1.132.277.760 bytes =1079,82 MiB**; reload
**791.752.704 bytes =755,07 MiB**. Python train peak RSS231,01MiB.
memory.events max/oom/oom_kill/oom_group_kill đều0. Có warning hostname/BLAS
fallback như trước nhưng fit/save/reload thành công; không sửa limits/deps để
che lỗi. Các thời gian là một lượt quan sát, không benchmark lặp/chứng minh
tốc độ ở mọi điều kiện.

Envelope V2 vẫn **2048 MiB** gồm sources/samples/artifacts/runtime cũ:

- Trước **192.709.811 bytes =183,782 MiB**; sau **193.200.113 bytes =184,250 MiB**.
- Mới **490.302 bytes =0,468 MiB**; còn **1863,750 MiB**, không cấp lại budget.
- Reference bundles đã tính trong prior bytes một lần. Artifact guarded quota
  reserve toàn bộ tmpfs ceiling512MiB, giữ64KiB metadata/atomic temps; conservative
  managed-high-water + toàn ceiling dưới697MiB. Chưa đo peak scratch thực,
  không gọi ceiling là số đo; không cam kết giới hạn toàn C/VHDX/pagefile.
- C trống **76,08718 →76,08881 GiB** (+1,66MiB toàn ổ); host available RAM
  **2193,15 →2329,58 MiB**, commit headroom **17,06011 →17,30835 GiB**.
  Không quy biến động toàn máy cho file model; không áp ngưỡng host2GiB.

Fingerprints hai artifact trees cũ **116 files**, 21 protected file hashes giữ
nguyên (gồm .env/PBIX/P3/v1/calendar/feature builder/loader/reports cũ). Bốn
services production giữ full ID/StartedAt/healthy, không restart/recreate.
Temporary containers `--rm`, private scratch finally cleanup; không còn job mới.
Giữ artifacts mới; không prune/delete volumes hoặc outputs cũ.

## 5. Files thay đổi và lệnh

- `src/nyc_taxi/ml/pretrip_training_model.py`: native4stages/nominal/guard/compat.
- `src/nyc_taxi/ml/pretrip_training.py`: encoding option/pre-fit freeze/metadata/reload.
- `src/nyc_taxi/ml/pretrip_training_reference.py`: pinned absolute comparison helper.
- `tests/test_pretrip_v22_model.py`, `tests/test_pretrip_v22_reference.py`,
  `tests/test_pretrip_v22_training.py`, `tests/test_pretrip_v22_spark.py`.
- `scripts/ci_python.py`, `tests/test_ci_python.py`: inventory.
- Chỉ hunk V2.2 README và report này; giữ nguyên hunk P3/PBIX/changes có trước.

Worker execute options thực tế:
`--execute --encoding-profile native --loss-profile squared`
`--reference-artifacts .sandbox/pretrip-v2/training-20261027-pilot-01/artifacts`
`--comparison-artifacts .sandbox/pretrip-v2/training-20261027-absolute-01/artifacts`
`--output-dir .sandbox/pretrip-v2/training-20261027-native-01/artifacts --prior-task-bytes 192709811`
`--code-commit 05e5fac29dc59d5214e3333eacfef74b8bb21259 --runtime-image-id <ID trên>`.
Fresh worker dùng `--reload-artifacts <native artifacts> --encoding-profile native`
`--output-dir <attempt>/fresh-reload --prior-task-bytes 193192965`, cùng limits.
PLAN mặc định; không chạy đè attempt tồn tại.

**Validation gate PASS không phải final holdout PASS:
August vẫn đóng, chưa promotion/deployment hoặc phục vụ V2.2 trên app.**
