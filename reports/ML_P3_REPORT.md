# ML P3 REPORT — frozen June holdout

2026-10-23. **Evaluation PASS; không retrain/tuning/promote.**

## 1. Model identity và sample

- Training run **`d2772f1904d7472490a73b0ac1c0c5d4`**, FINISHED; model/preprocessing/baseline tải từ đúng run, so byte SHA với outputs P2.1 read-only. Tree SHA256 (40 files, 55.660 bytes): **`759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`**. Baseline cố định **12,1 USD**.
- Giữ 9 features, train-only categorical indexers/unknown=keep, GBT absolute loss/20 trees/depth4/seed42 và toàn bộ params P2.1. Hash 8 inputs code/dependencies, gồm cohort/key/feature và holiday code, khớp training report. Chi tiết [model card](ML_P3_MODEL_CARD.md).
- Cohort không đổi: fare finite>0, ratecode=1, PU/DO1–263, distance finite [0,200] mile; giữ zero-distance/uncapped fare. Actual pickup NTZ NYC-assumed, không chuyển UTC; train Jan–Apr, validation May, holdout June.
- Gold 23.886.265 rows/24 files/6 source months: manifest SHA `bb755aa6…b57ff`, toàn bộ size/mtime/footer row counts và P1 profile SHA khớp training lineage. Đây không phải checksum toàn bộ bytes Gold.
- P2.1 không lưu sample/fingerprint. Tái tạo đúng group/business canonical v1 và hash 1% seed42: trước/sau dedup **110.356 / 29.979 / 27.310**, loại trùng **0**; cả 10 overlap/invalid counters **0 trên sample ba split**, không tuyên bố full-cohort overlap.
- `candidate_lock.json` trước đọc Gold; `sample_lock.json` trước predict. June fingerprint mới **`750f197818023bb946081bd922ea7afd0fbb07b94afcff237e9ca8a16dacb4c4`**. Không có fingerprint cũ để đối chiếu byte-wise; bằng chứng tái tạo là frozen code/lineage/counts/guards, không chọn lại seed.

## 2. Metrics cùng rows, không clip/drop

| Split (n) | Model | MAE USD | RMSE USD | R² |
|---|---|---:|---:|---:|
| May (29.979) | Median baseline | 7,88742 | 13,22285 | -0,14274 |
| May (29.979) | GBT | 2,41535 | 3,80180 | 0,90553 |
| June (27.310) | Median baseline | 7,90497 | 13,30918 | -0,13432 |
| June (27.310) | GBT | **2,25014** | **3,52047** | **0,92063** |

June giảm MAE **5,65483 USD / 71,5351%** so baseline. GBT thấp hơn May **0,16520 USD / 6,84%**; không dùng kết quả để đổi ứng viên.

| June absolute error (USD) | P50 | P90 | P95 |
|---|---:|---:|---:|
| Baseline | 4,200 | 20,300 | 30,800 |
| GBT | 1,444 | 5,085 | 7,168 |

Quantiles empirical `percentile_approx`, accuracy100.000, rank error1e-5, không phải median nội suy/sai số USD. Prediction âm **0**, zero **0**, không hữu hạn **0** cho cả baseline/GBT; target/numeric inputs/arithmetic invalid **0**. Nonfinite/overflow khiến toàn evaluation fail, không tính finite-only metrics.

## 3. Error analysis và giới hạn

| Nhóm distance (mile) | n | GBT MAE USD |
|---|---:|---:|
| 0 | 153 | 3,983 |
| (0,1] | 6.979 | 1,442 |
| (1,3] | 13.690 | 2,045 |
| (3,5] | 2.982 | 2,972 |
| (5,10] | 2.353 | 3,496 |
| (10,20] | 1.065 | 4,714 |
| (20,50] | 88 | 7,761 — ít dữ liệu |
| (50,200] | 0 | chưa có bằng chứng |

Fare bands USD **(0,10] / (10,20] / (20,50] / (50,100] / (100,1000] / >1000**:
n **9.600 / 11.229 / 5.696 / 770 / 15 / 0**; GBT MAE
**1,140 / 1,986 / 3,986 / 6,765 / 19,369 / N/A**.
Nhóm fare>100 chỉ 15 rows, R² **−2,811**, P95 error **42,379 USD**: không che ca khó, không kết luận tail performance tốt.

Pickup borough n / GBT MAE USD: **Manhattan 25.231 / 2,106; Queens 1.947 / 4,081; Brooklyn 120 / 2,880; Bronx 12 / 2,425**. EWR/Staten Island n0. Nhóm n<100 gắn `limited=true`, không suy rộng geographic fairness/coverage. Lookup CSV tĩnh unique265 IDs, join trái không fanout/missing; không thêm borough vào features.

Unseen categories so train: **PU6 (0,0220%), DO7 (0,0256%)**, giữ unknown bucket, không drop/refit. Báo cáo MLflow lưu đủ MAE/RMSE/R²/P50/P90/P95 và diagnostics của từng slice; không có dữ liệu từng chuyến.

## 4. MLflow, files và kiểm chứng

Evaluation run **`58d5008f0ed048f58599a859877ae336` FINISHED**, experiment1; [mở MLflow local](http://127.0.0.1:5000/#/experiments/1/runs/58d5008f0ed048f58599a859877ae336). Tags liên kết training run/model SHA; **10 parameters, 17 metrics**. Ba artifacts `candidate_lock.json`, `sample_lock.json`, `evaluation_report.json` lưu persistent subpath volume cũ, server SHA khớp local. Metadata/metrics/params/tags training run cũ không đổi.

Files P3: `src/nyc_taxi/ml/holdout.py`, `holdout_metrics.py`; `scripts/evaluate_holdout.py`, `evaluate_holdout.ps1`; `docker-compose.ml-holdout.yml`; hai `test_ml_holdout_*.py`; Dockerfile.ml COPY override; CI Python/Compose helpers và inventory tests; `.gitignore` thêm đúng hai tài liệu; README và model card/report này. Không sửa bốn training core modules. Runtime JSON/logs/model downloads nằm riêng `models/ml-p3/20261023-june01/`, Git ignored.

Lệnh đã chạy:

```powershell
python -B scripts/ci_validate_compose.py
.\.venv\Scripts\python.exe -B -m pytest -q -p no:cacheprovider tests/test_ml_holdout_contract.py tests/test_ci_python.py tests/test_ci_compose.py
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/evaluate_holdout.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/evaluate_holdout.ps1 -Build
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/evaluate_holdout.ps1 -TestOnly
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/evaluate_holdout.ps1 -Execute
```

Compose **12/12 PASS**, PowerShell parse PASS; host offline **84 PASS /3,28s**. Worker **64 holdout tests PASS /66,73s**, **87 ML regression fixture/mock tests PASS /100,20s**, không ETL suites; một số tests cũ fit fixture tiny, không retrain ứng viên/dữ liệu thật. Python mặc định host thiếu pytest; chuyển venv hiện có và chạy lại thành công, không cài thêm. `pip check`, versions và hash probe **55 files PASS**. CI remote chưa chạy cho thay đổi chưa commit.

Image **`nyc-taxi-worker:ml-p3-20261023`**, ID `sha256:d0b29b688b4255ae5d26aa18e23d25e917d24b029980f2e7e342267531cad55c`; code-refresh từ clean dependencies P6.2, không clean-build lại/nâng dependency. Python3.11.17/Java17.0.20.1/PySpark4.2.0/MLflow3.5.1/NumPy2.4.6.

| Giai đoạn evaluation | Giây |
|---|---:|
| Freeze/download/verify artifacts | 4,548 |
| Spark + fitted model load | 23,905 |
| Gold lineage + hash sample/dedup | 125,333 |
| Sample overlap + fingerprint lock | 10,678 |
| June frozen transform/predict/category counts | 7,653 |
| Cached metrics/slices aggregate | 7,994 |

Report elapsed **180,112s**, Docker wall **189,232s**, exit0. **local[2]/2 CPU/3 GiB**, heap max1.536MiB, peak cgroup **1,684 GiB**, Python peak187,24MiB. Không reset OS cache; không gọi đây là cold-cache benchmark. Đã `spark.stop()` và không còn ML job container nền.

## 5. Demo và bảo toàn

**Phù hợp demo lịch sử 2025 có điều kiện theo distance, không phải serving/production PASS.** MAE tốt hơn baseline nhưng zero-distance/long-distance/high-fare còn khó; sample không đại diện rare tails hoặc borough hiếm. Actual distance/destination là proxy cho user estimates, không định tuyến trước chuyến. Calendar/feature contract chỉ 2025, không chứng minh giá hiện tại; chưa có uncertainty/traffic/route model. June đã được dùng một lần, không dùng tuning rồi gọi lại là independent holdout. Chưa promote hoặc tích hợp dashboard.

Trước/sau P3: PostgreSQL **930e1ad167c9**, MLflow **6db6f54eda60**, dashboard **cd02addf38c8**, đều healthy; IDs/StartedAt/mounts/ports và hai named volumes cũ không đổi. StartedAt thực tế tại đầu P3 là PG `2026-10-23T00:37:36.683897646Z`, MLflow `.660314804Z`, dashboard `.690685179Z`; chúng đã khác checkpoint P2.1 **trước khi P3 bắt đầu**, không thể xác nhận vòng đời không đổi giữa hai bước. Hai containers thử cũng giữ nguyên identity/lifecycle/mounts (health inspect `starting` trước/sau; không sử dụng hoặc khởi động lại chúng).

SHA .env/PBIX/bốn manifests/base Compose/dependencies khớp trước/sau; **157 files output P2.1**, metadata training run và sáu image tags cũ nguyên trạng. Không ETL/loader/SQL/training thật, không sửa Gold/PostgreSQL, không recreate services, xóa volumes, commit/push. Chỉ thêm evaluation run/artifacts MLflow được yêu cầu. Dừng sau ML P3.
