# ML P4 REPORT — Streamlit + frozen Spark inference

2026-10-23 (Asia/Saigon). **Tích hợp và parity PASS; không retrain/tuning/đánh giá lại June.**

## 1. Files và kiến trúc

- Backend mới: `src/nyc_taxi/ml/inference.py`, `inference_server.py`, `Dockerfile.inference`, `docker-compose.inference.yml`.
- Web: giữ analytics trong `dashboard/app.py`, thêm `src/nyc_taxi/dashboard/prediction.py`, `fare_page.py`; test target trong `Dockerfile.dashboard`.
- Kiểm chứng: `config/ml_serving_parity.json`, `scripts/check_fare_inference.py`, `check_fare_ui.py`, `tests/test_ml_inference.py`, `test_dashboard_prediction.py`.
- Docs/CI inventory: README, model card P3, `.gitignore` thêm đúng report này; CI Python/Compose helpers, inventory tests và `.github/Dockerfile.python-tests` COPY fixture công khai mới. Không đổi workflow/quyền GitHub, dependencies hoặc core preprocessing/training.

Dashboard HTTP client → **network nội bộ riêng** → `fare-inference`: một Spark `local[2]`/fitted model mỗi process, serialize requests, 2 CPU/3 GiB, heap1536MiB. Không publish backend port. Dashboard thực tế **không có PySpark/Java**; không mở Spark khi người dùng mở trang. Backend non-root10001, rootFS read-only, drop capabilities/no-new-privileges; chỉ bốn artifact binds RO, không DB credentials/MLflow URI/SQLite volume hoặc network PostgreSQL. `/tmp` tmpfs1GiB riêng cần `exec` cho Snappy, vẫn nosuid/nodev. Đây là stdlib HTTP demo nội bộ, chưa có public API authentication/TLS/rate limiting.

## 2. Identity, artifacts và feature contract

- Training **`d2772f1904d7472490a73b0ac1c0c5d4`**; evaluation **`58d5008f0ed048f58599a859877ae336`**.
- Full model tree SHA256 **`759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`**: toàn bộ40files/55.660bytes, so cả inventory path/bytes/fileSHA và treeSHA, không dùng hash rút gọn.
- Pinned P3 lock SHA256 **`12442b5684943bb81169bb084cffc8c1b54043882d75711e295a0f1a0ff7338d`**; feature contract, zone CSV và tám core code/dependency inputs phải khớp lock. Đã kiểm hash lại trực tiếp trên artifacts volume MLflow bằng thao tác chỉ đọc; model local và server khớp.
- Backend chỉ load artifacts local RO, không gọi MLflow lúc serving; không latest/fallback/fit. `/ready`503 khi chưa load/lỗi; `/health`200 chỉ liveness. Deadline inference30s, client45s; busy503, timeout504, model error503/no price. Timeout/model failure làm unready, không tự đổi model.
- Dùng nguyên `prepare_features(..., include_labels=False)` và saved StringIndexers/VectorAssembler/GBT. Bốn inputs: distance mile, pickup TimestampNTZ local NYC-assumed, PU/DO IDs1–263. Chín vector features/unknown bucket giữ nguyên, không thêm target/charges/actual duration/dropoff time.

## 3. Giao diện và validation

Sidebar **Phân tích lịch sử / Ước tính cước**; analytics là mặc định. Form chọn zone có ID + borough + tên, ánh xạ đúng LocationID; chỉ predict khi submit và xác nhận standard metered. Giá là `fare_amount` USD theo distance cung cấp, **không gồm tip/tolls/taxes/surcharges**, không routing/ETA/báo giá hiện tại. MAE không phải khoảng bảo đảm từng chuyến.

Reject distance NaN/Inf/ngoài[0,200]/bool, zone IDs không nguyên/ngoài1–263, datetime sai/UTC offset/ngoài2025, thiếu/thừa fields (gồm target). Jan–Jun/2025 là thời gian đã đánh giá; Jul–Dec được phép nhưng warning ngoại suy. Zero-distance, >20mile, cước cao và unseen có cảnh báo; không clip giá âm/0, không tạo giá thay thế. Lỗi metadata/model/API hiện lỗi an toàn; submit thất bại xóa quote cũ, kết quả còn lại luôn ghi rõ inputs của lần gửi gần nhất.

## 4. Parity, tests và độ trễ thực tế

Fixture **8 inputs hoàn toàn tổng hợp**, không labels/Gold/June rows; SHA256
**`22292e16ef60b74890bbf834e3c93211f8c506b61bf4438165602b7ef4881d99`**.
Gồm thường, zero-distance/holiday, PU1 unseen, DO5 unseen, cả hai unseen,100mile,200mile/Dec31 và local microseconds. Reference chạy độc lập trong **immutable P3 image** `sha256:d0b29b688b4255ae5d26aa18e23d25e917d24b029980f2e7e342267531cad55c`, load model gốc và preprocessing gốc; không gọi inference engine mới để tạo expected.

- **8/8 API predictions khớp reference, max diff0 USD, tolerance1e-9**; unseen flags đúng. Model load count1 và Spark application `local-1792758826962` giữ nguyên qua mọi requests.
- AppTest **gọi API thật**: 3/3 ca thường/zero/unseen-both khớp raw predictions và formatted USD; inputs sau form bằng đúng fixture, không query PostgreSQL. Lỗi connection-refused thực tế hiển thị error/no price, không dừng backend để thử.
- 5 requests sai thực tế trả400/no-price; thiếu model trong container tạm riêng trả `/ready`503 và `/predict`503, **không tạo Spark**. Unit tests còn kiểm nonfinite/overflow/hash drift/timeout/busy/failure và raw negative/zero predictions.

| Đo thực tế | Giây |
|---|---:|
| Model + Spark load, không warm predict | 27,029 |
| Docker up → healthy | 30,104 |
| Request đầu sau ready: HTTP / server | 5,562 / 5,559 |
| Requests tiếp theo trong fixture: HTTP | 0,989–1,363 |
| Lặp request thường: HTTP | 0,940 |
| UI/AppTest submit thường / zero / unseen | 1,720 / 0,996 / 1,044 |

Không cache prediction, không restart/xóa OS cache để benchmark; không gọi đây là cold-cache benchmark hoặc latency bảo đảm. Peak cgroup quan sát **972.902.400bytes (~0,906GiB)**, limit3GiB. Original-reference Docker wall51,703s (report44,920s), chỉ tám inputs.

**Tests**: backend/CI helper host132PASS; web image **87PASS/1skip** (DB integration opt-in không chạy trong suite); đầy đủ local CI Python image **374PASS/1deselected trong6,40s**, không PySpark/MLflow/Java/network, đúng một SQL integration test được loại tường minh. Compose **14/14 quiet checks PASS**, diff/AST/inventory PASS; source probe production backend39files và web5files khớp workspace. CI remote chưa chạy vì không commit/push.

Analytics thật: một aggregate **REPEATABLE READ, READ ONLY**, SQL **29,184s**,38 rows trả về; Total Trips **23.886.265**, January **3.473.175**, ba chart totals đều khớp. KPI: Average Fare **17,897621 USD**, Average Distance **6,861429 mile**, Total Fare **427.507.325,11 USD**. AppTest render đủ4KPI/3chart datasets từ snapshot thật, không thêm SQL. Không đổi query/ML cohort vào analytics.

Lỗi giữ lại để truy vết: lượt reference đầu fail vì tmpfs noexec/Snappy; sửa riêng scratch exec rồi chạy lại PASS. Một AppTest default3s timeout ở lượt web đầu; chạy nguyên suite lại tuần tự PASS, **không đổi timeout/bỏ test**, chưa định lượng nguyên nhân timing. Harness UI stdin bị encoding PowerShell; chuyển sang file UTF-8 COPY trong test image rồi parity PASS. Một preflight host dùng sai contract-directory thiếu feature file; mounts đúng trong container đã validate toàn bộ hashes PASS. Tất cả logs fail vẫn ở runtime riêng, không báo các lượt fail là PASS.

## 5. Lệnh chạy, URL và giới hạn

Đứng tại project root, kiểm tra PG/MLflow healthy trước; không `up` toàn stack:

```powershell
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml config --quiet
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml build fare-inference dashboard
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml up -d --no-deps fare-inference
# Chờ fare-inference healthy (/ready=true).
docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml up -d --no-deps dashboard
```

URL **http://127.0.0.1:8501 → Ước tính cước**. Backend chỉ `http://fare-inference:8000` trong network nội bộ. Artifacts không bake vào image/Git: clone mới cần chuẩn bị đúng paths và hashes từ run đã chốt (README); không tự fetch latest/bootstrap. `start.ps1` chưa điều phối override inference này.

Images: backend `nyc-taxi-inference:ml-p4`, ID `sha256:ef0b3f0fde22bf1257d5d2ef92be8bb8779ade8a186f5144af64ae4ee496a42b`; web `nyc-taxi-dashboard:ml-p4`, ID `sha256:e8ea9111705fe05ea6fbfc4314cbc733f9bdc8349ee4d68ada9daed3724f1bd8`. Không cài thêm host packages/đổi dependencies; backend tái sử dụng clean runtime P6.2, không tuyên bố clean-build mới.

**Không có browser tool để kiểm render thật**: chỉ HTTP + unit/AppTest + real API/chart datasets, không coi HTTP200 là UI PASS. Serving chỉ demo lịch sử standard-rate theo distance, không kiểm chứng giá hiện tại, accuracy ngoài Jan–Jun2025, fairness/rare tails, public production security hoặc multi-user load. Không promote model registry.

## 6. Bảo toàn

| Service thật | ID trước → sau | StartedAt UTC trước/sau | Kết quả |
|---|---|---|---|
| PostgreSQL | `930e1ad167c9` → cùng ID | `2026-10-23T12:12:08.029689222Z` | healthy; mounts/ports không đổi |
| MLflow | `6db6f54eda60` → cùng ID | `2026-10-23T12:12:08.041211105Z` | healthy; mounts/ports không đổi |
| Dashboard | `cd02addf38c8` → `3cce40a779cf` | `12:12:08.046845255Z` → `12:36:38.186288375Z` | cập nhật có chủ đích, healthy/port8501 giữ nguyên |

Inference mới `a9b1b04d73e6`, healthy. Hai volumes thật `nyc-taxi-bigdata_postgres-data`/`nyc-taxi-bigdata_mlflow-data` vẫn gắn đúng services cũ; không volume mới thay thế/xóa volume. Hai containers thử cũng nguyên identity/lifecycle/mounts (health inspect starting có sẵn, không sử dụng).

SHA `.env`, PBIX, bốn manifests/base Compose/dependencies không đổi. **24 Gold Parquet checksum đầy đủ +11 JSON P3**, **157 files P2.1**, metadata training/evaluation MLflow và7 worker image tags cũ khớp trước/sau. Evidence: `models/ml-p4/20261023-serving01/` (Git ignored), gồm startup/reference/service-parity/real-ui-parity/access/preservation audits và logs.

Không ETL/loader, sửa PostgreSQL/Gold/.env/PBIX, retrain/tuning/đánh giá lại June, restart/recreate PG/MLflow, xóa volumes, commit/push. Dừng sau ML P4.
