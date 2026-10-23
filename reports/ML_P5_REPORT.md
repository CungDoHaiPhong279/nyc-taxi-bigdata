# ML P5 REPORT — vận hành và giao diện demo

2026-10-24, Asia/Saigon. Hoàn tất P5; **không training/tuning/đánh giá lại June, không commit/push**.

## 1. Files và lệnh sử dụng

- Startup: `scripts/start.ps1`, mới `start_inference.ps1`, `model_artifacts.ps1`.
- UI: `src/nyc_taxi/dashboard/fare_page.py`, `tests/test_dashboard_prediction.py`.
- Mock tests/CI: mới `tests/start_inference.Tests.ps1`, `model_artifacts.Tests.ps1`; inventory `scripts/ci_pester.ps1`; workflow chỉ bổ sung packaged web imports `prediction`, `fare_page`.
- Docs: README, model card P3, report này; `.gitignore` chỉ thêm ngoại lệ đúng report P5. Không sửa Compose/Dockerfiles/model/backend/preprocessing trong P5. Giữ nguyên các thay đổi P2–P4/PBIX đang có.

```powershell
# Kiểm artifact riêng; không cần Python/Docker, không ghi file.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\model_artifacts.ps1
# Mở lại cả analytics và inference, không build mặc định.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference
# Lần đầu, chỉ sau khi chuẩn bị bundle và dependency runtime p6-clean:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference -Build
```

## 2. Artifacts, drift, health và chạy lại

Base Compose + `docker-compose.inference.yml`, project giữ `nyc-taxi-bigdata`; thứ tự PostgreSQL → MLflow → inference → dashboard. Không bật worker/job. Không có switch vẫn giữ analytics startup cũ; inference + bootstrap-test bị chặn.

Preflight kiểm **40 files**, canonical tree SHA256 **`759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`**; lock SHA256 **`12442b5684943bb81169bb084cffc8c1b54043882d75711e295a0f1a0ff7338d`**; contract/zones/tám core source hashes. Thiếu/sai → exit1 trước build/lifecycle/tạo `.env`, hướng dẫn chuẩn bị bundle; không latest/fallback/download. Reject symlink/junction và path ngoài root.

Frozen training **`d2772f1904d7472490a73b0ac1c0c5d4`**, evaluation **`58d5008f0ed048f58599a859877ae336`**. Sau bounded Docker health wait, kiểm `/ready` đúng boolean/status, full identities và `model_load_count=1`; HTTP3s/native command tối đa10s, không predict để kiểm readiness. Healthy reuse, stopped start, missing dùng target `--no-deps --no-recreate --no-build --pull never`. Drift exit2, không tự recreate; in lệnh cập nhật riêng đúng hai Compose files. Stopped + drift bị chặn trước mọi lifecycle.

**Thực tế:** hai lượt `-WithInference` từ cwd `C:\` đều exit0; `.env` không đổi. Chỉ kiểm DB presence/load metadata trong READ ONLY, không đếm lại 24 triệu facts; báo 6 loaded/6 entries, 0 unfinished, không coi đó là full validation/bootstrap.

## 3. UI

Một cảnh báo unseen: “Vùng này chưa có trong dữ liệu huấn luyện; kết quả có thể kém chính xác hơn.” Run IDs/full SHA/API timing nằm trong expander thu gọn. Giá và tên vùng/thời gian/distance luôn thuộc **inputs đã submit**, không gắn nhầm với form đang sửa. Giữ USD/mile/cước cơ bản, standard-rate/demo2025 và giới hạn zero/long/high fare; không đổi giá/preprocessing/font toàn app.

## 4. Kiểm chứng local và prediction

| Kiểm tra thực tế | Kết quả |
|---|---|
| `ci_pester.ps1 -ModulePath <isolated-Pester4.10.1>` | **204/204 PASS**, 7 suites, 56,30s; parser17files; không skip/pending |
| Python CI image `nyc-taxi-ci-python:ml-p5-check`: `pip check`, `ci_python.py --collect-only`, `ci_python.py` | **380/381 discovered**, **380 PASS/1deselected**, lần cuối3,83s; không network/repo mounts/Spark/MLflow |
| Web test target `nyc-taxi-dashboard:ml-p5-test`, ba dashboard test files | **93 PASS/1deselected**, lần cuối3,25s; không network/artifacts |
| `ci_validate_compose.py`, default/all profiles | **14/14 PASS** với Compose5.6.0 và đúng CI2.40.0 |
| actionlint1.7.12, `git diff --check`, packaged web `pip check` | PASS |
| API `check_fare_inference.py --mode service`, fixture/reference P4 RO | **8/8**, max diff **0 USD**, tolerance1e-9; thêm5 invalid requests400/no price |
| `check_fare_ui.py`, AppTest gọi API thật | **3/3** thường/zero/unseen, max diff0; unavailable thật hiện error/no price, SQL calls0 |

Đúng một SQL integration test bị deselect tường minh; Spark/DB integration/P6.3/training không thuộc CI mặc định. Stopped/missing/artifact drift/timeout thử bằng mock, không dừng services thật. Không cần artifacts thật/secrets trên CI.

API warm hiện hữu: request đầu phép thử **2,336s**, các request tiếp **1,150–1,540s**, lặp **1,205s**; AppTest submit cuối **0,575–0,842s**. Không phải cold-start benchmark. Spark application vẫn `local-1792758826962`, model load count1. Fixture SHA256 **`22292e16ef60b74890bbf834e3c93211f8c506b61bf4438165602b7ef4881d99`**; dùng reference độc lập P4, không đánh giá lại June. Analytics AppTests vẫn PASS; không chạy thêm full fact aggregate.

Lỗi kiểm chứng được giữ log: assertion/mocks mới ban đầu dùng wildcard/scope/provider metadata không phù hợp Pester4, đã sửa rồi chạy nguyên suite PASS. Review cuối sửa inventory sang ordinal dictionary để không gộp hai đường dẫn chỉ khác hoa/thường; thêm regression test13/13 artifact tests PASS và validator thật vẫn khớp full hash. UI chỉ suppress cảnh báo unseen, không suppress mọi câu có từ category. Compose2.40 lượt đầu timeout60s ở clean-worker; chạy lại nguyên14checks tuần tự PASS, không tăng timeout/bỏ tổ hợp; chưa xác định nguyên nhân timing. Build CI chỉ cảnh báo ENV password **dummy công khai**, không chứa secret thật.

## 5. Checkpoint lifecycle và bảo toàn

Chỉ cập nhật UI bằng `docker compose -p nyc-taxi-bigdata -f docker-compose.yml -f docker-compose.inference.yml build dashboard`, rồi `up -d --no-deps --no-build --pull never dashboard`; không build/recreate inference/PG/MLflow.

| Service | Trước cập nhật UI → sau UI | IDs sau startup1 / startup2 | StartedAt sau UI, UTC (giữ qua cả hai lần) |
|---|---|---|---|
| PostgreSQL | `930e1ad167c9` → như cũ | như cũ / như cũ | `2026-10-23T12:12:08.029689222Z` |
| MLflow | `6db6f54eda60` → như cũ | như cũ / như cũ | `2026-10-23T12:12:08.041211105Z` |
| Inference | `a9b1b04d73e6` → như cũ | như cũ / như cũ | `2026-10-23T12:33:37.519848437Z` |
| Dashboard | `3cce40a779cf` → `a5ebee1e5153` | `a5ebee1e5153` / `a5ebee1e5153` | `2026-10-23T17:53:16.860480896Z` |

Dashboard trước update StartedAt `2026-10-23T12:36:38.186288375Z`. Có hai cập nhật riêng dashboard: bản đầu `b1efb70b2109`/`17:33:33.897448522Z`, bản cuối sau siết warning như bảng; cả hai chỉ target dashboard. Mỗi bản có hai lượt startup exit0 riêng và checkpoints giữ lại; bảng thể hiện hai lượt cuối. Cả4healthy; health dashboard/MLflow200. Hai volumes vẫn **`nyc-taxi-bigdata_postgres-data`**, **`nyc-taxi-bigdata_mlflow-data`**; mounts/RO flags và protected image tags giữ nguyên. Dashboard thực tế không Java/PySpark; packaged source hashes khớp workspace.

Hash **302 files** không đổi: `.env`, PBIX, bốn manifests, Gold Parquet, model/P2.1/P3/P4 outputs và Compose/fixture công khai. Không ghi DB/Gold/model artifacts, không ETL/loader. Chứng cứ IDs đầy đủ/hash/checkpoints/logs riêng tại `.sandbox/ml-p5/20261024-demo01/` (Git/Docker ignored); không đưa runtime vào Git.

## 6. Giới hạn trước commit/push

URL **http://127.0.0.1:8501 → Ước tính cước**; MLflow http://localhost:5000. Chưa kiểm browser thật: AppTest + HTTP không chứng minh browser render. Chưa commit/push nên **không có CI remote PASS cho P5**. Máy mới phải nhận đúng trusted artifact bundle, chuẩn bị clean dependency runtime và dữ liệu analytics riêng; startup không tự export/train/bootstrap. Serving vẫn là demo nội bộ, chưa public authentication/TLS/rate limiting. Không thay model/đánh giá lại holdout trong P5.
