# PACKAGE P1 REPORT

Ghi nhận lịch sử P1. PACKAGE P2 thay thế kết luận về model distribution bằng
bundle kèm source có provenance notice; full bootstrap E2E vẫn chưa kiểm chứng.

2026-10-24. Đã hoàn tất entrypoint và kiểm chứng offline/PLAN; **chưa đóng gói
hoàn chỉnh cho clone mới** vì model distribution và full bootstrap còn thiếu.

## 1. Files và lệnh mục tiêu

- Mới: `scripts/setup_system.ps1`, `package_environment.ps1`,
  `package_operations.ps1`, `model_bundle.ps1`; `docker-compose.package.yml`,
  `docker-compose.package-test.yml`, `config/model_bundle.lock.json`.
- Bổ sung context package vào `scripts/start.ps1`, `bootstrap.ps1`,
  `docker_environment.ps1`, `start_inference.ps1`, `model_artifacts.ps1`;
  `Dockerfile.inference` có build ARG, default cũ giữ nguyên.
- Tests: mới bốn `tests/{model_bundle,package_environment,package_system,package_validation}.Tests.ps1`;
  cập nhật `tests/start_inference.Tests.ps1`, `tests/test_ci_compose.py`,
  `scripts/ci_pester.ps1`, `scripts/ci_validate_compose.py`.
- Docs: README, checklist bàn giao, report này và ignore whitelist chính xác.
  PBIX dirty có sẵn giữ nguyên, không stage/commit/push.

```powershell
# Mở lại hệ thống cũ đã có dữ liệu/model, không bootstrap:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference

# Lệnh mục tiêu MÁY MỚI, chỉ khi đã nhận đúng bundle; CHƯA chạy trong P1:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -BundleDirectory 'D:\trusted-bundles\ml-p4-v1' -Execute
```

## 2. Khởi tạo/chạy lại và nguồn

PLAN mặc định. Execute: preflight/config → images chuẩn → PostgreSQL/MLflow →
bootstrap hiện có (14 CLI steps) Calendar → Raw → Bronze → Silver → Gold →
dimensions/sáu facts → indexes/view → full SQL validation → cài model riêng →
inference/dashboard/readiness. Đủ Jan–Jun/2025, không fixture/training/latest.
Gold cần9Silver, loader cần6Gold; chưa có chế độ một tháng độc lập.

Raw lấy Yellow Parquet/zone CSV từ [TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
weather ERA5 Open-Meteo archive (UTC), Calendar local. Đã đối chiếu code/nguồn
công khai; **chưa tải và kiểm chứng acquisition**. Free weather API có điều kiện
non-commercial/attribution theo [terms](https://open-meteo.com/en/terms).

Rerun dùng SHA/manifest/lineage skip và loader idempotency/repair cũ, không force/
recreate-schema. Có exclusive lock, checkpoint stage, fail-fast/timeout; checkpoint
không thay cho validation. READY cần report mới từ COUNT/FK/sanity SQL thực tế,
khớp Gold counts/batches, dimensions/indexes/view và pinned model ready. Startup
light check chỉ health/schema/existence/load-state, không chứng minh tổng fact.
Drift hoặc tài nguyên cũ chưa có package ownership: từ chối adopt/recreate, in
lệnh cập nhật riêng đúng Compose layers/project; không thực thi lệnh đó.

## 3. Model local và blocker phân phối

Đã tạo và check thật `.sandbox/package-bundles/ml-p4-v1/`: ZIP63.680 bytes,
manifest9.822 bytes,43files (model40files/55.660bytes + lock/contract/zones),
không dữ liệu chuyến/credentials/MLflow SQLite. Pin đầy đủ:

| Identity | Giá trị |
|---|---|
| Training | `d2772f1904d7472490a73b0ac1c0c5d4` |
| Evaluation | `58d5008f0ed048f58599a859877ae336` |
| Model SHA256 | `759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee` |
| ZIP SHA256 | `7770b7a64722b0a801015f99f0c6b5d2ea3aa39dbc15bc01cb7b6dd376293105` |
| Manifest SHA256 | `da6d5229edd41bbfda490a8585f161024d7dcb3d532e2ee58309fb13e7d14ca7` |

Kiểm tra độc lập public pin, inventory/size/ZIP traversal và tám frozen code/
dependency inputs. Cài atomic vào model root riêng, đúng thì skip, sai thì dừng;
không ghi đè model gốc. **Chưa upload/publish**, chưa có URL/source phát hành
hoặc xác nhận quyền phân phối. Phương án: chủ artifact duyệt provenance/quyền,
phát hành đúng version bất biến (ví dụ Release asset), pin URL + checksum trong
lock rồi bổ sung fetch hữu hạn. Hiện chỉ import bundle do chủ model cung cấp;
không tuyên bố chỉ clone là chạy được.

## 4. Kiểm chứng thực tế

| Check | Kết quả |
|---|---|
| PowerShell parser + central Pester4.10.1 | **326/326 PASS**,11suites,25files parse,294,17s;0fail/skip/pending |
| Python CI helpers (`test_ci_compose.py`, `test_ci_python.py`) | **44 PASS**,1,47s; local venv, không tuyên bố fresh dependency install |
| Compose quiet, default/all profiles | **18/18 PASS** với pinned2.40.0 và Docker Desktop5.6.0; không start services |
| Bundle Create trước ngắt / Check sau tiếp tục | **PASS**, full hashes trên; không cài vào test/runtime gốc |
| PLAN/preflight package-test, cả từ cwd ngoài repo | **PASS**,exit0; không build/pull/start/install/worker/bootstrap |
| PLAN trỏ bundle không tồn tại | Dừng đúng,**exit1**, không fallback/download/training |
| Diff check tracked/staged và file mới; public secret-pattern scan | PASS; không in secret, runtime/bundle được ignore |

Lỗi cũ308/309 là test missing-report dùng lại thư mục tạm; đã cách ly mỗi ca.
Đã bổ sung junction/bind leaf guards, kiểm tra actual volumes/networks/ports,
drift command đúng môi trường, concurrent lock/release, manifest sáu tháng độc
nhất, chặn UNC/URL trước filesystem lookup và freshness/full-validation contract.
Mọi sửa code đã nằm trong lượt326tests cuối.
Không chạy lại training/evaluation, Spark pipeline tests hoặc CI remote.

## 5. Môi trường trống và kiểm chứng tiếp theo

Đã chuẩn bị **config + bốn runtime directories trống**, không khởi động stack:
project `nyc-taxi-package-test`; không fixed container names; MLflow127.0.0.1:15000,
dashboard127.0.0.1:18501; PG/inference không publish. Hai volumes
`nyc-taxi-package-test_{postgres,mlflow}-data`, hai networks riêng default/inference;
RW binds chỉ `.sandbox/package-test/{data,logs,models,reports}/`, serving root
`model-bundle/` riêng (chưa cài). Không dùng bootstrap-test/fixture P6.3. Chưa có
containers/volumes/networks package-test thật.

```powershell
# Đã chạy config-only trước ngắt, PLAN lại sau sửa:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Environment package-test -PrepareTest
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Environment package-test

# Dự kiến bước sau, CHƯA THỰC THI:
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Environment package-test -BundleDirectory '.sandbox/package-bundles/ml-p4-v1' -Execute
```

Đo sau tiếp tục: physical host free drive runtime71,27–71,54GiB; hostRAM15,35GiB;
DockerRAM7,44GiB/16CPUs. Guard tối thiểu60GiB physical free/8GiB host/7GiB Docker/
2CPUs. Đây là tổng RAM cấp phát, không phải headroom đang trống; cần rà ngân sách
khi chạy hai stack. Nếu VHD ở drive khác cần kiểm tra thêm drive đó. **Ước tính**, chưa đo:
60GiB headroom,3–8+giờ hoặc lâu hơn tùy network/I/O/WAL/shuffle; worker2CPU/3GiB,
JVM1536MiB. Sau Execute riêng cần full actual counts/lineage + readiness, rerun
skip/no duplicates và UI thật; không dùng mocks/PLAN thay bằng chứng E2E.

## 6. Bảo toàn và phần chưa đạt

Snapshot trước P1 và sau tiếp tục: **4 services healthy, IDs/StartedAt/images/
mounts giữ nguyên**,4protectedimage tags và302hashes (gồm .env/PBIX/manifests/
Gold/model gốc) không đổi. Hai volumes thật cùng attachments cũ. Không SQL write,
ETL/loader, bootstrap Execute, restart/recreate/down/prune hoặc xóa volumes.
Không commit/push. Không còn job/package build/test nền khi hoàn tất kiểm tra.

Local evidence: `.sandbox/package-p1/20261024-preflight01/` (private/ignored).
Chưa build các tags package mới, chưa bật stack mới/model readiness từ bundle,
chưa acquisition/full bootstrap/rerun E2E trên môi trường trống, chưa xác nhận
UI của stack mới hoặc CI remote cho P1. **Distribution approval/source + E2E
riêng là blockers trước khi gọi đóng gói hoàn chỉnh.**
