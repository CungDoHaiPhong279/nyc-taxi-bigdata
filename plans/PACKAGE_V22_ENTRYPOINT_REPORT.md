# PACKAGE V2.2 — ENTRYPOINT REPORT

Base `67ae12fd9e66ffe603b4b7c078db41315afd8ede`.
Phạm vi: code entrypoint pretrip độc lập, tests mock/static và PLAN; chưa
Execute/build/startup thật, chưa fresh-machine E2E hoặc đóng gói analytics.

## Code và đường chạy

- Mới: `scripts/setup_pretrip_v22.ps1`, `scripts/pretrip_app_images.ps1`,
  `docker-compose.pretrip-app.yml`, `config/pretrip_app.env`.
- Sửa `dashboard/app.py`: `NYC_TAXI_DASHBOARD_MODE=pretrip` buộc V2.2, mở thẳng
  form; không import analytics ở nhánh này, không cần DB. Thiếu network/token
  thì hướng dẫn trước mọi API. Default full giữ hành vi cũ.
- `.dockerignore` loại host `*.egg-info` khỏi build context; helper cũng loại
  metadata này khỏi input inventory. `Dockerfile.dashboard` chỉ thêm COPY tests
  vào test target, không đổi dependencies/runtime build steps.
- Mới 5 test files: `pretrip_v22_app.Tests.ps1`,
  `pretrip_app_images.Tests.ps1`, `test_dashboard_pretrip_mode.py`,
  `test_dashboard_pretrip_mode_static.py`, `test_pretrip_app_compose.py`.
  CI Pester/Python inventories và assertions cập nhật; CI Compose thêm 2
  variants app/test (tổng24 quiet validations khi chạy CI). Không thêm job.
- README chỉ thêm mục entrypoint; PACKAGE P3 và PBIX ngoài phạm vi.

CLI PLAN mặc định; `-Execute` dự kiến preflight → cài bundle từ Git → build/reuse
images → inference → full readiness/identity → dashboard. Không acquisition,
ETL/training/PostgreSQL/MLflow hoặc gọi production startup. Root lấy từ script,
không phụ thuộc cwd. Public empty envfile ngăn tự đọc project `.env`.

| | App | Test |
|---|---|---|
| Project | nyc-taxi-pretrip-v22 | nyc-taxi-pretrip-v22-test |
| Loopback port |18522|28522|
| Network | project_app | project_app |
| Persistent volumes |none|none|

Owner SHA gắn checkout root + environment; container/network foreign, hostport
collision hoặc config/image drift đều dừng. Chỉ một RO bind model repository
đã kiểm, không có writable host bind/DB credentials. Rerun healthy reuse;
exact stopped container start; missing service tạo tường minh no-deps/no-recreate.
Lỗi dừng stage sau, giữ outputs đã có; không tự reset/restart/overwrite.

## Model và image guards

Bundle dùng nguyên ZIP92059bytes,47files/465289expandedbytes, SHA
`ae809b654b931a717b5f4f9646257e6163ce4913b87be7e9d275e1612a3ce80b`.
Model-tree giữ nguyên
`27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3`.
Dùng installer/lock/full artifact guard đã kiểm; không chạy lại31 tests cũ.
16 parity fixtures nguồn train/validation vẫn giữ nguyên, không synthetic.

Images có tags theo source/dependency hashes, ownership/provenance labels và
byte-verification offline trước lifecycle. Inference identity gồm worker base
identity. Cold path build Dockerfile.spark rồi Dockerfile.inference với explicit
WORKER_RUNTIME_IMAGE; dashboard build target dashboard từ Dockerfile.dashboard.
Không phụ thuộc P6/local image tác giả. Worker là build base, không job. Reuse
Execute cũng kiểm runtime files, pip/import/assets trong verifier không network,
không mount/load model. PLAN chỉ read-only Docker metadata, không chạy verifier.
Dependency inputs được pin bằng hash; requirements/ranges hiện có chưa khóa
đầy đủ resolved dependencies. Cold build/download/time/disk chưa đo.

## Kiểm chứng thực tế

- PowerShell5.1.26100.9711/Pester4.10.1 cache:28/28 orchestration PASS,
  16/16 image guards PASS,0skip/pending. Docker/build/network/lifecycle đều mock.
- Lỗi đầu là Pester scope/fixture: isolate Context, Assert-MockCalled ScopeIt,
  snapshot hai baseline ổn định, mock container biến rõ ràng; không nới assertions.
- Python:9/9 stdlib routing/static PASS;22 CI/Compose/inventory tests liên quan
  PASS. Đây không phải Streamlit AppTest.
- AppTest: checkpoint28/10 chưa chạy do host thiếu Streamlit. Ngày29/10/2026,
  đã dùng image test sẵn có + clean-copy source read-only theo phạm vi mới được
  duyệt: **9/9 PASS,0failed,0skipped**. Không cài dependencies hoặc build/pull.
  Danh sách chính xác và bằng chứng cô lập ở phần bổ sung bên dưới.
- Compose config --quiet thật:app/test PASS; không in resolved environment,
  không start services. Parser/inventory và clean-copy PLAN ghi tiếp bên dưới.

## Clean copy, tài nguyên và giới hạn

- Clean copy `.sandbox/pv22app-01/`: 274 files / 6.872.101 bytes,
  từ Git base + đúng overlay entrypoint; không .env/data/models/candidate hoặc
  PACKAGE P3 dirty. Git archive chuyển EOL theo attributes; đã đối chiếu LF
  của bootstrap/package helpers/.gitignore với base, không lấy dirty bản gốc.
- CLI thực từ `C:\\Users`:
  `powershell.exe -NoProfile -ExecutionPolicy Bypass -File
  <clean-copy>\\scripts\\setup_pretrip_v22.ps1 -Environment test`.
  Exit0, PLAN, 25,859s; toàn bộ hashes và file/directory mtime không đổi.
  Model chưa cài, cả3 images báo build-required; không build/download/start.
  Evidence riêng tại `.sandbox/pretrip-v2/entrypoint-20261028-01/`.
- Giữ cả hai checkpoint PLAN lỗi: harness Python kế thừa PS7 module path khiến
  PS5 không autoload được Get-FileHash (CommandNotFoundException). Probe cùng
  command chỉ đọc PASS khi bỏ PSMODULEPATH không phân biệt hoa/thường để PS5 dùng
  native defaults; CLI -File khi đó PASS. Đây là sửa harness, không phải lỗi
  ứng dụng hoặc guard được nới; không ghi đè before/after cũ.
- PS5 parser:5 files mới/bị sửa PASS; inventory15 Pester suites khớp. Scoped
  diff --check PASS. Kiểm cuối5 boundary tests PASS0,37s sau edit tests cuối.
- Accounting bảo thủ: 727.15/2048MiB, gồm toàn runtime V2 cũ,
  hai clean copies, toàn bộ scoped source và distribution, image-layer deltas
  cũ13.268.829bytes + scratch reserve512MiB theo report trước. New clean copy
  6,554MiB; runtime evidence tăng 297453bytes; không có layer/context build mới.
  Đây là logical files + reserve, không cam kết toàn ổC/VHDX/pagefile.
- C trống snapshot 71.62→71.41GiB; không quy toàn bộ chênh lệch cho tác vụ.
  Peak RAM chưa đo; không model/containers thật, không ngưỡng RAM host2GiB.
- 9 protected hashes khớp trước/sau (.env,PBIX và P3); ZIP distribution vẫn
  đúng hash đã pin. Không thay services/volumes, artifacts, startup/full setup
  cũ hoặc stage/commit/push. Không xóa tài nguyên có trước.

Chưa commit/push. Chưa build/download image, Execute/startup/rerun thật hoặc
browser/API/model; không chứng nhận accuracy hiện tại. Full analytics/bootstrap
vẫn là luồng riêng, không được tự mở khóa. AppTest đã hoàn tất trong bổ sung
29/10; bước tiếp theo vẫn là build/startup trên môi trường thử được duyệt,
đo cold build/network/storage trước tuyên bố fresh-machine E2E.


## Bổ sung 29/10/2026 — đúng 9 AppTest offline

Image có sẵn `nyc-taxi-dashboard:ml-p5-test`, full ID:
`sha256:06af6f642a39ef68dc2e0db4e23d713800b9d8f2579f378602faec5c9fcddc7c`.
Created2026-10-23T17:51:59.875567762Z; size907.795.754bytes là image đã có,
không phải dung lượng tăng thêm. Runtime Python3.11.17, Streamlit1.66.0,
pytest9.1.1. Không pull/build/install hoặc dùng model/clients production.

### Node IDs và kết quả

```text
tests/test_dashboard_pretrip_mode.py::test_pretrip_opens_real_fare_form_without_analytics_and_rerun_reuses_metadata
tests/test_dashboard_pretrip_mode.py::test_unconfigured_pretrip_explains_runtime_config_without_any_api[FARE_ADDRESS_ENABLED]
tests/test_dashboard_pretrip_mode.py::test_unconfigured_pretrip_explains_runtime_config_without_any_api[FARE_ADDRESS_ALLOW_NETWORK]
tests/test_dashboard_pretrip_mode.py::test_unconfigured_pretrip_explains_runtime_config_without_any_api[MAPBOX_ACCESS_TOKEN]
tests/test_dashboard_pretrip_mode.py::test_pretrip_rejects_wrong_model_without_v1_fallback[v1]
tests/test_dashboard_pretrip_mode.py::test_pretrip_rejects_wrong_model_without_v1_fallback[invalid]
tests/test_dashboard_pretrip_mode.py::test_unknown_dashboard_mode_fails_closed_without_database
tests/test_dashboard_pretrip_mode.py::test_default_and_explicit_full_preserve_analytics_navigation[None]
tests/test_dashboard_pretrip_mode.py::test_default_and_explicit_full_preserve_analytics_navigation[full]
```

**9 collected/9 passed/0 failed/0 skipped**, pytest9,22s; tới khi thu metrics
harness16,172s. Không sửa runtime code hoặc tests, nên đây là kết quả của đúng
bản cuối đã có. Không chạy lại44Pester,9static hoặc22tests trước.

- Pretrip mở form V2.2/Đi ngay, không có analytics navigation/KPI; rerun chỉ
  đọc mocked metadata một lần, không tạo Search/coordinator hoặc gọi SQL.
- Thiếu từng flag/token hiển thị hướng dẫn trước mọi API. Sai model v1/invalid
  và sai mode đều fail closed, không fallback.
- Default/full đều giữ trang analytics, navigation, KPI và3charts trên snapshot
  synthetic đã mock. Không dùng database thật để làm bằng chứng này.
- Mỗi ca có clients mock theo test; harness bổ sung fail-fast HTTP và DB
  transports. Counters thực tế **HTTP0/DB0**; module Spark/model inference
  không được load (0). Không mount model/bundle/candidate/data/.env/socket.

### Cô lập, sửa harness và cleanup

Một container thực sự chạy: `dcc78b4702048165ed01c68c5302e3c9799f021f33c820d1b3b11bbc01a0abfc`.
Networknone, rootread-only,1CPU/768MiB RAM+swap cap,128PIDs; private tmpfs
/tmp128MiB cho HOME/cache/pytest temp, cacheprovider tắt. Chỉ6 RO mounts từ
clean copy:src/dashboard/tests/public config/assets/pyproject.toml. Các imports
chạy từ /workspace, không dùng code cũ nằm sẵn trong image.

Lượt tạo đầu tiên bị Docker từ chối **trước khi start/test** vì local log driver
bật compression không cho max-file1. Đã xác minh owner và xóa đúng Created
container; đổi riêng harness max-file2,max-size1MiB. Không sửa app/guards hoặc
retry API. Container chạy tests được xóa trong finally; hai container IDs của
harness (một Created-never-started,một chạy) đều đã dọn, tmpfs/logs tạm cũng mất.
Image không bị xóa/thay; không có test container còn lại.

### Tài nguyên và bảo toàn

Cgroup memory.peak **245.354.496bytes =233,99MiB**, limit805.306.368bytes;
memory.events oom/oom_kill/max/high đều0, OOMKilled=false. Không dùng snapshot
host để suy ra peak. Accounting cùng envelope khoảng **727,17/2048MiB**, đã
bao gồm runtime/copies cũ, old layer deltas và scratch reserve512MiB theo cách
đếm trước; tmpfs128MiB thuộc reserve này, không cấp lại budget. Evidence mới
12.807bytes tại `.sandbox/pretrip-v2/entrypoint-apptest-20261029-01/`.
C trống70,507→70,501GiB chỉ là snapshots toàn máy, không quy toàn bộ delta cho tests.

Clean copy giữ toàn bộ hashes/file+directory mtime so với snapshot PLAN trước.
9protected hashes khớp (.env,PBIX/P3),4production IDs giữ nguyên và healthy:
49c3dc54a838/dashboard,24b4ef7ca0ed/inference,930e1ad167c9/postgres,
6db6f54eda60/mlflow. Không gọi production APIs, thay lifecycle/volumes/artifacts.
Files có nội dung sửa trong lượt này chỉ report và câu trạng thái tương
ứng ở README; code/tests giữ nguyên. Evidence/harness nằm trong .sandbox,
không stage/commit/push.

**Giới hạn giữ nguyên:** đây là Streamlit AppTest trên runtime image cũ +
source mới RO, không phải browser live hoặc smoke image mới. Image build,
setup -Execute/startup/rerun thật và full fresh-machine E2E **chưa kiểm**;
không chứng nhận accuracy hiện tại hoặc đóng gói toàn bộ analytics.
