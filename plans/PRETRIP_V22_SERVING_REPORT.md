# PRETRIP V2.2 — serving và giao diện pretrip (chưa deploy)

Base HEAD `77cc0fc2b783b92698c5758fb9cd13a113571dbd`. Lượt này chỉ thêm đường
serving tường minh, chuẩn bị bundle local, tests tổng hợp và parity offline.
Không training/tuning, mở lại August, gọi Mapbox/inference production/DB hoặc
thay vòng đời services. Production mặc định vẫn là v1.

## 1. Ứng viên và chính sách phục vụ

Không thay artifact/model/preprocessing/baselines. Pins đầy đủ:

| Identity | SHA256 |
|---|---|
| Model tree | `27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3` |
| Artifact manifest | `8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75` |
| Sample identity | `7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86` |
| Dictionaries | `aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c` |
| Feature builder | `0f2cd9b8b3d84ddbd7f9b0aa49cbcc3acdd8d7ed75432281d6f62501f9fa091a` |
| Calendar | `00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac` |
| NYC TZif | `e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95` |
| Public zone lookup | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` |

Backend kiểm manifest và toàn bộ listed files trước khi load PipelineModel,
kiểm exact pipeline inventory, effective parameters, dictionaries, runtime
feature builder/calendar/TZif/lookup. Model load một lần trong process đã chọn;
không fit/refit, không load đồng thời v1 và v2, không tự fallback. Vector giữ
đúng tám chiều: sáu numeric time features, hai nominal PU/DO indexes với
118/225 categories gồm unknown; native/squared/maxBins256 và GBT cũ nguyên vẹn.

**Chính sách serving mới sau kiểm định:** PU phải có trong dictionary PU train,
DO phải có trong dictionary DO train. Chỉ có ở vai trò đối diện vẫn bị từ chối;
unseen trả “chưa hỗ trợ ước tính”, không trả giá hay baseline thay thế. Không
dùng fare thực tế/fare band để chọn model. Giữ geometry/standard-rate ở UI và
guard tuyến đặc biệt ở backend (EWR; JFK–Manhattan theo lookup đã pin).

Chính sách này **không** loại 64 unseen rows khỏi 21.833 August rows hoặc tính
lại MAE: [holdout report](PRETRIP_V22_HOLDOUT_REPORT.md) và mọi kết quả đã công bố
giữ nguyên. Native vẫn kém baseline ở nhóm unseen và fare thấp. August đã dùng
kiểm định cuối, không dùng tuning tiếp. Không coi MAE lịch sử là cam kết sai số
hiện tại hoặc khoảng ±MAE cho một chuyến.

## 2. UI và contract thời gian

`FARE_MODEL_VERSION=v2.2` chọn client/contract riêng; mặc định là v1. Luồng địa
chỉ dùng autocomplete, geometry, routing, bản đồ và marker hiện có. Mặc định
**Đi ngay** chốt UTC instant lúc submit rồi chuyển America/New_York; hẹn giờ
là tùy chọn trong bảy ngày, với calendar hữu hạn 2025–2027 và guard DST/fold
chung train–serve. Không ánh xạ ngày hiện tại về 2025. Backend kiểm freshness
của submit theo đồng hồ riêng (không quá 60 giây cũ hoặc 5 giây trong tương lai).

Kết quả lưu cùng submission UUID và inputs đã gửi, không đổi theo form chưa
submit; rerun không gọi lại Search/Directions/predict/map. Metadata công khai đã
xác minh được giữ theo session, URL và full candidate identity; rerun không gọi
lại cả readiness/zones sau khi tải thành công. Đổi URL/identity làm mất cache;
mỗi prediction vẫn kiểm response thật, không coi cache là bằng chứng health.
UI ghi rõ cước cơ bản
USD chưa gồm phụ phí; model chỉ dùng zone/thời gian. Khoảng cách tuyến trên map
chỉ tham khảo, **không** đi vào vector V2.2. Chi tiết kỹ thuật nằm trong expander.
UI mới chưa deploy; AppTest/mock không thay cho browser live.

## 3. Bundle local và startup guards

Nguồn duy nhất là frozen candidate:
`.sandbox/pretrip-v2/training-20261027-native-01/artifacts/`.
Lock Git-trackable: `config/model_bundle_v22.lock.json`; không đưa model binaries
vào Git. CLI `scripts/prepare_pretrip_serving_bundle.py` mặc định PLAN, không
mở candidate/ghi output. Execute dùng worker có mount nguồn read-only và
output riêng, **không** yêu cầu Python/Java trên host:

```text
python scripts/prepare_pretrip_serving_bundle.py --execute
  --prior-task-bytes <bytes toàn runtime V2 đã đo trên host ngay trước lượt>
  --disk-budget-mib 2048 --scratch-reserve-mib 512
```

Đây là một lệnh CLI xuống dòng minh họa tham số, không phải hướng dẫn bỏ qua
harness/mount/resource guards. Worker không mount raw/samples/August; vì vậy
không được suy số bytes toàn tác vụ từ filesystem nhìn thấy trong container.
`--prior-task-bytes` bắt buộc khi execute, tính cả runtime cũ mà không đếm lại
candidate; chừa 512 MiB scratch và metadata reserve, chặn trước khi ghi vượt
quota. Giới hạn logical files của tác vụ, không giới hạn biến động C:/VHDX/pagefile.

Bundle tại `.sandbox/pretrip-v2/serving-bundle-v22-01/` gồm `artifacts/` giữ nguyên
bytes của frozen manifest/listed files, `zones.csv` pin riêng và
`serving_manifest.json` ghi identities/chính sách seen-only/no fallback.
Baselines còn trong inventory để bảo toàn artifact identity, không dùng thay giá
khi serving. Không overwrite nguồn/model v1; chạy lại chỉ reuse sau kiểm hashes.
**Bundle local-only**, chưa phân phối qua Git/URL: clone mới không tự có candidate
và không được tuyên bố một lệnh setup V2.2 từ máy trống.

`start.ps1 -WithInference -ModelVersion v2.2` chọn đúng ba Compose layers, xác
minh bundle và readiness/full identity/load count trước chấp nhận; drift chỉ
đưa lệnh cập nhật riêng, không tự recreate/fallback. `setup_system.ps1` vẫn
bootstrap v1; chọn `-ModelVersion v2.2` bị chặn rõ trước ETL/lifecycle và hướng
dẫn chuẩn bị bundle + startup riêng, không âm thầm chạy package với v1.

## 4. Kiểm chứng và tài nguyên

| Kiểm tra thực sự đã chạy | Kết quả |
|---|---|
| Worker unit tests bị ảnh hưởng, dữ liệu tổng hợp | **131 PASS** |
| UI/AppTest bị ảnh hưởng, clients mock | **67 ca phân biệt PASS**, gồm 15 ca V2 sau sửa cache metadata |
| Pester startup/model selection bị ảnh hưởng, Docker mock | **76 ca phân biệt PASS**, gồm guard package V2 và full identity/fit_calls |
| Compose base + inference + V2.2 override `config --quiet` | **PASS**, không in resolved secrets |
| Bundle CLI PLAN | **PASS**, không mở candidate/ghi files |
| Bundle prepare/hash/reuse thực tế + host startup artifact guard | **PASS**, source không overwrite, full identities khớp |
| Frozen pipeline ↔ serving/client parity tổng hợp | **1 PASS**: 8 direct fixtures + 1 HTTP/client fixture; sai lệch tối đa **0**, fit_calls=0, model_load_count=1 |
| Source-mounted web package smoke và runtime NYC TZif | **PASS**, public assets/263 geometry IDs/pins, không load model |
| Backend/bundle trong web image không có PySpark | **PASS** imports và 2 mock regression cases đã có trong 131 ca phía trên |

Parity dùng inputs tổng hợp, không đọc raw/sample/August hoặc gọi service thật.
Reference và serving dùng chính cùng frozen pipeline đã load một lần; nominal
metadata giữ tám chiều, PU/DO118/225. Đây là parity implementation, không phải
một lần đánh giá accuracy mới. Guard package V2 trả lỗi trước context, Docker,
config, bootstrap và model installation; case riêng đã chạy mock PASS. Ba lỗi
assertion ban đầu (deepcopy đối tượng mock và biểu diễn float nhị phân) đã sửa
ở tests, rồi chạy lại đúng cases liên quan; không đổi số học hoặc model để PASS.

Không chạy lại training/holdout/preparation/CI cũ. Synthetic worker và UI chạy
tuần tự, image sẵn có + source/config read-only, không build/pull/install.
Worker parity giữ 2 CPU/3 GiB memory, tổng memory+swap cũng 3 GiB (swap=0),
heap1536 MiB, PID512, thread limits,
tmpfs exec512 MiB và network none; không áp ngưỡng RAM host 2 GiB.

Bundle thêm **468.062 bytes (0,446 MiB)**; tổng V2 sau bundle **197.126.230 bytes
(187,994 MiB)**. Cộng reserve scratch512 MiB là **699,994/2048 MiB**, không cấp
lại ngân sách từ đầu. Peak cgroup memory của parity **901.500.928 bytes
(859,738 MiB)**; summary elapsed **50,138 giây**, pytest **51,86 giây**, container
**55,743 giây**. Không gọi reserve tmpfs là peak scratch thực đo. Các số bytes
trên chỉ là tác vụ quản lý, không quy biến động toàn ổ C:/VHDX/pagefile cho job.
Python peak RSS là 162.328 KiB; cgroup OOM/oom_kill đều 0. Host available RAM
trước/sau checkpoint là 2160,94/968,44 MiB; C trống 78,256/78,013 GiB
(biến động toàn ổ -248,188 MiB, không khẳng định đều do tác vụ).

Đối chiếu cuối: cả 22 hashes bảo vệ không đổi; 45 listed artifact members của
candidate gốc vẫn khớp manifest. Bốn services production đều running/healthy,
ID/StartedAt nguyên vẹn; containers/scratch tạm đã dọn, bundle local được giữ.
HEAD giữ nguyên, index trống. PBIX/.env/PACKAGE P3/v1 không sửa.

## 5. Files và triển khai/rollback — chỉ kế hoạch

Nhóm code: `pretrip_inference.py`, `pretrip_serving_contract.py`,
`inference_server.py`; dashboard `pretrip_prediction.py`, `prediction.py`,
`fare_page.py`, `address_page.py`, `route_fare.py`; bundler/CLI/model-selection
helpers, `start.ps1`, `start_inference.ps1`, `setup_system.ps1`; lock và override;
Dockerfiles/package smoke, CI inventories/Compose checks; tests liên quan;
README và report này. Không sửa feature helper/contract/calendar frozen của v1/V2
hoặc phần PACKAGE P3/PBIX ngoài phạm vi.

**Chưa thực hiện các lệnh dưới đây.** Cần duyệt bước deploy riêng và bảo đảm
ngân sách build/cache; không mặc định cache chắc chắn hit. Tags ứng viên riêng:
`nyc-taxi-inference:pretrip-v22-candidate` và
`nyc-taxi-dashboard:pretrip-v22-candidate`. Với bundle đã chuẩn bị/xác minh:

```powershell
docker compose -f docker-compose.yml -f docker-compose.inference.yml -f docker-compose.pretrip-v22.yml build fare-inference dashboard
docker compose -f docker-compose.yml -f docker-compose.inference.yml -f docker-compose.pretrip-v22.yml up -d --no-deps --no-build --pull never --wait --wait-timeout 180 fare-inference
# Chỉ tiếp tục sau khi /ready và startup guard xác nhận full V2 identity,
# fit_calls=0, model_load_count=1. Health đơn thuần chưa thay cho identity.
docker compose -f docker-compose.yml -f docker-compose.inference.yml -f docker-compose.pretrip-v22.yml up -d --no-deps --no-build --pull never --wait --wait-timeout 180 dashboard
```

Chỉ hai services hiện có, không tạo inference service thứ hai. Hai model không
được chạy đồng thời. Address network vẫn opt-in runtime, giữ token local hiện có,
không đưa secrets vào image. PostgreSQL/MLflow/worker/model v1 không cập nhật.
Muốn dùng địa chỉ online, bật `FARE_ADDRESS_ALLOW_NETWORK=1` trong môi trường
tiến trình Compose được duyệt lúc deploy; giữ token local hiện có, không sửa
`.env` trong lượt này. Requirements không đổi và COPY nằm sau dependency install,
nhưng chưa build nên chưa đo được image delta/cache hit; không cam kết chỉ cần
468 KiB hay chắc chắn không tải base/dependencies ở bước build tương lai.
Sau deploy và khi config đã khớp, mở lại bằng:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference -ModelVersion v2.2
```

Rollback riêng hai services, dùng nguyên base+inference và giữ tags cũ
`nyc-taxi-inference:ml-p4` / `nyc-taxi-dashboard:2b-autocomplete-20261026-01`:

```powershell
docker compose -f docker-compose.yml -f docker-compose.inference.yml up -d --no-deps --no-build --pull never --wait --wait-timeout 180 fare-inference
# Xác nhận readiness/full v1 identity trước khi trả UI về phiên bản cũ.
docker compose -f docker-compose.yml -f docker-compose.inference.yml up -d --no-deps --no-build --pull never --wait --wait-timeout 180 dashboard
```

Sau rollback dùng startup mặc định v1; không tự fallback khi V2 lỗi. Chuẩn bị
image/triển khai/browser live/health trên service thật **chưa kiểm chứng trong
lượt này**. Không stage/commit/push.
