# PRETRIP V2.2 — local deployment attempts

**Trạng thái mới nhất, attempt 2 tại commit `24fab1e`: V2.2 đang chạy;
Chrome live và startup PASS về vận hành.** Xem phần attempt 2 cuối báo cáo.
Lịch sử attempt 1 bị chặn và rollback dưới đây được giữ nguyên làm đối chiếu.

## Attempt 1 — bc808337, historical: live blocked, rolled back

Ngày 28/10/2026 (Asia/Saigon), base commit
`bc808337f52c1ac8c1d334e4962fecd9b6614f39`. Không commit/push trong lượt này.
**Kết luận: image/readiness/startup V2 đạt; luồng browser live chưa hoàn tất.
Đã rollback riêng inference/dashboard về đúng images/config v1.**

## Images, nguồn gốc và guards

| Image tag | Docker image ID |
|---|---|
| `nyc-taxi-inference:pretrip-v22-bc808337` | `sha256:16b99fa6bc0510064394fd6b8b3fd7633eecc52063b28a24c2744a4ce5740c78` |
| `nyc-taxi-dashboard:pretrip-v22-bc808337` | `sha256:200cb26ec6dcbd12cb44812e53d4a625bbd47b0d08fdd07a0c6db8280f743374` |

Inference dùng Dockerfile.inference tại commit, base worker local
`sha256:32f066cdb5e0827d3b45abc387972ca45650cf43ed05e602cdcb6287dff43cd4`.
Dashboard refresh từ production local (không dùng test image):
`sha256:39524a1b05168e70ce3e6ee7ffe0f6cb4d7b2ed0c42aae903dda3c3065797ac2`.
Không pull/install/nâng dependencies. Builds dùng `--pull=false --network=none`.

Context lấy từ Git commit, không từ scripts/README/PACKAGE P3 đang dirty.
Guard đầu tiên phát hiện requirements dashboard không byte-match: Git archive
theo cấu hình Windows xuất CRLF188 bytes, trong khi blob Git và production là
LF181 bytes. Đã sửa **exporter**, không đổi requirements hoặc nới guard:
`git -c core.autocrlf=false -c core.eol=lf archive ...`. Cả 123 files xuất lại
được đối chiếu SHA1 blob bằng `git hash-object --no-filters`. Requirements
dashboard sau sửa đúng SHA256
`73c07ff1d515c39f1d29dda1579272f7ccc3a5f2b1f430dcb79c282924c13962`;
ba dependency inputs worker cũng byte-match trước build.

Pip check/imports trong hai image mới PASS. Hash runtime so context commit:
113 files inference và 32 files dashboard PASS; geometry263 IDs, component/
frontend assets, public contracts và NYC TZif PASS. Không load model trong
containers kiểm image, không mount source vào services đã triển khai.

Frozen model không đổi:

- Model tree: `27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3`.
- Artifact manifest: `8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75`.
- Bundle local-only `.sandbox/pretrip-v2/serving-bundle-v22-01/`; full artifact/
  dictionary/contract/calendar/TZif guards PASS. Không thay candidate/baseline,
  không training, parity hoặc mở lại August. Parity0/fit_calls0 trước đây giữ
  nguyên là bằng chứng riêng, không chạy lại trong lượt deploy.

## Triển khai và startup thực tế

Đúng project từ labels: `nyc-taxi-bigdata`. Chỉ sửa hai image pins trong
`docker-compose.pretrip-v22.yml`. Ba layers: base + inference + pretrip-v22.
Validate `config --quiet` PASS; config hashes PostgreSQL/MLflow khớp checkpoint.

Recreate riêng `fare-inference`, rồi `dashboard`, mỗi lệnh dùng
`up -d --no-deps --no-build --pull never --wait --wait-timeout 180`.
V1 inference đã exited **trước** lượt này; không start v1 trước V2, không có
hai model-serving containers chạy đồng thời. V2 inference healthy, `/ready`
HTTP200, full identity khớp, fit_calls0/model_load_count1, startup30,288 giây.
Dictionary cho phép PU164 và DO43. Dashboard V2 healthy, mount count0.
Flags địa chỉ/network chỉ đặt trong environment tiến trình Compose/startup;
token lấy bằng cơ chế runtime hiện có, không sửa/in `.env`.

Lệnh đã thực sự chạy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference -ModelVersion v2.2
```

Exit0, tái sử dụng cả bốn containers; IDs/StartedAt trước–sau lệnh không đổi.
Chỉ metadata data-presence/load state của startup; không COUNT fact/full SQL
validation. Kết quả startup này thuộc **thời điểm V2 đang triển khai**, không
có nghĩa production hiện vẫn là V2 sau rollback.

## Chrome live — kết quả và giới hạn đếm

Chrome có sẵn, một tab incognito/profile riêng, WebGL giữ nguyên; image mới
thực sự chạy, không source/assets mounts. Không chạy lại CI/tests/holdout.

| Tiêu chí | Kết quả thực tế |
|---|---|
| Trang V2, mặc định Đi ngay, copy zone/time và distance tham khảo | PASS quan sát UI thật |
| Pickup Search/suggest/retrieve và geometry/role check | PASS, PU164 |
| DO43 có trong dictionary serving | PASS metadata, chưa chứng minh địa chỉ live đã map tới DO43 |
| Chọn điểm đến | BLOCKED: iframe ở trạng thái `searching`, không có choices |
| Giá/payload thời điểm submit thực/identity trong prediction | CHƯA KIỂM, không submit |
| Tuyến/marker A-B/logo/attribution/map render | CHƯA KIỂM, không có route/map load |
| Result UUID/giá/map và requests giữ nguyên sau rerun | CHƯA KIỂM, chưa có result |

Harness ghi nhận iframe dropoff thay trong lúc pickup retrieve hoàn tất.
Seed chọn trang của harness cũ cũng không khớp widget hiện tại: đã thấy trang
analytics mặc định trước khi chọn radio Ước tính. Vì vậy có thể phát sinh
aggregate READ ONLY thông thường của trang mặc định; không đọc/đối chiếu counts
để làm SQL validation riêng trong lượt này.
Lượt gõ sớm mất draft; sau khi gắn lại iframe hiện tại và nhập một lần trong
budget, ô này vẫn hiển thị “Đang tìm địa chỉ…”, trong khi trang không còn busy,
không có exception/alert API-error. Tám dòng logs dashboard gần nhất không có
exception class; không in raw logs. **Chưa xác định lỗi thuộc bridge/state,
harness hay provider; không quy lỗi này cho inference hoặc khẳng định HTTP lỗi.**
Không sửa code để vượt lỗi, không gửi thêm request sau khi quyết định dừng.

Đếm trong bộ nhớ bằng các `streamlit:setComponentValue` event IDs duy nhất và
guard Map constructor/submit click, không HAR/trace/raw responses:

- Browser phát **3 suggest events + 1 retrieve event**, không vượt 6/2.
- Xác nhận thành công qua UI: **1 suggest và 1 retrieve** cho pickup.
  Hai suggest events dropoff chưa có kết quả: không có outbound HTTP transport
  trace để khẳng định chúng đều đã ra mạng. Actual suggest HTTP được xác nhận
  trong khoảng1–3, retrieve1; không báo events là số HTTP đã chứng minh tuyệt đối.
- **Directions0, map loads0, POST predict từ UI0**, submit clicks0.
- Không retry HTTP/predict, không làm parity mới; không lưu token, địa chỉ/
  tọa độ hoặc raw Mapbox responses vào evidence. Screenshot được quan sát trực
  tiếp trong RAM, không lưu file; thấy theme tối và các nhãn/input chính rõ.

## Rollback, cleanup và bảo toàn

Theo điều kiện live chưa hoàn tất, đã đóng context/Chrome thử và rollback riêng
inference rồi dashboard bằng **hai layers v1**, flags public checkpoint1/1,
không build/pull/restart lặp. Images v1:

- Inference `sha256:ef0b3f0fde22bf1257d5d2ef92be8bb8779ade8a186f5144af64ae4ee496a42b`.
- Dashboard `sha256:39524a1b05168e70ce3e6ee7ffe0f6cb4d7b2ed0c42aae903dda3c3065797ac2`.

Hai image/config hashes/mount sets/limits khớp checkpoint; cả hai healthy.
GET readiness v1 HTTP200, model identity đúng, load_count1; không gọi predict.
Container IDs sau rollback (được recreate theo phạm vi đã duyệt):
inference `615660f57801cb43ab0c05e9dc2d5f0a1560e71d2b4b7d47ea80af9cc4a5c5b6`,
dashboard `8dd2b6c3524588056ceed73e1c854de33acc68aa210414b0fde36643ed45936e`.

PostgreSQL/MLflow giữ nguyên IDs/StartedAt/mounts/volumes và healthy. Cả12 hashes
bảo vệ (.env/PBIX/PACKAGE P3/base Compose/candidate manifest) không đổi.
Profile, hai contexts và hai tar exports tự tạo đã dọn bằng explicit validated
paths; không prune/xóa image hoặc volume. Giữ cả images mới, images rollback,
bundle, sanitized checkpoint và build recipe dưới runtime deploy riêng.
Override V2 vẫn pin tags mới để chuẩn bị cho bước sau, nhưng **không nằm trong
layers production đang chạy**. Production hiện là v1; chưa chứng nhận accuracy
cho chuyến hiện tại, không thay đổi metrics August hoặc chính sách unseen.

## Tài nguyên và bước tiếp theo

- Inference build11,845 giây; dashboard refresh16,821 giây.
- Canonical payload4.984.895 bytes (~4,75 MiB), cộng recipe/pins nhỏ; context
  truyền BuildKit1,11 MB inference và3,88 MB dashboard. Context export CRLF lỗi
  ban đầu cũng chỉ là file tạm và đã dọn.
- New layer deltas so đúng bases: inference1.841.106 bytes (~1,76 MiB), dashboard
  5.713.294 bytes (~5,45 MiB); không cộng full image sizes. Tổng delta7.554.400
  bytes là mức cộng bảo thủ (có empty diff-layer dùng chung), không đo physical
  containerd/VHDX/cache bằng phép cộng này.
- V2 inference cgroup memory.peak quan sát1.098.121.216 bytes (~1047,25 MiB),
  OOM/oom_kill0; chưa có predict. Không thu được peak toàn vòng đời dashboard/
  Chrome trước cleanup, không thay reserve bằng số đo peak giả.
- Runtime V2 sau cleanup khoảng188,02 MiB; cộng new layer deltas và scratch
  reserve512 MiB khoảng707,23/2048 MiB. Không cấp lại ngân sách; prebuild có
  kiểm cả runtime cũ và reserves. Không tuyên bố đây là hard cap C:/pagefile/VHDX.
- C trống đầu/cuối khoảng70,088/73,532 GiB; host available RAM2084,07/1847,09
  MiB. Đây là snapshots toàn máy, không quy toàn bộ biến động cho tác vụ.
  Cửa sổ checkpoint tới kiểm cuối khoảng28,4 phút, không phải timeout tổng.

Bước sau cần **tái hiện riêng dropoff bridge/iframe lifecycle bằng dữ liệu giả**,
phân biệt với harness trước khi một lượt live mới được duyệt. Chưa sửa runtime
code hoặc retry live trong lượt này. Files tracked thay đổi chỉ hai pins trong
override, README section deploy và report này; không stage/commit/push.

## Offline autocomplete follow-up (2026-10-28, resumed after limit)

Completed the isolated mock diagnosis and minimal completion/render fix described
in `AUTOCOMPLETE_V22_COMPLETION_REPORT.md`: 66 targeted regressions passed after
the final edit; real Chrome/Streamlit mock cases covered both selection orders,
overlapping retrieve/typing, debounce reruns, rapid input and empty/error.
Final frontend versions/statuses matched backend; terminal reruns added no mock
transport. Original before/after traces retained; final evidence uses new files.

This follow-up did not build/deploy an image or call Mapbox/inference. Production
is still the checkpointed healthy **v1 after rollback**; the V2 deployment remains
**NOT live PASS**. All four production IDs/StartedAt/images and protected file
hashes remained unchanged. Only the isolated trial container/Chrome/profile were
removed. No staging/commit/push.

## Attempt 2 — 24fab1e, live/startup operational PASS

Base `24fab1ed1d670442382e4e8f5ef912cf47a7fe2d`; CI PASS được tái sử dụng,
không chạy lại CI, 66 tests, Chrome mock, parity, training hoặc August.
Checkpoint 28/10/2026 06:44:40 UTC; kiểm cuối sau cleanup 07:40:07 UTC.
Đây là cửa sổ làm việc gồm chuẩn bị/harness/quan sát, không phải benchmark
latency hay deadline tổng. **Không rollback attempt 2; production hiện V2.2.**

### Images và frozen identities

| Service | Tag | Full Docker image ID |
|---|---|---|
| Dashboard mới | `nyc-taxi-dashboard:pretrip-v22-24fab1e` | `sha256:87ed57f003c1d3c478a359f5fa8d92b46918a4d198d370e0e5c93b50835992ae` |
| Inference tái sử dụng | `nyc-taxi-inference:pretrip-v22-bc808337` | `sha256:16b99fa6bc0510064394fd6b8b3fd7633eecc52063b28a24c2744a4ce5740c78` |

Dashboard refresh từ đúng full ID base
`sha256:200cb26ec6dcbd12cb44812e53d4a625bbd47b0d08fdd07a0c6db8280f743374`;
giữ nguyên toàn bộ base layers. Context 33 files lấy bằng Git archive LF từ
đúng commit, đối chiếu blob bytes; không lấy PACKAGE P3 dirty, `.env`, dữ liệu,
models hoặc PBIX. Requirements byte-match, build `--pull=false --network=none`,
không pip install/pull/dependency upgrade. Build **15,43 s**. Pip check/imports,
32 runtime SHA256, geometry263 IDs/frontends/public V2 contract smoke PASS.

Inference không cần refresh: 41 file backend/shared-config/dependency input
Git-blob hashes khớp commit hiện tại; backend/guards không đổi. Provenance
toàn image inference vẫn là `bc808337`, không gọi đây là rebuild từ `24fab1e`
vì image này còn chứa các file dashboard cũ không dùng trong backend.

Candidate và local-only bundle được kiểm trước load: 45 artifact files,
model tree `27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3`,
manifest `8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75`,
dictionary `aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c`
cùng calendar/contract/TZif và zones lock đều khớp. Bundle mount read-only.
Full V2 readiness guard PASS: **fit_calls=0, model_load_count=1**.

### Triển khai, live và startup

Project qua labels `nyc-taxi-bigdata`; đúng ba layers base + inference + V2.
Inference giữ nguyên pin V2 đã xác minh, dashboard pin đổi sang tag mới.
Compose quiet/config hashes data-services đạt; chỉ recreate inference một lần,
sau full identity readiness mới recreate dashboard một lần. Không chạy hai
model-serving containers đồng thời; dashboard mount count0, chạy code image.
Flags1/1 chỉ ở process environment, token theo `.env` hiện có; không sửa `.env`.
Checkpoint đầu lượt thấy v1 inference running/unhealthy; không điều tra hoặc
gán nguyên nhân mới. V2 sau deploy/predict/startup/cleanup đều healthy.

| Tiêu chí Chrome thật | Kết quả |
|---|---|
| Hai ô suggest/retrieve, pickup rồi dropoff | PASS, mỗi ô v1/selected, PU164/DO43 đúng vai trò |
| Không kẹt/remount mất selection | PASS, trusted input còn nguyên; iframe witnesses không đổi qua rerun |
| Đi ngay dùng New York tại submit | PASS: click `07:26:48.729Z`, payload/result `2026-10-28T03:26:48.762094-04:00`, lệch hiển thị giây0 |
| Giá và full frozen V2 identity | PASS, rendered metric **$15.95**, response được client kiểm identity/submission echo; không dùng ngày2025 |
| Tuyến/map/marker/attribution | PASS: **6,075 km**, A/B, route source/layer, Mapbox logo + Mapbox/OpenStreetMap attribution; quan sát screenshot trong RAM |
| Nội dung | Cước cơ bản USD/chưa gồm tip, tolls, phụ phí; zone/thời gian là predictors, distance chỉ tham khảo; chữ đọc được trong theme tối |
| Rerun không submit lại | PASS: một BackMsg `rerun_script`, một ForwardMsg `script_finished` success; giữ inputs, UUID, giá, timestamp, map object và hai markers |

Counters thành công: **2 suggest / 2 retrieve / 1 Directions / 1 UI POST predict /
1 map instance load**, trong limits6/2/1/1/1; không retry API/predict. Search
dựa vào **backend** `args.request_counts` 0→1 mỗi loại/mỗi ô, cùng terminal
results/selected khớp version và code thật một `open`, không redirect/retry.
Directions/predict dựa vào một submit được guard và một immutable outcome
route/model thành công, control flow một lần; **không có transport/access-log
telemetry độc lập**. Không suy HTTP count từ browser postMessage events.
Map constructor counter của harness không cài được trong iframe; dùng một map
đã load và witness cùng object trước/sau rerun, không báo counter0 là map loads0.
Sau rerun backend counters vẫn1/1 mỗi ô, submitClicks1, cùng UUID
`6c0f3640-aee2-4590-b420-3eb4cda3cf1a`, giá15.95, map witness không đổi.

Harness đã sửa riêng: PS5 stripping quote của probe Python gây SyntaxError
**trước GET** (service đã healthy); sửa quoting rồi resume kiểm identity mà
không recreate inference lần hai. CDP cần chờ Chrome sẵn sàng. Prediction-first
seed không chọn đúng radio thực tế dù đã tính ID; dùng native radio click.
Vì trang mặc định có thể đã bắt đầu aggregate analytics READ ONLY thông thường,
không khẳng định không có query analytics; không dùng kết quả để SQL validation.
Sửa selector expander có icon và parser distance theo markdown node (tránh nối
chuỗi metric/label). Không sửa runtime app để vượt lỗi, không gửi lại request.

Lệnh startup thực chạy với cùng process flags1/1:

```powershell
$env:FARE_ADDRESS_ENABLED = '1'
$env:FARE_ADDRESS_ALLOW_NETWORK = '1'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start.ps1 -WithInference -ModelVersion v2.2
```

**Exit0**, reuse cả4 containers, IDs/StartedAt/images trước–sau không đổi.
Chỉ đọc schema/tables/rows-present và6/6 loaded metadata; không COUNT/full SQL
validation, worker/bootstrap/ETL không chạy.

### Services, tài nguyên, cleanup và rollback giữ lại

- Inference ID `24b4ef7ca0ed27947f1451e6aeac00d2eea8147e77a91d74a4c7a0beff162e51`,
  StartedAt `2026-10-28T07:15:43.547669823Z`; dashboard ID
  `49c3dc54a838f545b8100863eaae75c0da0518f2ee1449d8f9f2d67e9cf7df30`,
  StartedAt `2026-10-28T07:18:41.60525348Z`; cả hai healthy.
- PostgreSQL/MLflow giữ **ID/StartedAt/image/config hash/mounts/volumes** của
  checkpoint đầu lượt, healthy. Cả13 protected hashes khớp, gồm `.env`, PBIX,
  PACKAGE P3, base/inference Compose và hai manifests candidate/bundle.
- Context **4.056.173 bytes**, new image layers **5.714.429 bytes (~5,45 MiB)**
  so base V2, profile trước dọn **51.358.177 bytes (~48,98 MiB)**. Context và
  profile đã xóa đúng validated paths sau đóng Chrome; giữ recipe/checkpoints/
  metadata safe, cả images V1/V2 và bundle. Không prune hoặc xóa volumes.
- V2 runtime retained ở số đo cuối **188,28 MiB**. Tính cả old layer deltas
  7.554.400 bytes, new delta và scratch reserve512 MiB khoảng **712,94/2048 MiB**;
  metadata cuối tăng thêm vài KiB. Không cấp lại budget; không cộng full sizes
  của images có shared layers hoặc cam kết hard cap toàn C/VHDX/pagefile.
- C trống đầu/cuối **72,618/72,419 GiB**, RAM available **2352,71/1196,23 MiB**.
  Snapshots toàn máy, không quy toàn bộ chênh204,14 MiB cho tác vụ. Cgroup peaks
  inference **759,56 MiB**, dashboard **249,55 MiB**, OOM/oom_kill0. Không đo được
  tổng peak Chrome đa process; không dùng available RAM làm peak tác vụ.
- Rollback **chưa thực hiện** vì live/startup đạt. Giữ V1 images/full IDs và
  config checkpoint ở đầu attempt; khi cần được phép rollback riêng inference
  rồi dashboard bằng **hai layers V1**, flags1/1, từng lệnh
  `up -d --no-deps --no-build --pull never --wait --wait-timeout 180 <service>`.
  Không dùng V2 override cho rollback, không down/đụng data services.

Evidence ignored: `.sandbox/pretrip-v2/deploy-24fab1e-20261028-01/`
(`checkpoint.json`, expected hashes, recipe/build log, `live-evidence.json`,
`final.json`, safe phase/harness). Không lưu token/raw provider responses,
tọa độ hoặc user-query vào evidence; screenshots chỉ quan sát trong RAM.
Files dự án đổi trong attempt2: dashboard image pin trong V2 override,
README hunk attempt2 và report này. Inference pin đã đúng nên giữ nguyên.
Cả images V2 và bundle vẫn **local-only**, chưa publish/phân phối qua registry;
Git clone không tự mang các tài nguyên đó. **Chưa kiểm clone sạch trên máy khác.**
Tại checkpoint kiểm chứng này chưa stage/commit/push. **Operational live PASS không chứng nhận accuracy
cho chuyến hiện tại, không sửa nguyên nhân JVM/Docker cũ hoặc metrics August.**
