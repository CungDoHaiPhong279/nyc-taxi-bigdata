# PRETRIP V2 — offline preparation checkpoint

Ngày: 27/10/2026 (Asia/Saigon). Triển khai theo [thiết kế đã chốt](ML_PRETRIP_PLAN.md).
**Chưa training, chưa có metrics/model v2, chưa hỗ trợ inference hiện tại.**
Không thay v1, dashboard, `/predict`, ETL, Gold, PostgreSQL hoặc model bundle.

## Code đã có

- `ml/pretrip_features.py`: contract `pretrip-v2-zone-time-1`, cùng implementation
  train/serve. Hai zone là string categorical; local hour/day-of-week sin/cos,
  weekend và holiday là sáu numeric features. Không đọc recorded distance hay
  đưa label, phí, payment, actual dropoff/duration vào vector.
- `config/pretrip_v2_contract.json` pin feature allowlist, clock policy, calendar
  và pilot policy. Seed/split/fraction/caps bị sửa ngoài policy thì fail rõ ràng.
  Valid zone 1–263 chưa thấy trong train vẫn giữ category; IDs 264/265/sai kiểu
  bị từ chối. Fitted unknown-category preprocessing chưa tồn tại ở bước này.
- `config/pretrip_v2_calendar.json`: nationwide US federal **observed** holidays
  từ [OPM](https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/),
  coverage 2025–2027, 34 ngày, gồm 31/12/2027 observed New Year 2028; không gồm
  Inauguration Day chỉ dành cho vùng DC. Đây không phải mọi ngày lễ/sự kiện NYC.
  Version `us-opm-nationwide-observed-2025-2027-v1`; SHA256:
  `00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac`.
  Ngoài coverage hoặc checksum sai: từ chối, không mặc định holiday=False.
- Snapshot “Đi ngay” lấy clock backend một lần tại submit, chuyển sang
  `America/New_York`, lưu UUID/instant/offset/fold/calendar/TZif identity để caller
  giữ qua rerun. Hẹn giờ tối đa 7 ngày, past tolerance 60 giây; DST gap bị từ
  chối, giờ lặp phải chọn fold. TLC NTZ vẫn là local wall time; không tự gán
  UTC/fold cho lịch sử hoặc ánh xạ ngày hiện tại về 2025.
- `ml/pretrip_data.py`: PyArrow batch 8.192 rows, bảy cột cần thiết, không pandas
  toàn bảng/Spark/shuffle. Completed fare dương hữu hạn + final RatecodeID=1 +
  named zones + timestamps/vendor hợp lệ là cohort hồi cứu. Fare cực trị dương
  giữ nguyên; không có distance filter hay predictor. Final ratecode chỉ là
  cohort, không chứng minh một request mới chắc chắn thuộc standard-rate.
- Stable journey key SHA256 từ VendorID, pickup/dropoff local và PU/DO; dropoff
  chỉ phục vụ identity offline. Cùng key có fare hợp lệ khác nhau thì dừng;
  key không thể phân biệt hai chuyến trùng toàn bộ các trường đó. Hash sample
  1%, seed 42 **trước dedup**; split theo actual pickup: June train, July
  validation, August holdout, half-open. Caps 100k/50k/50k; vượt cap hoặc split
  rỗng thì dừng, không truncate/đổi seed. Dedup/overlap chỉ xác minh trên sample,
  không tuyên bố audit toàn cohort. Holdout chỉ chuẩn bị/counts, không predict.
- Outputs riêng `.sandbox/pretrip-v2/<attempt>/`: features và labels/identity
  thành hai Parquet riêng, cùng thứ tự journey key; hashes, counts, source-file
  SHA256, feature-builder/contract/calendar/TZif identity và checkpoint aggregate.
  Không ghi đè attempt có sẵn; lỗi giữ FAILED checkpoint/partial outputs.
  Nguồn local được checksum riêng một lượt đọc; kiểm size/mtime/inode xuyên
  checksum/decode để chặn nguồn đổi trong khi chuẩn bị.
- CLI `scripts/prepare_pretrip_pilot.py`: PLAN mặc định, không tạo thư mục,
  không mở Parquet, không HTTP. `--head` là lựa chọn riêng, chỉ HEAD có timeout,
  không GET/body/redirect/retry. Không có chức năng download. Execute yêu cầu
  ba file local đúng size snapshot và disk-budget được cấp riêng; output guard
  chặn đường ngoài runtime, symlink/junction và overwrite.

Chạy PLAN bằng runtime Python sẵn có (không thực thi preparation):

```powershell
python .\scripts\prepare_pretrip_pilot.py
```

Lệnh dự kiến **sau khi được duyệt acquisition/tài nguyên và đã có nguồn local**;
chưa chạy trên dữ liệu thật trong lượt này:

```powershell
python .\scripts\prepare_pretrip_pilot.py --execute --disk-budget-mib 512 `
  --source-dir .sandbox/pretrip-v2/sources `
  --output-dir .sandbox/pretrip-v2/pilot-2026-06-08
```

Đường dẫn tương đối được tính theo project root, không theo cwd. Execute chỉ
chuẩn bị sample, không training/ETL/DB. Số 512 là dự trù phải phê duyệt, không phải
ngân sách tự cấp. Chưa thêm Compose job/image mới chứa code v2. Không chạy lệnh
trên bằng worker cũ thiếu code. Thực tế ở lượt này dùng image sẵn có với code
mount read-only, không tuyên bố code v2 đã được bake vào image.

## Metadata nguồn — đã đo HEAD, không đọc Parquet

Nguồn [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page);
URL cố định và headers trong `config/pretrip_v2_sources.json`.

| Nguồn Yellow 2026 | HTTP | Content-Length (bytes) | MiB |
|---|---:|---:|---:|
| June | 200 | 65.465.637 | 62,433 |
| July | 200 | 61.685.033 | 58,827 |
| August | 200 | 59.043.961 | 56,309 |
| Tổng | | 186.194.631 | 177,569 |

Ba HEAD đầu qua host Python trả URLError, không giữ nguyên nhân bên trong nên
không kết luận lỗi TLS/mạng cụ thể. Ba HEAD bằng Windows HTTPS native sau đó
thành công. Tổng **6 HEAD attempts, 0 GET, 0 Parquet body bytes**, không tải nội
dung/file footer. Content-Length/ETag không phải content checksum; chưa xác minh
schema, row count hoặc tính đầy đủ dữ liệu nguồn. CLI chỉ chấp nhận đúng ba URL
public đã pin; file local tương lai phải được hash trong preparation.

## Peak tối giản — đo riêng với ước tính

| Thành phần | MiB | Cơ sở |
|---|---:|---|
| Ba nguồn nén local | 177,569 | Content-Length đã đo |
| Sample features + labels | 64 | Dự phòng, chưa đo dữ liệu thật |
| Temporary output reserve | 64 | Dự phòng, không tạo thêm bản ETL |
| Metadata/checkpoint reserve | 8 | Dự phòng |
| Tổng thành phần | 313,569 | Ước tính preparation, không phải peak đo |

Đề xuất envelope riêng **512 MiB disk, 1 CPU, RAM 384–768 MiB ước tính** cho
preparation bằng runtime đã có; chưa cấp hoặc tiêu dùng envelope này. Không có
Bronze/Silver/Gold copies, Spark shuffle hay model training. Ước tính 10–25 GiB
trong thiết kế ban đầu dành cho đường nhiều tầng, không áp dụng nguyên xi cho
đường đọc trực tiếp tối giản này. Chưa đo selected-column compression, Arrow
decode/native RSS, số sample/conflicts thực, filesystem/VHDX physical growth,
image/build/download staging hoặc mọi chi phí training. Không cam kết thời gian
quét ba file thực từ thời gian unit tests.

## Kiểm chứng thực tế

- **112 tests PASS, 5,52 giây** sau sửa cuối: ba file
  `test_pretrip_v2_{features,data,pilot}.py` và đúng một inventory test
  `test_ci_python.py::test_inventory_accepts_the_complete_classified_suite`.
  Bao gồm parity train/serve, allowlist/leakage, DST/clock/calendar/tamper,
  unknown zones, stable hash sampling, duplicate/conflict/split/caps, projection,
  nguồn đổi giữa checksum/decode, guards/PLAN/HEAD mock và failure checkpoint.
- Image sẵn có `nyc-taxi-dashboard:ml-p5-test`, ID
  `sha256:06af6f642a39ef68dc2e0db4e23d713800b9d8f2579f378602faec5c9fcddc7c`;
  container tạm `--pull never --network none --read-only --cpus 1 --memory 768m
  --memory-swap 768m`, timeout 180s, source/config/tests/scripts read-only,
  synthetic Parquet trong tmpfs 128 MiB. Không mount dữ liệu/model/.env.
  Container đã tự xóa; không cài/build hay chạy toàn CI.
- PLAN chạy lại sau sửa cuối từ cwd `/tmp`, network none, cùng image + source
  read-only: `PLAN_NOT_EXECUTED`, ba nguồn local chưa có, `budget_granted=false`.
  Không tạo runtime output. CI inventory thêm ba test files, không thêm job hay
  dependency; không có remote CI mới vì chưa commit/push.
- TZif thực trong runtime: `system-zoneinfo`, SHA256
  `e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95`.
  Không suy ra release tzdb từ Python; artifact v2 tương lai phải pin bytes này.

## Blockers trước pilot / serving

1. Cần duyệt ngân sách/acquisition riêng; chưa tải ba file. Đo footer/schema/
   row counts, pin SHA256 local và xác minh source snapshot trước preparation.
2. Chưa biết sample counts, key collisions/conflicting fares/caps và peak RSS
   trên nguồn thật. Chưa có preparation thật, training, baseline hay metrics.
3. Trainer tương lai phải đọc cặp features/labels với hash + row alignment
   đã kiểm, ghép cùng thứ tự trước Spark; không đọc hai Spark DataFrame riêng
   rồi giả định chúng giữ thứ tự. Không đưa label/key/time identity vào vector.
4. Chưa fit categorical dictionaries/unknown bucket; chỉ fit trên train. Không
   dùng holdout để tuning, không quảng bá temporal/current accuracy từ v1.
5. Chưa build/tag v2, nối dashboard/inference hoặc promote artifact. Route-aware
   vẫn cần paired route/label hợp pháp; không dùng TLC recorded distance hoặc
   tâm zone để giả dữ liệu route. Không cấp quyền hiện tại cho model 2025.

Các thay đổi PACKAGE P3/PBIX có trước được giữ nguyên, ngoài phạm vi v2. Không
stage/commit/push, gọi model/Mapbox, training, ETL hoặc thay vòng đời services.

Đối chiếu cuối lượt: dashboard/inference/PostgreSQL/MLflow đều running/healthy,
ID và StartedAt không đổi. Hash sáu file v1/ETL/model lock được bảo vệ khớp
baseline; `.env` khớp checkpoint, index trống. `git diff --check` và kiểm whitespace
các file mới đạt. Không còn container test/PLAN tạm. Code/config/test/report mới
khoảng 101 KiB; không có source/model downloads hoặc image layers mới.

C trống đo trước 87.195.643.904 bytes và sau 87.074.672.640 bytes (khoảng
81,208 → 81,095 GiB); chênh toàn ổ không được quy toàn bộ cho lượt này. RAM
available trước 2.901.588 KiB, sau 2.722.160 KiB; đây là snapshots host, không
phải peak RSS preparation. Không tự tăng/cấp lại ngân sách chuyển tiếp.
