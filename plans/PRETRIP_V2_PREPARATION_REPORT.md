# PRETRIP V2 — acquisition và preparation thật

Ngày: 27/10/2026 (Asia/Saigon). Ngân sách người dùng duyệt cho toàn lượt:
**2048 MiB (2.147.483.648 bytes)**, dùng chung acquisition + preparation,
không cấp lại theo từng phase. Chưa training, predict/evaluate holdout hoặc deploy.

## Checkpoint thực thi

- Quota đã commit/push tại `45cd138b77480bafdcfe1ff41b140cb25a28669e`;
  không tạo commit trùng hoặc chạy lại 57 tests cũ.
- Runtime riêng `.sandbox/pretrip-v2/` ban đầu chưa tồn tại, không có job V2 cũ.
  Giữ mọi dữ liệu/attempt, không xóa hay overwrite tài nguyên có trước.
- Image sẵn có `nyc-taxi-dashboard:ml-p5-test`, ID
  `sha256:06af6f642a39ef68dc2e0db4e23d713800b9d8f2579f378602faec5c9fcddc7c`.
  Code/config/scripts được mount read-only; chỉ runtime V2 writable.
  Không build/pull/install; không mount `.env`, dữ liệu/model gốc hoặc toàn repository.
- Mỗi job tạm: 1 CPU, memory/swap limit 768 MiB, read-only root, tmpfs 32 MiB,
  log-driver none; preparation không có network. Không áp ngưỡng RAM host 2 GiB.
- Baseline C trống khoảng 79,713 GiB; RAM available 2,409 GiB. Đây là snapshots,
  không phải budget hay peak riêng của preparation.

## Acquisition — COMPLETE

CLI thực được gọi bằng runtime image trên:

```text
python scripts/acquire_pretrip_sources.py --execute --disk-budget-mib 2048
```

Ba GET, HTTP 200, không HEAD/redirect/retry; verified HTTPS, timeout 30 giây mỗi
thao tác, chunk tối đa 1 MiB. Ghi streaming thẳng vào filename cuối, không giữ
bản download trung gian. Content-Length và opaque ETag khớp snapshot đã pin.

| Yellow 2026 | Bytes | Footer rows | Row groups | SHA256 tự tính |
|---|---:|---:|---:|---|
| June | 65.465.637 | 3.837.248 | 4 | `f340c62a18885c56176433c9db28e97d0a8a14e6e49869276ad8afb20ebf088b` |
| July | 61.685.033 | 3.530.109 | 4 | `3260bb629c2f334055f8a566cbb02c15476b15120c45d7ad0aae46fc4bb06612` |
| August | 59.043.961 | 3.336.716 | 4 | `6683a025443afcbb28ac4f238fcbb6334767dc69fab4a16b1eb16a78dcd013cd` |

Tổng nguồn 186.194.631 bytes (177,569 MiB), 10.704.073 rows. Cả ba đủ bảy cột:
VendorID/PU/DO int32, pickup/dropoff timestamp[us] **không timezone**,
RatecodeID int64, fare_amount double. Footer/schema đã đọc trước preparation;
không coi row count footer là kiểm toàn bộ row-level data.

**SHA256 là checksum local tự tính từ stream, không phải checksum TLC công bố.**
ETag multipart chỉ là guard phiên bản khớp pin, không được gọi là SHA256 nguồn.
Manifest/checkpoint (metadata, không chứa trip rows) giữ tại
`.sandbox/pretrip-v2/sources/acquisition-33d464b0830d4fc9993f2b0bfdf4d069.{report,checkpoint}.json`.

Acquisition mất **17,178 giây**. Process peak RSS **78.176 KiB**; cgroup
memory.peak **52.748.288 bytes**, memory.events OOM/OOM-kill/max đều 0. RSS và
cgroup accounting có scope khác nhau, không cộng hai số này thành host peak.
Task files khi kết thúc phase 186.204.121 bytes; transient peak 186.208.868
bytes, đều dưới ngân sách chung 2048 MiB.

## Preparation — PREPARED_NOT_TRAINED

```text
python scripts/prepare_pretrip_pilot.py --execute --disk-budget-mib 2048
  --budget-input <acquisition checkpoint>
  --budget-input <acquisition report>
```

Hai metadata files acquisition được tính vào ledger cùng ba nguồn, không cấp
budget mới hay đếm trùng nguồn. Output mới:
`.sandbox/pretrip-v2/pilot-2026-06-08/`. Hash sampling 1%, seed 42; actual pickup
June/July/August là train/validation/holdout, caps 100k/50k/50k giữ nguyên.
Calendar/feature contract v2 và v1 model không thay đổi.

Job tạm `nyc-taxi-pretrip-preparation-20261027` đã exit 0 và tự xóa; checkpoint
cuối `PREPARED_NOT_TRAINED`, `source_snapshot_verified=true`. Không overwrite
attempt, đổi cohort/seed/caps hoặc chạy lại preparation để tạo số đẹp hơn.
August chỉ preparation/counts/kiểm cấu trúc, không predict hoặc metrics.

| Split theo actual pickup NYC | Eligible | Sample trước dedup | Duplicate loại | Sample cuối / cap |
|---|---:|---:|---:|---:|
| June train | 2.650.423 | 26.573 | 0 | 26.573 / 100.000 |
| July validation | 2.343.375 | 23.526 | 0 | 23.526 / 50.000 |
| August holdout | 2.185.069 | 21.833 | 0 | 21.833 / 50.000 |
| Tổng | 7.178.867 | 71.932 | 0 | 71.932 |

Đã quét đủ 10.704.073 rows, bằng tổng footer. Waterfall loại theo nguyên nhân
đầu tiên: 19 pickup ngoài pilot window; 50.973 fare không dương/không hữu hạn;
3.449.338 final RatecodeID khác 1; 24.876 zone không thuộc 1–263; 0 invalid
timestamp/vendor/completed-journey identity. Không có distance filter, không
cắt fare cực trị dương hoặc thay cohort để cải thiện kết quả.

Conflict retained sample: 0 (khác label cùng journey key sẽ làm job dừng).
Sample overlap train/validation, train/holdout, validation/holdout đều 0. Không
gọi đây là dedup/overlap audit toàn bộ eligible cohort 7,18 triệu rows.

Preparation mất **721,094 giây (12 phút 01 giây)**. Process peak RSS
**257.732 KiB (~251,69 MiB)**; cgroup memory.peak **237.715.456 bytes**;
memory.events max/OOM/OOM-kill đều 0. Không có Spark/JVM hoặc training.

## Kiểm chứng sample và dung lượng cuối

- Rehash ba nguồn: khớp acquisition SHA256 và preparation lineage. Rehash đủ
  sáu output Parquet: khớp `preparation_report.json`; builder/contract/calendar/
  TZif identity khớp runtime. Không sửa file nguồn/output trong phép kiểm.
- Tất cả 71.932 cặp rows kiểm bằng Arrow theo thứ tự: exact schema 8 features,
  labels riêng, keys hợp lệ/tăng nghiêm ngặt/không trùng, số dòng và split đúng,
  quyết định hash sample seed 42 đúng, journey-set hashes khớp. Tái tính độc lập
  sáu numeric time/calendar features: sai lệch lớn nhất 0; PU/DO là categorical
  strings hợp lệ. Không fit preprocessing/model hoặc thống kê target metrics.
- Đối chiếu raw giới hạn: bảy cột cần thiết, 8.192 rows đầu mỗi nguồn; tự dựng
  canonical journey key và so pickup/fare label/PU/DO với sample. Khớp 71 keys
  June, 85 July, 80 August (236 keys). Đây là raw-reference subset, không phải
  đối chiếu từng raw record của toàn cohort; toàn bộ stored sample pairs đã
  được kiểm schema/row alignment/time features/hashes ở bước trước.
- Harness kiểm byte ban đầu giả định snapshot `preparation_report.json` phải
  bằng checkpoint cuối nên assertion fail. Đã sửa **chỉ harness đọc** theo
  semantics đã ghi trong quota report: mỗi metadata document giữ snapshot
  thời điểm nó commit. Không đổi quota hay viết lại outputs. Report snapshot
  dùng 191.795.436 bytes; **checkpoint cuối khớp actual 191.797.209 bytes**.
  Lượt verification sau sửa PASS, 7,186 giây, process peak RSS 190.712 KiB;
  cgroup max/OOM/OOM-kill đều 0.
- Runtime có **13 files**, logical total **191.797.209 bytes (~182,91 MiB)**;
  gồm 3 nguồn, 6 sample Parquet, 4 metadata JSON. Quota peak (kể cả checkpoint
  cũ + scratch) **191.801.833 bytes**, còn **1.955.686.439 bytes (~1865,09 MiB)**
  trong ngân sách 2048 MiB. Không đếm trùng raw khi chuyển phase; metadata
  acquisition được giữ và tính như input read-only của preparation.
- Nguồn và samples giữ lại cho bước sau; không xóa runtime. Container tạm
  acquisition/preparation/verification đã tự xóa, không còn job V2 nền.
- C trống baseline **79,713 GiB**, cuối **79,320 GiB** (giảm toàn ổ khoảng
  402,28 MiB); không quy toàn bộ biến động C cho managed files 182,91 MiB.
  Không cam kết quota logical kiểm soát VHDX/pagefile/physical allocation.
  RAM available host cuối khoảng 2,834 GiB; không nhầm snapshots với job peak.
- PostgreSQL/MLflow/dashboard/inference đều healthy, ID/StartedAt giữ nguyên.
  15 hashes bảo vệ (gồm `.env`, PBIX, v1/model lock và các file P3) khớp baseline.
  Không mount real data/model, gọi model/Mapbox, ETL, DB hay thay services.

## Code mới và kiểm chứng trước download

- `ml/pretrip_acquisition.py`, `scripts/acquire_pretrip_sources.py`: PLAN mặc định,
  TLS/header/bytes/SHA/footer guards; giữ partial sau lỗi, chặn overwrite;
  reuse chỉ sau COMPLETE record đúng source version rồi rehash/footer lại.
- `pretrip_data.py`/`pretrip_pilot.py`: `--budget-input` read-only để tính metadata
  acquisition trong cùng budget; snapshot bất biến; nguồn dùng lại tính một lần.
- `test_pretrip_v2_acquisition.py` và tests budget-input liên quan, CI inventory.
  **31 tests PASS, 4,84 giây** trong container offline/source read-only/tmpfs,
  trước download. Một lỗi vòng snapshot mới bị tests phát hiện đã sửa trước
  thực thi; không thay quota/cohort để PASS. Không chạy toàn CI/57 tests cũ.

Checkpoint acquisition và hỗ trợ chỉ gồm code/tests/metadata/tài liệu;
không bao gồm sources, samples, model hay outputs/checkpoints runtime.
PRETRIP v2 chưa có metrics/model hoặc hỗ trợ inference hiện tại. Trước training:
cần triển khai trainer v2 riêng dùng đúng sample/contract/calendar/lineage đã
chốt, ghép cặp features/labels theo Arrow order đã xác minh trước khi chuyển
Spark, fit categorical preprocessing chỉ trên train với unknown bucket, baseline
train-only và validation gates theo thiết kế. Chưa được dùng August để tuning;
chỉ mở evaluation sau khi khóa ứng viên. Chưa có build/image training v2 hoặc
ngân sách/tài nguyên training được tự động duyệt bởi lượt acquisition này.
