# PACKAGE V2.2 — portable frozen model bundle

Base: `886de447462cc923fcf8d453ce0e6591c78dd817`. Resume từ
`.sandbox/pretrip-v2/package-v22-20261028-01/`, không ghi đè bốn checkpoints/
scripts debug cũ. Job Pester cũ đã kết thúc; không có container thử cũ cần thu
kết quả. Installer và tests mới thực sự còn trong working tree, không chỉ có
debug files. **Kết luận: bundle portable PASS; full fresh-machine E2E chưa kiểm.**

## Source, payload và ngoại lệ đã duyệt

Source là serving bundle đã khóa; model tree
`27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3`, artifact
manifest `8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75`.
Không sửa candidate, fitted dictionaries, contract/calendar hoặc các cờ lịch sử.

| Nhóm payload | Files | Bytes |
|---|---:|---:|
| Frozen Spark pipeline | 36 | 55.822 |
| Frozen JSON metadata/reference | 9 | 387.561 |
| Original artifact manifest | 1 | 9.575 |
| Public TLC zone lookup | 1 | 12.331 |
| Tổng | **47** | **465.289** |

Source có48 files/468.062 bytes. File ngoài cùng `serving_manifest.json`,
2.773 bytes, là bookkeeping của bước preparation; code installer/startup guard/
inference không yêu cầu nó. Bản cài sạch47 files đã qua chính full artifact
guard khi không có file này. Bản legacy48 chỉ được reuse read-only nếu file
phụ có đúng hash trong distribution lock. Không chạy lại preparation CLI cũ.

User đã cho phép ngoại lệ duy nhất **16 records `parity_inputs.json` =8 train
+8 validation**, chỉ tám features và `parity_id`, **lấy từ dữ liệu, không phải
synthetic**. SHA256 của file giữ nguyên:
`0c33848a48ad320000cddce98d3bcd3c15393bf2a2dd0a48692025de28010b15`.
Schema allowlist loại fare labels từng chuyến, journey keys, địa chỉ/tọa độ và
exact pickup timestamps. Reference files chứa prediction đã lưu, không phải
nhãn thực tế. Không kèm raw/sample datasets hoặc August rows. Có lineage hashes
lịch sử, không mở dữ liệu nguồn/holdout để đóng gói.

Parquet footer audit đã có: hai one-row StringIndexer dictionaries,616 GBT
nodes và20 tree-metadata rows; không phải trip schema. Baselines chứa fitted
aggregate medians (1.402 pair groups/1.601 time groups, mỗi nhóm>=5), không phải
danh sách nhãn từng chuyến. Text audit không phát hiện credential patterns hoặc
absolute author/profile paths. Provenance giữ project-origin weights và thông
báo nguồn TLC lookup từ v1; không tự tạo license hoặc yêu cầu chủ model duyệt.

## Distribution và installer

| File | Bytes | SHA256 |
|---|---:|---|
| `nyc-taxi-model-pretrip-v2.2-native.zip` | **92.059** | `ae809b654b931a717b5f4f9646257e6163ce4913b87be7e9d275e1612a3ce80b` |
| `nyc-taxi-model-pretrip-v2.2-native.manifest.json` | **11.844** | `0c243ff50a8a2eec0c3876128d68b9f38198b6bef8e1a1ddb5334c6e04cfeef0` |

Thư mục distribution còn có checksum list, `PROVENANCE.md`, `CONTENT_AUDIT.json`.
Manifest pin từng path/size/SHA, toàn bộ model/artifact/dictionary/sample/
contract/calendar/TZif/zones identities và GBT metadata/parameters; distribution
lock pin độc lập ZIP/manifest. ZIP chứa byte-identical source members, sorted
paths, timestamp ZIP cố định; không đổi source mtimes để tạo archive.

`scripts/install_pretrip_v22_bundle.ps1` là PowerShell5.1, PLAN mặc định có
xác minh thật nhưng không ghi file; chỉ `-Execute` cài vào runtime V2 hiện có.
Không cần host Python/Java, candidate, Docker hoặc network. Reuse v1 bounded
ZIP/path/hash utilities và **giữ nguyên** V2 artifact guard. Chặn unsafe paths,
case/file-directory collisions, ZIP links/special entries, filesystem reparse;
caps archive2MiB, expanded2MiB, file512KiB, manifest128KiB, tối đa64 members.
Staging riêng, full guard rồi atomic publish; output khác/incomplete/foreign
dừng không overwrite/xóa. Existing đúng được reuse mà không đổi file/directory
mtime. Stage fail giữ bằng chứng; chỉ dọn own known scaffolding rỗng sau success.

## Sáu lỗi cũ và tests sau sửa cuối

Không tìm thấy log test cũ trên disk; dùng transcript đã thu (25/31, sáu lỗi)
và repro script cũ. Repro chứng minh directory mtime của fixture đổi giữa hai
snapshot **trước PLAN**, nhưng sau PLAN không đổi. Harness warm-up metadata,
đòi hai baseline giống nhau trước gọi app, vẫn giữ đầy đủ hash/file+dir mtime
assertions; evidence không ghi vào cây đang kiểm.

Năm lỗi sau là Pester4 mock scope: tiny quota spec còn sống trong Describe và
bị dùng lại. Mỗi case nay lấy object mới từ original factory, reset sau case;
không sửa/nới installer quota để PASS. Installer implementation không đổi so
bản trước limit.

- Sáu ca lỗi trước: **6/6 PASS**,17,83 s.
- Sau đó toàn bộ tests mới: **31/31 PASS**,29,62 s;0 skip/pending.
- Runtime: Windows PowerShell `5.1.26100.9711`, Pester `4.10.1` cache sẵn;
  không download/install hoặc chạy suites/CI/autocomplete cũ.
- CI Pester tự discover `*.Tests.ps1` và đối chiếu inventory cố định; checkpoint commit bổ sung `pretrip_v22_distribution.Tests.ps1` vào `scripts/ci_pester.ps1`. Chỉ kiểm parser/inventory cho thay đổi này, dùng lại bằng chứng 31/31 ở trên; không chạy lại suites.

## Bản sao sạch và reuse thật

`.sandbox/pv22c-01/` dùng255 tracked files từ Git base (archive LF), loại `.env`,
data/models/logs, PBIX và runtime; overlay chỉ code/tests/config/distribution
PACKAGE V2. Không copy PACKAGE P3 dirty; ba file nhạy phạm vi kiểm lại đúng blob
HEAD. Chạy từ TEMP ngoài cả hai project roots, không truyền author source path:

1. CLI PLAN: verified, writes=false; toàn cây file/hash/mtime và việc chưa có
   runtime `.sandbox` giữ nguyên.
2. CLI `-Execute`: cài thật đúng47 files/465.289 bytes; full unchanged
   `Assert-PretripV22Artifacts` PASS, model hash khớp; không load model.
3. Rerun `-Execute`: `VERIFIED_REUSED`, writes=false; mọi hashes và file/directory
   mtimes giữ nguyên, staging scaffolding đã dọn. Tổng chuỗi kiểm34,51 s.

Không có candidate, `.env`, top-level data/models hoặc outer serving manifest
trong bản kiểm. Source project chỉ chạy PLAN read-only để xác nhận legacy48
được nhận diện; không Execute vào production. Model source48 files có bytes/hash
khớp checkpoint. Mtime tick số trong checkpoint cũ đã qua JS serialization nên
không chính xác từng100ns; không dùng chúng để tuyên bố historical mtime equality.
Kiểm reuse/PLAN mới dùng tick strings chính xác, không ghi đè checkpoint cũ.
Sau chuẩn hóa distribution lock sang LF như Git checkout, đã kiểm thêm rerun
thực: vẫn `VERIFIED_REUSED`, hashes/mtime output không đổi; không chạy lại suite.

## Files và phạm vi còn lại

- Thêm `scripts/install_pretrip_v22_bundle.ps1`,
  `tests/pretrip_v22_distribution.Tests.ps1`,
  `config/pretrip_v22_distribution.lock.json` và5 distribution files.
- `.gitattributes` chỉ thêm rules đúng bundle V2/new lock; không cần sửa ignore.
- README chỉ thêm mục PACKAGE V2.2; report này mới. P3/PBIX và các dirty files
  ngoài phạm vi được bảo toàn. Tại checkpoint kiểm bundle chưa stage/commit/push; lượt chốt sau đó chỉ đưa đúng phạm vi PACKAGE V2.2 vào Git.

Tài nguyên tính cả runtime V2 cũ, clean copy, distribution/code mới,13.268.829
bytes image-layer deltas có trước và scratch reserve512MiB: khoảng **721/2048
MiB**, không cấp lại budget. Đây là task logical-byte accounting, không phải
hard cap của C/VHDX/pagefile. Không build/pull image, dùng model hoặc gọi API.
Bản sao sạch và evidence mới được giữ để bàn giao; không dọn tài nguyên cũ.
Số đo cuối:720,23MiB accounted; C trống72,37→72,96GiB, RAM available2309,72→
1938,89MiB (snapshots toàn máy, không phải peak hoặc chênh do tác vụ đã chứng minh).
Bốn services vẫn healthy, giữ ID/image/StartedAt;14 protected hashes khớp,
gồm PBIX, `.env`, PACKAGE P3, v1 helpers và frozen V2 guards.48 source hashes
khớp. Không còn job kiểm chứng; không tạo container thử trong bước này.

**Chưa kiểm** image build, full bootstrap, startup/inference trên máy khác hoặc
fresh-machine E2E; full setup V2 vẫn chưa được mở khóa. Bản kiểm dùng snapshot
Git+changes hiện tại, chưa phải clone origin sau commit vì files mới chưa push.
Windows PS5 test đã qua staged path tối đa243 ký tự; clone path sâu hơn/MAX_PATH
chưa kiểm. Mở lại deployment hiện tại vẫn theo lệnh explicit V2 trong README;
installer này chỉ chuẩn bị model files, không bật services hoặc fallback.
