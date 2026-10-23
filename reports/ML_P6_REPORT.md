# ML P6 REPORT — rà soát, Git và CI remote

2026-10-24. Không training/tuning/đánh giá lại June, ETL hoặc thay lifecycle services.

## 1. Phạm vi commit và phần để lại

Commit ML **`afbf22c4886e10aaf6a81fe6221b2807154362c9`**:
**58 files**, 9.947 insertions/35 deletions. Gồm pilot/cohort/dedup/sample/holdout,
inference + UI/CLI/startup, ba ML Compose overrides/hai Dockerfiles mới, tests và
CI inventories, parity fixture **tổng hợp**8inputs, thiết kế/reports/model card,
profile P1 **thống kê tổng hợp**36.272bytes và checklist bàn giao. Không Git-track
dữ liệu từng chuyến, model binaries, logs hay runtime JSON khác.

`.gitattributes` cố định LF cho tám frozen source/dependency inputs và profile P1:
Windows checkout trước đó có `core.autocrlf=true`, có thể làm hash lệch dù code
giữ nguyên ý nghĩa. Kiểm checkout-index vào thư mục thử riêng với autocrlf=true:
**9/9 exact hashes khớp lock/profile**, không đổi bytes nguồn/model lock. README
làm rõ trạng thái hiện tại và nguồn/paths/identity/hash/check artifacts trên máy mới.

Chỉ sửa thêm ba dòng trống EOF trong Dockerfile ML/ML Compose/pilot CLI để staged
`diff --check` đạt; không đổi logic/model. CLI AST và14Compose checks đúng2.40.0
PASS. Future ML job phải build code-refresh/hash theo wrapper sau các thay đổi
đóng gói, không mount source để che image stale; P6 không chạy/build job ML.

PBIX **`reports/NYC_Taxi_Dashboard.pbix`** giữ nguyên thay đổi local, không stage.
`.env`, dữ liệu, `.sandbox`, models/logs/MLflow store tiếp tục ngoài commit.
Commit tài liệu tiếp theo chỉ cập nhật README/checklist/report này để ghi bằng
chứng CI; không thay code đã đạt kiểm thử.

## 2. Branch, remote và push

- Branch **`main`**, origin **`https://github.com/CungDoHaiPhong279/nyc-taxi-bigdata.git`**;
  GitHub API xác nhận đúng repo public/không archived, `main` không protected.
- Không tìm thấy AGENTS.md hoặc hướng dẫn cấm push nhánh này. Remote trước push
  khớp local base `fb0afae823697492360447f3e264b6f1e74d58c0`.
- `git push origin HEAD:refs/heads/main`: **thành công**, không force/merge/reset/
  stash/clean hay sửa branch protection. Stage đúng danh sách58paths, rà staged
  diff; sau commit chỉ PBIX modified.

## 3. CI remote và kiểm chứng

Run đúng code SHA trên: **[40001163976](https://github.com/CungDoHaiPhong279/nyc-taxi-bigdata/actions/runs/40001163976)**,
event `push`, branch `main`, status **completed/success** từ GitHub Actions API.

| Job | Kết quả thực tế |
|---|---|
| [Python offline unit tests](https://github.com/CungDoHaiPhong279/nyc-taxi-bigdata/actions/runs/40001163976/job/120577300514) | SUCCESS |
| [PowerShell5.1 mock tests](https://github.com/CungDoHaiPhong279/nyc-taxi-bigdata/actions/runs/40001163976/job/120577301025) | SUCCESS |
| [Compose validation and dashboard packaging](https://github.com/CungDoHaiPhong279/nyc-taxi-bigdata/actions/runs/40001163976/job/120577301075) | SUCCESS |

Không lỗi ML/CI cần nới test/bỏ check. Reuse P5 local Python380/web93/Pester204;
P6 chạy lại actionlint, parser17files, inventory,14Compose checks, CLI AST,
staged whitespace/content/size/secret-pattern checks và Windows checkout hashes.
Heuristic scan không thấy secret thực; ba credential-URI matches được đối chiếu
là negative-test fixtures tổng hợp, không in userinfo/secret. Không coi heuristic
là bảo đảm tuyệt đối. Docker ignore vẫn loại env/data/models/reports/PBIX.

Báo cáo này được commit sau run code ở trên: **không tự nhận run đó là CI của
commit tài liệu mới**. HEAD/SHA/run cuối được theo dõi riêng và ghi trong kết quả
bàn giao ML P6; không cần chạy lại ML để xác nhận docs.

## 4. Checklist và giới hạn

[PROJECT_HANDOFF_CHECKLIST.md](PROJECT_HANDOFF_CHECKLIST.md) tách bốn nhóm: đã đạt
có bằng chứng; chưa kiểm chứng; chưa triển khai; nâng cao. Bao phủ pipeline,
analytics/ML/inference/startup/CI, bundle máy mới, bootstrap đầy đủ, cache miss,
kỳ dữ liệu mới, auto-retraining và PBIX. Không tìm thấy rubric gốc giảng viên,
không biến đề xuất nâng cao/auto-merge thành yêu cầu bắt buộc.

**Máy mới chưa clone-là-chạy:** chưa xác nhận release/URL phân phối bundle;
chủ training/evaluation runs phải cung cấp model/contract/lock/zone CSV đúng hash,
dependency runtime và dữ liệu analytics riêng. Full acquisition+bootstrap sáu
tháng từ clone chưa E2E; fixture P6.3 không thay bằng chứng đó. Browser inference,
production load, kỳ mới/retraining tự động và PBIX đang sửa chưa được xác nhận.
Cache miss vẫn hàng chục giây theo P4.1; persisted aggregates chưa triển khai.

## 5. Bảo toàn

Đối chiếu checkpoint trước/sau push: **4 services** healthy giữ nguyên IDs/StartedAt/
images/mounts; PostgreSQL `930e1ad167c9`, MLflow `6db6f54eda60`, dashboard
`a5ebee1e5153`, inference `a9b1b04d73e6`. Hai volumes vẫn
`nyc-taxi-bigdata_postgres-data` / `nyc-taxi-bigdata_mlflow-data`.
**302 file hashes không đổi** gồm `.env`, PBIX, bốn manifests/Gold Parquet và
model/P2.1/P3/P4 outputs. `model_artifacts.ps1` readonly PASS full frozen hash.
Không ghi DB/Gold/artifacts hoặc khởi động/recreate service/worker; CI chạy trên
GitHub-hosted runners, không truy cập hệ thống local. Checkpoints/logs kiểm chứng
ở `.sandbox/ml-p6/20261024-handoff01/`, không đưa lên Git.
