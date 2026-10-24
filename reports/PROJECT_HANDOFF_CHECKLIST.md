# Checklist bàn giao — ML P6

Các mục ML P6 là mốc lịch sử; cập nhật PACKAGE P1/P2 ở cuối file thay thế trạng
thái model distribution tương ứng, không thay đổi bằng chứng E2E chưa có.

2026-10-24. Đây là kiểm kê kỹ thuật, không phải rubric chấm điểm: chưa tìm thấy
đề/rubric gốc của giảng viên trong repo. Các yêu cầu từng bước và đề xuất nâng cao
không tự động được xem là yêu cầu bắt buộc của thầy.

## Đã đạt, có bằng chứng

| Hạng mục | Bằng chứng và phạm vi |
|---|---|
| Pipeline dữ liệu Jan–Jun/2025 | Raw → Bronze → Silver → Gold → PostgreSQL: checkpoint 23.886.265 facts; dimensions 265/365/4.344, manifest và [load report](postgres_load_report.json). P6 không chạy lại ETL/count toàn fact. |
| Docker và cách ly bootstrap thử | Worker clean đã build; P6.3 fixture 30 raw → 20 facts, rerun skip/idempotency PASS; README Docker P6.2/P6.3. Không đồng nghĩa full bootstrap PASS. |
| Dashboard lịch sử | 4 KPI/3 charts, filters nhất quán, aggregate PostgreSQL READ ONLY; 23.886.265 toàn bộ, January 3.473.175, January+Manhattan 3.087.471; README P4/P4.1 và [P4 report](ML_P4_REPORT.md). |
| ML pilot và holdout | [P2.1](ML_P2_1_REPORT.md): train110.356/validation29.979; [P3](ML_P3_REPORT.md): June27.310, GBT MAE2,25014USD/RMSE3,52047/R²0,92063, baseline MAE7,90497USD. Cohort/sampling1% seed42 cố định; overlap được kiểm trên sample, không full cohort. |
| Frozen inference | [Model card](ML_P3_MODEL_CARD.md), [P4](ML_P4_REPORT.md)/[P5](ML_P5_REPORT.md): API8/8 + AppTest/API3/3 parity0USD, đúng preprocessing/model, no latest/fallback; demo lịch sử2025 theo distance, không routing/báo giá hiện tại. |
| Startup có inference | P5 hai lượt healthy reuse giữ IDs/StartedAt; artifacts/full hash/drift/ready guard. Stopped/missing/timeout kiểm bằng mock; không dừng services thật. |
| CI local | P5 Python380PASS/1SQLdeselected, web93PASS/1SQLdeselected, Pester204PASS, Compose14PASS/actionlintPASS; offline, không fitted artifacts/Spark/training mặc định. |
| CI remote phần ML | Commit **`afbf22c4886e10aaf6a81fe6221b2807154362c9`**, push `main`: [run40001163976](https://github.com/CungDoHaiPhong279/nyc-taxi-bigdata/actions/runs/40001163976) **SUCCESS**, cả3jobs Python/Pester/Compose+dashboard. Đây là bằng chứng remote, không suy từ local; commit cập nhật tài liệu được theo dõi CI riêng. |

## Chưa kiểm chứng

- Bootstrap tường minh đủ sáu tháng từ clone mới: chưa chạy end-to-end acquisition
  qua mạng → toàn ETL → nạp DB mới bằng script. P6.3 chỉ fixture nhỏ, không acquisition.
- Browser render thật của trang inference: đã AppTest/API/HTTP, chưa browser thật.
- Power BI: PBIX đã tồn tại và có thay đổi local chưa commit; không stage/sửa/đọc
  nội dung trong ML P6, chưa xác nhận render/refresh/nội dung bản đang sửa.
- Cold-cache/production load, nhiều người dùng và rare fare/geographic tails;
  chưa có latency/SLA bảo đảm. June đã đánh giá, không dùng lại để tuning.

## Chưa triển khai / giới hạn bàn giao

- **Model bundle trên máy mới:** chưa có nơi phân phối công khai/release được
  xác nhận. Model/contract/lock/zone CSV phải được người giữ MLflow/backup của đúng
  training `d2772f1904d7472490a73b0ac1c0c5d4` và evaluation
  `58d5008f0ed048f58599a859877ae336` cung cấp riêng. README ML P5 có bốn paths,
  full hashes và `scripts/model_artifacts.ps1`. Git clone không tự có model/data/
  dependency image; startup không train/export/bootstrap thay người dùng.
- Thêm kỳ dữ liệu mới chưa thành quy trình tự động đã kiểm chứng. Serving chỉ2025,
  Jul–Dec chỉ cảnh báo ngoại suy; cần nguồn/calendar/cohort/lineage/temporal split,
  kiểm định và model identity mới được duyệt, không ghi đè ứng viên hiện tại.
- Retraining/scheduling/model promotion tự động chưa có; không có nút training
  khi mở dashboard. Cần dữ liệu phát triển và independent holdout mới.
- Cache miss còn chậm: P4.1 SQL51,42s không lọc /33,83s January /24,78s
  January+Manhattan, cache hit nhanh không giải quyết cold/miss. Chưa có persisted
  aggregate/materialized view, không tự tạo index/schema hoặc thay DB để tối ưu.
- Chưa có artifact registry/distribution, public API authentication/TLS/rate
  limiting/monitoring hay chiến lược backup/restore đã diễn tập trên máy mới.

## Hạng mục nâng cao, cần chọn phạm vi riêng

- Required checks/branch protection và auto-merge PR sau CI; chưa triển khai,
  không phải một phần ngầm của commit/push ML P6.
- Thử full bootstrap trên môi trường thực sự tách biệt, tối ưu persisted aggregates,
  kiểm thử tải/browser và phát hành bundle có kiểm soát.
- Airflow/retraining/drift monitoring, mô hình định tuyến trước chuyến hoặc
  uncertainty calibration; không suy ra đây là yêu cầu bắt buộc nếu chưa có rubric.

## PACKAGE P1 — cập nhật sau ML P6

- Có entrypoint `scripts/setup_system.ps1` (PLAN mặc định, Execute tường minh),
  tái sử dụng bootstrap/start hiện có, checkpoint và guards cách ly; bundle local
  ml-p4-v1 kèm checksum/version lock. Không training/latest hoặc pipeline thứ hai.
- Môi trường trống `nyc-taxi-package-test` được chuẩn bị riêng cả config, ports,
  containers, volumes, networks và mọi writable runtime path; không dùng DB
  bootstrap-test đang giữ fixture. Mới kiểm tra mock/PLAN, chưa khởi động stack mới.
- **Blocker máy mới:** chưa có nguồn phân phối model/quyền phát hành được xác
  nhận. Chủ artifact phải cung cấp đúng bundle đã pin; chưa upload/publish.
- Full acquisition/bootstrap Jan–Jun từ clone mới và chạy lại vẫn **chưa E2E
  kiểm chứng**; không coi PLAN/mock hoặc fixture20facts là PASS dữ liệu thật.
- Một lệnh `start.ps1 -WithInference` mở lại máy đã có model/dữ liệu vẫn giữ
  nguyên; không đồng nghĩa một lệnh từ clone mới. Model June đã chốt, không tự
  retrain khi có dữ liệu mới. Các giới hạn cache miss/PBIX/CI remote nêu trên
  giữ nguyên; P1 chưa commit/push hoặc có lượt CI remote cho code mới.
- Chi tiết/lệnh kiểm chứng tiếp theo: [PACKAGE P1 report](PACKAGE_P1_REPORT.md).

## PACKAGE P2 — cập nhật model distribution

- Bundle cố định `distribution/models/ml-p4-v1/` nay là nguồn mặc định của setup;
  không cần chủ model export/download thủ công. Bundle đi cùng source; P2 ghi
  nhận trạng thái trước commit, trạng thái phát hành sau đó xem Git log/CI đúng SHA.
- Audit 43 files =40 model gốc+candidate lock+feature contract+zone lookup; cùng
  full model/ZIP SHA, không taxi observations/secrets. Manifest P2 chỉ đổi
  external distribution metadata. Provenance/attribution và điều khoản NYC được
  ghi rõ, không tự gán license hay đặt gate chủ model bên ngoài không có bằng chứng.
- Gate phân phối model chung ở P1 được thay thế; dự án vẫn chưa chọn license
  tái sử dụng tổng thể/model-specific. Bundle không được bake vào images.
- Full bootstrap/acquisition/rerun dữ liệu thật trên máy mới vẫn chưa E2E; model
  bundle test/PLAN không thay chứng cứ đó. Chi tiết [PACKAGE P2 report](PACKAGE_P2_REPORT.md).
