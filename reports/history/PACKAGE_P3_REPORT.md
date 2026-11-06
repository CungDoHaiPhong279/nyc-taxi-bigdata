# PACKAGE P3 REPORT

Ngày chốt: 2026-10-25. Mốc code trước P3:
`0eaa3148dd6a40a42591eb74a109b1652381b5c9`.

## Kết quả đã kiểm chứng

Lệnh setup dùng dữ liệu thật Jan–Jun/2025 trong môi trường tách biệt:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_system.ps1 -Environment package-test -Execute
```

Sau hai lỗi được giữ nguyên checkpoint để sửa và resume, lượt
`setup-resume02` hoàn thành đủ 14 bước Calendar → acquisition → Bronze →
Silver → Gold → PostgreSQL → indexes/view → validation, exit `0` và báo
`READY`. Validation dùng SQL thực tế, không dùng `etl_load_log` thay cho COUNT:

- fact theo tháng 2025-01…06: 3.473.175 / 3.572.379 / 4.122.977 /
  3.935.784 / 4.527.573 / 4.254.377;
- tổng fact: **23.886.265**;
- dimensions location/calendar/weather: **265 / 365 / 4.344**;
- model phục vụ đúng SHA256
  `759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`.

Report SQL chi tiết từng query từng nằm trong runtime thử và đã được dọn theo
yêu cầu; transcript audit còn giữ xác nhận validation thực tế thành công và
trạng thái READY, nhưng không chứa lại toàn bộ raw query output.

## Sửa lỗi P3 được giữ lại

- Checkpoint atomic replace dùng `[NullString]::Value`; đây là lỗi binding
  `$null` thành chuỗi rỗng của Windows PowerShell 5.1, làm resume thất bại.
- Bootstrap package lưu một JSON diagnostics riêng cho từng stage, redact
  secrets, giới hạn mỗi stdout/stderr ở 1 MiB, không ghi đè attempt cũ và chặn
  traversal/reparse path. Chế độ non-package không tạo diagnostics này.

## Giới hạn và trạng thái sau kiểm tra

Lượt chạy lại toàn bộ cùng lệnh để chốt skip/idempotency **chưa hoàn tất**;
không có transcript rerun hoàn chỉnh nên không báo PASS. P3 cũng không phải
bằng chứng một lần chạy liên tục trên máy hoàn toàn mới: lần kiểm chứng đã dùng
checkpoint/resume sau khi sửa hai lỗi trên.

Containers, named volumes, networks, runtime `.sandbox/package-test` và các
image/cache riêng của P3 đã được dọn có chọn lọc. Audit bị Git ignore được giữ
tại `.sandbox/package-p3/20261024-full01/` và
`.sandbox/reclaim-p3/20261024-1451/`. Hệ thống thật, dữ liệu/model gốc, `.env`
và PBIX không thuộc phạm vi thay đổi code P3.
