# Kế hoạch cải tiến: KPI nhanh ở lần đầu mở và kiểm chứng chế độ "Đặt lịch"

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Trang lịch sử hiện KPI dưới vài giây ngay lần đầu mở, và báo cáo có số liệu kiểm chứng cho cả hai chế độ gợi ý giờ đi.

**Architecture:** Task 1 chuyển phần gom nhóm `buckets` của truy vấn KPI (đang quét 23,9 triệu dòng `analytics.fact_trip` mỗi lần) vào một Materialized View `analytics.mv_kpi_buckets`, dựng cùng chỗ với hai view sẵn có trong `sql/postload/create_aggregates.sql`. Phần `GROUPING SETS` phía sau giữ nguyên, nên kết quả không đổi. Task 2 thêm `--mode scheduled` cho `scripts/eval_departure_advice.py`, kèm một test bảo đảm script chọn giờ giống hệt `_find_nearest_better_hour` của app.

**Tech Stack:** PostgreSQL 16, SQLAlchemy `text()`, pytest, numpy/pyarrow (script kiểm chứng).

**Spec:** Mục 1 và 2 trong đề xuất cải tiến ngày 2026-11-08 (KPI lần đầu 26–52 giây; chế độ "Đặt lịch" chưa kiểm chứng). Không có file spec riêng.

## Global Constraints

- Python 3.11; test chạy bằng `.\.venv\Scripts\python.exe` (Python hệ thống không có pytest).
- Không sửa file bị khóa hash (`.gitattributes`: `distribution/models/**`, `config/pretrip_v2_*.json`, `ml/pilot*.py`, `ml/pretrip_features.py`, `src/nyc_taxi/etl/silver.py`).
- Không thêm `skip`/`importorskip` vào test nằm trong `TEST_FILES` của `scripts/ci_python.py`; không thêm file test mới (dùng file sẵn có để khỏi sửa inventory).
- Bộ lọc hợp lệ phải trùng `VALID_FARE_SQL = "f.fare_amount > 0 AND f.fare_amount <= 500"` và `VALID_DISTANCE_SQL = "f.trip_distance > 0 AND f.trip_distance <= 100"`.
- Tổng số chuyến và `total_fare` vẫn tính trên mọi dòng (đối soát được với 23.886.265 dòng).
- Văn bản hiển thị và tài liệu bằng tiếng Việt; số viết kiểu Việt (`1,2`, `10.147`).
- Commit/push chỉ khi người dùng đồng ý.

## Review Focus

1. Postgres đã nạp trước thay đổi này, chưa có `mv_kpi_buckets`: dashboard phải báo rõ "chạy `load_postgres.py --create-indexes`", không phải câu chung "Không thể đọc PostgreSQL" (test ở Task 1, Step 1).
2. Chọn tháng không có dữ liệu (`2000-01`) hoặc borough không khớp: vẫn trả 0 chuyến, trung bình `None` như cũ (đã có trong test live, Task 1 Step 6 giữ nguyên các case).
3. Chuyến có `pickup_location_id` NULL hoặc ngoài `dim_location`: bị loại ở phép JOIN giống hệt trước (MV không lọc thêm gì; test Task 1 Step 1 kiểm MV không có `WHERE`).
4. Chế độ "Đặt lịch" lúc 0h–3h hoặc 20h–23h: không xét giờ ngoài 0..23 (test parity Task 2 Step 1 có giờ 1 và 22).
5. Hai giờ cùng điểm số: script và app phải chọn cùng một giờ (thứ tự offset `1, -1, 2, -2, …`; test parity Task 2 Step 1).

---

### Task 1: Materialized View cho KPI

**Files:**
- Modify: `sql/postload/create_aggregates.sql` (thêm view đầu file, trước `mv_multisource_summary`)
- Modify: `src/nyc_taxi/dashboard/queries.py:52-114` (`build_aggregate_query`), `:180-187` (`fetch_dashboard` xử lý lỗi)
- Modify: `scripts/load_postgres.py:190` (dòng log)
- Test: `tests/test_dashboard_queries.py` (sửa các test ở dòng 37-73, 101-122, 150-234)
- Docs: `README.md:15`, `reports/FINAL_REPORT_OUTLINE.md` (dòng có `26–52`), `CLAUDE.md` (câu "rebuilds both" → "rebuilds all three")

**Interfaces:**
- Produces: `analytics.mv_kpi_buckets(source_year, source_month, pickup_hour, pickup_location_id, trip_count, fare_sum, valid_fare_sum, valid_fare_count, valid_distance_sum, valid_distance_count)`, gom theo 4 cột đầu, không `WHERE`, không `HAVING`.
- `build_aggregate_query(filters) -> tuple[TextClause, dict]` giữ chữ ký; CTE `buckets` chỉ còn `SELECT <10 cột> FROM analytics.mv_kpi_buckets AS f {month_where}`.

- [ ] **Step 1: Sửa test cho dạng mới (sẽ fail)**

Trong `test_all_groupings_share_bound_filters_and_averages_skip_invalid_records`: thay dòng 45 bằng `assert "analytics.fact_trip" not in sql` và `assert sql.count("FROM analytics.mv_kpi_buckets AS f") == 1`; bỏ các assert dòng 51, 54, 55 khỏi SQL truy vấn và kiểm chúng trên khối MV:

```python
def _kpi_view_sql():
    summary = (Path(__file__).resolve().parents[1] / "sql" / "postload" / "create_aggregates.sql").read_text(encoding="utf-8")
    start = summary.index("CREATE MATERIALIZED VIEW analytics.mv_kpi_buckets AS")
    return summary[start:summary.index(";", start)]

def test_kpi_view_keeps_every_row_and_filters_only_averages():
    block = _kpi_view_sql()
    assert "FROM analytics.fact_trip AS f" in block
    assert "WHERE" not in block.replace("FILTER (WHERE ", "")
    assert "HAVING" not in block and "JOIN" not in block
    assert "SUM(f.fare_amount) AS fare_sum" in block
    assert f"FILTER (WHERE {queries.VALID_FARE_SQL}) AS valid_fare_sum" in block
    assert f"FILTER (WHERE {queries.VALID_DISTANCE_SQL}) AS valid_distance_sum" in block
    assert "GROUP BY f.source_year, f.source_month, f.pickup_hour, f.pickup_location_id" in block
```

Trong `test_fact_is_reduced_before_join_and_borough_filter`: bỏ assert dòng 71 (GROUP BY giờ nằm trong MV). Trong `test_readonly_timeout_and_disposal_are_enforced_before_aggregate` dòng 117: `"FROM analytics.mv_kpi_buckets" in ...`. Thêm test lỗi thiếu view, theo mẫu `test_statement_timeout_has_actionable_safe_message`:

```python
def test_missing_kpi_summary_tells_how_to_rebuild(monkeypatch):
    class UndefinedTable(Exception):
        sqlstate = "42P01"
    engine, connection, _ = fake_engine(monkeypatch)
    connection.execute.side_effect = ProgrammingError("", None, UndefinedTable("private"))
    with pytest.raises(queries.DashboardQueryError, match="--create-indexes"):
        queries.fetch_dashboard(queries.DashboardFilters())
    engine.dispose.assert_called_once()
```

(`ProgrammingError` nhập từ `sqlalchemy.exc` cạnh `OperationalError`.)

- [ ] **Step 2: Chạy, xác nhận fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_dashboard_queries.py -q -o addopts=`
Expected: FAIL ở các test vừa sửa (`mv_kpi_buckets` chưa có).

- [ ] **Step 3: Thêm view vào `create_aggregates.sql`**

Đặt đầu file, mẫu `DROP MATERIALIZED VIEW IF EXISTS` + `CREATE MATERIALIZED VIEW` như hai view sẵn có; thân lấy nguyên CTE `buckets` hiện tại (dòng 79-89 của `queries.py`) với bộ lọc viết thẳng giá trị. Comment một dòng: lý do (lần đầu 26–52 giây vì quét toàn bộ fact) và "bộ lọc trùng `VALID_*_SQL`".

- [ ] **Step 4: Đổi `build_aggregate_query` và lỗi `42P01`**

CTE `buckets` đọc 10 cột từ `analytics.mv_kpi_buckets AS f` (giữ alias `f` để `month_where` dùng tiếp `f.source_year`). Sửa comment dòng 71-75 cho đúng (không còn "partial/final parallel aggregation of the fact"). Trong `fetch_dashboard`, trước nhánh `57014`, thêm nhánh `sqlstate == "42P01"` với thông điệp:
`"Chưa có bảng tóm tắt KPI. Hãy chạy `python scripts/load_postgres.py --create-indexes` rồi tải lại trang."`
Sửa dòng log `load_postgres.py:190` thành `"analytics.mv_kpi_buckets, mv_multisource_summary and mv_route_hour_fare rebuilt"`.

- [ ] **Step 5: Chạy test, xác nhận pass**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_dashboard_queries.py tests/test_postgres_loader.py tests/test_dashboard_app.py -q -o addopts=`
Expected: PASS (1 skip là test live opt-in).

- [ ] **Step 6: Sửa test live `test_postgres_readonly_weighted_aggregates_against_row_reference`**

Test này thay `analytics.fact_trip` bằng `fact_fixture`; giờ truy vấn đọc MV. Thêm vào `fixtures` một CTE `kpi_fixture AS (<thân _kpi_view_sql() sau chữ "AS", với analytics.fact_trip → fact_fixture>)` và thay `analytics.mv_kpi_buckets` → `kpi_fixture`. Giữ nguyên 7 case và các phép so sánh. Test vẫn bị deselect trong CI; chỉ chạy được khi có Postgres:
Run: `$env:NYC_TAXI_DASHBOARD_SQL_TEST='1'; .\.venv\Scripts\python.exe -m pytest tests/test_dashboard_queries.py::test_postgres_readonly_weighted_aggregates_against_row_reference -q -o addopts=`
Expected: PASS (cần `docker compose up -d postgres` và `.env`; hỏi người dùng trước khi bật Docker).

- [ ] **Step 7: Dựng view trên Postgres thật và đo**

Hỏi người dùng trước (ghi vào DB, tốn vài phút). Chạy `python scripts/load_postgres.py --create-indexes`. Kiểm:
- `SELECT COUNT(*), SUM(trip_count) FROM analytics.mv_kpi_buckets;` → `SUM = 23886265`.
- Mở dashboard (http://127.0.0.1:8501), xóa cache Streamlit (menu ⋮ → Clear cache), đo thời gian KPI lần đầu với "tất cả tháng" và với 1 tháng + 1 borough (dòng "Aggregate SQL: … giây" trên trang, `dashboard/app.py:191`; ghi thêm thời gian tổng cảm nhận được vì dòng này chỉ đo câu SQL).
- So 3 KPI (số chuyến, cước TB `19,48`, quãng đường TB `3,39`) với trước khi đổi: phải giống hệt.

- [ ] **Step 8: Cập nhật tài liệu bằng số đo thật**

`README.md:15` và dòng tương ứng trong `reports/FINAL_REPORT_OUTLINE.md`: thay "1,2–4,7 giây khi đã có cache và 26–52 giây ở lần đầu" bằng số vừa đo, nêu số dòng của `mv_kpi_buckets`. Thêm một dòng vào bảng view ở `FINAL_REPORT_OUTLINE.md` (cạnh `mv_route_hour_fare`, dòng 53). `CLAUDE.md`: thêm `mv_kpi_buckets` vào câu về view do `--create-indexes` dựng lại.

- [ ] **Step 9: Chạy CI Python, commit (sau khi người dùng đồng ý)**

Run: `.\.venv\Scripts\python.exe scripts/ci_python.py` → Expected: tất cả pass, `1 deselected`.

```bash
git add sql/postload/create_aggregates.sql src/nyc_taxi/dashboard/queries.py scripts/load_postgres.py tests/test_dashboard_queries.py README.md reports/FINAL_REPORT_OUTLINE.md CLAUDE.md
git commit -m "perf(dashboard): read KPI buckets from a precomputed summary"
```

---

### Task 2: Kiểm chứng chế độ "Đặt lịch"

**Files:**
- Modify: `scripts/eval_departure_advice.py` (`choose`, `evaluate`, `main`)
- Test: `tests/test_ai_fare_advisor.py` (thêm 1 test)
- Docs: `README.md` mục 4.4, `reports/FINAL_REPORT_OUTLINE.md` mục 4.5

**Interfaces:**
- Consumes: `_find_nearest_better_hour` và `_historical_saving_usd` trong `src/nyc_taxi/dashboard/ai_fare_advisor.py` (không sửa).
- Produces: `choose(stats, model, pu, do, wk, hour, source, mode="now")`; `mode="scheduled"` xét offset theo đúng thứ tự `(1, -1, 2, -2, 3, -3, 4, -4)`, bỏ giờ ngoài 0..23. CLI thêm `--mode {now,scheduled}` (mặc định `now`). Không lọc "giờ đã qua" vì ngày kiểm chứng nằm trong quá khứ, coi như đặt trước.

- [ ] **Step 1: Viết test parity (sẽ fail)**

Nạp script bằng `importlib.util.spec_from_file_location("eval_departure_advice", ROOT / "scripts/eval_departure_advice.py")`. Dựng `stats` cỡ nhỏ: mảng `np.zeros((264, 264, 2, 24))` cho khóa `hn, hs, hq` (chỉ cần tiền tố `h`), điền một tuyến (pu=161, do=236, ngày thường) có giá trung bình theo giờ khác nhau rõ rệt (≥ 400 chuyến, phương sai 4) ở vài giờ. Với `hour` trong `(1, 12, 17, 22)`, so:

```python
picked = module.choose(stats, {}, 161, 236, 0, hour, "history", mode="scheduled")
option = _find_nearest_better_hour(
    hourly_total_curve=<lấy từ build_fare_explanation_and_advice với curve phẳng như _midday_advice>,
    current_hour=hour, ..., mode="scheduled",
    pickup_dt=datetime(2025, 6, 11, hour, 30, tzinfo=NYC_TZ),
    now_nyc=datetime(2025, 6, 1, tzinfo=NYC_TZ),   # ngày trước đó: không giờ nào bị coi là đã qua
    route_hour_history=<cùng số liệu, dạng {hour: _hour_stats(mean)}>,
)
assert (picked[1] if picked else None) == (option["hour"] if option["has_better_nearby"] else None)
if picked:
    assert picked[2] == pytest.approx(option["savings_usd"], abs=0.01)
```

Tên test: `test_backtest_script_picks_the_same_scheduled_hour_as_the_app`. Cách gọn nhất: gọi `build_fare_explanation_and_advice(..., mode="scheduled", now_nyc=...)` rồi đọc `["nearest_better_option"]`, khỏi tự dựng curve.

- [ ] **Step 2: Chạy, xác nhận fail**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_ai_fare_advisor.py::test_backtest_script_picks_the_same_scheduled_hour_as_the_app -q -o addopts=`
Expected: FAIL với `TypeError: choose() got an unexpected keyword argument 'mode'`.

- [ ] **Step 3: Thêm `mode` vào script**

`choose`: offset theo mode như Interfaces; điểm số dùng `abs(offset)` (`saving / (abs(offset) * 0.75 + 0.5)`), so điểm bằng `>` nghiêm ngặt để giờ xét trước thắng khi hòa (giống `max()` của app). `evaluate` nhận và truyền `mode`; `main` thêm `--mode`, in mode trong nhãn mỗi dòng kết quả. Sửa docstring đầu file thêm ví dụ `--mode scheduled`.

- [ ] **Step 4: Chạy test, xác nhận pass**

Run: `.\.venv\Scripts\python.exe -m pytest tests/test_ai_fare_advisor.py -q -o addopts=`
Expected: PASS.

- [ ] **Step 5: Chạy kiểm chứng thật**

Cần `data/gold/taxi_enriched/year=2025/month=01..06`. Chạy (nặng RAM vừa phải, không cần Docker), cho kết quả vào scratchpad:
```
.\.venv\Scripts\python.exe scripts/eval_departure_advice.py <out_dir> --holdout-month 6 --mode scheduled
.\.venv\Scripts\python.exe scripts/eval_departure_advice.py <out_dir> --holdout-month 5 --mode scheduled
.\.venv\Scripts\python.exe scripts/eval_departure_advice.py <out_dir> --holdout-month 6
```
Lệnh cuối (mode `now`) phải ra đúng số cũ: 300 tuyến, lịch sử, không phụ phí `n=1147 … cheaper=88.8%`. Lệch là script bị sửa sai.

- [ ] **Step 6: Đưa số vào tài liệu**

README mục 4.4 và outline mục 4.5: thêm cột hoặc bảng "Đặt lịch" (cùng 6 chỉ tiêu như bảng hiện có, chỉ dòng "Lịch sử tuyến") và một câu kết quả tháng 05. Bỏ cụm "và chỉ kiểm chứng chế độ \"Đi ngay\"". Nếu chế độ "Đặt lịch" kém rõ rệt (rẻ hơn thực sự < 80%), ghi trung thực vào phần giới hạn, không sửa ngưỡng để số đẹp hơn.

- [ ] **Step 7: Chạy CI Python, commit (sau khi người dùng đồng ý)**

Run: `.\.venv\Scripts\python.exe scripts/ci_python.py` → Expected: tất cả pass.

```bash
git add scripts/eval_departure_advice.py tests/test_ai_fare_advisor.py README.md reports/FINAL_REPORT_OUTLINE.md
git commit -m "test(advice): backtest scheduled departure advice"
```

---

## Ngoài phạm vi (ghi vào "Hướng phát triển" của báo cáo)

- Hiệu chỉnh mức tiết kiệm hứa (1,26 → 1,04 USD trên tháng 05).
- Đánh giá điều chỉnh quãng đường Mapbox (cần gửi dữ liệu ra Mapbox).
- Dựng lại Gold cho 20.246 chuyến thiếu thời tiết.
- Huấn luyện lại mô hình cho ratecode NULL / loại giá khác.
