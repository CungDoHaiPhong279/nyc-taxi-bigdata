# NYC yellow taxi — kế hoạch chuyển sang ước tính trước chuyến

Ngày: 26/10/2026. **Chỉ là thiết kế; chưa đổi model, date guards, pipeline hay triển khai.**
UI offline được chỉnh riêng trong lượt này không làm model hiện tại hỗ trợ ngày hôm nay.

## 1. Phạm vi và quyết định đề xuất

- Ước tính **fare_amount — cước cơ bản USD** cho yellow taxi standard-metered tại NYC;
  không phải total_amount, báo giá được bảo đảm hoặc giá đồng hồ cuối chuyến.
- Luồng chính: địa chỉ → tọa độ retrieve → Taxi Zones → tuyến ô tô → model đã đánh giá.
  Giữ kiểm vùng ngay sau lựa chọn và gate standard-rate lần cuối; không tự gán zone gần nhất.
- Model v1 vẫn là demo 2025: training run `d2772f1904d7472490a73b0ac1c0c5d4`,
  evaluation run `58d5008f0ed048f58599a859877ae336`, SHA256
  `759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee`.
  Không nới năm rồi gọi model này là model hiện tại; không ánh xạ ngày 2026 về 2025.
- June 2025 đã được đánh giá, không còn là test độc lập cho tuning mới.
  MAE June 2025 không chứng minh sai số hiện tại, không tạo khoảng dự đoán ±MAE.
- Weather chưa thuộc phạm vi: chỉ xét sau với dữ liệu dự báo thực sự sẵn có trước chuyến,
  thí nghiệm có đối chứng; không dùng quan trắc/reanalysis tương lai làm feature.

## 2. Hai luồng, chung artifact có phiên bản

**Training/evaluation offline:** snapshot các chuyến đã hoàn thành → cohort/dedup/split
theo thời gian → fit preprocessing trên train → chọn bằng validation → đánh giá holdout
đúng một lần → bundle ứng viên. Bundle gồm fitted preprocessing/model, baseline, feature
contract, calendar/version/timezone, zone/geometry identity, training data lineage,
metrics/slices, input domain, cutoff và hashes từng file/tree. Pin version cụ thể,
không dùng `latest`; v1 và v2 nằm riêng, không ghi đè artifact gốc.

**Inference online:** nhận yêu cầu mới chưa có nhãn → validate domain/calendar → cùng
feature derivation đã version → load fitted bundle một lần/process → predict. Không fit,
tự đổi model, ETL hoặc gọi MLflow SQLite trong request. Train/eval và serving dùng đúng
một feature implementation v2 cùng contract/hashes; parity test trên inputs tổng hợp
trước rollout. Chuyển bundle phải có review, targeted deployment và rollback riêng.

Yêu cầu ước tính mới **không phải dữ liệu để retrain**: chưa có fare cuối chuyến.
Chỉ dữ liệu completed có nhãn hợp lệ và quyền sử dụng mới được đưa vào đánh giá/retrain;
không lấy chính prediction làm label. Không tự retrain khi thêm kỳ dữ liệu hay khi mở UI.

## 3. Features trước chuyến và khoảng cách

Ứng viên đối chứng nhỏ nhất: **PU/DO zone categorical + pickup local hour/day-of-week
cyclic + weekend + holiday v2**, tất cả biết trước chuyến. Không đưa ID vào như một số
thứ tự địa lý. Có thể dùng model này để đo mức hữu ích của thông tin biết trước chuyến
trên TLC gần hiện tại; chưa thể mặc định đưa vào phục vụ.

Ứng viên route-aware thêm **route distance được tính trước chuyến** từ Mapbox.
Giữ meters gốc, hiển thị meters/1000 km và suy ra mile bằng meters/1609.344,
không chuyển từ km đã làm tròn. Pin routing profile/options và provenance/version.
Mapbox định nghĩa distance là meters: [Directions API](https://docs.mapbox.com/api/navigation/directions/).

**Blocker dữ liệu route-aware:** TLC `trip_distance` là quãng đường thực tế đồng hồ;
`RatecodeID` là rate code cuối chuyến. Nó không phải quãng đường route được biết trước,
và ratecode cuối chuyến không được đưa vào vector như thông tin sẵn có khi đặt xe.
[TLC Yellow data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).
TLC chỉ có zone, không đủ tọa độ hai đầu để tái tạo đúng route từng chuyến. Không gọi
route giữa tâm zone rồi coi là tọa độ thật; không train bằng recorded distance rồi
tuyên bố holdout đó xác minh route-distance serving. Model v1 làm vậy chỉ là proxy lịch sử.

Muốn đánh giá route-aware phải có tập **paired** hợp pháp: feature route lúc yêu cầu,
timestamp/phiên bản, endpoints thật và fare của đúng completed trip để so sánh trên
cùng cohort. Đánh giá thêm route/recorded distance shift và lỗi theo nhóm; nếu thiếu
paired ground truth thì ghi route-aware chưa kiểm chứng, không bịa phép hiệu chỉnh.
Search Box mặc định dành cho temporary use; không mặc nhiên lưu tọa độ/response
Search để training. Quyền lưu và nguồn endpoints hợp lệ là điều kiện cần của tập paired:
[Mapbox Search Box API](https://docs.mapbox.com/api/search/search-box/).

Loại khỏi vector: target/fare và thành phần phí, tip/total, payment, actual dropoff
datetime/duration, recorded meter distance của chuyến mới, ratecode cuối chuyến,
weather sau thời điểm đặt, load ID/source month/lineage. Label fare và ratecode hoàn
thành vẫn được dùng cho cohort/evaluation offline, không phải feature.
Standard-rate gate trước chuyến giữ conservative exclusion sân bay/tuyến đặc biệt;
zone hợp lệ không tự chứng minh mọi chuyến trong zone đều standard-rate.

## 4. Ngày giờ và ngày lễ — thay đổi có kiểm soát ở v2

Mặc định **Đi ngay**: tại lúc submit backend lấy `datetime.now(ZoneInfo("America/New_York"))`,
không lấy giờ host/browser Việt Nam và không lấy lúc render. Hẹn giờ là tùy chọn;
validate giờ NYC, không cho quá khứ ngoài clock tolerance, horizon dự kiến tối đa 7 ngày
(cần chốt trước triển khai). Snapshot giữ thời điểm submit, offset, timezone và UUID;
rerun không tạo thời điểm hoặc request mới.

Hợp đồng v2 phải giữ instant + `America/New_York`/offset. Hẹn giờ gặp DST nonexistent
thì từ chối; ambiguous thì yêu cầu chọn lần thứ nhất/thứ hai và kiểm offset/fold.
Derive feature từ **giờ địa phương NYC**, không từ giờ UTC. Historical TLC TimestampNTZ
vẫn giữ wall-clock giả định NYC như v1; không tự đổi dữ liệu cũ thành UTC hay gán fold
khi không có bằng chứng. Pin tzdb/calendar coverage trong artifact; thiếu coverage thì
fail rõ ràng, không ngầm cho mọi ngày mới `is_holiday=0`.

| File/function hiện có | Ràng buộc thực tế | Thay đổi đề xuất, chưa thực hiện |
|---|---|---|
| `dashboard/address_page.py::render_page` | mặc định 15/06/2025, lịch 01/01–30/06/2025 | chế độ Đi ngay/Hẹn giờ v2; chỉ lấy now ở submit; snapshot cũ không đổi |
| `dashboard/route_fare.py::_pickup_time`, `RouteFareCoordinator.estimate` | naive datetime Jan–Jun/2025 và warning lịch sử | validator theo artifact/date policy v2, giữ v1 tái hiện và final standard gate |
| `dashboard/fare_page.py::render_page` | lịch chỉ 2025; cảnh báo sau tháng 6 | giữ trang manual v1 ở chế độ historical hoặc chuyển rõ sang contract v2, không chỉ sửa nhãn |
| `dashboard/prediction.py::build_prediction_request`, `_lineage`, `InferenceClient._request` | năm 2025, naive ISO; fixed v1 identity/error | client v2 tường minh, kiểm bundle identity/domain mới, không tự fallback v1 |
| `ml/inference.py::validate_inputs`, `input_warnings`, `FrozenPredictor._predict` | năm 2025; TimestampNTZ input; warnings gắn June 2025 | endpoint/contract v2 với timezone validation và feature builder v2; warnings theo evidence mới |
| `ml/inference_server.py::handler_for` | thông báo lỗi cứng local datetime 2025 | version contract/error/readiness tương ứng, giữ endpoint v1 nếu còn cần |
| `ml/pilot_model.py::prepare_features` | year==2025; import `FEDERAL_HOLIDAYS_2025` | **không sửa helper frozen v1**; thêm `ml/pretrip_features.py` v2 dùng chung train/serve |
| `etl/silver.py::FEDERAL_HOLIDAYS_2025` | 11 ngày liên bang toàn quốc 2025 | giữ nguyên vì thuộc source hash của v1; calendar v2 riêng, không sửa ETL lịch sử |
| `ml/pilot_data.py::cohort_predicates`, `_split_expression`; `ml/pilot.py::load_gold` | pickup Jan–Jun 2025, split cứng, đúng 6 manifests và profile v1 | orchestration v2 dùng lại helpers thuần phù hợp nhưng config cohort/split/lineage riêng; không nới contract pilot cũ |
| `scripts/create_calendar.py`, `etl/silver.py::transform_calendar`/`validate_silver_output` | source mặc định 2025 holiday=False, Silver gán OPM 2025/365 rows/11 holidays | serving v2 không đọc đây để nhận ngày hiện tại; không phải đổi Gold/calendar DB trong bước ML v2 |
| `ml/inference.py::validate_artifacts`; `scripts/model_artifacts.ps1`, `model_bundle.ps1`, `start.ps1`, `setup_system.ps1`; `config/model_bundle.lock.json` | pin model v1/source hashes, bundle ml-p4-v1 | thêm bundle/version policy v2 tường minh sau promotion, giữ rollback/v1; không tắt kiểm hash |

Calendar v2 đề xuất table Git-trackable đã đối chiếu OPM theo từng năm, đủ khoảng
train/serve và các năm lân cận để xử lý ngày observed vắt qua năm. Định nghĩa tường minh
**nationwide US federal observed holiday**, loại Inauguration Day chỉ áp dụng vùng DC;
không đồng nghĩa ngày nghỉ địa phương NY, school holiday hay mọi sự kiện ảnh hưởng taxi.
Năm 2025 phải trùng bộ 11 ngày frozen để kiểm hồi quy; thêm tests weekend/observed,
giao năm, leap day và DST. Nguồn: [OPM federal holidays](https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays/).
Không thay đổi logic holiday bị pin trong artifact v1 để cứu inference hiện tại.

## 5. Dữ liệu và gate trước khi thay model

Ngày 26/10/2026, trang TLC công khai liệt kê Yellow Jan–Aug 2026; có độ trễ xuất bản
khoảng 2 tháng. **Chỉ kiểm trang tài liệu, chưa tải/kiểm bytes các file.**
[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
Pilot nhỏ nhất đề xuất: **June 2026 train, July 2026 validation, August 2026 holdout**,
chia theo actual pickup local, không phải tháng nguồn. Một tháng train không chứng
minh seasonality; dùng 3 tháng gần nhất chỉ là pilot drift ban đầu, chưa đại diện
October 2026. Chốt thời điểm publication/cutoff và snapshots trước khi chạy.

Chuẩn bị retrospective positive finite fare, standard final ratecode, named zones;
giữ zero/rare/extreme theo cohort đã khai báo và báo số bị loại, không cắt để đẹp metrics.
Recorded distance có thể dùng kiểm chất lượng nhãn/nhóm phân tích, không làm feature
của đối chứng zone-time-only. Cohort route-aware cần tiêu chí riêng theo nguồn paired.
Business key từ fields ổn định của completed trip, không chỉ load ID; exact dedup,
journey-key split overlap guard; duplicate cùng key cùng quyết định hash sample.
Hash sample 1%, seed 42, predeclared caps khoảng 100k train/50k val/50k test; nếu vượt
caps hoặc split rỗng thì dừng và review, không ngầm đổi seed/lấy seed tốt.
Giữ labels/outcomes mới đến đủ trưởng thành; không ghép một request bất kỳ với fare
không cùng chuyến. Fit mọi preprocessing/categorical dictionaries chỉ trên train.

Baseline: median train toàn cục và median PU/DO×time-bin có fallback train-only,
chỉ chọn độ phức tạp bằng validation. Ứng viên nhỏ: Spark GBT zone-time-only;
route-aware là thí nghiệm riêng sau khi đủ dữ liệu paired hợp pháp. Không grid search
lớn. Chỉ mở holdout sau khi khóa ứng viên/cohort/features/thresholds/artifact identity.

Gate đề xuất cần chốt trước chạy: không leakage/overlap; finite predictions 100% trên
cohort đánh giá, giữ nguyên số dòng; validation MAE cải thiện ít nhất 10% so baseline
tốt nhất; final holdout phải thắng baseline và báo RMSE/R²/P50/P90/P95 absolute error.
Báo coverage và slices theo borough, zone/unseen, giờ, distance/fare bands (fare bands
chỉ đánh giá, không input), ít mẫu ghi giới hạn. Không clip âm hoặc bỏ ca khó để đạt gate.
Gate là tiêu chí chọn pilot, không phải cam kết dollar-error cho một yêu cầu hiện tại.
Nếu holdout thất bại, báo thất bại; không tune trên nó rồi tiếp tục gọi độc lập.
Triển khai shadow/opt-in chỉ sau cả kiểm offline, serving parity và review train-serving
distance gap; inference từ route chưa có paired test không được quảng bá accuracy toàn luồng.

## 6. Phạm vi triển khai tiếp và tài nguyên ước tính

Đây là **ước tính cho bước được duyệt sau**, không chi từ ngân sách lượt UI hiện tại.
Chưa đo size Parquet 2026 hoặc chạy profile/training nên không có cam kết thời gian.

| Bước tiếp theo | Files/images/services dự kiến | Nhu cầu tăng thêm ước tính, điều kiện |
|---|---|---|
| Contract/time/calendar v2 và tests offline | thêm `ml/pretrip_features.py`, `ml/pretrip_data.py`, contract/calendar config; tests boundary/DST/parity | code/config vài chục–trăm KiB; không cần dependency GIS/ML mới; kiểm tzdb sẵn có trước build |
| Acquire/profile 3 tháng và pilot đối chứng | worker CLI v2 gọi lại acquisition/ETL helpers trong runtime riêng, image worker tag ML mới | download Yellow nén khoảng 1–3 GiB/3 tháng; cả raw/Parquet/sample/shuffle peak khoảng 10–25 GiB, phải đo manifest/file sizes và xin ngân sách riêng trước tải; 2 CPU/3 GiB container ban đầu, JVM heap + overhead trong limit; khoảng 30 phút–vài giờ, không bảo đảm |
| Paired route-aware study | nguồn endpoints/outcome được cấp quyền, contract route provenance, evaluator mới | chưa thể ước tính API/dung lượng/thời gian khi chưa có nguồn/quyền và size; không tự phát requests/billing |
| Candidate bundle + serving | model v2 tag/version mới, `Dockerfile.inference`, inference/code contract; identity/startup/package guards; dashboard image cho Đi ngay/Hẹn giờ | nếu dependencies không đổi có thể refresh layer, nhưng cache chưa được bảo đảm; dự phòng thêm vài chục–vài trăm MiB chưa kể bundle/model; xác minh budget trước build, không tự cấp lại |
| Rollout có rollback | chỉ dashboard + fare-inference mới sau kiểm; artifact RO và model-loaded readiness | dashboard nhẹ như hiện tại; inference khởi điểm 2 CPU/3 GiB, load once; số đo thật sau chạy; không recreate PostgreSQL/MLflow hoặc đổi volumes để rollout model |

MLflow giữ vai trò tracking offline; PostgreSQL/dashboard analytics giữ nghĩa lịch sử,
không tự cần cập nhật khi chỉ thay model. Nếu dùng pipeline hiện có với kỳ 2026 cần
bổ sung hỗ trợ manifest/calendar theo cấu hình riêng ở bước sau, không giả rằng CLI
Jan–Jun 2025 hiện tại đã hỗ trợ. Không tải đủ mọi kỳ hay ghi đè Gold 2025 trong lượt này.

**Blockers trước lời hứa “ước tính hiện tại”:** cần phê duyệt ngân sách/data plan,
kiểm schema các file mới, triển khai contract/calendar v2, khóa/đánh giá ứng viên mới;
route-aware còn thiếu paired ground truth hợp pháp. UI đẹp hơn và parity v1 không
tự giải quyết temporal drift hoặc chứng nhận accuracy địa chỉ→giá hiện tại.
