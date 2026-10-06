# STEP 4.5 — Data profiling & timezone decision

Phạm vi: Yellow Taxi từ sáu **source partition** 2025-01 đến 2025-06. Số liệu chi tiết và lineage nằm trong [`step4_5_profile.json`](step4_5_profile.json); profile đọc Silver cho các chuyến được giữ và Bronze cho các chuyến bị loại vì thời lượng. Không sửa Raw, Bronze, Silver hay tạo Gold.

## 1. Timezone: evidence, inference, assumption

**Evidence từ nguồn chính thức.** [Yellow Taxi Data Dictionary ngày 18/03/2025](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) định nghĩa `tpep_pickup_datetime`/`tpep_dropoff_datetime` là thời điểm meter bật/tắt, nhưng không ghi timezone cho hai cột này. [Trip Records User Guide](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf) và [TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) mô tả nguồn dữ liệu do technology providers cung cấp, không đặc tả phép chuyển timezone khi xuất Parquet. [Quy định TSP §66-24(f)(3)](https://www.nyc.gov/assets/tlc/downloads/pdf/technology_system_provider_rules_03_29.pdf) yêu cầu thời gian trong *Trip Data gửi cho TLC* được đo theo “Eastern Standard Time”; đây không phải đặc tả trực tiếp của hai cột trong Parquet công khai năm 2025. [Industry Notice 05-11](https://www.nyc.gov/assets/tlc/downloads/pdf/archived_industry_notices/industry_notice_05_11.pdf) cho thấy meter từng phải điều chỉnh khi bắt đầu daylight saving time, nên không diễn giải cụm “Eastern Standard Time” thành bảo đảm UTC−5 cố định quanh năm. Workbook TSP Data Specs được [TLC liên kết](https://www.nyc.gov/site/tlc/businesses/technology-system-provider-tsp.page) nhưng chưa đọc được trong môi trường này; không rút kết luận từ nội dung workbook. **Official TLC documentation reviewed does not explicitly specify the timezone of the published 2025 Parquet `tpep_*` fields.**

**Inference từ dữ liệu.** Hai cột Taxi được Spark đọc là `timestamp_ntz` (giờ đồng hồ không kèm offset). Phân bố pickup đạt 1.740.554 chuyến lúc 18:00 và 170.234 lúc 04:00, phù hợp với nhịp hoạt động địa phương nhưng không chứng minh timezone. Ngày NYC chuyển sang DST 09/03/2025 không có pickup hoặc dropoff ở giờ 02:00 trong Silver tháng 03, trong khi các ngày lân cận có dữ liệu. Đây là bằng chứng thực nghiệm mạnh cho cách hiểu *NYC local wall-clock*, không phải cam kết của TLC cho mọi vendor/ngày.

**Project assumption — CASE B.** Với độ tin cậy **moderate-high cho mục đích phân tích**, coi `pickup_datetime_local` và `dropoff_datetime_local` là giờ đồng hồ `America/New_York` khi căn chỉnh Gold. Giữ `timestamp_ntz`; không tự chuyển Taxi sang UTC hay áp UTC−5 cố định. Weather đã có `weather_timestamp_utc` và `weather_timestamp_nyc`.

### DST sanity check

Số chuyến Silver của source partition tháng 03; ngày và giờ lấy từ timestamp Taxi, không phải giờ UTC. Theo [US Naval Observatory](https://aa.usno.navy.mil/calculated/daylightsaving?submit=Get+Data&year=2025), DST tại Mỹ bắt đầu ngày 09/03/2025.

| Giờ local | Pickup 08/03 | Pickup 09/03 | Pickup 10/03 | Dropoff 09/03 |
| ---: | ---: | ---: | ---: | ---: |
| 00 | 8.026 | 9.528 | 1.512 | 10.005 |
| 01 | 6.330 | 7.864 | 662 | 8.352 |
| 02 | 4.167 | **0** | 327 | **0** |
| 03 | 2.872 | 6.178 | 279 | 6.463 |
| 04 | 1.752 | 3.914 | 368 | 4.406 |

Giờ 01:00 lặp lại khi *fall-back* tháng 11 sẽ mơ hồ nếu chỉ có wall-clock; tháng 11 không nằm trong Taxi Jan–Jun của STEP 5.

## 2. Invalid duration: Bronze → Silver

Silver đã loại **197.119/24.083.384** dòng Bronze (**0,818485%**) vì `dropoff <= pickup`; do đó phải đọc Bronze để phân loại, không thể tìm chúng trong Silver.

| Source month | Dropoff < pickup | Dropoff = pickup | Tổng loại | Tỷ lệ trên Bronze |
| --- | ---: | ---: | ---: | ---: |
| 2025-01 | 124 | 1.927 | 2.051 | 0,059018% |
| 2025-02 | 93 | 5.071 | 5.164 | 0,144345% |
| 2025-03 | 81 | 22.199 | 22.280 | 0,537482% |
| 2025-04 | 163 | 34.606 | 34.769 | 0,875671% |
| 2025-05 | 101 | 64.171 | 64.272 | 1,399699% |
| 2025-06 | 231 | 68.352 | 68.583 | 1,586482% |
| **Tổng** | **793** | **196.326** | **197.119** | **0,818485%** |

[TLC dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) ánh xạ `VendorID=7` tới **Helix**, `6` tới **Myle Technologies**. Cả **191.785/191.785** dòng vendor 7 trong Bronze có pickup = dropoff và bị loại; nhóm này chiếm **97,29%** số invalid duration. Từ tháng 01 đến 06, số invalid tăng 66.532, còn nhóm vendor 7 tăng 66.367 — giải thích **99,75% mức tăng về mặt số học**, nhưng chưa xác định được nguyên nhân ở hệ thống nguồn. Vendor 6 chiếm **749/793** trường hợp dropoff trước pickup. Không đổi quality rule trong STEP 4.5.

## 3. Fare và các anomaly được giữ

Silver giữ **1.322.979** chuyến `fare_amount < 0` (**5,538660%** của 23.886.265 chuyến), tất cả thuộc `vendor_id=2` (**Curb Mobility** theo [TLC dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)). Theo source month, số lượng lần lượt là **144.107, 182.645, 208.716, 186.709, 325.250, 275.552** từ tháng 01 đến 06. `payment_type=0` Flex Fare có **917.296** dòng (~69,34% negative fare); `4` Dispute **256.964** (~19,42%); `2` Cash **96.344** (~7,28%); `3` No charge **52.024** (~3,93%); `1` Credit card **351**; `5` Unknown và `6` Voided trip đều **0**. `ratecode_id` null ở **917.296** dòng và bằng `1` ở **358.638** dòng; breakdown còn lại nằm trong JSON. Mã thanh toán được diễn giải theo TLC dictionary; không suy ra mọi fare âm đều là hoàn tiền/tranh chấp.

Fare âm có min **−1.807,60**, max **−0,01**, quantile xấp xỉ p01 **−74,80**, p50 **−4,75**, p99 **−0,76**. `trip_distance` trong nhóm này có p50 **2,08 mi**, p99 **18,63 mi** nhưng max **320.136,29 mi**; `total_amount` có p50 **+2,96**. Phân bố không đồng nhất, nên không áp ngưỡng tùy ý hoặc gán một nguyên nhân nghiệp vụ duy nhất.

Silver còn giữ **9.726** fare bằng 0, **655.348** distance bằng 0 (**2,743619%**), **5.417.233** passenger count null (**22,679280%**) và **137.707** passenger count bằng 0 (**0,576511%**). Theo source month, distance bằng 0 là **90.201 / 99.105 / 102.960 / 90.602 / 139.598 / 132.882**; passenger null là **539.930 / 806.764 / 916.484 / 745.469 / 1.195.946 / 1.212.640**. Không loại hoặc impute các dòng này ở Silver. Ngoài ra, **137** pickup nằm ngoài tháng file nguồn; **24** có calendar month ngoài Jan–Jun (21 ở 12/2024, 3 ở 2007/2009). “Ngoài Jan–Jun” không đồng nghĩa chắc chắn không ghép được Weather: dữ liệu UTC tháng 01 có thể phủ một phần tối 31/12 tại NYC.

## 4. Policy cho Gold và ML tương lai

**Gold temporal alignment:** ghép `pickup_date` + `pickup_hour` của Taxi với `weather_date_nyc` + `weather_hour_nyc` của Weather theo giờ local; không chuyển Taxi NTZ sang UTC chỉ để ghép theo giờ. Dùng phép ghép giữ Taxi, đếm và công bố khóa Weather không khớp, không âm thầm làm mất chuyến. Phạm vi Weather thô là **2025-01-01 00:00 đến 2025-06-30 23:00 UTC**, tương ứng khoảng **2024-12-31 19:00 đến 2025-06-30 19:00 NYC**; bốn giờ local cuối ngày 30/06 không có Weather từ nguồn hiện tại. Cần kiểm chứng unmatched thực tế ở STEP 5, không suy ra từ riêng tháng file. Đây là policy, **chưa thực hiện join/Gold**.

**ML target preview:** Gold phân tích giữ nguyên fare anomalies và gắn nhãn. Với mô hình dự đoán fare dương trước chuyến đi, đề xuất chỉ ở tập train tương lai `fare_amount > 0`: loại ứng viên **1.332.705** dòng fare âm/bằng 0 (**5,579378%** Silver), còn **22.553.560** dòng trước các rule ML khác. Cần đánh giá selection bias vì fare âm tập trung ở vendor 2. Không dùng `total_amount`, `tip_amount`, `tolls_amount`, `payment_type`, `dropoff_datetime_local` hoặc `trip_distance` *thực đo* làm predictor trước chuyến đi; distance chỉ hợp lệ nếu có **ước lượng tuyến đường sẵn tại thời điểm dự đoán**. Xử lý passenger null/zero, outlier và tính sẵn có của dropoff location thuộc thiết kế ML riêng, không phải quy tắc Silver.

## 5. Checkpoint và giới hạn

**READY FOR STEP 5, với caveats:** assumption timezone được ghi rõ, kiểm tra DST hỗ trợ nó, và policy join/unmatched đã xác định; đây không phải xác nhận timezone chính thức của TLC. Chưa kiểm tra được workbook TSP, chưa giải thích nguyên nhân dữ liệu vendor 7, và chưa đo unmatched Weather bằng join thực tế. STEP 4.5 chỉ tạo báo cáo nhỏ, không thay đổi dữ liệu hoặc triển khai STEP 5.

Có thể tái tạo profile bằng `python scripts/profile_step4_5.py` trong môi trường Spark của project; script đọc từng tháng, đối chiếu số dòng/anomaly với Bronze và Silver manifests và chỉ ghi JSON tổng hợp.
