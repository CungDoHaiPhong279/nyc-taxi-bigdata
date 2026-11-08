-- Precomputed KPI buckets (first page load took 26-52s scanning all of fact_trip; filters match VALID_FARE_SQL/VALID_DISTANCE_SQL).
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_kpi_buckets;
CREATE MATERIALIZED VIEW analytics.mv_kpi_buckets AS
SELECT
    f.source_year,
    f.source_month,
    f.pickup_hour,
    f.pickup_location_id,
    COUNT(*) AS trip_count,
    SUM(f.fare_amount) AS fare_sum,
    SUM(f.fare_amount) FILTER (WHERE f.fare_amount > 0 AND f.fare_amount <= 500) AS valid_fare_sum,
    COUNT(*) FILTER (WHERE f.fare_amount > 0 AND f.fare_amount <= 500) AS valid_fare_count,
    SUM(f.trip_distance) FILTER (WHERE f.trip_distance > 0 AND f.trip_distance <= 100) AS valid_distance_sum,
    COUNT(*) FILTER (WHERE f.trip_distance > 0 AND f.trip_distance <= 100) AS valid_distance_count
FROM analytics.fact_trip AS f
GROUP BY f.source_year, f.source_month, f.pickup_hour, f.pickup_location_id;

-- Precomputed multisource summary for the historical dashboard: joining every
-- fact row to weather and calendar on each page load took over 180 seconds.
-- Rebuilt by `load_postgres.py --create-indexes`; rerun after loading new months.
-- The fare and distance filters match VALID_FARE_SQL and VALID_DISTANCE_SQL in
-- src/nyc_taxi/dashboard/queries.py.
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_multisource_summary;
CREATE MATERIALIZED VIEW analytics.mv_multisource_summary AS
SELECT
    f.source_year,
    f.source_month,
    f.pickup_location_id,
    CASE
        WHEN COALESCE(w.precipitation, 0) >= 2.0 OR COALESCE(w.snowfall, 0) > 0
            THEN '3. Mưa lớn / Có tuyết (>= 2 mm/h)'
        WHEN COALESCE(w.precipitation, 0) > 0
            THEN '2. Mưa nhỏ & Mưa vừa (< 2 mm/h)'
        ELSE '1. Trời quang / Khô ráo (0 mm/h)'
    END AS weather_bucket,
    CASE
        WHEN COALESCE(c.is_holiday, false)
            THEN '3. Ngày nghỉ lễ Liên bang (Holiday)'
        WHEN COALESCE(c.is_weekend, false)
            THEN '2. Cuối tuần (Thứ 7 & Chủ Nhật)'
        ELSE '1. Ngày làm việc (Thứ 2 – Thứ 6)'
    END AS calendar_bucket,
    COUNT(*) AS trip_count,
    SUM(f.fare_amount) FILTER (WHERE f.fare_amount > 0 AND f.fare_amount <= 500) AS fare_sum,
    COUNT(*) FILTER (WHERE f.fare_amount > 0 AND f.fare_amount <= 500) AS fare_count,
    SUM(f.trip_distance) FILTER (WHERE f.trip_distance > 0 AND f.trip_distance <= 100) * 1.60934 AS distance_km_sum,
    COUNT(*) FILTER (WHERE f.trip_distance > 0 AND f.trip_distance <= 100) AS distance_count
FROM analytics.fact_trip AS f
LEFT JOIN analytics.dim_weather AS w ON w.weather_id = f.weather_id
LEFT JOIN analytics.dim_calendar AS c ON c.date_id = COALESCE(f.calendar_date_id, f.pickup_date)
GROUP BY 1, 2, 3, 4, 5;

-- Fare history by route and hour for departure advice. Jan-May only, because the
-- rule was validated by predicting June from these months (see the README).
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_route_hour_fare;
CREATE MATERIALIZED VIEW analytics.mv_route_hour_fare AS
SELECT
    f.pickup_location_id,
    f.dropoff_location_id,
    c.is_weekend,
    f.pickup_hour,
    COUNT(*) AS trip_count,
    AVG(f.fare_amount) AS mean_fare_usd,
    VAR_POP(f.fare_amount) AS fare_variance
FROM analytics.fact_trip AS f
JOIN analytics.dim_calendar AS c ON c.date_id = COALESCE(f.calendar_date_id, f.pickup_date)
WHERE f.source_year = 2025 AND f.source_month <= 5
    AND f.ratecode_id = 1
    AND f.fare_amount > 0 AND f.fare_amount <= 500
    AND f.pickup_location_id BETWEEN 1 AND 263
    AND f.dropoff_location_id BETWEEN 1 AND 263
    AND NOT c.is_holiday
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) >= 30;
CREATE UNIQUE INDEX mv_route_hour_fare_key
    ON analytics.mv_route_hour_fare (pickup_location_id, dropoff_location_id, is_weekend, pickup_hour);
