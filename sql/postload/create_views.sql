-- Optional, non-materialized convenience view. The four LEFT JOINs preserve
-- every fact row, including the 24 calendar and 20,246 weather unmatched trips.
CREATE OR REPLACE VIEW analytics.vw_trip_enriched AS
SELECT
    f.*,
    pu.borough AS pickup_borough,
    pu.zone AS pickup_zone,
    pu.service_zone AS pickup_service_zone,
    do_zone.borough AS dropoff_borough,
    do_zone.zone AS dropoff_zone,
    do_zone.service_zone AS dropoff_service_zone,
    c.day_of_week,
    c.day_name,
    c.is_weekend,
    c.is_holiday,
    c.holiday_name,
    w.weather_timestamp_utc,
    w.weather_timestamp_nyc,
    w.temperature_2m,
    w.precipitation,
    w.snowfall,
    w.wind_speed_10m,
    w.weather_code
FROM analytics.fact_trip AS f
LEFT JOIN analytics.dim_location AS pu
    ON f.pickup_location_id = pu.location_id
LEFT JOIN analytics.dim_location AS do_zone
    ON f.dropoff_location_id = do_zone.location_id
LEFT JOIN analytics.dim_calendar AS c
    ON f.calendar_date_id = c.date_id
LEFT JOIN analytics.dim_weather AS w
    ON f.weather_id = w.weather_id;
