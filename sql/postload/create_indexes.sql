-- Schema version 2: deferred, non-essential analytical indexes.
-- Apply after the initial bulk load; safe to rerun.
CREATE INDEX IF NOT EXISTS fact_trip_pickup_datetime_idx
    ON analytics.fact_trip (pickup_datetime_local);
CREATE INDEX IF NOT EXISTS fact_trip_pickup_location_idx
    ON analytics.fact_trip (pickup_location_id);
CREATE INDEX IF NOT EXISTS fact_trip_dropoff_location_idx
    ON analytics.fact_trip (dropoff_location_id);
CREATE INDEX IF NOT EXISTS fact_trip_calendar_date_idx
    ON analytics.fact_trip (calendar_date_id);
CREATE INDEX IF NOT EXISTS fact_trip_weather_idx
    ON analytics.fact_trip (weather_id);
CREATE INDEX IF NOT EXISTS fact_trip_pickup_hour_idx
    ON analytics.fact_trip (pickup_hour);
CREATE INDEX IF NOT EXISTS fact_trip_dashboard_bucket_idx
    ON analytics.fact_trip (source_year, source_month, pickup_hour, pickup_location_id)
    INCLUDE (fare_amount, trip_distance);
-- NYC local wall-clock hour is deliberately NOT unique: future fall-back DST
-- periods may contain two different UTC observations for one local hour.
CREATE INDEX IF NOT EXISTS dim_weather_local_hour_idx
    ON analytics.dim_weather (weather_date_nyc, weather_hour_nyc);
