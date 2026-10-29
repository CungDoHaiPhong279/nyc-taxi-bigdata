-- Stage G3: Additive PostgreSQL tables and view for Gold V3 ML predictions and metrics.
-- Safe to run multiple times (idempotent); never modifies existing fact_trip or dim_* tables.

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.model_metrics (
    contract_version VARCHAR(64) NOT NULL,
    model_key VARCHAR(64) NOT NULL,
    model_name VARCHAR(128) NOT NULL,
    split VARCHAR(32) NOT NULL,
    uses_weather BOOLEAN NOT NULL,
    row_count INTEGER NOT NULL,
    mae_usd DOUBLE PRECISION,
    rmse_usd DOUBLE PRECISION,
    r2 DOUBLE PRECISION,
    p50_abs_error_usd DOUBLE PRECISION,
    p90_abs_error_usd DOUBLE PRECISION,
    p95_abs_error_usd DOUBLE PRECISION,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (contract_version, model_key, split)
);

CREATE TABLE IF NOT EXISTS analytics.fact_fare_prediction (
    prediction_id BIGSERIAL PRIMARY KEY,
    contract_version VARCHAR(64) NOT NULL,
    split VARCHAR(16) NOT NULL,
    row_id INTEGER NOT NULL,
    pickup_datetime_local TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    pickup_date DATE NOT NULL,
    pickup_hour SMALLINT NOT NULL,
    pickup_location_id SMALLINT NOT NULL REFERENCES analytics.dim_location(location_id),
    dropoff_location_id SMALLINT NOT NULL REFERENCES analytics.dim_location(location_id),
    zone_centroid_distance_km DOUBLE PRECISION NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    temperature_2m DOUBLE PRECISION NOT NULL,
    precipitation DOUBLE PRECISION NOT NULL,
    snowfall DOUBLE PRECISION NOT NULL,
    wind_speed_10m DOUBLE PRECISION NOT NULL,
    weather_code SMALLINT NOT NULL,
    actual_fare_usd DOUBLE PRECISION NOT NULL,
    pred_b0_hierarchical_usd DOUBLE PRECISION NOT NULL,
    pred_m1_lr_usd DOUBLE PRECISION NOT NULL,
    pred_m2_rf_usd DOUBLE PRECISION NOT NULL,
    pred_m3_gbt_weather_usd DOUBLE PRECISION NOT NULL,
    pred_m3_nw_gbt_usd DOUBLE PRECISION NOT NULL,
    abs_error_m3_weather_usd DOUBLE PRECISION NOT NULL,
    abs_error_m3_nw_usd DOUBLE PRECISION NOT NULL,
    UNIQUE (contract_version, split, row_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_fare_pred_split_date
    ON analytics.fact_fare_prediction (split, pickup_date);

CREATE INDEX IF NOT EXISTS idx_fact_fare_pred_pu_do
    ON analytics.fact_fare_prediction (pickup_location_id, dropoff_location_id);

CREATE OR REPLACE VIEW analytics.vw_fare_prediction_enriched AS
SELECT
    p.prediction_id,
    p.contract_version,
    p.split,
    p.row_id,
    p.pickup_datetime_local,
    p.pickup_date,
    p.pickup_hour,
    p.pickup_location_id,
    pu.borough AS pickup_borough,
    pu.zone AS pickup_zone_name,
    p.dropoff_location_id,
    dl.borough AS dropoff_borough,
    dl.zone AS dropoff_zone_name,
    p.zone_centroid_distance_km,
    p.is_weekend,
    p.is_holiday,
    p.temperature_2m,
    p.precipitation,
    p.snowfall,
    p.wind_speed_10m,
    p.weather_code,
    CASE WHEN p.precipitation > 0 THEN 'Rain / Precip' ELSE 'Clear / Dry' END AS weather_condition,
    p.actual_fare_usd,
    p.pred_b0_hierarchical_usd,
    p.pred_m1_lr_usd,
    p.pred_m2_rf_usd,
    p.pred_m3_gbt_weather_usd,
    p.pred_m3_nw_gbt_usd,
    p.abs_error_m3_weather_usd,
    p.abs_error_m3_nw_usd,
    (p.pred_m3_gbt_weather_usd - p.actual_fare_usd) AS residual_m3_weather_usd
FROM analytics.fact_fare_prediction p
LEFT JOIN analytics.dim_location pu ON p.pickup_location_id = pu.location_id
LEFT JOIN analytics.dim_location dl ON p.dropoff_location_id = dl.location_id;
