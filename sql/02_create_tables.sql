CREATE TABLE IF NOT EXISTS analytics.dim_location (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR(100) NOT NULL,
    zone VARCHAR(150) NOT NULL,
    service_zone VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS analytics.dim_calendar (
    date_id DATE PRIMARY KEY,
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    day_name VARCHAR(20) NOT NULL,
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    year SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    holiday_name VARCHAR(150)
);

CREATE TABLE IF NOT EXISTS analytics.dim_weather (
    weather_id BIGSERIAL PRIMARY KEY,
    observation_datetime TIMESTAMP NOT NULL,
    temperature NUMERIC(6, 2),
    precipitation NUMERIC(8, 3),
    snow NUMERIC(8, 3),
    wind_speed NUMERIC(7, 2),
    weather_condition VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS analytics.fact_trip (
    trip_id BIGINT PRIMARY KEY,
    pickup_datetime TIMESTAMP NOT NULL,
    dropoff_datetime TIMESTAMP NOT NULL,
    pickup_location_id INTEGER NOT NULL,
    dropoff_location_id INTEGER NOT NULL,
    trip_distance NUMERIC(10, 3),
    trip_duration_minutes NUMERIC(10, 2),
    passenger_count SMALLINT,
    fare_amount NUMERIC(10, 2) NOT NULL,
    total_amount NUMERIC(10, 2),
    payment_type VARCHAR(50),
    weather_id BIGINT,
    date_id DATE NOT NULL,
    CONSTRAINT fk_fact_trip_pickup_location
        FOREIGN KEY (pickup_location_id) REFERENCES analytics.dim_location (location_id),
    CONSTRAINT fk_fact_trip_dropoff_location
        FOREIGN KEY (dropoff_location_id) REFERENCES analytics.dim_location (location_id),
    CONSTRAINT fk_fact_trip_weather
        FOREIGN KEY (weather_id) REFERENCES analytics.dim_weather (weather_id),
    CONSTRAINT fk_fact_trip_calendar
        FOREIGN KEY (date_id) REFERENCES analytics.dim_calendar (date_id),
    CONSTRAINT chk_fact_trip_datetimes
        CHECK (dropoff_datetime >= pickup_datetime)
);

