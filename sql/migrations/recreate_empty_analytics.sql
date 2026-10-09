-- Opt-in migration from the STEP 1 skeleton to schema version 2.
-- Execute this file, then ../01_create_schema.sql and ../02_create_tables.sql in ONE
-- transaction. This script never drops the database, schema, volume, or any
-- unknown object. It refuses to remove even one row from a known table.
CREATE SCHEMA IF NOT EXISTS analytics;

DO $$
DECLARE
    table_name TEXT;
    has_rows BOOLEAN;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'fact_trip', 'etl_load_log', 'dim_weather', 'dim_calendar', 'dim_location'
    ] LOOP
        IF to_regclass(format('analytics.%I', table_name)) IS NOT NULL THEN
            EXECUTE format('LOCK TABLE analytics.%I IN ACCESS EXCLUSIVE MODE', table_name);
            EXECUTE format('SELECT EXISTS (SELECT 1 FROM analytics.%I)', table_name)
                INTO has_rows;
            IF has_rows THEN
                RAISE EXCEPTION 'Refusing to recreate analytics: %.% is not empty',
                    'analytics', table_name;
            END IF;
        END IF;
    END LOOP;
END
$$;

DROP VIEW IF EXISTS analytics.vw_trip_enriched;
DROP TABLE IF EXISTS analytics.fact_trip;
DROP TABLE IF EXISTS analytics.etl_load_log;
DROP TABLE IF EXISTS analytics.dim_weather;
DROP TABLE IF EXISTS analytics.dim_calendar;
DROP TABLE IF EXISTS analytics.dim_location;
