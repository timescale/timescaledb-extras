CREATE EXTENSION IF NOT EXISTS timescaledb;

SET timezone = 'UTC';
\pset pager off

-- create a base table for sensor data
CREATE TABLE IF NOT EXISTS sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id INTEGER NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data', 'time',
    chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO sensor_data
SELECT
    timestamp '2026-01-01' + (i * INTERVAL '4 hours') AS time,
    (i % 5) + 1 AS sensor_id,
    15 + 15 * random() AS temperature,
    30 + 60 * random() AS humidity,
    980 + 40 * random() AS pressure
FROM generate_series(0, 1250) AS i;

CREATE MATERIALIZED VIEW sensor_hourly_avg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    sensor_id,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(pressure) AS avg_pressure,
    COUNT(*) AS reading_count
FROM sensor_data
GROUP BY bucket, sensor_id
WITH NO DATA;

-- refresh the aggregate completely
CALL refresh_continuous_aggregate('sensor_hourly_avg', NULL, NULL);

CREATE MATERIALIZED VIEW sensor_daily_avg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS bucket,
    sensor_id,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity,
    AVG(pressure) AS avg_pressure,
    COUNT(*) AS reading_count
FROM sensor_data
GROUP BY bucket, sensor_id
WITH NO DATA;

CALL refresh_continuous_aggregate('sensor_daily_avg', NULL, NULL);

-- Load the view and function
\i cagg_manual_refresh_ranges.sql
