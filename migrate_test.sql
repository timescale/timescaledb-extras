-- CREATE hypertable with 1 hour chunk time
CREATE TABLE sensor_data (
  ts TIMESTAMPTZ NOT NULL,
  sensor_id INTEGER,
  temperature DOUBLE PRECISION,
  cpu DOUBLE PRECISION
);

-- INSERT one day of data
INSERT INTO sensor_data (ts, sensor_id, cpu, temperature)
SELECT
  ts,
  sensor_id,
  random() AS cpu,
  random()*100 AS temperature
FROM generate_series(now() - interval '1 day', now(), interval '5 minute') AS g1(time), generate_series(1,4,1) AS g2(sensor_id);


-- Create the copy table to receive the data
CREATE TABLE sensor_data_new (LIKE sensor_data INCLUDING DEFAULTS INCLUDING CONSTRAINTS EXCLUDING INDEXES);

SELECT create_hypertable('sensor_data_new', 'ts', chunk_time_interval=>'1 hour'::interval);

-- Migrate the data with a single thread.
CALL migrate_to_hypertable('sensor_data','sensor_data_new');

-- Verify that all rows were copied. On a real, large database, this would not be
-- an efficient way to verify the data.
SELECT count(*) = 0 as worked FROM sensor_data s1 FULL OUTER JOIN sensor_data_new s2 ON s1 = s2 WHERE s1 IS NULL OR s2 is NULL;
SELECT * FROM sensor_data s1 FULL OUTER JOIN sensor_data_new s2 ON s1 = s2 WHERE s1 IS NULL OR s2 is NULL;

-- Truncate the hypertable to setup
truncate table sensor_data_new;

-- DROP the migration table for the single process migration
SELECT drop_migrate_log('sensor_data_new');

-- test parallel migration using two processes and 2 hour batch sizes
-- In a migration of real data, these would have to be run in separate
-- PostgreSQL sessions.
CALL migrate_to_hypertable('sensor_data','sensor_data_new', '2 hours'::interval, 'sensor_id', 2, 1);
CALL migrate_to_hypertable('sensor_data','sensor_data_new', '2 hours'::interval, 'sensor_id', 2, 2);

-- Verify that all rows were migrated again.
SELECT count(*) = 0 as worked FROM sensor_data s1 FULL OUTER JOIN sensor_data_new s2 ON s1 = s2 WHERE s1 IS NULL OR s2 is NULL;

-- drop the logging table again to repeat testing.
SELECT drop_migrate_log('sensor_data_new');
-- Drop both testing tables to start fresh.
drop table sensor_data cascade;
drop table  sensor_data_new cascade;