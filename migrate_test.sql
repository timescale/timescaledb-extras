-- CREATE hypertable with 1 hour chunk time
CREATE TABLE sensor_data (
  time TIMESTAMPTZ NOT NULL,
  sensor_id INTEGER,
  temperature DOUBLE PRECISION,
  cpu DOUBLE PRECISION
);

SELECT create_hypertable('sensor_data', 'time', chunk_time_interval=>'1 hour'::interval);

-- Create the copy table to receive the data
CREATE TABLE sensor_data_copy (LIKE sensor_data INCLUDING DEFAULTS INCLUDING CONSTRAINTS EXCLUDING INDEXES);

-- INSERT one day of data
INSERT INTO sensor_data_copy (time, sensor_id, cpu, temperature)
SELECT
  time,
  sensor_id,
  random() AS cpu,
  random()*100 AS temperature
FROM generate_series(now() - interval '1 day', now(), interval '5 minute') AS g1(time), generate_series(1,4,1) AS g2(sensor_id);

CALL migrate_to_hypertable('sensor_data_copy','sensor_data');

SELECT count(*) = 0 as worked FROM sensor_data s1 FULL OUTER JOIN sensor_data_copy s2 ON s1 = s2 WHERE s1 IS NULL OR s2 is NULL;
SELECT * FROM sensor_data s1 FULL OUTER JOIN sensor_data_copy s2 ON s1 = s2 WHERE s1 IS NULL OR s2 is NULL;

truncate table sensor_data;

SELECT drop_migrate_log('sensor_data');
-- test parallel
CALL migrate_to_hypertable('sensor_data_copy','sensor_data', '2 hours'::interval, 'sensor_id', 2, 1);
CALL migrate_to_hypertable('sensor_data_copy','sensor_data', '2 hours'::interval, 'sensor_id', 2, 2);
SELECT count(*) = 0 as worked FROM sensor_data s1 FULL OUTER JOIN sensor_data_copy s2 ON s1 = s2 WHERE s1 IS NULL OR s2 is NULL;

SELECT drop_migrate_log('sensor_data');
drop table sensor_data cascade;
drop table  sensor_data_copy cascade;