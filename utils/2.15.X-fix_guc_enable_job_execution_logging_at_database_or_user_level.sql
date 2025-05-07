--
-- Since: https://github.com/timescale/timescaledb/pull/7131 the context of GUC
-- `timescaledb.enable_job_execution_logging` changed to PGC_SIGHUP and it means
-- that now it will only work either changing postgresql.conf or using ALTER SYSTEM
-- 
-- Before the context was PGC_USERSET, and with this context was possible to set at
-- session, database or user level. Setting it at database or user level create an
-- entry in the `pg_catalog.pg_db_role_setting` Postgres metadata table. Now after
-- changing the default context PG_USERSET to PG_SIGHUP make impossible to reset 
-- this GUC from database/user level leading to the following error:
-- 
--   tsdb=> alter database tsdb reset timescaledb.enable_job_execution_logging;
--   ERROR:  parameter "timescaledb.enable_job_execution_logging" cannot be changed now
--
-- So this script will remove potential `timescaledb.enable_job_execution_logging`
-- entries from `pg_catalog.pg_db_role_setting` and set it at system level using
-- `ALTER SYSTEM` statement when necessary
-- 
-- WARNING! this script *SHOULD* be executed by a *SUPERUSER* using `psql` console.
--

-- Check if exists `timescaledb.enable_job_execution_logging=on` for database/user
SELECT
  count(*) > 0 AS has_enable_job_execution_logging_on
FROM
  pg_catalog.pg_db_role_setting
WHERE
  'timescaledb.enable_job_execution_logging=on' = ANY(setconfig) \gset

-- If exists then remove it from `pg_catalog.pg_db_role_setting` and set it
-- at system level 
\if :has_enable_job_execution_logging_on
  BEGIN;

  -- Remove the `timescaledb.enable_job_execution_logging=on` from database/user
  UPDATE
    pg_catalog.pg_db_role_setting
  SET
    setconfig = pg_catalog.array_remove(setconfig, 'timescaledb.enable_job_execution_logging=on')
  WHERE
    'timescaledb.enable_job_execution_logging=on' = ANY(setconfig);

  -- Make sure we don't leave a row without a GUC
  DELETE
  FROM
    pg_catalog.pg_db_role_setting
  WHERE
    coalesce(array_length(setconfig, 1), 0) = 0;

  COMMIT;

  -- Set the GUC at system level and reload Postgres configuration
  ALTER SYSTEM SET timescaledb.enable_job_execution_logging=on;
  SELECT pg_reload_conf();
\endif

-- Check if exists `timescaledb.enable_job_execution_logging=off` for database/user
SELECT
  count(*) > 0 AS has_enable_job_execution_logging_off
FROM
  pg_catalog.pg_db_role_setting
WHERE
  'timescaledb.enable_job_execution_logging=off' = ANY(setconfig) \gset

-- If exists then remove it from `pg_catalog.pg_db_role_setting`
\if :has_enable_job_execution_logging_off
  BEGIN;

  -- Remove the `timescaledb.enable_job_execution_logging=off` from database/user
  UPDATE
    pg_catalog.pg_db_role_setting
  SET
    setconfig = pg_catalog.array_remove(setconfig, 'timescaledb.enable_job_execution_logging=off')
  WHERE
    'timescaledb.enable_job_execution_logging=off' = ANY(setconfig);

  -- Make sure we don't leave a row without a GUC
  DELETE
  FROM
    pg_catalog.pg_db_role_setting
  WHERE
    coalesce(array_length(setconfig, 1), 0) = 0;

  COMMIT;
\endif
