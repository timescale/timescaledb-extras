SET timezone = 'UTC';
\pset pager off

SET client_min_messages TO NOTICE;
SELECT * FROM  _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- ============================================================
-- Test 1: Verify generated refresh statement works as expected
-- when there are gaps in the CAgg
-- ============================================================

INSERT INTO sensor_data
SELECT
    timestamp '2026-01-01' + (i * INTERVAL '4 hours') AS time,
    (i % 5) + 1 AS sensor_id,
    20.0 AS temperature,
    50.0 AS humidity,
    1000.0 AS pressure
FROM generate_series(0, 11) AS i;

SELECT * FROM  hyper_invlog_view;
-- Truncate the invalidation log and insert a pending range to cover the same window to simulate a gap
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_ranges
    (materialization_id, lowest_modified_value, greatest_modified_value)
SELECT mat_hypertable_id,
       EXTRACT(EPOCH FROM '2026-01-01 00:00:00+00'::timestamptz)::bigint * 1000000,
       EXTRACT(EPOCH FROM '2026-01-03 00:00:00+00'::timestamptz)::bigint * 1000000
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'sensor_hourly_avg';

-- Check the CAgg state before refresh
\echo '\n=== CAgg state before refresh ==='
SELECT bucket, sensor_id, reading_count
FROM sensor_hourly_avg
WHERE bucket >= '2026-01-01 00:00:00+00' AND bucket < '2026-01-03 00:00:00+00'
ORDER BY bucket, sensor_id;

\echo '\n=== Pending ranges ==='
SELECT * FROM public.cagg_pending_ranges;

\echo '\n=== Generated refresh + delete commands ==='
SELECT * FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg');

\echo '\n=== Running generated refresh command ==='
SELECT refresh_command FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg') LIMIT 1
\gexec

-- Verify the CAgg updated correctly
\echo '\n=== CAgg state after refresh ==='
SELECT bucket, sensor_id, reading_count
FROM sensor_hourly_avg
WHERE bucket >= '2026-01-01 00:00:00+00' AND bucket < '2026-01-03 00:00:00+00'
ORDER BY bucket, sensor_id;

-- Check that the pending range was consumed
\echo '\n=== Pending ranges after refresh ==='
SELECT * FROM public.cagg_pending_ranges;

-- Run the generated delete command
-- This does nothing for versions <2.26.0 since the materialization_ranges table is cleared after the refresh
\echo '\n=== Running generated delete command ==='
SELECT delete_command FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg') LIMIT 1
\gexec

-- Check that the pending range was consumed
\echo '\n=== Pending ranges after delete ==='
SELECT * FROM public.cagg_pending_ranges;

-- ============================================================
-- Test 2: Overlapping ranges
-- ============================================================
\echo '\n=== Test 2: Overlapping ranges ==='

DELETE FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Insert two overlapping ranges for sensor_hourly_avg:
INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_ranges
    (materialization_id, lowest_modified_value, greatest_modified_value)
SELECT mat_hypertable_id,
       EXTRACT(EPOCH FROM lo::timestamptz)::bigint * 1000000,
       EXTRACT(EPOCH FROM hi::timestamptz)::bigint * 1000000
FROM _timescaledb_catalog.continuous_agg,
     (VALUES ('2026-01-01 00:00:00+00', '2026-01-05 00:00:00+00'),
             ('2026-01-03 00:00:00+00', '2026-01-07 00:00:00+00')) AS v(lo, hi)
WHERE user_view_name = 'sensor_hourly_avg';

\echo 'Before refresh — two overlapping ranges:'
SELECT * FROM public.cagg_pending_ranges;

\echo '\n=== Generated refresh + delete commands ==='
SELECT * FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg');

-- Run only the first generated refresh command
\echo '\n=== Running first generated refresh command ==='
SELECT refresh_command FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg') LIMIT 1
\gexec

-- Verify one pending range still exists
SELECT * FROM public.cagg_pending_ranges;

-- ============================================================
-- Test 3: Overlapping ranges in the materialization window
-- ============================================================
\echo '\n=== Test 3: Overlapping ranges in the materialization window ==='

DELETE FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Insert invalidations into existing buckets (4-hour intervals, cycling sensor_id 1-5)
INSERT INTO sensor_data
SELECT ts, ((ROW_NUMBER() OVER (ORDER BY ts) - 1) % 5 + 1)::int, 20.0, 50.0, 1000.0
FROM (
    SELECT generate_series('2026-01-04 08:00:00+00'::timestamptz, '2026-01-05 00:00:00+00'::timestamptz, INTERVAL '4 hours') AS ts
    UNION ALL
    SELECT generate_series('2026-01-06 00:00:00+00'::timestamptz, '2026-01-06 16:00:00+00'::timestamptz, INTERVAL '4 hours')
) sub;

SELECT * FROM  hyper_invlog_view;
TRUNCATE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

-- Insert two overlapping ranges:
INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_ranges
    (materialization_id, lowest_modified_value, greatest_modified_value)
SELECT mat_hypertable_id,
       EXTRACT(EPOCH FROM lo::timestamptz)::bigint * 1000000,
       EXTRACT(EPOCH FROM hi::timestamptz)::bigint * 1000000
FROM _timescaledb_catalog.continuous_agg,
     (VALUES ('2026-01-01 00:00:00+00', '2026-01-05 00:00:00+00'),
             ('2026-01-03 00:00:00+00', '2026-01-07 00:00:00+00')) AS v(lo, hi)
WHERE user_view_name = 'sensor_hourly_avg';

SELECT * FROM public.cagg_pending_ranges;

\echo 'CAgg state before refresh:'
SELECT bucket, sensor_id, reading_count
FROM sensor_hourly_avg
WHERE bucket >= '2026-01-01 00:00:00+00' AND bucket < '2026-01-07 00:00:00+00'
ORDER BY bucket, sensor_id;

\echo '\n=== Generated refresh + delete commands ==='
SELECT * FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg');

-- Run first generated refresh command
\echo '\n=== Running first generated refresh command ==='
SELECT refresh_command FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg') LIMIT 1
\gexec

\echo 'Pending ranges after first refresh:'
SELECT * FROM public.cagg_pending_ranges;

\echo 'CAgg state after first refresh (Jan 1 - Jan 7):'
SELECT bucket, sensor_id, reading_count
FROM sensor_hourly_avg
WHERE bucket >= '2026-01-01 00:00:00+00' AND bucket < '2026-01-07 00:00:00+00'
ORDER BY bucket, sensor_id;

-- Run second generated refresh command
\echo '\n=== Running second generated refresh command ==='
SELECT refresh_command FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg') LIMIT 1
\gexec

\echo 'Pending ranges after second refresh:'
SELECT * FROM public.cagg_pending_ranges;

\echo 'CAgg state after second refresh (Jan 1 - Jan 7):'
SELECT bucket, sensor_id, reading_count
FROM sensor_hourly_avg
WHERE bucket >= '2026-01-01 00:00:00+00' AND bucket < '2026-01-07 00:00:00+00'
ORDER BY bucket, sensor_id;

-- ============================================================
-- Test 4: Pending range + invalidation logs in the same window
-- ============================================================
\echo '\n=== Test 4: Pending range with invalidation logs present ==='

DELETE FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Insert data in to generate invalidation log entries
INSERT INTO sensor_data
SELECT
    timestamp '2026-03-01' + (i * INTERVAL '4 hours') AS time,
    (i % 5) + 1 AS sensor_id,
    20.0 AS temperature,
    50.0 AS humidity,
    1000.0 AS pressure
FROM generate_series(0, 35) AS i;

\echo 'Invalidation log entries after insert:'
SELECT hypertable_id,
       _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest_modified_value,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
ORDER BY lowest_modified_value;

-- Also insert a pending range covering the same window
INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_ranges
    (materialization_id, lowest_modified_value, greatest_modified_value)
SELECT mat_hypertable_id,
       EXTRACT(EPOCH FROM '2026-03-01 00:00:00+00'::timestamptz)::bigint * 1000000,
       EXTRACT(EPOCH FROM '2026-03-05 00:00:00+00'::timestamptz)::bigint * 1000000
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'sensor_hourly_avg';

\echo 'Pending ranges before refresh:'
SELECT * FROM public.cagg_pending_ranges;

\echo 'CAgg state before refresh (Mar 1 - Mar 5):'
SELECT bucket, sensor_id, reading_count
FROM sensor_hourly_avg
WHERE bucket >= '2026-03-01 00:00:00+00' AND bucket < '2026-03-05 00:00:00+00'
ORDER BY bucket, sensor_id
LIMIT 10;

\echo '\n=== Generated refresh + delete commands ==='
SELECT * FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg');

-- Run the generated refresh command : ERRORs out
-- this fails due to pending ranges but the invalidation ranges are cleared
-- This shouldn't fail post 2.26.0 release
\echo '\n=== Running generated refresh command ==='
SELECT refresh_command FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg') LIMIT 1
\gexec

-- Invalidation logs are cleared after the refresh attempt
\echo 'Invalidation log after refresh attempt:'
SELECT hypertable_id,
       _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest_modified_value,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
ORDER BY lowest_modified_value;

\echo 'Pending ranges after refresh:'
SELECT * FROM public.cagg_pending_ranges;

\echo 'CAgg state after first refresh (Mar 1 - Mar 5):'
SELECT bucket, sensor_id, reading_count
FROM sensor_hourly_avg
WHERE bucket >= '2026-03-01 00:00:00+00' AND bucket < '2026-03-05 00:00:00+00'
ORDER BY bucket, sensor_id
LIMIT 10;

\echo '\n=== Generated refresh + delete commands after failed refresh ==='
SELECT * FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg');

-- ============================================================
-- Test 5: Test force refresh
-- ============================================================
\echo '\n=== Test 5: Test force refresh ==='

DELETE FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

-- Insert a pending range and do a force refresh
INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_ranges
    (materialization_id, lowest_modified_value, greatest_modified_value)
SELECT mat_hypertable_id,
       EXTRACT(EPOCH FROM '2026-03-01 00:00:00+00'::timestamptz)::bigint * 1000000,
       EXTRACT(EPOCH FROM '2026-03-05 00:00:00+00'::timestamptz)::bigint * 1000000
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'sensor_hourly_avg';

\echo 'Pending ranges before refresh:'
SELECT * FROM public.cagg_pending_ranges;

\echo 'CAgg state before refresh (Mar 1 - Mar 5):'
SELECT bucket, sensor_id, reading_count
FROM sensor_hourly_avg
WHERE bucket >= '2026-03-01 00:00:00+00' AND bucket < '2026-03-05 00:00:00+00'
ORDER BY bucket, sensor_id
LIMIT 10;

\echo '\n=== Generated refresh + delete commands ==='
SELECT * FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg');

-- Run the generated refresh command : ERRORs out
-- this fails due to pending ranges
-- This shouldn't fail post 2.26.0 release with the removal of the ranges table
\echo '\n=== Running force refresh command for expected generated statement ==='
CALL refresh_continuous_aggregate('public.sensor_hourly_avg', '2026-03-01 00:00:00+00'::timestamptz, '2026-03-05 00:00:00+00'::timestamptz, force=>true);

-- Refresh entries
SELECT * FROM public.cagg_pending_ranges;
