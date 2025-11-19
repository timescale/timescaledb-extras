-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE and LICENSE for copyright and licensing information.

-- collection of diagnostic checks for TimescaleDB
--
-- Checks included:
-- - deprecated features
--    - hypercore access method
--    - continuous aggregates non-finalized form
-- - catalog corruption
--   - chunk_column_stats with range_start > range_end
--   - orphaned chunks
-- - scheduler checks
--   - launcher running
--   - exactly 1 scheduler running in current database
--   - job failures
-- - compression
--   - compressed batch sizes
-- - continuous aggregates
--   - continuous aggregate large materialization ranges
--   - continuous aggregate chunk interval vs bucket width

SET search_path TO pg_catalog, pg_temp;

-- deprecated features
DO $$
BEGIN
  -- check for hypertables with hypercore access method
  PERFORM FROM pg_class c join pg_am am ON c.relam=am.oid AND am.amname='hypercore' LIMIT 1;
  IF FOUND THEN
    RAISE WARNING 'Found relations using the deprecated hypercore access method.';
  END IF;

  -- check for continuous aggregates using non-finalized form
  PERFORM FROM _timescaledb_catalog.continuous_agg WHERE NOT finalized;
  IF FOUND THEN
    RAISE WARNING 'Found continuous aggregates using deprecated non-finalized form.';
  END IF;
END
$$;

-- catalog corruption checks
DO $$
DECLARE
  v_count int8;
  v_relnames text[];
  v_rels regclass[];
BEGIN
  -- corrupt _timescaledb_catalog.chunk_column_stats entries
  SELECT count(*) INTO v_count FROM _timescaledb_catalog.chunk_column_stats WHERE range_start > range_end;
  IF v_count >= 1 THEN
    RAISE WARNING 'Found %s entries in _timescaledb_catalog.chunk_column_stats with range_start > range_end', v_count;
  END IF;

  -- chunks with missing relations
  SELECT count(*), array_agg(format('%I.%I',ch.schema_name,ch.table_name)) INTO v_count, v_relnames
  FROM _timescaledb_catalog.chunk ch WHERE
    NOT dropped AND
    NOT EXISTS (SELECT FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relname=ch.table_name AND n.nspname = ch.schema_name);
  IF v_count > 0 THEN
    RAISE WARNING 'Found % chunk entries without relations: %', v_count, v_relnames[1:20];
  END IF;

  -- orphaned chunks
  SELECT count(*), array_agg(oid::regclass) INTO v_count, v_rels
  FROM pg_class
  WHERE
    relnamespace='_timescaledb_internal'::regnamespace AND
    relkind='r' AND
    relname LIKE '%_chunk' AND
    NOT EXISTS(SELECT FROM _timescaledb_catalog.chunk where schema_name='_timescaledb_internal' and table_name = relname);
  IF v_count > 0 THEN
    RAISE WARNING 'Found % orphaned chunk relations: %', v_count, v_rels[1:20];
  END IF;
END
$$;

-- scheduler checks
DO $$
DECLARE
  v_count int8;
BEGIN
  IF NOT pg_is_in_recovery() THEN
    PERFORM FROM pg_stat_activity WHERE application_name = 'TimescaleDB Background Worker Launcher';
    IF NOT FOUND THEN
      RAISE WARNING 'TimescaleDB launcher not running';
    END IF;
    PERFORM FROM pg_stat_activity WHERE application_name = 'TimescaleDB Background Worker Scheduler' AND datname = current_database();
    IF NOT FOUND THEN
      RAISE WARNING 'TimescaleDB scheduler not running in current database';
    END IF;
    SELECT count(*) INTO v_count FROM pg_stat_activity WHERE application_name = 'TimescaleDB Background Worker Scheduler' AND datname = current_database();
    IF v_count > 1 THEN
      RAISE WARNING 'Multiple TimescaleDB scheduler (%) running in current database', v_count;
    END IF;
  END IF;
END
$$;

-- job failure checks
DO $$
DECLARE
  v_failed int;
  v_total int;
  v_failed_distinct int;
  v_job_id int;
  v_count int;
  v_job_name text;
BEGIN
  -- check for job failures in the last 7 days
  SELECT
    count(*) FILTER (WHERE NOT succeeded) AS failed,
    count(succeeded) AS total,
    count(DISTINCT job_id) FILTER (WHERE NOT succeeded) failed_distinct
  INTO v_failed, v_total, v_failed_distinct
  FROM _timescaledb_internal.bgw_job_stat_history
  WHERE execution_start > now() - '7 day'::interval;
  IF v_failed > 0 THEN
    RAISE WARNING '%/% job executions of % distinct jobs failed in last 7 days', v_failed, v_total, v_failed_distinct;
    FOR v_job_id, v_count, v_job_name IN
      SELECT job_id, count(*) AS count, (SELECT application_name FROM _timescaledb_config.bgw_job WHERE id = job_id) AS job_name
      FROM _timescaledb_internal.bgw_job_stat_history
      WHERE execution_start > now() - '7 day'::interval AND NOT succeeded
      GROUP BY job_id
      ORDER BY count DESC
      LIMIT 5
    LOOP
      RAISE WARNING '  Job % had % failures', v_job_name, v_count;
    END LOOP;
  END IF;
END
$$;

-- continuous aggregate checks
-- continuous aggregates with large materialization ranges
DO $$
DECLARE
  v_cagg regclass;
  v_range text;
BEGIN
  FOR v_cagg, v_range IN
    SELECT
      format('%I.%I', c.user_view_schema, c.user_view_name)::regclass AS continuous_aggregate,
      CASE
        WHEN d.column_type =ANY('{int4,int8}'::regtype[]) THEN (r.greatest_modified_value - r.lowest_modified_value)::text
        WHEN d.column_type =ANY('{timestamp,timestamptz}'::regtype[]) THEN _timescaledb_functions.to_interval(r.greatest_modified_value - r.lowest_modified_value)::text
      END AS range
    FROM _timescaledb_catalog.continuous_aggs_materialization_ranges r
    JOIN _timescaledb_catalog.continuous_agg c ON c.mat_hypertable_id=r.materialization_id
    JOIN _timescaledb_catalog.continuous_aggs_bucket_function f on f.mat_hypertable_id=c.mat_hypertable_id
    JOIN _timescaledb_catalog.dimension d ON d.hypertable_id=c.mat_hypertable_id
    WHERE
    CASE
      WHEN d.column_type =ANY('{int4,int8}'::regtype[]) THEN (r.greatest_modified_value - r.lowest_modified_value) > 250 * f.bucket_width::int
      WHEN d.column_type =ANY('{timestamp,timestamptz}'::regtype[]) THEN _timescaledb_functions.to_interval(r.greatest_modified_value - r.lowest_modified_value) > 250 * f.bucket_width::interval
    END
  LOOP
    RAISE WARNING 'Continuous aggregate % has large materialization range ''%''.', v_cagg, v_range;
  END LOOP;
END
$$;

-- continuous aggregates with chunk interval smaller than bucket width
DO $$
DECLARE
  v_cagg regclass;
  v_cagg_width text;
  v_chunk_width text;
BEGIN
  FOR v_cagg, v_chunk_width, v_cagg_width IN
    SELECT
      format('%I.%I', c.user_view_schema, c.user_view_name)::regclass AS continuous_aggregate,
      CASE
        WHEN d.column_type =ANY('{int4,int8}'::regtype[]) THEN d.interval_length::text
        WHEN d.column_type =ANY('{timestamp,timestamptz}'::regtype[]) THEN _timescaledb_functions.to_interval(d.interval_length)::text
      END AS chunk_width,
      f.bucket_width cagg_width
    FROM _timescaledb_catalog.continuous_agg c
    JOIN _timescaledb_catalog.continuous_aggs_bucket_function f ON f.mat_hypertable_id=c.mat_hypertable_id
    JOIN _timescaledb_catalog.dimension d ON d.hypertable_id=c.mat_hypertable_id
    WHERE
      CASE
        WHEN d.column_type =ANY('{int4,int8}'::regtype[]) THEN d.interval_length <= f.bucket_width::int
        WHEN d.column_type =ANY('{timestamp,timestamptz}'::regtype[]) THEN _timescaledb_functions.to_interval(d.interval_length) <= f.bucket_width::interval
      END
  LOOP
    RAISE WARNING 'Continuous aggregate % has chunk width smaller than bucket width % <= %', v_cagg, v_chunk_width, v_cagg_width;
  END LOOP;
END
$$;


-- compression checks
-- compressed chunk batch sizes
DO $$
DECLARE
    v_hypertable_id int;
    v_hypertable regclass;
    v_chunk regclass;
    v_compressed_chunk regclass;
    v_batch_sub100 int8;
    v_batch_sub100_pct float;
    v_batch_sub100_avg float;
    v_batch_total  int8;
    v_chunks_sub100 int8;
    v_chunks_total int8;
    v_returned_rows int;
BEGIN
    FOR v_hypertable_id, v_hypertable IN SELECT id, format('%I.%I',schema_name, table_name)::regclass FROM _timescaledb_catalog.hypertable WHERE compressed_hypertable_id IS NOT NULL
    LOOP
        v_chunks_total := 0;
        v_chunks_sub100 := 0;

        RAISE NOTICE 'Checking hypertable %', v_hypertable;
        FOR v_chunk, v_compressed_chunk IN
          SELECT
            format('%I.%I',ch.schema_name, ch.table_name) chunk,
            format('%I.%I',ch2.schema_name, ch2.table_name) compressed
          FROM _timescaledb_catalog.chunk ch
          JOIN _timescaledb_catalog.chunk ch2 ON ch2.id = ch.compressed_chunk_id
          WHERE ch.hypertable_id = v_hypertable_id
        LOOP
            RAISE DEBUG '  Checking chunk: %', v_chunk;
            v_chunks_total := v_chunks_total + 1;

            -- check if batch has more than 20% of batches with less than 100 tuples
            EXECUTE format('
              SELECT
                count(_ts_meta_count) FILTER (WHERE _ts_meta_count < 100) batch_sub100,
                avg(_ts_meta_count) FILTER (WHERE _ts_meta_count < 100) batch_sub100_avg,
                (count(_ts_meta_count) FILTER (WHERE _ts_meta_count < 100)/count(_ts_meta_count)::float) * 100 batch_sub100_pct,
                count(_ts_meta_count) batch_total
              FROM %s HAVING (count(_ts_meta_count) FILTER (WHERE _ts_meta_count < 100)/count(_ts_meta_count)::float) > 0.2;
            ', v_compressed_chunk) INTO v_batch_sub100, v_batch_sub100_avg, v_batch_sub100_pct, v_batch_total;

            GET DIAGNOSTICS v_returned_rows = ROW_COUNT;
            IF v_returned_rows > 0 THEN
                v_chunks_sub100 := v_chunks_sub100 + 1;
                -- only print first 5 warnings to avoid flooding
                IF v_chunks_sub100 <= 5 THEN
                    RAISE WARNING '  Chunk % has %/% (% %% avg %) batches with less than 100 tuples.', v_chunk, v_batch_sub100, v_batch_total, v_batch_sub100_pct::decimal(4,1), v_batch_sub100_avg::decimal(4,1);
                END IF;
            END IF;
        END LOOP;
        IF v_chunks_sub100 > 0 THEN
            RAISE WARNING '%/% chunks found with more than 20%% of batches having less than 100 tuples in %.', v_chunks_sub100, v_chunks_total, v_hypertable;
        END IF;
    END LOOP;
END
$$;

