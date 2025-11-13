-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE and LICENSE for copyright and licensing information.

-- collection of diagnostic checks for TimescaleDB

CREATE OR REPLACE FUNCTION check_deprecated_features() RETURNS void LANGUAGE plpgsql AS
$$
BEGIN
  -- check for hypertables with hypercore access method
  PERFORM FROM pg_class c join pg_am am ON c.relam=am.oid AND am.amname='hypercore' LIMIT 1;
  IF FOUND THEN
    RAISE WARNING 'Found relations using the deprecated hypercore access method.';
  END IF;
  -- check for continuous aggregates using non-finalized form
  PERFORM FROM _timescaledb_catalog.continuous_agg WHERE NOT finalized;
  IF FOUND THEN
    RAISE WARNING 'Found continuous aggregates using non-finalized form.';
  END IF;
END
$$ SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION check_job_failures() RETURNS void LANGUAGE plpgsql AS
$$
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
      RAISE WARNING '  Job % had % failures', v_job_id, v_job_name, v_count;
    END LOOP;
  END IF;

END
$$ SET search_path = pg_catalog, pg_temp;


CREATE OR REPLACE FUNCTION check_compressed_chunk_batch_sizes() RETURNS void LANGUAGE plpgsql AS
$$
DECLARE
    v_hypertable_id int;
    v_hypertable regclass;
    v_chunk regclass;
    v_compressed_chunk regclass;
    v_batch_sub100 int8;
    v_batch_sub100_pct float;
    v_batch_sub100_avg float;
    v_batch_total	int8;
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
$$ SET search_path = pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION run_checks() RETURNS void LANGUAGE plpgsql AS
$$
BEGIN
  PERFORM check_deprecated_features();
  PERFORM check_job_failures();
  PERFORM check_compressed_chunk_batch_sizes();
END
$$;

-- to support installing in a non-default schema, set search_path for the main function to current schema
DO $$
BEGIN
  PERFORM format('ALTER FUNCTION run_checks() SET search_path to pg_catalog, %I, pg_temp', current_schema());
END
$$;

SELECT run_checks();

