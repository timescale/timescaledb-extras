CREATE SCHEMA IF NOT EXISTS _timescaledb_additional;

CREATE TABLE IF NOT EXISTS _timescaledb_additional.incremental_continuous_aggregate_refreshes (
    id bigint GENERATED ALWAYS AS IDENTITY,
    continuous_aggregate regclass not null,
    window_start timestamptz not null,
    window_end timestamptz not null CHECK (window_end > window_start),
    scheduled timestamptz not null default pg_catalog.clock_timestamp(),
    priority int not null default 1,
    started timestamptz,
    finished timestamptz,
    worker_pid integer,
    primary key (id),
    CONSTRAINT incr_cagg_refreshes_workers_have_started CHECK (num_nulls(worker_pid, started) IN (0, 2))
);

GRANT USAGE ON SCHEMA _timescaledb_additional TO public;
REVOKE ALL ON TABLE _timescaledb_additional.incremental_continuous_aggregate_refreshes FROM PUBLIC;
GRANT SELECT ON TABLE _timescaledb_additional.incremental_continuous_aggregate_refreshes TO public;
GRANT ALL ON TABLE _timescaledb_additional.incremental_continuous_aggregate_refreshes TO pg_database_owner;

COMMENT ON COLUMN _timescaledb_additional.incremental_continuous_aggregate_refreshes.worker_pid IS
$$This column will be populated with the pid that is currently running this task.
This allows us to keep track of things, as well as allow us to reschedule an item if
a worker_pid is no longer active (for whatever reason)$$;

COMMENT ON COLUMN _timescaledb_additional.incremental_continuous_aggregate_refreshes.scheduled IS
$$To ensure we do actually get to do all the work, the workers will always pick up the
task that has the lowest priority, and then which one was scheduled first.
In that way, we have a bit of a priority queue.$$;

-- We want to avoid scheduling the same thing twice, for those tasks that have not yet been
-- picked up by any worker.
CREATE UNIQUE INDEX IF NOT EXISTS incr_cagg_refreshes_distinct_tasks_unq ON _timescaledb_additional.incremental_continuous_aggregate_refreshes(
    continuous_aggregate,
    window_start,
    window_end
) WHERE worker_pid IS NULL AND finished IS NULL;

CREATE INDEX IF NOT EXISTS incr_cagg_refreshes_find_first_work_item_idx ON _timescaledb_additional.incremental_continuous_aggregate_refreshes(
    priority,
    scheduled
) WHERE worker_pid IS NULL;

CREATE INDEX IF NOT EXISTS incr_cagg_refreshes_active_workers_idx ON _timescaledb_additional.incremental_continuous_aggregate_refreshes(
    worker_pid
) WHERE worker_pid IS NOT NULL;

-- Calculate the time bucket using the continuous aggregate bucket function
-- configuration
CREATE OR REPLACE FUNCTION _timescaledb_additional.cagg_time_bucket(INTEGER, TIMESTAMPTZ)
RETURNS TIMESTAMPTZ AS
$$
DECLARE
  params TEXT[];
  stmt TEXT;
  r RECORD;
  result TIMESTAMPTZ;
BEGIN
  SELECT * INTO r FROM _timescaledb_catalog.continuous_aggs_bucket_function WHERE mat_hypertable_id = $1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Continuous Aggregate % not found', $1;
  END IF;

  params := array_append(params, format('%I => %L::timestamptz', 'ts', $2));

  IF r.bucket_width IS NOT NULL THEN
    params := array_append(params, format('%I => %L::interval', 'bucket_width', r.bucket_width));
  END IF;

  IF r.bucket_origin IS NOT NULL THEN
    params := array_append(params, format('%I => %L::timestamptz', 'origin', r.bucket_origin));
  END IF;

  IF r.bucket_offset IS NOT NULL THEN
    params := array_append(params, format('%I => %L::interval', 'offset', r.bucket_offset));
  END IF;

  IF r.bucket_timezone IS NOT NULL THEN
    params := array_append(params, format('%I => %L::text', 'timezone', r.bucket_timezone));
  END IF;

  stmt := format('SELECT time_bucket(%s)', array_to_string(params, ', '));
  RAISE DEBUG '%', stmt;

  EXECUTE stmt
  INTO result;

  RETURN result;
END;
$$
LANGUAGE plpgsql STABLE;

-- Discover continuous aggregates built on top of tiered hypertables and
-- schedule their refresh
-- CREATE OR REPLACE PROCEDURE _timescaledb_additional.schedule_osm_cagg_refresh(
--     name_mask TEXT DEFAULT '%',
--     nbuckets INTEGER DEFAULT 5,
--     dry_run BOOLEAN DEFAULT true,
--     priority INTEGER DEFAULT 100
-- ) AS
-- $$
-- BEGIN
--     -- Find caggs built on top of tiered hypertables
--     WITH ranges AS (
--         SELECT
--             cagg.mat_hypertable_id,
--             ht.schema_name,
--             ht.table_name,
--             (
--                 SELECT column_type
--                 FROM _timescaledb_catalog.dimension d
--                 WHERE d.hypertable_id = ht.id
--                 ORDER BY d.id ASC LIMIT 1
--             ) AS dim_type,
--             user_view_schema,
--             user_view_name,
--             bf.bucket_width::interval AS bucket_width,
--             _timescaledb_additional.cagg_time_bucket(mat_hypertable_id, range.start) AS global_start,
--             _timescaledb_additional.cagg_time_bucket(mat_hypertable_id, range.end) + (bf.bucket_width::interval + '1 millisecond'::interval) AS global_end
--         FROM _timescaledb_catalog.continuous_agg cagg
--         JOIN _timescaledb_catalog.continuous_aggs_bucket_function bf USING (mat_hypertable_id)
--         JOIN _timescaledb_catalog.hypertable ht ON (ht.id = cagg.raw_hypertable_id)
--         JOIN _osm_catalog.table_map tm ON (tm.hypertable_name = ht.table_name AND tm.hypertable_schema = ht.schema_name)
--         JOIN (
--             -- the time window of tiered data
--             SELECT
--                 osm_table_id,
--                 _osm_internal.dimension_pg_usec_to_timestamp(min(range_start)) as start,
--                 _osm_internal.dimension_pg_usec_to_timestamp(max(range_end)) as end
--             FROM _osm_catalog.chunk_map
--             JOIN _osm_catalog.chunk_object_map USING (chunk_id)
--             GROUP BY osm_table_id
--         ) AS range USING (osm_table_id)
--         WHERE user_view_name LIKE name_mask
--     )
--     INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
--     SELECT
--         ranges.mat_hypertable_id,
--         (extract(epoch from global_start) * 1000000)::bigint AS invalidation_start,
--         (extract(epoch from global_end) * 1000000)::bigint AS invalidation_end
--     FROM
--         ranges
--     WHERE
--         dim_type = 'TIMESTAMPTZ'::REGTYPE;

--     WITH ranges AS (
--         SELECT
--             cagg.mat_hypertable_id,
--             ht.schema_name,
--             ht.table_name,
--             (
--                 SELECT column_type AS dim_type
--                 FROM _timescaledb_catalog.dimension d
--                 WHERE d.hypertable_id = ht.id
--                 ORDER BY d.id ASC LIMIT 1
--             ) AS dim_type,
--             user_view_schema,
--             user_view_name,
--             bf.bucket_width::interval AS bucket_width,
--             _timescaledb_additional.cagg_time_bucket(mat_hypertable_id, range.start) AS global_start,
--             _timescaledb_additional.cagg_time_bucket(mat_hypertable_id, range.end) + (bf.bucket_width::interval + '1 millisecond'::interval) AS global_end
--         FROM _timescaledb_catalog.continuous_agg cagg
--         JOIN _timescaledb_catalog.continuous_aggs_bucket_function bf USING (mat_hypertable_id)
--         JOIN _timescaledb_catalog.hypertable ht ON (ht.id = cagg.raw_hypertable_id)
--         JOIN _osm_catalog.table_map tm ON (tm.hypertable_name = ht.table_name AND tm.hypertable_schema = ht.schema_name)
--         JOIN (
--             -- the time window of tiered data
--             SELECT
--                 osm_table_id,
--                 _osm_internal.dimension_pg_usec_to_timestamp(min(range_start)) as start,
--                 _osm_internal.dimension_pg_usec_to_timestamp(max(range_end)) as end
--             FROM _osm_catalog.chunk_map
--             JOIN _osm_catalog.chunk_object_map USING (chunk_id)
--             GROUP BY osm_table_id
--         ) AS range USING (osm_table_id)
--         WHERE user_view_name LIKE name_mask
--     )
--     -- schedule the refresh for given interval
--     INSERT INTO _timescaledb_additional.incremental_continuous_aggregate_refreshes
--         (continuous_aggregate, window_start, window_end, priority)
--     SELECT
--         format('%I.%I', ranges.user_view_schema, ranges.user_view_name)::regclass,
--         start AS window_start,
--         start + (ranges.bucket_width * nbuckets) AS window_end,
--         priority
--     FROM
--         ranges
--         -- Split ranges with 5 times the bucket width
--         JOIN LATERAL generate_series(ranges.global_start, ranges.global_end, (bucket_width * nbuckets)) AS start ON true
--         -- JOIN timescaledb_osm.tiered_chunks ch
--         --     ON (ranges.schema_name, ranges.table_name) = (ch.hypertable_schema, ch.hypertable_name)
--         --     AND tstzrange(start, (start + (ranges.bucket_width * nbuckets)), '[)') && tstzrange(ch.range_start, ch.range_end, '[)')

--     WHERE
--         dim_type = 'TIMESTAMPTZ'::REGTYPE;

-- END
-- $$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE _timescaledb_additional.schedule_osm_cagg_refresh(
    name_mask TEXT DEFAULT '%',
    nbuckets INTEGER DEFAULT 5,
    dry_run BOOLEAN DEFAULT true,
    priority INTEGER DEFAULT 100
) AS
$$
BEGIN

    -- WITH RECURSIVE caggs AS (
    -- SELECT mat_hypertable_id, parent_mat_hypertable_id, user_view_name
    -- FROM _timescaledb_catalog.continuous_agg
    -- WHERE user_view_name = 'metrics_by_week'
    -- UNION ALL
    -- SELECT continuous_agg.mat_hypertable_id, continuous_agg.parent_mat_hypertable_id, continuous_agg.user_view_name
    -- FROM _timescaledb_catalog.continuous_agg
    -- JOIN caggs ON caggs.parent_mat_hypertable_id = continuous_agg.mat_hypertable_id
    -- )
    -- SELECT * FROM caggs ORDER BY mat_hypertable_id;

    -- WITH RECURSIVE caggs AS (
    --     SELECT
    --         cagg.mat_hypertable_id,
    --         cagg.parent_mat_hypertable_id,
    --         bf.bucket_width::interval AS bucket_width,
    --         tch.range_start,
    --         tch.range_end
    --     FROM
    --         timescaledb_osm.tiered_chunks tch
    --         JOIN _timescaledb_catalog.hypertable ht ON (tch.hypertable_name = ht.table_name AND tch.hypertable_schema = ht.schema_name)
    --         JOIN _timescaledb_catalog.continuous_agg cagg ON (cagg.raw_hypertable_id = ht.id)
    --         JOIN _timescaledb_catalog.continuous_aggs_bucket_function bf USING (mat_hypertable_id)
    --     WHERE cagg.parent_mat_hypertable_id IS NULL
    --     UNION ALL
    --     SELECT
    --         cagg.mat_hypertable_id,
    --         cagg.parent_mat_hypertable_id,
    --         bf.bucket_width::interval AS bucket_width,
    --         caggs.range_start,
    --         caggs.range_end
    --     FROM
    --         caggs
    --         JOIN _timescaledb_catalog.continuous_agg cagg ON (cagg.parent_mat_hypertable_id = caggs.mat_hypertable_id)
    --         JOIN _timescaledb_catalog.continuous_aggs_bucket_function bf ON (bf.mat_hypertable_id = cagg.mat_hypertable_id)
    -- )
    -- SELECT * FROM caggs;


    -- Find caggs built on top of tiered hypertables
    INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
    SELECT
        mat_hypertable_id,
        ((extract(epoch from _timescaledb_additional.cagg_time_bucket(cagg.mat_hypertable_id, MIN(range_start)))) * 1000000)::bigint AS invalidation_start,
        ((extract(epoch from _timescaledb_additional.cagg_time_bucket(cagg.mat_hypertable_id, MAX(range_end)) + (bf.bucket_width::interval + interval '1 millisecond')))* 1000000)::bigint AS invalidation_end
    FROM
        timescaledb_osm.tiered_chunks tch
        JOIN _timescaledb_catalog.hypertable ht ON (tch.hypertable_name = ht.table_name AND tch.hypertable_schema = ht.schema_name)
        JOIN _timescaledb_catalog.continuous_agg cagg ON (cagg.raw_hypertable_id = ht.id)
        JOIN _timescaledb_catalog.continuous_aggs_bucket_function bf USING (mat_hypertable_id)
    WHERE
        user_view_name LIKE name_mask
    GROUP BY
        mat_hypertable_id, bf.bucket_width;

    -- schedule the refresh for given interval
    INSERT INTO _timescaledb_additional.incremental_continuous_aggregate_refreshes
        (continuous_aggregate, window_start, window_end, priority)
    SELECT
        format('%I.%I', cagg.user_view_schema, cagg.user_view_name)::regclass,
        _timescaledb_additional.cagg_time_bucket(cagg.mat_hypertable_id, range_start) AS window_start,
        _timescaledb_additional.cagg_time_bucket(cagg.mat_hypertable_id, range_end) + (bf.bucket_width::interval + interval '1 millisecond') AS window_end,
        priority
    FROM
        timescaledb_osm.tiered_chunks tch
        JOIN _timescaledb_catalog.hypertable ht ON (tch.hypertable_name = ht.table_name AND tch.hypertable_schema = ht.schema_name)
        JOIN _timescaledb_catalog.continuous_agg cagg ON (cagg.raw_hypertable_id = ht.id)
        JOIN _timescaledb_catalog.continuous_aggs_bucket_function bf USING (mat_hypertable_id)
    WHERE
        user_view_name LIKE name_mask
    ORDER BY
        range_start;
END
$$ LANGUAGE plpgsql;
