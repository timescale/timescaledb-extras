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

DROP PROCEDURE IF EXISTS _timescaledb_additional.schedule_refresh_continuous_aggregate_incremental;
CREATE PROCEDURE _timescaledb_additional.schedule_refresh_continuous_aggregate_incremental (
    job_id int,
    config jsonb
) LANGUAGE plpgsql AS $BODY$
DECLARE
    cagg_regclass regclass := (config ->> 'continuous_aggregate')::regclass;
    start_offset INTERVAL;
    end_offset INTERVAL  := (config ->> 'end_offset')::INTERVAL;
    increment_size INTERVAL;
    priority int := coalesce((config ->> 'priority')::integer, 100);
BEGIN
    IF pg_catalog.num_nulls(cagg_regclass, end_offset) > 0 THEN
        RAISE EXCEPTION 'Invalid configuration for scheduling an incremental refresh: %', config;
    END IF;

    -- We gather some data on the CAgg itself, its name, and its oid,
    -- as well as the size of the increment if it wasn't specified
    SELECT
        -- We default to the dimension interval_length if not explicitly specified
        coalesce(increment_size, interval_length * interval '1 microsecond', '1 hour'),
        -- And we default to the known watermark
        coalesce(start_offset, now() - _timescaledb_functions.to_timestamp(watermark))
    INTO
        increment_size,
        start_offset
    FROM
        _timescaledb_catalog.continuous_agg AS cagg
    JOIN
        _timescaledb_catalog.hypertable AS h ON (h.id = raw_hypertable_id)
    JOIN
        _timescaledb_catalog.dimension AS dim ON (h.id = dim.hypertable_id)
    LEFT JOIN
        _timescaledb_catalog.continuous_aggs_watermark AS cw ON (cw.mat_hypertable_id = cagg.mat_hypertable_id)
    WHERE
        format('%I.%I', user_view_schema, user_view_name)::regclass = cagg_regclass
    -- If there are multiple dimensions, we only want the first one
    ORDER BY
        dim.id ASC
    LIMIT
        1;

    -- If explicitly configured, those values take precedent.
    increment_size := coalesce((config ->> 'increment_size')::INTERVAL, increment_size, '1 hour');
    start_offset := coalesce((config ->> 'start_offset')::INTERVAL, start_offset, '1 year');

    -- Remove stale values
    WITH stale AS (
        SELECT
            id
        FROM
            _timescaledb_additional.incremental_continuous_aggregate_refreshes
        WHERE
            worker_pid IS NOT NULL
            AND finished IS NULL
            -- There is a small chance for a race condition between a consumer and
            -- this producer. The consumer will very quickly take out a row level
            -- lock after an intermediate commit (first statement after that commit),
            -- but it may not have it *yet*.
            -- By adding this filter, we should prevent these
            -- rows from being marked as stale.
            AND started > pg_catalog.clock_timestamp() - interval '3 seconds'
            AND NOT EXISTS (
                SELECT
                FROM
                    pg_stat_activity
                WHERE
                    pid = worker_pid
            )
        FOR UPDATE SKIP LOCKED
    )
    DELETE
    FROM
        _timescaledb_additional.incremental_continuous_aggregate_refreshes AS q
    USING
        stale AS s
    WHERE
        s.id = q.id;

    DECLARE
        start_t timestamptz := now() - start_offset;
        end_t   timestamptz := now() - end_offset;
        
        incr_end   timestamptz := public.time_bucket(increment_size, now() - end_offset);
        incr_start timestamptz := incr_end;

        count bigint := 0;
        added bigint := 0;
        hit bool := false;
    BEGIN
        WHILE incr_start >= start_t
        LOOP
            incr_start := public.time_bucket(increment_size, incr_end - increment_size);

            INSERT INTO _timescaledb_additional.incremental_continuous_aggregate_refreshes
                (continuous_aggregate, window_start, window_end, priority)
            VALUES
                (cagg_regclass, incr_start, incr_end, priority)
            ON CONFLICT
                DO NOTHING
            RETURNING
                true
            INTO
                hit;

            count := count + 1;
            IF hit THEN
                added := added + 1;
            END IF;

            incr_end := incr_start;
        END LOOP;

        RAISE NOTICE
            E'Scheduled incremental refreshes for % (% - %). Tasks evaluated: %, newly inserted: %.\nStart offset: %, end offset: %, increment: %',
            cagg_regclass::text,
            start_t,
            end_t,
            count,
            added,
            start_offset,
            end_offset,
            increment_size;
    END;
END;
$BODY$;

GRANT EXECUTE ON PROCEDURE _timescaledb_additional.schedule_refresh_continuous_aggregate_incremental TO pg_database_owner;

COMMENT ON PROCEDURE _timescaledb_additional.schedule_refresh_continuous_aggregate_incremental IS
$$schedule_refresh_continuous_aggregate_incremental is a pretty non-intelligent procedure.
For the provided continuous aggregate it will write records into this table:
    _timescaledb_additional.incremental_continuous_aggregate_refreshes
Which will then be tasks picked up by task_refresh_continuous_aggregate_incremental_runner$$;
