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


DROP PROCEDURE IF EXISTS _timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner;
CREATE OR REPLACE PROCEDURE _timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner (
    job_id int,
    config jsonb
) LANGUAGE plpgsql AS $BODY$
DECLARE
    max_runtime interval := (config->>'max_runtime')::interval;
    enable_tiered boolean := (config->>'enable_tiered_reads')::boolean;
    old_enable_tiered_reads boolean;
    global_start_time timestamptz := pg_catalog.clock_timestamp();
    global_end_time timestamptz;
BEGIN
    old_enable_tiered_reads := current_setting('timescaledb.enable_tiered_reads')::boolean;

    IF enable_tiered IS NOT NULL THEN
        IF enable_tiered THEN
            SET timescaledb.enable_tiered_reads = 'on';
        ELSE
            SET timescaledb.enable_tiered_reads = 'off';
        END IF;
    END IF;

    max_runtime := coalesce(max_runtime, interval '6 hours');
    global_end_time := global_start_time + max_runtime;

    WHILE pg_catalog.clock_timestamp() < global_end_time LOOP
        SET LOCAL lock_timeout TO '1 min';

        -- Prevent a hot loop
        PERFORM pg_catalog.pg_sleep(0.2);

        DECLARE
            p_id bigint;
            p_cagg regclass;
            p_window_start timestamptz;
            p_window_end timestamptz;
            p_start_time timestamptz;
            p_end_time timestamptz;
        BEGIN
            SELECT
                q.id,
                q.continuous_aggregate,
                q.window_start,
                q.window_end
            INTO
                p_id,
                p_cagg,
                p_window_start,
                p_window_end
            FROM
                _timescaledb_additional.incremental_continuous_aggregate_refreshes AS q
            WHERE
                q.worker_pid IS NULL AND q.finished IS NULL
                -- We don't want multiple workers to be active on the same range,
                -- as ranges can differ in size, we'll use the overlap (&&) operator
                -- to ensure we're good.
                AND NOT EXISTS (
                    SELECT
                    FROM
                        _timescaledb_additional.incremental_continuous_aggregate_refreshes AS a
                    WHERE
                        a.worker_pid IS NOT NULL
                        AND a.finished IS NOT NULL
                        AND q.continuous_aggregate = a.continuous_aggregate
                        AND tstzrange(q.window_start, q.window_end, '[)') && tstzrange(a.window_start, a.window_end, '[)')
                )
            ORDER BY
                q.priority ASC,
                q.scheduled ASC
            FOR NO KEY UPDATE SKIP LOCKED
            LIMIT
                1;

            IF p_cagg IS NULL THEN
                COMMIT;
                -- There are no items in the queue that we can currently process. We therefore
                -- sleep a while before continuing.
                -- PERFORM pg_catalog.pg_sleep(3.0);
                -- CONTINUE;
                EXIT;
            END IF;

            UPDATE
                _timescaledb_additional.incremental_continuous_aggregate_refreshes
            SET
                worker_pid = pg_backend_pid(),
                started = clock_timestamp()
            WHERE
                id = p_id;
            -- We need to ensure that all other workers now know we are working on this
            -- task. We therefore need to commit once now.
            COMMIT;

            -- We take out a row-level-lock to signal to concurrent workers that *we*
            -- are working on it. By taking this type of lock, we can clean up
            -- this table from different tasks: They can update/delete these rows
            -- if no active worker is working on them, and no lock is established.
            PERFORM
            FROM
                _timescaledb_additional.incremental_continuous_aggregate_refreshes
            WHERE
                id = p_id
            FOR NO KEY UPDATE;

            CALL public.refresh_continuous_aggregate(
                p_cagg,
                p_window_start,
                p_window_end
            );

            UPDATE
                _timescaledb_additional.incremental_continuous_aggregate_refreshes
            SET
                finished = clock_timestamp()
            WHERE
                id = p_id;
            COMMIT;

            RAISE NOTICE
                '% - Processing %, (% - %)',
                pg_catalog.to_char(pg_catalog.clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3OF'),
                p_cagg,
                p_window_start,
                p_window_end;
        END;
    END LOOP;

    IF old_enable_tiered_reads THEN
        SET timescaledb.enable_tiered_reads = 'on';
    ELSE
        SET timescaledb.enable_tiered_reads = 'off';
    END IF;

    RAISE NOTICE 'Shutting down `task_refresh_continuous_aggregate_incremental_runner`';
END;
$BODY$;

