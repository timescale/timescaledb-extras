
DROP PROCEDURE IF EXISTS _timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner;
CREATE PROCEDURE _timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner (
    job_id int,
    config jsonb
) LANGUAGE plpgsql AS $BODY$
DECLARE
    max_runtime interval := (config->>'max_runtime')::interval;
    global_start_time timestamptz := pg_catalog.clock_timestamp();
    global_end_time timestamptz;
    app_name text;
BEGIN
    max_runtime := coalesce(max_runtime, interval '6 hours');
    global_end_time := global_start_time + max_runtime;

    WHILE pg_catalog.clock_timestamp() < global_end_time LOOP
        SET search_path TO 'pg_catalog,pg_temp';
        SET lock_timeout TO '3s';
        SET application_name TO 'cagg incremental refresh consumer - idle';

        -- Prevent a hot loop
        PERFORM pg_catalog.pg_sleep(1.0);

        -- By serializing the picking items from the queue, we prevent some race conditions.
        LOCK TABLE _timescaledb_additional.incremental_continuous_aggregate_refreshes
            IN ACCESS EXCLUSIVE MODE;

        SET application_name TO 'cagg incremental refresh consumer - retrieving new task';

        DECLARE
            p_id bigint;
            p_cagg regclass;
            p_window_start timestamptz;
            p_window_end timestamptz;
            p_start_time timestamptz;
            p_end_time timestamptz;
            p_mat_hypertable_id int;
            p_job_id int;
        BEGIN
            SELECT
                q.id,
                q.continuous_aggregate,
                q.window_start,
                q.window_end,
                cagg.mat_hypertable_id,
                coalesce(jobs.job_id, -1)
            INTO
                p_id,
                p_cagg,
                p_window_start,
                p_window_end,
                p_mat_hypertable_id,
                p_job_id
            FROM
                _timescaledb_additional.incremental_continuous_aggregate_refreshes AS q
            JOIN
                pg_catalog.pg_class AS pc ON (q.continuous_aggregate=oid)
            JOIN
                pg_catalog.pg_namespace AS pn ON (relnamespace=pn.oid)
            JOIN
                _timescaledb_catalog.continuous_agg AS cagg ON (cagg.user_view_schema=nspname AND cagg.user_view_name=pc.relname)
            JOIN
                _timescaledb_catalog.hypertable AS h ON (cagg.mat_hypertable_id=h.id)
            LEFT JOIN
                timescaledb_information.jobs ON (proc_name='policy_refresh_continuous_aggregate' AND proc_schema='_timescaledb_functions' AND jobs.config->>'mat_hypertable_id' = cagg.mat_hypertable_id::text)
            WHERE
                q.worker_pid IS NULL AND q.finished IS NULL
                -- We don't want multiple workers to be active on the same CAgg,
                AND NOT EXISTS (
                    SELECT
                    FROM
                        _timescaledb_additional.incremental_continuous_aggregate_refreshes AS a
                    JOIN
                        pg_catalog.pg_stat_activity ON (pid=worker_pid)
                    WHERE
                        a.finished IS NULL
                        -- If pids ever get recycled (container/machine restart),
                        -- this filter ensures we ignore the old ones
                        AND started > backend_start
                        AND q.continuous_aggregate = a.continuous_aggregate
                )
            ORDER BY
                q.priority ASC,
                q.scheduled ASC
            FOR NO KEY UPDATE OF q SKIP LOCKED
            LIMIT
                1;

            IF p_cagg IS NULL THEN
                COMMIT;
                -- There are no items in the queue that we can currently process. We therefore
                -- sleep a while longer before attempting to try again.
                IF global_end_time - interval '30 seconds' < now () THEN
                    EXIT;
                ELSE
                    SET application_name TO 'cagg incremental refresh consumer - waiting for next task';
                    PERFORM pg_catalog.pg_sleep(0.1);
                    CONTINUE;
                END IF;                
            END IF;

            UPDATE
                _timescaledb_additional.incremental_continuous_aggregate_refreshes
            SET
                worker_pid = pg_backend_pid(),
                started = clock_timestamp()
            WHERE
                id = p_id;

            -- Inform others of what we are doing.
            app_name := ' refresh ' || p_window_start::date;
            IF p_window_end::date != p_window_start::date THEN
                app_name := app_name || ' ' || p_window_end::date;
            ELSE
                app_name := app_name || to_char(p_window_start, 'THH24:MI');
            END IF;
            IF length(app_name) + length(p_cagg::text) > 63 THEN
                app_name := '...' || right(p_cagg::text, 60 - length(app_name)) || app_name;
            ELSE
                app_name := p_cagg::text || app_name;
            END IF;
            PERFORM pg_catalog.set_config(
                'application_name',
                app_name,
                false
            );

            RAISE NOTICE
                '% - Processing %, (% - %)',
                pg_catalog.to_char(pg_catalog.clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS.FF3OF'),
                p_cagg,
                p_window_start,
                p_window_end;
            
            -- We need to ensure that all other workers now know we are working on this
            -- task. We therefore need to commit once now. This also releases our
            -- access exclusive lock on the queue table.
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

            CALL _timescaledb_functions.policy_refresh_continuous_aggregate(
                -1,
                config => jsonb_build_object(
                    'end_offset', (clock_timestamp() - p_window_end)::interval(0),
                    'start_offset', (clock_timestamp() - p_window_start)::interval(0),
                    'mat_hypertable_id', p_mat_hypertable_id
                )
            );

            UPDATE
                _timescaledb_additional.incremental_continuous_aggregate_refreshes
            SET
                finished = clock_timestamp()
            WHERE
                id = p_id;
            COMMIT;

            SET application_name TO 'cagg incremental refresh consumer - idle';
        END;
    END LOOP;

    RAISE NOTICE 'Shutting down worker, as we exceeded our maximum runtime (%)', max_runtime;
END;
$BODY$;

GRANT EXECUTE ON PROCEDURE _timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner TO pg_database_owner;
