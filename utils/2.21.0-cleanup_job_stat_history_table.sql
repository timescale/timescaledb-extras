INSERT INTO _timescaledb_internal.bgw_job_stat_history(job_id, pid, execution_start, execution_finish, succeeded) SELECT 1, 1, t, t + interval '1 minute', random(0, 1)::boolean FROM generate_series(now() - interval '3 years', now(), interval '1 minute') AS t;

explain (analyze) select * from _timescaledb_internal.bgw_job_stat_history where succeeded is true order by execution_finish desc limit 100;

create index idx_btree on _timescaledb_internal.bgw_job_stat_history (succeeded, execution_finish desc);

create index idx_brin on _timescaledb_internal.bgw_job_stat_history using brin (execution_finish);


select member, project_id, service_id, db_id, relname, pg_size_pretty(relation_size+toast_relation_size+index_relation_size+toast_index_relation_size) as total_size, n_live_tup, n_dead_tup
from current.stat_tables
join current.ts_class using (db_id, relid)
join public.database using (db_id)
where relname = 'bgw_job_stat_history' and member ~ '-an-0$'
-- order by (relation_size+toast_relation_size+index_relation_size+toast_index_relation_size) desc
order by coalesce(n_live_tup, 0) desc
limit 30;




SELECT
    database.project_id,
    database.service_id,
    bgw_job.job_id,
    bgw_job.observed,
    bgw_job.config->>'drop_after' AS drop_after
FROM
    current.bgw_job
    JOIN public.collection
        ON collection.db_id = bgw_job.db_id
        AND collection.collected = bgw_job.observed
        AND collection.is_in_recovery IS FALSE
    JOIN public.database ON database.db_id = collection.db_id
WHERE
    bgw_job.proc_name = 'policy_job_stat_history_retention'
    AND (bgw_job.config->>'drop_after')::interval IS DISTINCT FROM '1 month'::interval;


SELECT execution_finish,'2025-05-24 12:12:57.628183-03' < execution_finish, '2025-05-24 12:12:57.628183-03' > execution_finish FROM _timescaledb_internal.bgw_job_stat_history WHERE id = 1014247703;


DO
$$
DECLARE
  id_lower BIGINT;
  id_upper BIGINT;
  id_middle BIGINT DEFAULT 0;
  split_point TIMESTAMPTZ;
  target_tz TIMESTAMPTZ;
  range_start TIMESTAMPTZ;
  range_end TIMESTAMPTZ;
  drop_after INTERVAL;
BEGIN

  SELECT now() - (config->>'drop_after')::interval, (config->>'drop_after')::interval
  INTO split_point, drop_after
  FROM _timescaledb_config.bgw_job
  WHERE id = 3;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'system job 3 (job execution history retention) not found';
    RETURN;
  END IF;

  range_start := time_bucket(drop_after, split_point);
  range_end := range_start + drop_after;

  SELECT COALESCE(min(id), 0), COALESCE(max(id), 0)
  INTO id_lower, id_upper
  FROM _timescaledb_internal.bgw_job_stat_history;

  IF id_lower = 0 AND id_upper = 0 THEN
    RAISE INFO 'no job stat history logs do process';
    RETURN;
  END IF;

  LOOP
    id_middle := ((id_upper - id_lower) / 2) + id_middle;

    SELECT execution_finish
    INTO target_tz
    FROM _timescaledb_internal.bgw_job_stat_history
    WHERE id = id_middle;

    RAISE INFO 'split_point %, range_start %, range_end %, target_tz %, id_lower %, id_upper %, id_middle %, le %, gt %',
      split_point, range_start, range_end, target_tz, id_lower, id_upper, id_middle, (target_tz <= split_point), (target_tz > split_point);

    IF NOT FOUND THEN
      RAISE INFO 'not found %', id_middle;
      CONTINUE;
    END IF;

    EXIT WHEN target_tz BETWEEN range_start AND range_end;

    IF split_point <= target_tz THEN
      id_upper  := id_middle;
      id_middle := 0;
    ELSE
      id_lower := id_middle;
    END IF;
  END LOOP;
END;
$$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION job_history_bsearch(search_point TIMESTAMPTZ) RETURNS BIGINT
AS
$$
DECLARE
  id_lower BIGINT;
  id_upper BIGINT;
  id_middle BIGINT DEFAULT 0;
  range_start TIMESTAMPTZ;
  range_end TIMESTAMPTZ;
  drop_after INTERVAL;
  target_tz TIMESTAMPTZ;
BEGIN
  SELECT (config->>'drop_after')::interval
  INTO drop_after
  FROM _timescaledb_config.bgw_job
  WHERE id = 3;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'system job 3 (job execution history retention) not found';
  END IF;

  -- Calculate window range for searching
  range_start := time_bucket(drop_after, search_point);
  range_end := range_start + drop_after;

  RAISE DEBUG 'search_point %, range_start %, range_end %',
    search_point, range_start, range_end;

  SELECT COALESCE(min(id), 0), COALESCE(max(id), 0)
  INTO id_lower, id_upper
  FROM _timescaledb_internal.bgw_job_stat_history;

  IF id_lower = 0 AND id_upper = 0 THEN
    RETURN NULL;
  END IF;

  WHILE id_lower <= id_upper LOOP
    id_middle := id_lower + (id_upper - id_lower) / 2;

    SELECT execution_finish
    INTO target_tz
    FROM _timescaledb_internal.bgw_job_stat_history
    WHERE id = id_middle;

    RAISE DEBUG 'target_tz %, id_lower %, id_upper %, id_middle %, le %, gt %, in range %',
      target_tz, id_lower, id_upper, id_middle, (target_tz <= search_point),
      (target_tz > search_point), (target_tz BETWEEN range_start AND range_end);

    IF NOT FOUND OR target_tz BETWEEN range_start AND range_end THEN
      RETURN id_middle;
    ELSIF search_point <= target_tz THEN
      id_upper := id_middle - 1;
    ELSE
      id_lower := id_middle + 1;
    END IF;
  END LOOP;

  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

SELECT now() - interval '1 month' AS search_point \gset
SELECT job_history_bsearch(:'search_point') AS job_id_found \gset

EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM _timescaledb_internal.bgw_job_stat_history
WHERE id >= :'job_id_found' AND execution_finish >= :'search_point';


DO
$$
DECLARE
  search_point TIMESTAMPTZ;
  id_found BIGINT;
BEGIN

  SELECT now() - (config->>'drop_after')::interval
  INTO search_point
  FROM _timescaledb_config.bgw_job
  WHERE id = 3;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'system job 3 (job execution history retention) not found';
    RETURN;
  END IF;

  id_found := job_history_bsearch(search_point);

  IF id_found IS NULL THEN
    RAISE WARNING 'no job history for cleaning up';
    RETURN;
  END IF;

  LOCK TABLE _timescaledb_internal.bgw_job_stat_history
    IN ACCESS EXCLUSIVE MODE;

  CREATE TEMP TABLE __tmp_bgw_job_stat_history ON COMMIT DROP AS
    SELECT * FROM _timescaledb_internal.bgw_job_stat_history WHERE id >= id_found AND execution_finish >= search_point;

  TRUNCATE _timescaledb_internal.bgw_job_stat_history;
 
  INSERT INTO _timescaledb_internal.bgw_job_stat_history
  SELECT * FROM __tmp_bgw_job_stat_history;
END;
$$
LANGUAGE plpgsql;
