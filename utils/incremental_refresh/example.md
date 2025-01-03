Create a custom job
```sql
SELECT add_job(
    '_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner',
    '1 minute',
    config => '{"enable_tiered_reads": true}');
```

Produce ranges to be refreshed
```sql
CALL _timescaledb_additional.schedule_osm_cagg_refresh('g_data_16_41_1minute'); -- An specific CAgg
CALL _timescaledb_additional.schedule_osm_cagg_refresh(); -- All CAggs
```

Check the queue status
```sql
SELECT
    continuous_aggregate,
    count(*) FILTER (WHERE started IS NULL) AS "not started",
    count(*) FILTER (WHERE started IS NOT NULL AND finished IS NULL) AS "started",
    count(*) FILTER (WHERE started IS NOT NULL AND finished IS NOT NULL) AS "finished"
FROM
    _timescaledb_additional.incremental_continuous_aggregate_refreshes
GROUP BY
    continuous_aggregate
ORDER BY
    continuous_aggregate;
```

Check the jobs execution
```sql
SELECT
    clock_timestamp()::timestamptz(0),
    pid,
    wait_event,
    application_name,
    (now() - xact_start)::interval(0) AS xact_age
FROM
    pg_stat_activity
WHERE
    state <> 'idle'
    AND application_name LIKE '%refresh%';
```
