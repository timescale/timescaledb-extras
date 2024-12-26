Create a custom job
```sql
SELECT add_job(
    '_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner',
    '1 minute',
    config => '{"enable_tiered_reads": true}');
```

Add tasks
```sql
CALL _timescaledb_additional.schedule_osm_cagg_refresh('g_data_16_41_1minute');
NOTICE:  Scheduled incremental refreshes for hf_dss.g_data_fenceline_1hour (2024-01-01 00:00:00+00 - 2024-10-14 00:00:00+00). Tasks evaluated: 30, newly inserted: 30
```

Check the status
```sql
TABLE _timescaledb_additional.incremental_continuous_aggregate_refreshes;
```

```sql
SELECT
    count(finished) finished,
    SUM(CASE WHEN finished IS NULL THEN 1 ELSE 0 END) left
FROM _timescaledb_additional.incremental_continuous_aggregate_refreshes;
 finished | left
----------+------
      161 |   20
(1 row)
```
