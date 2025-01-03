# Incremental Continuous Aggregate Refresh on Tiered Data

## Introduction
This directory has a set of SQL procedures to create/refresh a continuous aggregates from tiered data on S3, only available on Timescale Cloud.

> **IMPORTANT NOTE:**
> This procedure only adds the **TIERED PORTION** of the hypertable's data to the continuous aggregate.

We use a producer-consumer pattern to incrementally build the Continuous Aggregate from tiered data. We first identify the time range that we will refresh, split it into a bunch of smaller intervals and then start refreshing these intervals.

## Producer
The procedure `_timescaledb_additional.schedule_osm_cagg_refresh` finds the time range of the tiered data for the hypertable (corresponding to the Continuous Aggregate) and splits the range into a set of time intervals. These intervals are added to the `_timescaledb_additional.incremental_continuous_aggregate_refreshes` table.

## Consumer
The procedure `_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner` picks the time intervals from the `_timescaledb_additional.incremental_continuous_aggregate_refreshes` table and calls `refresh_continuous_aggregate` procedure on these intervals until the list is exhausted.

## How do I run this?


### 1. Produce ranges to be refreshed:
```sql
CALL _timescaledb_additional.schedule_osm_cagg_refresh('myschema-name', 'mycagg-name');
```

### 2. Add a consumer job to refresh the produced ranges:
```sql
SELECT
    add_job(
        '_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner',
        schedule_interval => '5 seconds',
        config => '{"enable_tiered_reads": true, "max_runtime": "5 minutes"}'
    );
```

This call adds a job that runs in the background and refreshes the continuous aggregate.

**NOTE**
The `enable_tiered_reads = true` config is necessary if the default DB settings for `timescaleb_osm.enable_tiered_reads` GUC is `false`.

### 3. Monitoring

The `_timescaledb_additional.osm_incremental_refresh_status` view can be used to monitor progress of the refresh execution:
```sql
tsdb=> SELECT * FROM _timescaledb_additional.osm_incremental_refresh_status;
       continuous_aggregate       | not started | started | finished 
----------------------------------+-------------+---------+----------
 hf_schema.cagg_data_1minute      |         256 |       0 |        3
```

`finished` shows the number of refresh time intervals that have been completed, `not started` shows how many are remaining.

The `_timescaledb_additional.job_cagg_refresh_status` view can be used to check the status of the job executions (consumers):
```sql
tsdb=> SELECT * FROM _timescaledb_additional.job_cagg_refresh_status;
    clock_timestamp     |  pid   |  wait_event  |                        application_name                        | xact_age | backend_age 
------------------------+--------+--------------+----------------------------------------------------------------+----------+-------------
 2025-01-04 14:31:03+00 | 153087 |              | hf_schema.cagg_data_1minute refresh 2021-01-19 2021-02-18    | 00:00:10 | 00:00:44
```

## Advanced Usecase

1. You can use the schedule_osm_cagg_refresh procedure to generate refresh ranges for multiple continuous aggregates.

```sql
CALL _timescaledb_additional.schedule_osm_cagg_refresh(); -- All CAggs with tiered data
```

```sql
CALL _timescaledb_additional.schedule_osm_cagg_refresh(schema_mask => 'hf_schema'); -- All CAggs with tiered data under hf_schema 
```

```sql
CALL _timescaledb_additional.schedule_osm_cagg_refresh(name_mask => 'cagg_data%'); -- All CAggs with name beginning with cagg_data, that appears in any schema  
```

```sql
CALL _timescaledb_additional.schedule_osm_cagg_refresh(schema_mask => 'hf_schema%', name_mask => 'cagg_data%'); -- All CAggs with name beginning with cagg_data under schemas that begin with the name hf_schema.  
```

2. Multiple continuous aggregates can be refreshed in parallel. You can create multiple jobs like so:

```sql
SELECT
    add_job(
        '_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner',
        schedule_interval => '5 seconds',
        config => '{"enable_tiered_reads": true, "max_runtime": "5 minutes"}'
    )
FROM
    generate_series(1, 4);
```

Every job is executed by a background worker. Please keep you max background worker settings and database load in mind while adding jobs so that you do not exhaust DB resources.

**NOTE**  
A Continuous Aggregate will be refreshed by exactly 1 (one) job using this framework i.e. one continuous aggregate cannot be refreshed in parallel. 

The job progress can be monitored using `_timescaledb_additional.osm_incremental_refresh_status` view.
```sql
tsdb=> SELECT * FROM _timescaledb_additional.osm_incremental_refresh_status;
       continuous_aggregate       | not started | started | finished
----------------------------------+-------------+---------+----------
 hf_schema.cagg_1minute           |         256 |       0 |        3
 hf_schema.cagg_2_1hour           |         255 |       1 |        3
 hf_schema.cagg_2_1minute         |         258 |       1 |       27
```

