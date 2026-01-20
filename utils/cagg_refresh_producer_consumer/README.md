# Continous Aggregates, incremental parallel setup

This code is exploring the possibilities to do incremental CAgg refreshes in
parallel. The setup it uses is as following.

At a very high level these are the components:

- a table that acts as a work queue:
  `_timescaledb_additional.incremental_continuous_aggregate_refreshes`
- one (or more) producer jobs that schedule CAgg refreshes
- one (or more) consumer jobs that process the jobs based on priority

The producer jobs can be scheduled very frequently, as no duplicate tasks will
be written to the work queue.

## Producer

We have a producer procedure
(`schedule_refresh_continuous_aggregate_incremental`), which schedules tasks to
be picked up by the consumers.

The configuration for this call contains the following keys:

```json
{
    "end_offset": "similar to end-offset in the policy",
    "start_offset": "similar to start-offset in the policy",
    "continuous_aggregate": "regclass / fully qualified name of the user view for the CAgg",
    "increment_size": "the size of each individual task, default: chunk_interval",
    "priority": "priority for these tasks. Lower numbers get processed earlier, default: 100"
}
```

### Producer Examples

#### Schedule multiple jobs for this cagg, with increments of 1 week

We schedule 2 sets

```sql
CALL _timescaledb_additional.schedule_refresh_continuous_aggregate_incremental(
    job_id => null,
    config => '
{
    "end_offset": "6 weeks",
    "start_offset": "3 years",
    "continuous_aggregate": "public.test_cagg_incr_refresh_cagg",
    "increment_size": "3 days"
}');
```

with the most recent data having the highest priority:

```sql
CALL _timescaledb_additional.schedule_refresh_continuous_aggregate_incremental(
    job_id => null,
    config => '
{
    "end_offset": "1 day",
    "start_offset": "6 weeks",
    "continuous_aggregate": "public.test_cagg_incr_refresh_cagg",
    "increment_size": "1 week",
    "priority": 1
}');
```

## Consumer

For the consumer(s), we schedule as many jobs as we want to be able to run in
parallel. Likely, a reasonable maximum for these is not too high, for example,
4-6. While we *can* do incremental CAgg refreshes, we cannot (as of december
2024) schedule parallel refreshes for the same CAgg. This should therefore never
be higher than your number of CAggs.

These jobs will be consuming a connection all the time, as they are designed to
run all the time.

```sql
SELECT
    public.add_job(
        proc => '_timescaledb_additional.task_refresh_continuous_aggregate_incremental_runner'::regproc,
        -- This isn't really needed, but this ensures the workers do not run forever,
        -- but once they terminate, they will be restarted within 15 minutes or so.
        schedule_interval => interval '15 minutes',
        config => '{"max_runtime": "11 hours"}',
        initial_start => now()
    )
FROM
    generate_series(1, 4);
```
