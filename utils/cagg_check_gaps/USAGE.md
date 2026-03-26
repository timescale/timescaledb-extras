We created a helper SQL script, cagg_manual_refresh_ranges.sql, to help identify and manually recover continuous aggregate refresh ranges that a failed manual refresh or a failed policy execution may have left behind.

This script is intended for troubleshooting and recovery scenarios. It does not execute any refresh automatically. Instead, it creates a view and a function, both on the public schema, that allow you to inspect the pending materialization ranges and generate the corresponding refresh_continuous_aggregate() statements for manual review and execution.

To load the script in psql, run:
```
\i cagg_manual_refresh_ranges.sql
```

Once loaded, you can inspect the ranges currently recorded with:
```
SELECT * 
FROM public.cagg_pending_ranges;
```

Please note that these ranges may represent either **actively being processed work** or stale entries left behind by a previously failed refresh. For that reason, the script is intentionally conservative and only generates statements for manual review. 

To generate the refresh statements for a specific continuous aggregate appearing on the public.cagg_pending_ranges view, run:
```
CREATE TEMPORARY TABLE cagg_get_manual_refresh_tmp AS
SELECT *
FROM public.cagg_get_manual_refresh_stmt('public.sensor_hourly');
```

```
tsdb=> select * from public.cagg_get_manual_refresh_stmt('public.telemetry_rec_monitor_node');
refresh_command | CALL refresh_continuous_aggregate('public.telemetry_rec_monitor_node', '2026-03-24 16:45:50+00'::timestamptz, '2026-03-24 16:46:40+00'::timestamptz);
delete_command  | SELECT _timescaledb_functions.remove_materialization_ranges(943, 1774370750000000, 1774370800000000);

```

The output has ready-to-run delete and refresh commands.
The intended workflow to address gaps is to first save the view output into a temporary table, review the generated commands, then execute the delete_command , followed by the refresh_command
Saving the view output to a temporary table early provides a safeguard against an edge case involving concurrent refresh failures. This approach ensures the ranges remain available for reprocessing even if the refresh step encounters that error. 

Again, the ranges that appear when querying the public.cagg_pending_ranges view **may represent either actively being processed work or stale entries left behind by a previously failed refresh**. For that reason, the script is intentionally conservative and only generates statements for manual review. 
