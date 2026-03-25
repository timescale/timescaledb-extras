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

Please note that these ranges may represent either **actively being processed work** or stale entries left behind by a previously failed refresh. For that reason, the script is intentionally conservative and only generates statements for manual review. On the same line, this view will help us monitor jobs, as requested.
To generate the refresh statements for a specific continuous aggregate appearing on the public.cagg_pending_ranges view, run:
```
CREATE TEMPORARY TABLE cagg_get_manual_refresh_tmp AS
SELECT *
FROM public.cagg_get_manual_refresh_stmt('public.sensor_hourly');
```

The output will include ready-to-run refresh command statements and a corresponding DELETE query for each range you see on the public.cagg_pending_ranges view.
The intended workflow to address gaps is to first save the view output into a temporary table, review the generated commands, then execute the DELETE statement, and finally run the corresponding refresh_continuous_aggregate() call. Saving the view output to a temporary table early provides a safeguard against an edge case involving concurrent refresh failures. This approach ensures the ranges remain available for reprocessing even if the refresh step encounters that error. 

Again, the ranges that appear when querying the public.cagg_pending_ranges view **may represent either actively being processed work or stale entries left behind by a previously failed refresh**. For that reason, the script is intentionally conservative and only generates statements for manual review. Here, if you have any questions, please let us know.

For example:
```
CALL refresh_continuous_aggregate(
    'public.sensor_hourly',
    '2025-01-01 00:00:00+00'::timestamptz,
    '2025-01-02 00:00:00+00'::timestamptz
);
```

If you encounter an overlap error while running one of the generated refresh commands, such as a message indicating that the requested materialization range overlaps with an existing range, that generally means one of two things: 
A refresh is actively working on that interval.
A previous failed operation left behind a stale range entry.
In that case, the recommended approach is first to verify whether a manual refresh or a policy job is currently processing that range. 
