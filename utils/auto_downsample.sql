-- AUTO AGGREGATE SELECTION AND DOWNSAMPLING
-- Created by: David Bailey, Jun. 2023
-- Twitter: @XasinTheSystem


-- This file implements the functionality of an automatic continuous aggregate selection system
-- for TimescaleDB databases, as well as a few needed helper functions needed to perform this.
--
-- The aggregate selection and auto-downsampling is performed via the "auto_downsample" function. This function takes
-- in a root hypertable name, a downsampling interval that is used for time_bucket grouping, a JSONB structure
-- defining available downsampling methods, a set of columns used for a GROUP BY clause alongside the time_bucket,
-- and a string defining further JOIN and WHERE clauses needed in the query.
--
-- Please note that it is HIGHLY advised to place the time filtering query inside the query filter argument.
-- If it is applied outside the function, the TimescaleDB planner will NOT be able to perform chunk exclusion
-- properly, which will lead to major query slowdowns.
--
-- The aggregate selection JSONB structure is as follows:
-- It is an Array of Objects. Each array element represents one possible downsampling method, and one candidate
-- will be chosen based on given parameters.
-- Each array element can have the following key/value pairs:
--  - "with_columns": *Must* be set. Must be an array of strings, each string representing a column that must be
--    present in a given table in order for the downsampling method to be deemed compatible with said table.
--  - "aggregate": *Must* be set. Must be a single string, defining the clause, or clauses, used in the SELECT
--    statement to downsample data. Please note that the function's return type is statically determined by the
--    user at call-time, and as such all aggregates must return the same type.
--  - "priority": May optionally be set to an integer. Higher numbers mean this query will be preferentially
--    selected in case multiple aggregate options are valid.
--
-- The function will try and find a matching hypertable as follows:
-- - It first looks up all known Continuous Aggregates built on the root hypertable, and fetches their
--   intervals as well as available columns.
-- - It then looks for the largest bucketing interval aggregate table whose bucket size is smaller or equal to
--   the requested data, AND which has at least one combination of columns matching a "with_colums" clause.
--   The root hypertable is assumed to have an interval of "0", i.e. no downsampling.
-- - If no matching table has been found, it will look for the next-largest table with a valid column combination.
--   This only happens when the supplied aggregation options do not include a valid combination for the root hypertable,
--   and may be used to e.g. force the downsampling to always run off of a continuous aggregate.
-- - It will then return the result of a query constructed from the given parameters. If no Table was found, it will
--   return an empty table.
--
---- EXAMPLE QUERY:
-- SELECT *
-- FROM auto_downsample('cpu', INTERVAL '10m',
-- -- JSONB list of aggregate options, made of "with_colum" "aggregate" pairs.
-- $aggs$
--	[
--		{"with_columns":["values"], "aggregate":"avg(value) AS value"},
--		{"with_columns":["value_avg"], "aggregate":"avg(value_avg) AS value"},
--		{"with_columns":["value_stats"], "aggregate":"average(rollup(value_stats))"}
--	]
-- $aggs$,
-- 'tags', -- List of fields or other calculations used in the GROUP BY clause. Must be included in return type list.
-- -- Additional filter clauses. MUST include a filter on the time column.
-- $$
--   WHERE time BETWEEN NOW()-INTERVAL'30d' AND NOW()-INTERVAL'10d'
--   AND tags@>'{"metric":"usage_idle","cpu":"cpu-total"}'
-- $$)
-- -- This function needs to have a user-assigned  return type due to the RETURNS SETOF RECORD type.
-- AS (time TIMESTAMP, tags JSONB, value DOUBLE PRECISION);


-- Recursively go through all known VIEWs for a given root table, and return the set of all VIEWs originating
-- from the given table name.
--
-- Credit to Chris Engelbert for suggesting the recursive CTE query!
CREATE OR REPLACE FUNCTION get_hypertable_caggs(target_table_name TEXT, target_table_schema TEXT DEFAULT 'public')
RETURNS TABLE (table_name TEXT, table_schema TEXT)
LANGUAGE SQL
STABLE
AS $BODY$
WITH RECURSIVE cagg_tables as (
    select vcu1.view_schema, vcu1.view_name
    from information_schema.view_column_usage vcu1
    where vcu1.table_schema = target_table_schema
      and vcu1.table_name = target_table_name

    union all

	select vcu2.view_schema, vcu2.view_name
    from information_schema.view_column_usage vcu2, cagg_tables t
    where t.view_schema = vcu2.table_schema
      and t.view_name = vcu2.table_name
)
SELECT DISTINCT view_name AS table_name,
	view_schema AS table_schema	
	FROM cagg_tables
	WHERE view_schema != '_timescaledb_internal'
$BODY$;

-- For a single given aggregate table, return the bucket_width as Interval
CREATE OR REPLACE FUNCTION get_cagg_interval(table_name TEXT, table_schema TEXT DEFAULT 'public')
RETURNS INTERVAL
LANGUAGE SQL
STABLE
AS $BODY$
	SELECT bucket_width * INTERVAL '1us'
	FROM _timescaledb_catalog.continuous_agg 
	WHERE table_schema = user_view_schema AND table_name = user_view_name
	LIMIT 1
$BODY$;

-- For a single given table, return a JSONB array containing the list of table columns.
-- (FYI, JSONB over TEXT[] to be able to easily use the JSONB array includes subset operator later on)
CREATE OR REPLACE FUNCTION get_jsonb_table_columns(in_table_name TEXT, in_table_schema TEXT DEFAULT 'public')
RETURNS JSONB
LANGUAGE SQL
STABLE
AS $BODY$
	SELECT jsonb_agg(column_name)
	FROM information_schema.columns
	WHERE table_name = in_table_name AND table_schema = in_table_schema
$BODY$;

-- Perform the aggregate choice step. Returns a JSONB.
-- Given the name of a root hypertable, the wanted downsampling interval, and a list of aggregate choices,
-- this function will pick the largest-interval available compatible hypertable or continuous aggregate
-- compatible with the downsampling interval and aggregate "with_column" constraints.
--
-- The returned JSONB will be the chosen aggregate object joined with extra metadata for the chosen
-- table_schema, table_name and table_interval.
CREATE OR REPLACE FUNCTION aggregate_choice(hypertable TEXT, selection_interval INTERVAL, 
	aggregate_types JSONB, hypertable_schema TEXT DEFAULT 'public')
RETURNS JSONB
LANGUAGE SQL
STABLE
AS $BODY$
	WITH available_tables AS (
		SELECT table_name, table_schema, get_cagg_interval(table_name, table_schema) AS table_interval, get_jsonb_table_columns(table_name, table_schema) AS table_columns
		FROM get_hypertable_caggs(hypertable, hypertable_schema)

		UNION ALL

		SELECT hypertable AS table_name, hypertable_schema AS table_schema, INTERVAL '0s' AS table_interval, get_jsonb_table_columns(hypertable, hypertable_schema) AS table_columns
	), available_aggregates AS (
		SELECT *
		FROM jsonb_array_elements(aggregate_types) AS j (aggregate_option)
	)
	SELECT jsonb_build_object('table_name', table_name,
		'table_schema', table_schema,
		'table_interval', table_interval,
		'interval_matched', table_interval = selection_interval) || aggregate_option
	FROM available_tables, available_aggregates, (VALUES(1),(-1)) AS swp(interval_swap)
	WHERE table_columns@>(aggregate_option->'with_columns') AND (table_interval*interval_swap) <= selection_interval
	ORDER BY (table_interval*interval_swap) DESC, coalesce((aggregate_option->>'priority')::int) DESC
	LIMIT 1
$BODY$;


-- This function will perform automatic downsampling of data
-- from a given hypertable OR compatible continuous aggregate of the table.
-- See top of the file for more extensive documentation.
--
-- The parameters are as follows:
-- - hypertable: Name of the root hypertable to use.
-- - data_interval: Interval that will be downsampled to using time_bucket
-- - aggregate_choices: JSONB array of Objects of configurations for possible downsampling methods
-- - groupby_clause: Clauses that will be inserted to the SELECT and GROUP BY query. Note that
--   this may not be left empty. Instead, a dummy "true" or "0" can be inserted to ignore grouping.
-- - filter_query: A clause inserted to the SELECT query after the FROM clause. Used to perform
--   optional JOINs as well as the WHERE clause. MUST include a WHERE filter clause for the time column!
-- - hypertable_schema: Schema of the hypertable, default 'public'
-- - time_column: Name of the time column, default 'time'
CREATE OR REPLACE FUNCTION auto_downsample(hypertable TEXT, data_interval INTERVAL,
	aggregate_choices JSONB, 
	groupby_clause TEXT, filter_query TEXT,
	hypertable_schema TEXT DEFAULT 'public',
	time_column TEXT DEFAULT 'time',
	debug_query BOOLEAN DEFAULT FALSE)
RETURNS SETOF RECORD
LANGUAGE plpgsql
STABLE
AS $BODY$
DECLARE
	selected_parameters jsonb;

	aggregator_column TEXT;

	query_construct TEXT;
BEGIN
	SELECT aggregate_choice(hypertable, data_interval, aggregate_choices, hypertable_schema) INTO selected_parameters;

	IF NOT FOUND THEN
		RAISE EXCEPTION 'No fitting hypertable or aggregate found for given columns and table %!', hypertable;
		RETURN;
	END IF;

	RAISE NOTICE 'Using parameter set %', selected_parameters;

	aggregator_column := selected_parameters->>'aggregate';

	IF aggregator_column IS NULL THEN
		RAISE EXCEPTION 'No aggregator given!' USING HINT = 'Supply a "aggregate" field in the JSON aggregate object'
		RETURN;
	END IF;

	query_construct := format($qry$
			SELECT time_bucket(%L, %I) AS time, %s, %s
			FROM %I.%I
			%s
			GROUP BY 1, %s
			ORDER BY 1
		$qry$, data_interval, time_column, groupby_clause, aggregator_column,
		selected_parameters->>'table_schema', selected_parameters->>'table_name',
		filter_query,
		groupby_clause);

	IF debug_query THEN
		RAISE NOTICE 'Generated query output:'
		RAISE NOTICE query_construct
	END IF;

	RETURN QUERY EXECUTE query_construct;
END;
$BODY$;



