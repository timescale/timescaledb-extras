# Auto Aggregate Selection

The file [utils/aggregate_selector.sql](/utils/aggregate_selector.sql) implements an automatic continuous aggregate selection system for TimescaleDB databases, which is meant to optimize the plotting of time series data in tools such as Grafana, by easing and automating the use of continuous aggregates.

Its general usage as well as a few examples are documented here.

## Quickstart:

Execute the [utils/aggregate_selector.sql](/utils/aggregate_selector.sql) file by logging in to the database via `psql` and then running `\i aggregate_selector.sql`. *Always* verify the contents of the file when doing so. I encourage you to read through the SQL source, as it's not very long!

Create an arbitrary number of continuous aggregates on top of your existing hypertables. Make sure to name their columns reasonably, preferably with the type of downsampling in the column name, such as `value_avg`. Exact specifics don't matter, as long as all columns of the same name hold the same type of data.

The aggregation function is defined as follows:
```SQL
ts_autoagg(hypertable TEXT, 
	data_interval INTERVAL,
	aggregate_choices JSONB, 
	groupby_clause TEXT,
	filter_query TEXT,
	hypertable_schema TEXT DEFAULT 'public',
	time_column TEXT DEFAULT 'time')
```

And a fairly standard aggregation selection looks as follows:
```SQL
SELECT *
FROM ts_autoagg( -- Root hypertable name
				'cpu', 
				-- Requested downsampling interval
				INTERVAL '10m',
				-- JSONB Array of Objects. Each object represents one downsampling option, and
				-- should at least have a "with_columns" field and an "aggregate" field. May
				-- also have a "priority" field.
				$aggregate_options$
					[
						{"with_columns": ["values"],      "aggregate":"avg(value) AS value"},
						{"with_columns": ["value_avg"],   "aggregate":"avg(value_avg) AS value"},
						{"with_columns": ["value_stats"], "aggregate":"average(rollup(value_stats))", "priority":10}
					]
				$aggregate_options$,
				-- List of fields to use as GROUP BY keys. This string is inserted into the SELECT
				-- clause as well as after the GROUP BY time_bucket() key, so it *can not be empty*.
				'tags', 
				-- Additional JOIN and WHERE clause(s). This string will be inserted into the query
				-- after the FROM statement. It *really should* include a time-column constraint to
				-- let TimescaleDB do chunk exclusion here.
				$where_clause$
								WHERE time BETWEEN NOW()-INTERVAL'30d' AND NOW()-INTERVAL'10d'
									AND tags@>'{"metric":"usage_idle","cpu":"cpu-total"}'
				$where_clause$)
				-- The user MUST define the return columns of this function, it's a PostgreSQL quirk.
				AS (time TIMESTAMP, tags JSONB, value DOUBLE PRECISION);
```

## Detailed explanation

The aggregate selection and auto-downsampling is performed via the "ts_autoagg" function. 
The function will, internally, search through all Continuous Aggregates built on top of the given root hypertable. It will then select one aggregate (or the hypertable itself) that matches the given input values for downsampling_interval and aggregation options. The data is then filtered with the `filter_query` parameter, and a `GROUP BY` is performed using both `time_bucket(<time_column>, <data_interval>) AS time` and the list of `groupby_clause`s. The result of this query is then returned.

The function takes in the following arguments:

```SQL
ts_autoagg(hypertable TEXT, 
	data_interval INTERVAL,
	aggregate_choices JSONB, 
	groupby_clause TEXT,
	filter_query TEXT,
	hypertable_schema TEXT DEFAULT 'public',
	time_column TEXT DEFAULT 'time')
```

The function will try and find a matching hypertable as follows:
- It first looks up all known Continuous Aggregates built on the root hypertable, and fetches their
intervals as well as available columns.
- It then looks for the largest bucketing interval aggregate table whose bucket size is smaller or equal to
the requested data, AND which has at least one combination of columns matching a `with_colums` clause from one of the `aggregate_choices` entries.
The root hypertable is assumed to have an interval of "0", i.e. no downsampling.
- If no matching table has been found, it will look for the next-largest table with a valid column combination.
This only happens when the supplied aggregation options do not include a valid combination for the root hypertable,
and may be used to e.g. force the downsampling to always run off of a continuous aggregate.
- It will then return the result of a query constructed from the given parameters. If no table was found, it will return an empty table.


Please note that it is HIGHLY advised to place the time filtering query inside the query filter argument.
If it is applied outside the function, the TimescaleDB planner will NOT be able to perform chunk exclusion
properly, which will lead to major query slowdowns.

### aggregate_choices structure

The aggregate selection JSONB structure is as follows:
It is an Array of Objects. Each array element represents one possible downsampling method, and one candidate
will be chosen based on given parameters.
Each array element can have the following key/value pairs:
- "with_columns": *Must* be set. Must be an array of strings, each string representing a column that must be
present in a given table in order for the downsampling method to be deemed compatible with said table.
- "aggregate": *Must* be set. Must be a single string, defining the clause, or clauses, used in the SELECT
statement to downsample data. Please note that the function's return type is statically determined by the
user at call-time, and as such all aggregates must return the same type.
- "priority": May optionally be set to an integer. Higher numbers mean this query will be preferentially
selected in case multiple aggregate options are valid.