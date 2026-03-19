-- Generate refresh commands for unrefreshed continuous aggregate ranges.
--
-- Usage:
--   1. Load the function:  \i sql/cagg_manual_refresh_ranges.sql
--   2. Call it:            SELECT * FROM cagg_get_manual_refresh_stmt('sensor_hourly_avg');
--                          SELECT * FROM cagg_get_manual_refresh_stmt('my_schema.my_cagg');
--
-- The function reads pending materialization ranges from
-- _timescaledb_catalog.continuous_aggs_materialization_ranges
-- and returns CALL refresh_continuous_aggregate(...) commands
-- (without executing them), one per range row.

CREATE OR REPLACE VIEW public.cagg_pending_ranges AS
    SELECT
        format('%I.%I', ca.user_view_schema, ca.user_view_name) AS cagg_name,
        CASE
            WHEN d.column_type = 'timestamptz'::regtype THEN
                _timescaledb_functions.to_timestamp(mr.lowest_modified_value)::text
            WHEN d.column_type = 'timestamp'::regtype THEN
                _timescaledb_functions.to_timestamp(mr.lowest_modified_value)::timestamp::text
            WHEN d.column_type = 'date'::regtype THEN
                _timescaledb_functions.to_date(mr.lowest_modified_value)::text
            ELSE
                mr.lowest_modified_value::text
        END AS lowest_modified_value,
        CASE
            WHEN d.column_type = 'timestamptz'::regtype THEN
                _timescaledb_functions.to_timestamp(mr.greatest_modified_value)::text
            WHEN d.column_type = 'timestamp'::regtype THEN
                _timescaledb_functions.to_timestamp(mr.greatest_modified_value)::timestamp::text
            WHEN d.column_type = 'date'::regtype THEN
                _timescaledb_functions.to_date(mr.greatest_modified_value)::text
            ELSE
                mr.greatest_modified_value::text
        END AS greatest_modified_value
    FROM _timescaledb_catalog.continuous_aggs_materialization_ranges mr
    JOIN _timescaledb_catalog.continuous_agg ca
        ON mr.materialization_id = ca.mat_hypertable_id
    JOIN _timescaledb_catalog.dimension d
        ON d.hypertable_id = ca.raw_hypertable_id
        AND d.interval_length IS NOT NULL
    ORDER BY ca.user_view_schema, ca.user_view_name, mr.lowest_modified_value;

DROP FUNCTION IF EXISTS cagg_get_manual_refresh_stmt(text);
CREATE OR REPLACE FUNCTION cagg_get_manual_refresh_stmt(cagg_name text)
RETURNS TABLE(refresh_command text, delete_command text)
LANGUAGE sql
STABLE
AS $$
    WITH cagg_info AS (
        SELECT
            ca.mat_hypertable_id,
            ca.user_view_schema,
            ca.user_view_name,
            d.column_type
        FROM _timescaledb_catalog.continuous_agg ca
        JOIN _timescaledb_catalog.dimension d
            ON d.hypertable_id = ca.raw_hypertable_id
            AND d.interval_length IS NOT NULL
        WHERE format('%I.%I', ca.user_view_schema, ca.user_view_name) = cagg_name
           OR ca.user_view_name = cagg_name
    ),
    ranges AS (
        SELECT
            ci.mat_hypertable_id,
            mr.lowest_modified_value,
            mr.greatest_modified_value
        FROM _timescaledb_catalog.continuous_aggs_materialization_ranges mr
        JOIN cagg_info ci ON mr.materialization_id = ci.mat_hypertable_id
        ORDER BY mr.lowest_modified_value
    )
    SELECT format(
        'CALL refresh_continuous_aggregate(%L, %s, %s);',
        format('%I.%I', ci.user_view_schema, ci.user_view_name),
        CASE
            WHEN ci.column_type = 'timestamptz'::regtype THEN
                format('%L::timestamptz', _timescaledb_functions.to_timestamp(r.lowest_modified_value))
            WHEN ci.column_type = 'timestamp'::regtype THEN
                format('%L::timestamp', _timescaledb_functions.to_timestamp(r.lowest_modified_value)::timestamp)
            WHEN ci.column_type = 'date'::regtype THEN
                format('%L::date', _timescaledb_functions.to_date(r.lowest_modified_value))
            ELSE
                r.lowest_modified_value::text
        END,
        CASE
            WHEN ci.column_type = 'timestamptz'::regtype THEN
                format('%L::timestamptz', _timescaledb_functions.to_timestamp(r.greatest_modified_value))
            WHEN ci.column_type = 'timestamp'::regtype THEN
                format('%L::timestamp', _timescaledb_functions.to_timestamp(r.greatest_modified_value)::timestamp)
            WHEN ci.column_type = 'date'::regtype THEN
                format('%L::date', _timescaledb_functions.to_date(r.greatest_modified_value))
            ELSE
                r.greatest_modified_value::text
        END
    ),
    format(
        'DELETE FROM _timescaledb_catalog.continuous_aggs_materialization_ranges WHERE materialization_id = %s AND lowest_modified_value = %s AND greatest_modified_value = %s;',
        r.mat_hypertable_id,
        r.lowest_modified_value,
        r.greatest_modified_value
    )
    FROM ranges r
    CROSS JOIN cagg_info ci
    ORDER BY r.lowest_modified_value;
$$;
