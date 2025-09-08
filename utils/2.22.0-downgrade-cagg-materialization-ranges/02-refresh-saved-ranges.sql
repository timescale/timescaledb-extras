\set ON_ERROR_STOP 1

DO $$
DECLARE
    range_record RECORD;
BEGIN
    FOR range_record IN
        SELECT sr.materialization_id, sr.lowest_modified_value AS lowest, sr.greatest_modified_value AS greatest,
               ca.user_view_schema || '.' || ca.user_view_name AS cagg_name
        FROM _timescaledb_internal.saved_ranges sr
        JOIN _timescaledb_catalog.continuous_agg ca ON sr.materialization_id = ca.mat_hypertable_id
        ORDER BY sr.materialization_id
    LOOP
    RAISE NOTICE 'force refreshing %s: start:%s end:%s', range_record.cagg_name, to_timestamp(range_record.lowest/1000000), to_timestamp(range_record.greatest/1000000);
        CALL refresh_continuous_aggregate(range_record.cagg_name, to_timestamp(range_record.lowest/1000), to_timestamp(range_record.greatest/1000), force => true);
    END LOOP;
END $$;

DROP TABLE _timescaledb_internal.saved_ranges;

\set ON_ERROR_STOP 0
