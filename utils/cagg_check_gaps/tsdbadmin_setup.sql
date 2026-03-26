CREATE OR REPLACE FUNCTION
  _timescaledb_functions.remove_materialization_ranges(                         
      mat_hypertable_id INTEGER, start_value BIGINT, end_value BIGINT
  ) RETURNS VOID AS $$
      DELETE FROM _timescaledb_catalog.continuous_aggs_materialization_ranges
      WHERE materialization_id = mat_hypertable_id and lowest_modified_value = start_value and greatest_modified_value = end_value;
  $$ LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp;


 REVOKE ALL ON FUNCTION
  _timescaledb_functions.remove_materialization_ranges(INTEGER, BIGINT, BIGINT) FROM PUBLIC;
  GRANT EXECUTE ON FUNCTION
  _timescaledb_functions.remove_materialization_ranges(INTEGER, BIGINT, BIGINT) TO tsdbadmin;
