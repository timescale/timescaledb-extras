\set ON_ERROR_STOP ON

SELECT *
INTO _timescaledb_internal.saved_ranges
FROM _timescaledb_catalog.continuous_aggs_materialization_ranges;

TRUNCATE _timescaledb_catalog.continuous_aggs_materialization_ranges;

\set ON_ERROR_STOP OFF
