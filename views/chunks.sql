-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE and LICENSE for copyright and licensing information.

DROP VIEW IF EXISTS chunks_ts, chunks_tstz;

-- Show time range, hypertable, and tablespace for chunks belonging to
-- hypertables that have a time dimension.
--
-- This do not show chunks belonging to hypertables that use an
-- integer time dimension.
CREATE OR REPLACE VIEW chunks_tstz AS
SELECT format('%1$I.%2$I', ch.schema_name, ch.table_name)::regclass AS chunk
     , format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass AS hypertable
     , tstzrange(CASE WHEN sl.range_start = -9223372036854775808 THEN NULL
                 ELSE _timescaledb_internal.to_timestamp(sl.range_start) END,
                 CASE WHEN sl.range_end = 9223372036854775807 THEN NULL
                 ELSE _timescaledb_internal.to_timestamp(sl.range_end) END)
       AS time_range
     , (SELECT nspname FROM pg_class JOIN pg_namespace ns ON relnamespace = ns.oid
        WHERE chunk_id = pg_class.oid) AS tablespace
  FROM _timescaledb_catalog.chunk ch
  JOIN _timescaledb_catalog.hypertable ht ON ch.hypertable_id = ht.id
  JOIN _timescaledb_catalog.dimension di ON di.hypertable_id = ht.id
  JOIN _timescaledb_catalog.chunk_constraint cn ON cn.chunk_id = ch.id
  JOIN _timescaledb_catalog.dimension_slice sl ON cn.dimension_slice_id = sl.id
 WHERE column_type = 'timestamptz'::regtype;

CREATE OR REPLACE VIEW chunks_ts AS
SELECT format('%1$I.%2$I', ch.schema_name, ch.table_name)::regclass AS chunk
     , format('%1$I.%2$I', ht.schema_name, ht.table_name)::regclass AS hypertable
     , tsrange(CASE WHEN sl.range_start = -9223372036854775808 THEN NULL
               ELSE _timescaledb_internal.to_timestamp_without_timezone(sl.range_start) END,
               CASE WHEN sl.range_end = 9223372036854775807 THEN NULL
	       ELSE _timescaledb_internal.to_timestamp_without_timezone(sl.range_end) END)
       AS time_range
     , (SELECT nspname FROM pg_class JOIN pg_namespace ns ON relnamespace = ns.oid
        WHERE chunk_id = pg_class.oid) AS tablespace
  FROM _timescaledb_catalog.chunk ch
  JOIN _timescaledb_catalog.hypertable ht ON ch.hypertable_id = ht.id
  JOIN _timescaledb_catalog.dimension di ON di.hypertable_id = ht.id
  JOIN _timescaledb_catalog.chunk_constraint cn ON cn.chunk_id = ch.id
  JOIN _timescaledb_catalog.dimension_slice sl ON cn.dimension_slice_id = sl.id
 WHERE column_type = 'timestamp'::regtype;
