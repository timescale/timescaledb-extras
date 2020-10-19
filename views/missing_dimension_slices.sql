-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE and LICENSE for copyright and licensing information.

DROP VIEW IF EXISTS missing_dimension_slices;

-- Due to a bug in versions before TimescaleDB 1.7.5 dimension slices
-- could be removed resulting in broken dependencies. This view list
-- any such missing slices and also show the associated constraint.
--
-- This can be used to re-construct the missing dimension slices,
-- which is done in the repair_dimension_slices procedure in
-- procs/repair_dimension_slices.sql
CREATE VIEW missing_dimension_slices AS
SELECT DISTINCT
    format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
    format('%I.%I', ch.schema_name, ch.table_name)::regclass AS chunk,
    attname AS column_name,
    dimension_slice_id,
    constraint_name,
    pg_get_expr(conbin, conrelid) AS constraint_expr
FROM
    _timescaledb_catalog.chunk_constraint cc
    JOIN _timescaledb_catalog.chunk ch ON cc.chunk_id = ch.id
    JOIN _timescaledb_catalog.hypertable ht ON ht.id = ch.hypertable_id
    JOIN pg_constraint ON conname = constraint_name
    JOIN pg_namespace ns ON connamespace = ns.oid
        AND ns.nspname = ch.schema_name
    JOIN pg_attribute ON attnum = conkey[1]
        AND attrelid = conrelid
WHERE
    dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice);
