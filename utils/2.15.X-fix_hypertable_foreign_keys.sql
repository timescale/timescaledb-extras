-- Fix compressed hypertables with FOREIGN KEY constraints that were created with TimescaleDB versions before 2.15.0
CREATE OR REPLACE FUNCTION pg_temp.constraint_columns(regclass, int2[]) RETURNS text[] AS
$$
  SELECT array_agg(attname) FROM unnest($2) un(attnum) LEFT JOIN pg_attribute att ON att.attrelid=$1 AND att.attnum = un.attnum;
$$ LANGUAGE SQL SET search_path TO pg_catalog, pg_temp;

DO $$
DECLARE
  ht_id int;
  ht regclass;
  chunk regclass;
  con_oid oid;
  con_frelid regclass;
  con_name text;
  con_columns text[];
  chunk_id int;

BEGIN

  -- iterate over all hypertables that have foreign key constraints
  FOR ht_id, ht in
    SELECT
      ht.id,
      format('%I.%I',ht.schema_name,ht.table_name)::regclass
    FROM _timescaledb_catalog.hypertable ht
    WHERE
      EXISTS (
        SELECT FROM pg_constraint con
        WHERE
          con.contype='f' AND
          con.conrelid=format('%I.%I',ht.schema_name,ht.table_name)::regclass
      )
  LOOP
    RAISE NOTICE 'Hypertable % has foreign key constraint', ht;

    -- iterate over all foreign key constraints on the hypertable
    -- and check that they are present on every chunk
    FOR con_oid, con_frelid, con_name, con_columns IN
      SELECT con.oid, con.confrelid, con.conname, pg_temp.constraint_columns(con.conrelid,con.conkey)
      FROM pg_constraint con
      WHERE
        con.contype='f' AND
        con.conrelid=ht
    LOOP
      RAISE NOTICE 'Checking constraint % %', con_name, con_columns;
      -- check that the foreign key constraint is present on the chunk

      FOR chunk_id, chunk IN
        SELECT
          ch.id,
          format('%I.%I',ch.schema_name,ch.table_name)::regclass
        FROM _timescaledb_catalog.chunk ch
        WHERE
          ch.hypertable_id=ht_id
      LOOP
        RAISE NOTICE 'Checking chunk %', chunk;
        IF NOT EXISTS (
          SELECT FROM pg_constraint con
          WHERE
            con.contype='f' AND
            con.conrelid=chunk AND
            con.confrelid=con_frelid  AND
            pg_temp.constraint_columns(con.conrelid,con.conkey) = con_columns
        ) THEN
          RAISE WARNING 'Restoring constraint % on chunk %', con_name, chunk;
          PERFORM _timescaledb_functions.constraint_clone(con_oid, chunk);
          INSERT INTO _timescaledb_catalog.chunk_constraint(chunk_id, dimension_slice_id, constraint_name, hypertable_constraint_name) VALUES (chunk_id, NULL, con_name, con_name);
        END IF;

      END LOOP;
    END LOOP;

  END LOOP;

END
$$;

DROP FUNCTION pg_temp.constraint_columns(regclass, int2[]);
