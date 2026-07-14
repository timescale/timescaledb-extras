--
-- This file is licensed under the Apache License, see LICENSE-APACHE
-- at the top level directory of the TimescaleDB distribution.

-- Optional pre-upgrade step for 2.27.2 -> 2.28.0: rename chunk constraints to
-- their 2.28.0 names now, per chunk with short locks, so the upgrade doesn't do
-- it under one long lock. Safe to re-run or skip. Commits every batch_size
-- chunks (default 1000); edit the CALL below to change it.
--
--   psql -d <database> -f 2.28.0-preugrade_rename_chunk_constraints.sql

CREATE PROCEDURE pg_temp.rename_chunk_constraints(batch_size int DEFAULT 1000)
LANGUAGE plpgsql AS $$
DECLARE
    chunk_ids int[];
    cid int;
    r RECORD;
    processed int := 0;
    total int;
BEGIN
    -- 2.28.0+ dropped the catalog: nothing to do.
    IF to_regclass('_timescaledb_catalog.chunk_constraint') IS NULL THEN
        RAISE NOTICE 'No chunk constraints catalog table found, skipping rename_chunk_constraints.';
        RETURN;
    END IF;

    SELECT array_agg(DISTINCT cc.chunk_id)
    INTO chunk_ids
    FROM _timescaledb_catalog.chunk_constraint cc
    WHERE cc.dimension_slice_id IS NULL
      AND cc.hypertable_constraint_name IS NOT NULL;

    IF chunk_ids IS NULL THEN
        RAISE NOTICE 'No chunk constraints to rename.';
        RETURN;
    END IF;

    total := array_length(chunk_ids, 1);

    -- Commit every batch_size chunks to bound how long locks are held.
    FOREACH cid IN ARRAY chunk_ids LOOP
        FOR r IN
            SELECT pg_catalog.format('%I.%I', c.schema_name, c.table_name) AS chunk_table,
                   cc.constraint_name AS old_name,
                   CASE WHEN parent.contype = 'f' THEN cc.hypertable_constraint_name
                        ELSE pg_catalog.format('%s_%s', c.id, cc.hypertable_constraint_name)
                   END AS new_name
            FROM _timescaledb_catalog.chunk_constraint cc
            JOIN _timescaledb_catalog.chunk c ON c.id = cc.chunk_id
            JOIN _timescaledb_catalog.hypertable ht ON ht.id = c.hypertable_id
            JOIN pg_constraint parent
                ON parent.conrelid = pg_catalog.format('%I.%I', ht.schema_name, ht.table_name)::regclass
                AND parent.conname = cc.hypertable_constraint_name
                AND parent.contype IN ('f', 'u', 'p', 'x', 't')
            WHERE cc.chunk_id = cid
              AND cc.dimension_slice_id IS NULL
              AND cc.hypertable_constraint_name IS NOT NULL
        LOOP
            IF r.old_name <> r.new_name THEN
                EXECUTE pg_catalog.format('ALTER TABLE %s RENAME CONSTRAINT %I TO %I',
                                          r.chunk_table, r.old_name, r.new_name);
                -- Keep the catalog in sync for 2.27.2.
                UPDATE _timescaledb_catalog.chunk_constraint
                   SET constraint_name = r.new_name
                 WHERE chunk_id = cid
                   AND constraint_name = r.old_name;
            END IF;
        END LOOP;

        processed := processed + 1;
        IF processed % batch_size = 0 THEN
            COMMIT;
            RAISE NOTICE 'renamed chunk constraints: % / % chunks', processed, total;
        END IF;
    END LOOP;

    COMMIT;
    IF processed % batch_size <> 0 THEN
        RAISE NOTICE 'renamed chunk constraints: % / % chunks', processed, total;
    END IF;
END;
$$;

set timescaledb.restoring to on;
CALL pg_temp.rename_chunk_constraints();
set timescaledb.restoring to off;
DROP PROCEDURE pg_temp.rename_chunk_constraints;
