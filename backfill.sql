-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE and LICENSE for copyright and licensing information.

-- A set of functions and procedures to help backfill data into compressed ranges
-- All assume that whatever schema TimescaleDB is installed in, it is in the search_path at the time of run

/* 
**** USING BACKFILL **** 
*
*The backfill procedure is useful for backfilling data into TimescaleDB hypertables that
*have compressed chunks. To use it, first insert the data you wish to backfill into a
*temporary (or normal) table that has the same schema as the hypertable we are backfilling
*into. Then call the decompress_backfill procedure with the staging table and hypertables.
*
*
*As an example, suppose we have a hypertable called `cpu` we create a temporary table by
* running something like:
*
*`CREATE TEMPORARY TABLE cpu_temp AS SELECT * FROM cpu WITH NO DATA;` 
*
* Then we can call our backfill procedure:
*
*` CALL decompress_backfill(staging_table=>'cpu_temp'::regclass,
*   destination_hypertable=>'cpu'::regclass );`
*
* And it will backfill into the cpu hypertable. 
*
* We recommend creating an index on the time column of the temporary table, to make scans
* of that table in certain ranges faster. (ie `CREATE INDEX ON cpu_temp(time);`)
*/

---- Some helper functions and procedures before the main event
CREATE OR REPLACE FUNCTION get_schema_and_table_name(IN regclass, OUT nspname name, OUT relname name) AS $$
    SELECT n.nspname, c.relname  
    FROM pg_class c INNER JOIN pg_namespace n ON c.relnamespace = n.oid 
    WHERE c.oid = $1::oid
$$ LANGUAGE SQL STABLE;

-- decompress all chunks in a dimension slice
CREATE OR REPLACE PROCEDURE decompress_dimension_slice(IN dimension_slice_row _timescaledb_catalog.dimension_slice, INOUT chunks_decompressed bool) 
AS $$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    chunks_decompressed = false;
    FOR chunk_row IN 
        SELECT c.*
        FROM _timescaledb_catalog.chunk_constraint cc INNER JOIN _timescaledb_catalog.chunk c ON cc.chunk_id = c.id 
        WHERE cc.dimension_slice_id = dimension_slice_row.id
        AND c.compressed_chunk_id IS NOT NULL
        AND NOT c.dropped
    LOOP 
        RAISE NOTICE 'Decompressing chunk: %.%', chunk_row.schema_name, chunk_row.table_name;
        PERFORM decompress_chunk(format('%I.%I', chunk_row.schema_name, chunk_row.table_name)::regclass);
        -- Actually got a chunk decompressed, so we'll set this to true now. We only want to recompress chunks in slices that were already compressed.
        chunks_decompressed = true;
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;

-- compress all chunks in a dimension slice
CREATE OR REPLACE PROCEDURE compress_dimension_slice(dimension_slice_row _timescaledb_catalog.dimension_slice) 
AS $$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    FOR chunk_row IN 
        SELECT c.*
        FROM _timescaledb_catalog.chunk_constraint cc INNER JOIN _timescaledb_catalog.chunk c ON cc.chunk_id = c.id 
        WHERE cc.dimension_slice_id = dimension_slice_row.id 
        AND c.compressed_chunk_id IS NULL
        AND NOT c.dropped
    LOOP 
        RAISE NOTICE 'Compressing chunk: %.%', chunk_row.schema_name, chunk_row.table_name;
        PERFORM compress_chunk(format('%I.%I', chunk_row.schema_name, chunk_row.table_name)::regclass);
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION move_compression_job(IN hypertable_id int, IN schema_name name, IN table_name name, IN new_time timestamptz, OUT old_time timestamptz) 
AS $$
DECLARE
    compression_job_id int;
    version int;
BEGIN
    SELECT split_part(extversion, '.', 1)::INT INTO version FROM pg_catalog.pg_extension WHERE extname='timescaledb' LIMIT 1;

    IF version = 1 THEN
        SELECT job_id INTO compression_job_id FROM _timescaledb_config.bgw_policy_compress_chunks b WHERE b.hypertable_id = move_compression_job.hypertable_id; 
    ELSE
        SELECT s.job_id INTO compression_job_id FROM timescaledb_information.jobs j
          INNER JOIN timescaledb_information.job_stats s ON j.job_id = s.job_id
          WHERE j.proc_name = 'policy_compression' AND s.hypertable_schema = schema_name AND s.hypertable_name = table_name;
    END IF;

    IF compression_job_id IS NULL THEN 
        old_time = NULL::timestamptz;
    ELSE
        SELECT next_start INTO old_time FROM _timescaledb_internal.bgw_job_stat WHERE job_id = compression_job_id;

        IF version = 1 THEN
            PERFORM alter_job_schedule(compression_job_id, next_start=> new_time);
        ELSE 
            PERFORM alter_job(compression_job_id, next_start=> new_time);
        END IF;
    END IF;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- The main event
-- staging_table is the (possibly temporary) table from which rows will be moved 
-- destination_hypertable is the table where rows will be moved
-- on_conflict action controls how duplicate rows are handled on insert, it has 3 allowed values that correspond to the ON CONFLICT actions in Postgres, 'NOTHING' (default), 'UPDATE', 'RESTRICT'. Their actions:
--   - NOTHING: ignore conflicting rows, the first inserted takes precedence
--   - UPDATE: replace values in the conflicting row according to the *on_conflict_update_columns parameter*, if this is set, the *on_conflict_update_columns* parameter must be set
--   - RESTRICT: error if there is a conflicting insert
-- delete_from_staging specifies whether we should delete from the staging table as we go or leave rows there
-- compression_job_push_interval specifies how long push out the compression job as we are running, ie the max amount of time you expect the backfill to take
-- on_conflict_update_columns is an array of columns to use in the update clause when a conflict arises and the on_conflict_action is set to 'Update'
CREATE OR REPLACE PROCEDURE decompress_backfill(staging_table regclass, 
    destination_hypertable regclass, 
    on_conflict_action text DEFAULT 'NOTHING', 
    delete_from_staging bool DEFAULT true, 
    compression_job_push_interval interval DEFAULT '1 day',
    on_conflict_update_columns text[] DEFAULT '{}')
AS $proc$
DECLARE
    source text := staging_table::text; -- Forms a properly quoted table name from our regclass
    dest   text := destination_hypertable::text;
    
    dest_nspname name;
    dest_relname name;
    
    hypertable_row _timescaledb_catalog.hypertable;
    dimension_row _timescaledb_catalog.dimension;
    dimension_slice_row _timescaledb_catalog.dimension_slice;
    
    min_time_internal bigint;
    max_time_internal bigint;
    
    unformatted_move_stmt text ;
    on_conflict_clause text := '';

    r_start text := NULL;
    r_end text := NULL;
    r_end_prev text := NULL;
    affected bigint;

    old_compression_job_time timestamptz;
    chunks_decompressed bool;
    
BEGIN
    SELECT (get_schema_and_table_name(destination_hypertable)).* INTO STRICT dest_nspname, dest_relname;
    --This should throw an error if we can't cast the staging table's type into the hypertable's type, which means the inserts won't work. 
    EXECUTE FORMAT('SELECT row(h.*)::%1$s FROM %2$s AS h LIMIT 1', source, dest);

    -- Make sure our source table has been analyzed so our selects are better later
    EXECUTE FORMAT('ANALYZE %s', source);
    --Get our hypertable
    SELECT h.* INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable h 
    WHERE table_name = dest_relname AND schema_name = dest_nspname ;
    
    --And our time dimension, which is always the first dimension
    SELECT d.* INTO STRICT dimension_row FROM _timescaledb_catalog.dimension d WHERE hypertable_id = hypertable_row.id ORDER BY id LIMIT 1 ;
    
    -- Push the compression job out for some period of time so we don't end up compressing a decompressed chunk 
    -- Don't disable completely because at least then if we fail and fail to move it back things won't get completely weird
    SELECT move_compression_job(hypertable_row.id, hypertable_row.schema_name, hypertable_row.table_name, now() + compression_job_push_interval) INTO old_compression_job_time;

    --Get the min and max times in timescale internal format from the source table, this will tell us which chunks we need to decompress
    EXECUTE FORMAT($$SELECT _timescaledb_internal.time_to_internal(min(%1$I)) , 
        _timescaledb_internal.time_to_internal(max(%1$I)) 
        FROM %2$s $$, dimension_row.column_name, source)
        INTO STRICT min_time_internal, max_time_internal;
    
    --Set up our move statement to be used with the right formatting in each of the loop executions
    -- Note that the table names and literal time values are properly formatted outside and so are 
    -- passed in as raw strings. We cannot re-format as they will then have extra quotes.
    IF delete_from_staging THEN 
        unformatted_move_stmt = $$  
            WITH to_insert AS (DELETE 
            FROM %1$s --source table
            WHERE %2$I >= %3$s -- time column >= range start
            AND %2$I < %4$s -- time column <= range end
            RETURNING * )
            INSERT INTO %5$s 
            SELECT * FROM to_insert
            %6$s -- ON CONFLICT CLAUSE if it exists
            $$;
    ELSE
        unformatted_move_stmt = $$  
            WITH to_insert AS (SELECT *
            FROM %1$s --source table
            WHERE %2$I >= %3$s -- time column >= range start
            AND %2$I < %4$s) -- time column <= range end)
            INSERT INTO %5$s 
            SELECT * FROM to_insert
            %6$s -- ON CONFLICT CLAUSE if it exists
            $$;
    END IF;

    IF UPPER(on_conflict_action) = 'NOTHING' THEN
        on_conflict_clause = 'ON CONFLICT DO NOTHING';
    ELSEIF UPPER(on_conflict_action) = 'UPDATE' THEN
        SELECT 'ON CONFLICT DO UPDATE SET ' || STRING_AGG(FORMAT('%1$I = EXCLUDED.%1$I', on_conflict_update_column), ', ')
        FROM UNNEST(on_conflict_update_columns) AS on_conflict_update_column INTO on_conflict_clause;
    END IF;

    --Loop through the dimension slices that that are impacted
    FOR dimension_slice_row IN 
        SELECT ds.* 
        FROM _timescaledb_catalog.dimension_slice ds 
        WHERE dimension_id = dimension_row.id
        -- find the dimension slices that overlap with the data in our staging table 
        -- the range_ends are non inclusive, the range_starts are inclusive
        AND max_time_internal >= ds.range_start AND min_time_internal < ds.range_end
        ORDER BY ds.range_end
    LOOP
        -- decompress the chunks in the dimension slice, committing transactions after each decompress
        CALL decompress_dimension_slice(dimension_slice_row, chunks_decompressed);


        --Set the previous r_end, so that we can insert from the previous (or the min) to
        --the start, this will catch any rows that are in the source table for which we
        --haven't yet made a chunk in the dest hypertable. 
        r_end_prev = COALESCE(r_end, _timescaledb_internal.time_literal_sql(min_time_internal, dimension_row.column_type));
        -- now actually move rows
        r_start = _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimension_row.column_type);
        r_end = _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type);
        
        -- catch any stray rows that fall into a chunk that doesn't exist yet by expanding
        -- our range to the lower of r_end_prev and r_start, there is a case where r_start
        -- can be lower, which is if r_end_prev was actually the minimum in the in the
        -- source table.  We won't compress the new chunks that are created, the
        -- compression job will pick those up when we re-activate it.
        r_start =LEAST(r_end_prev, r_start);

        EXECUTE FORMAT(unformatted_move_stmt
            , source 
            , dimension_row.column_name
            , r_start 
            , r_end
            , dest 
            , on_conflict_clause
            );
        GET DIAGNOSTICS affected = ROW_COUNT;
        RAISE NOTICE '% rows moved in range % to %', affected, r_start, r_end ;
        COMMIT;
        -- recompress the chunks in the dimension slice, committing transactions after each recompress
        IF chunks_decompressed THEN
            CALL compress_dimension_slice(dimension_slice_row);
        END IF;
    END LOOP;

    -- catch any stray rows that fall into new chunks that need to be created between our
    -- final chunk and the max in the source table, We won't compress the new chunks that are
    -- created, the job will pick those up when we re-activate it.
    r_start = COALESCE(r_end, _timescaledb_internal.time_literal_sql(min_time_internal, dimension_row.column_type)); --if there were no rows inserted into a chunk, r_end wouldn't be defined.
    r_end = _timescaledb_internal.time_literal_sql(max_time_internal+1, dimension_row.column_type); -- add one here, so that we can still use < rather than <= (our internal representation is a bigint)
    EXECUTE FORMAT(unformatted_move_stmt
        , source 
        , dimension_row.column_name
        , r_start
        , r_end
        , dest 
        , on_conflict_clause
        );
    GET DIAGNOSTICS affected = ROW_COUNT;
    RAISE NOTICE '% rows moved in range % to %', affected, r_start, r_end ;
    COMMIT;
--Move our job back to where it was
SELECT move_compression_job(hypertable_row.id, hypertable_row.schema_name, hypertable_row.table_name, old_compression_job_time) INTO old_compression_job_time;
COMMIT;
END;

$proc$
LANGUAGE PLPGSQL;
