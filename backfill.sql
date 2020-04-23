-- A set of functions and procedures to help backfill data into compressed ranges
-- All assume that whatever schema TimescaleDB is installed in, it is in the search_path at the time of run


---- Some helper functions and procedures before the main event
CREATE OR REPLACE FUNCTION get_schema_and_table_name(regclass) RETURNS table(nspname name, relname name) AS $$
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
        PERFORM decompress_chunk(format('%s.%s', chunk_row.schema_name, chunk_row.table_name)::regclass);
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
        PERFORM compress_chunk(format('%s.%s', chunk_row.schema_name, chunk_row.table_name)::regclass);
        COMMIT;
    END LOOP;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION move_compression_job(IN hypertable_id int, IN new_time timestamptz, OUT old_time timestamptz) 
AS $$
DECLARE
    compression_job_id int;
BEGIN
    SELECT job_id INTO compression_job_id FROM _timescaledb_config.bgw_policy_compress_chunks b WHERE b.hypertable_id = move_compression_job.hypertable_id;
    IF compression_job_id IS NULL THEN 
        old_time = NULL::timestamptz;
    ELSE
        SELECT next_start INTO old_time FROM _timescaledb_internal.bgw_job_stat WHERE job_id = compression_job_id;
        PERFORM alter_job_schedule(compression_job_id, next_start=> new_time);
    END IF;
END;
$$ LANGUAGE PLPGSQL VOLATILE;

-- The main event
-- Specify your staging table from which rows will be deleted as they are moved
CREATE OR REPLACE PROCEDURE decompress_backfill(staging_table regclass, 
    destination_hypertable regclass, 
    on_conflict_do_nothing bool DEFAULT true, 
    delete_from_staging bool DEFAULT true, 
    compression_job_push_interval interval DEFAULT '1 day')
AS $proc$
DECLARE
    source text := staging_table::text;
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

    r_start text;
    r_end text;
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
    SELECT move_compression_job(hypertable_row.id, now() + compression_job_push_interval) INTO old_compression_job_time;

    --Get the min and max times in timescale internal format from the source table, this will tell us which chunks we need to decompress
    EXECUTE FORMAT($$SELECT _timescaledb_internal.time_to_internal(min(%1$s)) , 
        _timescaledb_internal.time_to_internal(max(%1$s)) 
        FROM %2$s $$, dimension_row.column_name, source)
        INTO STRICT min_time_internal, max_time_internal;
    
    --Set up our move statement to be used with the right formatting in each of the loop executions
    IF delete_from_staging THEN 
        unformatted_move_stmt = $$  
            WITH to_insert AS (DELETE 
            FROM %1$s --source table
            WHERE %2$s >= %3$s -- time column >= range start
            AND %2$s <= %4$s -- time column <= range end
            RETURNING * )
            INSERT INTO %5$s 
            SELECT * FROM to_insert
            %6$s -- ON CONFLICT CLAUSE if it exists
            $$;
    ELSE
        unformatted_move_stmt = $$  
            WITH to_insert AS (SELECT *
            FROM %1$s --source table
            WHERE %2$s >= %3$s -- time column >= range start
            AND %2$s <= %4$s) -- time column <= range end)
            INSERT INTO %5$s 
            SELECT * FROM to_insert
            %6$s -- ON CONFLICT CLAUSE if it exists
            $$;
    END IF;

    IF on_conflict_do_nothing THEN
        on_conflict_clause = 'ON CONFLICT DO NOTHING';
    END IF;

    --Loop through the dimension slices that that are impacted
    FOR dimension_slice_row IN 
        SELECT ds.* 
        FROM _timescaledb_catalog.dimension_slice ds 
        WHERE dimension_id = dimension_row.id
        -- find the dimension slices that overlap with the data in our staging table 
        -- the range_ends are non inclusive, the range_starts are inclusive
        AND max_time_internal >= ds.range_start AND min_time_internal < ds.range_end
    LOOP
        -- decompress the chunks in the dimension slice, committing transactions after each decompress
        CALL decompress_dimension_slice(dimension_slice_row, chunks_decompressed);

        -- now actually move rows
        r_start = _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimension_row.column_type);
        r_end = _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type);

        EXECUTE FORMAT(unformatted_move_stmt
            , source -- source table name (already formatted properly)
            , quote_ident(dimension_row.column_name) -- have to wrap our column name in quote ident
            , r_start
            , r_end
            , dest -- dest hypertable name (already formatted properly) 
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

--Move our job back to where it was
SELECT move_compression_job(hypertable_row.id, old_compression_job_time) INTO old_compression_job_time;
COMMIT;
END;

$proc$
LANGUAGE PLPGSQL;
