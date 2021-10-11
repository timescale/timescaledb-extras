-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE and LICENSE for copyright and licensing information.

-- A procedure to migrate data from a normal table to a hypertable or between hypertables. 
-- For now the source and target tables must have the same columns in the same order.

/* ***** USING MIGRATE *****
*
* First create a hypertable with the proper schema (same columns in the same order, constraints/indexes etc may be different)
* Then create the procedures by running this entire file (or copy/pasting it)
* We recommend creating an index on the time partitioning column on the source table
* Then call the migrate_to_hypertable procedure with your source / sink tables {FINISH}
* If you would like to use parallelism in your 
*/
-- ***** HELPER FUNCTIONS *****

---- Some helper functions and procedures before the main event
CREATE OR REPLACE FUNCTION get_schema_and_table_name(IN regclass, OUT nspname name, OUT relname name) AS $$
    SELECT n.nspname, c.relname  
    FROM pg_class c INNER JOIN pg_namespace n ON c.relnamespace = n.oid 
    WHERE c.oid = $1::oid
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION get_dimension_details(sink_table REGCLASS) 
RETURNS _timescaledb_catalog.dimension AS
$$
DECLARE
    dimension_row _timescaledb_catalog.dimension;
    
BEGIN
    --And our time dimension, which is always the first dimension
    SELECT d.* INTO STRICT dimension_row FROM _timescaledb_catalog.dimension d 
    WHERE hypertable_id = (select id from _timescaledb_catalog.hypertable WHERE (schema_name, table_name) = get_schema_and_table_name(sink_table))
     ORDER BY d.id LIMIT 1;
    
    RETURN dimension_row;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION make_log_table(log_table_name NAME,  
    parallel_worker_num int, 
    source_table regclass,
    sink_table regclass, 
    batch_time interval DEFAULT NULL) 
RETURNS VOID 
as $func$
DECLARE
    create_table_stmt TEXT;

    dimension_row _timescaledb_catalog.dimension;

    min_time_internal bigint;
    max_time_internal bigint;
    interval_internal bigint;
    
    sql_text text;
BEGIN
    SELECT (get_dimension_details(sink_table)).* INTO STRICT dimension_row;
    
    --Get the min and max times in timescale internal format from the source table, this will tell us which chunks we need to decompress
    sql_text:=FORMAT($$SELECT _timescaledb_internal.time_to_internal(min(%1$I)) , 
        _timescaledb_internal.time_to_internal(max(%1$I)) 
        FROM %2$s $$, dimension_row.column_name, source_table);

    EXECUTE sql_text
    INTO STRICT min_time_internal, max_time_internal;
    
    SELECT FORMAT($$
    CREATE TABLE IF NOT EXISTS %1$I (
        start_t BIGINT NOT NULL,
        end_t BIGINT NOT NULL,
        parallel_worker_num int,
        migrated bool DEFAULT FALSE, 
        PRIMARY KEY (parallel_worker_num, start_t)
        )
    $$, log_table_name) INTO create_table_stmt;

   -- Execute the CREATE TABLE
   EXECUTE create_table_stmt;

   -- populate the table with rows for each chunk
   -- of data to process. We default to 1/10th the 
   -- chunk_time_interval is currently set to for this
   -- hypertable for "INSERT INTO... SELECT..." statements 
   -- unless another interval size is provided
   IF batch_time IS NOT NULL THEN
     -- convert the passed in interval to microseconds
     SELECT EXTRACT(epoch from batch_time)*100000 into interval_internal;
   ELSE
     SELECT dimension_row.interval_length/10 into interval_internal;
   END IF;


EXECUTE FORMAT($$
    INSERT INTO %1$I (start_t, end_t, parallel_worker_num, migrated) 
    SELECT s as start_t, s + %4$s as end_t, %5$s::INT as parallel_worker_num, false 
FROM (select generate_series(%2$s, %3$s,%4$s) as s) f ON CONFLICT DO NOTHING $$, 
log_table_name, min_time_internal,max_time_internal, interval_internal,parallel_worker_num);


END;
$func$ LANGUAGE PLPGSQL VOLATILE;


-- ***** Now the main event *****
CREATE OR REPLACE PROCEDURE 
migrate_to_hypertable(
    source_table regclass,
    sink_table regclass, 
    batch_time interval DEFAULT NULL, -- default to 1/10 chunk size for hypertable
    parallelize_column TEXT DEFAULT NULL, -- default null
    parallel_workers int DEFAULT NULL, -- default null
    parallel_worker_num int DEFAULT 0) 
AS $proc$
DECLARE
    sink_dim _timescaledb_catalog.dimension;
    log_table_name TEXT;
    select_next_row_stmt TEXT;
    move_statement TEXT;
    update_statement TEXT;
    next_row RECORD;
    done bool := false;
    r_start TEXT;
    r_end TEXT;
    affected BIGINT;

BEGIN
    -- This is used for mod-ing, which is zero-based. For simplicity,
    -- we kept this 1-based so that users wouldn't be confused at runtime.
    parallel_worker_num = parallel_worker_num-1;
    SELECT (get_dimension_details(sink_table)).* INTO sink_dim;
    SELECT FORMAT('_ts_migrate_log_%1$s', sink_table::oid ) INTO log_table_name;
    PERFORM make_log_table(log_table_name, parallel_worker_num, source_table, sink_table, batch_time);


    -- we use skip locked to make this parallelism stuff work well
    SELECT FORMAT($$ SELECT l.* FROM %1$I as l 
        WHERE NOT migrated AND parallel_worker_num = %2$s  
        ORDER BY start_t ASC LIMIT 1 
        FOR UPDATE SKIP LOCKED$$, log_table_name, parallel_worker_num) INTO select_next_row_stmt;

    -- The extra percent character before the two variables below allow this prepared
    -- statement to be reused further down, and now "%%1" & "%%2" will be replaced/formatted
    -- in the next statement with timestamps
    SELECT FORMAT($$ INSERT INTO %1$s
            SELECT * FROM %2$s 
            WHERE %3$I >= %%1$s AND %3$I < %%2$s
            $$ , 
            sink_table, source_table, sink_dim.column_name) 
    INTO move_statement;
    
    IF parallelize_column IS NOT NULL AND parallel_workers IS NOT NULL AND parallel_worker_num IS NOT NULL THEN
        SELECT move_statement || FORMAT($$ AND mod(_timescaledb_internal.get_partition_hash(%1$I), %2$s) = %3$s $$,  parallelize_column, parallel_workers, parallel_worker_num)
        INTO move_statement;
    END IF; 

    SELECT FORMAT($$UPDATE %1$I SET migrated = true WHERE start_t = %%1$s AND parallel_worker_num = %%2$s::int$$, log_table_name) 
    INTO update_statement;

    COMMIT;

    EXECUTE select_next_row_stmt INTO next_row;
    IF next_row IS NULL THEN 
        done = true;
    END IF;

    WHILE NOT done LOOP 
        r_start = _timescaledb_internal.time_literal_sql(next_row.start_t, sink_dim.column_type);
        r_end = _timescaledb_internal.time_literal_sql(next_row.end_t, sink_dim.column_type);
        RAISE DEBUG '% Moving times FROM % to % worker %', now(), r_start, r_end, parallel_worker_num +1;
        EXECUTE FORMAT(move_statement,  r_start, r_end);
        GET DIAGNOSTICS affected = ROW_COUNT;
        EXECUTE FORMAT(update_statement, next_row.start_t, parallel_worker_num);
        RAISE DEBUG '% Moved % rows  FROM % to % worker %', now(), affected, r_start, r_end, parallel_worker_num +1; 
        COMMIT;
        EXECUTE select_next_row_stmt INTO next_row;
        IF next_row IS NULL THEN 
            done = true;
        END IF;
        
    END LOOP;
    RAISE NOTICE '% No more work to do for worker %', now(), parallel_worker_num+1;
    COMMIT;
END;
$proc$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION drop_migrate_log(sink_table regclass) 
RETURNS VOID 
AS $func$
BEGIN
    EXECUTE FORMAT('DROP TABLE IF EXISTS _ts_migrate_log_%1$s', sink_table::oid );
END;
$func$ LANGUAGE PLPGSQL VOLATILE;

