-- This utility is for helping to make major DDL changes to compressed 
-- hypertables. Because you must decompress the entire hypertable, one
-- might not have sufficient space to do that. An alternate method is to
-- copy the data from one table to a new table with the correct schema
-- or constraints, etc. This tool helps you do that chunk by chunk,
-- compressing as you go to greatly reduce the space requirements to
-- perform the migration.
--
-- Prerequisites:
--   The target table must be created first, and compression must be
--   configured for it.
--    
--   You must have sufficient space for the compressed data, so roughly
--   double your current hypertable size, which is usually still
--   considerably smaller than all the uncompressed data.
--
-- Variables:
--   old_table is the original table to copy from
--   new_table is the table you are copying to
--   older_than is the limit for the chunks to recompress. It will
--   recompress all chunks older than the interval specified
--
-- Limitations:
--   This tool does not re-create policies, continuous aggregates, or
--   other dependent objects. It only copies the data to a new table.
--
-- Usage:
--   CALL migrate_data('copy_from', 'copy_to', '30 days');
--
 


CREATE OR REPLACE procedure migrate_data(
	old_table text, 
	new_table text, 
	older_than interval)
language plpgsql
as 
$$
DECLARE
c_row record;
c_curs cursor for 
select * from chunknames;
last_chunk text;

BEGIN

--create temp table for storing original hypertable chunk names
CREATE TEMPORARY TABLE chunknames (chunkname text);

insert into chunknames
(select * from show_chunks(old_table));

-- loop through (one at a time) the chunks and copy the data to the new 
-- hypertable
FOR c_row in c_curs LOOP
	EXECUTE format('insert into %I
	(select * from %s)', new_table, c_row.chunkname);
	
	-- get most recent chunk from the new hypertable after copying the 
	-- whole other chunk is finished.
	-- it will only grab chunks to drop up to the older_than interval 
	-- specified.
	select a.show_chunks into last_chunk from 
	(select * from 
	show_chunks('copyto', older_than => older_than)
    order by show_chunks DESC
    LIMIT 1) a;
	
	RAISE NOTICE 'Copied Chunk % into %', t_row.chunkname, last_chunk;
	
	-- compress that last chunk. 
	PERFORM compress_chunk(last_chunk);
	
	RAISE NOTICE 'Compressed %', last_chunk;
	END LOOP;
	

drop table chunknames;
END;
$$;
