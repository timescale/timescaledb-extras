-- Fix compressed chunk schema by re-adding sequence number column and
-- setting the chunks as unordered because the sequence number values are going to be missing

-- This function updates existing fully compressed chunk that does not contain
-- sequence number metadata. Steps to fix the chunk:
-- - Add sequence number column to the compressed chunk.
-- - Drop all indexes on the compressed chunk.
-- - Create new index based on segmentby and sequence number columns.
-- - Mark the chunk as unordered.
--
-- Returns successfulness status of the operation.
--
-- Last step will disable some optimizations so a warning should be printed that
-- this chunk should be fully recompressed in order to restore the sequence number values.
CREATE OR REPLACE FUNCTION pg_temp.add_sequence_number_metadata_column(
	comp_ch_schema_name text,
	comp_ch_table_name text
)
	RETURNS BOOL LANGUAGE PLPGSQL AS
$BODY$
DECLARE
	chunk_schema_name text;
	chunk_table_name text;
	index_name text;
	segmentby_columns text;
BEGIN
	SELECT ch.schema_name, ch.table_name INTO STRICT chunk_schema_name, chunk_table_name
	FROM _timescaledb_catalog.chunk ch
	INNER JOIN  _timescaledb_catalog.chunk comp_ch
	ON ch.compressed_chunk_id = comp_ch.id
	WHERE comp_ch.schema_name = comp_ch_schema_name
	AND comp_ch.table_name = comp_ch_table_name;

    IF NOT FOUND THEN
		RAISE USING
			ERRCODE = 'feature_not_supported',
			MESSAGE = 'Cannot migrate compressed chunk to version 2.16.1, chunk not found';
    END IF;

	-- Add sequence number column to compressed chunk
	EXECUTE format('ALTER TABLE %s.%s ADD COLUMN _ts_meta_sequence_num INT DEFAULT NULL', comp_ch_schema_name, comp_ch_table_name);

	-- Remove all indexes from compressed chunk
	FOR index_name IN
		SELECT format('%s.%s', i.schemaname, i.indexname)
		FROM pg_indexes i
		WHERE i.schemaname = comp_ch_schema_name
		AND i.tablename = comp_ch_table_name
	LOOP
		EXECUTE format('DROP INDEX %s;', index_name);
	END LOOP;

	-- Fetch the segmentby columns from compression settings
	SELECT string_agg(cs.segmentby_column, ',') INTO segmentby_columns
	FROM ( 
		SELECT unnest(segmentby)
		FROM _timescaledb_catalog.compression_settings
		WHERE relid = format('%s.%s', comp_ch_schema_name, comp_ch_table_name)::regclass::oid
		AND segmentby IS NOT NULL
	) AS cs(segmentby_column);

	-- Create compressed chunk index based on sequence num metadata column
	-- if there are segmentby columns in the settings
    IF FOUND AND segmentby_columns IS NOT NULL THEN
		EXECUTE format('CREATE INDEX ON %s.%s (%s, _ts_meta_sequence_num);', comp_ch_schema_name, comp_ch_table_name, segmentby_columns);
    END IF;


	-- Mark compressed chunk as unordered
	UPDATE _timescaledb_catalog.chunk
	SET status = status | 2 -- set unordered bit
	WHERE schema_name = chunk_schema_name
	AND table_name = chunk_table_name;
	
	RETURN true;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

DO $$
DECLARE
	chunk_record record;
BEGIN
	FOR chunk_record IN
	SELECT comp_ch.*
	FROM _timescaledb_catalog.chunk ch 
	INNER JOIN _timescaledb_catalog.chunk comp_ch
	ON ch.compressed_chunk_id = comp_ch.id
	WHERE not exists (
		SELECT 
		FROM pg_attribute att 
		WHERE attrelid=format('%I.%I',comp_ch.schema_name,comp_ch.table_name)::regclass 
		AND attname='_ts_meta_sequence_num') 
		AND NOT ch.dropped
	LOOP
		IF pg_temp.add_sequence_number_metadata_column(chunk_record.schema_name, chunk_record.table_name)
		THEN
			RAISE LOG 'Migrated compressed chunk %s.%s to version 2.16.1', chunk_record.schema_name, chunk_record.table_name;
		END IF;
	END LOOP;

	RAISE LOG 'Migration successful!';
END
$$;

DROP FUNCTION pg_temp.add_sequence_number_metadata_column(text, text);
