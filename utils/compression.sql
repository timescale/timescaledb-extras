-- Update metadata for chunks compressed before 2.0.
--
-- After you have updated from pre-2.0 it might be that some
-- compressed chunks are missing information about the approximate row
-- count. To correct that, call this function. Note that it will only
-- update approximate row counts that are NULL, so running it several
-- times will not overwrite existing values.
--
-- To avoid blocking too many tables, it will commit after each chunk,
-- which means that the function might be slow if there is a lot of
-- data to update, but it will not block other queries.
CREATE PROCEDURE fix_compression_row_count_stats ()
LANGUAGE PLPGSQL
AS $$
DECLARE
  plain_chunk RECORD;
  comp_chunk TEXT;
  rowcount_pre BIGINT;
  rowcount_post BIGINT;
BEGIN
  FOR plain_chunk IN
      SELECT chunk_id, compressed_chunk_id
        FROM _timescaledb_catalog.compression_chunk_size
       WHERE numrows_pre_compression IS NULL OR numrows_post_compression IS NULL
  LOOP
      SELECT format('%I.%I', schema_name, table_name) INTO comp_chunk
      FROM _timescaledb_catalog.chunk
      WHERE id = plain_chunk.compressed_chunk_id;
      
      EXECUTE format('SELECT sum(_ts_meta_count), count(*) FROM %s', comp_chunk) INTO rowcount_pre, rowcount_post;

      UPDATE _timescaledb_catalog.compression_chunk_size
         SET numrows_post_compression = rowcount_post
       WHERE chunk_id = plain_chunk.chunk_id
         AND (numrows_post_compression IS NULL OR numrows_pre_compression IS NULL);
       COMMIT;
    END LOOP;
END
$$;
