-- Downgrade the adaptive integer compression (AIC) introduced in TimescaleDB 2.31.0.
--
-- AIC (compression algorithm id 8) is disabled by default in 2.31.0 and is only used
-- when `timescaledb.enable_aic_compression` was switched on. While it is on, every
-- int2, int4, int8, date, timestamp and timestamptz column is compressed with AIC
-- instead of deltadelta. Earlier TimescaleDB versions cannot read AIC batches, so
-- chunks holding them must be converted back before the extension is downgraded.
--
-- Run this script ON 2.31.0 (the version that understands AIC), BEFORE
-- `ALTER EXTENSION timescaledb UPDATE TO '<previous version>'`.
--
-- What it does:
--
--   1. Finds every compressed chunk that holds at least one AIC batch. AIC cannot be
--      enabled per column: all integer-like columns of a batch are compressed in the
--      same call under the same GUC value, and the AIC compressor never falls back
--      to another algorithm. So the algorithm of any one non-NULL integer-like value
--      in a batch is the algorithm of all of them. The script therefore runs ONE
--      probe per chunk: for each batch it reads the algorithm of the first
--      integer-like column that actually holds values (a column that is all NULL in
--      a batch is stored as a null-compression marker and is skipped) and stops at
--      the first AIC batch found. The primary dimension column is tried first; it
--      is NOT NULL by construction, so in practice it is one call per batch.
--
--   2. Switches `timescaledb.enable_aic_compression` off for this session and, for
--      every chunk found, decompresses it and compresses it again, now with
--      deltadelta. Each chunk is committed on its own, so the script must run with
--      autocommit on and NOT inside a BEGIN ... COMMIT block (psql's default is
--      fine). It can be re-run: chunks already converted are not found again.
--
-- Notes:
--
--   * Run as a superuser or the owner of all affected hypertables.
--   * Pause compression and retention policies while this runs, otherwise a policy
--     may compress new data with AIC behind the script's back (the policy's session
--     still sees the GUC as on until it is reset persistently).
--   * Before downgrading, RESET `timescaledb.enable_aic_compression` wherever it is
--     persisted (postgresql.conf, ALTER SYSTEM, ALTER DATABASE, ALTER ROLE). The
--     downgrade script refuses to run while it is on and names the place.

--
-- Phase 1: find the chunks that hold AIC batches, one probe per chunk
--
DO $$
DECLARE
	chunk_rec RECORD;
	dim_column NAME;
	probe_expr TEXT;
	found BOOLEAN;
	n_chunks INT := 0;
	n_probed INT := 0;
	n_hits INT := 0;
BEGIN
	-- stores the aic_chunk information for recompression in phase 2
	CREATE TEMP TABLE IF NOT EXISTS aic_chunks (
		hypertable REGCLASS,
		chunk REGCLASS
	);
	TRUNCATE aic_chunks;

	FOR chunk_rec IN
		SELECT
			ht.id AS hypertable_id,
			format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
			ch.relid AS chunk,
			cs.compress_relid AS compressed_chunk
		FROM _timescaledb_catalog.hypertable ht
		INNER JOIN _timescaledb_catalog.chunk ch ON ch.hypertable_id = ht.id
		INNER JOIN _timescaledb_catalog.compression_settings cs ON cs.relid = ch.relid
		WHERE cs.compress_relid IS NOT NULL
		  AND NOT ch.osm_chunk
		ORDER BY ht.id, ch.id
	LOOP
		n_chunks := n_chunks + 1;

		-- the primary (first) dimension of the hypertable, usually the time column
		SELECT d.column_name INTO dim_column
		FROM _timescaledb_catalog.dimension d
		WHERE d.hypertable_id = chunk_rec.hypertable_id
		ORDER BY d.id
		LIMIT 1;

		-- integer-like columns that are compressed in this chunk are
                -- collected here. primary dimension first, then NOT NULL columns.
                --
		SELECT string_agg(
		         format('nullif((SELECT algorithm FROM _timescaledb_functions.compressed_data_info(x.%I)), ''NULL'')', c.attname),
		         ', ' ORDER BY (c.attname = dim_column) DESC, u.attnotnull DESC, c.attnum)
		  INTO probe_expr
		FROM pg_catalog.pg_attribute c
		INNER JOIN pg_catalog.pg_attribute u
			ON u.attrelid = chunk_rec.chunk AND u.attname = c.attname AND NOT u.attisdropped
		WHERE c.attrelid = chunk_rec.compressed_chunk
		  AND c.attnum > 0
		  AND NOT c.attisdropped
		  AND c.atttypid = '_timescaledb_internal.compressed_data'::regtype
		  AND u.atttypid = ANY (ARRAY['int2', 'int4', 'int8', 'date', 'timestamp', 'timestamptz']::regtype[]);

		IF probe_expr IS NULL THEN
			CONTINUE; -- no integer-like compressed column, the chunk cannot hold AIC
		END IF;
		probe_expr := format('coalesce(%s) = %L', probe_expr, 'AIC');

		n_probed := n_probed + 1;
		EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s x WHERE %s)', chunk_rec.compressed_chunk, probe_expr)
		INTO found;

		IF found THEN
			INSERT INTO aic_chunks (hypertable, chunk) VALUES (chunk_rec.hypertable, chunk_rec.chunk);
			n_hits := n_hits + 1;
		END IF;
	END LOOP;

	RAISE NOTICE 'AIC downgrade: % compressed chunks, % probed, % hold AIC batches',
		n_chunks, n_probed, n_hits;

	FOR chunk_rec IN SELECT hypertable, chunk FROM aic_chunks ORDER BY hypertable::text, chunk::text
	LOOP
		RAISE NOTICE 'AIC found: chunk % of hypertable %', chunk_rec.chunk, chunk_rec.hypertable;
	END LOOP;
END;
$$;

--
-- Phase 2: convert the chunks found
--
SET timescaledb.enable_aic_compression = 'off';

DO $$
DECLARE
	rec RECORD;
	n_done INT := 0;
BEGIN
	FOR rec IN SELECT hypertable, chunk FROM aic_chunks ORDER BY hypertable::text, chunk::text
	LOOP
		RAISE NOTICE 'Decompressing chunk % (hypertable %)', rec.chunk, rec.hypertable;
		PERFORM decompress_chunk(rec.chunk, if_compressed => true);

		BEGIN
			RAISE NOTICE 'Compressing chunk % with AIC disabled', rec.chunk;
			PERFORM compress_chunk(rec.chunk, if_not_compressed => true);
		EXCEPTION WHEN OTHERS THEN
			RAISE WARNING 'chunk % left decompressed, compress_chunk failed: %', rec.chunk, SQLERRM;
		END;

		n_done := n_done + 1;

		COMMIT;
	END LOOP;

	RAISE NOTICE 'AIC downgrade: % chunks converted', n_done;
END;
$$;

DROP TABLE IF EXISTS pg_temp.aic_chunks;
