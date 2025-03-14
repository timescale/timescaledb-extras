-- Downgrade the new compression algorithms introduced in 2.19.0.
--
-- The new BOOL compression algorithm replaced the old ARRAY compression for boolean columns.
-- Since previous versions of TimescaleDB do not support the BOOL compression algorithm, we need
-- to revert to the old ARRAY compression for boolean columns.
--
-- The script iterates over all hypertables with compressed chunks and boolean columns and recompresses
-- the chunks that have BOOL compressed values.
--
-- The script also disables the BOOL compression algorithm by setting the enable_bool_compression GUC to 'off'.
--
-- Once the BOOL compression is disabled, the script recompresses the chunks that have BOOL compressed values.
--
--
-- After downgrading the BOOL compression, we need to downgrade the NULL compression.
--
--
-- The null compression feature was also introduced in 2.19.0. This script replaces
-- the base64 encoded value 'Bg==' with the NULL value in the compressed chunks. With
-- this, the new null-compression is reverted and the old method of storing NULL
-- values is used.
--
-- (The 'Bg==' value is the base64 encoded value of the 'null-compression', value (6)).
--
-- These special null-compression values were introduced to handle a special case
-- to distunguish between NULL values and default values. (Bug #7714)
--
-- The script iterates over all compressed chunks and all attributes
-- and prints a notice for each attribute that has been updated.
--

--
-- Downgrade BOOL compression
--
DO $$
DECLARE
	catalog_record RECORD;
	sel_rec RECORD;
	recomp_rec RECORD;
	ht_regclass REGCLASS;
	comp_regclass REGCLASS;
	uncomp_regclass REGCLASS;
	script TEXT;
	attname NAME;
BEGIN
	CREATE TEMP TABLE bool_compressed_chunks (
		ht_regclass REGCLASS,
		comp_regclass REGCLASS,
		uncomp_regclass REGCLASS,
		counts INT
	) ON COMMIT DROP;

	-- Find all hypertables with compressed chunks and bool columns
	FOR catalog_record IN
		SELECT
				format('%I.%I', ht.schema_name, ht.table_name)::regclass as ht_regclass,
				format('%I.%I', comp.schema_name, comp.table_name)::regclass as comp_regclass,
				format('%I.%I', uncomp.schema_name, uncomp.table_name)::regclass as uncomp_regclass,
				att.attname as attname
		FROM
				_timescaledb_catalog.hypertable ht
				INNER JOIN _timescaledb_catalog.chunk comp ON ht.compressed_hypertable_id = comp.hypertable_id
				INNER JOIN _timescaledb_catalog.chunk uncomp ON comp.id = uncomp.compressed_chunk_id
				INNER JOIN pg_catalog.pg_class cl ON cl.relname = ht.table_name
				INNER JOIN pg_catalog.pg_namespace ns ON cl.relnamespace = ns.oid
				INNER JOIN pg_catalog.pg_attribute att ON att.attrelid = cl.oid
				INNER JOIN pg_catalog.pg_type ty ON att.atttypid = ty.oid
		WHERE
				att.attnum > 0 AND
				ns.nspname = ht.schema_name AND
				uncomp.dropped IS FALSE AND
				uncomp.compressed_chunk_id IS NOT NULL AND
				uncomp.dropped IS FALSE AND
				ty.typname = 'bool'
	LOOP
		ht_regclass := catalog_record.ht_regclass;
		comp_regclass := catalog_record.comp_regclass;
		uncomp_regclass := catalog_record.uncomp_regclass;
		attname := catalog_record.attname;

		-- Iterate over all compressed bool values and count the ones that use the BOOL algorithm
		FOR sel_rec IN
			EXECUTE format('SELECT COUNT(*) as counts FROM %s x WHERE (SELECT algorithm FROM _timescaledb_functions.compressed_data_info(x.%s)) = %L', 
							comp_regclass, attname, 'BOOL')
		LOOP
			INSERT INTO bool_compressed_chunks VALUES (ht_regclass, comp_regclass, uncomp_regclass, sel_rec.counts);
		END LOOP;
	END LOOP;

	-- Disable the BOOL compression, so we revert to the old ARRAY compression for bools
	SET timescaledb.enable_bool_compression = 'off';

	-- Now iterate over all compressed chunks that have BOOL compressed values and recompress them
	FOR recomp_rec IN
		SELECT ch.uncomp_regclass, sum(counts) FROM bool_compressed_chunks ch GROUP BY 1 HAVING (sum(counts)>0) 
	LOOP
		RAISE NOTICE 'Decompressing chunk: %', recomp_rec.uncomp_regclass;
		EXECUTE format('select decompress_chunk((%L)::regclass)', recomp_rec.uncomp_regclass);
	END LOOP;

	DROP TABLE bool_compressed_chunks;
END;
$$;

--
-- Donwgrade NULL compression
--
DO $$
DECLARE
	comp_regclass REGCLASS;
	ht_regclass REGCLASS;
	catalog_record RECORD;
	attname NAME;
	script TEXT;
	rows_updated INT;
BEGIN
	-- Iterate over all compressed chunks
	FOR catalog_record IN
		SELECT
			format('%I.%I', comp.schema_name, comp.table_name)::regclass as comp_regclass,
			att.attname,
			format('%I.%I', ht.schema_name, ht.table_name)::regclass as ht_regclass
		FROM
			_timescaledb_catalog.hypertable ht
			INNER JOIN _timescaledb_catalog.chunk comp ON ht.compressed_hypertable_id = comp.hypertable_id
			INNER JOIN _timescaledb_catalog.chunk uncomp ON comp.id = uncomp.compressed_chunk_id
			INNER JOIN pg_catalog.pg_class cl ON cl.relname = ht.table_name
			INNER JOIN pg_catalog.pg_namespace ns ON cl.relnamespace = ns.oid
			INNER JOIN pg_catalog.pg_attribute att ON att.attrelid = cl.oid
		WHERE
			att.attnum > 0 AND
			ns.nspname = ht.schema_name AND
			uncomp.dropped IS FALSE AND
			uncomp.compressed_chunk_id IS NOT NULL AND
			uncomp.dropped IS FALSE
	LOOP
		comp_regclass := catalog_record.comp_regclass;
		attname := catalog_record.attname;
		ht_regclass := catalog_record.ht_regclass;

		-- Iterate over all compressed columns in the chunks and update the NULL compressed values to NULL
		script := format('UPDATE %s SET %s = NULL WHERE (%s)::text = %L', comp_regclass, attname, attname, 'Bg==');
		EXECUTE script;
		GET DIAGNOSTICS rows_updated = ROW_COUNT;

		IF rows_updated > 0 THEN
			RAISE NOTICE 'Updated % compressed blocks in %s.%s (hypretable: %s)', rows_updated, comp_regclass, attname, ht_regclass;
		END IF;
	END LOOP;
END;
$$;
