-- The naming scheme for the composite bloom filter metadata columns has changed in 2.27 to
-- always include a hash of the concatenated column names, even if the concatenated column
-- names are short. The new naming scheme uses the zero byte as a separator in the hash input,
-- while the old scheme used underscore as separator. This caused an ambiguity in the old
-- naming scheme where the composite filter column name was the same for ('a_b', 'c')
-- and ('a', 'b_c'), which could lead to an error when compressing a chunk.
--
-- See bug report here: https://github.com/timescale/timescaledb/issues/9578
--

--
-- This script iterates over the compression settings for the compressed chunks and
-- checks if there are any composite bloom filters. It generates the old and the new
-- metadata column names and verifies in the catalog that the old column name actually
-- exists. If it does, then it renames the column to the new name.
--

--
-- Because this script verifies the existence of the old column names and verifies
-- the compression settings to be of type bloom and being a composite filter, it
-- is safe to run it even if the cluster has already been on 2.27 or if there are
-- mixed data, or no composite bloom filters at all.
--

SET timescaledb.restoring TO on;

DO $$
DECLARE
    rename_data RECORD;
BEGIN
    FOR rename_data IN
        --
        -- Make sure the old meta name actually exists for the compressed chunk
        -- relation, so the renaming that follows only impact real columns not
        -- some hallucinated ones.
        --
        SELECT
            att.attrelid::regclass,
            e.old_meta_name,
            e.new_meta_name
        FROM
            pg_attribute att,
            (
            --
            -- Calculate the old and new metadata column names of the composite bloom filters.
            -- Note that the new scheme always use a hash string to distinguish between the
            -- composite columns, but the old one only used the hash if the concatenated column
            -- names were too long.
            --
            SELECT
                compress_relid,
                CASE
                    WHEN length(joined_cols_underscores) > 39
                        THEN '_ts_meta_v2_bloomh_' || hash_underscores || '_' || joined_cols_underscores
                ELSE '_ts_meta_v2_bloomh_' || joined_cols_underscores
                END as old_meta_name,
                '_ts_meta_v2_bloomh_' || hash_zeroes || '_' || joined_cols_underscores as new_meta_name
            FROM
                (
                --
                -- Calculate the first 4 characters of the md5 hashes of both the
                -- zero and underscore concatenated column names of the composite
                -- bloom filters.
                --
                SELECT
                    compress_relid,
                    substr(md5(joined_cols_zeroes),1,4) as hash_zeroes,
                    substr(md5(joined_cols_underscores),1,4) as hash_underscores,
                    joined_cols_underscores
                FROM (
                    --
                    -- Select the compression settings objects that are actually a
                    -- a 'bloom' filter, out of the already selected 'column' arrays
                    -- and return the compressed chunk relation along with the column
                    -- names concatenated with underscores as well as zeroes.
                    --
                    SELECT
                        compress_relid,
                        (SELECT string_agg(value::bytea, '\x00'::bytea) FROM jsonb_array_elements_text(cols::jsonb)) as joined_cols_zeroes,
                        array_to_string(array(select jsonb_array_elements_text(cols::jsonb)), '_') as joined_cols_underscores
                    FROM (
                        --
                        -- Select the settings where the column field is an array
                        -- which is a must for the composite bloom filters
                        --
                        SELECT
                            *,
                            ae->>'column' cols
                        FROM (
                            --
                            -- Capture the compression settings for the compressed
                            -- tables, and separate the individual settings along
                            -- with their types 
                            --
                            SELECT
                                compress_relid::text,
                                jsonb_array_elements(index) ae,
                                jsonb_array_elements(index)->>'type' ty
                            FROM _timescaledb_catalog.compression_settings
                            WHERE compress_relid::text LIKE '%compress_hyper%'
                        ) a
                    WHERE jsonb_typeof(ae->'column') = 'array'
                    ) b
                 WHERE ty = 'bloom'
                ) c
            ) d
        ) e
        WHERE att.attrelid = e.compress_relid::regclass AND att.attname = e.old_meta_name
    LOOP
        RAISE NOTICE 'RENAMING: %s.%I to %I',
            rename_data.attrelid,
            rename_data.old_meta_name,
            rename_data.new_meta_name;

        EXECUTE format(
            'ALTER TABLE %s RENAME COLUMN %I TO %I',
            rename_data.attrelid,
            rename_data.old_meta_name,
            rename_data.new_meta_name
        );
    END LOOP;
END;
$$;

RESET timescaledb.restoring;
