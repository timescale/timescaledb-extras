-- Calculate the time bucket using the continuous aggregate bucket function
-- configuration
CREATE OR REPLACE FUNCTION _timescaledb_additional.cagg_time_bucket(INTEGER, TIMESTAMPTZ)
RETURNS TIMESTAMPTZ AS
$$
DECLARE
  params TEXT[];
  stmt TEXT;
  r RECORD;
  result TIMESTAMPTZ;
BEGIN
  SELECT * INTO r FROM _timescaledb_catalog.continuous_aggs_bucket_function WHERE mat_hypertable_id = $1;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'Continuous Aggregate % not found', $1;
  END IF;

  params := array_append(params, format('%I => %L::timestamptz', 'ts', $2));

  IF r.bucket_width IS NOT NULL THEN
    params := array_append(params, format('%I => %L::interval', 'bucket_width', r.bucket_width));
  END IF;

  IF r.bucket_origin IS NOT NULL THEN
    params := array_append(params, format('%I => %L::timestamptz', 'origin', r.bucket_origin));
  END IF;

  IF r.bucket_offset IS NOT NULL THEN
    params := array_append(params, format('%I => %L::interval', 'offset', r.bucket_offset));
  END IF;

  IF r.bucket_timezone IS NOT NULL THEN
    params := array_append(params, format('%I => %L::text', 'timezone', r.bucket_timezone));
  END IF;

  stmt := format('SELECT time_bucket(%s)', array_to_string(params, ', '));
  RAISE DEBUG '%', stmt;

  EXECUTE stmt
  INTO result;

  RETURN result;
END;
$$
LANGUAGE plpgsql STABLE;

-- Discover continuous aggregates built on top of tiered hypertables and
-- schedule their refresh
CREATE OR REPLACE PROCEDURE _timescaledb_additional.schedule_cagg_refresh(
    name_mask TEXT DEFAULT '%',
    nbuckets INTEGER DEFAULT 5,
    dry_run BOOLEAN DEFAULT true,
    priority INTEGER DEFAULT 100
) AS
$$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN (
        -- Find caggs built on top of tiered hypertables
        WITH ranges AS (
            SELECT
                cagg.mat_hypertable_id,
                ht.schema_name,
                ht.table_name,
                (
                    SELECT column_type AS dim_type
                    FROM _timescaledb_catalog.dimension d
                    WHERE d.hypertable_id = ht.id
                    ORDER BY d.id ASC LIMIT 1
                ) AS dim_type,
                user_view_schema,
                user_view_name,
                bf.bucket_width::interval AS bucket_width,
                _timescaledb_additional.cagg_time_bucket(mat_hypertable_id, range.start) AS global_start,
                _timescaledb_additional.cagg_time_bucket(mat_hypertable_id, range.end) + (bf.bucket_width::interval + '1 millisecond'::interval) AS global_end
            FROM _timescaledb_catalog.continuous_agg cagg
            JOIN _timescaledb_catalog.continuous_aggs_bucket_function bf USING (mat_hypertable_id)
            JOIN _timescaledb_catalog.hypertable ht ON (ht.id = cagg.raw_hypertable_id)
            JOIN _osm_catalog.table_map tm ON (tm.hypertable_name = ht.table_name AND tm.hypertable_schema = ht.schema_name)
            JOIN (
                -- the time window of tiered data
                SELECT
                    osm_table_id,
                    _osm_internal.dimension_pg_usec_to_timestamp(min(range_start)) as start,
                    _osm_internal.dimension_pg_usec_to_timestamp(max(range_end)) as end
                FROM _osm_catalog.chunk_map
                JOIN _osm_catalog.chunk_object_map USING (chunk_id)
                GROUP BY osm_table_id
            ) AS range USING (osm_table_id)
        )
        SELECT
            mat_hypertable_id,
            dim_type,
            user_view_schema,
            user_view_name,
            global_start,
            global_end,
            start,
            start + (bucket_width * 5) AS end,
            (extract(epoch from start) * 1000000)::bigint AS invalidation_start,
            (extract(epoch from (start + (bucket_width * nbuckets))) * 1000000)::bigint AS invalidation_end
        FROM
            ranges,
            -- Split ranges with 5 times the bucket width
            LATERAL generate_series(ranges.global_start, ranges.global_end, (bucket_width * nbuckets)) AS start
        WHERE user_view_name LIKE name_mask
    )
    LOOP
        -- skip non-timestamptz based caggs
        IF rec.dim_type != 'TIMESTAMPTZ'::REGTYPE THEN
            RAISE NOTICE 'SKIPPING ''%.%'' (dim type ''%''): %-%',
                rec.user_view_schema, rec.user_view_name,
                rec.dim_type, rec.start, rec.end;
            CONTINUE;
        END IF;

        IF dry_run THEN
            -- do nothing on dry run
            RAISE NOTICE 'refresh ''%.%'': %-%',
                rec.user_view_schema, rec.user_view_name,
                rec.start, rec.end;
        ELSE
            -- insert an invalidation record from
            INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
                VALUES (
                    rec.mat_hypertable_id,
                    rec.invalidation_start,
                    rec.invalidation_end
                );

            -- schedule the refresh for given interval
            INSERT INTO _timescaledb_additional.incremental_continuous_aggregate_refreshes
                (continuous_aggregate, window_start, window_end, priority)
            VALUES
                (cagg_regclass, rec.start, rec.end, priority);
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;
