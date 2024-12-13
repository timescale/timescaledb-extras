-- Discover continuous aggregates built on top of tiered hypertables and
-- schedule their refresh
CREATE OR REPLACE PROCEDURE _timescaledb_additional.schedule_cagg_refresh(
    name_mask TEXT DEFAULT '%',
    lower_bound TIMESTAMPTZ DEFAULT NULL,
    upper_bound TIMESTAMPTZ DEFAULT NULL,
    dry_run BOOLEAN DEFAULT true,
    priority INTEGER DEFAULT 100
) AS
$$
DECLARE
    rec RECORD;
    window_start TIMESTAMPTZ;
    window_end TIMESTAMPTZ;
BEGIN
    FOR rec IN (
        -- Find caggs built on top of tiered hypertables
        SELECT
            cagg.mat_hypertable_id,
            ht.schema_name,
            ht.table_name,
            (
                SELECT column_type as dim_type
                FROM _timescaledb_catalog.dimension d
                WHERE d.hypertable_id = ht.id
                ORDER BY d.id ASC LIMIT 1
            ) as dim_type,
            user_view_schema,
            user_view_name,
            range.start as range_start,
            range.end as range_end
        FROM _timescaledb_catalog.continuous_agg cagg
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
        ) as range USING (osm_table_id)
        WHERE user_view_name LIKE name_mask
    )
    LOOP
        -- limit the scope if specified
        window_start := greatest(rec.range_start, lower_bound);
        window_end := least(rec.range_end, upper_bound);

        IF window_end < window_start THEN
            RAISE NOTICE 'SKIPPING ''%.%'', end date is less than start date: %-%',
                rec.user_view_schema, rec.user_view_name,
                window_start, window_end;
            CONTINUE;
        END IF;

        -- skip non-timestamptz based caggs
        IF rec.dim_type != 'TIMESTAMPTZ'::REGTYPE THEN
            RAISE NOTICE 'SKIPPING ''%.%'' (dim type ''%''): %-%',
                rec.user_view_schema, rec.user_view_name,
                rec.dim_type,
                window_start, window_end;
            CONTINUE;
        END IF;

        IF dry_run THEN
            -- do nothing on dry run
            RAISE NOTICE 'refresh ''%.%'': %-%',
                rec.user_view_schema, rec.user_view_name,
                window_start, window_end;
        ELSE
            -- insert an invalidation record from
            INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
                VALUES (
                    rec.mat_hypertable_id,
                    extract(epoch from window_start) * 1000000,
                    extract(epoch from window_end) * 1000000
                );

            -- schedule the refresh for given interval
            PERFORM _timescaledb_additional.produce_refresh_intervals(
                format('%I.%I', rec.user_view_schema, rec.user_view_name)::REGCLASS,
                window_start,
                window_end,
                priority);
        END IF;
    END LOOP;
END
$$ LANGUAGE plpgsql;

-- Generate refresh intervals for a single continuous aggregate
CREATE OR REPLACE FUNCTION _timescaledb_additional.produce_refresh_intervals(
    cagg_regclass REGCLASS,
    start_t TIMESTAMPTZ,
    end_t   TIMESTAMPTZ,
    priority INTEGER DEFAULT 100
) RETURNS BIGINT AS
$$
DECLARE
    count       bigint := 0;
    added       bigint := 0;
    hit         bool := false;
    increment_size INTERVAL;
    raw_ht      REGCLASS;
    rec         RECORD;
BEGIN
    IF increment_size IS NULL THEN
        SELECT
            -- We default to the dimension interval_length if not explicitly specified
            coalesce(increment_size, interval_length * interval '1 microsecond'),
            format('%I.%I', ht.schema_name, ht.table_name)
        INTO increment_size, raw_ht
        FROM _timescaledb_catalog.continuous_agg AS cagg
        JOIN _timescaledb_catalog.dimension AS dim ON (mat_hypertable_id = dim.hypertable_id)
        JOIN _timescaledb_catalog.hypertable AS ht ON (raw_hypertable_id = ht.id)
        WHERE format('%I.%I', user_view_schema, user_view_name)::regclass = cagg_regclass
        -- If there are multiple dimensions, we only want the first one
        ORDER BY dim.id ASC
        LIMIT 1;
    END IF;

    -- Generate ranges that intersect with tiered chunks
    FOR rec IN (
        SELECT i.incr_start, i.incr_end
        FROM timescaledb_osm.tiered_chunks ch
        JOIN _timescaledb_additional.generate_increments(start_t, end_t, increment_size) AS i
            ON tstzrange(i.incr_start, i.incr_end, '[)') && tstzrange(ch.range_start, ch.range_end, '[)')
        WHERE format('%I.%I', hypertable_schema, hypertable_name)::REGCLASS = raw_ht
    ) LOOP
        INSERT INTO _timescaledb_additional.incremental_continuous_aggregate_refreshes
            (continuous_aggregate, window_start, window_end, priority)
        VALUES
            (cagg_regclass, rec.incr_start, rec.incr_end, priority)
        ON CONFLICT
            DO NOTHING
        RETURNING
            true
        INTO
            hit;

        count := count + 1;
        IF hit THEN
            added := added + 1;
        END IF;
    END LOOP;

    RAISE NOTICE
        'Scheduled incremental refreshes for % (% - %). Tasks evaluated: %, newly inserted: %',
        cagg_regclass::text,
        start_t,
        end_t,
        count,
        added;

    RETURN added;
END;
$$
LANGUAGE plpgsql;

-- Generate increments
CREATE OR REPLACE FUNCTION _timescaledb_additional.generate_increments(
    start_t TIMESTAMPTZ,
    end_t TIMESTAMPTZ,
    increment_size INTERVAL
)
RETURNS TABLE (incr_start TIMESTAMPTZ, incr_end TIMESTAMPTZ) AS
$$
DECLARE
    i INTEGER := 0;
BEGIN
    incr_end := start_t;

    WHILE incr_end < end_t
    LOOP
        incr_start := public.time_bucket(increment_size, start_t + increment_size * i);
        incr_end := public.time_bucket(increment_size, start_t + increment_size * (i + 1));
        RETURN NEXT;

        i := i + 1;
    END LOOP;
END
$$
LANGUAGE plpgsql;
