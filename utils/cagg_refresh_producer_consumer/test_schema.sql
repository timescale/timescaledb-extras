CREATE TABLE public.test_cagg_incr_refresh(
    time timestamptz not null,
    value int4
);

CREATE FUNCTION public.slow_accum(int8, int4)
RETURNS int8
LANGUAGE sql AS
$$SELECT $1 + $2$$ STRICT;

CREATE OR REPLACE FUNCTION public.slow_final(int8)
RETURNS int8
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_catalog.pg_sleep(30);
    RETURN $1;
END;
$$;

CREATE AGGREGATE public.slow_sum(int4)
(
        INITCOND = 0,
        STYPE = int8,
        SFUNC = public.slow_accum,
        FINALFUNC = public.slow_final
);

SELECT
    public.create_hypertable(
        'public.test_cagg_incremental_refresh',
        'time'
    );
CREATE TABLE public.(
    time timestamptz not null,
    value int4
);

CREATE FUNCTION public.slow_accum(int8, int4)
RETURNS int8
LANGUAGE sql AS
$$SELECT $1 + $2$$ STRICT;

CREATE OR REPLACE FUNCTION public.slow_final(int8)
RETURNS int8
LANGUAGE plpgsql AS $$
BEGIN
    -- This ensures the aggregate will be slow always,
    -- regardless of the amount of rows.
    PERFORM pg_catalog.pg_sleep(1);
    RETURN $1;
END;
$$;

CREATE AGGREGATE public.slow_sum(int4)
(
        INITCOND = 0,
        STYPE = int8,
        SFUNC = public.slow_accum,
        FINALFUNC = public.slow_final
);

SELECT
    public.create_hypertable(
        'public.test_cagg_incr_refresh',
        'time'
    );

CREATE MATERIALIZED VIEW public.test_cagg_incr_refresh_cagg
WITH (timescaledb.continuous) AS
SELECT
   time_bucket(interval '12 hours', time) AS bucket,
   slow_sum(value) AS sum_values,
   count(*) AS n_values
FROM
    public.test_cagg_incr_refresh
GROUP BY
    bucket
WITH NO DATA;

-- 1096 days worth of data. As we sleep 1 second for every hour,
-- we have quite a lot of sleeping to be done, allowing us to
-- observe the interactions between jobs, priorities etc.
INSERT INTO
    public.test_cagg_incr_refresh (time, value)
SELECT
    t,
    (random() * 2000_000_000)::int
FROM
    pg_catalog.generate_series(
        '2022-01-01T00:00:00+00',
        '2025-01-01T00:00:00+00',
        -- Slightly offset from the bucket size, to allow some differences in buckets
        interval '10 hours'
    ) AS _(t);

-- First, we schedule the older data, with a low priority
CALL _timescaledb_additional.schedule_refresh_continuous_aggregate_incremental(
    job_id => null,
    config =>
'{
    "end_offset": "7 days",
    "start_offset": "3 years",
    "continuous_aggregate": "public.test_cagg_incr_refresh_cagg",
    "increment_size": "3 days",
    "priority": 100
}'
);

-- Next, we schedule the newer data, with a higher priority
CALL _timescaledb_additional.schedule_refresh_continuous_aggregate_incremental(
    job_id => null,
    config =>
'{
    "end_offset": "10 minutes",
    "start_offset": "8 days",
    "continuous_aggregate": "public.test_cagg_incr_refresh_cagg",
    "increment_size": "24 hours",
    "priority": 1
}'
);