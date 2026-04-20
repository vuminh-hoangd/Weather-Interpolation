-- 04_monthly_partition.sql
-- Creates one partition per month for weather_observations.
-- Run this BEFORE ingesting data for that month.
-- PostgreSQL will route INSERTs to the correct partition automatically.

-- ─── CREATE A PARTITION FOR A GIVEN MONTH ────────────────────────────────────
-- Pattern: weather_observations_YYYY_MM
-- Covers: FROM the 1st of the month TO the 1st of the next month (exclusive).

-- March 2026 (starting month)
CREATE TABLE IF NOT EXISTS weather_observations_2026_03
    PARTITION OF weather_observations
    FOR VALUES FROM ('2026-03-01 00:00:00+00')
               TO   ('2026-04-01 00:00:00+00');

-- April 2026
CREATE TABLE IF NOT EXISTS weather_observations_2026_04
    PARTITION OF weather_observations
    FOR VALUES FROM ('2026-04-01 00:00:00+00')
               TO   ('2026-05-01 00:00:00+00');

-- Add more months following the same pattern as you extend the dataset.

-- ─── FUNCTION: auto-create partition for any given month ─────────────────────
-- Call this from ingest.py before inserting a new month's data.
-- Example: SELECT create_monthly_partition('2024-06-01');
CREATE OR REPLACE FUNCTION create_monthly_partition(month_start DATE)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    partition_name TEXT;
    month_end      DATE;
BEGIN
    partition_name := 'weather_observations_'
                      || TO_CHAR(month_start, 'YYYY_MM');
    month_end      := month_start + INTERVAL '1 month';

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I
         PARTITION OF weather_observations
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        month_start::TIMESTAMPTZ,
        month_end::TIMESTAMPTZ
    );
END;
$$;

-- ─── VERIFY: list all existing partitions and their row counts ────────────────
SELECT
    child.relname                          AS partition,
    pg_get_expr(child.relpartbound, child.oid) AS bounds,
    pg_relation_size(child.oid)            AS size_bytes,
    (SELECT COUNT(*) FROM weather_observations
     WHERE observed_at >= (SELECT lo FROM
         (SELECT lower(pg_get_expr(child.relpartbound, child.oid)::text) AS lo) t
         LIMIT 1)
    )                                      AS approx_rows
FROM   pg_inherits
JOIN   pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN   pg_class child  ON pg_inherits.inhrelid  = child.oid
WHERE  parent.relname = 'weather_observations'
ORDER  BY child.relname;
