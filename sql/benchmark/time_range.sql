-- benchmark/time_range.sql
-- Compares time-range query performance across three index strategies:
--   • No index    → sequential scan
--   • BRIN        → block-range index, tiny size, good for ordered data
--   • B-tree      → classic, precise, faster for narrow ranges
--
-- BRIN vs B-tree trade-off:
--   BRIN stores only min/max per block — very small, low insert overhead,
--   but imprecise (still scans whole blocks). B-tree stores every value,
--   larger, but pinpoints exact rows immediately.

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 1 — BASELINE: sequential scan (no index on observed_at)
-- ════════════════════════════════════════════════════════════════════════════
DROP INDEX IF EXISTS idx_obs_time_brin;
DROP INDEX IF EXISTS idx_obs_time_btree;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COUNT(*)
FROM   weather_observations
WHERE  observed_at BETWEEN '2026-03-01 00:00:00+00'
                       AND '2026-03-07 23:00:00+00';

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 2 — WITH BRIN INDEX
-- Works best when rows were inserted in timestamp order (which Open-Meteo
-- data typically is — chronological ingestion gives ordered physical layout).
-- ════════════════════════════════════════════════════════════════════════════
CREATE INDEX idx_obs_time_brin ON weather_observations USING BRIN(observed_at);

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COUNT(*)
FROM   weather_observations
WHERE  observed_at BETWEEN '2026-03-01 00:00:00+00'
                       AND '2026-03-07 23:00:00+00';

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 3 — REPLACE BRIN WITH B-tree
-- B-tree is faster for narrow time windows; BRIN wins on storage and bulk.
-- ════════════════════════════════════════════════════════════════════════════
DROP INDEX IF EXISTS idx_obs_time_brin;
CREATE INDEX idx_obs_time_btree ON weather_observations(observed_at);

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT COUNT(*)
FROM   weather_observations
WHERE  observed_at BETWEEN '2026-03-01 00:00:00+00'
                       AND '2026-03-07 23:00:00+00';

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 4 — COMBINED: spatial filter + time range (realistic production query)
-- Uses both GiST (on geog) and B-tree (on observed_at) together.
-- ════════════════════════════════════════════════════════════════════════════
CREATE INDEX IF NOT EXISTS idx_locations_geog_gist ON locations USING GIST(geog);

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    l.name,
    wo.observed_at,
    wo.temperature,
    wo.rain
FROM   weather_observations wo
JOIN   locations l ON l.id = wo.location_id
WHERE  ST_DWithin(l.geog, ST_MakePoint(2.352, 48.857)::geography, 200000)
  AND  wo.observed_at BETWEEN '2026-03-01 00:00:00+00'
                          AND '2026-03-07 23:00:00+00'
ORDER  BY wo.observed_at;
