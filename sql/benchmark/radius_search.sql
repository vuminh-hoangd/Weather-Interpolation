-- benchmark/radius_search.sql
-- Compares radius search (ST_DWithin) with no index vs. GiST index.
-- Target: all grid points within 150 km of Lyon (45.764°N, 4.836°E).
--
-- ST_DWithin(geog_a, geog_b, metres) returns TRUE when the geodesic
-- distance between two geography values is within the given radius.
-- With a GiST index, PostgreSQL uses a bounding-box pre-filter before
-- computing the exact geodesic distance — much faster on large tables.

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 1 — BASELINE: sequential scan
-- ════════════════════════════════════════════════════════════════════════════
DROP INDEX IF EXISTS idx_locations_geog_gist;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    l.name,
    l.lat,
    l.lon,
    ST_Distance(l.geog, ST_MakePoint(4.836, 45.764)::geography) AS dist_m
FROM   locations l
WHERE  ST_DWithin(l.geog, ST_MakePoint(4.836, 45.764)::geography, 150000)
ORDER  BY dist_m;

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 2 — WITH GiST INDEX
-- ════════════════════════════════════════════════════════════════════════════
CREATE INDEX idx_locations_geog_gist ON locations USING GIST(geog);

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    l.name,
    l.lat,
    l.lon,
    ST_Distance(l.geog, ST_MakePoint(4.836, 45.764)::geography) AS dist_m
FROM   locations l
WHERE  ST_DWithin(l.geog, ST_MakePoint(4.836, 45.764)::geography, 150000)
ORDER  BY dist_m;

-- ════════════════════════════════════════════════════════════════════════════
-- STEP 3 — RADIUS SEARCH JOINED WITH weather_observations
-- All training observations within 150 km of Lyon at a specific hour.
-- ════════════════════════════════════════════════════════════════════════════
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    l.name,
    l.lat,
    l.lon,
    wo.temperature,
    wo.humidity,
    wo.rain,
    wo.soil_temp,
    ST_Distance(l.geog, ST_MakePoint(4.836, 45.764)::geography) AS dist_m
FROM   training_observations wo
JOIN   locations l ON l.id = wo.location_id
WHERE  ST_DWithin(l.geog, ST_MakePoint(4.836, 45.764)::geography, 150000)
  AND  wo.observed_at = '2026-03-15 12:00:00+00'
ORDER  BY dist_m;
