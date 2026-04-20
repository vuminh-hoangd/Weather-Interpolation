-- 03_seed_france.sql
-- 1. Insert France 0.18° grid locations (~3,800 points, ~20 km spacing).
-- 2. Insert 10 test-zone cities with city-proportional exclusion radii.
-- 3. Mark locations that fall inside any test zone.

-- ─── 1. FRANCE 0.18° GRID ───────────────────────────────────────────────────
-- Bounding box: lat 42.0–51.25°N, lon -5.0–8.25°E
-- generate_series produces every 0.18° step within the box.
INSERT INTO locations (name, lat, lon)
SELECT
    'grid_' || lat::TEXT || '_' || lon::TEXT,
    lat,
    lon
FROM (
    SELECT
        ROUND((42.0 + i * 0.18)::NUMERIC, 2)::DOUBLE PRECISION AS lat,
        ROUND((-5.0 + j * 0.18)::NUMERIC, 2)::DOUBLE PRECISION AS lon
    FROM generate_series(0, 51) AS i,   -- 42.00 → 51.18 (52 steps)
         generate_series(0, 73) AS j    -- -5.00 →  8.14 (74 steps)
) grid
ON CONFLICT DO NOTHING;

-- ─── 2. TEST ZONES — city-proportional exclusion radii ──────────────────────
-- Radii reflect each city's metropolitan footprint and geographic constraints.
-- Larger metros (Paris) warrant a bigger exclusion zone; compact coastal cities
-- (Nice, hemmed between sea and Alps) use a smaller radius.
INSERT INTO test_zones (name, center_geog, radius_m) VALUES
    ('Paris',       ST_MakePoint( 2.352,  48.857)::geography, 40000),
    ('Lyon',        ST_MakePoint( 4.836,  45.764)::geography, 28000),
    ('Grenoble',    ST_MakePoint( 5.724,  45.188)::geography, 15000),
    ('Toulouse',    ST_MakePoint( 1.444,  43.605)::geography, 24000),
    ('Bordeaux',    ST_MakePoint(-0.579,  44.838)::geography, 24000),
    ('Lille',       ST_MakePoint( 3.057,  50.629)::geography, 20000),
    ('Nantes',      ST_MakePoint(-1.554,  47.218)::geography, 20000),
    ('Rennes',      ST_MakePoint(-1.678,  48.117)::geography, 16000),
    ('Strasbourg',  ST_MakePoint( 7.752,  48.573)::geography, 16000),
    ('Avignon',     ST_MakePoint( 4.805,  43.949)::geography, 24000)
ON CONFLICT DO NOTHING;

-- ─── 3. MARK LOCATIONS INSIDE TEST ZONES ────────────────────────────────────
UPDATE locations l
SET    is_test_zone = TRUE
WHERE  EXISTS (
    SELECT 1
    FROM   test_zones tz
    WHERE  ST_DWithin(l.geog, tz.center_geog, tz.radius_m)
);

-- Verify counts
SELECT
    is_test_zone,
    COUNT(*) AS location_count
FROM locations
GROUP BY is_test_zone
ORDER BY is_test_zone;
