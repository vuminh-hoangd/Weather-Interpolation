-- 02_indexes.sql
-- Index management for benchmarking.
-- Run DROP + CREATE pairs selectively to isolate each index type.
-- Use the benchmark/ queries with EXPLAIN ANALYZE to measure the difference.

-- ════════════════════════════════════════════════════════════════════════════
-- SPATIAL INDEXES — locations.geog
-- ════════════════════════════════════════════════════════════════════════════

-- GiST (best for kNN <->, ST_DWithin radius, && bounding box)
DROP INDEX IF EXISTS idx_locations_geog_gist;
CREATE INDEX idx_locations_geog_gist ON locations USING GIST(geog);

-- ════════════════════════════════════════════════════════════════════════════
-- TEMPORAL INDEXES — weather_observations.observed_at
-- ════════════════════════════════════════════════════════════════════════════

-- BRIN (very small, works well when rows are physically ordered by time)
DROP INDEX IF EXISTS idx_obs_time_brin;
CREATE INDEX idx_obs_time_brin ON weather_observations USING BRIN(observed_at);

-- B-tree (precise range scans, larger than BRIN but faster for narrow ranges)
DROP INDEX IF EXISTS idx_obs_time_btree;
CREATE INDEX idx_obs_time_btree ON weather_observations(observed_at);

-- ════════════════════════════════════════════════════════════════════════════
-- COMPOSITE INDEX — location_id + observed_at (used by kNN + time queries)
-- ════════════════════════════════════════════════════════════════════════════

DROP INDEX IF EXISTS idx_obs_loc_time;
CREATE INDEX idx_obs_loc_time ON weather_observations(location_id, observed_at);

-- ════════════════════════════════════════════════════════════════════════════
-- QUICK DROP ALL (reset to no-index baseline before benchmarking)
-- ════════════════════════════════════════════════════════════════════════════
-- DROP INDEX IF EXISTS idx_locations_geog_gist;
-- DROP INDEX IF EXISTS idx_obs_time_brin;
-- DROP INDEX IF EXISTS idx_obs_time_btree;
-- DROP INDEX IF EXISTS idx_obs_loc_time;
