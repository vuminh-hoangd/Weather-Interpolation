-- 01_schema.sql
-- Core tables — NO indexes (managed in 02_indexes.sql).
-- weather_observations is PARTITIONED BY MONTH so each month of data
-- lives in its own physical sub-table, keeping queries fast and focused.

CREATE EXTENSION IF NOT EXISTS postgis;

-- ─── LOCATIONS ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS locations (
    id           SERIAL PRIMARY KEY,
    name         TEXT,
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    is_test_zone BOOLEAN NOT NULL DEFAULT FALSE,

    -- Computed column: automatically derived from lat/lon on every INSERT/UPDATE.
    -- Stored on disk so a GiST index can be built on it.
    -- ST_MakePoint(lon, lat) → raw point in degrees
    -- ST_SetSRID(..., 4326)  → tags it as WGS84 (the GPS coordinate system)
    -- ::geography            → casts to geodesic type (distances in metres)
    geog GEOGRAPHY(Point, 4326)
         GENERATED ALWAYS AS (
             ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography
         ) STORED
);

-- ─── TEST ZONES ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS test_zones (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    center_geog GEOGRAPHY(Point, 4326) NOT NULL,
    radius_m    DOUBLE PRECISION NOT NULL
);

-- ─── WEATHER OBSERVATIONS (partitioned by month) ─────────────────────────────
-- Each month is a separate physical sub-table (partition).
-- Queries that filter by observed_at only scan the relevant month's partition,
-- not the entire table — this is called partition pruning.
--
-- Primary key uses (location_id, observed_at) because PostgreSQL requires
-- the partition key (observed_at) to be part of any unique constraint.
CREATE TABLE IF NOT EXISTS weather_observations (
    location_id INTEGER     NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,   -- °C  (2 m air temperature)
    humidity    DOUBLE PRECISION,   -- %   (relative humidity at 2 m)
    rain        DOUBLE PRECISION,   -- mm  (hourly precipitation)
    soil_temp   DOUBLE PRECISION,   -- °C  (soil temperature 7–28 cm depth)
    PRIMARY KEY (location_id, observed_at)
) PARTITION BY RANGE (observed_at);

-- ─── TRAINING VIEW ───────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW training_observations AS
SELECT wo.*
FROM   weather_observations wo
JOIN   locations l ON l.id = wo.location_id
WHERE  l.is_test_zone = FALSE;
