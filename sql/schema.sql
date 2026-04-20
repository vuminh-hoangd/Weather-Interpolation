-- schema.sql
-- Weather Database: Open-Meteo spatial kNN prediction system
-- Requires: PostgreSQL + PostGIS extension

CREATE EXTENSION IF NOT EXISTS postgis;

-- ─── 1. LOCATIONS ────────────────────────────────────────────────────────────
-- Each row is a geographic point (grid cell or named city) we fetch data for.
CREATE TABLE locations (
    id           SERIAL PRIMARY KEY,
    name         TEXT,
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    is_test_zone BOOLEAN NOT NULL DEFAULT FALSE,
    geog         GEOGRAPHY(Point, 4326)
                     GENERATED ALWAYS AS (
                         ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography
                     ) STORED
);

-- GiST index enables fast kNN ordering with the <-> operator
CREATE INDEX idx_locations_geog ON locations USING GIST(geog);

-- ─── 2. TEST ZONES ───────────────────────────────────────────────────────────
-- Fixed cities/areas withheld from training; used only for evaluation.
CREATE TABLE test_zones (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    center_geog GEOGRAPHY(Point, 4326) NOT NULL,
    radius_m    DOUBLE PRECISION NOT NULL        -- exclusion radius in metres
);

CREATE INDEX idx_test_zones_geog ON test_zones USING GIST(center_geog);

-- ─── 3. WEATHER OBSERVATIONS (raw hourly data from Open-Meteo) ───────────────
CREATE TABLE weather_observations (
    id          BIGSERIAL PRIMARY KEY,
    location_id INTEGER     NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    observed_at TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION,   -- °C  (2 m air temperature)
    humidity    DOUBLE PRECISION,   -- %   (relative humidity at 2 m)
    rain        DOUBLE PRECISION,   -- mm  (hourly precipitation)
    soil_temp   DOUBLE PRECISION,   -- °C  (soil temperature 7–28 cm depth)
    UNIQUE (location_id, observed_at)
);

CREATE INDEX idx_obs_loc_time ON weather_observations (location_id, observed_at);
CREATE INDEX idx_obs_time     ON weather_observations (observed_at);

-- ─── 4. TRAINING VIEW ────────────────────────────────────────────────────────
-- Excludes test-zone locations; all kNN queries should target this view.
CREATE VIEW training_observations AS
SELECT wo.*
FROM   weather_observations wo
JOIN   locations l ON l.id = wo.location_id
WHERE  l.is_test_zone = FALSE;
