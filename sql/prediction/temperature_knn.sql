-- prediction/temperature_knn.sql
-- Temperature prediction via kNN Inverse-Distance Weighting (IDW).
-- Requires: GiST index on locations.geog + B-tree on observed_at.
-- Test zones are automatically excluded via the training_observations view.

-- ─── QUERY 1: raw k nearest neighbours ──────────────────────────────────────
-- Returns the 5 nearest training points to a target location at a given hour.
-- Replace target coordinates and timestamp as needed.
SELECT
    l.name,
    l.lat,
    l.lon,
    wo.temperature,
    wo.humidity,
    wo.rain,
    wo.soil_temp,
    ST_Distance(l.geog, ST_MakePoint(2.352, 48.857)::geography) AS dist_m
FROM   training_observations wo
JOIN   locations l ON l.id = wo.location_id
WHERE  wo.observed_at = '2024-01-15 12:00:00+00'
ORDER  BY l.geog <-> ST_MakePoint(2.352, 48.857)::geography
LIMIT  5;


-- ─── QUERY 2: IDW predicted temperature ─────────────────────────────────────
-- predicted_temp = Σ(temp_i / dist_i) / Σ(1 / dist_i)
-- The 1 m floor prevents division-by-zero when the target is an exact grid point.
WITH neighbours AS (
    SELECT
        wo.temperature,
        GREATEST(
            ST_Distance(l.geog, ST_MakePoint(2.352, 48.857)::geography),
            1.0
        ) AS dist_m
    FROM   training_observations wo
    JOIN   locations l ON l.id = wo.location_id
    WHERE  wo.observed_at = '2024-01-15 12:00:00+00'
    ORDER  BY l.geog <-> ST_MakePoint(2.352, 48.857)::geography
    LIMIT  5
)
SELECT
    ROUND(
        (SUM(temperature / dist_m) / SUM(1.0 / dist_m))::NUMERIC,
        2
    ) AS predicted_temperature_c
FROM neighbours;


-- ─── QUERY 3: batch evaluation — MAE & RMSE across all test zones ─────────
-- For each withheld observation, predicts via IDW from 5 nearest training
-- neighbours at the same timestamp, then computes aggregate error.
WITH test_obs AS (
    SELECT
        wo.id,
        wo.observed_at,
        wo.temperature AS actual_temp,
        l.geog
    FROM   weather_observations wo
    JOIN   locations l ON l.id = wo.location_id
    WHERE  l.is_test_zone = TRUE
),
predictions AS (
    SELECT
        t.id          AS test_obs_id,
        t.actual_temp,
        (
            SELECT SUM(temperature / GREATEST(dist_m, 1.0))
                   / SUM(1.0 / GREATEST(dist_m, 1.0))
            FROM (
                SELECT wo2.temperature,
                       ST_Distance(l2.geog, t.geog) AS dist_m
                FROM   training_observations wo2
                JOIN   locations l2 ON l2.id = wo2.location_id
                WHERE  wo2.observed_at = t.observed_at
                ORDER  BY l2.geog <-> t.geog
                LIMIT  5
            ) neighbours
        ) AS predicted_temp
    FROM test_obs t
)
SELECT
    COUNT(*)                                                            AS evaluated_points,
    ROUND(AVG(ABS(actual_temp - predicted_temp))::NUMERIC,  3)         AS mae_c,
    ROUND(SQRT(AVG((actual_temp - predicted_temp) ^ 2))::NUMERIC, 3)   AS rmse_c
FROM predictions
WHERE predicted_temp IS NOT NULL;
